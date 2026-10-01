from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .world import World


WAITING = "waiting"
RUNNING = "running"
COMPLETED = "completed"
FAILED = "failed"
CANCELLED = "cancelled"
ENDED = "ended"

TERMINAL_STATUSES = frozenset({COMPLETED, FAILED, CANCELLED, ENDED})

# Same-time arbitration rules, recorded verbatim on every transition:
# - Events at the same time are arbitrated by (time, sequence). A due event is
#   scheduled when the request is accepted, so an expiry that becomes due at the
#   same instant a withdrawal is published is delivered first; the withdrawal is
#   then rejected because execution has already started.
# - Actors leaving during a same-time batch leave only after the whole batch has
#   been delivered. Promises still waiting are then ended with an explicit
#   reason; running promises whose promiser has left are ended without erasing
#   behaviour that already happened.
DUE_BEFORE_WITHDRAWAL = "due_before_withdrawal"
WITHDRAWN_WHILE_WAITING = "withdrawn_while_waiting"
WITHDRAWAL_REJECTED_AFTER_START = "withdrawal_rejected_after_start"
PARTICIPANT_LEFT_BEFORE_START = "participant_left_before_start"
PROMISER_LEFT_AFTER_START = "promiser_left_after_start"
REQUESTER_LEFT_WHILE_RUNNING = "requester_left_while_running"
SUPERSEDED_WHILE_WAITING = "superseded_while_waiting"
ACTION_COMPLETED = "action_completed"
ACTION_FAILED = "action_failed"
ACTION_INTERRUPTED = "action_interrupted"
UNKNOWN_ACTION_TYPE = "unknown_action_type"


@dataclass(frozen=True, slots=True)
class PromiseRequest:
    promiser: str
    action_name: str
    action_config: dict[str, Any]
    deadline: float
    requester: str | None = None
    reason: str | None = None
    source_promise_id: str | None = None
    origin_event_sequence: int | None = None


class PromiseBook:
    """Durable ledger of time-limited promises for one world."""

    def __init__(self, world: "World") -> None:
        self._world = world
        self._counter = 0
        self._records: dict[str, dict[str, Any]] = {}
        self._order: list[str] = []
        world.events.subscribe(self._on_event)

    def request(
        self,
        promiser: str,
        action_name: str,
        action_config: dict[str, Any] | None = None,
        *,
        deadline: float,
        requester: str | None = None,
        reason: str | None = None,
        source_promise_id: str | None = None,
        origin_event_sequence: int | None = None,
        supersedes: str | None = None,
    ) -> str:
        now = self._world.clock.now()
        if promiser not in self._world.actor_ids:
            raise ValueError(f"unknown promiser: {promiser!r}")
        if requester is not None and requester not in self._world.actor_ids:
            raise ValueError(f"unknown requester: {requester!r}")
        if deadline < now:
            raise ValueError("promise deadline must not be in the past")
        if supersedes is not None:
            old = self._records[supersedes]
            if old["status"] != WAITING:
                raise ValueError("only waiting promises can be revised")
            if old["requester"] is not None and requester not in (
                None,
                old["requester"],
            ):
                raise ValueError("only the original requester may revise a promise")
            self._terminate(supersedes, CANCELLED, SUPERSEDED_WHILE_WAITING)

        self._counter += 1
        promise_id = f"promise-{self._counter}"
        requested = self._world.publish(
            "promise_requested",
            {
                "promise_id": promise_id,
                "promiser": promiser,
                "requester": requester,
                "action_name": action_name,
                "deadline": deadline,
                "reason": reason,
                "source_promise_id": source_promise_id,
                "supersedes": supersedes,
            },
            actor_id=promiser,
        )
        due = self._world.publish(
            "promise_due",
            {
                "promise_id": promise_id,
                "action_name": action_name,
                "deadline": deadline,
                "waiting_reason": reason or f"waiting_until_{deadline:g}",
                "request_event_sequence": requested.sequence,
            },
            actor_id=promiser,
            delay=max(0.0, deadline - now),
        )
        record = {
            "promise_id": promise_id,
            "promiser": promiser,
            "requester": requester,
            "action_name": action_name,
            "action_config": deepcopy(action_config or {}),
            "requested_at": now,
            "deadline": deadline,
            "waiting_reason": reason or f"waiting_until_{deadline:g}",
            "status": WAITING,
            "run_id": None,
            "end_reason": None,
            "committed_event_sequence": None,
            "rejected_withdrawal_sequences": [],
            "request_event_sequence": requested.sequence,
            "due_event_sequence": due.sequence,
            "source_promise_id": source_promise_id,
            "origin_event_sequence": origin_event_sequence,
            "supersedes": supersedes,
            "transitions": [
                {
                    "time": now,
                    "from_status": None,
                    "to_status": WAITING,
                    "rule": "requested",
                    "effective_event_sequence": requested.sequence,
                    "source_promise_id": source_promise_id,
                    "origin_event_sequence": origin_event_sequence,
                }
            ],
        }
        self._records[promise_id] = record
        self._order.append(promise_id)
        return promise_id

    def withdraw(
        self,
        promise_id: str,
        *,
        requester: str | None = None,
        reason: str | None = None,
    ) -> None:
        record = self._records[promise_id]
        if (
            record["requester"] is not None
            and requester is not None
            and record["requester"] != requester
        ):
            raise PermissionError(
                f"{requester!r} did not request promise {promise_id!r}"
            )
        # Published as an event so a withdrawal that competes with a due event
        # at the same instant is arbitrated by (time, sequence).
        self._world.publish(
            "promise_withdrawn",
            {"promise_id": promise_id, "requester": requester, "reason": reason},
            actor_id=record["promiser"],
        )

    def revise(self, promise_id: str, request: PromiseRequest) -> str:
        old = self._records[promise_id]
        return self.request(
            request.promiser,
            request.action_name,
            request.action_config,
            deadline=request.deadline,
            requester=request.requester or old["requester"],
            reason=request.reason,
            source_promise_id=request.source_promise_id,
            origin_event_sequence=request.origin_event_sequence,
            supersedes=promise_id,
        )

    def start(self, promise_id: str, run_id: int, event_sequence: int) -> None:
        record = self._records[promise_id]
        if record["status"] != WAITING:
            return
        record["status"] = RUNNING
        record["run_id"] = run_id
        record["transitions"].append(
            {
                "time": self._world.clock.now(),
                "from_status": WAITING,
                "to_status": RUNNING,
                "rule": DUE_BEFORE_WITHDRAWAL,
                "effective_event_sequence": event_sequence,
            }
        )
        self._world.publish(
            "promise_started",
            {
                "promise_id": promise_id,
                "promiser": record["promiser"],
                "requester": record["requester"],
                "action_name": record["action_name"],
                "run_id": run_id,
                "request_event_sequence": record["request_event_sequence"],
                "source_promise_id": record["source_promise_id"],
            },
        )
        self._notify_requester(record, "promise_started", run_id)

    def finish_run(self, promise_id: str, run_id: int, status_value: str) -> None:
        record = self._records.get(promise_id)
        if record is None or record["status"] != RUNNING:
            return
        if record["run_id"] != run_id:
            return
        if status_value == "completed":
            self._terminate(promise_id, COMPLETED, ACTION_COMPLETED)
        elif status_value == "failed":
            self._terminate(promise_id, FAILED, ACTION_FAILED)
        else:
            self._terminate(promise_id, CANCELLED, ACTION_INTERRUPTED)

    def get(self, promise_id: str) -> dict[str, Any]:
        return deepcopy(self._records[promise_id])

    def find(self, promise_id: str) -> dict[str, Any] | None:
        record = self._records.get(promise_id)
        return deepcopy(record) if record is not None else None

    def abort_unknown_action(self, promise_id: str) -> None:
        if promise_id not in self._records:
            return
        self._world.events.discard_promise_events(promise_id)
        self._terminate(promise_id, FAILED, UNKNOWN_ACTION_TYPE)

    def list(
        self,
        *,
        status: str | None = None,
        promiser: str | None = None,
    ) -> tuple[dict[str, Any], ...]:
        records = []
        for promise_id in self._order:
            record = self._records[promise_id]
            if status is not None and record["status"] != status:
                continue
            if promiser is not None and record["promiser"] != promiser:
                continue
            records.append(deepcopy(record))
        return tuple(records)

    def settle_participant_left(self, actor_id: str) -> None:
        for promise_id in tuple(self._order):
            record = self._records[promise_id]
            status = record["status"]
            if status in TERMINAL_STATUSES:
                continue
            if record["promiser"] == actor_id:
                self._world.events.discard_promise_events(promise_id)
                if status == WAITING:
                    if record["committed_event_sequence"] is not None:
                        self._terminate(
                            promise_id, ENDED, PROMISER_LEFT_AFTER_START
                        )
                        continue
                    self._terminate(
                        promise_id, ENDED, PARTICIPANT_LEFT_BEFORE_START
                    )
                else:
                    self._terminate(promise_id, ENDED, PROMISER_LEFT_AFTER_START)
            elif record["requester"] == actor_id:
                self._world.events.discard_promise_events(promise_id)
                if status == WAITING:
                    if record["committed_event_sequence"] is not None:
                        # Due was arbitrated first in this batch: the promised
                        # behaviour is due to run even though the requester is
                        # gone. Leave the promise runnable; record the loss.
                        record["transitions"].append(
                            {
                                "time": self._world.clock.now(),
                                "from_status": WAITING,
                                "to_status": WAITING,
                                "rule": REQUESTER_LEFT_WHILE_RUNNING,
                                "effective_event_sequence": record[
                                    "committed_event_sequence"
                                ],
                            }
                        )
                        continue
                    self._terminate(
                        promise_id, ENDED, PARTICIPANT_LEFT_BEFORE_START
                    )
                else:
                    record["transitions"].append(
                        {
                            "time": self._world.clock.now(),
                            "from_status": RUNNING,
                            "to_status": RUNNING,
                            "rule": REQUESTER_LEFT_WHILE_RUNNING,
                            "effective_event_sequence": None,
                        }
                    )

    def snapshot(self) -> dict[str, Any]:
        return {
            "counter": self._counter,
            "order": list(self._order),
            "records": deepcopy(self._records),
        }

    def restore(self, data: dict[str, Any] | None) -> None:
        data = data or {}
        self._counter = int(data.get("counter", 0))
        records = deepcopy(data.get("records", {}))
        self._records = dict(records)
        order = data.get("order")
        if order is None:
            order = list(self._records)
        self._order = [
            promise_id for promise_id in order if promise_id in self._records
        ]

    def _on_event(self, event: Any) -> None:
        if event.type == "promise_due":
            self._commit_due(event)
        elif event.type == "promise_withdrawn":
            self._apply_withdrawal(event)

    def _commit_due(self, event: Any) -> None:
        promise_id = event.payload.get("promise_id")
        record = self._records.get(promise_id)
        if record is None or record["status"] != WAITING:
            return
        if record["committed_event_sequence"] is None:
            record["committed_event_sequence"] = event.sequence

    def _apply_withdrawal(self, event: Any) -> None:
        promise_id = event.payload["promise_id"]
        record = self._records.get(promise_id)
        if record is None:
            return
        status = record["status"]
        if status == WAITING:
            committed_sequence = record["committed_event_sequence"]
            if (
                committed_sequence is not None
                and committed_sequence < event.sequence
            ):
                # The due event was delivered first in this same-time batch.
                # The decision stands: execution starts on the next tick and
                # this withdrawal cannot erase it.
                record["rejected_withdrawal_sequences"].append(event.sequence)
                record["transitions"].append(
                    {
                        "time": event.time,
                        "from_status": WAITING,
                        "to_status": WAITING,
                        "rule": DUE_BEFORE_WITHDRAWAL,
                        "effective_event_sequence": committed_sequence,
                        "rejected_event_sequence": event.sequence,
                    }
                )
                return
            self._world.events.discard_promise_events(promise_id)
            self._terminate(
                promise_id,
                CANCELLED,
                WITHDRAWN_WHILE_WAITING,
                effective_event_sequence=event.sequence,
            )
            return
        if status != RUNNING:
            return
            record["transitions"].append(
                {
                    "time": event.time,
                    "from_status": RUNNING,
                    "to_status": RUNNING,
                    "rule": rejected_rule,
                    "effective_event_sequence": event.sequence,
                }
            )

    def _terminate(
        self,
        promise_id: str,
        status: str,
        reason: str,
        *,
        effective_event_sequence: int | None = None,
    ) -> None:
        record = self._records[promise_id]
        previous = record["status"]
        if previous in TERMINAL_STATUSES:
            return
        record["status"] = status
        record["end_reason"] = reason
        record["transitions"].append(
            {
                "time": self._world.clock.now(),
                "from_status": previous,
                "to_status": status,
                "rule": reason,
                "effective_event_sequence": effective_event_sequence,
            }
        )
        self._world.publish(
            "promise_ended",
            {
                "promise_id": promise_id,
                "promiser": record["promiser"],
                "requester": record["requester"],
                "status": status,
                "end_reason": reason,
                "request_event_sequence": record["request_event_sequence"],
                "source_promise_id": record["source_promise_id"],
            },
            actor_id=record["promiser"],
        )
        self._notify_requester(record, "promise_ended", status, reason)

    def _notify_requester(
        self,
        record: dict[str, Any],
        kind: str,
        *details: Any,
    ) -> None:
        requester = record["requester"]
        if requester is None or requester == record["promiser"]:
            return
        payload = {
            "promise_id": record["promise_id"],
            "promiser": record["promiser"],
            "status": record["status"],
        }
        if kind == "promise_started":
            payload["run_id"] = details[0]
        else:
            payload["end_reason"] = details[1]
        self._world.publish(
            f"{kind}_notice",
            payload,
            actor_id=requester,
        )
