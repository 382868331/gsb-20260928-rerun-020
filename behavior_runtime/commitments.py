from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable

# Commitment lifecycle states. "waiting" and "started" are kept strictly
# separate: a started commitment records behavior that already happened and
# can never be withdrawn or re-fired.
WAITING = "waiting"
STARTED = "started"
COMPLETED = "completed"
WITHDRAWN = "withdrawn"
EXPIRED = "expired"
ENDED = "ended"

TERMINAL_STATES = frozenset({COMPLETED, WITHDRAWN, EXPIRED, ENDED})

# Same-moment precedence rules, recorded on every decision so the final
# outcome of a conflict can be audited later.
RULE_WITHDRAWAL_FIRST = "explicit-withdrawal-precedes-settlement"
RULE_REMOVAL_FIRST = "participant-removal-precedes-settlement"
RULE_EXPIRY_BEFORE_START = "expiry-precedes-activation"
RULE_STARTED_IMMUTABLE = "started-commitments-cannot-be-withdrawn"

PublishFn = Callable[[str, dict[str, Any], str | None], int]


@dataclass(slots=True)
class Commitment:
    """A time-bounded promise that `performer_id` will run `action` later."""

    commitment_id: str
    requester_id: str
    performer_id: str
    action: str
    params: dict[str, Any]
    created_at: float
    due_at: float
    expires_at: float | None
    wait_reason: str
    cause: dict[str, Any]
    state: str = WAITING
    end_reason: str | None = None
    started_at: float | None = None
    history: list[dict[str, Any]] = field(default_factory=list)

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def record(
        self,
        time: float,
        kind: str,
        *,
        rule: str | None = None,
        detail: dict[str, Any] | None = None,
        event_sequence: int | None = None,
    ) -> None:
        self.history.append(
            {
                "time": time,
                "kind": kind,
                "rule": rule,
                "detail": deepcopy(detail) if detail else {},
                "event_sequence": event_sequence,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "commitment_id": self.commitment_id,
            "requester_id": self.requester_id,
            "performer_id": self.performer_id,
            "action": self.action,
            "params": deepcopy(self.params),
            "created_at": self.created_at,
            "due_at": self.due_at,
            "expires_at": self.expires_at,
            "wait_reason": self.wait_reason,
            "cause": deepcopy(self.cause),
            "state": self.state,
            "end_reason": self.end_reason,
            "started_at": self.started_at,
            "history": deepcopy(self.history),
        }

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> "Commitment":
        return cls(
            commitment_id=values["commitment_id"],
            requester_id=values["requester_id"],
            performer_id=values["performer_id"],
            action=values["action"],
            params=dict(values.get("params") or {}),
            created_at=float(values["created_at"]),
            due_at=float(values["due_at"]),
            expires_at=(
                None if values.get("expires_at") is None else float(values["expires_at"])
            ),
            wait_reason=values.get("wait_reason", "scheduled"),
            cause=dict(values.get("cause") or {}),
            state=values.get("state", WAITING),
            end_reason=values.get("end_reason"),
            started_at=values.get("started_at"),
            history=[dict(entry) for entry in values.get("history", ())],
        )


class CommitmentLedger:
    """Deterministic registry of commitments for one world.

    Settlement order at any moment is fixed: explicit withdrawals and
    participant removals apply immediately when requested (before the next
    settlement), then settlement expires waiting commitments, and only then
    activates due ones. Every transition records the rule that decided it.
    """

    def __init__(self) -> None:
        self._commitments: dict[str, Commitment] = {}
        self._next_id = 0

    def get(self, commitment_id: str) -> Commitment:
        try:
            return self._commitments[commitment_id]
        except KeyError:
            raise KeyError(f"unknown commitment: {commitment_id}") from None

    def waiting(self) -> tuple[Commitment, ...]:
        return tuple(c for c in self._commitments.values() if c.state == WAITING)

    def started(self) -> tuple[Commitment, ...]:
        return tuple(c for c in self._commitments.values() if c.state == STARTED)

    def all(self) -> tuple[Commitment, ...]:
        return tuple(self._commitments.values())

    def request(
        self,
        *,
        now: float,
        requester_id: str,
        performer_id: str,
        action: str,
        params: dict[str, Any] | None = None,
        due_at: float,
        expires_at: float | None = None,
        wait_reason: str = "scheduled",
        cause: dict[str, Any] | None = None,
        publish: PublishFn | None = None,
    ) -> Commitment:
        if expires_at is not None and expires_at < due_at:
            raise ValueError("expires_at must not be earlier than due_at")
        self._next_id += 1
        commitment = Commitment(
            commitment_id=f"c{self._next_id}",
            requester_id=requester_id,
            performer_id=performer_id,
            action=action,
            params=dict(params or {}),
            created_at=now,
            due_at=float(due_at),
            expires_at=expires_at,
            wait_reason=wait_reason,
            cause=dict(cause or {}),
        )
        self._commitments[commitment.commitment_id] = commitment
        sequence = self._emit(publish, "commitment_requested", commitment, now)
        commitment.record(now, "requested", detail={"wait_reason": wait_reason},
                          event_sequence=sequence)
        return commitment

    def withdraw(
        self,
        commitment_id: str,
        *,
        now: float,
        by: str | None = None,
        publish: PublishFn | None = None,
    ) -> bool:
        commitment = self.get(commitment_id)
        if commitment.state == WAITING:
            commitment.state = WITHDRAWN
            commitment.end_reason = f"withdrawn_by:{by}" if by else "withdrawn"
            sequence = self._emit(publish, "commitment_withdrawn", commitment, now,
                                  extra={"by": by})
            commitment.record(now, "withdrawn", rule=RULE_WITHDRAWAL_FIRST,
                              detail={"by": by}, event_sequence=sequence)
            return True
        commitment.record(
            now,
            "withdrawal_rejected",
            rule=RULE_STARTED_IMMUTABLE if commitment.state == STARTED else None,
            detail={"by": by, "state": commitment.state},
        )
        return False

    def update(
        self,
        commitment_id: str,
        *,
        now: float,
        by: str | None = None,
        params: dict[str, Any] | None = None,
        due_at: float | None = None,
        expires_at: float | None = None,
        publish: PublishFn | None = None,
    ) -> bool:
        commitment = self.get(commitment_id)
        if commitment.state != WAITING:
            commitment.record(now, "update_rejected",
                              detail={"by": by, "state": commitment.state})
            return False
        changes: dict[str, Any] = {}
        if params is not None:
            commitment.params = dict(params)
            changes["params"] = deepcopy(params)
        if due_at is not None:
            commitment.due_at = float(due_at)
            changes["due_at"] = float(due_at)
        if expires_at is not None:
            commitment.expires_at = float(expires_at)
            changes["expires_at"] = float(expires_at)
        if commitment.expires_at is not None and commitment.expires_at < commitment.due_at:
            raise ValueError("expires_at must not be earlier than due_at")
        sequence = self._emit(publish, "commitment_updated", commitment, now,
                              extra={"by": by, "changes": changes})
        commitment.record(now, "updated", detail={"by": by, **changes},
                          event_sequence=sequence)
        return True

    def complete(self, commitment_id: str, *, now: float) -> bool:
        commitment = self.get(commitment_id)
        if commitment.state != STARTED:
            return False
        commitment.state = COMPLETED
        commitment.end_reason = "completed"
        commitment.record(now, "completed")
        return True

    def settle(self, now: float, *, publish: PublishFn | None = None) -> list[Commitment]:
        """Expire, then activate. A commitment can never fire twice."""
        fired: list[Commitment] = []
        for commitment in tuple(self._commitments.values()):
            if commitment.state != WAITING:
                continue
            if commitment.expires_at is not None and commitment.expires_at <= now:
                commitment.state = EXPIRED
                commitment.end_reason = "expired"
                sequence = self._emit(publish, "commitment_expired", commitment, now)
                commitment.record(now, "expired", rule=RULE_EXPIRY_BEFORE_START,
                                  event_sequence=sequence)
        for commitment in tuple(self._commitments.values()):
            if commitment.state != WAITING:
                continue
            if commitment.due_at <= now:
                commitment.state = STARTED
                commitment.started_at = now
                sequence = self._emit(publish, "commitment_started", commitment, now)
                commitment.record(now, "started", event_sequence=sequence)
                fired.append(commitment)
        return fired

    def end_for_actor(
        self,
        actor_id: str,
        *,
        now: float,
        publish: PublishFn | None = None,
    ) -> list[Commitment]:
        """End every live commitment that lost a participant."""
        ended: list[Commitment] = []
        for commitment in tuple(self._commitments.values()):
            if commitment.state not in (WAITING, STARTED):
                continue
            if actor_id not in (commitment.requester_id, commitment.performer_id):
                continue
            commitment.state = ENDED
            commitment.end_reason = f"participant_lost:{actor_id}"
            sequence = self._emit(publish, "commitment_ended", commitment, now,
                                  extra={"lost_participant": actor_id})
            commitment.record(now, "ended", rule=RULE_REMOVAL_FIRST,
                              detail={"lost_participant": actor_id},
                              event_sequence=sequence)
            ended.append(commitment)
        return ended

    def _emit(
        self,
        publish: PublishFn | None,
        event_type: str,
        commitment: Commitment,
        now: float,
        *,
        extra: dict[str, Any] | None = None,
    ) -> int | None:
        if publish is None:
            return None
        payload = {
            "commitment_id": commitment.commitment_id,
            "requester_id": commitment.requester_id,
            "performer_id": commitment.performer_id,
            "action": commitment.action,
            "due_at": commitment.due_at,
            "expires_at": commitment.expires_at,
            "cause": deepcopy(commitment.cause),
        }
        if extra:
            payload.update(extra)
        return publish(event_type, payload, commitment.performer_id)

    def snapshot(self) -> dict[str, Any]:
        return {
            "next_id": self._next_id,
            "commitments": [
                commitment.to_dict() for commitment in self._commitments.values()
            ],
        }

    def restore(self, data: dict[str, Any] | None) -> None:
        self._commitments = {}
        self._next_id = 0
        if not data:
            return
        self._next_id = int(data.get("next_id", 0))
        for entry in data.get("commitments", ()):
            commitment = Commitment.from_dict(entry)
            self._commitments[commitment.commitment_id] = commitment
