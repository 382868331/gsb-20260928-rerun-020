from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .clock import Clock, VirtualClock
from .commitments import Commitment, CommitmentLedger, JointCommitment
from .events import Event, EventBus


@dataclass(frozen=True, slots=True)
class Observation:
    actor_id: str
    time: float
    state: dict[str, Any]
    events: tuple[Event, ...]


class World:
    """Shared, explicitly mutated world state and event queue."""

    def __init__(
        self,
        state: dict[str, Any] | None = None,
        *,
        clock: Clock | None = None,
        event_bus: EventBus | None = None,
    ) -> None:
        self._state = deepcopy(state or {})
        self.clock: Clock = clock or VirtualClock()
        self.events = event_bus or EventBus()
        self._mailboxes: dict[str, list[Event]] = {}
        self._actors: dict[str, None] = {}
        self._batch_participants: tuple[str, ...] = ()
        self._batch_new_members: set[str] = set()
        self._pending_actor_changes: dict[str, bool] = {}
        self._dispatching = False
        self.commitments = CommitmentLedger()

    def register_actor(self, actor_id: str) -> None:
        if self._dispatching:
            self._pending_actor_changes[actor_id] = True
            return
        self._actors.setdefault(actor_id, None)
        self._mailboxes.setdefault(actor_id, [])

    def remove_actor(self, actor_id: str) -> None:
        if self._dispatching:
            self._pending_actor_changes[actor_id] = False
            return
        self._actors.pop(actor_id, None)
        self._mailboxes.pop(actor_id, None)
        self.events.discard_actor_events(actor_id)
        self.commitments.end_for_actor(
            actor_id, now=self.clock.now(), publish=self._publish_commitment_event
        )

    def unregister_actor(self, actor_id: str) -> None:
        self.remove_actor(actor_id)

    def actor_order(self, actor_id: str) -> int | None:
        try:
            return list(self._actors).index(actor_id)
        except ValueError:
            return None

    @property
    def state(self) -> dict[str, Any]:
        return self._state

    @property
    def actor_ids(self) -> tuple[str, ...]:
        return tuple(self._actors)

    def snapshot(self) -> dict[str, Any]:
        return {
            "state": deepcopy(self._state),
            "time": self.clock.now(),
            "clock": deepcopy(self.clock.snapshot()),
            "events": deepcopy(self.events.snapshot()),
            "actors": list(self._actors),
            "actor_order": list(self._actors),
            "mailboxes": {
                actor_id: [
                    event.to_dict()
                    for event in self._mailboxes.get(actor_id, ())
                ]
                for actor_id in self._actors
            },
            "commitments": self.commitments.snapshot(),
        }

    def restore(self, data: dict[str, Any]) -> None:
        self._dispatching = False
        self._pending_actor_changes = {}
        self._batch_participants = ()
        self._batch_new_members = set()
        self._state = deepcopy(data["state"])
        clock_data = data.get("clock") or {"time": data.get("time", 0.0)}
        if not hasattr(self.clock, "restore"):
            raise TypeError("world clock does not support restoration")
        self.clock.restore(clock_data)
        self.events.restore(deepcopy(data["events"]))
        actor_ids = data.get("actor_order", data.get("actors", ()))
        self._actors = dict.fromkeys(actor_ids)
        self._mailboxes = {
            actor_id: [Event.from_dict(event_data) for event_data in events]
            for actor_id, events in data.get("mailboxes", {}).items()
            if actor_id in self._actors
        }
        for actor_id in self._actors:
            self._mailboxes.setdefault(actor_id, [])
        self.commitments.restore(deepcopy(data.get("commitments")))
        for event_data in list(self.events.snapshot().get("scheduled", ())) + list(
            self.events.snapshot().get("due", ())
        ):
            target_actor_id = event_data.get("actor_id")
            if target_actor_id is not None and target_actor_id not in self._actors:
                self.events.discard_actor_events(target_actor_id)

    def observe(self, actor_id: str) -> Observation:
        events = tuple(self._mailboxes.pop(actor_id, []))
        return Observation(
            actor_id=actor_id,
            time=self.clock.now(),
            state=deepcopy(self._state),
            events=events,
        )

    def publish(
        self,
        type: str,
        payload: dict[str, Any] | None = None,
        *,
        actor_id: str | None = None,
        action_run_id: int | None = None,
        delay: float = 0.0,
    ) -> Event:
        return self.events.publish(
            type,
            payload,
            time=self.clock.now(),
            actor_id=actor_id,
            action_run_id=action_run_id,
            delay=delay,
        )

    def dispatch_due(self) -> list[Event]:
        self._dispatching = True
        self._batch_participants = tuple(self._actors)
        try:
            self.commitments.settle(
                self.clock.now(), publish=self._publish_commitment_event
            )
            dispatched = self.events.dispatch_due(self.clock.now())
        finally:
            self._dispatching = False
            self._apply_pending_actor_changes()
        for event in dispatched:
            if event.actor_id is not None:
                if event.actor_id in self._actors:
                    self._mailboxes.setdefault(event.actor_id, []).append(event)
            else:
                for actor_id in self._batch_participants:
                    if actor_id in self._actors and actor_id not in self._batch_new_members:
                        self._mailboxes.setdefault(actor_id, []).append(event)
        self._batch_participants = ()
        self._batch_new_members = set()
        return dispatched

    def request_commitment(
        self,
        *,
        requester_id: str,
        performer_id: str,
        action: str,
        params: dict[str, Any] | None = None,
        delay: float = 0.0,
        valid_for: float | None = None,
        wait_reason: str = "scheduled",
        cause: dict[str, Any] | None = None,
    ) -> Commitment:
        now = self.clock.now()
        return self.commitments.request(
            now=now,
            requester_id=requester_id,
            performer_id=performer_id,
            action=action,
            params=params,
            due_at=now + delay,
            expires_at=None if valid_for is None else now + valid_for,
            wait_reason=wait_reason,
            cause=cause,
            publish=self._publish_commitment_event,
        )

    def withdraw_commitment(self, commitment_id: str, *, by: str | None = None) -> bool:
        return self.commitments.withdraw(
            commitment_id,
            now=self.clock.now(),
            by=by,
            publish=self._publish_commitment_event,
        )

    def update_commitment(
        self,
        commitment_id: str,
        *,
        by: str | None = None,
        params: dict[str, Any] | None = None,
        delay: float | None = None,
        valid_for: float | None = None,
    ) -> bool:
        now = self.clock.now()
        return self.commitments.update(
            commitment_id,
            now=now,
            by=by,
            params=params,
            due_at=None if delay is None else now + delay,
            expires_at=None if valid_for is None else now + valid_for,
            publish=self._publish_commitment_event,
        )

    def complete_commitment(self, commitment_id: str) -> bool:
        return self.commitments.complete(commitment_id, now=self.clock.now())

    def request_joint_commitment(
        self,
        *,
        requester_id: str,
        action: str,
        participants: list[dict[str, Any]],
        params: dict[str, Any] | None = None,
        resources: tuple[str, ...] = (),
        wait_reason: str = "scheduled",
        cause: dict[str, Any] | None = None,
    ) -> JointCommitment:
        return self.commitments.request_joint(
            now=self.clock.now(),
            requester_id=requester_id,
            action=action,
            participants=participants,
            params=params,
            resources=resources,
            wait_reason=wait_reason,
            cause=cause,
            publish=self._publish_commitment_event,
        )

    def request_phased_joint_commitment(
        self,
        *,
        requester_id: str,
        action: str,
        participants: list[dict[str, Any]],
        phases: tuple[str, ...] = ("prepare", "execute", "handover"),
        phase_resources: dict[str, tuple[str, ...]] | None = None,
        params: dict[str, Any] | None = None,
        wait_reason: str = "scheduled",
        cause: dict[str, Any] | None = None,
    ) -> JointCommitment:
        """Register a joint task whose phases negotiate resources separately.

        Each participant may pass a phases mapping with a per-phase delay and
        valid_for. Readiness for an earlier phase never occupies the
        resources requested by later phases.
        """
        return self.commitments.request_joint(
            now=self.clock.now(),
            requester_id=requester_id,
            action=action,
            participants=participants,
            params=params,
            resources=(),
            wait_reason=wait_reason,
            cause=cause,
            publish=self._publish_commitment_event,
            phases=phases,
            phase_resources=phase_resources,
        )

    def finish_joint_phase(
        self, group_id: str, phase_id: str | None = None
    ) -> JointCommitment | None:
        return self.commitments.finish_phase(
            group_id, phase_id, now=self.clock.now(),
            publish=self._publish_commitment_event,
        )

    def cancel_joint_phase(
        self,
        group_id: str,
        phase_id: str,
        *,
        by: str | None = None,
        reason: str | None = None,
    ) -> JointCommitment | None:
        return self.commitments.cancel_phase(
            group_id,
            phase_id,
            now=self.clock.now(),
            by=by,
            reason=reason,
            publish=self._publish_commitment_event,
        )

    def reschedule_joint_phase(
        self,
        group_id: str,
        phase_id: str,
        *,
        by: str | None = None,
        reason: str | None = None,
        delay: float = 0.0,
        valid_for: float | None = None,
        replacements: dict[str, str] | None = None,
        resources: tuple[str, ...] | None = None,
    ) -> JointCommitment | None:
        """Re-plan a not-yet-started phase with delayed/replaced arrangements.

        Returns the group when an arrangement is adopted, or None when no
        arrangement is currently adoptable (resources still held, or a
        replacement member busy); in that case the group keeps waiting.
        """
        return self.commitments.reschedule_phase(
            group_id,
            phase_id,
            now=self.clock.now(),
            by=by,
            reason=reason,
            delay=delay,
            valid_for=valid_for,
            replacements=replacements,
            resources=resources,
            publish=self._publish_commitment_event,
        )

    def diagnose_joint_waits(self) -> dict[str, Any]:
        return self.commitments.diagnose_waits(now=self.clock.now())

    def withdraw_joint_commitment(
        self,
        group_id: str,
        *,
        by: str | None = None,
        reason: str | None = None,
        replacement: dict[str, str] | None = None,
    ) -> JointCommitment | None:
        return self.commitments.withdraw_joint(
            group_id,
            now=self.clock.now(),
            by=by,
            reason=reason,
            replacement=replacement,
            publish=self._publish_commitment_event,
        )

    def complete_joint_commitment(self, group_id: str) -> bool:
        return self.commitments.complete_joint(
            group_id,
            now=self.clock.now(),
            publish=self._publish_commitment_event,
        )

    def joint_status(self, group_id: str) -> dict[str, Any]:
        return self.commitments.joint_status(group_id, now=self.clock.now())

    def _publish_commitment_event(
        self, event_type: str, payload: dict[str, Any], actor_id: str | None
    ) -> int:
        event = self.publish(event_type, payload, actor_id=actor_id)
        return event.sequence

    def _apply_pending_actor_changes(self) -> None:
        while self._pending_actor_changes:
            actor_id = next(iter(self._pending_actor_changes))
            should_join = self._pending_actor_changes.pop(actor_id)
            if should_join:
                self._batch_new_members.add(actor_id)
                self.register_actor(actor_id)
            else:
                self.remove_actor(actor_id)
