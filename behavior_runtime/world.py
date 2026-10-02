from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .clock import Clock, VirtualClock
from .commitments import Commitment, CommitmentLedger
from .events import Event, EventBus
from .groups import GroupMember, JointCommitmentGroup, JointGroupLedger


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
        self.joint_groups = JointGroupLedger()

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
        self.joint_groups.end_for_actor(
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
            "joint_groups": self.joint_groups.snapshot(),
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
        self.joint_groups.restore(deepcopy(data.get("joint_groups")))
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
            self.joint_groups.settle(
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

    # ------------------------------------------------------------------
    # Joint commitment groups
    # ------------------------------------------------------------------

    def request_joint_group(
        self,
        *,
        action: str,
        members: list[dict[str, Any] | tuple[str, str]],
        resources: tuple[str, ...] | list[str] = (),
        params: dict[str, Any] | None = None,
        valid_for: float | None = None,
        member_valid_for: float | None = None,
        requester_id: str,
        wait_reason: str = "joint-scheduled",
        cause: dict[str, Any] | None = None,
        private_notes: dict[str, dict[str, Any]] | None = None,
    ) -> JointCommitmentGroup:
        now = self.clock.now()
        group_members = [self._build_member(m, now, member_valid_for) for m in members]
        return self.joint_groups.request(
            now=now,
            action=action,
            params=params,
            members=group_members,
            resources=tuple(resources),
            valid_until=None if valid_for is None else now + valid_for,
            requester_id=requester_id,
            wait_reason=wait_reason,
            cause=cause,
            private_notes=private_notes,
            publish=self._publish_commitment_event,
        )

    @staticmethod
    def _build_member(
        spec: dict[str, Any] | tuple[str, str],
        now: float,
        member_valid_for: float | None,
    ) -> GroupMember:
        if isinstance(spec, dict):
            valid_for = spec.get("valid_for", member_valid_for)
            return GroupMember(
                actor_id=spec["actor_id"],
                role=spec.get("role", "participant"),
                action=spec["action"],
                params=dict(spec.get("params") or {}),
                valid_until=None if valid_for is None else now + valid_for,
            )
        actor_id, role = spec
        return GroupMember(
            actor_id=actor_id,
            role=role,
            action="",
            valid_until=None if member_valid_for is None else now + member_valid_for,
        )

    def mark_joint_member_ready(
        self,
        group_id: str,
        actor_id: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> bool:
        return self.joint_groups.mark_ready(
            group_id,
            actor_id,
            now=self.clock.now(),
            params=params,
            publish=self._publish_commitment_event,
        )

    def withdraw_joint_group(
        self, group_id: str, *, by: str | None = None, reason: str | None = None
    ) -> bool:
        return self.joint_groups.withdraw(
            group_id,
            now=self.clock.now(),
            by=by,
            reason=reason,
            publish=self._publish_commitment_event,
        )

    def propose_joint_replacement(
        self,
        old_group_id: str,
        *,
        action: str,
        members: list[dict[str, Any] | tuple[str, str]],
        resources: tuple[str, ...] | list[str] = (),
        params: dict[str, Any] | None = None,
        valid_for: float | None = None,
        member_valid_for: float | None = None,
        requester_id: str,
        reason: str | None = None,
        wait_reason: str = "joint-replacement",
        cause: dict[str, Any] | None = None,
        private_notes: dict[str, dict[str, Any]] | None = None,
    ) -> JointCommitmentGroup:
        now = self.clock.now()
        group_members = [self._build_member(m, now, member_valid_for) for m in members]
        return self.joint_groups.propose_replacement(
            old_group_id,
            now=now,
            action=action,
            params=params,
            members=group_members,
            resources=tuple(resources),
            valid_until=None if valid_for is None else now + valid_for,
            requester_id=requester_id,
            reason=reason,
            wait_reason=wait_reason,
            cause=cause,
            private_notes=private_notes,
            publish=self._publish_commitment_event,
        )

    def complete_joint_group(self, group_id: str) -> bool:
        return self.joint_groups.complete(
            group_id, now=self.clock.now(), publish=self._publish_commitment_event
        )

    def joint_group_status(self, group_id: str) -> dict[str, Any]:
        return self.joint_groups.get(group_id).status_view(self.joint_groups.resources)

    def resource_status(self) -> list[dict[str, Any]]:
        return self.joint_groups.resources.status()

    def set_group_private_note(
        self, group_id: str, actor_id: str, key: str, value: Any
    ) -> None:
        self.joint_groups.set_private_note(group_id, actor_id, key, value)

    def get_group_private_note(
        self, group_id: str, actor_id: str, key: str, default: Any = None
    ) -> Any:
        return self.joint_groups.get_private_note(group_id, actor_id, key, default)

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
