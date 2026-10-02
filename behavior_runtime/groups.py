from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable

# Joint commitment group lifecycle. A group only activates once *all*
# members are ready at the same settlement. Once started it can never go
# back to waiting, mirroring the single-commitment rule.
GROUP_WAITING = "waiting"
GROUP_STARTED = "started"
GROUP_COMPLETED = "completed"
GROUP_WITHDRAWN = "withdrawn"
GROUP_EXPIRED = "expired"
GROUP_ABANDONED = "abandoned"
GROUP_SUPERSEDED = "superseded"

GROUP_TERMINAL_STATES = frozenset(
    {
        GROUP_COMPLETED,
        GROUP_WITHDRAWN,
        GROUP_EXPIRED,
        GROUP_ABANDONED,
        GROUP_SUPERSEDED,
    }
)

# Resource reservation lifecycle.
RES_FREE = "free"
RES_PROVISIONAL = "provisional"   # group holds an option while still waiting
RES_HELD = "held"                 # group started, action occupies the resource

# Same-moment precedence rules for joint decisions.
RULE_GROUP_WITHDRAWAL_FIRST = "group-withdrawal-precedes-settlement"
RULE_GROUP_REMOVAL_FIRST = "group-participant-removal-precedes-settlement"
RULE_GROUP_EXPIRY_BEFORE_START = "group-expiry-precedes-activation"
RULE_ALL_READY_BEFORE_START = "all-members-ready-before-joint-activation"
RULE_STARTED_GROUP_IMMUTABLE = "started-group-cannot-be-withdrawn"
RULE_RESOURCE_ORDER = "resource-contention-earliest-request-wins"
RULE_LOSER_RELEASES = "resource-contention-loser-releases-reservation"
RULE_SUPERSEDED_PRESERVED = "superseded-proposal-kept-for-history"

PublishFn = Callable[[str, dict[str, Any], str | None], int]


@dataclass(slots=True)
class ResourceReservation:
    """A named shared resource and its current occupant."""

    resource_id: str
    state: str = RES_FREE
    group_id: str | None = None
    acquired_at: float | None = None
    released_at: float | None = None
    history: list[dict[str, Any]] = field(default_factory=list)

    def record(self, time: float, kind: str, detail: dict[str, Any] | None = None) -> None:
        self.history.append({"time": time, "kind": kind, "detail": deepcopy(detail or {})})

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource_id": self.resource_id,
            "state": self.state,
            "group_id": self.group_id,
            "acquired_at": self.acquired_at,
            "released_at": self.released_at,
            "history": deepcopy(self.history),
        }

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> "ResourceReservation":
        return cls(
            resource_id=values["resource_id"],
            state=values.get("state", RES_FREE),
            group_id=values.get("group_id"),
            acquired_at=values.get("acquired_at"),
            released_at=values.get("released_at"),
            history=[dict(entry) for entry in values.get("history", ())],
        )


class ResourceRegistry:
    """Deterministic registry of shareable resources.

    Reservations are provisional while a joint group is still waiting:
    they are visible to users as "prepared occupation" but never block
    other groups from competing at settlement. When a contention loser
    releases, nothing remains occupied.
    """

    def __init__(self) -> None:
        self._resources: dict[str, ResourceReservation] = {}

    def _ensure(self, resource_id: str) -> ResourceReservation:
        resource = self._resources.get(resource_id)
        if resource is None:
            resource = ResourceReservation(resource_id)
            self._resources[resource_id] = resource
        return resource

    def provision(self, resource_id: str, group_id: str, now: float) -> bool:
        resource = self._ensure(resource_id)
        if resource.state in (RES_PROVISIONAL, RES_HELD):
            return resource.group_id == group_id
        resource.state = RES_PROVISIONAL
        resource.group_id = group_id
        resource.acquired_at = now
        resource.released_at = None
        resource.record(
            now, "provisioned", {"group_id": group_id}
        )
        return True

    def holder(self, resource_id: str) -> str | None:
        resource = self._resources.get(resource_id)
        if resource is None or resource.state == RES_FREE:
            return None
        return resource.group_id

    def confirm(self, resource_id: str, group_id: str, now: float) -> None:
        resource = self._ensure(resource_id)
        if resource.group_id != group_id:
            return
        resource.state = RES_HELD
        resource.record(now, "held", {"group_id": group_id})

    def release_for_group(self, group_id: str, now: float) -> list[str]:
        released: list[str] = []
        for resource in self._resources.values():
            if resource.group_id != group_id:
                continue
            if resource.state == RES_FREE:
                continue
            resource.state = RES_FREE
            resource.group_id = None
            resource.acquired_at = None
            resource.released_at = now
            resource.record(now, "released", {"group_id": group_id})
            released.append(resource.resource_id)
        return released

    def occupied_for(self, group_id: str) -> tuple[str, ...]:
        return tuple(
            resource.resource_id
            for resource in self._resources.values()
            if resource.group_id == group_id and resource.state != RES_FREE
        )

    def status(self) -> list[dict[str, Any]]:
        return [
            {
                "resource_id": resource.resource_id,
                "state": resource.state,
                "group_id": resource.group_id,
            }
            for resource in self._resources.values()
            if resource.state != RES_FREE
        ]

    def snapshot(self) -> dict[str, Any]:
        return {"resources": [r.to_dict() for r in self._resources.values()]}

    def restore(self, data: dict[str, Any] | None) -> None:
        self._resources = {}
        if not data:
            return
        for entry in data.get("resources", ()):
            resource = ResourceReservation.from_dict(entry)
            self._resources[resource.resource_id] = resource


@dataclass(slots=True)
class GroupMember:
    """One participant's promise inside a joint commitment group."""

    actor_id: str
    role: str
    action: str
    params: dict[str, Any] = field(default_factory=dict)
    ready: bool = False
    ready_at: float | None = None
    valid_until: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "actor_id": self.actor_id,
            "role": self.role,
            "action": self.action,
            "params": deepcopy(self.params),
            "ready": self.ready,
            "ready_at": self.ready_at,
            "valid_until": self.valid_until,
        }

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> "GroupMember":
        return cls(
            actor_id=values["actor_id"],
            role=values.get("role", "participant"),
            action=values["action"],
            params=dict(values.get("params") or {}),
            ready=bool(values.get("ready", False)),
            ready_at=values.get("ready_at"),
            valid_until=values.get("valid_until"),
        )


class JointCommitmentGroup:
    """A joint decision that only activates once every member is ready."""

    def __init__(
        self,
        *,
        group_id: str,
        action: str,
        params: dict[str, Any],
        members: list[GroupMember],
        resources: tuple[str, ...],
        created_at: float,
        valid_until: float,
        requester_id: str,
        wait_reason: str = "joint-scheduled",
        cause: dict[str, Any] | None = None,
        proposal_id: str | None = None,
        supersedes: str | None = None,
        private_notes: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.group_id = group_id
        self.action = action
        self.params = deepcopy(params)
        self.members = list(members)
        self.resources = tuple(resources)
        self.created_at = created_at
        self.valid_until = valid_until
        self.requester_id = requester_id
        self.wait_reason = wait_reason
        self.cause = deepcopy(cause or {})
        self.proposal_id = proposal_id or group_id
        self.supersedes = supersedes
        self.state: str = GROUP_WAITING
        self.end_reason: str | None = None
        self.started_at: float | None = None
        self.decision_id: str | None = None
        self.decision_event_sequence: int | None = None
        self.withdrawal_reason: str | None = None
        self.history: list[dict[str, Any]] = []
        # Per-participant private notes: separate alternatives stay isolated.
        self.private_notes: dict[str, dict[str, Any]] = {
            actor_id: deepcopy(values)
            for actor_id, values in (private_notes or {}).items()
        }

    @property
    def terminal(self) -> bool:
        return self.state in GROUP_TERMINAL_STATES

    @property
    def participants(self) -> tuple[str, ...]:
        return tuple(member.actor_id for member in self.members)

    def member(self, actor_id: str) -> GroupMember:
        for candidate in self.members:
            if candidate.actor_id == actor_id:
                return candidate
        raise KeyError(f"{actor_id} is not a member of {self.group_id}")

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

    def waiting_on(self) -> tuple[str, ...]:
        if self.state != GROUP_WAITING:
            return ()
        return tuple(member.actor_id for member in self.members if not member.ready)

    def satisfied_constraints(self) -> tuple[str, ...]:
        return tuple(member.actor_id for member in self.members if member.ready)

    def prepared_resources(self) -> tuple[str, ...]:
        if self.state == GROUP_WAITING:
            return tuple(self.resources)
        return ()

    def status_view(self, resources: ResourceRegistry) -> dict[str, Any]:
        return {
            "group_id": self.group_id,
            "proposal_id": self.proposal_id,
            "state": self.state,
            "action": self.action,
            "waiting_on": list(self.waiting_on()),
            "satisfied": list(self.satisfied_constraints()),
            "prepared_resources": list(self.prepared_resources()),
            "occupied_resources": list(resources.occupied_for(self.group_id)),
            "supersedes": self.supersedes,
            "decision_id": self.decision_id,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "group_id": self.group_id,
            "action": self.action,
            "params": deepcopy(self.params),
            "members": [member.to_dict() for member in self.members],
            "resources": list(self.resources),
            "created_at": self.created_at,
            "valid_until": self.valid_until,
            "requester_id": self.requester_id,
            "wait_reason": self.wait_reason,
            "cause": deepcopy(self.cause),
            "proposal_id": self.proposal_id,
            "supersedes": self.supersedes,
            "state": self.state,
            "end_reason": self.end_reason,
            "started_at": self.started_at,
            "decision_id": self.decision_id,
            "decision_event_sequence": self.decision_event_sequence,
            "withdrawal_reason": self.withdrawal_reason,
            "history": deepcopy(self.history),
            "private_notes": deepcopy(self.private_notes),
        }

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> "JointCommitmentGroup":
        group = cls(
            group_id=values["group_id"],
            action=values["action"],
            params=dict(values.get("params") or {}),
            members=[GroupMember.from_dict(m) for m in values.get("members", ())],
            resources=tuple(values.get("resources", ())),
            created_at=float(values["created_at"]),
            valid_until=float(values["valid_until"]),
            requester_id=values["requester_id"],
            wait_reason=values.get("wait_reason", "joint-scheduled"),
            cause=dict(values.get("cause") or {}),
            proposal_id=values.get("proposal_id"),
            supersedes=values.get("supersedes"),
            private_notes={k: dict(v) for k, v in values.get("private_notes", {}).items()},
        )
        group.state = values.get("state", GROUP_WAITING)
        group.end_reason = values.get("end_reason")
        group.started_at = values.get("started_at")
        group.decision_id = values.get("decision_id")
        group.decision_event_sequence = values.get("decision_event_sequence")
        group.withdrawal_reason = values.get("withdrawal_reason")
        group.history = [dict(entry) for entry in values.get("history", ())]
        return group


class JointGroupLedger:
    """Deterministic registry of joint commitment groups and resources."""

    def __init__(self) -> None:
        self._groups: dict[str, JointCommitmentGroup] = {}
        self._next_id = 0
        self.resources = ResourceRegistry()

    def get(self, group_id: str) -> JointCommitmentGroup:
        try:
            return self._groups[group_id]
        except KeyError:
            raise KeyError(f"unknown joint group: {group_id}") from None

    def all(self) -> tuple[JointCommitmentGroup, ...]:
        return tuple(self._groups.values())

    def waiting(self) -> tuple[JointCommitmentGroup, ...]:
        return tuple(g for g in self._groups.values() if g.state == GROUP_WAITING)

    def request(
        self,
        *,
        now: float,
        action: str,
        members: list[GroupMember],
        resources: tuple[str, ...] = (),
        params: dict[str, Any] | None = None,
        valid_until: float | None = None,
        requester_id: str,
        wait_reason: str = "joint-scheduled",
        cause: dict[str, Any] | None = None,
        supersedes: str | None = None,
        private_notes: dict[str, dict[str, Any]] | None = None,
        publish: PublishFn | None = None,
    ) -> JointCommitmentGroup:
        actor_ids = [member.actor_id for member in members]
        if len(set(actor_ids)) != len(actor_ids):
            raise ValueError("joint group members must be distinct participants")
        if not members:
            raise ValueError("joint group needs at least one member")
        self._next_id += 1
        group_id = f"g{self._next_id}"
        deadline = valid_until if valid_until is not None else float("inf")
        group = JointCommitmentGroup(
            group_id=group_id,
            action=action,
            params=params or {},
            members=members,
            resources=tuple(resources),
            created_at=now,
            valid_until=deadline,
            requester_id=requester_id,
            wait_reason=wait_reason,
            cause=cause,
            proposal_id=group_id,
            supersedes=supersedes,
            private_notes=private_notes,
        )
        self._groups[group_id] = group
        for resource_id in resources:
            self.resources.provision(resource_id, group_id, now)
        sequence = self._emit(publish, "joint_group_requested", group, now)
        group.record(now, "requested", detail={"resources": list(resources)},
                     event_sequence=sequence)
        if supersedes is not None:
            old = self._groups.get(supersedes)
            if old is not None and old.state == GROUP_WAITING:
                self.resources.release_for_group(old.group_id, now)
                old.state = GROUP_SUPERSEDED
                old.end_reason = f"superseded_by:{group_id}"
                old.record(now, "superseded", rule=RULE_SUPERSEDED_PRESERVED,
                           detail={"replacement": group_id})
            elif old is not None:
                # A withdrawn/expired original is retained as-is; only link it.
                old.record(now, "replacement_proposed",
                           rule=RULE_SUPERSEDED_PRESERVED,
                           detail={"replacement": group_id})
        return group

    def mark_ready(
        self,
        group_id: str,
        actor_id: str,
        *,
        now: float,
        params: dict[str, Any] | None = None,
        publish: PublishFn | None = None,
    ) -> bool:
        group = self.get(group_id)
        if group.state != GROUP_WAITING:
            return False
        member = group.member(actor_id)
        if member.ready:
            return True
        if member.valid_until is not None and member.valid_until <= now:
            self._expire(group, now, publish,
                         end_reason=f"member_commitment_expired:{actor_id}")
            return False
        member.ready = True
        member.ready_at = now
        if params is not None:
            member.params = deepcopy(params)
        sequence = self._emit(
            publish, "joint_member_ready", group, now, extra={"actor_id": actor_id}
        )
        group.record(now, "member_ready", detail={"actor_id": actor_id},
                     event_sequence=sequence)
        return True

    def set_private_note(self, group_id: str, actor_id: str, key: str,
                         value: Any) -> None:
        group = self.get(group_id)
        notes = group.private_notes.setdefault(actor_id, {})
        notes[key] = deepcopy(value)

    def get_private_note(self, group_id: str, actor_id: str, key: str,
                         default: Any = None) -> Any:
        group = self.get(group_id)
        value = group.private_notes.get(actor_id, {}).get(key, default)
        return deepcopy(value)

    def withdraw(
        self,
        group_id: str,
        *,
        now: float,
        by: str | None = None,
        reason: str | None = None,
        publish: PublishFn | None = None,
    ) -> bool:
        group = self.get(group_id)
        if group.state == GROUP_WAITING:
            group.state = GROUP_WITHDRAWN
            group.end_reason = f"withdrawn_by:{by}" if by else "withdrawn"
            group.withdrawal_reason = reason
            self.resources.release_for_group(group_id, now)
            sequence = self._emit(
                publish, "joint_group_withdrawn", group, now,
                extra={"by": by, "reason": reason},
            )
            group.record(now, "withdrawn", rule=RULE_GROUP_WITHDRAWAL_FIRST,
                         detail={"by": by, "reason": reason},
                         event_sequence=sequence)
            return True
        group.record(now, "withdrawal_rejected",
                     rule=RULE_STARTED_GROUP_IMMUTABLE if group.state == GROUP_STARTED else None,
                     detail={"by": by, "state": group.state})
        return False

    def propose_replacement(
        self,
        old_group_id: str,
        *,
        now: float,
        action: str,
        members: list[GroupMember],
        resources: tuple[str, ...] = (),
        params: dict[str, Any] | None = None,
        valid_until: float | None = None,
        requester_id: str,
        reason: str | None = None,
        wait_reason: str = "joint-replacement",
        cause: dict[str, Any] | None = None,
        private_notes: dict[str, dict[str, Any]] | None = None,
        publish: PublishFn | None = None,
    ) -> JointCommitmentGroup:
        old = self.get(old_group_id)
        if old.state in (GROUP_STARTED, GROUP_COMPLETED):
            raise ValueError("a started joint decision cannot be replaced")
        merged_cause = dict(cause or {})
        merged_cause.update({"replaces_group": old_group_id, "replace_reason": reason})
        return self.request(
            now=now,
            action=action,
            params=params or old.params,
            members=members,
            resources=resources,
            valid_until=valid_until,
            requester_id=requester_id,
            wait_reason=wait_reason,
            cause=merged_cause,
            supersedes=old_group_id,
            private_notes=private_notes,
            publish=publish,
        )

    def complete(self, group_id: str, *, now: float,
                 publish: PublishFn | None = None) -> bool:
        group = self.get(group_id)
        if group.state != GROUP_STARTED:
            return False
        group.state = GROUP_COMPLETED
        group.end_reason = "completed"
        self.resources.release_for_group(group_id, now)
        sequence = self._emit(publish, "joint_group_completed", group, now)
        group.record(now, "completed", event_sequence=sequence)
        return True

    def settle(self, now: float, *, publish: PublishFn | None = None) -> list[JointCommitmentGroup]:
        """Expire, resolve resource contention, then jointly activate.

        At one moment: validity expiry and participant departure are checked
        before activation; among ready groups contending for a resource the
        earliest request wins deterministically and every loser releases its
        provisional reservation, leaving nothing occupied.
        """
        # 1. Expiry first.
        for group in self.waiting():
            expired_member = next(
                (m for m in group.members
                 if not m.ready
                 and m.valid_until is not None and m.valid_until <= now),
                None,
            )
            if group.valid_until <= now:
                self._expire(group, now, publish, end_reason="expired")
            elif expired_member is not None:
                self._expire(
                    group, now, publish,
                    end_reason=f"member_commitment_expired:{expired_member.actor_id}",
                )
        # 2. Resource contention among fully-ready groups: earliest request wins.
        ready_groups = [g for g in self.waiting() if not g.waiting_on()]
        winners: set[str] = set()
        losers: set[str] = set()
        claimants: dict[str, str] = {}
        for group in sorted(ready_groups, key=lambda g: (g.created_at, g.group_id)):
            conflict = next(
                (resource_id for resource_id in group.resources
                 if resource_id in claimants),
                None,
            )
            if conflict is None:
                winners.add(group.group_id)
                for resource_id in group.resources:
                    claimants[resource_id] = group.group_id
            else:
                losers.add(group.group_id)
                group.record(now, "resource_lost", rule=RULE_RESOURCE_ORDER,
                             detail={"resource_id": conflict,
                                     "winner": claimants[conflict]})
                released = self.resources.release_for_group(group.group_id, now)
                group.record(now, "resources_released", rule=RULE_LOSER_RELEASES,
                             detail={"resources": released})
        # 3. Activate winners only once.
        fired: list[JointCommitmentGroup] = []
        for group in ready_groups:
            if group.group_id not in winners or group.state != GROUP_WAITING:
                continue
            group.state = GROUP_STARTED
            group.started_at = now
            group.decision_id = f"decision-{group.group_id}"
            for resource_id in group.resources:
                self.resources.confirm(resource_id, group.group_id, now)
            sequence = self._emit(publish, "joint_group_started", group, now)
            group.decision_event_sequence = sequence
            group.record(now, "started", rule=RULE_ALL_READY_BEFORE_START,
                         detail={"participants": list(group.participants)},
                         event_sequence=sequence)
            fired.append(group)
        return fired

    def end_for_actor(
        self,
        actor_id: str,
        *,
        now: float,
        publish: PublishFn | None = None,
    ) -> list[JointCommitmentGroup]:
        ended: list[JointCommitmentGroup] = []
        for group in tuple(self._groups.values()):
            if group.state in (GROUP_WAITING, GROUP_STARTED) and actor_id in group.participants:
                group.state = GROUP_ABANDONED
                group.end_reason = f"participant_lost:{actor_id}"
                self.resources.release_for_group(group.group_id, now)
                sequence = self._emit(
                    publish, "joint_group_ended", group, now,
                    extra={"lost_participant": actor_id},
                )
                group.record(now, "ended", rule=RULE_GROUP_REMOVAL_FIRST,
                             detail={"lost_participant": actor_id},
                             event_sequence=sequence)
                ended.append(group)
        return ended

    def _expire(self, group: JointCommitmentGroup, now: float,
                publish: PublishFn | None, *, end_reason: str) -> None:
        group.state = GROUP_EXPIRED
        group.end_reason = end_reason
        self.resources.release_for_group(group.group_id, now)
        sequence = self._emit(publish, "joint_group_expired", group, now,
                              extra={"end_reason": end_reason})
        group.record(now, "expired", rule=RULE_GROUP_EXPIRY_BEFORE_START,
                     detail={"end_reason": end_reason}, event_sequence=sequence)

    def _emit(self, publish: PublishFn | None, event_type: str,
              group: JointCommitmentGroup, now: float,
              *, extra: dict[str, Any] | None = None) -> int | None:
        if publish is None:
            return None
        payload = {
            "group_id": group.group_id,
            "proposal_id": group.proposal_id,
            "action": group.action,
            "participants": list(group.participants),
            "waiting_on": list(group.waiting_on()),
            "resources": list(group.resources),
            "supersedes": group.supersedes,
            "decision_id": group.decision_id,
        }
        if extra:
            payload.update(extra)
        # Joint decisions are broadcasts: every participant must observe them.
        return publish(event_type, payload, None)

    def snapshot(self) -> dict[str, Any]:
        return {
            "next_id": self._next_id,
            "groups": [group.to_dict() for group in self._groups.values()],
            "resources": self.resources.snapshot(),
        }

    def restore(self, data: dict[str, Any] | None) -> None:
        self._groups = {}
        self._next_id = 0
        self.resources = ResourceRegistry()
        if not data:
            return
        self._next_id = int(data.get("next_id", 0))
        for entry in data.get("groups", ()):
            group = JointCommitmentGroup.from_dict(entry)
            self._groups[group.group_id] = group
        self.resources.restore(data.get("resources"))
