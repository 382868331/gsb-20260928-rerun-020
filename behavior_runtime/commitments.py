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
RULE_JOINT_ALL_READY = "joint-activation-requires-all-members"
RULE_MEMBER_EXPIRY_ENDS_GROUP = "member-expiry-ends-joint-group"
RULE_RESOURCE_PRIORITY = "earliest-request-wins-resource"

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
    group_id: str | None = None
    role: str | None = None
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
            "group_id": self.group_id,
            "role": self.role,
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
            group_id=values.get("group_id"),
            role=values.get("role"),
            history=[dict(entry) for entry in values.get("history", ())],
        )


@dataclass(slots=True)
class JointCommitment:
    """A group of member commitments that can only take effect together.

    The joint action starts only when every member commitment is still
    waiting and due, and every shared resource can be acquired atomically.
    A started group records a decision id so follow-up commitments can
    point back to the exact joint decision they depend on.
    """

    group_id: str
    requester_id: str
    action: str
    params: dict[str, Any]
    members: list[dict[str, Any]]
    resources: tuple[str, ...]
    created_at: float
    wait_reason: str
    cause: dict[str, Any]
    state: str = WAITING
    decision_id: str | None = None
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
            "group_id": self.group_id,
            "requester_id": self.requester_id,
            "action": self.action,
            "params": deepcopy(self.params),
            "members": deepcopy(self.members),
            "resources": list(self.resources),
            "created_at": self.created_at,
            "wait_reason": self.wait_reason,
            "cause": deepcopy(self.cause),
            "state": self.state,
            "decision_id": self.decision_id,
            "end_reason": self.end_reason,
            "started_at": self.started_at,
            "history": deepcopy(self.history),
        }

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> "JointCommitment":
        return cls(
            group_id=values["group_id"],
            requester_id=values["requester_id"],
            action=values["action"],
            params=dict(values.get("params") or {}),
            members=[dict(member) for member in values.get("members", ())],
            resources=tuple(values.get("resources", ())),
            created_at=float(values["created_at"]),
            wait_reason=values.get("wait_reason", "scheduled"),
            cause=dict(values.get("cause") or {}),
            state=values.get("state", WAITING),
            decision_id=values.get("decision_id"),
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
        self._groups: dict[str, JointCommitment] = {}
        self._next_group_id = 0
        self._next_decision_id = 0
        self._resource_holds: dict[str, str] = {}

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

    def get_group(self, group_id: str) -> JointCommitment:
        try:
            return self._groups[group_id]
        except KeyError:
            raise KeyError(f"unknown joint commitment group: {group_id}") from None

    def groups(self) -> tuple[JointCommitment, ...]:
        return tuple(self._groups.values())

    def resource_holds(self) -> dict[str, str]:
        return dict(self._resource_holds)

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
        group_id: str | None = None,
        role: str | None = None,
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
            group_id=group_id,
            role=role,
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
        if commitment.group_id is not None and commitment.state == WAITING:
            return (
                self.withdraw_joint(
                    commitment.group_id, now=now, by=by, publish=publish
                )
                is not None
            )
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
        self._release_orphaned_holds(now, publish=publish)
        for commitment in tuple(self._commitments.values()):
            if commitment.state != WAITING:
                continue
            if commitment.expires_at is not None and commitment.expires_at <= now:
                commitment.state = EXPIRED
                commitment.end_reason = "expired"
                sequence = self._emit(publish, "commitment_expired", commitment, now)
                commitment.record(now, "expired", rule=RULE_EXPIRY_BEFORE_START,
                                  event_sequence=sequence)
        self._settle_group_expiries(now, publish=publish)
        for commitment in tuple(self._commitments.values()):
            if commitment.state != WAITING:
                continue
            if commitment.group_id is not None:
                continue
            if commitment.due_at <= now:
                commitment.state = STARTED
                commitment.started_at = now
                sequence = self._emit(publish, "commitment_started", commitment, now)
                commitment.record(now, "started", event_sequence=sequence)
                fired.append(commitment)
        self._settle_group_activation(now, publish=publish)
        return fired

    def request_joint(
        self,
        *,
        now: float,
        requester_id: str,
        action: str,
        participants: list[dict[str, Any]],
        params: dict[str, Any] | None = None,
        resources: tuple[str, ...] = (),
        wait_reason: str = "scheduled",
        cause: dict[str, Any] | None = None,
        publish: PublishFn | None = None,
    ) -> JointCommitment:
        """Register a group of commitments that must take effect together."""
        if not participants:
            raise ValueError("a joint commitment needs at least one participant")
        self._next_group_id += 1
        group_id = f"g{self._next_group_id}"
        members: list[dict[str, Any]] = []
        for participant in participants:
            role = participant.get("role") or participant["performer_id"]
            commitment = self.request(
                now=now,
                requester_id=requester_id,
                performer_id=participant["performer_id"],
                action=action,
                params=participant.get("params", params),
                due_at=now + float(participant.get("delay", 0.0)),
                expires_at=(
                    None
                    if participant.get("valid_for") is None
                    else now + float(participant["valid_for"])
                ),
                wait_reason=wait_reason,
                cause=cause,
                publish=publish,
                group_id=group_id,
                role=role,
            )
            members.append(
                {
                    "role": role,
                    "participant_id": participant["performer_id"],
                    "commitment_id": commitment.commitment_id,
                }
            )
        group = JointCommitment(
            group_id=group_id,
            requester_id=requester_id,
            action=action,
            params=dict(params or {}),
            members=members,
            resources=tuple(resources),
            created_at=now,
            wait_reason=wait_reason,
            cause=dict(cause or {}),
        )
        self._groups[group_id] = group
        sequence = self._emit_joint(publish, "joint_commitment_requested", group, now)
        group.record(now, "requested", detail={"wait_reason": wait_reason},
                     event_sequence=sequence)
        return group

    def withdraw_joint(
        self,
        group_id: str,
        *,
        now: float,
        by: str | None = None,
        reason: str | None = None,
        replacement: dict[str, str] | None = None,
        publish: PublishFn | None = None,
    ) -> JointCommitment | None:
        """Withdraw a waiting group, optionally proposing a replacement plan.

        The original group and its member commitments stay recorded with the
        withdrawal reason; a replacement creates a new group whose cause
        points back to the withdrawn one.
        """
        group = self.get_group(group_id)
        if group.state != WAITING:
            group.record(
                now,
                "withdrawal_rejected",
                rule=RULE_STARTED_IMMUTABLE if group.state == STARTED else None,
                detail={"by": by, "state": group.state},
            )
            return None
        detail = {"by": by, "reason": reason}
        group.state = WITHDRAWN
        group.end_reason = f"withdrawn_by:{by}" if by else "withdrawn"
        if reason:
            group.end_reason += f":{reason}"
        sequence = self._emit_joint(publish, "joint_commitment_withdrawn", group,
                                    now, extra=detail)
        group.record(now, "withdrawn", rule=RULE_WITHDRAWAL_FIRST,
                     detail=detail, event_sequence=sequence)
        for member in group.members:
            commitment = self._commitments[member["commitment_id"]]
            if commitment.state == WAITING:
                commitment.state = WITHDRAWN
                commitment.end_reason = f"group_withdrawn:{group_id}"
                commitment.record(now, "withdrawn", rule=RULE_WITHDRAWAL_FIRST,
                                  detail={"by": by, "group_id": group_id},
                                  event_sequence=sequence)
        self._release_group_holds(group, now)
        if not replacement:
            return group
        participants = []
        for member in group.members:
            commitment = self._commitments[member["commitment_id"]]
            if member["role"] == replacement.get("role"):
                participants.append(
                    {"role": member["role"],
                     "performer_id": replacement["performer_id"]}
                )
            else:
                participants.append(
                    {
                        "role": member["role"],
                        "performer_id": member["participant_id"],
                        "delay": max(0.0, commitment.due_at - now),
                        "valid_for": (
                            None
                            if commitment.expires_at is None
                            else max(0.0, commitment.expires_at - now)
                        ),
                    }
                )
        new_group = self.request_joint(
            now=now,
            requester_id=group.requester_id,
            action=group.action,
            participants=participants,
            params=deepcopy(group.params),
            resources=group.resources,
            wait_reason=group.wait_reason,
            cause={
                "supersedes": group_id,
                "replacement": dict(replacement),
                "reason": reason,
            },
            publish=publish,
        )
        new_group.record(now, "replacement_proposed",
                         detail={"supersedes": group_id, **replacement})
        group.record(now, "replaced_by", detail={"group_id": new_group.group_id})
        return new_group

    def complete_joint(self, group_id: str, *, now: float,
                       publish: PublishFn | None = None) -> bool:
        group = self.get_group(group_id)
        if group.state != STARTED:
            return False
        group.state = COMPLETED
        group.end_reason = "completed"
        sequence = self._emit_joint(publish, "joint_commitment_completed", group, now)
        group.record(now, "completed", event_sequence=sequence)
        for member in group.members:
            self.complete(member["commitment_id"], now=now)
        self._release_group_holds(group, now)
        return True

    def joint_status(self, group_id: str, *, now: float) -> dict[str, Any]:
        """Explain who is still awaited and what is already satisfied."""
        group = self.get_group(group_id)
        members = []
        waiting_on = []
        satisfied = []
        for member in group.members:
            commitment = self._commitments[member["commitment_id"]]
            ready = commitment.state == WAITING and commitment.due_at <= now
            entry = {
                "role": member["role"],
                "participant_id": member["participant_id"],
                "commitment_id": commitment.commitment_id,
                "state": commitment.state,
                "due_at": commitment.due_at,
                "expires_at": commitment.expires_at,
                "ready": ready,
            }
            members.append(entry)
            if ready or commitment.state == STARTED:
                satisfied.append(member["role"])
            elif commitment.state == WAITING:
                waiting_on.append(member["role"])
        return {
            "group_id": group.group_id,
            "state": group.state,
            "decision_id": group.decision_id,
            "action": group.action,
            "members": members,
            "waiting_on": waiting_on,
            "satisfied": satisfied,
            "resources": list(group.resources),
            "resource_holds": {
                resource: holder
                for resource, holder in self._resource_holds.items()
                if resource in group.resources
            },
        }

    def end_for_actor(
        self,
        actor_id: str,
        *,
        now: float,
        publish: PublishFn | None = None,
    ) -> list[Commitment]:
        """End every live commitment that lost a participant."""
        ended: list[Commitment] = []
        for group in tuple(self._groups.values()):
            if group.state not in (WAITING, STARTED):
                continue
            if actor_id not in [m["participant_id"] for m in group.members] and \
                    actor_id != group.requester_id:
                continue
            group.state = ENDED
            group.end_reason = f"participant_lost:{actor_id}"
            sequence = self._emit_joint(publish, "joint_commitment_ended", group,
                                        now, extra={"lost_participant": actor_id})
            group.record(now, "ended", rule=RULE_REMOVAL_FIRST,
                         detail={"lost_participant": actor_id},
                         event_sequence=sequence)
            self._release_group_holds(group, now)
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

    def _emit_joint(
        self,
        publish: PublishFn | None,
        event_type: str,
        group: JointCommitment,
        now: float,
        *,
        extra: dict[str, Any] | None = None,
    ) -> int | None:
        if publish is None:
            return None
        payload = {
            "group_id": group.group_id,
            "requester_id": group.requester_id,
            "action": group.action,
            "decision_id": group.decision_id,
            "members": deepcopy(group.members),
            "resources": list(group.resources),
            "cause": deepcopy(group.cause),
        }
        if extra:
            payload.update(extra)
        return publish(event_type, payload, None)

    def _settle_group_expiries(
        self, now: float, *, publish: PublishFn | None = None
    ) -> None:
        """End waiting groups whose members can no longer all take effect."""
        for group in tuple(self._groups.values()):
            if group.state != WAITING:
                continue
            broken = [
                self._commitments[m["commitment_id"]]
                for m in group.members
                if self._commitments[m["commitment_id"]].state != WAITING
            ]
            if not broken:
                continue
            group.state = EXPIRED
            group.end_reason = f"member_unavailable:{broken[0].commitment_id}"
            sequence = self._emit_joint(publish, "joint_commitment_expired", group,
                                        now, extra={
                                            "member_commitment_id":
                                                broken[0].commitment_id,
                                        })
            group.record(now, "expired", rule=RULE_MEMBER_EXPIRY_ENDS_GROUP,
                         detail={"member_commitment_id": broken[0].commitment_id},
                         event_sequence=sequence)
            for member in group.members:
                commitment = self._commitments[member["commitment_id"]]
                if commitment.state == WAITING:
                    commitment.state = ENDED
                    commitment.end_reason = f"group_expired:{group.group_id}"
                    commitment.record(now, "ended",
                                      rule=RULE_MEMBER_EXPIRY_ENDS_GROUP,
                                      detail={"group_id": group.group_id},
                                      event_sequence=sequence)
            self._release_group_holds(group, now)

    def _settle_group_activation(
        self, now: float, *, publish: PublishFn | None = None
    ) -> None:
        """Activate ready groups in a fixed order, atomically on resources."""
        waiting = sorted(
            (g for g in self._groups.values() if g.state == WAITING),
            key=lambda g: (g.created_at, g.group_id),
        )
        for group in waiting:
            members = [self._commitments[m["commitment_id"]] for m in group.members]
            if not all(c.state == WAITING and c.due_at <= now for c in members):
                continue
            blockers = {
                resource: self._resource_holds[resource]
                for resource in group.resources
                if resource in self._resource_holds
            }
            if blockers:
                sequence = self._emit_joint(
                    publish, "joint_commitment_deferred", group, now,
                    extra={"blockers": blockers},
                )
                group.record(now, "activation_deferred",
                             rule=RULE_RESOURCE_PRIORITY,
                             detail={"blockers": blockers},
                             event_sequence=sequence)
                continue
            for resource in group.resources:
                self._resource_holds[resource] = group.group_id
            self._next_decision_id += 1
            group.decision_id = f"d{self._next_decision_id}"
            group.state = STARTED
            group.started_at = now
            sequence = self._emit_joint(publish, "joint_commitment_started",
                                        group, now)
            group.record(now, "started", rule=RULE_JOINT_ALL_READY,
                         detail={"decision_id": group.decision_id},
                         event_sequence=sequence)
            for commitment in members:
                commitment.state = STARTED
                commitment.started_at = now
                commitment.record(now, "started", rule=RULE_JOINT_ALL_READY,
                                  detail={"group_id": group.group_id,
                                          "decision_id": group.decision_id},
                                  event_sequence=sequence)

    def _release_group_holds(self, group: JointCommitment, now: float) -> None:
        for resource in list(self._resource_holds):
            if self._resource_holds[resource] == group.group_id:
                del self._resource_holds[resource]
                group.record(now, "resource_released",
                             detail={"resource": resource})

    def _release_orphaned_holds(
        self, now: float, *, publish: PublishFn | None = None
    ) -> None:
        """No terminal group may keep a resource hold across a settlement."""
        for resource, group_id in list(self._resource_holds.items()):
            group = self._groups.get(group_id)
            if group is None or group.state != STARTED:
                del self._resource_holds[resource]
                if group is not None:
                    group.record(now, "resource_released",
                                 detail={"resource": resource,
                                         "reason": "orphaned_hold"})

    def snapshot(self) -> dict[str, Any]:
        return {
            "next_id": self._next_id,
            "commitments": [
                commitment.to_dict() for commitment in self._commitments.values()
            ],
            "next_group_id": self._next_group_id,
            "next_decision_id": self._next_decision_id,
            "groups": [group.to_dict() for group in self._groups.values()],
            "resource_holds": dict(self._resource_holds),
        }

    def restore(self, data: dict[str, Any] | None) -> None:
        self._commitments = {}
        self._next_id = 0
        self._groups = {}
        self._next_group_id = 0
        self._next_decision_id = 0
        self._resource_holds = {}
        if not data:
            return
        self._next_id = int(data.get("next_id", 0))
        for entry in data.get("commitments", ()):
            commitment = Commitment.from_dict(entry)
            self._commitments[commitment.commitment_id] = commitment
        self._next_group_id = int(data.get("next_group_id", 0))
        self._next_decision_id = int(data.get("next_decision_id", 0))
        for entry in data.get("groups", ()):
            group = JointCommitment.from_dict(entry)
            self._groups[group.group_id] = group
        self._resource_holds = {
            str(resource): str(group_id)
            for resource, group_id in data.get("resource_holds", {}).items()
        }
