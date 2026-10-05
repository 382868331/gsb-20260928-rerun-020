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
CANCELLED = "cancelled"

TERMINAL_STATES = frozenset({COMPLETED, WITHDRAWN, EXPIRED, ENDED, CANCELLED})

# Same-moment precedence rules, recorded on every decision so the final
# outcome of a conflict can be audited later.
RULE_WITHDRAWAL_FIRST = "explicit-withdrawal-precedes-settlement"
RULE_REMOVAL_FIRST = "participant-removal-precedes-settlement"
RULE_EXPIRY_BEFORE_START = "expiry-precedes-activation"
RULE_STARTED_IMMUTABLE = "started-commitments-cannot-be-withdrawn"
RULE_JOINT_ALL_READY = "joint-activation-requires-all-members"
RULE_MEMBER_EXPIRY_ENDS_GROUP = "member-expiry-ends-joint-group"
RULE_RESOURCE_PRIORITY = "earliest-request-wins-resource"
RULE_PHASE_SEQUENCE = "phases-start-in-order"
RULE_FINISHED_WORK_IMMUTABLE = "finished-phases-cannot-be-cancelled"
RULE_RESCHEDULE_RECHECK = "reschedule-rechecks-holds-before-adoption"
RULE_WAIT_OVER_FALSE_END = "unresolved-handover-keeps-waiting"
RULE_PARTIAL_CANCEL_ONLY = "partial-cancel-revokes-only-unstarted-promises"

# Default phase ordering for a phased joint task.
PHASE_PREPARE = "prepare"
PHASE_EXECUTE = "execute"
PHASE_HANDOVER = "handover"
DEFAULT_PHASES = (PHASE_PREPARE, PHASE_EXECUTE, PHASE_HANDOVER)

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
    phase_id: str | None = None
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
            "phase_id": self.phase_id,
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
                None if values.get("expires_at") is None
                else float(values["expires_at"])
            ),
            wait_reason=values.get("wait_reason", "scheduled"),
            cause=dict(values.get("cause") or {}),
            state=values.get("state", WAITING),
            end_reason=values.get("end_reason"),
            started_at=values.get("started_at"),
            group_id=values.get("group_id"),
            role=values.get("role"),
            phase_id=values.get("phase_id"),
            history=[dict(entry) for entry in values.get("history", ())],
        )


class CommitmentLedger:
    """Deterministic registry of commitments for one world.

    Settlement order at any moment is fixed: explicit withdrawals and
    participant removals apply immediately when requested (before the next
    settlement), then settlement expires waiting commitments, and only then
    advances due groups and activates due commitments. Every transition
    records the rule that decided it.
    """

    def __init__(self) -> None:
        self._commitments: dict[str, Commitment] = {}
        self._next_id = 0
        self._groups: dict[str, JointCommitment] = {}
        self._next_group_id = 0
        self._next_decision_id = 0
        # resource -> (group_id, phase_id): a hold belongs to exactly one
        # active phase and disappears when that phase is finished.
        self._resource_holds: dict[str, tuple[str, str]] = {}

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
        return {
            resource: holder[0] for resource, holder in self._resource_holds.items()
        }

    def resource_hold_details(self) -> dict[str, dict[str, str]]:
        return {
            resource: {"group_id": holder[0], "phase_id": holder[1]}
            for resource, holder in self._resource_holds.items()
        }

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
        phase_id: str | None = None,
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
            phase_id=phase_id,
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
        """Expire, then advance groups, then activate. Never fires twice."""
        fired: list[Commitment] = []
        self._release_orphaned_holds(now)
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
        self._advance_groups(now, publish=publish)
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
        phases: tuple[str, ...] = (PHASE_EXECUTE,),
        phase_resources: dict[str, tuple[str, ...]] | None = None,
    ) -> JointCommitment:
        """Register a group whose members and resources align per phase.

        Every member contributes one readiness commitment per phase, each
        with its own delay/valid_for. A member ready for an early phase does
        not reserve anything for later phases: those commitments stay
        waiting until the preceding phase is finished.
        """
        if not participants:
            raise ValueError("a joint commitment needs at least one participant")
        if not phases:
            raise ValueError("a phased joint commitment needs at least one phase")
        seen: set[str] = set()
        for phase_id in phases:
            if phase_id in seen:
                raise ValueError(f"duplicate phase: {phase_id}")
            seen.add(phase_id)
        self._next_group_id += 1
        group_id = f"g{self._next_group_id}"

        legacy = phases == (PHASE_EXECUTE,)
        resolved_resources = dict(phase_resources or {})
        if legacy:
            resolved_resources.setdefault(PHASE_EXECUTE, tuple(resources))
        unknown = set(resolved_resources).difference(phases)
        if unknown:
            raise ValueError(f"resources given for unknown phases: {sorted(unknown)}")

        members: list[dict[str, Any]] = []
        roles: list[str] = []
        for participant in participants:
            role = participant.get("role") or participant["performer_id"]
            roles.append(role)
            members.append(
                {
                    "role": role,
                    "participant_id": participant["performer_id"],
                    "capabilities": tuple(participant.get("capabilities") or ()),
                    "commitment_id": None,
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
            phases=tuple(phases),
            phase_resources={
                phase_id: tuple(resolved_resources.get(phase_id, ()))
                for phase_id in phases
            },
        )
        self._groups[group_id] = group

        phase_commitment_ids: dict[str, list[str]] = {}
        for phase_index, phase_id in enumerate(phases):
            ids: list[str] = []
            for participant, role in zip(participants, roles):
                timing = self._participant_phase_timing(participant, phase_id,
                                                        phase_index, legacy)
                phase_cause = dict(cause or {})
                if not legacy:
                    phase_cause.update(
                        {"group_id": group_id, "phase_id": phase_id,
                          "phase_index": phase_index}
                    )
                commitment = self.request(
                    now=now,
                    requester_id=requester_id,
                    performer_id=participant["performer_id"],
                    action=action,
                    params=participant.get("params", params),
                    due_at=now + timing["delay"],
                    expires_at=(
                        None
                        if timing["valid_for"] is None
                        else now + timing["valid_for"]
                    ),
                    wait_reason=wait_reason,
                    cause=phase_cause,
                    publish=publish,
                    group_id=group_id,
                    role=role,
                    phase_id=None if legacy else phase_id,
                )
                ids.append(commitment.commitment_id)
            phase_commitment_ids[phase_id] = ids
        group.phase_commitments = phase_commitment_ids

        if legacy:
            for member, commitment_id in zip(
                group.members, phase_commitment_ids[PHASE_EXECUTE]
            ):
                member["commitment_id"] = commitment_id
        else:
            for phase_id in phases:
                group.phase_state[phase_id] = WAITING

        sequence = self._emit_joint(publish, "joint_commitment_requested", group, now)
        group.record(now, "requested",
                      detail={"wait_reason": wait_reason,
                              "phases": list(phases),
                              "phase_resources": {
                                  pid: list(res)
                                  for pid, res in group.phase_resources.items()
                              }},
                      event_sequence=sequence)
        return group

    @staticmethod
    def _participant_phase_timing(
        participant: dict[str, Any],
        phase_id: str,
        phase_index: int,
        legacy: bool,
    ) -> dict[str, Any]:
        phases_cfg = participant.get("phases")
        if isinstance(phases_cfg, dict):
            phase_cfg = phases_cfg.get(phase_id) or {}
        elif isinstance(phases_cfg, (list, tuple)) and phase_index < len(phases_cfg):
            phase_cfg = phases_cfg[phase_index] or {}
        elif legacy:
            phase_cfg = participant
        else:
            phase_cfg = {}
        return {
            "delay": float(phase_cfg.get("delay", 0.0)),
            "valid_for": phase_cfg.get("valid_for"),
        }

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
        """Withdraw a group whose first phase has not started yet.

        Finished phases are never undone: once a phased group has entered
        any phase, withdrawal is rejected. Use cancel_phase for a later phase
        that has not started.
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
        for commitment in self._group_member_commitments(group):
            if commitment.state == WAITING:
                commitment.state = WITHDRAWN
                commitment.end_reason = f"group_withdrawn:{group_id}"
                commitment.record(now, "withdrawn", rule=RULE_WITHDRAWAL_FIRST,
                                   detail={"by": by, "group_id": group_id},
                                   event_sequence=sequence)
        for phase_id in group.phases:
            if group.phase_state.get(phase_id) == WAITING:
                group.phase_state[phase_id] = WITHDRAWN
        self._release_group_holds(group, now)
        if not replacement:
            return group
        return self._propose_replacement(group, now, replacement, reason, publish)

    def cancel_phase(
        self,
        group_id: str,
        phase_id: str,
        *,
        now: float,
        by: str | None = None,
        reason: str | None = None,
        publish: PublishFn | None = None,
    ) -> JointCommitment | None:
        """Cancel a phase that has not started.

        Only the named phase and every phase after it are cancelled; phases
        that are completed or still active stay exactly as they were, and
        resources held by an active phase remain held until that phase is
        finished. An active phase always runs to its safe finish via
        finish_phase; the group ends there with the cancellation reason
        instead of "completed". This operation can never revoke already
        finished work, and it never strands a resource hold: every hold is
        released either here (no active phase) or by the finishing phase.
        """
        group = self.get_group(group_id)
        if phase_id not in group.phases:
            raise KeyError(f"unknown phase {phase_id!r} for group {group_id}")
        target_index = group.phase_index(phase_id)
        phase_state = group.phase_state.get(
            phase_id, WAITING if phase_id == group.phases[0] else WAITING
        )
        if phase_state != WAITING:
            group.record(
                now,
                "phase_cancel_rejected",
                rule=(
                    RULE_FINISHED_WORK_IMMUTABLE
                    if phase_state in (COMPLETED, STARTED)
                    else None
                ),
                detail={"phase_id": phase_id, "by": by, "phase_state": phase_state},
            )
            return None
        if group.state not in (WAITING, STARTED):
            group.record(now, "phase_cancel_rejected",
                          detail={"phase_id": phase_id, "state": group.state})
            return None

        cancelled: list[str] = []
        for later_id in group.phases[target_index:]:
            if group.phase_state.get(later_id, WAITING) == WAITING:
                group.phase_state[later_id] = CANCELLED
                cancelled.append(later_id)
        end_reason = f"phase_cancelled:{phase_id}"
        if reason:
            end_reason += f":{reason}"
        active = group.active_phase
        if active is None:
            # Nothing is running: the group ends right away.
            group.state = ENDED
            group.end_reason = end_reason
            # Earlier phases that never started are closed with the group,
            # so no waiting commitment is left behind that nobody can settle.
            earlier_open = [
                earlier_id
                for earlier_id in group.phases[:target_index]
                if group.phase_state.get(earlier_id, WAITING) == WAITING
            ]
            for earlier_id in earlier_open:
                group.phase_state[earlier_id] = CANCELLED
            cancelled = earlier_open + cancelled
        else:
            # The active phase continues to its safe finish; the pending
            # end reason is applied by finish_phase. The group stays
            # STARTED so its holds are never orphaned and finish_phase
            # remains the single way to release them.
            group.end_reason = end_reason
        detail = {"by": by, "reason": reason, "phase_id": phase_id,
                   "cancelled_phases": cancelled,
                   "preserved_phases": [
                       earlier_id
                       for earlier_id in group.phases[:target_index]
                       if group.phase_state.get(earlier_id) != CANCELLED
                   ],
                   "active_phase": active,
                   "active_phase_policy": (
                       "run-to-safe-finish" if active is not None else None
                   )}
        sequence = self._emit_joint(publish, "joint_phase_cancelled", group, now,
                                    extra=detail)
        group.record(now, "phase_cancelled", rule=RULE_FINISHED_WORK_IMMUTABLE,
                      detail=detail, event_sequence=sequence)
        for later_id in cancelled:
            for commitment_id in group.phase_commitments.get(later_id, ()):
                commitment = self._commitments[commitment_id]
                if commitment.state == WAITING:
                    commitment.state = CANCELLED
                    commitment.end_reason = (
                        f"phase_cancelled:{group_id}:{later_id}"
                    )
                    commitment.record(now, "cancelled",
                                       detail={"phase_id": later_id, "by": by},
                                       event_sequence=sequence)
        if group.active_phase is None:
            self._release_group_holds(group, now)
        return group

    def offer_phase_reschedule(
        self,
        group_id: str,
        phase_id: str,
        *,
        now: float,
        by: str | None = None,
        reason: str | None = None,
        member_options: list[dict[str, Any]] | None = None,
        resource_options: list[Any] | None = None,
        delay: float = 0.0,
        publish: PublishFn | None = None,
    ) -> JointCommitment | None:
        """Opt into a delayed continuation of a phase that has not started.

        Used when a later phase (typically the handover) was cancelled or a
        member became unavailable while an earlier phase is still running.
        Only the named phase is re-authorized: completed and running phases
        are never touched. Candidate members bring their own readiness
        commitments and candidate resource bundles are only placeholders;
        actual resource occupation is re-checked against the live hold table
        at adoption time, so a bundle held by another task can never be
        borrowed early. If no arrangement resolves, the group simply keeps
        waiting instead of ending with a fake completion.
        """
        group = self.get_group(group_id)
        if not group.phased or phase_id not in group.phases:
            raise KeyError(f"unknown phase {phase_id!r} for group {group_id}")
        if group.state not in (WAITING, STARTED) or group.terminal:
            group.record(now, "reschedule_rejected",
                          detail={"phase_id": phase_id, "state": group.state})
            return None
        if group.phase_state.get(phase_id) in (STARTED, COMPLETED):
            group.record(now, "reschedule_rejected",
                          rule=RULE_FINISHED_WORK_IMMUTABLE,
                          detail={"phase_id": phase_id,
                                  "phase_state": group.phase_state[phase_id]})
            return None

        option_by_role: dict[str, list[dict[str, Any]]] = {}
        for option in member_options or ():
            role = option["role"]
            if role not in [member["role"] for member in group.members]:
                raise ValueError(f"unknown role {role!r} in reschedule offer")
            requirement = self._role_requirements(group, role)
            capabilities = tuple(option.get("capabilities") or ())
            if not set(requirement).issubset(set(capabilities)):
                group.record(now, "reschedule_rejected",
                              detail={"phase_id": phase_id, "role": role,
                                      "candidate": option["performer_id"],
                                      "missing_capabilities": sorted(
                                          set(requirement).difference(capabilities)
                                      )})
                return None
            readiness = option.get("delay", 0.0)
            commitment = self.request(
                now=now,
                requester_id=group.requester_id,
                performer_id=option["performer_id"],
                action=group.action,
                params=option.get("params", group.params),
                due_at=now + float(readiness),
                expires_at=(
                    None if option.get("valid_for") is None
                    else now + float(option["valid_for"])
                ),
                wait_reason=f"reschedule:{phase_id}",
                cause={"group_id": group_id, "phase_id": phase_id,
                       "reschedules": True, "role": role},
                group_id=group_id,
                role=role,
                phase_id=phase_id,
            )
            option_by_role.setdefault(role, []).append(
                {"participant_id": option["performer_id"],
                 "commitment_id": commitment.commitment_id,
                 "capabilities": list(capabilities)}
            )

        was_cancelled = group.phase_state.get(phase_id) == CANCELLED
        revived_roles: list[str] = []
        for index, member in enumerate(group.members):
            role = member["role"]
            original_id = group.phase_commitments[phase_id][index]
            original = self._commitments[original_id]
            if original.state in (WAITING, COMPLETED, STARTED):
                continue
            candidates = option_by_role.get(role)
            role_missing = role in group.missing_roles.get(phase_id, ())
            if not candidates and (was_cancelled or not role_missing):
                # Re-create the original member's promise: the user opted
                # into waiting for the same member after the delay.
                commitment = self.request(
                    now=now,
                    requester_id=group.requester_id,
                    performer_id=member["participant_id"],
                    action=group.action,
                    params=dict(group.params),
                    due_at=now + float(delay),
                    expires_at=None,
                    wait_reason=f"reschedule:{phase_id}",
                    cause={"group_id": group_id, "phase_id": phase_id,
                           "reschedules": True, "role": role},
                    group_id=group_id,
                    role=role,
                    phase_id=phase_id,
                )
                candidates = [
                    {"participant_id": member["participant_id"],
                     "commitment_id": commitment.commitment_id,
                     "capabilities": list(member.get("capabilities") or ())}
                ]
                option_by_role[role] = candidates
            revived_roles.append(role)

        bundles: list[dict[str, Any]] = [
            {"resources": list(group.phase_resources.get(phase_id, ())),
             "delay": 0.0}
        ]
        for option in resource_options or ():
            if isinstance(option, dict):
                bundles.append({"resources": list(option["resources"]),
                                "delay": float(option.get("delay", 0.0))})
            else:
                bundles.append({"resources": list(option), "delay": 0.0})

        offer = {
            "phase_id": phase_id,
            "by": by,
            "reason": reason,
            "created_at": now,
            "not_before": now + float(delay),
            "member_options": option_by_role,
            "resource_options": bundles,
            "adopted": None,
        }
        group.reschedule_offers[phase_id] = offer
        for role in revived_roles:
            roles = set(group.missing_roles.get(phase_id, ()))
            roles.discard(role)
            if roles:
                group.missing_roles[phase_id] = sorted(roles)
            else:
                group.missing_roles.pop(phase_id, None)
        if was_cancelled:
            group.phase_state[phase_id] = WAITING
            # A pending cancellation end reason no longer applies once the
            # later phase is explicitly re-authorized.
            group.end_reason = None
        detail = {"by": by, "reason": reason, "phase_id": phase_id,
                  "delay": float(delay),
                  "member_options": {role: [c["participant_id"]
                                             for c in candidates]
                                     for role, candidates
                                     in option_by_role.items()},
                  "resource_options": [bundle["resources"]
                                       for bundle in bundles],
                  "revived_cancelled_phase": was_cancelled,
                  "revived_roles": revived_roles}
        sequence = self._emit_joint(publish, "joint_phase_reschedule_offered",
                                    group, now, extra=detail)
        group.record(now, "reschedule_offered",
                      rule=RULE_PARTIAL_CANCEL_ONLY if was_cancelled else None,
                      detail=detail, event_sequence=sequence)
        return group

    def _role_requirements(self, group: JointCommitment, role: str) -> tuple[str, ...]:
        for member in group.members:
            if member["role"] == role:
                return tuple(member.get("capabilities") or ())
        return ()

    def _offer_can_resolve(
        self, group: JointCommitment, phase_id: str
    ) -> bool:
        """Whether every role still has a live promise under the offer.

        A role whose candidate promises all expired can never take effect;
        in that case the group must end honestly rather than wait forever.
        """
        offer = group.reschedule_offers.get(phase_id)
        if offer is None:
            return False
        for index, member in enumerate(group.members):
            role = member["role"]
            original = self._commitments[
                group.phase_commitments[phase_id][index]
            ]
            if original.state == WAITING:
                continue
            candidates = offer["member_options"].get(role, ())
            if not any(
                self._commitments[c["commitment_id"]].state == WAITING
                for c in candidates
            ):
                return False
        return True

    def _role_commitments(
        self, group: JointCommitment, phase_id: str
    ) -> list[tuple[dict[str, Any], Commitment]]:
        """Resolve one commitment per role for a phase, substitution-aware."""
        resolved: list[tuple[dict[str, Any], Commitment]] = []
        offer = group.reschedule_offers.get(phase_id)
        for index, member in enumerate(group.members):
            role = member["role"]
            original = self._commitments[
                group.phase_commitments[phase_id][index]
            ]
            chosen = original
            chosen_member = {
                "role": role,
                "participant_id": member["participant_id"],
                "commitment_id": original.commitment_id,
                "capabilities": list(member.get("capabilities") or ()),
                "replacement": False,
            }
            if original.state != WAITING and offer is not None:
                for candidate in offer["member_options"].get(role, ()):
                    commitment = self._commitments[candidate["commitment_id"]]
                    if commitment.state == WAITING:
                        chosen = commitment
                        chosen_member = {
                            "role": role,
                            "participant_id": candidate["participant_id"],
                            "commitment_id": commitment.commitment_id,
                            "capabilities": list(candidate.get("capabilities")
                                                  or ()),
                            "replacement": True,
                        }
                        break
            resolved.append((chosen_member, chosen))
        return resolved

    def finish_phase(
        self,
        group_id: str,
        phase_id: str | None = None,
        *,
        now: float,
        publish: PublishFn | None = None,
    ) -> JointCommitment | None:
        """Finish the active phase: record its work and release its resources.

        The next phase does not start here; it starts on a later settlement
        once all members are ready and its own resources are available.
        """
        group = self.get_group(group_id)
        active = phase_id or group.active_phase
        if group.state != STARTED or active is None:
            return None
        if group.phase_state.get(active) != STARTED:
            return None
        group.phase_state[active] = COMPLETED
        roster = group.phase_members.get(active)
        resolved = self._role_commitments(group, active)
        if roster:
            active_commitments = [
                self._commitments[entry["commitment_id"]] for entry in roster
            ]
        else:
            active_commitments = [commitment for _, commitment in resolved]
        for commitment in active_commitments:
            if commitment.state == STARTED:
                self.complete(commitment.commitment_id, now=now)
        members_ready: list[dict[str, Any]] = []
        for member, commitment in zip(roster or resolved, active_commitments):
            member_info = member if isinstance(member, dict) else member[0]
            note = {
                "role": member_info["role"],
                "participant_id": member_info["participant_id"],
                "phase_id": active,
                "commitment_id": commitment.commitment_id,
                "state": commitment.state,
                "replacement": bool(member_info.get("replacement")),
                "note": f"{member_info['role']} 已完成 {active}",
            }
            members_ready.append(note)
            commitment.record(now, "phase_finished",
                               detail={"phase_id": active})
        detail = {"phase_id": active, "members": members_ready}
        sequence = self._emit_joint(publish, "joint_phase_finished", group, now,
                                    extra=detail)
        group.record(now, "phase_finished", detail=detail,
                      event_sequence=sequence)
        self._release_phase_holds(group, active, now)
        group.active_phase = None
        next_phase = group.next_phase()
        if next_phase is None:
            if group.end_reason:
                # A later phase was cancelled while this one was running:
                # the group now ends with the cancellation reason.
                group.state = ENDED
                sequence = self._emit_joint(publish, "joint_commitment_ended",
                                            group, now,
                                            extra={"end_reason":
                                                   group.end_reason})
                group.record(now, "ended",
                              detail={"end_reason": group.end_reason},
                              event_sequence=sequence)
            else:
                group.state = COMPLETED
                group.end_reason = "completed"
                sequence = self._emit_joint(
                    publish, "joint_commitment_completed", group, now)
                group.record(now, "completed", event_sequence=sequence)
        else:
            # Safe-finish boundary: a later phase whose member is gone and
            # for which no delayed continuation was offered cannot run; end
            # there honestly rather than reporting a false completion. With
            # an offer the group keeps waiting for an adoptable plan.
            offer = group.reschedule_offers.get(next_phase)
            following = self._role_commitments(group, next_phase)
            unresolved = [
                (member, commitment)
                for member, commitment in following
                if commitment.state != WAITING
            ]
            offer_viable = offer is not None and self._offer_can_resolve(
                group, next_phase
            )
            if unresolved and not offer_viable:
                roles = [member["role"] for member, _ in unresolved]
                group.state = ENDED
                group.end_reason = (
                    f"member_unavailable:{next_phase}:{','.join(roles)}"
                )
                group.missing_roles[next_phase] = roles
                sequence = self._emit_joint(
                    publish, "joint_commitment_ended", group, now,
                    extra={"end_reason": group.end_reason,
                            "safe_finish_phase": active,
                            "unresolved_roles": roles},
                )
                group.record(now, "ended",
                              rule=RULE_WAIT_OVER_FALSE_END,
                              detail={"end_reason": group.end_reason,
                                      "safe_finish_phase": active},
                              event_sequence=sequence)
                self._release_group_holds(group, now)
        return group

    def complete_joint(self, group_id: str, *, now: float,
                        publish: PublishFn | None = None) -> bool:
        group = self.get_group(group_id)
        if group.state != STARTED:
            return False
        if group.phased:
            while group.state == STARTED:
                if group.active_phase is None:
                    self._advance_groups(now, publish=publish)
                if group.active_phase is None:
                    return group.state == COMPLETED
                self.finish_phase(group_id, now=now, publish=publish)
            return group.state == COMPLETED
        group.state = COMPLETED
        group.end_reason = "completed"
        sequence = self._emit_joint(publish, "joint_commitment_completed", group, now)
        group.record(now, "completed", event_sequence=sequence)
        for member in group.members:
            self.complete(member["commitment_id"], now=now)
        self._release_group_holds(group, now)
        return True

    def _propose_replacement(
        self,
        group: JointCommitment,
        now: float,
        replacement: dict[str, str],
        reason: str | None,
        publish: PublishFn | None,
    ) -> JointCommitment:
        participants = []
        first_phase = group.phases[0]
        for index, member in enumerate(group.members):
            if member["role"] == replacement.get("role"):
                participants.append(
                    {"role": member["role"],
                      "performer_id": replacement["performer_id"]}
                )
                continue
            phase_cfg: dict[str, dict[str, Any]] = {}
            for phase_id in group.phases:
                commitment = self._commitments[
                    group.phase_commitments[phase_id][index]
                ]
                phase_cfg[phase_id] = {
                    "delay": max(0.0, commitment.due_at - now),
                    "valid_for": (
                        None
                        if commitment.expires_at is None
                        else max(0.0, commitment.expires_at - now)
                    ),
                }
            participants.append(
                {
                    "role": member["role"],
                    "performer_id": member["participant_id"],
                    "phases": phase_cfg,
                }
            )
        new_group = self.request_joint(
            now=now,
            requester_id=group.requester_id,
            action=group.action,
            participants=participants,
            params=deepcopy(group.params),
            resources=group.resources if not group.phased else (),
            phases=group.phases,
            phase_resources=deepcopy(group.phase_resources),
            wait_reason=group.wait_reason,
            cause={
                "supersedes": group.group_id,
                "replacement": dict(replacement),
                "reason": reason,
            },
            publish=publish,
        )
        new_group.record(now, "replacement_proposed",
                          detail={"supersedes": group.group_id, **replacement})
        group.record(now, "replaced_by", detail={"group_id": new_group.group_id})
        return new_group

    def joint_status(self, group_id: str, *, now: float) -> dict[str, Any]:
        """Explain who is awaited, per phase, and what currently blocks it."""
        group = self.get_group(group_id)
        members = []
        waiting_on: list[str] = []
        satisfied: list[str] = []
        phases_report: list[dict[str, Any]] = []

        # The next scheduling decision for a phased group always concerns
        # the phase that has not started yet; an active phase is already
        # running and only finishes via an explicit call.
        target_phase = group.next_phase() if group.phased else (
            group.active_phase or group.next_phase()
        )
        if group.phased:
            for phase_id in group.phases:
                phase_info = self._phase_status(group, phase_id, now)
                phases_report.append(phase_info)
            if target_phase is not None:
                target_info = phases_report[group.phase_index(target_phase)]
                phase_waiting = list(target_info["waiting_on"])
                waiting_on = [entry["role"] for entry in phase_waiting]
                satisfied = list(target_info["satisfied"])
            else:
                phase_waiting = []
        else:
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

        requested = (
            group.phase_resources.get(target_phase, ())
            if group.phased and target_phase is not None
            else group.resources
        )
        resource_holds = {
            resource: holder[0]
            for resource, holder in self._resource_holds.items()
            if resource in requested and holder[0] != group.group_id
        }
        held_resources = {
            resource: holder[1]
            for resource, holder in self._resource_holds.items()
            if holder[0] == group.group_id
        }
        waiting_for = self._wait_description(
            group, target_phase, now,
            phase_waiting if group.phased else waiting_on,
            requested, resource_holds,
        )
        return {
            "group_id": group.group_id,
            "state": group.state,
            "decision_id": group.decision_id,
            "action": group.action,
            "active_phase": group.active_phase,
            "next_phase": group.next_phase(),
            "members": members,
            "phases": phases_report,
            "waiting_on": waiting_on,
            "satisfied": satisfied,
            "waiting_for": waiting_for,
            "resources": list(group.resources),
            "phase_resources": {
                phase_id: list(resources)
                for phase_id, resources in group.phase_resources.items()
            },
            "resource_holds": resource_holds,
            "held_resources": held_resources,
        }

    def _phase_status(
        self, group: JointCommitment, phase_id: str, now: float
    ) -> dict[str, Any]:
        waiting_on: list[dict[str, Any]] = []
        satisfied: list[str] = []
        member_entries: list[dict[str, Any]] = []
        phase_state = group.phase_state.get(phase_id, WAITING)
        if phase_id in group.phase_members:
            resolved = [
                (entry, self._commitments[entry["commitment_id"]])
                for entry in group.phase_members[phase_id]
            ]
        else:
            resolved = self._role_commitments(group, phase_id)
        for member, commitment in resolved:
            ready = commitment.state == WAITING and commitment.due_at <= now
            entry = {
                "role": member["role"],
                "participant_id": member["participant_id"],
                "commitment_id": commitment.commitment_id,
                "state": commitment.state,
                "due_at": commitment.due_at,
                "expires_at": commitment.expires_at,
                "ready": ready,
                "replacement": bool(member.get("replacement")),
                "note": commitment.wait_reason,
            }
            member_entries.append(entry)
            if phase_state == COMPLETED or ready or commitment.state == STARTED:
                satisfied.append(member["role"])
            if phase_state == WAITING and commitment.state == WAITING and not ready:
                waiting_on.append(
                    {"role": member["role"],
                      "participant_id": member["participant_id"],
                      "until": commitment.due_at,
                      "wait_reason": commitment.wait_reason}
                )
        requested = group.phase_resources.get(phase_id, ())
        blockers = {
            resource: {"group_id": holder[0], "phase_id": holder[1]}
            for resource, holder in self._resource_holds.items()
            if resource in requested and holder[0] != group.group_id
        }
        return {
            "phase_id": phase_id,
            "state": phase_state,
            "members": member_entries,
            "waiting_on": waiting_on,
            "satisfied": satisfied,
            "resources": list(requested),
            "resource_blockers": blockers,
            "reschedule": self._reschedule_status(group, phase_id, now),
            "missing_roles": list(group.missing_roles.get(phase_id, ())),
        }

    def _reschedule_status(
        self, group: JointCommitment, phase_id: str, now: float
    ) -> dict[str, Any] | None:
        offer = group.reschedule_offers.get(phase_id)
        if offer is None:
            return None
        member_choices: dict[str, list[dict[str, Any]]] = {}
        for role, candidates in offer["member_options"].items():
            choices = []
            for candidate in candidates:
                commitment = self._commitments[candidate["commitment_id"]]
                choices.append({
                    "participant_id": candidate["participant_id"],
                    "state": commitment.state,
                    "ready": commitment.state == WAITING
                    and commitment.due_at <= now,
                    "available_at": commitment.due_at,
                })
            member_choices[role] = choices
        requested = offer["resource_options"]
        available_bundles = []
        for bundle in requested:
            resources = tuple(bundle["resources"])
            available_at = max(
                offer["not_before"] + float(bundle.get("delay", 0.0)), now
            )
            blockers = {
                resource: {"group_id": holder[0], "phase_id": holder[1]}
                for resource, holder in self._resource_holds.items()
                if resource in resources and holder[0] != group.group_id
            }
            available_bundles.append({
                "resources": list(resources),
                "available_at": available_at,
                "blockers": blockers,
            })
        return {
            "offered": True,
            "by": offer["by"],
            "reason": offer["reason"],
            "not_before": offer["not_before"],
            "member_choices": member_choices,
            "resource_choices": available_bundles,
            "adopted": offer["adopted"],
        }

    def _wait_description(
        self,
        group: JointCommitment,
        target_phase: str | None,
        now: float,
        waiting_on: list[Any],
        requested: tuple[str, ...],
        resource_holds: dict[str, str],
    ) -> list[dict[str, Any]]:
        description: list[dict[str, Any]] = []
        for entry in waiting_on:
            if isinstance(entry, dict):
                description.append(
                    {"kind": "member",
                      "role": entry["role"],
                      "participant_id": entry["participant_id"],
                      "phase_id": target_phase,
                      "available_at": entry["until"],
                      "reason": entry["wait_reason"]}
                )
            else:
                description.append(
                    {"kind": "member", "role": entry, "phase_id": target_phase}
                )
        for resource, holder_group in resource_holds.items():
            holder = self._groups.get(holder_group)
            description.append(
                {"kind": "resource",
                  "resource": resource,
                  "phase_id": target_phase,
                  "held_by_group": holder_group,
                  "held_by_phase": holder.active_phase if holder else None,
                  "reason": (
                      f"等待 {holder_group}"
                      f"{':' + holder.active_phase if holder and holder.active_phase else ''}"
                      f" 释放 {resource}"
                  )}
            )
        if not description and group.state == STARTED and target_phase is None:
            if group.active_phase is not None:
                reason = (
                    f"阶段 {group.active_phase} 正在执行，继续到安全结束"
                )
                if group.end_reason:
                    reason += f"；后续阶段已取消（{group.end_reason}）"
                description.append({
                    "kind": "active_phase",
                    "phase_id": group.active_phase,
                    "reason": reason,
                })
            else:
                description.append(
                    {"kind": "nothing", "reason": "所有阶段均已完成"})
        return description

    def diagnose_waits(self, *, now: float) -> dict[str, Any]:
        """Build the waits-for graph across live groups and detect cycles.

        A group that has members ready for its next phase but cannot get that
        phase's resources waits on the group currently holding them. If that
        other group is itself waiting, the graph shows the chain; cycles are
        reported explicitly so a circular standstill can be diagnosed rather
        than appearing as a silent hang.
        """
        nodes: dict[str, dict[str, Any]] = {}
        edges: list[dict[str, Any]] = {}
        adjacency: dict[str, list[str]] = {}
        for group in self._groups.values():
            if group.terminal:
                continue
            phase_id = group.next_phase() if group.phased else (
                group.active_phase or group.next_phase()
            )
            phase_info = (
                self._phase_status(group, phase_id, now)
                if group.phased and phase_id is not None
                else None
            )
            waits: list[dict[str, Any]] = []
            if phase_info is not None:
                for entry in phase_info["waiting_on"]:
                    waits.append(
                        {"kind": "member", "role": entry["role"],
                          "participant_id": entry["participant_id"],
                          "phase_id": phase_id,
                          "available_at": entry["until"]}
                    )
                for resource, blocker in phase_info["resource_blockers"].items():
                    waits.append(
                        {"kind": "resource", "resource": resource,
                          "phase_id": phase_id,
                          "held_by_group": blocker["group_id"],
                          "held_by_phase": blocker["phase_id"]}
                    )
            elif not group.phased:
                for member in group.members:
                    commitment = self._commitments[member["commitment_id"]]
                    if commitment.state == WAITING and commitment.due_at > now:
                        waits.append(
                            {"kind": "member", "role": member["role"],
                              "participant_id": member["participant_id"],
                              "available_at": commitment.due_at}
                        )
                requested = group.resources
                if not waits:
                    for resource in requested:
                        holder = self._resource_holds.get(resource)
                        if holder and holder[0] != group.group_id:
                            waits.append(
                                {"kind": "resource", "resource": resource,
                                  "held_by_group": holder[0],
                                  "held_by_phase": holder[1]}
                            )
            nodes[group.group_id] = {
                "group_id": group.group_id,
                "state": group.state,
                "active_phase": group.active_phase,
                "next_phase": group.next_phase(),
                "waits_for": waits,
            }
            outgoing = sorted(
                {
                    wait["held_by_group"]
                    for wait in waits
                    if wait["kind"] == "resource"
                    and not self._groups[wait["held_by_group"]].terminal
                }
            )
            adjacency[group.group_id] = outgoing
            for target in outgoing:
                edges[f"{group.group_id}>{target}"] = {
                    "from_group": group.group_id,
                    "to_group": target,
                    "phase_id": phase_id,
                }
        cycles = self._find_cycles(adjacency)
        return {
            "time": now,
            "groups": nodes,
            "edges": list(edges.values()),
            "cycles": cycles,
            "deadlocked": bool(cycles),
        }

    @staticmethod
    def _find_cycles(adjacency: dict[str, list[str]]) -> list[list[str]]:
        cycles: list[list[str]] = []
        seen_signatures: set[tuple[str, ...]] = set()
        WHITE, GREY, BLACK = 0, 1, 2
        color = {node: WHITE for node in adjacency}
        stack: list[str] = []

        def visit(node: str) -> None:
            color[node] = GREY
            stack.append(node)
            for target in adjacency.get(node, ()):
                if color.get(target) == GREY:
                    start = stack.index(target)
                    cycle = stack[start:] + [target]
                    signature = tuple(sorted(cycle[:-1]))
                    if signature not in seen_signatures:
                        seen_signatures.add(signature)
                        cycles.append(cycle)
                elif color.get(target) == WHITE:
                    visit(target)
            stack.pop()
            color[node] = BLACK

        for node in sorted(adjacency):
            if color[node] == WHITE:
                visit(node)
        return cycles

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
            participant_indexes = [
                index for index, member in enumerate(group.members)
                if member["participant_id"] == actor_id
            ]
            is_requester = actor_id == group.requester_id
            if not participant_indexes and not is_requester:
                continue
            # A phased task that already started survives the departure up
            # to the safe-finish boundary: the running phase keeps its hold
            # and runs to finish; only promises of phases that have not
            # started are revoked. The departed member may be substituted
            # via a delayed-continuation offer.
            if (
                group.phased
                and group.state == STARTED
                and participant_indexes
                and group.active_phase is not None
            ):
                for phase_id in group.phases:
                    state = group.phase_state.get(phase_id, WAITING)
                    if state != WAITING:
                        # The active phase runs to its safe finish: the
                        # departing member keeps participating in recorded
                        # work until finish_phase closes it normally.
                        continue
                    for index in participant_indexes:
                        commitment = self._commitments[
                            group.phase_commitments[phase_id][index]
                        ]
                        role = group.members[index]["role"]
                        if commitment.state == WAITING:
                            commitment.state = ENDED
                            commitment.end_reason = (
                                f"participant_lost:{actor_id}:{phase_id}"
                            )
                            commitment.record(now, "ended",
                                               rule=RULE_REMOVAL_FIRST,
                                               detail={
                                                   "lost_participant": actor_id,
                                                   "phase_id": phase_id},
                                               event_sequence=None)
                missing_by_phase: dict[str, set[str]] = {}
                for phase_id in group.phases:
                    if group.phase_state.get(phase_id) != WAITING:
                        continue
                    resolved = self._role_commitments(group, phase_id)
                    gone = {
                        member["role"] for member, commitment in resolved
                        if commitment.state != WAITING
                    }
                    if gone:
                        missing_by_phase[phase_id] = gone
                if missing_by_phase:
                    group.missing_roles = {
                        phase_id: sorted(gone)
                        for phase_id, gone in missing_by_phase.items()
                    }
                    detail = {"lost_participant": actor_id,
                              "active_phase": group.active_phase,
                              "policy": "run-to-safe-finish",
                              "missing_by_phase": {
                                  pid: sorted(gone)
                                  for pid, gone in missing_by_phase.items()
                              }}
                    sequence = self._emit_joint(
                        publish, "joint_member_lost", group, now, extra=detail)
                    group.record(now, "member_lost", rule=RULE_REMOVAL_FIRST,
                                 detail=detail, event_sequence=sequence)
                continue
            group.state = ENDED
            group.end_reason = f"participant_lost:{actor_id}"
            sequence = self._emit_joint(publish, "joint_commitment_ended", group,
                                        now, extra={"lost_participant": actor_id})
            group.record(now, "ended", rule=RULE_REMOVAL_FIRST,
                         detail={"lost_participant": actor_id},
                         event_sequence=sequence)
            self._release_group_holds(group, now)
            for commitment in self._group_member_commitments(group):
                if commitment.state in (WAITING, STARTED):
                    commitment.state = ENDED
                    commitment.end_reason = f"group_ended:{group.group_id}"
                    commitment.record(now, "ended", rule=RULE_REMOVAL_FIRST,
                                      detail={"lost_participant": actor_id},
                                      event_sequence=sequence)
            for phase_id in group.phases:
                if group.phase_state.get(phase_id) == WAITING:
                    group.phase_state[phase_id] = ENDED
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

    def _group_member_commitments(
        self, group: JointCommitment
    ) -> list[Commitment]:
        ids: list[str] = []
        if group.phased:
            for phase_id in group.phases:
                ids.extend(group.phase_commitments.get(phase_id, ()))
        else:
            ids.extend(m["commitment_id"] for m in group.members)
        return [self._commitments[commitment_id] for commitment_id in ids]

    def _advance_groups(
        self, now: float, *, publish: PublishFn | None = None
    ) -> None:
        """Move live groups into their next phase when ready, in fixed order."""
        waiting = sorted(
            (g for g in self._groups.values() if g.state in (WAITING, STARTED)),
            key=lambda g: (g.created_at, g.group_id),
        )
        progress = True
        while progress:
            progress = False
            for group in waiting:
                if group.terminal or group.active_phase is not None:
                    continue
                phase_id = group.next_phase()
                if phase_id is None:
                    continue
                if group.phased:
                    started = self._try_start_phase(group, phase_id, now, publish)
                else:
                    started = self._try_start_legacy(group, now, publish)
                if started:
                    progress = True

    def _try_start_legacy(
        self, group: JointCommitment, now: float, publish: PublishFn | None
    ) -> bool:
        members = [self._commitments[m["commitment_id"]] for m in group.members]
        if not all(c.state == WAITING and c.due_at <= now for c in members):
            return False
        blockers = {
            resource: self._resource_holds[resource][0]
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
            return False
        for resource in group.resources:
            self._resource_holds[resource] = (group.group_id, PHASE_EXECUTE)
        self._next_decision_id += 1
        group.decision_id = f"d{self._next_decision_id}"
        group.state = STARTED
        group.started_at = now
        group.active_phase = PHASE_EXECUTE
        group.phase_state[PHASE_EXECUTE] = STARTED
        sequence = self._emit_joint(publish, "joint_commitment_started", group, now)
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
        return True

    def _try_start_phase(
        self,
        group: JointCommitment,
        phase_id: str,
        now: float,
        publish: PublishFn | None,
    ) -> bool:
        resolved = self._role_commitments(group, phase_id)
        members = [commitment for _, commitment in resolved]
        unready = [c for c in members if not (c.state == WAITING and c.due_at <= now)]
        offer = group.reschedule_offers.get(phase_id)
        bundles = offer["resource_options"] if offer else [
            {"resources": list(group.phase_resources.get(phase_id, ())),
             "delay": 0.0}
        ]
        not_before = offer["not_before"] if offer else now
        if unready:
            return False
        if now < not_before:
            return False
        # Live re-check against the actual hold table at adoption time:
        # candidate bundles never reserve anything beforehand, and a hold
        # owned by another group (or by this group's *other* active phase)
        # cannot be borrowed.
        adopted = None
        all_blockers: list[dict[str, Any]] = []
        for bundle in sorted(bundles, key=lambda item: item["delay"]):
            if now < not_before + float(bundle.get("delay", 0.0)):
                continue
            requested = tuple(bundle["resources"])
            blockers = {
                resource: {"group_id": holder[0], "phase_id": holder[1]}
                for resource, holder in self._resource_holds.items()
                if resource in requested and holder[0] != group.group_id
            }
            own_cross_phase = {
                resource: {"group_id": holder[0], "phase_id": holder[1]}
                for resource, holder in self._resource_holds.items()
                if resource in requested and holder[0] == group.group_id
                and holder[1] != phase_id
            }
            blockers.update(own_cross_phase)
            if not blockers:
                adopted = (bundle, requested)
                break
            all_blockers.append({"resources": list(requested),
                                 "blockers": blockers})
        if adopted is None:
            diagnosis = self.diagnose_waits(now=now)
            in_cycle = any(
                group.group_id in cycle for cycle in diagnosis["cycles"]
            )
            detail = {"phase_id": phase_id,
                      "blockers": all_blockers[-1]["blockers"] if all_blockers else {},
                      "tried": all_blockers,
                      "deadlock": in_cycle,
                      "reschedule": offer is not None,
                      "cycles": diagnosis["cycles"] if in_cycle else []}
            sequence = self._emit_joint(
                publish, "joint_phase_deferred", group, now,
                extra=detail,
            )
            group.record(
                now, "phase_deferred",
                rule=RULE_RESCHEDULE_RECHECK if offer else RULE_RESOURCE_PRIORITY,
                detail=detail, event_sequence=sequence,
            )
            return False
        bundle, requested = adopted
        for resource in requested:
            self._resource_holds[resource] = (group.group_id, phase_id)
        if offer is not None:
            offer["adopted"] = {
                "time": now,
                "resources": list(requested),
                "members": [member["participant_id"] for member, _ in resolved],
            }
            group.phase_resources[phase_id] = tuple(requested)
        first_start = group.state == WAITING
        if first_start:
            self._next_decision_id += 1
            group.decision_id = f"d{self._next_decision_id}"
            group.state = STARTED
            group.started_at = now
        group.active_phase = phase_id
        group.phase_state[phase_id] = STARTED
        members_detail = [
            {"role": member["role"],
              "participant_id": member["participant_id"],
              "commitment_id": commitment.commitment_id,
              "replacement": bool(member.get("replacement")),
              "note": (
                  f"{member['role']} 作为替代成员衔接 {phase_id}"
                  if member.get("replacement")
                  else f"{member['role']} 准备好进入 {phase_id}"
              )}
            for member, commitment in resolved
        ]
        group.phase_members[phase_id] = [dict(member) for member, _ in resolved]
        sequence = self._emit_joint(
            publish, "joint_phase_started", group, now,
            extra={"phase_id": phase_id,
                    "decision_id": group.decision_id,
                    "first_start": first_start,
                    "members": members_detail,
                    "resources": list(requested),
                    "reschedule_adopted": offer["adopted"] if offer else None},
        )
        if first_start:
            group.record(now, "started", rule=RULE_JOINT_ALL_READY,
                          detail={"decision_id": group.decision_id,
                                  "phase_id": phase_id},
                          event_sequence=sequence)
        group.record(now, "phase_started", rule=RULE_PHASE_SEQUENCE,
                      detail={"phase_id": phase_id, "members": members_detail},
                      event_sequence=sequence)
        for commitment, member in zip(members, resolved):
            commitment.state = STARTED
            commitment.started_at = now
            commitment.record(now, "phase_ready" if not member[0].get("replacement")
                                   else "reschedule_member_ready",
                               rule=RULE_PHASE_SEQUENCE,
                               detail={"phase_id": phase_id,
                                        "role": member[0]["role"],
                                        "participant_id": member[0]["participant_id"]},
                               event_sequence=sequence)
        return True

    def _settle_group_expiries(
        self, now: float, *, publish: PublishFn | None = None
    ) -> None:
        """End groups whose waiting commitments can no longer take effect."""
        for group in tuple(self._groups.values()):
            if group.state not in (WAITING, STARTED):
                continue
            if group.state == STARTED and group.active_phase is not None:
                continue
            broken: list[Commitment] = []
            if group.phased:
                next_phase = group.next_phase()
                if next_phase is None:
                    continue
                resolved = self._role_commitments(group, next_phase)
                for _, commitment in resolved:
                    if commitment.state != WAITING:
                        broken.append(commitment)
            else:
                broken = [
                    self._commitments[m["commitment_id"]]
                    for m in group.members
                    if self._commitments[m["commitment_id"]].state != WAITING
                ]
            if not broken:
                continue
            if group.phased and group.state == STARTED:
                # A task already underway that lost a later-phase member does
                # not end at the failure boundary: it keeps waiting (the
                # active phase first runs to its safe finish) so the user
                # can opt into a delayed continuation. Ending here would be
                # a fake finish while unstarted work remains possible.
                roles = {
                    member["role"]
                    for member, commitment in resolved
                    if commitment.state != WAITING
                }
                offer_viable = self._offer_can_resolve(group, next_phase)
                if offer_viable:
                    # A delayed continuation was opted into and still has
                    # live promises: keep waiting instead of ending.
                    existing = set(group.missing_roles.get(next_phase, ()))
                    if roles:
                        group.missing_roles[next_phase] = sorted(existing | roles)
                        group.record(
                            now, "phase_member_unavailable",
                            rule=RULE_WAIT_OVER_FALSE_END,
                            detail={"phase_id": next_phase, "roles": sorted(roles),
                                    "commitment_ids": [c.commitment_id
                                                       for c in broken]},
                        )
                    continue
                # The offer's promises are gone too: end honestly at the
                # wait boundary rather than holding a fake completion.
                group.state = ENDED
                group.end_reason = (
                    f"member_unavailable:{next_phase}:{','.join(sorted(roles))}"
                )
                sequence = self._emit_joint(
                    publish, "joint_commitment_ended", group, now,
                    extra={"end_reason": group.end_reason,
                            "unresolved_roles": sorted(roles)},
                )
                group.record(now, "ended", rule=RULE_WAIT_OVER_FALSE_END,
                              detail={"end_reason": group.end_reason,
                                      "commitment_ids": [c.commitment_id
                                                         for c in broken]},
                              event_sequence=sequence)
                for phase_id in group.phases:
                    if group.phase_state.get(phase_id) == WAITING:
                        group.phase_state[phase_id] = ENDED
                self._release_group_holds(group, now)
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
            if group.phased:
                next_phase = group.next_phase() or group.active_phase
                for phase_id in group.phases:
                    if group.phase_state.get(phase_id) == WAITING:
                        group.phase_state[phase_id] = ENDED
                    for cid in group.phase_commitments[phase_id]:
                        commitment = self._commitments[cid]
                        if commitment.state == WAITING:
                            commitment.state = ENDED
                            commitment.end_reason = (
                                f"group_expired:{group.group_id}:{phase_id}"
                            )
                            commitment.record(now, "ended",
                                               rule=RULE_MEMBER_EXPIRY_ENDS_GROUP,
                                               detail={"group_id": group.group_id,
                                                       "phase_id": phase_id},
                                               event_sequence=sequence)
            else:
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

    def _release_group_holds(self, group: JointCommitment, now: float) -> None:
        for resource in list(self._resource_holds):
            if self._resource_holds[resource][0] == group.group_id:
                phase_id = self._resource_holds[resource][1]
                del self._resource_holds[resource]
                group.record(now, "resource_released",
                              detail={"resource": resource, "phase_id": phase_id})

    def _release_phase_holds(
        self, group: JointCommitment, phase_id: str, now: float
    ) -> None:
        for resource in list(self._resource_holds):
            holder_group, holder_phase = self._resource_holds[resource]
            if holder_group == group.group_id and holder_phase == phase_id:
                del self._resource_holds[resource]
                group.record(now, "resource_released",
                              detail={"resource": resource, "phase_id": phase_id})

    def _release_orphaned_holds(self, now: float) -> None:
        """No terminal/inactive group may keep a resource hold."""
        for resource in list(self._resource_holds):
            group_id, phase_id = self._resource_holds[resource]
            group = self._groups.get(group_id)
            stale = (
                group is None
                or group.state != STARTED
                or group.active_phase != phase_id
            )
            if stale:
                del self._resource_holds[resource]
                if group is not None:
                    group.record(now, "resource_released",
                                  detail={"resource": resource, "phase_id": phase_id,
                                          "reason": "orphaned_hold"})

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
            "active_phase": group.active_phase,
            "cause": deepcopy(group.cause),
        }
        if extra:
            payload.update(extra)
        return publish(event_type, payload, None)

    def snapshot(self) -> dict[str, Any]:
        return {
            "next_id": self._next_id,
            "commitments": [
                commitment.to_dict() for commitment in self._commitments.values()
            ],
            "next_group_id": self._next_group_id,
            "next_decision_id": self._next_decision_id,
            "groups": [group.to_dict() for group in self._groups.values()],
            "resource_holds": {
                resource: list(holder)
                for resource, holder in self._resource_holds.items()
            },
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
        raw_holds = data.get("resource_holds", {})
        for resource, holder in raw_holds.items():
            if isinstance(holder, (list, tuple)):
                self._resource_holds[str(resource)] = (str(holder[0]),
                                                        str(holder[1]))
            else:
                group_id = str(holder)
                group = self._groups.get(group_id)
                phase_id = group.active_phase if group else PHASE_EXECUTE
                self._resource_holds[str(resource)] = (group_id, phase_id)






@dataclass(slots=True)
class JointCommitment:
    """A group of member commitments that can only take effect together.

    A phased joint task moves through an ordered sequence of phases
    (prepare, execute, handover by default). Each phase is a fresh resource
    negotiation: a member being ready for one phase never reserves resources
    of a later phase, and resources are released the moment a phase is
    finished. Finished phases are recorded history and cannot be undone by
    cancelling a phase that has not started yet.
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
    phases: tuple[str, ...] = (PHASE_EXECUTE,)
    phase_resources: dict[str, tuple[str, ...]] = field(default_factory=dict)
    phase_state: dict[str, str] = field(default_factory=dict)
    active_phase: str | None = None
    phase_commitments: dict[str, list[str]] = field(default_factory=dict)
    # Per-phase roster actually used when the phase starts. A rescheduled
    # phase can be carried out by replacement members; finished phases keep
    # their original roster and are never rewritten.
    phase_members: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # phase_id -> reschedule offer the user opted into for a delayed later
    # phase. An offer carries candidate members (each its own readiness
    # commitment) and candidate resource bundles; nothing here holds a
    # resource before adoption.
    reschedule_offers: dict[str, dict[str, Any]] = field(default_factory=dict)
    # phase_id -> roles whose member became unavailable before that phase.
    missing_roles: dict[str, list[str]] = field(default_factory=dict)
    history: list[dict[str, Any]] = field(default_factory=list)

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def phased(self) -> bool:
        return self.phases != (PHASE_EXECUTE,)

    def phase_index(self, phase_id: str) -> int:
        return self.phases.index(phase_id)

    def next_phase(self) -> str | None:
        """First phase that has not started yet, or None once all ran."""
        for phase_id in self.phases:
            if self.phase_state.get(phase_id, WAITING) == WAITING:
                return phase_id
        return None

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
            "phases": list(self.phases),
            "phase_resources": {
                phase_id: list(resources)
                for phase_id, resources in self.phase_resources.items()
            },
            "phase_state": dict(self.phase_state),
            "active_phase": self.active_phase,
            "phase_commitments": {
                phase_id: list(ids)
                for phase_id, ids in self.phase_commitments.items()
            },
            "phase_members": {
                phase_id: [dict(member) for member in members]
                for phase_id, members in self.phase_members.items()
            },
            "reschedule_offers": deepcopy(self.reschedule_offers),
            "missing_roles": {
                phase_id: list(roles)
                for phase_id, roles in self.missing_roles.items()
            },
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
            phases=tuple(values.get("phases", (PHASE_EXECUTE,))),
            phase_resources={
                phase_id: tuple(resources)
                for phase_id, resources in values.get("phase_resources", {}).items()
            },
            phase_state=dict(values.get("phase_state", {})),
            active_phase=values.get("active_phase"),
            phase_commitments={
                phase_id: list(ids)
                for phase_id, ids in values.get("phase_commitments", {}).items()
            },
            phase_members={
                phase_id: [dict(member) for member in members]
                for phase_id, members in values.get("phase_members", {}).items()
            },
            reschedule_offers={
                phase_id: dict(offer)
                for phase_id, offer in values.get("reschedule_offers", {}).items()
            },
            missing_roles={
                phase_id: list(roles)
                for phase_id, roles in values.get("missing_roles", {}).items()
            },
            history=[dict(entry) for entry in values.get("history", ())],
        )
