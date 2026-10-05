from __future__ import annotations

from behavior_runtime import (
    ActorRuntime,
    VirtualClock,
    World,
)
from behavior_runtime.commitments import (
    CANCELLED,
    COMPLETED,
    ENDED,
    PHASE_EXECUTE,
    PHASE_HANDOVER,
    PHASE_PREPARE,
    STARTED,
    WAITING,
    RULE_RESCHEDULE_RECHECK,
)
from behavior_runtime.examples import MoveToTarget, TargetPolicy


def make_world(actor_ids):
    world = World({"target": None}, clock=VirtualClock())
    for actor_id in actor_ids:
        ActorRuntime(actor_id, TargetPolicy(), world,
                     action_types=[MoveToTarget])
        world.register_actor(actor_id)
    return world


def track_events(world):
    seen = []
    world.events.subscribe(lambda event: seen.append(event))
    return seen


def request_phased(world, requester, members, phase_resources):
    return world.request_phased_joint_commitment(
        requester_id=requester,
        action="MoveToTarget",
        participants=[
            {
                "role": member,
                "performer_id": member,
                "phases": {
                    PHASE_PREPARE: {"delay": 0},
                    PHASE_EXECUTE: {"delay": 0},
                    PHASE_HANDOVER: {"delay": 0},
                },
            }
            for member in members
        ],
        phase_resources=phase_resources,
    )


def reach_execute(world, *groups):
    world.dispatch_due()
    for group in groups:
        assert group.active_phase == PHASE_PREPARE
    for group in groups:
        world.finish_joint_phase(group.group_id, PHASE_PREPARE)
    world.dispatch_due()
    for group in groups:
        assert group.active_phase == PHASE_EXECUTE


def phase_started_events(seen, group_id):
    return [
        e for e in seen
        if e.type == "joint_phase_started" and e.payload["group_id"] == group_id
    ]


def test_reschedule_handover_waits_for_device_release_and_rechecks_occupancy():
    world = make_world(("a1", "a2", "b1", "b2"))
    seen = track_events(world)
    g1 = request_phased(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("bridge",),
         PHASE_HANDOVER: ("dock",)},
    )
    g2 = request_phased(
        world, "b1", ("b1", "b2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("dock",), PHASE_HANDOVER: ()},
    )
    reach_execute(world, g1, g2)
    assert world.commitments.resource_holds() == {
        "bridge": g1.group_id, "dock": g2.group_id,
    }

    # The follow-up handover is cancelled while execute runs to safe finish.
    world.cancel_joint_phase(g1.group_id, PHASE_HANDOVER, by="a1",
                             reason="交接取消")
    assert g1.state == STARTED
    assert g1.phase_state[PHASE_HANDOVER] == CANCELLED

    # The candidate cannot borrow the handover device early: g2 still holds
    # it, so no arrangement is adoptable and the group keeps waiting.
    result = world.reschedule_joint_phase(
        g1.group_id, PHASE_HANDOVER, by="a1", resources=("dock",),
    )
    assert result is None
    world.dispatch_due()
    deferred = [e for e in seen
                if e.type == "joint_phase_reschedule_deferred"]
    assert len(deferred) == 1
    assert deferred[0].payload["blockers"] == {
        "dock": {"group_id": g2.group_id, "phase_id": PHASE_EXECUTE},
    }
    record = [h for h in g1.history if h["kind"] == "reschedule_deferred"]
    assert record[-1]["rule"] == RULE_RESCHEDULE_RECHECK
    # Keeping waiting, not a false ending.
    assert g1.state == STARTED
    assert g1.active_phase == PHASE_EXECUTE
    assert [e for e in seen if e.type in (
        "joint_commitment_ended", "joint_commitment_completed",
    )] == []

    # g2 releases the device at its own safe finish; occupancy is
    # re-checked on adoption, so the retry now succeeds.
    world.finish_joint_phase(g2.group_id, PHASE_EXECUTE)
    assert world.commitments.resource_holds() == {"bridge": g1.group_id}
    result = world.reschedule_joint_phase(
        g1.group_id, PHASE_HANDOVER, by="a1", resources=("dock",),
        reason="交接重排",
    )
    assert result is g1
    world.dispatch_due()
    adopted = [e for e in seen if e.type == "joint_phase_rescheduled"]
    assert len(adopted) == 1
    assert adopted[0].payload["resources"] == ["dock"]
    assert adopted[0].payload["cleared_end_reason"] == (
        f"phase_cancelled:{PHASE_HANDOVER}:交接取消"
    )
    # Partial cancellation only revoked the not-yet-started promises; the
    # reschedule keeps that outcome and adds fresh waiting promises.
    old_ids = adopted[0].payload["superseded_commitments"]
    assert old_ids
    for commitment_id in old_ids:
        old = world.commitments.get(commitment_id)
        assert old.state == CANCELLED
        assert old.end_reason == (
            f"phase_cancelled:{g1.group_id}:{PHASE_HANDOVER}"
        )
    new_ids = g1.phase_commitments[PHASE_HANDOVER]
    assert set(new_ids).isdisjoint(old_ids)
    assert all(
        world.commitments.get(cid).state == WAITING for cid in new_ids
    )

    # The executing phase reaches its safe finish late; the group is not
    # ended because the handover was rescheduled.
    world.finish_joint_phase(g1.group_id, PHASE_EXECUTE)
    assert g1.state == STARTED
    assert world.commitments.resource_holds() == {}
    world.dispatch_due()
    assert g1.active_phase == PHASE_HANDOVER
    assert world.commitments.resource_holds() == {"dock": g1.group_id}

    world.finish_joint_phase(g1.group_id, PHASE_HANDOVER)
    assert g1.state == COMPLETED
    world.dispatch_due()
    world.finish_joint_phase(g2.group_id, PHASE_HANDOVER)
    assert g2.state == COMPLETED
    assert world.commitments.resource_holds() == {}

    # Finished work was never re-executed: each phase started exactly once.
    starts = phase_started_events(seen, g1.group_id)
    assert [e.payload["phase_id"] for e in starts] == [
        PHASE_PREPARE, PHASE_EXECUTE, PHASE_HANDOVER,
    ]
    execute_commitments = g1.phase_commitments[PHASE_EXECUTE]
    assert all(
        world.commitments.get(cid).state == COMPLETED
        for cid in execute_commitments
    )


def test_member_quits_during_execute_and_replacement_rescues_handover():
    world = make_world(("a1", "a2", "a3", "b1", "b2"))
    seen = track_events(world)
    g1 = request_phased(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("bridge",),
         PHASE_HANDOVER: ("dock",)},
    )
    g2 = request_phased(
        world, "b1", ("b1", "b2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("scanner",),
         PHASE_HANDOVER: ()},
    )
    reach_execute(world, g1, g2)

    # A member quits mid-execution: the active phase runs to its safe
    # finish, the group keeps its holds, and only the lost member's
    # not-yet-started promises are ended.
    world.remove_actor("a2")
    world.dispatch_due()
    assert g1.state == STARTED
    assert g1.active_phase == PHASE_EXECUTE
    assert g1.end_reason == "participant_lost:a2"
    assert world.commitments.resource_holds() == {
        "bridge": g1.group_id, "scanner": g2.group_id,
    }
    lost_events = [e for e in seen if e.type == "joint_participant_lost"]
    assert len(lost_events) == 1
    assert lost_events[0].payload["lost_participant"] == "a2"
    assert lost_events[0].payload["active_phase_policy"] == (
        "run-to-safe-finish"
    )
    assert [e for e in seen if e.type == "joint_commitment_ended"] == []
    handover_ids = list(g1.phase_commitments[PHASE_HANDOVER])
    lost_commitment = world.commitments.get(handover_ids[1])
    assert lost_commitment.performer_id == "a2"
    assert lost_commitment.state == ENDED
    assert lost_commitment.end_reason == "participant_lost:a2"

    # A replacement who is busy elsewhere is not a capable candidate.
    result = world.reschedule_joint_phase(
        g1.group_id, PHASE_HANDOVER, by="a1", replacements={"a2": "b1"},
    )
    assert result is None
    world.dispatch_due()
    deferred = [e for e in seen
                if e.type == "joint_phase_reschedule_deferred"]
    assert len(deferred) == 1
    assert list(deferred[0].payload["busy_members"]) == ["b1"]
    assert g1.state == STARTED

    # A free replacement is adopted; the pending participant_lost ending
    # is cleared and only the remaining waiting promise is revoked.
    result = world.reschedule_joint_phase(
        g1.group_id, PHASE_HANDOVER, by="a1", replacements={"a2": "a3"},
    )
    assert result is g1
    assert g1.end_reason is None
    kept = world.commitments.get(handover_ids[0])
    assert kept.performer_id == "a1"
    assert kept.state == CANCELLED
    assert kept.end_reason == (
        f"phase_rescheduled:{g1.group_id}:{PHASE_HANDOVER}"
    )
    assert lost_commitment.state == ENDED  # history preserved, not erased

    # The safe finish lands, the rescheduled handover runs with the
    # replacement, and everything is released at the end.
    world.finish_joint_phase(g1.group_id, PHASE_EXECUTE)
    assert g1.state == STARTED
    world.dispatch_due()
    assert g1.active_phase == PHASE_HANDOVER
    assert world.commitments.resource_holds() == {
        "dock": g1.group_id, "scanner": g2.group_id,
    }
    handover_start = [
        e for e in phase_started_events(seen, g1.group_id)
        if e.payload["phase_id"] == PHASE_HANDOVER
    ]
    assert len(handover_start) == 1
    participants = {
        m["participant_id"] for m in handover_start[0].payload["members"]
    }
    assert participants == {"a1", "a3"}

    world.finish_joint_phase(g1.group_id, PHASE_HANDOVER)
    assert g1.state == COMPLETED
    world.finish_joint_phase(g2.group_id, PHASE_EXECUTE)
    world.dispatch_due()
    world.finish_joint_phase(g2.group_id, PHASE_HANDOVER)
    assert g2.state == COMPLETED
    assert world.commitments.resource_holds() == {}

    # The departed member's executed work is recorded, not re-run.
    execute_ids = g1.phase_commitments[PHASE_EXECUTE]
    departed_execute = world.commitments.get(execute_ids[1])
    assert departed_execute.performer_id == "a2"
    assert departed_execute.state == COMPLETED
    starts = phase_started_events(seen, g1.group_id)
    assert [e.payload["phase_id"] for e in starts] == [
        PHASE_PREPARE, PHASE_EXECUTE, PHASE_HANDOVER,
    ]


def test_late_safe_finish_with_delayed_followup_never_ends_falsely():
    world = make_world(("a1", "a2"))
    seen = track_events(world)
    group = request_phased(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("bridge",),
         PHASE_HANDOVER: ("dock",)},
    )
    reach_execute(world, group)

    world.cancel_joint_phase(group.group_id, PHASE_HANDOVER, by="a1")
    # The user allows a delayed follow-up phase; the device is free, so
    # the arrangement is adopted immediately.
    result = world.reschedule_joint_phase(
        group.group_id, PHASE_HANDOVER, by="a1", delay=3,
    )
    assert result is group
    new_ids = group.phase_commitments[PHASE_HANDOVER]
    assert all(
        world.commitments.get(cid).due_at == 3 for cid in new_ids
    )

    # The safe finish arrives late; the delayed members are not ready yet,
    # so the group waits instead of pretending to be done.
    world.clock.advance(2)
    world.finish_joint_phase(group.group_id, PHASE_EXECUTE)
    assert group.state == STARTED
    assert group.active_phase is None
    assert world.commitments.resource_holds() == {}
    world.dispatch_due()
    assert group.state == STARTED
    assert group.active_phase is None
    assert [e for e in seen if e.type in (
        "joint_commitment_ended", "joint_commitment_completed",
    )] == []
    status = world.joint_status(group.group_id)
    member_waits = [w for w in status["waiting_for"] if w["kind"] == "member"]
    assert {w["role"] for w in member_waits} == {"a1", "a2"}
    assert all(w["available_at"] == 3 for w in member_waits)

    # Once the delayed members are ready, the handover starts, holds the
    # device, and releases it at its own finish.
    world.clock.advance(1)
    world.dispatch_due()
    assert group.active_phase == PHASE_HANDOVER
    assert world.commitments.resource_holds() == {"dock": group.group_id}
    world.finish_joint_phase(group.group_id, PHASE_HANDOVER)
    assert group.state == COMPLETED
    assert group.end_reason == "completed"
    assert world.commitments.resource_holds() == {}
    starts = phase_started_events(seen, group.group_id)
    assert [e.payload["phase_id"] for e in starts] == [
        PHASE_PREPARE, PHASE_EXECUTE, PHASE_HANDOVER,
    ]
