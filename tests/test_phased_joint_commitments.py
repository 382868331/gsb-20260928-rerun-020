from __future__ import annotations

from behavior_runtime import (
    ActorRuntime,
    VirtualClock,
    World,
    WorldArchive,
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
    RULE_ACTIVE_PHASE_SAFE_END,
    RULE_FINISHED_WORK_IMMUTABLE,
    RULE_PHASE_SEQUENCE,
    RULE_RESOURCE_PRIORITY,
)
from behavior_runtime.examples import MoveToTarget, TargetPolicy


ACTORS = ("a1", "a2", "b1", "b2", "solo")
PHASE_TIMING = {
    PHASE_PREPARE: {"delay": 0},
    PHASE_EXECUTE: {"delay": 0},
    PHASE_HANDOVER: {"delay": 0},
}


def make_world(actor_ids=ACTORS):
    world = World({"target": None}, clock=VirtualClock())
    runtimes = {
        actor_id: ActorRuntime(
            actor_id, TargetPolicy(), world, action_types=[MoveToTarget]
        )
        for actor_id in actor_ids
    }
    for actor_id in actor_ids:
        world.register_actor(actor_id)
    return world, runtimes


def track_events(world):
    seen = []
    world.events.subscribe(lambda event: seen.append(event))
    return seen


def request_phased(world, requester, members, phase_resources, *,
                   phases=(PHASE_PREPARE, PHASE_EXECUTE, PHASE_HANDOVER),
                   timing=None):
    timing = PHASE_TIMING if timing is None else timing
    return world.request_phased_joint_commitment(
        requester_id=requester,
        action="MoveToTarget",
        participants=[
            {
                "role": member,
                "performer_id": member,
                "phases": {
                    phase: dict(timing.get(phase, {"delay": 0}))
                    for phase in phases
                },
            }
            for member in members
        ],
        phases=phases,
        phase_resources=phase_resources,
    )


def deadlock_pair(world):
    g1 = request_phased(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("X",), PHASE_HANDOVER: ("Y",)},
    )
    g2 = request_phased(
        world, "b1", ("b1", "b2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("Y",), PHASE_HANDOVER: ("X",)},
    )
    g3 = request_phased(
        world, "solo", ("solo",),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("Z",), PHASE_HANDOVER: ()},
    )
    return g1, g2, g3


def reach_handover_deadlock(world, g1, g2, g3):
    world.dispatch_due()
    assert g1.active_phase == PHASE_PREPARE
    assert g2.active_phase == PHASE_PREPARE
    assert g3.active_phase == PHASE_PREPARE
    for group in (g1, g2, g3):
        world.finish_joint_phase(group.group_id, PHASE_PREPARE)
    world.dispatch_due()
    assert g1.active_phase == PHASE_EXECUTE
    assert g2.active_phase == PHASE_EXECUTE
    assert g3.active_phase == PHASE_EXECUTE


def test_phases_request_different_resources_and_do_not_reserve_ahead():
    world, _ = make_world()
    group = request_phased(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: ("briefing",), PHASE_EXECUTE: ("bridge",),
         PHASE_HANDOVER: ("dock",)},
    )

    world.dispatch_due()
    assert group.state == STARTED
    assert group.active_phase == PHASE_PREPARE
    assert world.commitments.resource_holds() == {"briefing": group.group_id}
    status = world.joint_status(group.group_id)
    assert [phase["phase_id"] for phase in status["phases"]] == [
        PHASE_PREPARE, PHASE_EXECUTE, PHASE_HANDOVER,
    ]
    assert status["active_phase"] == PHASE_PREPARE
    assert status["next_phase"] == PHASE_EXECUTE
    # Members being ready for later phases never occupied those resources.
    assert "bridge" not in world.commitments.resource_holds()
    assert "dock" not in world.commitments.resource_holds()

    world.finish_joint_phase(group.group_id, PHASE_PREPARE)
    world.dispatch_due()
    assert group.active_phase == PHASE_EXECUTE
    assert world.commitments.resource_holds() == {"bridge": group.group_id}
    assert "briefing" not in world.commitments.resource_holds()


def test_wait_status_explains_member_and_resource_targets():
    world, _ = make_world()
    seen = track_events(world)
    group = request_phased(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("bridge",), PHASE_HANDOVER: ()},
        timing={
            PHASE_PREPARE: {"delay": 1},
            PHASE_EXECUTE: {"delay": 0},
            PHASE_HANDOVER: {"delay": 0},
        },
    )
    world.dispatch_due()
    status = world.joint_status(group.group_id)
    assert status["waiting_on"] == ["a1", "a2"]
    member_wait = [w for w in status["waiting_for"] if w["kind"] == "member"]
    assert {w["role"] for w in member_wait} == {"a1", "a2"}
    assert all(w["phase_id"] == PHASE_PREPARE for w in member_wait)
    assert all(w.get("available_at") == 1 for w in member_wait)

    blocker = request_phased(
        world, "solo", ("solo",),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("bridge",), PHASE_HANDOVER: ()},
    )
    world.clock.advance(1)
    world.dispatch_due()
    world.finish_joint_phase(blocker.group_id, PHASE_PREPARE)
    world.dispatch_due()
    blocker_execute = [
        e for e in seen
        if e.type == "joint_phase_started"
        and e.payload["group_id"] == blocker.group_id
    ]
    assert blocker_execute
    world.finish_joint_phase(group.group_id, PHASE_PREPARE)
    world.dispatch_due()
    waiting = world.joint_status(group.group_id)
    resource_wait = [w for w in waiting["waiting_for"] if w["kind"] == "resource"]
    assert len(resource_wait) == 1
    assert resource_wait[0]["resource"] == "bridge"
    assert resource_wait[0]["held_by_group"] == blocker.group_id
    assert resource_wait[0]["held_by_phase"] == PHASE_EXECUTE
    assert "bridge" in resource_wait[0]["reason"]
    deferred = [h for h in group.history if h["kind"] == "phase_deferred"]
    assert deferred and deferred[-1]["rule"] == RULE_RESOURCE_PRIORITY


def test_mutual_next_phase_wait_is_diagnosable_but_independent_task_runs():
    world, _ = make_world()
    seen = track_events(world)
    g1, g2, g3 = deadlock_pair(world)
    reach_handover_deadlock(world, g1, g2, g3)

    diagnosis = world.diagnose_joint_waits()
    assert diagnosis["deadlocked"] is True
    cycle = diagnosis["cycles"][0]
    assert cycle[0] == cycle[-1]
    assert set(cycle[:-1]) == {g1.group_id, g2.group_id}
    edge_groups = {(edge["from_group"], edge["to_group"])
                   for edge in diagnosis["edges"]}
    assert (g1.group_id, g2.group_id) in edge_groups
    assert (g2.group_id, g1.group_id) in edge_groups

    g1_status = world.joint_status(g1.group_id)
    assert g1_status["next_phase"] == PHASE_HANDOVER
    waits = {(w["kind"], w.get("resource")) for w in g1_status["waiting_for"]}
    assert ("resource", "Y") in waits

    # The independent task is unaffected and keeps progressing.
    assert g3.active_phase == PHASE_EXECUTE
    world.finish_joint_phase(g3.group_id, PHASE_EXECUTE)
    world.dispatch_due()
    assert g3.active_phase == PHASE_HANDOVER
    world.finish_joint_phase(g3.group_id, PHASE_HANDOVER)
    assert g3.state == COMPLETED
    assert [e for e in seen if e.type == "joint_phase_started"
            and e.payload["group_id"] == g3.group_id
            and e.payload["phase_id"] == PHASE_PREPARE]
    # Deadlocked groups remain stuck until something changes.
    assert g1.active_phase == PHASE_EXECUTE
    assert g2.active_phase == PHASE_EXECUTE


def test_cancel_unstarted_phase_breaks_cycle_without_undoing_finished_work():
    world, _ = make_world()
    seen = track_events(world)
    g1, g2, g3 = deadlock_pair(world)
    reach_handover_deadlock(world, g1, g2, g3)

    result = world.cancel_joint_phase(
        g2.group_id, PHASE_HANDOVER, by="b1", reason="交接取消"
    )
    assert result is g2
    # The running execute phase is not torn down: g2 stays STARTED and keeps
    # its execute hold until the phase is finished safely.
    assert g2.state == STARTED
    assert g2.ending_reason == f"phase_cancelled:{PHASE_HANDOVER}:交接取消"
    assert g2.end_reason is None
    # Completed/active phases stay recorded: cancellation does not undo work.
    assert g2.phase_state[PHASE_PREPARE] == COMPLETED
    assert g2.phase_state[PHASE_EXECUTE] == STARTED
    assert g2.phase_state[PHASE_HANDOVER] == CANCELLED
    world.dispatch_due()
    cancelled_event = [e for e in seen if e.type == "joint_phase_cancelled"]
    assert len(cancelled_event) == 1
    assert cancelled_event[0].payload["preserved_phases"] == [
        PHASE_PREPARE, PHASE_EXECUTE,
    ]
    assert cancelled_event[0].payload["active_phase"] == PHASE_EXECUTE
    assert cancelled_event[0].payload["active_phase_rule"] == (
        RULE_ACTIVE_PHASE_SAFE_END
    )

    # Global table and the task's own view report the same hold.
    holds = world.commitments.resource_holds()
    assert holds == {"X": g1.group_id, "Y": g2.group_id, "Z": g3.group_id}
    g2_status = world.joint_status(g2.group_id)
    assert g2_status["held_resources"] == {"Y": PHASE_EXECUTE}
    assert g2_status["ending_reason"] == g2.ending_reason
    active_wait = [w for w in g2_status["waiting_for"]
                   if w["kind"] == "active_phase"]
    assert active_wait and active_wait[0]["rule"] == RULE_ACTIVE_PHASE_SAFE_END
    # The waiting task learns the same fact about who holds Y.
    g1_status = world.joint_status(g1.group_id)
    y_wait = [w for w in g1_status["waiting_for"]
              if w["kind"] == "resource" and w["resource"] == "Y"]
    assert y_wait[0]["held_by_group"] == g2.group_id
    assert y_wait[0]["held_by_phase"] == PHASE_EXECUTE

    # Repeating the cancellation or re-dispatching adds no events and
    # changes no holds.
    again = world.cancel_joint_phase(g2.group_id, PHASE_HANDOVER, by="b1")
    assert again is None
    event_count = len(seen)
    world.dispatch_due()
    assert len(seen) == event_count
    assert world.commitments.resource_holds() == holds

    # Cannot cancel a phase that already ran.
    rejected = world.cancel_joint_phase(g2.group_id, PHASE_PREPARE, by="b1")
    assert rejected is None
    reject = [h for h in g2.history if h["kind"] == "phase_cancel_rejected"]
    assert reject[-1]["rule"] == RULE_FINISHED_WORK_IMMUTABLE

    # g2's execute phase runs to its safe end, releasing Y exactly once;
    # only then does the group end with the recorded cancellation reason.
    world.finish_joint_phase(g2.group_id, PHASE_EXECUTE)
    assert g2.state == ENDED
    assert g2.end_reason == f"phase_cancelled:{PHASE_HANDOVER}:交接取消"
    assert g2.active_phase is None
    assert "Y" not in world.commitments.resource_holds()
    world.dispatch_due()
    ended_events = [e for e in seen if e.type == "joint_commitment_ended"
                    and e.payload["group_id"] == g2.group_id]
    assert len(ended_events) == 1

    world.finish_joint_phase(g1.group_id, PHASE_EXECUTE)
    world.dispatch_due()
    assert g1.active_phase == PHASE_HANDOVER
    holds = world.commitments.resource_holds()
    assert holds == {"Y": g1.group_id, "Z": g3.group_id}
    world.finish_joint_phase(g1.group_id, PHASE_HANDOVER)
    assert g1.state == COMPLETED
    diagnosis = world.diagnose_joint_waits()
    assert diagnosis["cycles"] == []


def test_restored_wait_state_keeps_promises_events_and_notes_without_rerun():
    world, runtimes = make_world()
    seen = track_events(world)
    g1, g2, g3 = deadlock_pair(world)
    reach_handover_deadlock(world, g1, g2, g3)

    archive = WorldArchive.capture(world, runtimes, checkpoint_id="deadlocked")
    pre_started = [
        (e.type, e.sequence, e.payload.get("phase_id"))
        for e in seen if e.type == "joint_phase_started"
    ]
    policies = {a: TargetPolicy() for a in ACTORS}
    types = {a: [MoveToTarget] for a in ACTORS}

    restored, restored_runtimes = archive.restore(policies, action_types=types)
    restored_seen = track_events(restored)
    r1 = restored.commitments.get_group(g1.group_id)
    r2 = restored.commitments.get_group(g2.group_id)
    r3 = restored.commitments.get_group(g3.group_id)

    assert r1.active_phase == PHASE_EXECUTE
    assert r1.phase_state[PHASE_PREPARE] == COMPLETED
    assert r2.active_phase == PHASE_EXECUTE
    diagnosis = restored.diagnose_joint_waits()
    assert diagnosis["deadlocked"] is True
    assert set(diagnosis["cycles"][0][:-1]) == {g1.group_id, g2.group_id}
    assert restored.commitments.resource_holds() == {
        "X": g1.group_id, "Y": g2.group_id, "Z": g3.group_id,
    }
    # Already-executed events must not fire again after restore + settle.
    restored.dispatch_due()
    assert [e.type for e in restored_seen] == []

    # Cancelling on the restored branch keeps the active phase running to
    # its safe end; finishing it unblocks the other group exactly once.
    restored.cancel_joint_phase(r2.group_id, PHASE_HANDOVER, by="b1")
    assert r2.state == STARTED
    assert r2.ending_reason == f"phase_cancelled:{PHASE_HANDOVER}"
    restored.dispatch_due()
    assert restored.commitments.resource_holds()["Y"] == r2.group_id
    restored.finish_joint_phase(r2.group_id, PHASE_EXECUTE)
    assert r2.state == ENDED
    assert r2.end_reason == f"phase_cancelled:{PHASE_HANDOVER}"
    restored.finish_joint_phase(r1.group_id, PHASE_EXECUTE)
    restored.dispatch_due()
    assert r1.active_phase == PHASE_HANDOVER
    handover_starts = [
        e for e in restored_seen if e.type == "joint_phase_started"
        and e.payload["phase_id"] == PHASE_HANDOVER
        and e.payload["group_id"] == r1.group_id
    ]
    assert len(handover_starts) == 1
    handover_notes = {m["role"] for m in handover_starts[0].payload["members"]}
    assert handover_notes == {"a1", "a2"}
    restored.finish_joint_phase(r1.group_id, PHASE_HANDOVER)
    restored.dispatch_due()
    assert r1.state == COMPLETED

    # The original world and its branch stay independent.
    assert g1.active_phase == PHASE_EXECUTE
    assert world.commitments.resource_holds()["X"] == g1.group_id
    assert pre_started  # original history still intact
    assert restored_runtimes["a1"].world is restored

def reach_execute(world, group):
    world.dispatch_due()
    assert group.active_phase == PHASE_PREPARE
    world.finish_joint_phase(group.group_id, PHASE_PREPARE)
    world.dispatch_due()
    assert group.active_phase == PHASE_EXECUTE


def test_cancel_after_prepare_finished_ends_group_with_no_holds():
    world, _ = make_world()
    seen = track_events(world)
    group = request_phased(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: ("briefing",), PHASE_EXECUTE: ("bridge",),
         PHASE_HANDOVER: ("dock",)},
    )
    world.dispatch_due()
    world.finish_joint_phase(group.group_id, PHASE_PREPARE)
    # Prepare is done, execute has not started: nothing is held.
    assert world.commitments.resource_holds() == {}

    result = world.cancel_joint_phase(
        group.group_id, PHASE_EXECUTE, by="a1", reason="计划变更"
    )
    assert result is group
    assert group.state == ENDED
    assert group.end_reason == f"phase_cancelled:{PHASE_EXECUTE}:计划变更"
    assert group.ending_reason is None
    assert group.phase_state[PHASE_PREPARE] == COMPLETED
    assert group.phase_state[PHASE_EXECUTE] == CANCELLED
    assert group.phase_state[PHASE_HANDOVER] == CANCELLED
    assert world.commitments.resource_holds() == {}

    world.dispatch_due()
    cancelled = [e for e in seen if e.type == "joint_phase_cancelled"]
    assert len(cancelled) == 1
    assert cancelled[0].payload["cancelled_phases"] == [
        PHASE_EXECUTE, PHASE_HANDOVER,
    ]
    assert cancelled[0].payload["active_phase"] is None

    # Repeated cancellation and repeated dispatch add no events.
    assert world.cancel_joint_phase(group.group_id, PHASE_EXECUTE) is None
    event_count = len(seen)
    world.dispatch_due()
    world.dispatch_due()
    assert len(seen) == event_count
    # Member history of the finished phase is preserved.
    kinds = {h["kind"] for h in group.history}
    assert "phase_finished" in kinds and "phase_cancelled" in kinds


def test_cancel_while_handover_waiting_preserves_finished_execute():
    world, _ = make_world()
    seen = track_events(world)
    group = request_phased(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("bridge",),
         PHASE_HANDOVER: ("dock",)},
    )
    reach_execute(world, group)
    assert world.commitments.resource_holds() == {"bridge": group.group_id}
    world.finish_joint_phase(group.group_id, PHASE_EXECUTE)
    # Execute finished, handover not started: execute hold already released.
    assert world.commitments.resource_holds() == {}

    result = world.cancel_joint_phase(group.group_id, PHASE_HANDOVER, by="a2")
    assert result is group
    assert group.state == ENDED
    assert group.end_reason == f"phase_cancelled:{PHASE_HANDOVER}"
    assert group.phase_state[PHASE_EXECUTE] == COMPLETED
    assert group.phase_state[PHASE_HANDOVER] == CANCELLED
    assert world.commitments.resource_holds() == {}
    world.dispatch_due()
    assert len([e for e in seen if e.type == "joint_phase_cancelled"]) == 1
    status = world.joint_status(group.group_id)
    assert status["held_resources"] == {}


def test_cancel_during_execute_keeps_hold_until_safe_end_across_restore():
    world, runtimes = make_world()
    seen = track_events(world)
    group = request_phased(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("bridge",),
         PHASE_HANDOVER: ("dock",)},
    )
    other = request_phased(
        world, "solo", ("solo",),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("bridge",), PHASE_HANDOVER: ()},
    )
    reach_execute(world, group)

    # Snapshot before the cancellation, then cancel with execute running.
    before = WorldArchive.capture(world, runtimes, checkpoint_id="pre-cancel")
    world.cancel_joint_phase(group.group_id, PHASE_HANDOVER, by="a1",
                             reason="交接取消")
    assert group.state == STARTED
    assert group.ending_reason == f"phase_cancelled:{PHASE_HANDOVER}:交接取消"
    assert world.commitments.resource_holds() == {"bridge": group.group_id}

    # Snapshot after the cancellation; both branches must agree on the hold.
    after = WorldArchive.capture(world, runtimes, checkpoint_id="post-cancel")
    policies = {a: TargetPolicy() for a in ACTORS}
    types = {a: [MoveToTarget] for a in ACTORS}

    restored, _ = after.restore(policies, action_types=types)
    r_seen = track_events(restored)
    r_group = restored.commitments.get_group(group.group_id)
    assert r_group.state == STARTED
    assert r_group.ending_reason == group.ending_reason
    assert restored.commitments.resource_holds() == {"bridge": r_group.group_id}
    # Re-dispatch on the restored branch only delivers the cancellation
    # event that was already published before the snapshot; nothing refires.
    restored.dispatch_due()
    assert [e.type for e in r_seen] == ["joint_phase_cancelled"]
    restored.dispatch_due()
    assert len([e for e in r_seen if e.type == "joint_phase_cancelled"]) == 1
    assert restored.commitments.resource_holds() == {"bridge": r_group.group_id}
    # The active phase ends safely exactly once, then the group ends.
    restored.finish_joint_phase(r_group.group_id, PHASE_EXECUTE)
    restored.dispatch_due()
    assert r_group.state == ENDED
    assert r_group.end_reason == group.ending_reason
    assert restored.commitments.resource_holds() == {}
    assert len([e for e in r_seen if e.type == "joint_commitment_ended"]) == 1

    # The pre-cancel branch is unaffected and can still run to completion.
    pre, _ = before.restore(policies, action_types=types)
    p_group = pre.commitments.get_group(group.group_id)
    assert p_group.ending_reason is None
    assert p_group.phase_state[PHASE_HANDOVER] == WAITING
    assert pre.commitments.resource_holds() == {"bridge": p_group.group_id}

    # The independent task keeps waiting on the same fact, then proceeds
    # once the holder's execute phase reaches its safe end.
    other_status = world.joint_status(other.group_id)
    blocker = [w for w in other_status["waiting_for"]
               if w["kind"] == "resource" and w["resource"] == "bridge"]
    assert blocker and blocker[0]["held_by_group"] == group.group_id
    assert blocker[0]["held_by_phase"] == PHASE_EXECUTE
    world.finish_joint_phase(group.group_id, PHASE_EXECUTE)
    assert group.state == ENDED
    world.dispatch_due()
    reach_execute(world, other)
    assert world.commitments.resource_holds() == {"bridge": other.group_id}
