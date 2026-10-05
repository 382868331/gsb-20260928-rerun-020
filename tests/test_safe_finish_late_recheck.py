from __future__ import annotations

from tests.test_phase_reschedule import (
    advance_group,
    make_world,
    request,
    track,
)
from behavior_runtime.commitments import (
    COMPLETED,
    PHASE_EXECUTE,
    PHASE_HANDOVER,
    PHASE_PREPARE,
    STARTED,
)


def test_late_safe_finish_rechecks_stage_progress_holds_and_final_release():
    world, _ = make_world(("a1", "a2", "a3", "b1", "b2"))
    seen = track(world)
    group = request(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("bridge",),
         PHASE_HANDOVER: ("dock",)},
        capabilities={"a2": ("certified",)},
    )
    other = request(
        world, "b1", ("b1", "b2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: (),
         PHASE_HANDOVER: ("dock",)},
    )
    advance_group(world, group, PHASE_PREPARE)
    advance_group(world, other, PHASE_PREPARE)
    advance_group(world, other, PHASE_EXECUTE)
    # The other task holds the dock in its handover while group is
    # executing and holding its bridge.
    assert world.commitments.resource_holds() == {
        "bridge": group.group_id, "dock": other.group_id,
    }

    # Member exits during execute: the running phase keeps the bridge and
    # runs toward its (late) safe finish; the user allows a delayed handover.
    world.remove_actor("a2")
    world.dispatch_due()
    world.offer_joint_phase_reschedule(
        group.group_id, PHASE_HANDOVER, by="a1",
        member_options=[
            {"role": "a2", "performer_id": "a3",
             "capabilities": ("certified",), "delay": 0},
        ],
    )
    assert group.active_phase == PHASE_EXECUTE
    assert world.commitments.resource_holds() == {
        "bridge": group.group_id, "dock": other.group_id,
    }

    # Late safe finish of execute: bridge is released, but dock is still
    # held; repeated dispatch must never borrow it early or re-run phases.
    world.finish_joint_phase(group.group_id, PHASE_EXECUTE)
    started_counts = []
    for _ in range(3):
        world.dispatch_due()
        started_counts.append(len([
            e for e in seen if e.type == "joint_phase_started"
            and e.payload.get("group_id") == group.group_id
        ]))
    assert started_counts == [2, 2, 2]
    assert group.state == STARTED
    assert group.active_phase is None
    assert group.phase_state[PHASE_PREPARE] == COMPLETED
    assert group.phase_state[PHASE_EXECUTE] == COMPLETED
    assert world.commitments.resource_holds() == {"dock": other.group_id}
    status = world.joint_status(group.group_id)
    handover = status["phases"][2]
    assert handover["state"] == "waiting"
    assert handover["missing_roles"] == []
    blocker = [item for item in status["waiting_for"]
               if item["kind"] == "resource"]
    assert blocker[0]["resource"] == "dock"

    # Only the real release lets the delayed handover adopt, once.
    world.finish_joint_phase(other.group_id, PHASE_HANDOVER)
    world.dispatch_due()
    assert group.active_phase == PHASE_HANDOVER
    assert world.commitments.resource_holds() == {"dock": group.group_id}
    started = [e for e in seen if e.type == "joint_phase_started"
               and e.payload.get("group_id") == group.group_id
               and e.payload["phase_id"] == PHASE_HANDOVER]
    assert len(started) == 1
    assert started[0].payload["reschedule_adopted"]["members"] == ["a1", "a3"]
    world.finish_joint_phase(group.group_id, PHASE_HANDOVER)
    assert group.state == COMPLETED
    assert other.state == COMPLETED
    assert world.commitments.resource_holds() == {}
