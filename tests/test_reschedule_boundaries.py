from __future__ import annotations

from tests.test_phase_reschedule import (
    advance_group,
    make_world,
    request,
    track,
)
from behavior_runtime.commitments import (
    ENDED,
    PHASE_EXECUTE,
    PHASE_HANDOVER,
    PHASE_PREPARE,
)


def test_expired_reschedule_candidate_ends_honestly_after_safe_finish():
    world, _ = make_world(("a1", "a2", "a3"))
    group = request(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("bridge",),
         PHASE_HANDOVER: ("dock",)},
        capabilities={"a2": ("certified",)},
    )
    advance_group(world, group, PHASE_PREPARE)
    world.remove_actor("a2")
    world.dispatch_due()
    # Substitute is ready only between t=1 and t=3, but execute finishes late.
    world.offer_joint_phase_reschedule(
        group.group_id, PHASE_HANDOVER, by="a1",
        member_options=[
            {"role": "a2", "performer_id": "a3",
             "capabilities": ("certified",),
             "delay": 1, "valid_for": 2},
        ],
    )
    world.clock.advance(4)
    world.dispatch_due()
    # Execute is still running; the late safe finish finds no live promise.
    world.finish_joint_phase(group.group_id, PHASE_EXECUTE)
    world.dispatch_due()
    assert group.state == ENDED
    assert group.end_reason.startswith(f"member_unavailable:{PHASE_HANDOVER}")
    assert world.commitments.resource_holds() == {}


def test_member_removed_between_phases_ends_immediately():
    world, _ = make_world(("a1", "a2", "solo"))
    seen = track(world)
    group = request(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: (), PHASE_HANDOVER: ("dock",)},
    )
    holder = request(
        world, "solo", ("solo",),
        {PHASE_PREPARE: (), PHASE_EXECUTE: (), PHASE_HANDOVER: ("dock",)},
    )
    advance_group(world, group, PHASE_PREPARE)
    advance_group(world, holder, PHASE_PREPARE)
    advance_group(world, holder, PHASE_EXECUTE)
    advance_group(world, group, PHASE_EXECUTE)
    assert group.active_phase is None
    assert world.commitments.resource_holds() == {"dock": holder.group_id}
    world.remove_actor("a2")
    world.dispatch_due()
    assert group.state == ENDED
    assert group.end_reason == "participant_lost:a2"
    assert world.commitments.resource_holds() == {"dock": holder.group_id}
    assert [e for e in seen if e.type == "joint_commitment_ended"]
