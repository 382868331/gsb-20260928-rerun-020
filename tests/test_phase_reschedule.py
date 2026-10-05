from __future__ import annotations

from behavior_runtime import VirtualClock, World, WorldArchive, ActorRuntime
from behavior_runtime.commitments import (
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


PHASES = (PHASE_PREPARE, PHASE_EXECUTE, PHASE_HANDOVER)
TIMING = {phase: {"delay": 0} for phase in PHASES}


def make_world(actors=()):
    world = World({}, clock=VirtualClock())
    runtimes = {
        actor_id: ActorRuntime(
            actor_id, TargetPolicy(), world, action_types=[MoveToTarget]
        )
        for actor_id in actors
    }
    for actor_id in actors:
        world.register_actor(actor_id)
    return world, runtimes


def request(world, requester, members, phase_resources, *, capabilities=None):
    capabilities = capabilities or {}
    return world.request_phased_joint_commitment(
        requester_id=requester,
        action="TransferCargo",
        participants=[
            {
                "role": member,
                "performer_id": member,
                "capabilities": capabilities.get(member, ()),
                "phases": {phase: dict(TIMING[phase]) for phase in PHASES},
            }
            for member in members
        ],
        phases=PHASES,
        phase_resources=phase_resources,
    )


def track(world):
    events = []
    world.events.subscribe(lambda event: events.append(event))
    return events


def advance_group(world, group, phase):
    world.dispatch_due()
    assert group.active_phase == phase
    world.finish_joint_phase(group.group_id, phase)
    world.dispatch_due()


def test_two_tasks_contend_for_handover_device_reschedule_waits_for_release():
    world, _ = make_world(("a1", "a2", "b1", "b2"))
    seen = track(world)
    # Both tasks want the same single handover device "dock".
    g1 = request(world, "a1", ("a1", "a2"),
                 {PHASE_PREPARE: (), PHASE_EXECUTE: ("bridge"),
                  PHASE_HANDOVER: ("dock",)})
    g2 = request(world, "b1", ("b1", "b2"),
                 {PHASE_PREPARE: (), PHASE_EXECUTE: ("crane"),
                  PHASE_HANDOVER: ("dock",)})

    advance_group(world, g1, PHASE_PREPARE)
    advance_group(world, g2, PHASE_PREPARE)
    advance_group(world, g1, PHASE_EXECUTE)
    # g2 is still executing; cancel its handover, then opt into a delayed
    # handover that would reuse "dock".
    world.cancel_joint_phase(g2.group_id, PHASE_HANDOVER, by="b1")
    offered = world.offer_joint_phase_reschedule(
        g2.group_id, PHASE_HANDOVER, by="b1",
        reason="dock delayed", delay=0,
        resource_options=[("dock",)],
    )
    assert offered is g2
    assert g2.phase_state[PHASE_HANDOVER] == WAITING
    # g2 finishes execute and wants the handover, but dock is not free and
    # side-dock does not exist as a usable alternative here either; the
    # candidate bundle cannot borrow the occupied dock.
    world.finish_joint_phase(g2.group_id, PHASE_EXECUTE)
    world.dispatch_due()
    assert g2.state == STARTED
    assert g2.active_phase is None
    assert world.commitments.resource_holds() == {"dock": g1.group_id}
    status = world.joint_status(g2.group_id)
    blockers = [item for item in status["waiting_for"]
                if item["kind"] == "resource"]
    assert blockers and blockers[0]["resource"] == "dock"
    assert blockers[0]["held_by_group"] == g1.group_id
    record = [h for h in g2.history if h["kind"] == "phase_deferred"][-1]
    assert record["rule"] == RULE_RESCHEDULE_RECHECK
    # No fake end while the continuation is merely unavailable.
    assert not [e for e in seen if e.type == "joint_commitment_ended"]

    # g1 finishes its handover and frees the dock: same dispatch re-checks
    # the real hold table and adopts the delayed handover for g2.
    world.finish_joint_phase(g1.group_id, PHASE_HANDOVER)
    world.dispatch_due()
    assert g1.state == COMPLETED
    assert g2.active_phase == PHASE_HANDOVER
    assert world.commitments.resource_holds() == {"dock": g2.group_id}
    adopted = g2.reschedule_offers[PHASE_HANDOVER]["adopted"]
    assert adopted["resources"] == ["dock"]
    world.finish_joint_phase(g2.group_id, PHASE_HANDOVER)
    assert g2.state == COMPLETED
    assert world.commitments.resource_holds() == {}


def test_executing_member_exits_keeps_running_phase_and_takes_substitute():
    world, _ = make_world(("a1", "a2", "a3"))
    seen = track(world)
    group = request(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("bridge",),
         PHASE_HANDOVER: ("dock",)},
        capabilities={"a2": ("certified",)},
    )
    advance_group(world, group, PHASE_PREPARE)
    assert group.active_phase == PHASE_EXECUTE
    assert world.commitments.resource_holds() == {"bridge": group.group_id}

    # a2 leaves while execute is running. The execute phase must keep its
    # hold and run to safe finish; only unstarted promises are revoked.
    world.remove_actor("a2")
    world.dispatch_due()
    assert group.state == STARTED
    assert group.active_phase == PHASE_EXECUTE
    assert world.commitments.resource_holds() == {"bridge": group.group_id}
    assert group.phase_state[PHASE_EXECUTE] == STARTED
    assert group.phase_state[PHASE_PREPARE] == COMPLETED
    handover_cid = group.phase_commitments[PHASE_HANDOVER][1]
    assert world.commitments.get(handover_cid).end_reason == (
        f"participant_lost:a2:{PHASE_HANDOVER}"
    )
    lost = [e for e in seen if e.type == "joint_member_lost"]
    assert len(lost) == 1
    assert lost[0].payload["missing_by_phase"][PHASE_HANDOVER] == ["a2"]

    # Finishing execute without any delayed continuation ends honestly at
    # the safe boundary instead of pretending the handover completed.
    world.finish_joint_phase(group.group_id, PHASE_EXECUTE)
    assert group.state == ENDED
    assert group.end_reason.startswith(f"member_unavailable:{PHASE_HANDOVER}")
    assert world.commitments.resource_holds() == {}
    world.dispatch_due()
    ended = [e for e in seen if e.type == "joint_commitment_ended"]
    assert len(ended) == 1
    assert ended[0].payload["safe_finish_phase"] == PHASE_EXECUTE

    # Same scenario, but this time the user opts into a delayed handover
    # with a capable substitute member before execute finishes.
    world2, _ = make_world(("a1", "a2", "a3"))
    group2 = request(
        world2, "a1", ("a1", "a2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("bridge",),
         PHASE_HANDOVER: ("dock",)},
        capabilities={"a2": ("certified",)},
    )
    advance_group(world2, group2, PHASE_PREPARE)
    world2.remove_actor("a2")
    world2.dispatch_due()
    offered = world2.offer_joint_phase_reschedule(
        group2.group_id, PHASE_HANDOVER, by="a1",
        member_options=[
            {"role": "a2", "performer_id": "a3",
             "capabilities": ("certified",), "delay": 1},
        ],
    )
    assert offered is group2
    world2.finish_joint_phase(group2.group_id, PHASE_EXECUTE)
    world2.dispatch_due()
    # Substitute not ready yet: still waiting, holds remain released, no end.
    assert group2.state == STARTED
    assert group2.active_phase is None
    assert world2.commitments.resource_holds() == {}
    world2.clock.advance(1)
    world2.dispatch_due()
    assert group2.active_phase == PHASE_HANDOVER
    assert world2.commitments.resource_holds() == {"dock": group2.group_id}
    status = world2.joint_status(group2.group_id)
    phase = status["phases"][2]
    replacement = [m for m in phase["members"] if m["replacement"]]
    assert replacement[0]["participant_id"] == "a3"
    world2.finish_joint_phase(group2.group_id, PHASE_HANDOVER)
    assert group2.state == COMPLETED
    assert world2.commitments.resource_holds() == {}


def test_incapable_candidate_is_rejected_and_restore_keeps_single_adoption():
    world, _ = make_world(("a1", "a2", "a3"))
    group = request(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: (), PHASE_HANDOVER: ("dock",)},
        capabilities={"a2": ("certified",)},
    )
    advance_group(world, group, PHASE_PREPARE)
    world.remove_actor("a2")
    rejected = world.offer_joint_phase_reschedule(
        group.group_id, PHASE_HANDOVER,
        member_options=[
            {"role": "a2", "performer_id": "a3",
             "capabilities": (), "delay": 0},
        ],
    )
    assert rejected is None
    assert PHASE_HANDOVER not in group.reschedule_offers


def test_partial_cancel_and_reschedule_survive_archive_branching():
    world, runtimes = make_world(("a1", "a2", "a3", "b1"))
    group = request(
        world, "a1", ("a1", "a2"),
        {PHASE_PREPARE: (), PHASE_EXECUTE: ("bridge",),
         PHASE_HANDOVER: ("dock",)},
        capabilities={"a2": ("certified",)},
    )
    other = request(
        world, "b1", ("b1",),
        {PHASE_PREPARE: (), PHASE_EXECUTE: (), PHASE_HANDOVER: ("dock",)},
    )
    advance_group(world, group, PHASE_PREPARE)
    advance_group(world, other, PHASE_PREPARE)
    advance_group(world, other, PHASE_EXECUTE)
    world.remove_actor("a2")
    world.offer_joint_phase_reschedule(
        group.group_id, PHASE_HANDOVER, by="a1",
        member_options=[
            {"role": "a2", "performer_id": "a3",
             "capabilities": ("certified",), "delay": 0},
        ],
    )
    archive = WorldArchive.capture(world, runtimes, checkpoint_id="waiting")
    policies = {a: TargetPolicy() for a in ("a1", "a2", "a3", "b1")}
    types = {a: [MoveToTarget] for a in ("a1", "a2", "a3", "b1")}
    restored, restored_runtimes = archive.restore(policies, action_types=types)
    seen = track(restored)
    rg = restored.commitments.get_group(group.group_id)
    ro = restored.commitments.get_group(other.group_id)
    restored.finish_joint_phase(rg.group_id, PHASE_EXECUTE)
    restored.dispatch_due()
    assert rg.state == STARTED and rg.active_phase is None
    restored.finish_joint_phase(ro.group_id, PHASE_HANDOVER)
    restored.dispatch_due()
    assert rg.active_phase == PHASE_HANDOVER
    restored.finish_joint_phase(rg.group_id, PHASE_HANDOVER)
    restored.dispatch_due()
    restored.dispatch_due()
    assert rg.state == COMPLETED
    assert restored.commitments.resource_holds() == {}
    started = [e for e in seen if e.type == "joint_phase_started"
               and e.payload["phase_id"] == PHASE_HANDOVER
               and e.payload["group_id"] == rg.group_id]
    assert len(started) == 1
    assert started[0].payload["reschedule_adopted"]["members"] == ["a1", "a3"]
    # Original world is untouched by the branch.
    assert group.state == STARTED
    assert group.active_phase == PHASE_EXECUTE
