from behavior_runtime import (
    ActorRuntime,
    VirtualClock,
    World,
    WorldArchive,
)
from behavior_runtime.examples import MoveToTarget, TargetPolicy
from behavior_runtime.groups import (
    RULE_ALL_READY_BEFORE_START,
    RULE_GROUP_EXPIRY_BEFORE_START,
    RULE_GROUP_REMOVAL_FIRST,
    RULE_LOSER_RELEASES,
    RULE_RESOURCE_ORDER,
    RULE_SUPERSEDED_PRESERVED,
)


MEMBERS = [
    {"actor_id": "carrier", "role": "supply", "action": "Deliver"},
    {"actor_id": "gate", "role": "passage", "action": "OpenGate"},
    {"actor_id": "receiver", "role": "receive", "action": "Receive"},
]


def make_world():
    world = World({}, clock=VirtualClock())
    for actor_id in ("carrier", "gate", "receiver", "spare"):
        ActorRuntime(actor_id, TargetPolicy(), world, action_types=[MoveToTarget])
    return world


def test_group_waits_until_all_members_ready_and_shows_status():
    world = make_world()
    group = world.request_joint_group(
        action="Convoy", members=MEMBERS,
        resources=("route-a",), requester_id="carrier",
    )
    assert group.state == "waiting"

    world.mark_joint_member_ready(group.group_id, "carrier")
    world.clock.advance(1)
    world.mark_joint_member_ready(group.group_id, "gate")
    status = world.joint_group_status(group.group_id)
    assert status["waiting_on"] == ["receiver"]
    assert status["satisfied"] == ["carrier", "gate"]
    assert status["prepared_resources"] == ["route-a"]

    # Individual readiness never fires the joint action early.
    world.clock.advance(1)
    world.dispatch_due()
    assert group.state == "waiting"
    assert world.resource_status() == [
        {"resource_id": "route-a", "state": "provisional", "group_id": group.group_id}
    ]

    world.mark_joint_member_ready(group.group_id, "receiver")
    world.dispatch_due()
    assert group.state == "started"
    assert group.started_at == 2
    assert group.decision_id == f"decision-{group.group_id}"
    started_record = group.history[-1]
    assert started_record["rule"] == RULE_ALL_READY_BEFORE_START
    assert world.resource_status() == [
        {"resource_id": "route-a", "state": "held", "group_id": group.group_id}
    ]


def test_single_member_expiry_does_not_start_and_releases_resources():
    world = make_world()
    group = world.request_joint_group(
        action="Convoy", members=MEMBERS,
        resources=("route-a",), requester_id="carrier",
        member_valid_for=5,
    )
    world.mark_joint_member_ready(group.group_id, "carrier")
    world.clock.advance(6)
    world.dispatch_due()
    assert group.state == "expired"
    assert group.end_reason == "member_commitment_expired:gate"
    assert group.history[-1]["rule"] == RULE_GROUP_EXPIRY_BEFORE_START
    assert world.resource_status() == []


def test_group_validity_expires_even_if_ready():
    world = make_world()
    group = world.request_joint_group(
        action="Convoy", members=MEMBERS[:2],
        resources=("route-a",), requester_id="carrier", valid_for=3,
    )
    world.mark_joint_member_ready(group.group_id, "carrier")
    world.mark_joint_member_ready(group.group_id, "gate")
    world.clock.advance(3)
    world.dispatch_due()
    assert group.state == "expired"
    assert world.resource_status() == []


def test_started_group_cannot_be_withdrawn_and_fires_once():
    world = make_world()
    seen = []
    world.events.subscribe(lambda event: seen.append(event))
    group = world.request_joint_group(
        action="Convoy", members=MEMBERS[:2], requester_id="carrier",
    )
    for actor_id in ("carrier", "gate"):
        world.mark_joint_member_ready(group.group_id, actor_id)
    world.dispatch_due()
    assert group.state == "started"

    assert not world.withdraw_joint_group(group.group_id, by="carrier")
    assert group.state == "started"
    assert group.history[-1]["kind"] == "withdrawal_rejected"

    world.dispatch_due()
    world.dispatch_due()
    started = [e for e in seen if e.type == "joint_group_started"]
    assert len(started) == 1


def test_resource_contention_earliest_request_wins_loser_releases():
    world = make_world()
    first = world.request_joint_group(
        action="Convoy", members=MEMBERS[:2],
        resources=("route-a",), requester_id="carrier",
    )
    world.clock.advance(1)
    second = world.request_joint_group(
        action="OtherConvoy",
        members=[
            {"actor_id": "carrier", "role": "supply", "action": "Deliver2"},
            {"actor_id": "spare", "role": "receive", "action": "Receive2"},
        ],
        resources=("route-a",), requester_id="carrier",
    )

    for actor_id in ("carrier", "gate"):
        world.mark_joint_member_ready(first.group_id, actor_id)
    for actor_id in ("carrier", "spare"):
        world.mark_joint_member_ready(second.group_id, actor_id)

    world.dispatch_due()
    assert first.state == "started"
    # The loser stays waiting and may compete again later; nothing is occupied.
    assert second.state == "waiting"
    assert world.joint_groups.resources.holder("route-a") == first.group_id
    rules = [
        entry["rule"]
        for entry in second.history
        if entry["kind"] == "resource_lost"
    ]
    assert rules == [RULE_RESOURCE_ORDER]
    release = next(e for e in second.history if e["kind"] == "resources_released")
    assert release["rule"] == RULE_LOSER_RELEASES

    # Once the winner completes and frees the resource, the loser can start.
    world.complete_joint_group(first.group_id)
    assert world.resource_status() == []
    world.dispatch_due()
    assert second.state == "started"


def test_withdraw_abandons_group_and_frees_resources():
    world = make_world()
    group = world.request_joint_group(
        action="Convoy", members=MEMBERS,
        resources=("route-a",), requester_id="carrier",
    )
    assert world.withdraw_joint_group(
        group.group_id, by="carrier", reason="weather",
    )
    assert group.state == "withdrawn"
    assert group.withdrawal_reason == "weather"
    assert group.end_reason == "withdrawn_by:carrier"
    assert world.resource_status() == []
    world.dispatch_due()
    assert group.state == "withdrawn"


def test_replacement_proposal_preserves_original_and_isolates_notes():
    world = make_world()
    original = world.request_joint_group(
        action="Convoy", members=MEMBERS,
        resources=("route-a",), requester_id="carrier",
    )
    world.mark_joint_member_ready(original.group_id, "carrier")
    world.set_group_private_note(original.group_id, "gate", "code", "old-code")
    assert world.withdraw_joint_group(
        original.group_id, by="gate", reason="gate closed for maintenance"
    )

    replacement_members = [
        {"actor_id": "carrier", "role": "supply", "action": "Deliver"},
        {"actor_id": "spare", "role": "passage", "action": "OpenGate"},
        {"actor_id": "receiver", "role": "receive", "action": "Receive"},
    ]
    replacement = world.propose_joint_replacement(
        original.group_id,
        action="Convoy",
        members=replacement_members,
        resources=("route-b",),
        requester_id="carrier",
        reason="use spare gate",
        private_notes={"spare": {"code": "new-code"}},
    )
    # Original promise and its withdrawal reason remain available.
    assert original.state == "withdrawn"
    assert original.withdrawal_reason == "gate closed for maintenance"
    assert any(entry["kind"] == "requested" for entry in original.history)
    assert replacement.supersedes == original.group_id
    assert replacement.cause["replaces_group"] == original.group_id
    # Old provisional resource was released; the new proposal holds its own.
    assert world.joint_groups.resources.holder("route-a") is None
    assert world.joint_groups.resources.holder("route-b") == replacement.group_id
    # Private memories of the two proposals do not leak across alternatives.
    assert world.get_group_private_note(original.group_id, "gate", "code") == "old-code"
    assert world.get_group_private_note(replacement.group_id, "spare", "code") == "new-code"
    assert world.get_group_private_note(replacement.group_id, "gate", "code") is None

    # A started joint decision can never be replaced.
    started_group = world.request_joint_group(
        action="Convoy3", members=MEMBERS[:2], requester_id="carrier",
    )
    for actor_id in ("carrier", "gate"):
        world.mark_joint_member_ready(started_group.group_id, actor_id)
    world.dispatch_due()
    try:
        world.propose_joint_replacement(
            started_group.group_id, action="X", members=MEMBERS[:2],
            requester_id="carrier",
        )
    except ValueError:
        pass
    else:
        raise AssertionError("replacement of a started group must fail")


def test_waiting_group_replaced_is_marked_superseded():
    world = make_world()
    original = world.request_joint_group(
        action="Convoy", members=MEMBERS[:2],
        resources=("route-a",), requester_id="carrier",
    )
    replacement = world.propose_joint_replacement(
        original.group_id, action="Convoy", members=MEMBERS[:2],
        resources=("route-b",), requester_id="carrier", reason="reroute",
    )
    assert original.state == "superseded"
    assert original.end_reason == f"superseded_by:{replacement.group_id}"
    assert original.history[-1]["rule"] == RULE_SUPERSEDED_PRESERVED
    assert world.joint_groups.resources.holder("route-a") is None
    assert world.joint_groups.resources.holder("route-b") == replacement.group_id


def test_participant_leaving_abandons_group_and_frees_resources():
    world = make_world()
    group = world.request_joint_group(
        action="Convoy", members=MEMBERS,
        resources=("route-a",), requester_id="carrier",
    )
    world.mark_joint_member_ready(group.group_id, "carrier")
    world.remove_actor("gate")
    assert group.state == "abandoned"
    assert group.end_reason == "participant_lost:gate"
    assert group.history[-1]["rule"] == RULE_GROUP_REMOVAL_FIRST
    assert world.resource_status() == []
    # Leaving during dispatch (via event handlers) also settles at batch end.
    second = world.request_joint_group(
        action="Convoy2",
        members=[
            {"actor_id": "carrier", "role": "supply", "action": "D"},
            {"actor_id": "receiver", "role": "receive", "action": "R"},
        ],
        resources=("route-b",), requester_id="carrier",
    )
    world.events.subscribe(
        lambda event: world.remove_actor("receiver")
        if event.type == "joint_member_ready" else None
    )
    world.mark_joint_member_ready(second.group_id, "carrier")
    world.dispatch_due()
    assert second.state == "abandoned"
    assert world.resource_status() == []


def test_followup_commitment_remembers_which_joint_decision_caused_it():
    world = make_world()
    seen = []
    world.events.subscribe(lambda event: seen.append(event))
    group = world.request_joint_group(
        action="Convoy", members=MEMBERS[:2], requester_id="carrier",
    )
    for actor_id in ("carrier", "gate"):
        world.mark_joint_member_ready(group.group_id, actor_id)
    world.dispatch_due()
    started = next(e for e in seen if e.type == "joint_group_started")

    follow_up = world.request_commitment(
        requester_id="gate", performer_id="carrier", action="ReturnTrip", delay=1,
        cause={
            "joint_decision_id": started.payload["decision_id"],
            "joint_group_id": started.payload["group_id"],
            "event_sequence": started.sequence,
        },
    )
    assert follow_up.cause["joint_decision_id"] == group.decision_id
    assert follow_up.cause["joint_group_id"] == group.group_id
    world.clock.advance(1)
    world.dispatch_due()
    assert follow_up.state == "started"


def _world_state(world, runtimes):
    return (
        world.snapshot(),
        {aid: rt.snapshot().to_dict() for aid, rt in runtimes.items()},
    )


def _restore(archive):
    policies = {aid: TargetPolicy() for aid in
                ("carrier", "gate", "receiver", "spare")}
    return archive.restore(
        policies,
        action_types={aid: [MoveToTarget] for aid in policies},
    )


def test_savepoint_before_and_after_joint_decision_fires_action_once():
    # Save point before the joint decision.
    world = make_world()
    runtimes = {
        aid: ActorRuntime(aid, TargetPolicy(), world, action_types=[MoveToTarget])
        for aid in ("carrier", "gate", "receiver", "spare")
    }
    group = world.request_joint_group(
        action="Convoy", members=MEMBERS[:2],
        resources=("route-a",), requester_id="carrier",
    )
    world.mark_joint_member_ready(group.group_id, "carrier")
    before = WorldArchive.capture(world, runtimes, checkpoint_id="before-decision")

    def run(w):
        started_events = []
        w.events.subscribe(lambda e: started_events.append(e))
        w.mark_joint_member_ready(group.group_id, "gate")
        w.dispatch_due()
        w.clock.advance(1)
        w.dispatch_due()
        return started_events

    run(world)
    restarted_world, restarted_runtimes = _restore(before)
    restarted_events = run(restarted_world)
    assert [e.type for e in restarted_events].count("joint_group_started") == 1
    restarted_group = restarted_world.joint_groups.get(group.group_id)
    assert restarted_group.state == "started"
    # The joint action and its event sequence happen exactly once.
    assert restarted_group.decision_event_sequence == group.decision_event_sequence
    assert restarted_world.snapshot() == world.snapshot()

    # Save point after the joint decision: resuming must not re-fire.
    after = WorldArchive.capture(
        world, runtimes, checkpoint_id="after-decision"
    )
    resumed_world, resumed_runtimes = _restore(after)
    seen_after = []
    resumed_world.events.subscribe(lambda e: seen_after.append(e))
    resumed_world.dispatch_due()
    resumed_world.clock.advance(2)
    resumed_world.dispatch_due()
    assert [e.type for e in seen_after].count("joint_group_started") == 0
    assert resumed_world.joint_groups.get(group.group_id).state == "started"
    assert resumed_world.resource_status() == [
        {"resource_id": "route-a", "state": "held", "group_id": group.group_id}
    ]


def test_alternative_branches_isolate_waiting_conditions_notes_and_resources():
    world = make_world()
    runtimes = {
        aid: ActorRuntime(aid, TargetPolicy(), world, action_types=[MoveToTarget])
        for aid in ("carrier", "gate", "receiver", "spare")
    }
    group = world.request_joint_group(
        action="Convoy", members=MEMBERS,
        resources=("route-a",), requester_id="carrier",
        private_notes={"gate": {"code": "old"}},
    )
    archive = WorldArchive.capture(world, runtimes, checkpoint_id="branch-point")

    world_a, runtimes_a = _restore(archive)
    world_a.withdraw_joint_group(group.group_id, by="gate", reason="blocked")
    replacement = world_a.propose_joint_replacement(
        group.group_id,
        action="Convoy",
        members=[
            {"actor_id": "carrier", "role": "supply", "action": "Deliver"},
            {"actor_id": "spare", "role": "passage", "action": "Open"},
            {"actor_id": "receiver", "role": "receive", "action": "Receive"},
        ],
        resources=("route-b",),
        requester_id="carrier",
        private_notes={"spare": {"code": "new"}},
    )
    world_a.mark_joint_member_ready(replacement.group_id, "carrier")

    world_b, runtimes_b = _restore(archive)
    world_b.mark_joint_member_ready(group.group_id, "carrier")
    world_b.set_group_private_note(group.group_id, "gate", "code", "b-code")

    # Independent waiting conditions.
    assert world_a.joint_group_status(group.group_id)["state"] == "withdrawn"
    assert world_b.joint_group_status(group.group_id)["waiting_on"] == [
        "gate", "receiver"
    ]
    # Independent private notes.
    assert world_b.get_group_private_note(group.group_id, "gate", "code") == "b-code"
    assert world_a.get_group_private_note(group.group_id, "gate", "code") == "old"
    assert world_a.get_group_private_note(replacement.group_id, "spare", "code") == "new"
    assert replacement.group_id not in {g.group_id for g in world_b.joint_groups.all()}
    # Independent resource records.
    assert world_a.joint_groups.resources.holder("route-a") is None
    assert world_a.joint_groups.resources.holder("route-b") == replacement.group_id
    assert world_b.joint_groups.resources.holder("route-a") == group.group_id
    assert world_b.joint_groups.resources.holder("route-b") is None


def test_savepoint_at_expiry_and_removal_resumes_identically():
    def build():
        world = make_world()
        runtimes = {
            aid: ActorRuntime(aid, TargetPolicy(), world, action_types=[MoveToTarget])
            for aid in ("carrier", "gate", "receiver", "spare")
        }
        group = world.request_joint_group(
            action="Convoy", members=MEMBERS[:2],
            resources=("route-a",), requester_id="carrier", valid_for=5,
        )
        world.mark_joint_member_ready(group.group_id, "carrier")
        return world, runtimes, group

    world, runtimes, group = build()
    world.clock.advance(4)
    archive = WorldArchive.capture(world, runtimes, checkpoint_id="before-expiry")
    world.clock.advance(1)
    world.dispatch_due()

    restored_world, restored_runtimes = _restore(archive)
    restored_world.clock.advance(1)
    restored_world.dispatch_due()
    assert restored_world.snapshot() == world.snapshot()
    assert restored_world.joint_groups.get(group.group_id).state == "expired"
    assert restored_world.resource_status() == []

    world2, runtimes2, group2 = build()
    archive2 = WorldArchive.capture(world2, runtimes2, checkpoint_id="before-leave")
    world2.remove_actor("gate")
    world2.dispatch_due()
    restored2, _ = _restore(archive2)
    restored2.remove_actor("gate")
    restored2.dispatch_due()
    assert restored2.snapshot() == world2.snapshot()
    assert restored2.joint_groups.get(group2.group_id).state == "abandoned"
