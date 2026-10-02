from __future__ import annotations

from behavior_runtime import ActorRuntime, VirtualClock, World, WorldArchive
from behavior_runtime.commitments import (
    RULE_MEMBER_EXPIRY_ENDS_GROUP,
    RULE_REMOVAL_FIRST,
    RULE_RESOURCE_PRIORITY,
    RULE_STARTED_IMMUTABLE,
    RULE_WITHDRAWAL_FIRST,
)
from behavior_runtime.examples import MoveToTarget, TargetPolicy


def make_world(actor_ids=("alice", "bob", "carol")):
    world = World({"target": None}, clock=VirtualClock())
    runtimes = {
        actor_id: ActorRuntime(
            actor_id, TargetPolicy(), world, action_types=[MoveToTarget]
        )
        for actor_id in actor_ids
    }
    return world, runtimes


def track_events(world):
    seen = []
    world.events.subscribe(lambda event: seen.append(event))
    return seen


def request_group(world, resources=("bridge",), action="MoveToTarget"):
    return world.request_joint_commitment(
        requester_id="alice",
        action=action,
        participants=[
            {"role": "carrier", "performer_id": "bob", "delay": 2},
            {"role": "gatekeeper", "performer_id": "carol", "delay": 5},
        ],
        params={"target": "north"},
        resources=resources,
    )


def test_group_waits_for_all_members_before_starting():
    world, _ = make_world()
    seen = track_events(world)
    group = request_group(world)

    world.clock.advance(2)
    world.dispatch_due()
    assert group.state == "waiting"
    assert [e for e in seen if e.type == "joint_commitment_started"] == []

    status = world.joint_status(group.group_id)
    assert status["waiting_on"] == ["gatekeeper"]
    assert status["satisfied"] == ["carrier"]
    assert status["resources"] == ["bridge"]
    assert status["resource_holds"] == {}

    world.clock.advance(3)
    world.dispatch_due()
    assert group.state == "started"
    assert group.decision_id is not None
    started = [e for e in seen if e.type == "joint_commitment_started"]
    assert len(started) == 1
    assert started[0].payload["decision_id"] == group.decision_id
    assert world.commitments.resource_holds() == {"bridge": group.group_id}
    status = world.joint_status(group.group_id)
    assert status["waiting_on"] == []
    assert sorted(status["satisfied"]) == ["carrier", "gatekeeper"]


def test_single_member_expiry_ends_group_without_firing():
    world, _ = make_world()
    seen = track_events(world)
    group = world.request_joint_commitment(
        requester_id="alice",
        action="MoveToTarget",
        participants=[
            {"role": "carrier", "performer_id": "bob", "delay": 1},
            {"role": "gatekeeper", "performer_id": "carol", "delay": 2,
             "valid_for": 3},
        ],
    )

    world.clock.advance(3)
    world.dispatch_due()

    assert group.state == "expired"
    assert group.end_reason.startswith("member_unavailable:")
    assert [e for e in seen if e.type == "joint_commitment_started"] == []
    assert [e.type for e in seen].count("joint_commitment_expired") == 1
    assert group.history[-1]["rule"] == RULE_MEMBER_EXPIRY_ENDS_GROUP
    assert world.commitments.resource_holds() == {}
    for member in group.members:
        commitment = world.commitments.get(member["commitment_id"])
        assert commitment.terminal


def test_withdraw_abandons_group_and_preserves_reason():
    world, _ = make_world()
    group = request_group(world)

    original = world.withdraw_joint_commitment(
        group.group_id, by="bob", reason="卡车故障"
    )
    assert original is group
    assert group.state == "withdrawn"
    assert "卡车故障" in group.end_reason
    assert group.history[-1]["rule"] == RULE_WITHDRAWAL_FIRST
    assert group.history[-1]["detail"]["reason"] == "卡车故障"
    for member in group.members:
        commitment = world.commitments.get(member["commitment_id"])
        assert commitment.state == "withdrawn"
        assert commitment.end_reason == f"group_withdrawn:{group.group_id}"

    world.clock.advance(10)
    world.dispatch_due()
    assert not world.commitments.started()


def test_withdraw_with_replacement_creates_independent_new_plan():
    world, _ = make_world(("alice", "bob", "carol", "dave"))
    group = request_group(world)

    new_group = world.withdraw_joint_commitment(
        group.group_id,
        by="bob",
        reason="退出",
        replacement={"role": "carrier", "performer_id": "dave"},
    )

    assert new_group is not None and new_group.group_id != group.group_id
    assert group.state == "withdrawn"
    assert group.end_reason.startswith("withdrawn_by:bob")
    assert new_group.cause["supersedes"] == group.group_id
    assert new_group.cause["replacement"] == {
        "role": "carrier", "performer_id": "dave",
    }
    replaced = dict(
        (m["role"], m["participant_id"]) for m in new_group.members
    )
    assert replaced == {"carrier": "dave", "gatekeeper": "carol"}

    world.clock.advance(5)
    world.dispatch_due()
    assert new_group.state == "started"
    assert group.state == "withdrawn"


def test_started_group_cannot_be_withdrawn_and_decision_is_traceable():
    world, _ = make_world()
    seen = track_events(world)
    group = request_group(world)
    world.clock.advance(5)
    world.dispatch_due()
    assert group.state == "started"

    result = world.withdraw_joint_commitment(group.group_id, by="bob")
    assert result is None
    assert group.state == "started"
    rejected = group.history[-1]
    assert rejected["kind"] == "withdrawal_rejected"
    assert rejected["rule"] == RULE_STARTED_IMMUTABLE

    follow_up = world.request_commitment(
        requester_id="bob",
        performer_id="carol",
        action="MoveToTarget",
        delay=1,
        cause={"decision_id": group.decision_id, "reason": "组合动作后续"},
    )
    assert follow_up.cause["decision_id"] == group.decision_id
    started = [e for e in seen if e.type == "joint_commitment_started"]
    assert started[0].payload["decision_id"] == follow_up.cause["decision_id"]


def test_resource_contention_has_single_winner_and_no_leaked_holds():
    world, _ = make_world(("alice", "bob", "carol", "dave", "erin"))
    first = world.request_joint_commitment(
        requester_id="alice",
        action="MoveToTarget",
        participants=[{"role": "carrier", "performer_id": "bob"}],
        resources=("bridge",),
    )
    second = world.request_joint_commitment(
        requester_id="alice",
        action="MoveToTarget",
        participants=[{"role": "carrier", "performer_id": "dave"}],
        resources=("bridge",),
    )

    world.dispatch_due()
    assert first.state == "started"
    assert second.state == "waiting"
    assert world.commitments.resource_holds() == {"bridge": first.group_id}
    deferred = second.history[-1]
    assert deferred["kind"] == "activation_deferred"
    assert deferred["rule"] == RULE_RESOURCE_PRIORITY
    assert deferred["detail"]["blockers"] == {"bridge": first.group_id}

    assert world.complete_joint_commitment(first.group_id)
    assert world.commitments.resource_holds() == {}

    world.dispatch_due()
    assert second.state == "started"
    assert world.commitments.resource_holds() == {"bridge": second.group_id}


def test_same_moment_settlement_of_expiry_departure_and_release():
    world, _ = make_world(("alice", "bob", "carol", "dave"))
    seen = track_events(world)
    expiring = world.request_joint_commitment(
        requester_id="alice",
        action="MoveToTarget",
        participants=[
            {"role": "carrier", "performer_id": "bob", "delay": 1,
             "valid_for": 4},
            {"role": "gatekeeper", "performer_id": "dave", "delay": 9},
        ],
    )
    departing = world.request_joint_commitment(
        requester_id="alice",
        action="MoveToTarget",
        participants=[{"role": "carrier", "performer_id": "carol", "delay": 6}],
        resources=("dock",),
    )

    world.clock.advance(4)
    world.remove_actor("carol")
    world.dispatch_due()

    assert expiring.state == "expired"
    assert expiring.history[-1]["rule"] == RULE_MEMBER_EXPIRY_ENDS_GROUP
    assert departing.state == "ended"
    assert departing.end_reason == "participant_lost:carol"
    assert departing.history[-1]["rule"] == RULE_REMOVAL_FIRST
    assert world.commitments.resource_holds() == {}
    assert [e for e in seen if e.type == "joint_commitment_started"] == []


def test_restore_before_and_after_decision_fires_exactly_once():
    world, runtimes = make_world()
    group = request_group(world)
    world.clock.advance(2)
    before = WorldArchive.capture(world, runtimes, checkpoint_id="before-ready")

    world.clock.advance(3)
    world.dispatch_due()
    assert group.state == "started"
    after = WorldArchive.capture(
        world, runtimes, checkpoint_id="after-decision", parent=before
    )

    policies = {a: TargetPolicy() for a in ("alice", "bob", "carol")}
    types = {a: [MoveToTarget] for a in ("alice", "bob", "carol")}

    branch_world, _ = before.restore(policies, action_types=types)
    branch_seen = track_events(branch_world)
    branch_world.clock.advance(3)
    branch_world.dispatch_due()
    branch_group = branch_world.commitments.get_group(group.group_id)
    assert branch_group.state == "started"
    assert branch_group.decision_id == group.decision_id
    assert len([e for e in branch_seen
                if e.type == "joint_commitment_started"]) == 1

    resumed_world, _ = after.restore(policies, action_types=types)
    resumed_seen = track_events(resumed_world)
    resumed_world.clock.advance(5)
    resumed_world.dispatch_due()
    resumed_group = resumed_world.commitments.get_group(group.group_id)
    assert resumed_group.state == "started"
    assert [e for e in resumed_seen
            if e.type == "joint_commitment_started"] == []

    branch_world.clock.advance(5)
    branch_world.dispatch_due()
    assert len([e for e in branch_seen
                if e.type == "joint_commitment_started"]) == 1


def test_replacement_branches_keep_memory_and_resources_independent():
    world, runtimes = make_world(("alice", "bob", "carol", "dave"))
    group = request_group(world)
    archive = WorldArchive.capture(world, runtimes, checkpoint_id="pre-withdraw")
    policies = {a: TargetPolicy() for a in ("alice", "bob", "carol", "dave")}
    types = {a: [MoveToTarget] for a in ("alice", "bob", "carol", "dave")}

    abandon_world, abandon_runtimes = archive.restore(policies, action_types=types)
    abandon_world.withdraw_joint_commitment(group.group_id, by="bob",
                                            reason="放弃")
    abandon_runtimes["bob"].memory.set("secret", "private-to-abandon-branch")

    replace_world, replace_runtimes = archive.restore(policies, action_types=types)
    new_group = replace_world.withdraw_joint_commitment(
        group.group_id, by="bob", reason="换人",
        replacement={"role": "carrier", "performer_id": "dave"},
    )
    replace_world.clock.advance(5)
    replace_world.dispatch_due()

    assert replace_runtimes["bob"].memory.get("secret") is None
    assert abandon_world.commitments.get_group(group.group_id).state == "withdrawn"
    assert replace_world.commitments.get_group(group.group_id).state == "withdrawn"
    assert new_group.state == "started"
    assert abandon_world.commitments.resource_holds() == {}
    assert replace_world.commitments.resource_holds() == {
        "bridge": new_group.group_id
    }
    assert abandon_world.joint_status(group.group_id)["state"] == "withdrawn"
    assert replace_world.joint_status(new_group.group_id)["waiting_on"] == []
