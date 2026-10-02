from behavior_runtime import (
    ActorRuntime,
    VirtualClock,
    World,
    WorldArchive,
)
from behavior_runtime.commitments import (
    RULE_EXPIRY_BEFORE_START,
    RULE_REMOVAL_FIRST,
    RULE_STARTED_IMMUTABLE,
    RULE_WITHDRAWAL_FIRST,
)
from behavior_runtime.examples import MoveToTarget, TargetPolicy
from behavior_runtime.policy import KeepCurrent, StartAction


def make_world():
    world = World({"target": None}, clock=VirtualClock())
    runtimes = {
        "alice": ActorRuntime(
            "alice", TargetPolicy(), world, action_types=[MoveToTarget]
        ),
        "bob": ActorRuntime(
            "bob", TargetPolicy(), world, action_types=[MoveToTarget]
        ),
    }
    return world, runtimes


def track_events(world):
    seen = []
    world.events.subscribe(lambda event: seen.append(event))
    return seen


def test_withdraw_before_start_prevents_execution():
    world, _ = make_world()
    seen = track_events(world)
    commitment = world.request_commitment(
        requester_id="alice", performer_id="bob", action="MoveToTarget",
        params={"target": "north"}, delay=5, valid_for=10,
    )
    assert world.commitments.waiting()[0].commitment_id == commitment.commitment_id

    assert world.withdraw_commitment(commitment.commitment_id, by="alice")
    world.clock.advance(5)
    world.dispatch_due()

    assert commitment.state == "withdrawn"
    assert commitment.end_reason == "withdrawn_by:alice"
    assert not world.commitments.started()
    assert [e for e in seen if e.type == "commitment_started"] == []
    withdrawn_record = commitment.history[-1]
    assert withdrawn_record["rule"] == RULE_WITHDRAWAL_FIRST
    assert withdrawn_record["event_sequence"] is not None


def test_started_commitment_cannot_be_withdrawn_or_repeated():
    world, _ = make_world()
    seen = track_events(world)
    commitment = world.request_commitment(
        requester_id="alice", performer_id="bob", action="MoveToTarget", delay=2,
    )
    world.clock.advance(2)
    world.dispatch_due()
    assert commitment.state == "started"
    assert commitment.started_at == 2

    assert not world.withdraw_commitment(commitment.commitment_id, by="alice")
    assert commitment.state == "started"
    rejection = commitment.history[-1]
    assert rejection["kind"] == "withdrawal_rejected"
    assert rejection["rule"] == RULE_STARTED_IMMUTABLE

    world.dispatch_due()
    world.dispatch_due()
    started_events = [e for e in seen if e.type == "commitment_started"]
    assert len(started_events) == 1


def test_update_conditions_before_start():
    world, _ = make_world()
    commitment = world.request_commitment(
        requester_id="alice", performer_id="bob", action="MoveToTarget",
        params={"target": "north"}, delay=5,
    )
    assert world.update_commitment(
        commitment.commitment_id, by="alice",
        params={"target": "south"}, delay=3,
    )
    assert commitment.params == {"target": "south"}
    assert commitment.due_at == 3
    assert commitment.history[-1]["kind"] == "updated"

    world.clock.advance(3)
    world.dispatch_due()
    assert commitment.state == "started"
    assert not world.update_commitment(commitment.commitment_id, params={})
    assert commitment.history[-1]["kind"] == "update_rejected"


def test_same_tick_withdrawal_beats_expiry_and_records_rule():
    world, _ = make_world()
    commitment = world.request_commitment(
        requester_id="alice", performer_id="bob", action="MoveToTarget",
        delay=5, valid_for=5,
    )
    world.clock.advance(5)
    assert world.withdraw_commitment(commitment.commitment_id, by="alice")
    world.dispatch_due()
    assert commitment.state == "withdrawn"
    kinds = [entry["kind"] for entry in commitment.history]
    assert kinds == ["requested", "withdrawn"]


def test_same_tick_expiry_beats_activation_and_records_rule():
    world, _ = make_world()
    seen = track_events(world)
    commitment = world.request_commitment(
        requester_id="alice", performer_id="bob", action="MoveToTarget",
        delay=5, valid_for=5,
    )
    world.clock.advance(5)
    world.dispatch_due()
    assert commitment.state == "expired"
    assert commitment.end_reason == "expired"
    expiry = commitment.history[-1]
    assert expiry["rule"] == RULE_EXPIRY_BEFORE_START
    assert [e for e in seen if e.type == "commitment_started"] == []


def test_actor_removal_ends_commitments_with_clear_reason():
    world, _ = make_world()
    waiting = world.request_commitment(
        requester_id="alice", performer_id="bob", action="MoveToTarget", delay=5,
    )
    started = world.request_commitment(
        requester_id="bob", performer_id="alice", action="MoveToTarget", delay=1,
    )
    world.clock.advance(1)
    world.dispatch_due()
    assert started.state == "started"

    world.remove_actor("bob")
    assert waiting.state == "ended"
    assert waiting.end_reason == "participant_lost:bob"
    assert started.state == "ended"
    assert started.end_reason == "participant_lost:bob"
    for commitment in (waiting, started):
        assert commitment.history[-1]["rule"] == RULE_REMOVAL_FIRST
    # History of the started commitment is preserved, not erased.
    assert any(entry["kind"] == "started" for entry in started.history)


def test_action_triggered_commitment_is_traceable_to_origin():
    world, _ = make_world()
    first = world.request_commitment(
        requester_id="alice", performer_id="bob", action="MoveToTarget", delay=1,
    )
    world.clock.advance(1)
    dispatched = world.dispatch_due()
    started_event = next(e for e in dispatched if e.type == "commitment_started")

    follow_up = world.request_commitment(
        requester_id="bob", performer_id="alice", action="MoveToTarget",
        delay=2, wait_reason="chained-from-start",
        cause={
            "event_sequence": started_event.sequence,
            "commitment_id": first.commitment_id,
            "reason": "bob accepted after first commitment started",
        },
    )
    assert follow_up.cause["commitment_id"] == first.commitment_id
    assert follow_up.cause["event_sequence"] == started_event.sequence
    assert follow_up.wait_reason == "chained-from-start"
    started_record = next(
        entry for entry in first.history if entry["kind"] == "started"
    )
    assert started_record["event_sequence"] == started_event.sequence


def test_branches_isolate_commitments_and_memory():
    world, runtimes = make_world()
    commitment = world.request_commitment(
        requester_id="alice", performer_id="bob", action="MoveToTarget",
        delay=3, valid_for=10,
    )
    runtimes["alice"].memory.set("plan", "wait")
    archive = WorldArchive.capture(world, runtimes, checkpoint_id="before-due")

    # Branch A: withdraw the commitment.
    world_a, runtimes_a = archive.restore(
        {"alice": TargetPolicy(), "bob": TargetPolicy()},
        action_types={"alice": [MoveToTarget], "bob": [MoveToTarget]},
    )
    assert world_a.withdraw_commitment(commitment.commitment_id, by="alice")
    runtimes_a["alice"].memory.set("plan", "cancelled")
    world_a.clock.advance(3)
    world_a.dispatch_due()

    # Branch B: let it run.
    world_b, runtimes_b = archive.restore(
        {"alice": TargetPolicy(), "bob": TargetPolicy()},
        action_types={"alice": [MoveToTarget], "bob": [MoveToTarget]},
    )
    world_b.clock.advance(3)
    world_b.dispatch_due()

    commitment_a = world_a.commitments.get(commitment.commitment_id)
    commitment_b = world_b.commitments.get(commitment.commitment_id)
    assert commitment_a.state == "withdrawn"
    assert commitment_b.state == "started"
    assert runtimes_a["alice"].memory.get("plan") == "cancelled"
    assert runtimes_b["alice"].memory.get("plan") == "wait"


def test_save_right_before_expiry_resumes_identically_to_continuous_run():
    world, runtimes = make_world()
    world.request_commitment(
        requester_id="alice", performer_id="bob", action="MoveToTarget",
        delay=5, valid_for=5,
    )
    world.clock.advance(4)
    archive = WorldArchive.capture(world, runtimes, checkpoint_id="just-before-t5")

    # Continuous run: advance to the exact expiry moment and settle.
    world.clock.advance(1)
    world.dispatch_due()
    for runtime in runtimes.values():
        runtime.tick()

    restored_world, restored = archive.restore(
        {"alice": TargetPolicy(), "bob": TargetPolicy()},
        action_types={"alice": [MoveToTarget], "bob": [MoveToTarget]},
    )
    restored_world.clock.advance(1)
    restored_world.dispatch_due()
    for runtime in restored.values():
        runtime.tick()

    assert restored_world.snapshot() == world.snapshot()
    assert {
        actor_id: runtime.snapshot().to_dict()
        for actor_id, runtime in restored.items()
    } == {
        actor_id: runtime.snapshot().to_dict()
        for actor_id, runtime in runtimes.items()
    }


def test_save_right_before_actor_removal_resumes_identically():
    world, runtimes = make_world()
    world.request_commitment(
        requester_id="alice", performer_id="bob", action="MoveToTarget", delay=5,
    )
    archive = WorldArchive.capture(world, runtimes, checkpoint_id="before-removal")

    world.remove_actor("bob")
    world.dispatch_due()

    restored_world, restored = archive.restore(
        {"alice": TargetPolicy(), "bob": TargetPolicy()},
        action_types={"alice": [MoveToTarget], "bob": [MoveToTarget]},
    )
    restored_world.remove_actor("bob")
    restored_world.dispatch_due()

    assert restored_world.snapshot() == world.snapshot()
    ended = restored_world.commitments.all()[0]
    assert ended.state == "ended"
    assert ended.end_reason == "participant_lost:bob"


class CommitmentPolicy:
    """Starts the booked action when its commitment starts."""

    def decide(self, observation, memory, current_action):
        for event in observation.events:
            if event.type == "commitment_started":
                memory.set("activated", event.payload["commitment_id"])
                return StartAction(MoveToTarget(target="north"))
        return KeepCurrent()


def test_commitment_started_event_reaches_performer_mailbox():
    world = World({"target": None}, clock=VirtualClock())
    bob = ActorRuntime(
        "bob", CommitmentPolicy(), world, action_types=[MoveToTarget]
    )
    ActorRuntime("alice", TargetPolicy(), world, action_types=[MoveToTarget])
    world.request_commitment(
        requester_id="alice", performer_id="bob", action="MoveToTarget", delay=2,
    )
    world.clock.advance(2)
    world.dispatch_due()
    bob.tick()
    assert isinstance(bob.current_action, MoveToTarget)
    assert bob.memory.get("activated") == "c1"
