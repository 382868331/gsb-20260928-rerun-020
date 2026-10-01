from behavior_runtime import (
    ActorRuntime,
    CANCELLED,
    COMPLETED,
    DUE_BEFORE_WITHDRAWAL,
    ENDED,
    PARTICIPANT_LEFT_BEFORE_START,
    PROMISER_LEFT_AFTER_START,
    REQUESTER_LEFT_WHILE_RUNNING,
    RUNNING,
    SUPERSEDED_WHILE_WAITING,
    VirtualClock,
    WAITING,
    WITHDRAWN_WHILE_WAITING,
    WITHDRAWAL_REJECTED_AFTER_START,
    World,
    WorldArchive,
)
from behavior_runtime.actions import Action, ActionContext, ActionStatus
from behavior_runtime.policy import KeepCurrent


class KeepStill:
    def decide(self, observation, memory, current_action):
        return KeepCurrent()


class Deliver(Action):
    def __init__(self, item: str = "parcel", chain: bool = False) -> None:
        self.item = item
        self.chain = chain
        self.started = 0

    def on_start(self, context: ActionContext) -> None:
        self.started += 1
        context.memory.set("delivering", self.item)

    def tick(self, context: ActionContext) -> ActionStatus:
        context.memory.set("last_delivered", self.item)
        context.world.state["log"] = [
            *context.world.state.get("log", []),
            ("delivered", self.item, context.run_id),
        ]
        if self.chain:
            self.requested_chain = context.request_promise(
                "Deliver",
                {"item": f"follow-up-of-{self.item}"},
                deadline=context.observation.time + 2,
            )
        return ActionStatus.COMPLETED

    def to_config(self):
        return {"item": self.item, "chain": self.chain}

    @classmethod
    def from_config(cls, config):
        return cls(
            item=str(config.get("item", "parcel")),
            chain=bool(config.get("chain", False)),
        )


def turn(world, runtimes):
    world.dispatch_due()
    for runtime in runtimes:
        runtime.tick()
    world.clock.advance(1)


def make_pair():
    clock = VirtualClock()
    world = World({"log": []}, clock=clock)
    alice = ActorRuntime("alice", KeepStill(), world, action_types=[Deliver])
    bob = ActorRuntime("bob", KeepStill(), world, action_types=[Deliver])
    return world, [alice, bob]


def test_waiting_promise_can_be_withdrawn_before_deadline():
    world, (alice, bob) = make_pair()
    promise_id = world.promises.request(
        "alice",
        "Deliver",
        {"item": "letter"},
        deadline=5.0,
        requester="bob",
        reason="bob_needs_letter_at_5",
    )

    assert world.promises.get(promise_id)["status"] == WAITING
    world.clock.advance(2)
    world.dispatch_due()
    world.promises.withdraw(promise_id, requester="bob", reason="changed_mind")
    world.dispatch_due()

    record = world.promises.get(promise_id)
    assert record["status"] == CANCELLED
    assert record["end_reason"] == WITHDRAWN_WHILE_WAITING
    assert alice.current_action is None
    assert world.events.pending_count == 0

    world.clock.advance(5)
    world.dispatch_due()
    alice.tick()
    assert alice.memory.get("last_delivered") is None


def test_withdrawal_after_start_does_not_erase_or_rerun():
    world, (alice, bob) = make_pair()
    promise_id = world.promises.request(
        "alice", "Deliver", {"item": "cake"}, deadline=1.0, requester="bob"
    )

    world.clock.advance(1)
    world.dispatch_due()
    alice.tick()

    record = world.promises.get(promise_id)
    assert record["status"] == COMPLETED
    assert record["run_id"] == 1
    started_action = alice.history
    assert [entry["phase"] for entry in started_action if entry["promise_id"]] == [
        "started",
        "finished",
    ]

    world.promises.withdraw(promise_id, requester="bob")
    world.dispatch_due()

    record = world.promises.get(promise_id)
    assert record["status"] == COMPLETED
    assert record["end_reason"] == "action_completed"
    assert alice.run_id == 1
    assert world.state["log"] == [("delivered", "cake", 1)]


def test_same_time_due_and_withdrawal_are_due_first():
    world, (alice, bob) = make_pair()
    promise_id = world.promises.request(
        "alice", "Deliver", {"item": "urgent"}, deadline=2.0, requester="bob"
    )

    world.clock.advance(2)

    def withdraw_on_due(event):
        if event.type == "promise_due":
            world.promises.withdraw(promise_id, requester="bob")

    world.events.subscribe(withdraw_on_due)
    dispatched = world.dispatch_due()

    promise_events = [
        event.type
        for event in dispatched
        if event.type in ("promise_due", "promise_withdrawn")
    ]
    assert promise_events == ["promise_due", "promise_withdrawn"]
    record = world.promises.get(promise_id)
    rules = [entry["rule"] for entry in record["transitions"]]
    assert DUE_BEFORE_WITHDRAWAL in rules

    alice.tick()
    record = world.promises.get(promise_id)
    assert record["status"] == COMPLETED
    assert world.state["log"] == [("delivered", "urgent", 1)]


def test_same_time_withdrawal_published_before_due_wins():
    world, (alice, bob) = make_pair()
    promise_id = world.promises.request(
        "alice", "Deliver", {"item": "maybe"}, deadline=2.5, requester="bob"
    )

    world.clock.advance(2)
    # Published first at the due instant, so its sequence precedes the due one.
    world.promises.withdraw(promise_id, requester="bob")
    world.clock.advance(0.5)
    dispatched = world.dispatch_due()

    promise_events = [
        event.type
        for event in dispatched
        if event.type in ("promise_due", "promise_withdrawn")
    ]
    assert promise_events == ["promise_withdrawn"]
    record = world.promises.get(promise_id)
    assert record["status"] == CANCELLED
    assert record["end_reason"] == WITHDRAWN_WHILE_WAITING
    alice.tick()
    assert alice.memory.get("last_delivered") is None


def test_promiser_leaving_ends_waiting_and_running_promises():
    world, (alice, bob) = make_pair()
    waiting = world.promises.request(
        "alice", "Deliver", {"item": "later"}, deadline=5.0, requester="bob"
    )
    due = world.promises.request(
        "alice", "Deliver", {"item": "now"}, deadline=1.0, requester="bob"
    )
    world.clock.advance(1)
    world.dispatch_due()
    alice.tick()

    assert world.promises.get(due)["status"] == COMPLETED
    world.remove_actor("alice")

    waiting_record = world.promises.get(waiting)
    assert waiting_record["status"] == ENDED
    assert waiting_record["end_reason"] == PARTICIPANT_LEFT_BEFORE_START


def test_promiser_removal_during_same_time_batch_records_rule():
    world, (alice, bob) = make_pair()
    promise_id = world.promises.request(
        "alice", "Deliver", {"item": "edge"}, deadline=1.0, requester="bob"
    )

    def leave_on_due(event):
        if event.type == "promise_due":
            world.remove_actor("alice")

    world.events.subscribe(leave_on_due)
    world.clock.advance(1)
    world.dispatch_due()

    record = world.promises.get(promise_id)
    assert record["status"] == ENDED
    assert record["end_reason"] == PROMISER_LEFT_AFTER_START


def test_requester_leaving_while_running_is_recorded_but_action_completes():
    world, (alice, bob) = make_pair()
    promise_id = world.promises.request(
        "alice",
        "Deliver",
        {"item": "tracked", "chain": True},
        deadline=1.0,
        requester="bob",
    )
    world.clock.advance(1)
    world.dispatch_due()
    alice.tick()

    assert world.promises.get(promise_id)["status"] == COMPLETED
    chain_id = alice.memory.get("last_delivered")

    # Follow-up promise created by the completed action links provenance.
    followups = world.promises.list(status=WAITING)
    assert len(followups) == 1
    follow = followups[0]
    assert follow["source_promise_id"] == promise_id
    assert follow["origin_event_sequence"] is not None

    del chain_id
    world.remove_actor("bob")
    follow = world.promises.get(follow["promise_id"])
    rules = [entry["rule"] for entry in follow["transitions"]]
    # The requester left before the follow-up became due.
    assert PARTICIPANT_LEFT_BEFORE_START in rules


def test_revise_replaces_waiting_promise_without_touching_history():
    world, (alice, bob) = make_pair()
    first = world.promises.request(
        "alice", "Deliver", {"item": "north"}, deadline=3.0, requester="bob"
    )
    second = world.promises.revise(
        first,
        __import__(
            "behavior_runtime.promises", fromlist=["PromiseRequest"]
        ).PromiseRequest(
            promiser="alice",
            action_name="Deliver",
            action_config={"item": "south"},
            deadline=4.0,
        ),
    )

    old = world.promises.get(first)
    assert old["status"] == CANCELLED
    assert old["end_reason"] == SUPERSEDED_WHILE_WAITING
    new = world.promises.get(second)
    assert new["status"] == WAITING
    assert new["action_config"]["item"] == "south"
    assert new["supersedes"] == first

    world.clock.advance(4)
    world.dispatch_due()
    alice.tick()
    assert alice.memory.get("last_delivered") == "south"
    assert world.state["log"].count(("delivered", "north", 1)) == 0


def test_withdrawal_branching_is_independent_after_restore():
    world, (alice, bob) = make_pair()
    promise_id = world.promises.request(
        "alice", "Deliver", {"item": "branch"}, deadline=3.0, requester="bob"
    )
    world.clock.advance(1)
    turn(world, [alice, bob])
    base = WorldArchive.capture(
        world, {"alice": alice, "bob": bob}, checkpoint_id="waiting-at-t2"
    )

    policies = {"alice": KeepStill(), "bob": KeepStill()}
    types = {"alice": [Deliver], "bob": [Deliver]}

    cancel_world, cancel = base.restore(policies, action_types=types)
    keep_world, keep = base.restore(
        {"alice": KeepStill(), "bob": KeepStill()}, action_types=types
    )

    cancel_world.promises.withdraw(promise_id, requester="bob")
    cancel_world.clock.advance(2)
    cancel_world.dispatch_due()
    for runtime in cancel.values():
        runtime.tick()

    keep_world.clock.advance(2)
    keep_world.dispatch_due()
    for runtime in keep.values():
        runtime.tick()

    assert cancel_world.promises.get(promise_id)["status"] == CANCELLED
    assert cancel["alice"].memory.get("last_delivered") is None
    assert keep_world.promises.get(promise_id)["status"] == COMPLETED
    assert keep["alice"].memory.get("last_delivered") == "branch"
    keep["alice"].memory.set("private_note", "kept")
    assert cancel["alice"].memory.get("private_note") is None


def test_savepoint_exactly_before_due_replays_event_by_event():
    world, runtimes = make_pair()
    alice, bob = runtimes
    promise_id = world.promises.request(
        "alice", "Deliver", {"item": "sharp"}, deadline=2.0, requester="bob"
    )
    world.clock.advance(2)
    archive = WorldArchive.capture(
        world, {"alice": alice, "bob": bob}, checkpoint_id="just-before-due"
    )

    # Continuous run.
    continuous = world.dispatch_due()
    alice.tick()

    # Restored run.
    restored_world, restored = archive.restore(
        {"alice": KeepStill(), "bob": KeepStill()},
        action_types={"alice": [Deliver], "bob": [Deliver]},
    )
    restored_events = restored_world.dispatch_due()
    restored["alice"].tick()

    assert [event.type for event in continuous] == [
        event.type for event in restored_events
    ]
    assert (
        restored_world.promises.get(promise_id)["transitions"]
        == world.promises.get(promise_id)["transitions"]
    )
    assert restored["alice"].history == alice.history
    assert restored_world.state == world.state


def test_promise_runs_once_even_when_restored_after_commit():
    world, runtimes = make_pair()
    alice, bob = runtimes
    promise_id = world.promises.request(
        "alice", "Deliver", {"item": "idempotent"}, deadline=1.0, requester="bob"
    )
    world.clock.advance(1)
    world.dispatch_due()
    # The due event is committed but alice has not ticked yet.
    archive = WorldArchive.capture(
        world, {"alice": alice, "bob": bob}, checkpoint_id="committed-not-run"
    )

    restored_world, restored = archive.restore(
        {"alice": KeepStill(), "bob": KeepStill()},
        action_types={"alice": [Deliver], "bob": [Deliver]},
    )
    restored["alice"].tick()
    restored["alice"].tick()

    assert restored_world.promises.get(promise_id)["status"] == COMPLETED
    assert restored_world.state["log"] == [("delivered", "idempotent", 1)]


def test_savepoint_before_removal_ends_promise_identically():
    world, runtimes = make_pair()
    alice, bob = runtimes
    waiting_id = world.promises.request(
        "alice", "Deliver", {"item": "waiting"}, deadline=4.0, requester="bob"
    )
    world.clock.advance(1)
    archive = WorldArchive.capture(
        world, {"alice": alice, "bob": bob}, checkpoint_id="before-removal"
    )

    world.remove_actor("alice")
    expected = world.promises.get(waiting_id)

    restored_world, restored = archive.restore(
        {"alice": KeepStill(), "bob": KeepStill()},
        action_types={"alice": [Deliver], "bob": [Deliver]},
    )
    restored_world.remove_actor("alice")
    actual = restored_world.promises.get(waiting_id)

    assert actual["status"] == expected["status"] == ENDED
    assert (
        actual["end_reason"]
        == expected["end_reason"]
        == PARTICIPANT_LEFT_BEFORE_START
    )
    assert [entry["rule"] for entry in actual["transitions"]] == [
        entry["rule"] for entry in expected["transitions"]
    ]
