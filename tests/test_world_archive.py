from behavior_runtime import ActorRuntime, VirtualClock, World, WorldArchive
from behavior_runtime.examples import MoveToTarget, TargetPolicy


def run_turn(world, runtimes):
    world.dispatch_due()
    for runtime in runtimes.values():
        runtime.tick()
    world.clock.advance(1)


def make_world():
    world = World({"target": "north"}, clock=VirtualClock())
    runtimes = {
        "alice": ActorRuntime(
            "alice", TargetPolicy(), world, action_types=[MoveToTarget]
        ),
        "bob": ActorRuntime(
            "bob", TargetPolicy(), world, action_types=[MoveToTarget]
        ),
    }
    return world, runtimes


class OrderPolicy:
    def __init__(self, actor_id):
        self.actor_id = actor_id

    def decide(self, observation, memory, current_action):
        from behavior_runtime.policy import KeepCurrent

        if current_action is None:
            memory.set("observed_events", [event.sequence for event in observation.events])
        return KeepCurrent()


def test_world_archive_replays_uninterrupted_world():
    world, runtimes = make_world()
    run_turn(world, runtimes)
    archive = WorldArchive.capture(
        world, runtimes, checkpoint_id="t1", branch_id="main"
    )

    expected_world = world.snapshot()
    expected_runtimes = {
        actor_id: runtime.snapshot().to_dict()
        for actor_id, runtime in runtimes.items()
    }

    for _ in range(3):
        run_turn(world, runtimes)

    restored_world, restored = archive.restore(
        {"alice": TargetPolicy(), "bob": TargetPolicy()},
        action_types={
            "alice": [MoveToTarget],
            "bob": [MoveToTarget],
        },
    )
    assert restored_world.snapshot() == expected_world
    assert {
        actor_id: runtime.snapshot().to_dict()
        for actor_id, runtime in restored.items()
    } == expected_runtimes

    for _ in range(3):
        run_turn(restored_world, restored)

    assert restored_world.snapshot() == world.snapshot()
    assert {
        actor_id: runtime.snapshot().to_dict()
        for actor_id, runtime in restored.items()
    } == {
        actor_id: runtime.snapshot().to_dict()
        for actor_id, runtime in runtimes.items()
    }


def test_branches_isolate_messages_memory_and_decisions():
    world, runtimes = make_world()
    run_turn(world, runtimes)
    base = WorldArchive.capture(
        world, runtimes, checkpoint_id="waiting", branch_id="main"
    )

    policies = {"alice": TargetPolicy(), "bob": TargetPolicy()}
    action_types = {"alice": [MoveToTarget], "bob": [MoveToTarget]}

    north_world, north = base.restore(
        policies, action_types=action_types
    )
    south_world, south = base.restore(
        {"alice": TargetPolicy(), "bob": TargetPolicy()},
        action_types={
            "alice": [MoveToTarget],
            "bob": [MoveToTarget],
        },
    )

    assert south is not north
    south_world.state["target"] = "south"
    south["alice"].memory.set("branch_only", "south")
    south["bob"].memory.set("branch_only", "south")

    run_turn(south_world, south)
    assert north["alice"].memory.get("branch_only") is None
    assert north["bob"].memory.get("branch_only") is None
    assert north_world.state["target"] == "north"
    assert north_world.events.pending_count == 4
    assert south_world.events.pending_count == 10

    for _ in range(2):
        run_turn(north_world, north)
    for _ in range(2):
        run_turn(south_world, south)

    for runtime in north.values():
        assert runtime.memory.get("last_target") == "north"
        assert runtime.status.value == "completed"
    for actor_id, runtime in south.items():
        assert runtime.memory.get("last_target") == "south"
        assert runtime.status.value == "completed"
        assert runtime.stale_results_ignored >= 1
        decisions = [
            event
            for event in runtime.history
            if event["phase"] == "started"
        ]
        assert [event["action_config"]["target"] for event in decisions] == [
            "north",
            "south",
        ]
        assert runtime.interrupted_runs == [1]

    north_archive = WorldArchive.capture(
        north_world, north, checkpoint_id="north-done", parent=base
    )
    south_archive = WorldArchive.capture(
        south_world,
        south,
        checkpoint_id="south-done",
        branch_id="south-goal",
        parent=base,
    )

    assert north_archive.branch.branch_id == "main"
    assert north_archive.branch.parent_checkpoint_id == "waiting"
    assert south_archive.branch.branch_id == "south-goal"
    assert south_archive.branch.parent_checkpoint_id == "waiting"
    assert north_archive.data != south_archive.data


def test_archive_preserves_actor_iteration_and_decision_evidence():
    world = World(clock=VirtualClock())
    runtimes = {
        actor_id: ActorRuntime(actor_id, OrderPolicy(actor_id), world)
        for actor_id in ("zoe", "alice", "mike")
    }

    world.publish("shared", {"n": 1})
    world.dispatch_due()
    for runtime in runtimes.values():
        runtime.tick()
    archive = WorldArchive.capture(world, runtimes, checkpoint_id="ordered")

    assert archive.actor_ids == ("zoe", "alice", "mike")
    restored_world, restored = archive.restore(
        {actor_id: OrderPolicy(actor_id) for actor_id in runtimes}
    )
    assert restored_world.actor_ids == ("zoe", "alice", "mike")
    assert [
        runtime.tick_records[-1]["actor_order"] for runtime in restored.values()
    ] == [0, 1, 2]
    assert all(
        runtime.tick_records[-1]["observed_event_sequences"] == [1]
        for runtime in restored.values()
    )


def test_actor_membership_changes_apply_after_same_time_dispatch():
    world = World(clock=VirtualClock())
    alice = ActorRuntime("alice", TargetPolicy(), world, action_types=[MoveToTarget])
    bob = ActorRuntime("bob", TargetPolicy(), world, action_types=[MoveToTarget])

    def membership_change(event):
        if event.type == "remove_bob":
            world.remove_actor("bob")
            world.register_actor("carol")

    world.events.subscribe(membership_change)
    world.publish("remove_bob")
    world.publish("bob_only", actor_id="bob")
    world.publish("carol_only", actor_id="carol")
    dispatched = world.dispatch_due()

    assert [event.type for event in dispatched] == [
        "remove_bob",
        "bob_only",
        "carol_only",
    ]
    assert world.actor_ids == ("alice", "carol")
    mailboxes = world.snapshot()["mailboxes"]
    assert [event["type"] for event in mailboxes["alice"]] == ["remove_bob"]
    assert [event["type"] for event in mailboxes["carol"]] == ["carol_only"]
    assert "bob" not in mailboxes
    assert world.events.pending_count == 0


def test_actor_that_leaves_and_rejoins_does_not_replay_batch_broadcast():
    world = World(clock=VirtualClock())
    ActorRuntime("alice", TargetPolicy(), world, action_types=[MoveToTarget])
    ActorRuntime("bob", TargetPolicy(), world, action_types=[MoveToTarget])

    def membership_change(event):
        if event.type == "leave":
            world.remove_actor("bob")
            world.register_actor("bob")

    world.events.subscribe(membership_change)
    world.publish("leave")
    world.publish("bob_only", actor_id="bob")
    world.dispatch_due()

    mailboxes = world.snapshot()["mailboxes"]
    assert [event["type"] for event in mailboxes["alice"]] == ["leave"]
    assert [event["type"] for event in mailboxes["bob"]] == ["bob_only"]


def test_deleted_waiting_events_and_memory_are_not_shared_across_restores():
    world, runtimes = make_world()
    run_turn(world, runtimes)
    base = WorldArchive.capture(world, runtimes, checkpoint_id="base")

    left_world, left = base.restore(
        {"alice": TargetPolicy(), "bob": TargetPolicy()},
        action_types={"alice": [MoveToTarget], "bob": [MoveToTarget]},
    )
    other_world, other = base.restore(
        {"alice": TargetPolicy(), "bob": TargetPolicy()},
        action_types={"alice": [MoveToTarget], "bob": [MoveToTarget]},
    )

    left_world.remove_actor("bob")
    left["alice"].memory.set("private", "left")
    other["bob"].memory.set("private", "other")
    deleted_archive = WorldArchive.capture(left_world, left, checkpoint_id="bob-left")

    assert deleted_archive.actor_ids == ("alice",)
    assert deleted_archive.data["world"]["actor_order"] == ["alice"]
    restored_deleted_world, restored_deleted = deleted_archive.restore(
        {"alice": TargetPolicy()},
        action_types={"alice": [MoveToTarget]},
    )
    assert restored_deleted_world.actor_ids == ("alice",)
    assert restored_deleted_world.events.pending_count == (
        left_world.events.pending_count
    )
    serialized_events = restored_deleted_world.events.snapshot()
    assert all(
        event["actor_id"] != "bob"
        for event in [*serialized_events["scheduled"], *serialized_events["due"]]
    )
    assert other["alice"].memory.get("private") is None
    assert other["bob"].memory.get("private") == "other"
