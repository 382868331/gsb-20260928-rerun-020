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


def wait_for_arrivals(world, runtimes):
    world.clock.advance(1)
    delivered = world.dispatch_due()
    for runtime in runtimes.values():
        runtime.tick()
    return delivered


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


def test_restored_waiting_actions_replay_same_event_sequence():
    world, runtimes = make_world()
    run_turn(world, runtimes)
    checkpoint = WorldArchive.capture(
        world, runtimes, checkpoint_id="waiting", branch_id="main"
    )

    expected = wait_for_arrivals(world, runtimes)
    restored_world, restored = checkpoint.restore(
        {"alice": TargetPolicy(), "bob": TargetPolicy()},
        action_types={"alice": [MoveToTarget], "bob": [MoveToTarget]},
    )
    actual = wait_for_arrivals(restored_world, restored)

    assert [event.to_dict() for event in actual] == [
        event.to_dict() for event in expected
    ]
    assert [event.sequence for event in actual] == [1, 3, 2, 4]
    assert restored_world.snapshot() == world.snapshot()


def test_action_history_records_total_order_and_triggering_events():
    world, runtimes = make_world()
    run_turn(world, runtimes)
    world.clock.advance(2)
    world.dispatch_due()
    for runtime in runtimes.values():
        runtime.tick()

    started = [
        event
        for runtime in runtimes.values()
        for event in runtime.history
        if event["phase"] == "started"
    ]
    assert [event["actor_id"] for event in started] == ["alice", "bob"]
    assert [event["order"] for event in started] == [1, 2]
    assert [event["actor_index"] for event in started] == [0, 1]

    finished = [
        event
        for runtime in runtimes.values()
        for event in runtime.history
        if event["phase"] == "finished"
    ]
    assert [event["order"] for event in finished] == [3, 4]
