from behavior_runtime import EventBus, VirtualClock, World, ActorRuntime
from behavior_runtime.examples import MoveToTarget, TargetPolicy


class RecordingPolicy:
    def __init__(self, actions):
        self.actions = list(actions)
        self.calls = 0

    def decide(self, observation, memory, current_action):
        self.calls += 1
        if self.actions:
            action = self.actions.pop(0)
            if action is None:
                from behavior_runtime.policy import KeepCurrent

                return KeepCurrent()
            from behavior_runtime.policy import StartAction

            return StartAction(action)
        from behavior_runtime.policy import KeepCurrent

        return KeepCurrent()


def step(world, runtimes):
    world.dispatch_due()
    results = [runtime.tick() for runtime in runtimes]
    world.clock.advance(1)
    return results


def test_shared_observation_but_independent_runtime_state():
    clock = VirtualClock()
    world = World({"target": "north"}, clock=clock)
    alice = ActorRuntime("alice", TargetPolicy(), world, action_types=[MoveToTarget])
    bob = ActorRuntime("bob", TargetPolicy(), world, action_types=[MoveToTarget])

    step(world, [alice, bob])

    assert isinstance(alice.current_action, MoveToTarget)
    assert isinstance(bob.current_action, MoveToTarget)
    assert alice.run_id == 1
    assert bob.run_id == 1
    assert alice.memory.get("active_target") == "north"
    assert bob.memory.get("active_target") == "north"

    world.state["target"] = "south"
    step(world, [alice])

    assert alice.current_action.target == "south"
    assert bob.current_action.target == "north"
    assert bob.memory.get("active_target") == "north"


def test_target_change_interrupts_once_and_late_result_does_not_replace_state():
    clock = VirtualClock()
    world = World({"target": "north"}, clock=clock)
    actor = ActorRuntime("actor", TargetPolicy(), world, action_types=[MoveToTarget])

    step(world, [actor])
    first_action = actor.current_action
    world.state["target"] = "south"
    step(world, [actor])

    assert first_action.interrupt_count == 1
    assert actor.interrupted_runs == [1]
    assert isinstance(actor.current_action, MoveToTarget)
    assert actor.current_action.target == "south"
    assert actor.run_id == 2

    world.dispatch_due()
    status = actor.tick()

    assert actor.stale_results_ignored == 1
    assert status is not None
    assert status.value == "running"
    assert isinstance(actor.current_action, MoveToTarget)
    assert actor.current_action.target == "south"


def test_snapshot_restore_continues_current_action_run():
    clock = VirtualClock()
    world = World({"target": "north"}, clock=clock)
    actor = ActorRuntime("actor", TargetPolicy(), world, action_types=[MoveToTarget])

    step(world, [actor])
    world.state["target"] = "south"
    step(world, [actor])
    data = actor.snapshot().to_dict()

    restored = ActorRuntime("actor", TargetPolicy(), world, action_types=[MoveToTarget])
    restored.restore(data)

    assert restored.run_id == 2
    assert restored.memory.get("active_target") == "south"
    assert restored.current_action.target == "south"
    assert restored.interrupted_runs == [1]

    clock.advance(2)
    world.dispatch_due()
    restored.tick()
    assert restored.status.value == "completed"
    assert restored.memory.get("last_target") == "south"


def test_stop_is_one_time_cleanup():
    clock = VirtualClock()
    world = World({}, clock=clock)
    actor = ActorRuntime(
        "actor",
        RecordingPolicy([MoveToTarget("north"), None]),
        world,
        action_types=[MoveToTarget],
    )

    actor.tick()
    action = actor.current_action
    first_status = actor.stop()
    second_status = actor.stop()

    assert first_status.value == "cancelled"
    assert second_status.value == "cancelled"
    assert actor.current_action is None
    assert action.interrupt_count == 1
    assert actor.interrupted_runs == [1]


def test_controlled_time_preserves_event_order():
    clock = VirtualClock()
    bus = EventBus()
    bus.publish("later", {"n": 2}, time=clock.now(), delay=2)
    bus.publish("same_time_b", {"n": 1}, time=clock.now(), delay=1)
    bus.publish("same_time_a", {"n": 0}, time=clock.now(), delay=1)

    clock.advance(1)
    first_batch = bus.dispatch_due(clock.now())
    assert [(event.type, event.payload["n"]) for event in first_batch] == [
        ("same_time_b", 1),
        ("same_time_a", 0),
    ]

    clock.advance(1)
    second_batch = bus.dispatch_due(clock.now())
    assert [event.type for event in second_batch] == ["later"]


def test_same_time_events_keep_publication_and_causal_order_after_restore():
    clock = VirtualClock()
    bus = EventBus()

    def publish_follow_up(event):
        if event.type == "cause":
            bus.publish(
                "effect",
                {"by": event.sequence},
                time=clock.now(),
                caused_by=event.sequence,
            )

    unsubscribe = bus.subscribe(publish_follow_up)
    bus.publish("second", time=clock.now())
    bus.publish("cause", time=clock.now())
    bus.publish("first", time=clock.now())

    dispatched = bus.dispatch_due(clock.now())
    assert [event.type for event in dispatched] == [
        "second",
        "cause",
        "effect",
        "first",
    ]
    assert dispatched[2].caused_by == dispatched[1].sequence
    unsubscribe()

    restored_bus = EventBus()
    restored_bus.restore(bus.snapshot())
    clock.advance(1)
    restored_bus.publish("later", time=clock.now())
    assert [event.type for event in restored_bus.dispatch_due(clock.now())] == [
        "later"
    ]


def test_actor_registration_order_is_stable_and_survives_restore():
    clock = VirtualClock()
    world = World(clock=clock)
    world.register_actor("zoe")
    world.register_actor("alice")
    world.register_actor("mike")

    assert world.actor_ids == ("zoe", "alice", "mike")

    restored = World(clock=VirtualClock())
    restored.restore(world.snapshot())
    assert restored.actor_ids == ("zoe", "alice", "mike")

    world.remove_actor("alice")
    world.register_actor("alice")
    assert world.actor_ids == ("zoe", "mike", "alice")


def test_deleted_actor_does_not_receive_old_or_restored_private_events():
    clock = VirtualClock()
    world = World(clock=clock)
    world.register_actor("actor")
    world.publish("private", {"generation": 1}, actor_id="actor", delay=1)
    data = world.snapshot()

    world.remove_actor("actor")
    clock.advance(1)
    assert world.dispatch_due() == []

    world.register_actor("actor")
    assert world.dispatch_due() == []
    assert world.observe("actor").events == ()

    restored = World(clock=VirtualClock())
    restored.restore(data)
    restored.remove_actor("actor")
    restored.register_actor("actor")
    restored.clock.advance(1)
    restored.dispatch_due()
    assert restored.observe("actor").events == ()


def test_actor_join_during_dispatch_starts_on_next_dispatch():
    clock = VirtualClock()
    world = World(clock=clock)
    world.register_actor("present")
    world.publish("announcement")

    def join_late(event):
        if event.type == "announcement":
            world.register_actor("late")

    unsubscribe = world.events.subscribe(join_late)
    dispatched = world.dispatch_due()
    unsubscribe()

    assert [event.actor_id for event in dispatched] == [None]
    assert world.observe("present").events[0].type == "announcement"
    assert world.observe("late").events == ()

    world.publish("next_announcement")
    world.dispatch_due()
    assert world.observe("late").events[0].type == "next_announcement"
