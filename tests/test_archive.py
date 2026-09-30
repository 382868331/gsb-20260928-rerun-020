import copy

from behavior_runtime import (
    VirtualClock,
    World,
    ActorRuntime,
    WorldArchive,
    WorldCheckpoint,
    decisions_since,
    diff_decisions,
)
from behavior_runtime.examples import MoveToTarget
from behavior_runtime.policy import KeepCurrent, StartAction


TRAVEL_TIME = 4.0


class GoalPolicy:
    """Move toward a per-actor goal stored in serializable world state.

    Goals live at ``state['goals'][actor_id]`` so that changing a goal is part
    of the archived world and survives a branch/restore, rather than living on
    the non-serializable policy object.
    """

    def decide(self, observation, memory, current_action):
        seen = memory.get("seen_events", [])
        for event in observation.events:
            if event.action_run_id is None:
                seen.append(event.type)
        if seen:
            memory.set("seen_events", seen)

        goal = observation.state.get("goals", {}).get(observation.actor_id)
        remembered = memory.get("active_target")
        if goal is None:
            return KeepCurrent()
        if memory.get("last_target") == goal and current_action is None:
            return KeepCurrent()
        if not isinstance(current_action, MoveToTarget) or current_action.target != goal:
            if remembered != goal:
                memory.set("target_changed_at", observation.time)
            return StartAction(MoveToTarget(target=goal, travel_time=TRAVEL_TIME))
        return KeepCurrent()


def ordered(world, runtimes, order=("alice", "bob")):
    return [runtimes[name] for name in order if name in runtimes]


def run(world, runtimes, order=("alice", "bob"), ticks=1):
    log = []
    for _ in range(ticks):
        for event in world.dispatch_due():
            log.append((event.type, event.actor_id, event.action_run_id))
        statuses = [runtimes[name].tick() for name in order if name in runtimes]
        world.clock.advance(1)
    return log, statuses


def make_world(goals=("north", "east"), state=None):
    clock = VirtualClock()
    world_state = dict(state or {})
    world_state.setdefault("goals", {}).update(
        {actor_id: goal for actor_id, goal in zip(("alice", "bob"), goals)}
    )
    world = World(world_state, clock=clock)
    runtimes = {}
    for actor_id, goal in zip(("alice", "bob"), goals):
        runtimes[actor_id] = ActorRuntime(
            actor_id,
            GoalPolicy(),
            world,
            action_types=[MoveToTarget],
        )
    return world, runtimes


def start_in_flight_world(goals=("north", "east")):
    world, runtimes = make_world(goals)
    run(world, runtimes, ticks=2)
    return world, runtimes


def fresh_policies():
    return {"alice": GoalPolicy(), "bob": GoalPolicy()}


def test_checkpoint_captures_pending_events_waits_state_and_mailboxes():
    world, runtimes = start_in_flight_world()
    world.state["weather"] = "rain"

    archive = WorldArchive()
    checkpoint = archive.capture(world, ordered(world, runtimes))

    assert world.clock.now() == 2
    # Two arrivals (run 1 per actor) are scheduled but have not arrived yet.
    scheduled = checkpoint.world["events"]["scheduled"]
    assert [item[2]["type"] for item in scheduled] == [
        "movement_arrived",
        "movement_arrived",
    ]
    assert all(item[0] == TRAVEL_TIME for item in scheduled)
    assert checkpoint.world["state"]["weather"] == "rain"

    for snapshot in checkpoint.actors:
        assert snapshot.action_name == "MoveToTarget"
        assert snapshot.run_id == 1
        assert snapshot.memory["active_movement"]["run_id"] == 1

    restored_world, restored = archive.restore(
        checkpoint, policies=fresh_policies(), action_types=[MoveToTarget]
    )
    assert restored_world.clock.now() == 2
    for actor_id in ("alice", "bob"):
        assert isinstance(restored[actor_id].current_action, MoveToTarget)
        assert restored[actor_id].run_id == 1


def test_restored_world_is_equivalent_to_uninterrupted_run():
    control_world, control = start_in_flight_world()
    _, branch_runtimes = start_in_flight_world()

    archive = WorldArchive()
    checkpoint = archive.capture(
        branch_runtimes["alice"].world, ordered(branch_runtimes["alice"].world, branch_runtimes)
    )

    # Both worlds receive the same external change at the same tick.
    def continue_(world, runtimes):
        world.state["weather"] = "storm"
        return run(world, runtimes, ticks=5)

    control_log, _ = continue_(control_world, control)
    restored_world, restored = archive.restore(
        checkpoint, policies=fresh_policies(), action_types=[MoveToTarget]
    )
    restored_log, _ = continue_(restored_world, restored)

    assert restored_log == control_log
    for actor_id in ("alice", "bob"):
        assert restored[actor_id].status.value == "completed"
        assert restored[actor_id].history == control[actor_id].history
        assert restored[actor_id].memory.as_dict() == control[actor_id].memory.as_dict()
        assert restored[actor_id].stale_results_ignored == 0


def test_branches_share_no_writable_memory_world_state_or_messages():
    source_world, source = start_in_flight_world()
    archive = WorldArchive()
    base = archive.capture(source_world, ordered(source_world, source))

    def alice_goes_south(world, runtimes):
        world.state["weather"] = "fog"
        world.state["goals"]["alice"] = "south"

    changed = archive.branch(
        base,
        branch_name="alice-south",
        policies=fresh_policies(),
        action_types=[MoveToTarget],
        mutate=alice_goes_south,
    )
    unchanged = archive.branch(
        base,
        branch_name="keep",
        policies=fresh_policies(),
        action_types=[MoveToTarget],
    )

    # Checkpoint content is an independent deep copy of the source world.
    changed.world["state"]["weather"] = "hacked"
    assert "weather" not in base.world["state"]
    assert unchanged.world["state"].get("weather") is None

    changed_world, changed_runtimes = archive.restore(
        changed,
        policies=fresh_policies(),
        action_types=[MoveToTarget],
    )
    unchanged_world, unchanged_runtimes = archive.restore(
        unchanged, policies=fresh_policies(), action_types=[MoveToTarget]
    )

    run(changed_world, changed_runtimes, ticks=5)
    run(unchanged_world, unchanged_runtimes, ticks=5)

    assert changed_runtimes["alice"].memory.get("last_target") == "south"
    assert unchanged_runtimes["alice"].memory.get("last_target") == "north"
    # Bob never observes Alice's branch-specific change.
    assert changed_runtimes["bob"].memory.get("last_target") == "east"
    assert unchanged_runtimes["bob"].memory.get("last_target") == "east"
    # Memories are independent objects even before they diverge in content.
    changed_runtimes["alice"].memory.set("scratch", 1)
    assert unchanged_runtimes["alice"].memory.get("scratch") is None


def test_same_tick_target_change_and_late_old_result_after_restore():
    world, runtimes = start_in_flight_world()
    archive = WorldArchive()
    base = archive.capture(world, ordered(world, runtimes))

    def advance_branch(checkpoint, policies, alice_goal):
        branch_world, branch = archive.restore(
            checkpoint, policies=policies, action_types=[MoveToTarget]
        )
        # At the first post-restore tick the old run-1 arrivals come due,
        # and Alice simultaneously retargets before the old result is handled.
        branch_world.state["weather"] = "wind"
        branch_world.state["goals"]["alice"] = alice_goal
        run(branch_world, branch, ticks=1)
        run(branch_world, branch, ticks=5)
        return branch_world, branch

    _, south = advance_branch(base, fresh_policies(), "south")
    _, west = advance_branch(base, fresh_policies(), "west")

    # Old run-1 arrival arrived late, after run 2 had already started.
    assert south["alice"].interrupted_runs == [1]
    assert south["alice"].stale_results_ignored == 1
    assert west["alice"].stale_results_ignored == 1
    # Bob kept the same goal in both branches: run 1 completes, no stale result.
    assert south["bob"].stale_results_ignored == 0
    assert south["bob"].status.value == "completed"
    assert west["bob"].status.value == "completed"

    south_decisions = decisions_since(base, south)
    west_decisions = decisions_since(base, west)
    comparison = diff_decisions(south_decisions, west_decisions)

    south_actions = [entry["action"] for entry in south_decisions["alice"]]
    west_actions = [entry["action"] for entry in west_decisions["alice"]]
    assert "MoveToTarget" in south_actions
    assert south_actions == west_actions  # same action kinds, different targets
    # The decisions share shape; the changed target is the diverging input,
    # reflected in private memory and the final reached destination.
    assert south["alice"].memory.get("last_target") == "south"
    assert west["alice"].memory.get("last_target") == "west"
    assert comparison["alice"]["left"] == south_decisions["alice"]
    assert comparison["bob"]["identical"] is True

    # Divergence is the memory state, not the shape of decisions.
    south_run_2 = next(
        entry for entry in south["alice"].history if entry["run_id"] == 2
    )
    west_run_2 = next(
        entry for entry in west["alice"].history if entry["run_id"] == 2
    )
    assert south_run_2["time"] == west_run_2["time"] == 2


def test_lineage_traces_back_to_the_branched_state():
    world, runtimes = start_in_flight_world()
    archive = WorldArchive()
    root = archive.capture(world, ordered(world, runtimes), branch_name="root")

    changed = archive.branch(
        root,
        branch_name="alice-south",
        policies=fresh_policies(),
        action_types=[MoveToTarget],
        mutate=lambda w, r: w.state["goals"].__setitem__("alice", "south"),
    )
    unchanged = archive.branch(
        root,
        branch_name="control",
        policies=fresh_policies(),
        action_types=[MoveToTarget],
    )
    grandchild = archive.branch(
        changed,
        branch_name="alice-west",
        policies=fresh_policies(),
        action_types=[MoveToTarget],
        mutate=lambda w, r: w.state["goals"].__setitem__("alice", "west"),
    )

    assert root.lineage.parent_id is None
    assert root.lineage.root_id == root.lineage.checkpoint_id
    assert root.lineage.depth == 0

    assert changed.lineage.parent_id == root.lineage.checkpoint_id
    assert unchanged.lineage.parent_id == root.lineage.checkpoint_id
    assert changed.lineage.root_id == root.lineage.checkpoint_id
    assert changed.lineage.depth == 1

    assert grandchild.lineage.parent_id == changed.lineage.checkpoint_id
    assert grandchild.lineage.root_id == root.lineage.checkpoint_id
    assert grandchild.lineage.depth == 2

    # Two branches share the identical starting content hash, then diverge.
    assert changed.content_hash != unchanged.content_hash
    restored_root = WorldCheckpoint.from_dict(root.to_dict())
    assert restored_root.content_hash == root.content_hash
    assert restored_root.lineage.parent_id is None
    assert restored_root.lineage.checkpoint_id == root.lineage.checkpoint_id


def test_unread_messages_travel_with_the_branch_and_are_not_shared():
    world, runtimes = start_in_flight_world()
    # A broadcast event sits in both mailboxes, unread by either actor.
    world.dispatch_due()
    world.publish("announcement", {"note": "branch me"})
    world.dispatch_due()

    archive = WorldArchive()
    base = archive.capture(world, ordered(world, runtimes))
    branch_a = archive.branch(
        base,
        branch_name="a",
        policies=fresh_policies(),
        action_types=[MoveToTarget],
    )
    branch_b = archive.branch(
        base,
        branch_name="b",
        policies=fresh_policies(),
        action_types=[MoveToTarget],
    )

    world_a, runtimes_a = archive.restore(
        branch_a, policies=fresh_policies(), action_types=[MoveToTarget]
    )
    world_b, runtimes_b = archive.restore(
        branch_b, policies=fresh_policies(), action_types=[MoveToTarget]
    )

    run(world_a, runtimes_a, ticks=1)
    run(world_b, runtimes_b, ticks=1)

    seen_a = runtimes_a["alice"].memory.get("seen_events")
    seen_b = runtimes_b["alice"].memory.get("seen_events")
    assert seen_a[-1] == "announcement"
    assert seen_b[-1] == "announcement"
    # Consuming the message in one branch does not remove it from the other.
    run(world_a, runtimes_a, ticks=1)
    run(world_b, runtimes_b, ticks=1)
    assert runtimes_a["alice"].memory.get("seen_events").count("announcement") == 1
    assert runtimes_b["alice"].memory.get("seen_events").count("announcement") == 1
    # The source checkpoint's mailbox copy is untouched.
    assert base.world["mailboxes"]["alice"][-1]["type"] == "announcement"
    assert copy.deepcopy(base.to_dict())["world"]["mailboxes"]["alice"][-1][
        "type"
    ] == "announcement"
