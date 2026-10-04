from __future__ import annotations

from behavior_runtime import ActorRuntime, VirtualClock, World, WorldArchive
from behavior_runtime.commitments import (
    RULE_PHASE_CANCEL_PRESERVES,
    RULE_PHASE_RESOURCE_CHECK,
)
from behavior_runtime.examples import MoveToTarget, TargetPolicy


def make_world(actor_ids=("alice", "bob", "carol", "dave")):
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


def request_task(world, requester_id, phase_specs, action="MoveToTarget"):
    phases = [
        {
            "name": name,
            "resources": resources,
            "participants": [
                {"role": "lead", "performer_id": performer, "delay": delay}
            ],
        }
        for (name, resources, performer, delay) in phase_specs
    ]
    return world.request_joint_task(requester_id=requester_id, action=action,
                                    phases=phases)


def test_phases_acquire_resources_only_on_entry():
    world, _ = make_world()
    seen = track_events(world)
    task = request_task(world, "alice", [
        ("prepare", ("dock",), "bob", 1),
        ("execute", ("crane",), "bob", 0),
        ("handoff", ("truck",), "carol", 0),
    ])

    world.clock.advance(1)
    world.dispatch_due()
    assert task.state == "started"
    assert task.phases[0]["state"] == "started"
    assert world.commitments.resource_holds() == {"dock": task.task_id}

    assert world.complete_joint_phase(task.task_id)
    # Members for the next phases are ready immediately, but ready members
    # do not reserve future phase resources ahead of time.
    status = world.joint_task_status(task.task_id)
    assert status["current_phase"] == "execute"
    assert status["phases"][1]["members"][0]["ready"] is True
    assert "crane" not in status["resource_holds"]
    assert "truck" not in world.commitments.resource_holds()

    world.dispatch_due()
    assert task.phases[1]["state"] == "started"
    assert world.commitments.resource_holds() == {
        "dock": task.task_id,
        "crane": task.task_id,
    }
    assert world.complete_joint_phase(task.task_id)
    world.dispatch_due()
    assert task.phases[2]["state"] == "started"
    assert world.complete_joint_phase(task.task_id)
    assert task.state == "completed"
    assert world.commitments.resource_holds() == {}

    world.dispatch_due()
    started = [e for e in seen if e.type == "joint_phase_started"]
    assert [e.payload["phase"] for e in started] == [
        "prepare", "execute", "handoff",
    ]
    assert len({e.payload["decision_id"] for e in started}) == 3
    assert [e for e in seen if e.type == "joint_task_completed"]


def test_phase_entry_explains_blocked_resource():
    world, _ = make_world()
    seen = track_events(world)
    holder = request_task(world, "alice", [
        ("prepare", ("dock",), "bob", 0),
        ("execute", (), "bob", 0),
    ])
    waiter = request_task(world, "carol", [
        ("prepare", ("dock",), "dave", 0),
    ])

    world.dispatch_due()
    assert holder.state == "started"
    assert waiter.state == "waiting"

    status = world.joint_task_status(waiter.task_id)
    assert status["blocked_resources"] == {"dock": holder.task_id}
    assert status["waiting_on"] == []

    deferred = [e for e in seen if e.type == "joint_phase_deferred"]
    assert len(deferred) == 1
    assert deferred[0].payload["blockers"] == {"dock": holder.task_id}
    assert waiter.history[-1]["rule"] == RULE_PHASE_RESOURCE_CHECK

    # Re-settling without changes does not spam duplicate deferral records.
    world.dispatch_due()
    assert len([e for e in seen if e.type == "joint_phase_deferred"]) == 1


def build_deadlock(world):
    task_a = request_task(world, "alice", [
        ("prepare", ("dock",), "alice", 0),
        ("execute", ("crane",), "alice", 0),
    ])
    task_b = request_task(world, "bob", [
        ("prepare", ("crane",), "bob", 0),
        ("execute", ("dock",), "bob", 0),
    ])
    independent = request_task(world, "carol", [
        ("prepare", ("yard",), "carol", 0),
    ])
    return task_a, task_b, independent


def test_cyclic_wait_is_diagnosable_and_cancel_unblocks():
    world, _ = make_world()
    seen = track_events(world)
    task_a, task_b, independent = build_deadlock(world)

    world.dispatch_due()
    assert (task_a.state, task_b.state, independent.state) == (
        "started", "started", "started",
    )
    for task in (task_a, task_b, independent):
        assert world.complete_joint_phase(task.task_id)
    world.dispatch_due()

    # The independent task ran to completion and released its resource.
    assert independent.state == "completed"
    assert "yard" not in world.commitments.resource_holds()

    # A and B now wait on each other's next-phase resource.
    assert task_a.state == "waiting"
    assert task_b.state == "waiting"
    status_a = world.joint_task_status(task_a.task_id)
    status_b = world.joint_task_status(task_b.task_id)
    assert status_a["blocked_resources"] == {"crane": task_b.task_id}
    assert status_b["blocked_resources"] == {"dock": task_a.task_id}

    cycles = world.detect_joint_wait_cycles()
    assert len(cycles) == 1
    cycle = cycles[0]
    assert set(cycle["tasks"]) == {task_a.task_id, task_b.task_id}
    assert cycle["waits"][task_a.task_id]["resource"] == "crane"
    assert cycle["waits"][task_a.task_id]["holder"] == task_b.task_id
    assert cycle["waits"][task_b.task_id]["resource"] == "dock"

    # Cancelling B's unstarted execute phase keeps its completed prepare
    # work on record and releases what B still holds.
    assert world.cancel_joint_phase(task_b.task_id, "execute", by="bob",
                                    reason="吊车改派")
    assert task_b.state == "cancelled"
    assert task_b.phases[0]["state"] == "completed"
    assert task_b.phases[1]["state"] == "cancelled"
    cancelled = [e for e in task_b.history if e["kind"] == "cancelled"]
    assert cancelled[-1]["rule"] == RULE_PHASE_CANCEL_PRESERVES
    completed_phases = [
        e for e in seen
        if e.type == "joint_phase_completed"
        and e.payload["task_id"] == task_b.task_id
    ]
    assert len(completed_phases) == 1
    assert completed_phases[0].payload["phase"] == "prepare"

    # A can now acquire the crane and finish.
    world.dispatch_due()
    assert world.detect_joint_wait_cycles() == []
    assert task_a.state == "started"
    assert task_a.phases[1]["state"] == "started"
    assert world.complete_joint_phase(task_a.task_id)
    assert task_a.state == "completed"
    assert world.commitments.resource_holds() == {}

    # A started phase that was never cancelled cannot be cancelled.
    other = request_task(world, "dave", [
        ("prepare", ("dock",), "dave", 0),
        ("execute", ("crane",), "dave", 0),
    ])
    world.dispatch_due()
    assert not world.cancel_joint_phase(other.task_id, "prepare", by="dave")
    assert other.phases[0]["state"] == "started"


def test_archive_restore_keeps_wait_state_without_replaying():
    world, runtimes = make_world()
    seen = track_events(world)
    task_a, task_b, independent = build_deadlock(world)

    world.dispatch_due()
    for task in (task_a, task_b, independent):
        assert world.complete_joint_phase(task.task_id)
    world.dispatch_due()
    assert len(world.detect_joint_wait_cycles()) == 1

    archive = WorldArchive.capture(world, runtimes, checkpoint_id="deadlock")
    branch_world, _ = archive.restore(
        {actor_id: TargetPolicy() for actor_id in runtimes},
        action_types={actor_id: [MoveToTarget] for actor_id in runtimes},
    )
    branch_seen = track_events(branch_world)

    # Waiting state, holds and member explanations survive the restore.
    status = branch_world.joint_task_status(task_a.task_id)
    assert status["state"] == "waiting"
    assert status["blocked_resources"] == {"crane": task_b.task_id}
    assert branch_world.commitments.resource_holds() == {
        "dock": task_a.task_id,
        "crane": task_b.task_id,
    }
    assert len(branch_world.detect_joint_wait_cycles()) == 1

    # Continuing the branch does not re-fire already executed phases.
    assert branch_world.cancel_joint_phase(task_b.task_id, "execute", by="bob")
    branch_world.dispatch_due()
    branch_started = [
        e for e in branch_seen
        if e.type == "joint_phase_started"
        and e.payload["task_id"] == task_a.task_id
    ]
    assert [e.payload["phase"] for e in branch_started] == ["execute"]
    assert branch_world.complete_joint_phase(task_a.task_id)
    assert branch_world.commitments.get_task(task_a.task_id).state == "completed"

    # The original world is untouched by the branch and matches its own log.
    assert world.commitments.get_task(task_a.task_id).state == "waiting"
    assert len([e for e in seen if e.type == "joint_phase_started"]) == 3

    # Restoring again from the same archive replays identically.
    replay_world, _ = archive.restore(
        {actor_id: TargetPolicy() for actor_id in runtimes},
        action_types={actor_id: [MoveToTarget] for actor_id in runtimes},
    )
    assert replay_world.joint_task_status(task_a.task_id) == status
    replay_world.cancel_joint_phase(task_b.task_id, "execute", by="bob")
    replay_world.dispatch_due()
    assert replay_world.joint_task_status(task_a.task_id)["state"] == "started"
