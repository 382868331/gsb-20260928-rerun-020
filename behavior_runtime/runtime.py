from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Iterable

from .actions import Action, ActionContext, ActionStatus
from .events import Event
from .memory import Memory
from .policy import Decision, KeepCurrent, StartAction, StopCurrent
from .world import Observation, World


@dataclass(frozen=True, slots=True)
class RuntimeSnapshot:
    actor_id: str
    run_id: int
    status: str | None
    action_name: str | None
    action_config: dict[str, Any] | None
    memory: dict[str, Any]
    stale_results_ignored: int
    interrupted_runs: tuple[int, ...]
    history: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "actor_id": self.actor_id,
            "run_id": self.run_id,
            "status": self.status,
            "action_name": self.action_name,
            "action_config": self.action_config,
            "memory": self.memory,
            "stale_results_ignored": self.stale_results_ignored,
            "interrupted_runs": list(self.interrupted_runs),
            "history": list(self.history),
        }

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> "RuntimeSnapshot":
        return cls(
            actor_id=values["actor_id"],
            run_id=values["run_id"],
            status=values["status"],
            action_name=values["action_name"],
            action_config=values["action_config"],
            memory=dict(values["memory"]),
            stale_results_ignored=values["stale_results_ignored"],
            interrupted_runs=tuple(values["interrupted_runs"]),
            history=tuple(values["history"]),
        )


class ActorRuntime:
    """Independent behavior state machine for one actor."""

    LIFECYCLE_EVENT_TYPES = frozenset(
        {"action_interrupted", "action_finished", "stale_action_result_ignored"}
    )

    def __init__(
        self,
        actor_id: str,
        policy: Any,
        world: World,
        *,
        action_types: Iterable[type[Action]] = (),
        memory: Memory | None = None,
    ) -> None:
        self.actor_id = actor_id
        self.policy = policy
        self.world = world
        self.world.register_actor(actor_id)
        self.memory = memory or Memory()
        self._action_types = {action_type.action_name: action_type for action_type in action_types}
        self.run_id = 0
        self.current_action: Action | None = None
        self.status: ActionStatus | None = None
        self.stale_results_ignored = 0
        self.interrupted_runs: list[int] = []
        self.history: list[dict[str, Any]] = []

    def register_action_type(self, action_type: type[Action]) -> None:
        self._action_types[action_type.action_name] = action_type

    def tick(self) -> ActionStatus | None:
        observation = self.world.observe(self.actor_id)
        current_events, stale_events = self._partition_events(observation)
        for event in stale_events:
            self.stale_results_ignored += 1
            self.world.publish(
                "stale_action_result_ignored",
                {
                    "event_type": event.type,
                    "owner_run_id": event.action_run_id,
                    "current_run_id": self.run_id,
                },
                actor_id=self.actor_id,
                action_run_id=self.run_id,
            )

        decision = self.policy.decide(observation, self.memory, self.current_action)
        self._apply_decision(decision, observation, current_events)

        if self.current_action is None:
            return self.status

        context = self._context(observation, current_events)
        status = self.current_action.tick(context)
        if status is ActionStatus.RUNNING:
            self.status = status
            return status
        if not status.terminal:
            raise ValueError(f"action returned non-terminal status: {status!r}")
        self._finish_run(status, context)
        return status

    def stop(self) -> ActionStatus | None:
        if self.current_action is None:
            return self.status
        observation = self.world.observe(self.actor_id)
        current_events, stale_events = self._partition_events(observation)
        self.stale_results_ignored += len(stale_events)
        context = self._context(observation, ())
        status = self._interrupt_current(context)
        self.current_action = None
        self.status = status
        return status

    def snapshot(self) -> RuntimeSnapshot:
        action_name = None
        action_config = None
        status = None
        if self.current_action is not None:
            action_name = self.current_action.action_name
            action_config = self.current_action.to_config()
            status = self.status.value if self.status is not None else None
        return RuntimeSnapshot(
            actor_id=self.actor_id,
            run_id=self.run_id,
            status=status,
            action_name=action_name,
            action_config=action_config,
            memory=self.memory.as_dict(),
            stale_results_ignored=self.stale_results_ignored,
            interrupted_runs=tuple(self.interrupted_runs),
            history=tuple(self.history),
        )

    def restore(self, snapshot: RuntimeSnapshot | dict[str, Any]) -> None:
        if isinstance(snapshot, dict):
            snapshot = RuntimeSnapshot.from_dict(snapshot)
        if snapshot.actor_id != self.actor_id:
            raise ValueError("snapshot belongs to another actor")
        self.run_id = snapshot.run_id
        self.memory.load_dict(snapshot.memory)
        self.stale_results_ignored = snapshot.stale_results_ignored
        self.interrupted_runs = list(snapshot.interrupted_runs)
        self.history = list(snapshot.history)
        if snapshot.action_name is None:
            self.current_action = None
            self.status = None
            return
        action_type = self._action_types.get(snapshot.action_name)
        if action_type is None:
            raise KeyError(f"unregistered action type: {snapshot.action_name}")
        self.current_action = action_type.from_config(snapshot.action_config or {})
        self.status = ActionStatus(snapshot.status) if snapshot.status else ActionStatus.RUNNING

    def _apply_decision(
        self,
        decision: Decision,
        observation: Observation,
        current_events: tuple[Event, ...],
    ) -> None:
        if isinstance(decision, KeepCurrent):
            return
        if isinstance(decision, StopCurrent):
            if self.current_action is not None:
                context = self._context(observation, ())
                status = self._interrupt_current(context)
                self.current_action = None
                self.status = status
            return
        if not isinstance(decision, StartAction):
            raise TypeError(f"unsupported decision: {decision!r}")

        if self.current_action is not None:
            context = self._context(observation, ())
            status = self._interrupt_current(context)
            self.current_action = None
            self.status = status

        self.run_id += 1
        self.current_action = decision.action
        self._action_types.setdefault(
            decision.action.action_name, type(decision.action)
        )
        self.status = ActionStatus.RUNNING
        start_context = self._context(observation, current_events)
        self.current_action.on_start(start_context)
        self._record("started", self.current_action, start_context.run_id)

    def _interrupt_current(self, context: ActionContext) -> ActionStatus:
        if self.current_action is None:
            raise RuntimeError("no current action to interrupt")
        action = self.current_action
        status = action.interrupt(context)
        if not status.terminal:
            status = ActionStatus.CANCELLED
        self.interrupted_runs.append(context.run_id)
        self._record("interrupted", action, context.run_id, status)
        self.world.publish(
            "action_interrupted",
            {"action": action.action_name, "status": status.value},
            actor_id=self.actor_id,
            action_run_id=context.run_id,
        )
        self._finish_run(status, context, publish_finished=False)
        return status

    def _finish_run(
        self,
        status: ActionStatus,
        context: ActionContext,
        *,
        publish_finished: bool = True,
    ) -> None:
        action = self.current_action
        if action is None:
            return
        self.status = status
        self._record("finished", action, context.run_id, status)
        if publish_finished:
            self.world.publish(
                "action_finished",
                {"action": action.action_name, "status": status.value},
                actor_id=self.actor_id,
                action_run_id=context.run_id,
            )
        self.current_action = None

    def _partition_events(
        self, observation: Observation
    ) -> tuple[tuple[Event, ...], tuple[Event, ...]]:
        current: list[Event] = []
        stale: list[Event] = []
        for event in observation.events:
            if event.action_run_id is None:
                continue
            if event.type in self.LIFECYCLE_EVENT_TYPES:
                continue
            if self.current_action is not None and event.action_run_id == self.run_id:
                current.append(event)
            else:
                stale.append(event)
        return tuple(current), tuple(stale)

    def _context(
        self, observation: Observation, result_events: tuple[Event, ...]
    ) -> ActionContext:
        return ActionContext(
            actor_id=self.actor_id,
            run_id=self.run_id,
            world=self.world,
            observation=observation,
            memory=self.memory,
            result_events=result_events,
        )

    def _record(
        self,
        phase: str,
        action: Action,
        run_id: int,
        status: ActionStatus | None = None,
    ) -> None:
        self.history.append(
            {
                "time": self.world.clock.now(),
                "phase": phase,
                "run_id": run_id,
                "action": action.action_name,
                "action_config": deepcopy(action.to_config()),
                "status": status.value if status else None,
            }
        )
