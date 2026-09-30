from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .actions import Action, ActionContext, ActionStatus
from .memory import Memory
from .policy import Decision, KeepCurrent, StartAction
from .world import Observation


@dataclass
class HoldPosition(Action):
    duration: float = 1.0

    def on_start(self, context: ActionContext) -> None:
        context.memory.set("holding_run", context.run_id)
        context.emit("position_hold_began", {"duration": self.duration})

    def tick(self, context: ActionContext) -> ActionStatus:
        if context.observation.time >= context.observation.state.get("hold_until", 0.0):
            context.memory.set("last_held_run", context.run_id)
            return ActionStatus.COMPLETED
        return ActionStatus.RUNNING

    def interrupt(self, context: ActionContext) -> ActionStatus:
        context.memory.set("released_hold_run", context.run_id)
        context.emit("position_hold_released", {"run_id": context.run_id})
        return ActionStatus.CANCELLED

    def to_config(self) -> dict[str, Any]:
        return {"duration": self.duration}

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "HoldPosition":
        return cls(duration=float(config.get("duration", 1.0)))


@dataclass
class MoveToTarget(Action):
    target: str
    travel_time: float = 2.0
    interrupt_count: int = 0

    def on_start(self, context: ActionContext) -> None:
        started_at = context.observation.time
        due_at = started_at + self.travel_time
        context.emit(
            "movement_began",
            {"target": self.target, "due_at": due_at},
        )
        context.emit_result(
            "movement_arrived",
            {"target": self.target},
            delay=self.travel_time,
        )
        context.memory.set(
            "active_movement",
            {"run_id": context.run_id, "target": self.target, "due_at": due_at},
        )

    def tick(self, context: ActionContext) -> ActionStatus:
        arrived = any(
            event.type == "movement_arrived"
            and event.payload.get("target") == self.target
            for event in context.result_events
        )
        if arrived:
            context.memory.set("last_target", self.target)
            context.emit("movement_completed", {"target": self.target})
            return ActionStatus.COMPLETED
        return ActionStatus.RUNNING

    def interrupt(self, context: ActionContext) -> ActionStatus:
        self.interrupt_count += 1
        context.memory.set(
            "last_interruption",
            {"run_id": context.run_id, "target": self.target, "count": self.interrupt_count},
        )
        context.emit(
            "movement_cleanup",
            {"target": self.target, "cleanup_count": self.interrupt_count},
        )
        return ActionStatus.CANCELLED

    def to_config(self) -> dict[str, Any]:
        return {
            "target": self.target,
            "travel_time": self.travel_time,
            "interrupt_count": self.interrupt_count,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "MoveToTarget":
        return cls(
            target=str(config["target"]),
            travel_time=float(config.get("travel_time", 2.0)),
            interrupt_count=int(config.get("interrupt_count", 0)),
        )


class TargetPolicy:
    """Switch movement whenever the shared target changes."""

    def __init__(self, idle_action: Action | None = None) -> None:
        self.idle_action = idle_action

    def decide(
        self,
        observation: Observation,
        memory: Memory,
        current_action: Action | None,
    ) -> Decision:
        target = observation.state.get("target")
        remembered = memory.get("active_target")
        if target != remembered:
            memory.set("active_target", target)
            memory.set("target_changed_at", observation.time)
        if target is None:
            if isinstance(current_action, HoldPosition):
                return KeepCurrent()
            if self.idle_action is not None:
                return StartAction(self.idle_action)
            return KeepCurrent()
        if not isinstance(current_action, MoveToTarget) or current_action.target != target:
            return StartAction(MoveToTarget(target=target))
        return KeepCurrent()
