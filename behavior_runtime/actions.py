from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Self

from .events import Event

if TYPE_CHECKING:
    from .memory import Memory
    from .world import Observation, World


class ActionStatus(Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self is not ActionStatus.RUNNING


Continue = ActionStatus.RUNNING
Complete = ActionStatus.COMPLETED
Fail = ActionStatus.FAILED
Cancel = ActionStatus.CANCELLED


@dataclass(frozen=True, slots=True)
class ActionContext:
    actor_id: str
    run_id: int
    world: "World"
    observation: "Observation"
    memory: "Memory"
    result_events: tuple[Event, ...]

    def emit(self, event_type: str, payload: dict[str, Any] | None = None) -> None:
        self.world.publish(event_type, payload, actor_id=self.actor_id)

    def emit_result(
        self,
        event_type: str,
        payload: dict[str, Any] | None = None,
        *,
        delay: float = 0.0,
    ) -> None:
        self.world.publish(
            event_type,
            payload,
            actor_id=self.actor_id,
            action_run_id=self.run_id,
            delay=delay,
        )


class Action:
    """Stable base contract for both instantaneous and sustained actions.

    Actions communicate through the supplied context. They do not own actor
    identity, do not inspect the runtime's current run id except through the
    context, and remain usable after future perception or behavior types are
    added.
    """

    action_name: ClassVar[str]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "action_name" not in cls.__dict__:
            cls.action_name = cls.__name__

    def on_start(self, context: ActionContext) -> None:
        """Called once immediately after a new run becomes current."""

    def tick(self, context: ActionContext) -> ActionStatus:
        raise NotImplementedError

    def interrupt(self, context: ActionContext) -> ActionStatus:
        """Called at most once when the current run is replaced or stopped."""
        return ActionStatus.CANCELLED

    def to_config(self) -> dict[str, Any]:
        raise NotImplementedError(
            f"{type(self).__name__}.to_config must be implemented for persistence"
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Self:
        raise NotImplementedError(
            f"{cls.__name__}.from_config must be implemented for persistence"
        )
