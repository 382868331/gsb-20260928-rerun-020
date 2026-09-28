from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from .actions import Action

if TYPE_CHECKING:
    from .memory import Memory
    from .world import Observation


@dataclass(frozen=True, slots=True)
class StartAction:
    action: Action


@dataclass(frozen=True, slots=True)
class KeepCurrent:
    pass


@dataclass(frozen=True, slots=True)
class StopCurrent:
    pass


Decision = StartAction | KeepCurrent | StopCurrent


class Policy(Protocol):
    def decide(
        self,
        observation: "Observation",
        memory: "Memory",
        current_action: Action | None,
    ) -> Decision: ...
