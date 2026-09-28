from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .clock import Clock, VirtualClock
from .events import Event, EventBus


@dataclass(frozen=True, slots=True)
class Observation:
    actor_id: str
    time: float
    state: dict[str, Any]
    events: tuple[Event, ...]


class World:
    """Shared, explicitly mutated world state and event queue."""

    def __init__(
        self,
        state: dict[str, Any] | None = None,
        *,
        clock: Clock | None = None,
        event_bus: EventBus | None = None,
    ) -> None:
        self._state = deepcopy(state or {})
        self.clock: Clock = clock or VirtualClock()
        self.events = event_bus or EventBus()
        self._mailboxes: dict[str, list[Event]] = {}
        self._actors: set[str] = set()

    def register_actor(self, actor_id: str) -> None:
        self._actors.add(actor_id)
        self._mailboxes.setdefault(actor_id, [])

    @property
    def state(self) -> dict[str, Any]:
        return self._state

    def observe(self, actor_id: str) -> Observation:
        events = tuple(self._mailboxes.pop(actor_id, []))
        return Observation(
            actor_id=actor_id,
            time=self.clock.now(),
            state=deepcopy(self._state),
            events=events,
        )

    def publish(
        self,
        type: str,
        payload: dict[str, Any] | None = None,
        *,
        actor_id: str | None = None,
        action_run_id: int | None = None,
        delay: float = 0.0,
    ) -> Event:
        return self.events.publish(
            type,
            payload,
            time=self.clock.now(),
            actor_id=actor_id,
            action_run_id=action_run_id,
            delay=delay,
        )

    def dispatch_due(self) -> list[Event]:
        dispatched = self.events.dispatch_due(self.clock.now())
        for event in dispatched:
            if event.actor_id is not None:
                self._mailboxes.setdefault(event.actor_id, []).append(event)
            else:
                for actor_id in self._actors:
                    self._mailboxes.setdefault(actor_id, []).append(event)
        return dispatched
