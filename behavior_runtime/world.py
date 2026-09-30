from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .clock import Clock, VirtualClock
from .events import Event, EventBus, event_from_data, event_to_data


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

    def to_data(self) -> dict[str, Any]:
        """Capture shared state, time, pending events and unread mailboxes."""
        if not hasattr(self.clock, "to_data"):
            raise TypeError("world clock must support to_data() to be archived")
        return {
            "state": deepcopy(self._state),
            "clock": self.clock.to_data(),
            "events": self.events.to_data(),
            "actors": sorted(self._actors),
            "mailboxes": {
                actor_id: [event_to_data(event) for event in mailbox]
                for actor_id, mailbox in self._mailboxes.items()
            },
        }

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> "World":
        clock = VirtualClock()
        clock.load_data(data["clock"])
        event_bus = EventBus()
        event_bus.load_data(data["events"])
        world = cls(deepcopy(data.get("state") or {}), clock=clock, event_bus=event_bus)
        world._actors = set(data.get("actors") or ())
        world._mailboxes = {
            actor_id: [event_from_data(event_data) for event_data in mailbox]
            for actor_id, mailbox in (data.get("mailboxes") or {}).items()
        }
        for actor_id in world._actors:
            world._mailboxes.setdefault(actor_id, [])
        return world
