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
        self._actor_order: list[str] = []
        self._actor_incarnations: dict[str, int] = {}
        self._action_sequence = 0

    def register_actor(self, actor_id: str) -> None:
        if actor_id in self._actors:
            self._mailboxes.setdefault(actor_id, [])
            return
        self._actors.add(actor_id)
        self._actor_order.append(actor_id)
        self._actor_incarnations[actor_id] = (
            self._actor_incarnations.get(actor_id, 0) + 1
        )
        self._mailboxes.setdefault(actor_id, [])

    def remove_actor(self, actor_id: str) -> None:
        incarnation = self._actor_incarnations.get(actor_id)
        self._actors.discard(actor_id)
        if actor_id in self._actor_order:
            self._actor_order.remove(actor_id)
        self._actor_incarnations.pop(actor_id, None)
        self._mailboxes.pop(actor_id, None)
        self.events.cancel_for_actor(actor_id, incarnation)

    @property
    def state(self) -> dict[str, Any]:
        return self._state

    @property
    def actor_ids(self) -> tuple[str, ...]:
        return tuple(self._actor_order)

    def action_order(self, actor_id: str) -> tuple[int, int]:
        self._action_sequence += 1
        return self._action_sequence, self._actor_order.index(actor_id)

    def snapshot(self) -> dict[str, Any]:
        return {
            "state": deepcopy(self._state),
            "time": self.clock.now(),
            "clock": deepcopy(self.clock.snapshot()),
            "events": deepcopy(self.events.snapshot()),
            "actors": list(self._actors),
            "actor_order": list(self._actor_order),
            "actor_incarnations": dict(self._actor_incarnations),
            "action_sequence": self._action_sequence,
            "mailboxes": {
                actor_id: [
                    event.to_dict()
                    for event in self._mailboxes.get(actor_id, ())
                ]
                for actor_id in self._actors
            },
        }

    def restore(self, data: dict[str, Any]) -> None:
        self._state = deepcopy(data["state"])
        clock_data = data.get("clock") or {"time": data.get("time", 0.0)}
        if not hasattr(self.clock, "restore"):
            raise TypeError("world clock does not support restoration")
        self.clock.restore(clock_data)
        self.events.restore(deepcopy(data["events"]))
        self._actors = set(data.get("actors", ()))
        restored_order = data.get("actor_order")
        self._actor_order = (
            list(restored_order)
            if restored_order is not None
            else sorted(self._actors)
        )
        self._actor_order = [
            actor_id for actor_id in self._actor_order if actor_id in self._actors
        ]
        for actor_id in sorted(self._actors.difference(self._actor_order)):
            self._actor_order.append(actor_id)
        restored_incarnations = data.get("actor_incarnations")
        if restored_incarnations is not None:
            self._actor_incarnations = {
                actor_id: int(generation)
                for actor_id, generation in restored_incarnations.items()
                if actor_id in self._actors
            }
        else:
            self._actor_incarnations = {
                actor_id: 1 for actor_id in self._actor_order
            }
        self._action_sequence = int(data.get("action_sequence", 0))
        self._mailboxes = {
            actor_id: [Event.from_dict(event_data) for event_data in events]
            for actor_id, events in data.get("mailboxes", {}).items()
        }
        for actor_id in self._actors:
            self._mailboxes.setdefault(actor_id, [])

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
            actor_incarnation=(
                self._actor_incarnations.get(actor_id)
                if actor_id is not None
                else None
            ),
        )

    def dispatch_due(self) -> list[Event]:
        active_actors = self.actor_ids
        active_incarnations = dict(self._actor_incarnations)
        dispatched = self.events.dispatch_due(self.clock.now())
        for event in dispatched:
            if event.actor_id is not None:
                incarnation = active_incarnations.get(event.actor_id)
                if (
                    event.actor_id in active_actors
                    and (
                        event.actor_incarnation is None
                        or event.actor_incarnation == incarnation
                    )
                ):
                    self._mailboxes.setdefault(event.actor_id, []).append(event)
            else:
                for actor_id in active_actors:
                    self._mailboxes.setdefault(actor_id, []).append(event)
        for actor_id in list(self._mailboxes):
            if actor_id not in self._actors:
                del self._mailboxes[actor_id]
        return dispatched
