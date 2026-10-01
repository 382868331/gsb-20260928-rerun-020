from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable
from heapq import heapify, heappop, heappush


@dataclass(frozen=True, slots=True)
class Event:
    type: str
    payload: dict[str, Any] = field(default_factory=dict)
    time: float = 0.0
    actor_id: str | None = None
    action_run_id: int | None = None
    sequence: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "payload": dict(self.payload),
            "time": self.time,
            "actor_id": self.actor_id,
            "action_run_id": self.action_run_id,
            "sequence": self.sequence,
        }

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> "Event":
        return cls(
            type=values["type"],
            payload=dict(values.get("payload") or {}),
            time=float(values["time"]),
            actor_id=values.get("actor_id"),
            action_run_id=values.get("action_run_id"),
            sequence=int(values.get("sequence", 0)),
        )


EventHandler = Callable[[Event], None]


class EventBus:
    """Ordered in-process event bus with deterministic delayed delivery."""

    def __init__(self) -> None:
        self._sequence = 0
        self._due: list[Event] = []
        self._scheduled: list[tuple[float, int, Event]] = []
        self._handlers: list[EventHandler] = []

    def subscribe(self, handler: EventHandler) -> Callable[[], None]:
        self._handlers.append(handler)

        def unsubscribe() -> None:
            try:
                self._handlers.remove(handler)
            except ValueError:
                pass

        return unsubscribe

    def publish(
        self,
        type: str,
        payload: dict[str, Any] | None = None,
        *,
        time: float,
        actor_id: str | None = None,
        action_run_id: int | None = None,
        delay: float = 0.0,
    ) -> Event:
        if delay < 0:
            raise ValueError("event delay must be non-negative")
        self._sequence += 1
        event = Event(
            type=type,
            payload=dict(payload or {}),
            time=time + delay,
            actor_id=actor_id,
            action_run_id=action_run_id,
            sequence=self._sequence,
        )
        heappush(self._scheduled, (event.time, event.sequence, event))
        return event

    def dispatch_due(self, now: float) -> list[Event]:
        dispatched: list[Event] = []
        while self._due or (
            self._scheduled and self._scheduled[0][0] <= now
        ):
            if self._due:
                event = self._due.pop(0)
            else:
                _, _, event = heappop(self._scheduled)
            dispatched.append(event)
            for handler in tuple(self._handlers):
                handler(event)
        return dispatched

    def discard_actor_events(self, actor_id: str) -> None:
        self._scheduled = [
            entry
            for entry in self._scheduled
            if entry[2].actor_id != actor_id
        ]
        heapify(self._scheduled)
        self._due = [
            event for event in self._due if event.actor_id != actor_id
        ]

    def discard_promise_events(self, promise_id: str) -> None:
        """Drop every still-queued event that belongs to one promise."""

        def belongs(entry_event: Event) -> bool:
            return entry_event.payload.get("promise_id") == promise_id

        self._scheduled = [
            entry
            for entry in self._scheduled
            if not belongs(entry[2])
        ]
        heapify(self._scheduled)
        self._due = [
            event for event in self._due if not belongs(event)
        ]

    @property
    def pending_count(self) -> int:
        return len(self._scheduled) + len(self._due)

    def snapshot(self) -> dict[str, Any]:
        return {
            "sequence": self._sequence,
            "scheduled": [event.to_dict() for _, _, event in self._scheduled],
            "due": [event.to_dict() for event in self._due],
        }

    def restore(self, data: dict[str, Any]) -> None:
        self._sequence = int(data.get("sequence", 0))
        scheduled = []
        for event_data in data.get("scheduled", ()):
            event = Event.from_dict(event_data)
            scheduled.append((event.time, event.sequence, event))
        heapify(scheduled)
        self._scheduled = scheduled
        self._due = [Event.from_dict(event_data) for event_data in data.get("due", ())]
        self._handlers = []
