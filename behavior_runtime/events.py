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
    caused_by: int | None = None
    actor_incarnation: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "payload": dict(self.payload),
            "time": self.time,
            "actor_id": self.actor_id,
            "action_run_id": self.action_run_id,
            "sequence": self.sequence,
            "caused_by": self.caused_by,
            "actor_incarnation": self.actor_incarnation,
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
            caused_by=values.get("caused_by"),
            actor_incarnation=values.get("actor_incarnation"),
        )


EventHandler = Callable[[Event], None]


class EventBus:
    """Ordered in-process event bus with deterministic delayed delivery."""

    def __init__(self) -> None:
        self._sequence = 0
        self._due: list[Event] = []
        self._scheduled: list[tuple[float, int, Event]] = []
        self._handlers: list[EventHandler] = []
        self._current_sequence: int | None = None
        self._dispatch_now: float | None = None

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
        caused_by: int | None = None,
        actor_incarnation: int | None = None,
    ) -> Event:
        if delay < 0:
            raise ValueError("event delay must be non-negative")
        self._sequence += 1
        if caused_by is None and self._current_sequence is not None:
            caused_by = self._current_sequence
        event = Event(
            type=type,
            payload=dict(payload or {}),
            time=time + delay,
            actor_id=actor_id,
            action_run_id=action_run_id,
            sequence=self._sequence,
            caused_by=caused_by,
            actor_incarnation=actor_incarnation,
        )
        if self._dispatch_now is not None and event.time <= self._dispatch_now:
            self._due.append(event)
        else:
            heappush(self._scheduled, (event.time, event.sequence, event))
        return event

    def dispatch_due(self, now: float) -> list[Event]:
        dispatched: list[Event] = []
        self._dispatch_now = now
        try:
            while self._due or (
                self._scheduled and self._scheduled[0][0] <= now
            ):
                if self._due:
                    event = self._due.pop(0)
                else:
                    _, _, event = heappop(self._scheduled)
                dispatched.append(event)
                previous_sequence = self._current_sequence
                self._current_sequence = event.sequence
                for handler in tuple(self._handlers):
                    handler(event)
                self._current_sequence = previous_sequence
        finally:
            self._dispatch_now = None
        return dispatched

    @property
    def pending_count(self) -> int:
        return len(self._scheduled) + len(self._due)

    def snapshot(self) -> dict[str, Any]:
        return {
            "sequence": self._sequence,
            "scheduled": [event.to_dict() for _, _, event in sorted(self._scheduled)],
            "due": [event.to_dict() for event in self._due],
        }

    def restore(self, data: dict[str, Any]) -> None:
        self._sequence = int(data.get("sequence", 0))
        scheduled = []
        for event_data in data.get("scheduled", ()):
            event = Event.from_dict(event_data)
            scheduled.append((event.time, event.sequence, event))
        due = [Event.from_dict(event_data) for event_data in data.get("due", ())]
        heapify(scheduled)
        self._scheduled = scheduled
        self._due = list(due)
        self._handlers = []
        self._current_sequence = None

    def cancel_for_actor(
        self, actor_id: str, incarnation: int | None = None
    ) -> None:
        remaining = []
        for time, sequence, event in self._scheduled:
            if event.actor_id != actor_id:
                remaining.append((time, sequence, event))
                continue
            if incarnation is not None and event.actor_incarnation not in (
                None,
                incarnation,
            ):
                remaining.append((time, sequence, event))
        self._scheduled = remaining
        heapify(self._scheduled)
        self._due = [
            event
            for event in self._due
            if not (
                event.actor_id == actor_id
                and (
                    incarnation is None
                    or event.actor_incarnation in (None, incarnation)
                )
            )
        ]
