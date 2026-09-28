from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable
from heapq import heappop, heappush


@dataclass(frozen=True, slots=True)
class Event:
    type: str
    payload: dict[str, Any] = field(default_factory=dict)
    time: float = 0.0
    actor_id: str | None = None
    action_run_id: int | None = None
    sequence: int = 0


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
        while self._scheduled and self._scheduled[0][0] <= now:
            _, _, event = heappop(self._scheduled)
            self._due.append(event)

        while self._due:
            event = self._due.pop(0)
            dispatched.append(event)
            for handler in tuple(self._handlers):
                handler(event)
        return dispatched

    @property
    def pending_count(self) -> int:
        return len(self._scheduled) + len(self._due)
