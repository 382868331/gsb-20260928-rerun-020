from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .clock import Clock, VirtualClock
from .events import Event, EventBus
from .promises import PromiseBook


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
        self._actors: dict[str, None] = {}
        self._batch_participants: tuple[str, ...] = ()
        self._batch_new_members: set[str] = set()
        self._pending_actor_changes: dict[str, bool] = {}
        self._dispatching = False
        self.promises = PromiseBook(self)

    def register_actor(self, actor_id: str) -> None:
        if self._dispatching:
            self._pending_actor_changes[actor_id] = True
            return
        self._actors.setdefault(actor_id, None)
        self._mailboxes.setdefault(actor_id, [])

    def remove_actor(self, actor_id: str) -> None:
        if self._dispatching:
            self._pending_actor_changes[actor_id] = False
            return
        self._actors.pop(actor_id, None)
        self._mailboxes.pop(actor_id, None)
        self.events.discard_actor_events(actor_id)
        self.promises.settle_participant_left(actor_id)

    def unregister_actor(self, actor_id: str) -> None:
        self.remove_actor(actor_id)

    def actor_order(self, actor_id: str) -> int | None:
        try:
            return list(self._actors).index(actor_id)
        except ValueError:
            return None

    @property
    def state(self) -> dict[str, Any]:
        return self._state

    @property
    def actor_ids(self) -> tuple[str, ...]:
        return tuple(self._actors)

    def snapshot(self) -> dict[str, Any]:
        return {
            "state": deepcopy(self._state),
            "time": self.clock.now(),
            "clock": deepcopy(self.clock.snapshot()),
            "events": deepcopy(self.events.snapshot()),
            "actors": list(self._actors),
            "actor_order": list(self._actors),
            "promises": deepcopy(self.promises.snapshot()),
            "mailboxes": {
                actor_id: [
                    event.to_dict()
                    for event in self._mailboxes.get(actor_id, ())
                ]
                for actor_id in self._actors
            },
        }

    def restore(self, data: dict[str, Any]) -> None:
        self._dispatching = False
        self._pending_actor_changes = {}
        self._batch_participants = ()
        self._batch_new_members = set()
        self._state = deepcopy(data["state"])
        clock_data = data.get("clock") or {"time": data.get("time", 0.0)}
        if not hasattr(self.clock, "restore"):
            raise TypeError("world clock does not support restoration")
        self.clock.restore(clock_data)
        self.events.restore(deepcopy(data["events"]))
        # EventBus.restore drops handlers (they may capture stale branches);
        # re-subscribe this world's promise ledger to the fresh bus.
        self.promises = PromiseBook(self)
        self.promises.restore(deepcopy(data.get("promises")))
        actor_ids = data.get("actor_order", data.get("actors", ()))
        self._actors = dict.fromkeys(actor_ids)
        self._mailboxes = {
            actor_id: [Event.from_dict(event_data) for event_data in events]
            for actor_id, events in data.get("mailboxes", {}).items()
            if actor_id in self._actors
        }
        for actor_id in self._actors:
            self._mailboxes.setdefault(actor_id, [])
        for event_data in list(self.events.snapshot().get("scheduled", ())) + list(
            self.events.snapshot().get("due", ())
        ):
            target_actor_id = event_data.get("actor_id")
            if target_actor_id is not None and target_actor_id not in self._actors:
                self.events.discard_actor_events(target_actor_id)

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
        self._dispatching = True
        self._batch_participants = tuple(self._actors)
        try:
            dispatched = self.events.dispatch_due(self.clock.now())
        finally:
            self._dispatching = False
            self._apply_pending_actor_changes()
        for event in dispatched:
            if event.actor_id is not None:
                if event.actor_id in self._actors:
                    self._mailboxes.setdefault(event.actor_id, []).append(event)
            else:
                for actor_id in self._batch_participants:
                    if actor_id in self._actors and actor_id not in self._batch_new_members:
                        self._mailboxes.setdefault(actor_id, []).append(event)
        self._batch_participants = ()
        self._batch_new_members = set()
        return dispatched

    def _apply_pending_actor_changes(self) -> None:
        while self._pending_actor_changes:
            actor_id = next(iter(self._pending_actor_changes))
            should_join = self._pending_actor_changes.pop(actor_id)
            if should_join:
                self._batch_new_members.add(actor_id)
                self.register_actor(actor_id)
            else:
                self.remove_actor(actor_id)
