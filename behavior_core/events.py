"""全局事件日志：所有角色共享同一套单调序号，用于核对先后关系。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .clock import Timestamp


@dataclass(frozen=True)
class Event:
    seq: int
    tick: int
    us: int
    kind: str
    actor_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)


class EventLog:
    """append-only 事件流。seq 全局单调，跨存档恢复后继续递增。"""

    def __init__(self) -> None:
        self._events: list[Event] = []
        self._next_seq = 1

    def append(
        self,
        kind: str,
        now: Timestamp,
        *,
        actor_id: str | None = None,
        **payload: Any,
    ) -> Event:
        event = Event(
            seq=self._next_seq,
            tick=now.tick,
            us=now.us,
            kind=kind,
            actor_id=actor_id,
            payload=dict(payload),
        )
        self._next_seq += 1
        self._events.append(event)
        return event

    def events(self, kind: str | None = None) -> list[Event]:
        if kind is None:
            return list(self._events)
        return [e for e in self._events if e.kind == kind]

    def kinds(self) -> list[str]:
        return [e.kind for e in self._events]

    def get_state(self) -> dict:
        return {
            "next_seq": self._next_seq,
            "events": [
                {
                    "seq": e.seq,
                    "tick": e.tick,
                    "us": e.us,
                    "kind": e.kind,
                    "actor_id": e.actor_id,
                    "payload": e.payload,
                }
                for e in self._events
            ],
        }

    @classmethod
    def from_state(cls, state: dict) -> "EventLog":
        log = cls()
        log._events = [
            Event(
                seq=item["seq"],
                tick=item["tick"],
                us=item["us"],
                kind=item["kind"],
                actor_id=item["actor_id"],
                payload=dict(item["payload"]),
            )
            for item in state["events"]
        ]
        log._next_seq = state["next_seq"]
        return log
