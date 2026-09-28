from __future__ import annotations

from typing import Protocol


class Clock(Protocol):
    """Read-only clock used by actions and policies."""

    def now(self) -> float: ...


class VirtualClock:
    """Deterministic clock advanced explicitly by the embedding application."""

    def __init__(self, initial_time: float = 0.0) -> None:
        self._time = float(initial_time)

    def now(self) -> float:
        return self._time

    def advance(self, delta_seconds: float) -> float:
        if delta_seconds < 0:
            raise ValueError("delta_seconds must be non-negative")
        self._time += float(delta_seconds)
        return self._time
