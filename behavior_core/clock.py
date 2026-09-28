"""可控时钟：时间只能通过 tick 前进，便于确定性测试。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Timestamp:
    tick: int
    us: int

    def __str__(self) -> str:
        return f"t={self.tick}({self.us}us)"


class Clock:
    """单调时钟。tick 是逻辑帧，us 是对应的可控物理时间（微秒）。"""

    def __init__(self, tick_us: int = 100_000, *, tick: int = 0, us: int = 0):
        if tick_us <= 0:
            raise ValueError("tick_us 必须为正数")
        self._tick_us = tick_us
        self._tick = tick
        self._us = us

    @property
    def tick_us(self) -> int:
        return self._tick_us

    def now(self) -> Timestamp:
        return Timestamp(self._tick, self._us)

    def advance(self, ticks: int = 1) -> Timestamp:
        if ticks < 1:
            raise ValueError("每次至少前进一个 tick")
        self._tick += ticks
        self._us += ticks * self._tick_us
        return self.now()

    def get_state(self) -> dict:
        return {"tick_us": self._tick_us, "tick": self._tick, "us": self._us}

    @classmethod
    def from_state(cls, state: dict) -> "Clock":
        return cls(state["tick_us"], tick=state["tick"], us=state["us"])
