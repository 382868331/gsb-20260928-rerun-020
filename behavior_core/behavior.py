"""行为：根据共享观察和私有记忆决定本帧要做什么。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol, runtime_checkable

from .actions import Action


@dataclass(frozen=True)
class Decision:
    """None 表示维持当前动作；action 非 None 表示切换（当前动作将被中断）。"""

    action: Action | None = None
    reason: str = ""
    meta: dict[str, Any] | None = None

    @staticmethod
    def keep(reason: str = "") -> "Decision":
        return Decision(None, reason)

    @staticmethod
    def switch(action: Action, reason: str = "", **meta: Any) -> "Decision":
        return Decision(action, reason, dict(meta) if meta else None)


DecisionFn = Callable[[str, dict[str, Any], Any, int], "Action | Decision | None"]


@runtime_checkable
class Behavior(Protocol):
    """有状态行为实现 decide；无状态场景可直接传函数。"""

    def decide(
        self,
        actor_id: str,
        observation: dict[str, Any],
        memory: Any,
        tick: int,
    ) -> "Action | Decision | None": ...


def normalize_decision(raw: Any) -> Decision:
    if raw is None:
        return Decision.keep()
    if isinstance(raw, Decision):
        return raw
    if isinstance(raw, Action):
        return Decision.switch(raw)
    raise TypeError(f"不支持的决策结果: {raw!r}")
