"""动作基本约定。

扩展方向（感知、更复杂行为）不改变这里的约定：
- step 每个 tick 被调用一次，返回 RUNNING / FINISHED / PENDING；
- 外部结果通过 execution_id 异步回传，运行器拒绝过期结果；
- 中断先调一次 on_interrupt，随后 finalize 只执行一次；
- 动作自己负责 save_state/load_state。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from .actor import ActorRunner


class ActionStatus(str, Enum):
    RUNNING = "running"
    FINISHED = "finished"
    PENDING = "pending"


@dataclass(frozen=True)
class PendingResult:
    """本 tick 动作没有结论，等待外部结果，结果送达时携带该 token。"""

    token: Any


@dataclass(frozen=True)
class ActionResult:
    status: ActionStatus
    success: bool = True
    outcome: dict[str, Any] = field(default_factory=dict)
    pending: PendingResult | None = None

    @staticmethod
    def running(**note: Any) -> "ActionResult":
        return ActionResult(ActionStatus.RUNNING, outcome=dict(note))

    @staticmethod
    def finished(success: bool = True, **outcome: Any) -> "ActionResult":
        return ActionResult(ActionStatus.FINISHED, success=success, outcome=dict(outcome))

    @staticmethod
    def pending(token: Any, **note: Any) -> "ActionResult":
        return ActionResult(
            ActionStatus.PENDING,
            outcome=dict(note),
            pending=PendingResult(token),
        )


@dataclass(frozen=True)
class ActionContext:
    """动作每个 tick 能接触到的一切。"""

    now_tick: int
    us: int
    observation: dict[str, Any]
    memory: Any
    log: Any
    actor_id: str
    execution_id: int

    def emit(self, kind: str, **payload: Any) -> None:
        from .clock import Timestamp

        self.log.append(
            kind,
            Timestamp(self.now_tick, self.us),
            actor_id=self.actor_id,
            execution_id=self.execution_id,
            **payload,
        )


class Action:
    """动作基类。子类至少实现 step；按需覆盖生命周期钩子。"""

    name: str = "action"

    def on_start(self, ctx: ActionContext) -> None:
        """动作成为当前动作时调用一次。"""

    def step(self, ctx: ActionContext) -> ActionResult:  # pragma: no cover - 抽象约定
        raise NotImplementedError

    def on_result(self, ctx: ActionContext, token: Any, payload: dict[str, Any]) -> ActionResult:
        """PENDING 动作收到外部结果时调用；默认把结果载荷当作成功结束。"""
        return ActionResult.finished(**payload)

    def on_interrupt(self, ctx: ActionContext) -> None:
        """被新动作打断时调用一次（停止资源申请、发停止指令等）。"""

    def finalize_step(self, ctx: ActionContext) -> ActionResult:
        """收尾 tick：默认立即收尾完成，可覆盖为多 tick 平滑收尾。"""
        return ActionResult.finished(interrupted=True)

    def on_finish(self, ctx: ActionContext, success: bool, interrupted: bool) -> None:
        """动作离开运行器前最后调用一次，无论正常结束还是被中断。"""

    def save_state(self) -> dict:
        return {}

    def load_state(self, state: dict) -> None:
        """恢复存档时的额外字段；无状态动作无需覆盖。"""


ActionFactory = Callable[[dict[str, Any]], Action]
