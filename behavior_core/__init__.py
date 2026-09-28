"""可嵌入的角色行为运行核心。

基本约定见 README：行为产出 Decision，动作用 ActionResult 表达
"继续 / 结束 / 挂起结果"，运行器负责调度、中断一次与状态持久化。
"""

from .actions import Action, ActionContext, ActionResult, ActionStatus, PendingResult
from .actor import ActorRunner, ActorState
from .behavior import Behavior, Decision
from .blackboard import SharedBlackboard
from .clock import Clock, Timestamp
from .events import Event, EventLog
from .memory import Memory
from .runtime import Runtime

__all__ = [
    "Action",
    "ActionContext",
    "ActionResult",
    "ActionStatus",
    "PendingResult",
    "ActorRunner",
    "ActorState",
    "Behavior",
    "Decision",
    "SharedBlackboard",
    "Clock",
    "Timestamp",
    "Event",
    "EventLog",
    "Memory",
    "Runtime",
]
