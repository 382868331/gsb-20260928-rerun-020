"""演示用动作与行为：目标变化 -> 中止旧动作 -> 切换新动作。"""

from __future__ import annotations

from typing import Any

from behavior_core.actions import Action, ActionContext, ActionResult
from behavior_core.behavior import Decision


class GotoAction(Action):
    """持续动作：每个 tick 推进距离；可被多 tick 平滑中断。"""

    name = "goto"

    def __init__(self, saved: dict[str, Any] | None = None, target: str = "", steps: int = 3):
        saved = saved or {}
        self.target = saved.get("target", target)
        self.steps = saved.get("steps", steps)
        self.progress = saved.get("progress", 0)
        self.wind_down = saved.get("wind_down", 0)  # 中断收尾还需几个 tick
        self.stop_issued = saved.get("stop_issued", False)

    def step(self, ctx: ActionContext) -> ActionResult:
        self.progress += 1
        ctx.emit("goto.progress", target=self.target, progress=self.progress)
        if self.progress >= self.steps:
            return ActionResult.finished(arrived=self.target)
        return ActionResult.running(progress=self.progress)

    def on_interrupt(self, ctx: ActionContext) -> None:
        # 收尾只进行一次：这里发出一次停止指令，finalize 负责减速到停。
        self.stop_issued = True
        ctx.emit("goto.stop_issued", target=self.target)

    def finalize_step(self, ctx: ActionContext) -> ActionResult:
        self.wind_down += 1
        ctx.emit("goto.winddown", nth=self.wind_down)
        if self.wind_down < 2:
            return ActionResult.running(winding_down=True)
        return ActionResult.finished(interrupted=True, stopped_at=self.progress)

    def save_state(self) -> dict[str, Any]:
        return {
            "target": self.target,
            "steps": self.steps,
            "progress": self.progress,
            "wind_down": self.wind_down,
            "stop_issued": self.stop_issued,
        }


class PatrolAction(Action):
    """另一个持续动作，用于验证记忆驱动的分支。"""

    name = "patrol"

    def __init__(self, saved: dict[str, Any] | None = None):
        saved = saved or {}
        self.waypoint = saved.get("waypoint", 0)

    def step(self, ctx: ActionContext) -> ActionResult:
        self.waypoint += 1
        return ActionResult.running(waypoint=self.waypoint)

    def finalize_step(self, ctx: ActionContext) -> ActionResult:
        return ActionResult.finished(interrupted=True)

    def save_state(self) -> dict[str, Any]:
        return {"waypoint": self.waypoint}


class TargetBehavior:
    """看世界里的 target：目标变了就切换到 Goto；被打断过则先巡逻一圈。"""

    def __init__(self) -> None:
        self.last_target: str | None = None
        self.decisions = 0

    def decide(self, actor_id, observation, memory, tick):
        target = observation.get("target")
        if target is None or target == self.last_target:
            return Decision.keep("目标未变")
        old = self.last_target
        self.last_target = target
        self.decisions += 1
        memory.remember("target", target)
        memory.note(tick, "target_changed", old=old, new=target)
        reason = f"target {old} -> {target}"
        if old is not None and memory.count("action_end") == 0:
            # 演示记忆分支：首次改目标，不额外处理；后续由动作历史影响决策。
            pass
        return Decision.switch(GotoAction(target=target), reason)

    def get_state(self) -> dict[str, Any]:
        return {"last_target": self.last_target, "decisions": self.decisions}

    def set_state(self, state: dict[str, Any]) -> None:
        self.last_target = state["last_target"]
        self.decisions = state["decisions"]


registry = {
    "GotoAction": lambda saved: GotoAction(saved),
    "PatrolAction": lambda saved: PatrolAction(saved),
}
