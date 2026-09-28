"""单个角色的运行状态机：与其他角色完全独立。"""

from __future__ import annotations

from enum import Enum
from typing import Any, Callable

from .actions import Action, ActionContext, ActionResult, ActionStatus
from .behavior import normalize_decision
from .clock import Timestamp
from .memory import Memory


class ActorState(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    AWAITING = "awaiting"      # 当前动作在等外部结果
    FINALIZING = "finalizing"  # 被中断，正在收尾（收尾只进行一次）


RegisterPending = Callable[[str, int, Any, int, dict[str, Any]], None]
InvalidatePending = Callable[[str, int], None]


class ActorRunner:
    """驱动一个动作的生命周期，并保存角色私有记忆。

    execution_id 在每次启动动作时递增；外部回传结果必须携带当前 id，
    旧动作的晚到结果会被拒绝，无法覆盖新动作。
    """

    def __init__(
        self,
        actor_id: str,
        decide: Any,
        *,
        memory: Memory | None = None,
        action_registry: dict[str, Callable[[dict[str, Any]], Action]] | None = None,
    ) -> None:
        self.actor_id = actor_id
        self._decide = decide
        self.memory = memory or Memory()
        self._action_registry = action_registry or {}

        self.state = ActorState.IDLE
        self.action: Action | None = None
        self.execution_id = 0
        self.interruptions = 0

        # 由 Runtime 注入。
        self._register_pending: RegisterPending | None = None
        self._invalidate_pending: InvalidatePending | None = None

        self._interrupt_requested = False
        self._pending_token: Any = None
        self._delivered: tuple[Any, dict[str, Any]] | None = None
        # 收尾期间暂存新决策；收尾结束的同一 tick 立即启动新动作。
        self._pending_switch: Action | None = None
        self._pending_reason = ""

    # ---- 外部结果 ---------------------------------------------------

    def deliver_result(
        self, execution_id: int, token: Any, result: dict[str, Any] | None = None
    ) -> bool:
        """外部异步结果入口。只接受当前执行代号且仍在等待该 token 的结果。"""
        if self.state != ActorState.AWAITING:
            return False
        if execution_id != self.execution_id or token != self._pending_token:
            return False
        self._delivered = (token, dict(result or {}))
        return True

    @property
    def awaiting(self) -> bool:
        return self.state == ActorState.AWAITING and self._delivered is None

    def request_switch(self, action: Action, now: Timestamp, log: Any, reason: str = "") -> None:
        """供宿主直接下达动作指令（绕过决策函数）。"""
        self._request_switch(action, now, log, reason)

    # ---- 每 tick 驱动 -----------------------------------------------

    def tick(self, now: Timestamp, observation: dict[str, Any], log: Any) -> None:
        # 1) 决策：共享观察 + 私有记忆。收尾期间也允许决策，新目标先暂存。
        decision = normalize_decision(self._invoke_decide(observation, now.tick))
        if decision.action is not None:
            if self.state in (ActorState.FINALIZING, ActorState.IDLE):
                # 只在没有待启动动作时暂存/启动，避免清掉收尾结束后要接上的新动作。
                if self._pending_switch is not None:
                    if self.state == ActorState.FINALIZING:
                        # 收尾期间目标再次变化：以最新目标为准。
                        self._pending_switch = decision.action
                        self._pending_reason = decision.reason
                elif self.state == ActorState.FINALIZING:
                    self._pending_switch = decision.action
                    self._pending_reason = decision.reason
                else:
                    self._request_switch(decision.action, now, log, decision.reason)
            else:
                self._request_switch(decision.action, now, log, decision.reason)

        if self.state == ActorState.IDLE:
            return

        # 2) FINALIZING：on_interrupt 恰好一次，finalize 每个收尾 tick 一次。
        if self.state == ActorState.FINALIZING:
            ctx = self._context(now, observation, log)
            if self._interrupt_requested:
                self._interrupt_requested = False
                self.action.on_interrupt(ctx)
                self.interruptions += 1
                ctx.emit("action.interrupted", action=self.action.name)
            result = self.action.finalize_step(ctx)
            if result.status == ActionStatus.RUNNING:
                return
            self._end_action(now, observation, log, interrupted=True, success=result.success)
            if self._pending_switch is not None:
                action, reason = self._pending_switch, self._pending_reason
                self._pending_switch = None
                self._pending_reason = ""
                self._start(action, now, observation, log, reason)
                self._run_current(now, observation, log)
            return

        self._run_current(now, observation, log)

    def stats(self) -> dict[str, Any]:
        return {
            "actor_id": self.actor_id,
            "state": self.state.value,
            "action": self.action.name if self.action else None,
            "execution_id": self.execution_id,
            "interruptions": self.interruptions,
        }

    # ---- 内部 -------------------------------------------------------

    def _invoke_decide(self, observation: dict[str, Any], tick: int) -> Any:
        decide = self._decide
        if hasattr(decide, "decide"):
            return decide.decide(self.actor_id, observation, self.memory, tick)
        return decide(self.actor_id, observation, self.memory, tick)

    def _run_current(self, now: Timestamp, observation: dict[str, Any], log: Any) -> None:
        ctx = self._context(now, observation, log)

        if self.state == ActorState.AWAITING:
            if self._delivered is None:
                return
            token, payload = self._delivered
            self._delivered = None
            result = self.action.on_result(ctx, token, payload)
        else:
            result = self.action.step(ctx)

        if result.status == ActionStatus.RUNNING:
            self.state = ActorState.RUNNING
            return

        if result.status == ActionStatus.PENDING:
            self.state = ActorState.AWAITING
            self._pending_token = result.pending.token
            if self._register_pending is not None:
                self._register_pending(
                    self.actor_id,
                    self.execution_id,
                    self._pending_token,
                    now.tick,
                    result.outcome,
                )
            return

        self._end_action(now, observation, log, interrupted=False, success=result.success)

    def _request_switch(self, action: Action, now: Timestamp, log: Any, reason: str) -> None:
        if self.action is None:
            self._start(action, now, {}, log, reason)
            return
        # 作废旧执行代号：未送达结果即刻失效；收尾只安排一次。
        self.state = ActorState.FINALIZING
        self._interrupt_requested = True
        self._pending_token = None
        self._delivered = None
        if self._invalidate_pending is not None:
            self._invalidate_pending(self.actor_id, self.execution_id)
        self.memory.note(now.tick, "interrupt_request", old=self.action.name, new=action.name)
        log.append(
            "decision.switch",
            now,
            actor_id=self.actor_id,
            old=self.action.name,
            new=action.name,
            reason=reason,
        )
        self._pending_switch = action
        self._pending_reason = reason

    def _start(
        self, action: Action, now: Timestamp, observation: dict[str, Any], log: Any, reason: str
    ) -> None:
        self.action = action
        self.execution_id += 1
        self.state = ActorState.RUNNING
        self._interrupt_requested = False
        self._pending_token = None
        self.memory.note(now.tick, "action_start", action=action.name)
        log.append(
            "action.start",
            now,
            actor_id=self.actor_id,
            action=action.name,
            execution_id=self.execution_id,
            reason=reason,
        )
        action.on_start(self._context(now, observation, log))

    def _end_action(
        self,
        now: Timestamp,
        observation: dict[str, Any],
        log: Any,
        *,
        interrupted: bool,
        success: bool,
    ) -> None:
        action = self.action
        kind = "action.interrupted_end" if interrupted else (
            "action.finished" if success else "action.failed"
        )
        action.on_finish(self._context(now, observation, log), success, interrupted)
        log.append(
            kind,
            now,
            actor_id=self.actor_id,
            action=action.name,
            execution_id=self.execution_id,
            success=success,
        )
        self.memory.note(
            now.tick,
            "action_end",
            action=action.name,
            interrupted=interrupted,
            success=success,
        )
        self.action = None
        self.state = ActorState.IDLE
        self._pending_token = None
        self._delivered = None
        self._interrupt_requested = False

    def _context(self, now: Timestamp, observation: dict[str, Any], log: Any) -> ActionContext:
        return ActionContext(
            now_tick=now.tick,
            us=now.us,
            observation=observation,
            memory=self.memory,
            log=log,
            actor_id=self.actor_id,
            execution_id=self.execution_id,
        )

    # ---- 持久化 -----------------------------------------------------

    def save_state(self) -> dict[str, Any]:
        state: dict[str, Any] = {
            "actor_id": self.actor_id,
            "state": self.state.value,
            "execution_id": self.execution_id,
            "interruptions": self.interruptions,
            "interrupt_requested": self._interrupt_requested,
            "pending_token": self._pending_token,
            "delivered": list(self._delivered) if self._delivered is not None else None,
            "memory": self.memory.get_state(),
            "action": None,
            "pending_switch": None,
        }
        if self.action is not None:
            state["action"] = {
                "type": type(self.action).__name__,
                "state": self.action.save_state(),
            }
        if self._pending_switch is not None:
            state["pending_switch"] = {
                "type": type(self._pending_switch).__name__,
                "reason": self._pending_reason,
                "state": self._pending_switch.save_state(),
            }
        return state

    def load_state(self, state: dict[str, Any]) -> None:
        self.state = ActorState(state["state"])
        self.execution_id = state["execution_id"]
        self.interruptions = state["interruptions"]
        self._interrupt_requested = state["interrupt_requested"]
        self._pending_token = state["pending_token"]
        delivered = state.get("delivered")
        self._delivered = (delivered[0], delivered[1]) if delivered is not None else None
        self.memory = Memory.from_state(state["memory"])
        self.action = None
        self._pending_switch = None
        self._pending_reason = ""
        if state["action"] is not None:
            self.action = self._build(state["action"])
        if state["pending_switch"] is not None:
            item = state["pending_switch"]
            self._pending_switch = self._build(item)
            self._pending_reason = item["reason"]

    def _build(self, item: dict[str, Any]) -> Action:
        kind = item["type"]
        if kind not in self._action_registry:
            raise KeyError(f"恢复存档需要注册动作类型: {kind}")
        action = self._action_registry[kind](item["state"])
        action.load_state(item["state"])
        return action
