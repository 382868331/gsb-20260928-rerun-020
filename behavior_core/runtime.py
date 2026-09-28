"""运行核心：统一驱动多个角色，提供可控时间、共享世界与存档。"""

from __future__ import annotations
from typing import Any, Callable

from .actions import Action
from .actor import ActorRunner
from .blackboard import SharedBlackboard
from .clock import Clock
from .events import EventLog


class Runtime:
    """宿主工程只需要：更新世界 -> tick -> 回传异步结果。

    一个 tick 内：时钟前进 -> 生成一份世界快照 -> 所有角色依次用同一份快照决策。
    """

    def __init__(
        self,
        *,
        tick_us: int = 100_000,
        world: dict[str, Any] | None = None,
        clock: Clock | None = None,
        event_log: EventLog | None = None,
    ) -> None:
        self.clock = clock or Clock(tick_us)
        self.world = SharedBlackboard(world)
        self.events = event_log or EventLog()
        self._actors: dict[str, ActorRunner] = {}
        self._order: list[str] = []
        self._action_registry: dict[str, Callable[[dict[str, Any]], Action]] = {}
        # (actor_id, execution_id) -> {token, tick, outcome}
        self._pending: dict[tuple[str, int], dict[str, Any]] = {}
        # tick -> [(actor_id, execution_id, token, payload)]
        self._scheduled: dict[int, list[tuple[str, int, Any, dict[str, Any]]]] = {}

    # ---- 组装 -------------------------------------------------------

    def register_action(self, kind: str, factory: Callable[[dict[str, Any]], Action]) -> None:
        """注册动作工厂：fn(saved_state) -> Action，存档恢复时需要。"""
        self._action_registry[kind] = factory

    def add_actor(self, actor_id: str, decide: Any, *, memory: Any = None) -> ActorRunner:
        if actor_id in self._actors:
            raise ValueError(f"角色已存在: {actor_id}")
        runner = ActorRunner(
            actor_id,
            decide,
            memory=memory,
            action_registry=self._action_registry,
        )
        runner._register_pending = self._register_pending
        runner._invalidate_pending = self._invalidate_pending
        self._actors[actor_id] = runner
        self._order.append(actor_id)
        return runner

    def actor(self, actor_id: str) -> ActorRunner:
        return self._actors[actor_id]

    def stats(self) -> list[dict[str, Any]]:
        return [self._actors[a].stats() for a in self._order]

    # ---- 世界 -------------------------------------------------------

    def set_world(self, changes: dict[str, Any] | None = None, *, reason: str = "", **kwargs: Any) -> None:
        self.world.update(changes, **kwargs)
        self.events.append(
            "world.change",
            self.clock.now(),
            reason=reason,
            changes={**(changes or {}), **kwargs},
            revision=self.world.revision,
        )

    # ---- 异步结果 ---------------------------------------------------

    def pending_requests(self) -> list[dict[str, Any]]:
        return [
            {"actor_id": actor_id, "execution_id": execution_id, **info}
            for (actor_id, execution_id), info in self._pending.items()
        ]

    def deliver_result(
        self, actor_id: str, execution_id: int, token: Any, result: dict[str, Any] | None = None
    ) -> bool:
        """回传外部结果。execution_id 不匹配（旧动作晚到）时返回 False 并丢弃。"""
        accepted = self._actors[actor_id].deliver_result(execution_id, token, result)
        if accepted:
            self._pending.pop((actor_id, execution_id), None)
            self.events.append(
                "result.accepted",
                self.clock.now(),
                actor_id=actor_id,
                execution_id=execution_id,
                token=_safe(token),
            )
        else:
            self.events.append(
                "result.rejected",
                self.clock.now(),
                actor_id=actor_id,
                execution_id=execution_id,
                token=_safe(token),
                current=self._actors[actor_id].execution_id,
            )
        return accepted

    def schedule_result(
        self,
        deliver_tick: int,
        actor_id: str,
        execution_id: int,
        token: Any,
        result: dict[str, Any] | None = None,
    ) -> None:
        """安排结果在未来某 tick 角色执行前送达，用于确定性测试先后关系。"""
        self._scheduled.setdefault(deliver_tick, []).append(
            (actor_id, execution_id, token, dict(result or {}))
        )

    # ---- 主循环 -----------------------------------------------------

    def tick(self) -> int:
        now = self.clock.advance()
        # 先投递预定结果（全部角色共享同一时钟与同一份世界快照）。
        for actor_id, execution_id, token, payload in self._scheduled.pop(now.tick, []):
            self.deliver_result(actor_id, execution_id, token, payload)
        snapshot = self.world.snapshot()
        for actor_id in self._order:
            self._actors[actor_id].tick(now, snapshot, self.events)
        return now.tick

    def run(self, ticks: int) -> None:
        for _ in range(ticks):
            self.tick()

    # ---- 存档 -------------------------------------------------------

    def save(self) -> dict[str, Any]:
        data = {
            "version": 1,
            "clock": self.clock.get_state(),
            "world": self.world.get_state(),
            "events": self.events.get_state(),
            "order": list(self._order),
            "actors": {a: r.save_state() for a, r in self._actors.items()},
            "pending": [
                {"actor_id": actor_id, "execution_id": execution_id, **info}
                for (actor_id, execution_id), info in self._pending.items()
            ],
            "scheduled": [
                {"tick": tick, "actor_id": a, "execution_id": e, "token": _safe(t), "result": p}
                for tick, items in self._scheduled.items()
                for a, e, t, p in items
            ],
            "behavior_states": {
                a: r._decide.get_state()
                for a, r in self._actors.items()
                if hasattr(r._decide, "get_state")
            },
        }
        return data

    def restore(self, save: dict[str, Any]) -> None:
        """在重建好 Runtime 并 add_actor（同样的 decide）之后恢复运行状态。"""
        if save["version"] != 1:
            raise ValueError(f"不支持的存档版本: {save['version']}")
        self.clock = Clock.from_state(save["clock"])
        self.world = SharedBlackboard.from_state(save["world"])
        self.events = EventLog.from_state(save["events"])
        for actor_id in save["order"]:
            if actor_id not in self._actors:
                raise KeyError(f"恢复存档缺少角色: {actor_id}")
        self._order = list(save["order"])
        for actor_id, state in save["actors"].items():
            self._actors[actor_id].load_state(state)
        self._pending = {
            (item["actor_id"], item["execution_id"]): {
                "token": item["token"],
                "tick": item["tick"],
                "outcome": item["outcome"],
            }
            for item in save["pending"]
        }
        self._scheduled = {}
        for item in save["scheduled"]:
            self._scheduled.setdefault(item["tick"], []).append(
                (item["actor_id"], item["execution_id"], item["token"], item["result"])
            )
        for actor_id, state in save.get("behavior_states", {}).items():
            decide = self._actors[actor_id]._decide
            if hasattr(decide, "set_state"):
                decide.set_state(state)

    # ---- 回调 -------------------------------------------------------

    def _register_pending(
        self, actor_id: str, execution_id: int, token: Any, tick: int, outcome: dict[str, Any]
    ) -> None:
        self._pending[(actor_id, execution_id)] = {
            "token": token,
            "tick": tick,
            "outcome": outcome,
        }
        self.events.append(
            "result.requested",
            self.clock.now(),
            actor_id=actor_id,
            execution_id=execution_id,
            token=_safe(token),
        )

    def _invalidate_pending(self, actor_id: str, execution_id: int) -> None:
        info = self._pending.pop((actor_id, execution_id), None)
        if info is not None:
            self.events.append(
                "result.invalidated",
                self.clock.now(),
                actor_id=actor_id,
                execution_id=execution_id,
                token=_safe(info["token"]),
            )


def _safe(value: Any) -> Any:
    """事件/存档里的 token 尽量保留原值；不可深拷贝对象退化为字符串。"""
    try:
        import copy

        return copy.deepcopy(value)
    except Exception:
        return repr(value)
