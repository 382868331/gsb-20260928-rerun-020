from __future__ import annotations

from typing import Any

import pytest

from behavior_core import (
    Action,
    ActionContext,
    ActionResult,
    Decision,
    Runtime,
)
from behavior_core.actions import ActionStatus
from behavior_core.actor import ActorState
from examples.library import GotoAction, TargetBehavior, registry


class QueryAction(Action):
    """挂起等待外部结果的动作。"""

    name = "query"

    def __init__(self, saved: dict[str, Any] | None = None, **kwargs):
        saved = saved or {}
        self.query_id = saved.get("query_id", kwargs.get("query_id", "q1"))
        self.got = saved.get("got")

    def step(self, ctx):
        return ActionResult.pending(self.query_id)

    def on_result(self, ctx, token, payload):
        self.got = payload
        return ActionResult.finished(answer=payload.get("answer"))

    def finalize_step(self, ctx):
        return ActionResult.finished(interrupted=True)

    def save_state(self):
        return {"query_id": self.query_id, "got": self.got}


def make_runtime(world=None):
    rt = Runtime(world=world or {})
    for kind, factory in registry.items():
        rt.register_action(kind, factory)
    rt.register_action("QueryAction", lambda saved: QueryAction(saved))
    return rt


def target_runtime(target="A"):
    rt = make_runtime({"target": target})
    rt.add_actor("guard", TargetBehavior())
    return rt


def test_sustained_action_runs_over_multiple_ticks():
    rt = target_runtime()
    rt.run(2)
    runner = rt.actor("guard")
    assert runner.state == ActorState.RUNNING
    assert isinstance(runner.action, GotoAction)
    assert runner.action.progress == 2
    progress = rt.events.events("goto.progress")
    assert [e.tick for e in progress] == [1, 2]


def test_goal_change_interrupts_once_and_tail_runs_same_tick():
    rt = target_runtime()
    rt.run(2)
    rt.set_world({"target": "B"}, reason="换目标")
    rt.run(2)  # t3: 中断+收尾第1 tick；t4: 收尾完成并启动 B

    runner = rt.actor("guard")
    assert runner.interruptions == 1  # on_interrupt 只收尾一次
    assert runner.action is not None and runner.action.target == "B"

    kinds = rt.events.kinds()
    assert kinds.count("action.interrupted") == 1
    assert kinds.count("action.interrupted_end") == 1
    # 决策切换发生在 t3（world.change 在 set_world 的 t2 已记录）
    t3 = [e for e in rt.events.events() if e.tick == 3]
    t4 = [e for e in rt.events.events() if e.tick == 4]
    assert [e.kind for e in t3[:2]] == [
        "decision.switch",
        "goto.stop_issued",
    ]
    assert "action.interrupted" in [e.kind for e in t3]
    assert [e.kind for e in t4[:3]] == [
        "goto.winddown",
        "action.interrupted_end",
        "action.start",
    ]
    # goto A 的执行代号是 1，goto B 是 2
    starts = {e.payload["action"]: e.payload["execution_id"] for e in rt.events.events("action.start")}
    assert starts == {"goto": 2} or [e.payload["execution_id"] for e in rt.events.events("action.start")] == [1, 2]


def test_late_result_of_old_action_cannot_overwrite_new_action():
    rt = make_runtime()
    rt.add_actor("guard", lambda *a: None)
    runner = rt.actor("guard")
    runner.request_switch(QueryAction(query_id="q1"), rt.clock.now(), rt.events, "q1")
    rt.tick()  # t1 启动 q1 -> pending，execution_id=1
    assert runner.awaiting and runner.execution_id == 1

    runner.request_switch(QueryAction(query_id="q2"), rt.clock.now(), rt.events, "q2")
    rt.tick()  # t2 中断 q1，启动 q2 -> pending，execution_id=2
    assert runner.execution_id == 2

    # 旧动作 q1 的晚到结果必须被拒收
    assert rt.deliver_result("guard", 1, "q1", {"answer": "stale"}) is False
    assert runner.action.query_id == "q2" and runner.action.got is None
    # 错误 token 也拒收
    assert rt.deliver_result("guard", 2, "wrong", {}) is False
    # 当前结果正常接受
    assert rt.deliver_result("guard", 2, "q2", {"answer": "fresh"}) is True
    rt.tick()
    assert runner.action is None
    rejection = rt.events.events("result.rejected")
    assert {(e.payload["execution_id"], e.payload["current"]) for e in rejection} == {(1, 2), (2, 2)}


def test_scheduled_result_ordering_with_controlled_time():
    rt = make_runtime()
    rt.add_actor("guard", lambda a, o, m, t: None)  # 永不主动切换
    runner = rt.actor("guard")
    rt.tick()  # 空转 t1
    runner.request_switch(QueryAction(query_id="q"), rt.clock.now(), rt.events, "测试")
    rt.tick()  # t2 启动并挂起
    exec_id = runner.execution_id
    rt.schedule_result(4, "guard", exec_id, "q", {"answer": 42})
    rt.tick()  # t3 仍等待
    assert runner.awaiting
    rt.tick()  # t4 投递并消费
    assert runner.state == ActorState.IDLE
    accepted = rt.events.events("result.accepted")
    assert len(accepted) == 1 and accepted[0].tick == 4


def test_save_and_restore_continues_finalization():
    rt = target_runtime()
    rt.run(2)
    rt.set_world({"target": "B"})
    rt.tick()  # t3：goto A 在收尾中，goto B 已暂存
    assert rt.actor("guard").state == ActorState.FINALIZING
    data = rt.save()

    rt2 = make_runtime({"target": "B"})
    behavior = TargetBehavior()
    rt2.add_actor("guard", behavior)
    rt2.restore(data)
    restored = rt2.actor("guard")
    assert restored.state == ActorState.FINALIZING
    assert restored.execution_id == 1
    assert restored.interruptions == 1  # 已收尾过的一次不能重复
    rt2.tick()  # t4：收尾完成并启动 B
    assert restored.action.target == "B" and restored.execution_id == 2
    assert restored.interruptions == 1
    # 时钟、事件序号续上
    assert rt2.clock.now().tick == 4
    assert rt2.events.events()[-1].seq > rt.events.events()[-1].seq


def test_actors_share_observation_but_keep_independent_state():
    seen = {}

    def spy(actor_id, observation, memory, tick):
        seen.setdefault(actor_id, []).append(dict(observation))
        return None

    rt = make_runtime({"zone": "north", "target": "A"})
    rt.add_actor("a", spy)
    rt.add_actor("b", spy)
    rt.tick()
    seen["a"][0]["zone"] = "mutated"  # 篡改自己拿到的观察
    rt.tick()
    assert seen["a"][1]["zone"] == "north"  # 世界快照不受角色污染
    # 同一份世界，两个角色各记各的
    rt.actor("a").memory.remember("mine", "a-secret")
    assert rt.actor("a").memory.recall("mine") == "a-secret"
    assert rt.actor("b").memory.recall("mine") is None


def test_two_target_actors_run_independently():
    rt = make_runtime({"target": "A"})
    rt.add_actor("a", TargetBehavior())
    rt.add_actor("b", TargetBehavior())
    rt.tick()
    assert rt.actor("a").state == ActorState.RUNNING
    assert rt.actor("b").state == ActorState.RUNNING
    assert rt.actor("a").execution_id == rt.actor("b").execution_id == 1


def test_memory_drives_decision_branches():
    calls = {"switch": 0}

    class BranchingBehavior:
        def decide(self, actor_id, observation, memory, tick):
            if memory.recall("interrupted_before"):
                target = observation.get("target")
                if target and memory.recall("target") != target:
                    calls["switch"] += 1
                    memory.remember("target", target)
                    return Decision.switch(GotoAction(target=target), "记忆分支")
            return Decision.keep()

        def get_state(self):
            return {}

        def set_state(self, state):
            pass

    # 先制造一段中断历史写进记忆
    rt = target_runtime()
    rt.run(2)
    rt.set_world({"target": "B"})
    rt.run(2)
    memory_state = rt.actor("guard").memory.get_state()

    rt2 = make_runtime({"target": "X"})
    behavior = BranchingBehavior()
    from behavior_core.memory import Memory

    runner = rt2.add_actor("x", behavior, memory=Memory.from_state(memory_state))
    runner.memory.remember("interrupted_before", True)
    rt2.tick()
    assert calls["switch"] == 1
    assert runner.action.target == "X"


def test_event_log_is_global_and_monotonic():
    rt = target_runtime()
    rt.add_actor("scout", TargetBehavior())
    rt.run(3)
    seqs = [e.seq for e in rt.events.events()]
    assert seqs == list(range(1, len(seqs) + 1))
    # 同一 tick 两个角色的事件可按 seq 区分先后
    per_tick = {}
    for e in rt.events.events("action.start"):
        per_tick.setdefault(e.tick, []).append(e.payload["execution_id"])
    assert per_tick[1] == [1, 1]


def test_pending_request_invalidated_on_switch():
    rt = make_runtime()
    rt.add_actor("guard", lambda *a: None)
    runner = rt.actor("guard")
    rt.tick()
    runner.request_switch(QueryAction(query_id="q"), rt.clock.now(), rt.events, "t")
    rt.tick()
    assert rt.pending_requests()[0]["execution_id"] == runner.execution_id
    runner.request_switch(GotoAction(target="Z"), rt.clock.now(), rt.events, "t2")
    rt.tick()
    assert rt.pending_requests() == []
    assert "result.invalidated" in rt.events.kinds()
