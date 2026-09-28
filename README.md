# 角色行为运行核心（behavior_core）

一个可嵌入其他 Python 项目的运行核心（不是小游戏）。提供：

- **有记忆的决策分支**：行为函数/对象基于「共享世界观察 + 角色私有记忆」决定动作；
- **持续动作**：动作每 tick 被驱动一次，返回继续 / 结束 / 挂起等结果；
- **中断与收尾**：目标变化时旧动作只收到一次中断，收尾可跨多个 tick，结束后同 tick 接上新动作；
- **晚到结果保护**：每次启动动作分配新的 `execution_id`，旧动作迟到的异步结果一律拒收；
- **可控时间**：时钟只能随 tick 前进，结果可预约到指定 tick 送达，便于核对先后顺序；
- **存档续跑**：时钟、世界、事件、每个角色的动作与记忆整体可序列化恢复。

仅依赖 Python 3.10+ 标准库。

## 动作基本约定（后续扩展不需要改动）

```python
class Action:
    def on_start(self, ctx): ...                       # 启动一次
    def step(self, ctx) -> ActionResult: ...           # 每 tick 一次
    def on_result(self, ctx, token, payload): ...      # PENDING 后收到外部结果
    def on_interrupt(self, ctx): ...                   # 被切换时恰好一次
    def finalize_step(self, ctx) -> ActionResult: ...  # 收尾 tick（可多 tick 平滑停止）
    def on_finish(self, ctx, success, interrupted): ...# 离开前一次
    def save_state(self) -> dict: ...                  # 自身状态序列化
    def load_state(self, state): ...
```

`ActionResult`：

- `ActionResult.running(...)`：本 tick 仍在持续，下一 tick 继续 `step`；
- `ActionResult.finished(success=True, ...)`：动作结束；
- `ActionResult.pending(token, ...)`：等待外部结果（如感知系统、网络、物理查询），
  结果由 `runtime.deliver_result(actor_id, execution_id, token, payload)` 回传。

新增感知、更复杂行为时，只需新增 `Action` 子类与决策逻辑，以上约定不变。

## 行为约定

决策入口签名固定为 `decide(actor_id, observation, memory, tick)`，可以是函数，
也可以是带 `decide` 方法的对象（对象可实现可选的 `get_state/set_state` 参与存档）：

- 返回 `None` / `Decision.keep()`：维持当前动作；
- 返回 `Decision.switch(action, reason=...)` 或直接返回一个 `Action`：切换动作，
  当前动作进入「中断 → 收尾」流程。

角色在收尾期间若又产生新决策，以最新目标为准，收尾结束的同一 tick 立即启动。

## 使用方式

```python
from behavior_core import Runtime
from examples.library import TargetBehavior, registry

rt = Runtime(tick_us=100_000, world={"target": "A"})
for kind, factory in registry.items():
    rt.register_action(kind, factory)   # 存档恢复需要动作工厂
rt.add_actor("guard", TargetBehavior())

rt.tick()                               # t1 启动 goto A
rt.set_world({"target": "B"}, reason="指挥改目标")
rt.tick()                               # t3 中断 goto A，开始收尾
data = rt.save()                        # 在收尾中途存档

rt2 = Runtime(world={"target": "B"})   # 重建并恢复
for kind, factory in registry.items():
    rt2.register_action(kind, factory)
rt2.add_actor("guard", TargetBehavior())
rt2.restore(data)
rt2.tick()                              # 收尾结束，同一 tick 启动 goto B
```

异步结果与可控时间：

```python
# 真实环境：结果就绪时回传（execution_id 不符即旧动作晚到，返回 False）
rt.deliver_result("guard", exec_id, token, {"answer": 42})

# 测试环境：预约在未来第 4 个 tick、角色执行前送达
rt.schedule_result(4, "guard", exec_id, token, {"answer": 42})
```

## 一致性与隔离

- 一个 tick = 时钟前进 → 一份世界深拷贝快照 → 各角色依次使用同一份快照；
  角色对观察的任何修改不影响世界和其他角色。
- 每个角色拥有独立的 `ActorRunner` 与 `Memory`；事件日志 `EventLog` 全局共享，
  `seq` 单调递增，跨存档继续递增，可直接核对事件先后关系。
- 关键事件：`decision.switch`、`action.start`、`action.interrupted`、
  `action.interrupted_end`、`action.finished`、`action.failed`、
  `result.requested`、`result.accepted`、`result.rejected`、`result.invalidated`。

## 嵌入宿主工程

宿主每帧循环只需：

1. 通过 `rt.set_world({...})` 写入世界状态（也可直接操作 `rt.world`）；
2. 调 `rt.tick()`；
3. 从 `rt.pending_requests()` 取走外部请求，完成后 `rt.deliver_result(...)`；
4. 需要持久化时 `rt.save()` / `rt.restore(data)`。

## 演示与测试

```bash
python -m examples.demo      # 目标变化 -> 中断 -> 存档 -> 恢复
python -m pytest -q          # 10 个测试：中断一次、晚到拒收、续跑、顺序等
```

## 文件结构

- `behavior_core/clock.py`：可控时钟；
- `behavior_core/blackboard.py`：共享世界与每 tick 快照；
- `behavior_core/events.py`：全局单调事件日志；
- `behavior_core/memory.py`：角色私有记忆；
- `behavior_core/actions.py`：动作与结果的基本约定；
- `behavior_core/behavior.py`：决策协议；
- `behavior_core/actor.py`：单角色状态机（执行代号、中断收尾一次）；
- `behavior_core/runtime.py`：多角色调度、异步结果、存档恢复；
- `examples/`：演示动作/行为与 demo 脚本；
- `tests/`：行为测试。
