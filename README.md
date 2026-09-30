# 角色行为运行核心

这是一个可嵌入其他 Python 项目的角色行为运行核心，不包含小游戏循环或具体玩法。它提供共享世界观察、角色私有记忆、持续动作、目标驱动决策、一次中断收尾、动作代际隔离、可控时间、事件队列、单角色状态存取，以及整段世界的存档与分支。

## 核心约定

- `World` 保存共享状态，`Observation` 是某次决策时的不可变快照。
- `ActorRuntime` 是单个角色的状态机；多个角色共享同一个 `World`，但动作、记忆和 `run_id` 完全独立。
- `Action.tick()` 返回 `running`、`completed`、`failed` 或 `cancelled`。
- `Action.interrupt()` 只在旧动作被新动作或外部停止替换时调用一次，用于清理和发出收尾事件。
- 延迟结果必须通过 `ActionContext.emit_result(..., delay=...)` 发出，事件会携带当前动作的 `run_id`。
- 若属于旧 `run_id` 的结果晚到，运行时会计入 `stale_results_ignored`，不会调用或覆盖新动作。
- `VirtualClock` 只能显式推进，事件按 `(时间, 序号)` 确定顺序交付。
- `WorldArchive` 一次性保存共享世界状态、时钟、尚未到达的延迟事件、已派发但未读取的角色消息，以及每个角色正在等待的动作、`run_id`、记忆和历史。
- 从同一存档 `branch()` 出的分支彼此是深拷贝，不共享可写世界、私有记忆或未处理消息。
- 每个存档带 `Lineage`（`checkpoint_id`、`parent_id`、`root_id`、`depth`、`branch_name`），`content_hash` 标识起始内容，可用 `decisions_since`/`diff_decisions` 对比分支后的决策差异。

## 最小循环

```python
from behavior_runtime import ActorRuntime, VirtualClock, World
from behavior_runtime.examples import MoveToTarget, TargetPolicy

clock = VirtualClock()
world = World({"target": "archive"}, clock=clock)
actor = ActorRuntime("actor", TargetPolicy(), world, action_types=[MoveToTarget])

world.dispatch_due()
actor.tick()
clock.advance(1)

data = actor.snapshot().to_dict()
restored = ActorRuntime("actor", TargetPolicy(), world, action_types=[MoveToTarget])
restored.restore(data)
```

持久化动作需要实现 `to_config()` 和 `from_config()`，并在运行时构造时通过 `action_types` 注册。新增感知模块或更复杂行为时可以继续使用同一个动作接口；感知结果进入观察或动作上下文即可，不需要改变动作的基本状态约定。

## 整段世界存档与分支

```python
from behavior_runtime import WorldArchive

archive = WorldArchive()
root = archive.capture(world, [alice, bob], branch_name="root")

# 两个分支从同一状态分叉，互不共享可写状态或未处理消息
changed = archive.branch(
    root,
    branch_name="alice-south",
    policies={"alice": policy_a, "bob": policy_b},
    action_types=[MoveToTarget],
    mutate=lambda world, runtimes: world.state["goals"].__setitem__("alice", "south"),
)

# 存档是纯数据，可 JSON 落盘后在新的运行实例中重建
data = changed.to_dict()
world_b, runtimes_b = archive.restore(
    data,
    policies={"alice": policy_a, "bob": policy_b},
    action_types=[MoveToTarget],
)
```

策略对象本身是代码、不进入存档；需要随存档分叉的“目标”应放在可序列化的世界状态或角色记忆里。接续后旧动作的迟到结果仍按 `run_id` 判定为过期，行为与未中断的过程一致。

## 测试

```bash
python -m pytest
```
