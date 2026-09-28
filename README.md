# 角色行为运行核心

这是一个可嵌入其他 Python 项目的角色行为运行核心，不包含小游戏循环或具体玩法。它提供共享世界观察、角色私有记忆、持续动作、目标驱动决策、一次中断收尾、动作代际隔离、可控时间、事件队列和运行状态存取。

## 核心约定

- `World` 保存共享状态，`Observation` 是某次决策时的不可变快照。
- `ActorRuntime` 是单个角色的状态机；多个角色共享同一个 `World`，但动作、记忆和 `run_id` 完全独立。
- `Action.tick()` 返回 `running`、`completed`、`failed` 或 `cancelled`。
- `Action.interrupt()` 只在旧动作被新动作或外部停止替换时调用一次，用于清理和发出收尾事件。
- 延迟结果必须通过 `ActionContext.emit_result(..., delay=...)` 发出，事件会携带当前动作的 `run_id`。
- 若属于旧 `run_id` 的结果晚到，运行时会计入 `stale_results_ignored`，不会调用或覆盖新动作。
- `VirtualClock` 只能显式推进，事件按 `(时间, 序号)` 确定顺序交付。

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

## 测试

```bash
python -m pytest
```
