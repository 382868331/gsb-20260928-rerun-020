# 角色行为运行核心

这是一个可嵌入其他 Python 项目的角色行为运行核心，不包含小游戏循环或具体玩法。它提供共享世界观察、角色私有记忆、持续动作、目标驱动决策、一次中断收尾、动作代际隔离、可控时间、事件队列、单角色运行状态存取和整段世界存档分支。它还提供带有效期限的承诺：登记、到期启动、等待中撤回或改条件、已开始与等待分离、同刻先后仲裁以及存档分支隔离。

## 核心约定

- `World` 保存共享状态，`Observation` 是某次决策时的不可变快照。
- `ActorRuntime` 是单个角色的状态机；多个角色共享同一个 `World`，但动作、记忆和 `run_id` 完全独立。
- `Action.tick()` 返回 `running`、`completed`、`failed` 或 `cancelled`。
- `Action.interrupt()` 只在旧动作被新动作或外部停止替换时调用一次，用于清理和发出收尾事件。
- 延迟结果必须通过 `ActionContext.emit_result(..., delay=...)` 发出，事件会携带当前动作的 `run_id`。
- 若属于旧 `run_id` 的结果晚到，运行时会计入 `stale_results_ignored`，不会调用或覆盖新动作。
- `VirtualClock` 只能显式推进，事件按 `(时间, 序号)` 确定顺序交付；同刻事件处理器新发的同刻事件按发布顺序追加到当前批次。
- 角色按加入世界的顺序遍历，存档保存该顺序；决策记录包含角色序号和已观察事件序号，可解释同刻动作先后与触发来源。
- 事件派发过程中加入或离开世界会在当前同刻批次结束后生效；离开会清理未读邮箱和等待中的定向事件，新加入者不会回收加入前广播。
- `WorldArchive.capture()` 会同时保存世界状态、时钟、未投递事件、每个角色尚未消费的邮箱、当前动作等待、私有记忆和决策历史。
- 多次 `WorldArchive.restore()` 得到互不共享可写对象的分支；延迟事件仍携带原 `run_id`，旧动作结果迟到时继续计入 `stale_results_ignored`。
- 分支元数据记录当前 checkpoint 和父 checkpoint，可追溯分支起点；决策历史包含动作配置，可比较同一起点后哪些决策改变。
- 'World.promises.request(...)' 登记带截止时刻的承诺，'promise_due' 在到期时定向投给承诺者；等待中的承诺可用 'withdraw()' 撤回或用 'revise()' 改条件（旧承诺留下 'superseded_while_waiting' 记录）。
- 已开始与仍在等待的承诺分开：状态为 'waiting'、'running'、'completed'、'failed'、'cancelled' 或 'ended'。撤回不会删除账本记录；已开始执行后撤回只会留下 'withdrawal_rejected_after_start'，同一次预约的动作只执行一遍。
- 同刻仲裁严格按（时间, 序号）：到期事件在请求登记时入队，因此同刻到期与撤回，到期在前则采用 'due_before_withdrawal'，动作照常开始；撤回在前则承诺在等待中取消。
- 同刻批次中角色离开在整批事件投递后生效：仍等待的承诺以 'participant_left_before_start' 结束；承诺者离开时已到期或执行中的承诺以 'promiser_left_after_start' 结束；请求者离开不抹掉已发生行为。
- 动作可通过 'ActionContext.request_promise(...)' 触发新承诺，账本保留 'source_promise_id'、'origin_event_sequence'、'request_event_sequence' 和等待原因，可沿记录找回原请求与生效事件。
- 承诺账本与未投递事件一同进入 'WorldArchive'：从同一保存点派生的撤回/不撤回分支拥有各自独立的承诺状态与私有记忆；保存在到期前后或角色移除前，恢复后逐条事件与连续运行一致，失去参与者的承诺留下明确结束原因。

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

## 世界分支

```python
archive = WorldArchive.capture(
    world,
    {"alice": alice, "bob": bob},
    checkpoint_id="waiting-at-t1",
)

branch_world, branch_runtimes = archive.restore(
    {"alice": TargetPolicy(), "bob": TargetPolicy()},
    action_types={
        "alice": [MoveToTarget],
        "bob": [MoveToTarget],
    },
)
branch_world.state["target"] = "south"

follow_up = WorldArchive.capture(
    branch_world,
    branch_runtimes,
    checkpoint_id="south-decision",
    branch_id="south-goal",
    parent=archive,
)
assert follow_up.branch.parent_checkpoint_id == "waiting-at-t1"
```

策略对象通常不可序列化，因此恢复时需要由宿主程序重新提供；动作仍通过持久化的 `action_name` 和 `to_config()` 数据重建。

持久化动作需要实现 `to_config()` 和 `from_config()`，并在运行时构造时通过 `action_types` 注册。新增感知模块或更复杂行为时可以继续使用同一个动作接口；感知结果进入观察或动作上下文即可，不需要改变动作的基本状态约定。

## 测试

```bash
python -m pytest
```
