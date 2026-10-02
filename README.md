# 角色行为运行核心

这是一个可嵌入其他 Python 项目的角色行为运行核心，不包含小游戏循环或具体玩法。它提供共享世界观察、角色私有记忆、持续动作、目标驱动决策、一次中断收尾、动作代际隔离、可控时间、事件队列、单角色运行状态存取和整段世界存档分支。

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
- `World.request_commitment(...)` 登记带 `delay` 与 `valid_for` 有效期的承诺；到期前可用 `withdraw_commitment` 撤回或用 `update_commitment` 改条件，只有 `waiting` 状态的承诺可被撤回或修改。
- 承诺的 `waiting` 与 `started` 状态严格分开：开始执行后撤回只会记录一条 `withdrawal_rejected`，不会抹掉已发生的行为；同一承诺只会生效一次，重复结算不会重复触发。
- 同一时刻冲突按固定规则结算并逐条记录在承诺的 `history` 里：显式撤回与角色离开先于结算（`explicit-withdrawal-precedes-settlement`、`participant-removal-precedes-settlement`），结算内部到期先于生效（`expiry-precedes-activation`），已开始的承诺不可撤回（`started-commitments-cannot-be-withdrawn`）。
- 承诺可携带 `cause`（来源事件序号、父承诺 id、原因）与 `wait_reason`；生效、撤回、到期等事件序号都写入 `history`，可沿记录从动作追溯到原请求。
- 角色离开世界时，其参与的等待中或已开始的承诺以 `participant_lost:<actor_id>` 结束原因关闭，历史保留。
- 承诺随 `World.snapshot()` 一起存入存档；从同一存档派生的分支承诺与私有记忆互不影响，恰在到期或移除前保存后继续运行与连续运行逐条一致。

## 承诺

```python
commitment = world.request_commitment(
    requester_id="alice",
    performer_id="bob",
    action="MoveToTarget",
    params={"target": "north"},
    delay=5,        # 5 秒后生效
    valid_for=10,   # 10 秒后失效
    cause={"event_sequence": 3, "reason": "alice 的请求"},
)

world.withdraw_commitment(commitment.commitment_id, by="alice")  # 仅生效前有效
world.update_commitment(commitment.commitment_id, params={"target": "south"})

world.dispatch_due()  # 先结算到期，再生效到期的承诺并发出 commitment_started
```

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
