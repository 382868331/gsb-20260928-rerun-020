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
- `World.request_joint_commitment(...)` 登记共同生效的承诺组合：只有全部参与者的承诺都在等待且到期、且共享资源能一次性占得时，组合动作才生效；单个成员到期或准备好不会提前触发。
- `World.joint_status(group_id)` 报告仍在等待谁（`waiting_on`）、哪些约束已满足（`satisfied`）以及准备占用的共享资源与当前占用者。
- 组合生效时分配 `decision_id` 并随 `joint_commitment_started` 事件发出；后续承诺用 `cause` 记录依赖的是哪一次共同决定。已开始组合不能撤回或回到预约状态。
- 开始前参与者可经 `withdraw_joint_commitment(...)` 放弃整组，或用 `replacement` 提出替代方案；原组合、原承诺与撤回原因全部保留，新方案通过 `cause["supersedes"]` 指回原组合。
- 争用同一资源的组合按 `earliest-request-wins-resource` 规则结算，先登记者生效，其余记录 `activation_deferred` 且不持有任何占用；组合完成、到期或参与者离开时同一时刻释放资源，结算前还会清理任何遗留占用。
- 组合的到期、参与者离开与资源释放在同一次 `dispatch_due()` 中核对；组合随存档保存，共同决定前后的保存点恢复后动作与后续承诺只发生一次，不同替代方案分支的私有记忆、等待条件与资源记录彼此独立。
- `World.request_phased_joint_commitment(...)` 把联合任务拆成有序阶段（默认 `prepare`、`execute`、`handover`，可自定义）：每个阶段单独声明资源、单独等待成员就绪；成员在准备阶段就绪不代表占用执行或交接阶段的资源，阶段资源只在该阶段活动期间持有，`finish_joint_phase(...)` 后立即释放。
- `joint_status(group_id)` 的 `waiting_for` 逐项解释当前在等谁（成员角色、最早可用时刻）或等哪个资源（占用方分组与阶段）；`diagnose_joint_waits()` 输出跨分组的 waits-for 图、边与 `cycles`，两个任务互相等待对方下一阶段资源时可直接看到环，而不是静默卡住。
- `cancel_joint_phase(group_id, phase_id, ...)` 只取消尚未开始的阶段及其后续阶段；已完成或正在执行的阶段、成员说明与事件全部保留。已开始阶段的处理规则固定为“继续到安全结束”：若取消时某阶段正在执行，该阶段继续持有其资源并运行到 `finish_joint_phase(...)`，组合在该阶段安全结束时才以取消原因收尾（发出 `joint_commitment_ended`）；若没有活动阶段，组合立即结束并释放全部占用。两条路径都不会留下无人能够释放的资源：全局资源表、`joint_status` 的 `held_resources` 与其他任务看到的 `waiting_for` 始终是同一事实。重复取消只记录一条 `phase_cancel_rejected`，重复派发不会新增事件或泄漏占用；取消前后的存档各自恢复后，取消与结束事件仍只发生一次。
- 每个成员每个阶段各有一条就绪承诺（可分别给 `delay`/`valid_for`），阶段开始发出 `joint_phase_started` 并附带成员说明，阶段结束发出 `joint_phase_finished`；阶段状态、资源承诺、成员说明随世界存档保存，重开后已执行事件不会重复触发，恢复出的分支与原世界互不影响。
- 活动阶段因后续交接被取消或成员退出时，用户可经 `offer_joint_phase_reschedule(...)` 选择允许延迟的后续阶段：按角色给出具备能力的替代成员（各带 `delay`/`valid_for`）与候选资源束，系统只在阶段真正被采纳时对照实时资源占用表重新核对，尚未释放的资源不会被候选提前借用。
- 重排不会重做已完成工作：只有尚未开始的阶段承诺被撤销或替换，已完成阶段与其成员名册原样保留；替代成员只在替代阶段的 `joint_phase_started` 中标记 `replacement`。候选资源束在采纳前不产生任何占用，活动阶段继续持有的资源到 `finish_joint_phase(...)` 才释放；找不到可行安排（成员未就绪、能力不足或资源仍被占用）时组合保持等待，状态与 `waiting_for`/`reschedule` 诊断持续反映真实占用，绝不虚假结束；仅当活动阶段安全结束且后续阶段既无就绪成员也无重排提案时，才以 `member_unavailable:<phase>:<roles>` 诚实收尾并发出 `joint_commitment_ended`。
- 执行中成员退出发出 `joint_member_lost`：活动阶段按“继续到安全结束”保留占用，只撤销未开始阶段中该成员的承诺并登记 `missing_roles`；采用重排时再次核对全部占用，替代成员准备好且资源实际释放后下一阶段才开始，`reschedule_adopted` 记录实际采用的成员与资源束。提案、名册、缺失角色与采用事实随存档保存，分支恢复后阶段只启动一次，最终资源全部释放。

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


## 分阶段联合任务

```python
group = world.request_phased_joint_commitment(
    requester_id="alice",
    action="TransferCargo",
    participants=[
        {"role": "carrier", "performer_id": "bob",
         "phases": {"prepare": {"delay": 1}, "execute": {"delay": 2},
                    "handover": {"delay": 1}}},
        {"role": "gatekeeper", "performer_id": "carol",
         "phases": {"prepare": {"delay": 1}, "execute": {"delay": 4},
                    "handover": {"delay": 1}}},
    ],
    phase_resources={"prepare": ("briefing",), "execute": ("bridge",),
                     "handover": ("dock",)},
)

world.dispatch_due()                   # 成员到期后进入 prepare，只占 briefing
world.finish_joint_phase(group.group_id, "prepare")
world.dispatch_due()                   # 释放 briefing，成员就绪后占 bridge 进入 execute

status = world.joint_status(group.group_id)
status["waiting_for"]                  # 解释在等哪些成员或哪个资源（含占用方阶段）
world.diagnose_joint_waits()           # 跨分组等待图与死锁环 cycles
world.cancel_joint_phase(group.group_id, "handover", by="bob")  # 已完成阶段不受影响

# 活动阶段运行中，交接成员退出：先让活动阶段跑到安全结束，再选择允许延迟的交接
world.offer_joint_phase_reschedule(
    group.group_id,
    "handover",
    by="alice",
    member_options=[
        {"role": "gatekeeper", "performer_id": "dave",
         "capabilities": ("certified",), "delay": 2, "valid_for": 10},
    ],
    resource_options=[{"resources": ("dock",), "delay": 0}],
    delay=0,
)  # 采纳前不占用 dock；成员就绪且 dock 真正释放后的 dispatch 才启动交接
```

## 测试

```bash
python -m pytest
```
