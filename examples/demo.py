"""目标变化 -> 中断旧动作 -> 存档 -> 恢复继续。运行: python -m examples.demo"""

from __future__ import annotations

from behavior_core import Runtime
from examples.library import TargetBehavior, registry


def main() -> None:
    rt = Runtime(tick_us=100_000, world={"target": "A"})
    for kind, factory in registry.items():
        rt.register_action(kind, factory)
    rt.add_actor("guard", TargetBehavior())

    rt.tick()  # t1: 启动 goto A
    rt.tick()  # t2: 持续推进
    rt.set_world({"target": "B"}, reason="指挥中心改目标")
    rt.tick()  # t3: 决定切换，goto A 收到一次中断，开始收尾
    snapshot = rt.save()
    rt.tick()  # t4: 收尾结束 -> 同 tick 启动 goto B；存档里则停在 t3

    print("== 不存档连续运行 ==")
    for s in rt.stats():
        print(s)

    rt2 = Runtime(world={"target": "B"})
    for kind, factory in registry.items():
        rt2.register_action(kind, factory)
    rt2.add_actor("guard", TargetBehavior())
    rt2.restore(snapshot)
    rt2.tick()  # 从 t3 继续
    print("== 从存档恢复后 ==")
    for s in rt2.stats():
        print(s)
    print("事件顺序:", rt2.events.kinds())


if __name__ == "__main__":
    main()
