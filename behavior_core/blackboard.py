"""角色共享的世界黑板：每个 tick 只产生一份一致快照。"""

from __future__ import annotations

import copy
from typing import Any


class SharedBlackboard:
    """世界状态由外部（宿主工程）写入；角色只读取每 tick 的不可变快照副本。"""

    def __init__(self, data: dict[str, Any] | None = None) -> None:
        self._data: dict[str, Any] = copy.deepcopy(data) if data else {}
        self._revision = 0

    @property
    def revision(self) -> int:
        return self._revision

    def update(self, changes: dict[str, Any] | None = None, **kwargs: Any) -> int:
        if changes:
            self._data.update(copy.deepcopy(changes))
        if kwargs:
            self._data.update(copy.deepcopy(kwargs))
        self._revision += 1
        return self._revision

    def snapshot(self) -> dict[str, Any]:
        # 深拷贝：动作/行为对观察结果的任何修改都不会污染世界。
        return copy.deepcopy(self._data)

    def get_state(self) -> dict:
        return {"data": copy.deepcopy(self._data), "revision": self._revision}

    @classmethod
    def from_state(cls, state: dict) -> "SharedBlackboard":
        board = cls()
        board._data = copy.deepcopy(state["data"])
        board._revision = state["revision"]
        return board
