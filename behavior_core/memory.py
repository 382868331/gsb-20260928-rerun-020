"""角色私有记忆：各角色独立，不与其他角色共享。"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any


@dataclass
class MemoryRecord:
    tick: int
    kind: str
    detail: dict[str, Any] = field(default_factory=dict)


class Memory:
    """键值状态 + 带时间戳的记事流，供行为做决策分支。"""

    def __init__(self, data: dict[str, Any] | None = None) -> None:
        self._data: dict[str, Any] = copy.deepcopy(data) if data else {}
        self._records: list[MemoryRecord] = []

    def remember(self, key: str, value: Any) -> None:
        self._data[key] = copy.deepcopy(value)

    def recall(self, key: str, default: Any = None) -> Any:
        return copy.deepcopy(self._data.get(key, default))

    def note(self, tick: int, kind: str, **detail: Any) -> None:
        self._records.append(MemoryRecord(tick, kind, copy.deepcopy(detail)))

    def history(self, kind: str | None = None) -> list[MemoryRecord]:
        if kind is None:
            return list(self._records)
        return [r for r in self._records if r.kind == kind]

    def count(self, kind: str) -> int:
        return sum(1 for r in self._records if r.kind == kind)

    def get_state(self) -> dict:
        return {
            "data": copy.deepcopy(self._data),
            "records": [
                {"tick": r.tick, "kind": r.kind, "detail": r.detail}
                for r in self._records
            ],
        }

    @classmethod
    def from_state(cls, state: dict) -> "Memory":
        memory = cls()
        memory._data = copy.deepcopy(state["data"])
        memory._records = [
            MemoryRecord(item["tick"], item["kind"], copy.deepcopy(item["detail"]))
            for item in state["records"]
        ]
        return memory
