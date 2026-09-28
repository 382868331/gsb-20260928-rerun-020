from __future__ import annotations

from copy import deepcopy
from typing import Any


class Memory:
    """JSON-compatible private memory for one character."""

    def __init__(self, values: dict[str, Any] | None = None) -> None:
        self._values: dict[str, Any] = deepcopy(values or {})

    def get(self, key: str, default: Any = None) -> Any:
        return deepcopy(self._values.get(key, default))

    def set(self, key: str, value: Any) -> None:
        self._values[key] = deepcopy(value)

    def update(self, values: dict[str, Any]) -> None:
        for key, value in values.items():
            self._values[key] = deepcopy(value)

    def as_dict(self) -> dict[str, Any]:
        return deepcopy(self._values)

    def load_dict(self, values: dict[str, Any]) -> None:
        self._values = deepcopy(values)
