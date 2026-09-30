from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .actions import Action
from .clock import VirtualClock
from .runtime import ActorRuntime, RuntimeSnapshot
from .world import World


@dataclass(frozen=True, slots=True)
class BranchInfo:
    branch_id: str
    checkpoint_id: str
    parent_branch_id: str | None
    parent_checkpoint_id: str | None
    time: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "branch_id": self.branch_id,
            "checkpoint_id": self.checkpoint_id,
            "parent_branch_id": self.parent_branch_id,
            "parent_checkpoint_id": self.parent_checkpoint_id,
            "time": self.time,
        }

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> "BranchInfo":
        return cls(
            branch_id=values["branch_id"],
            checkpoint_id=values["checkpoint_id"],
            parent_branch_id=values.get("parent_branch_id"),
            parent_checkpoint_id=values.get("parent_checkpoint_id"),
            time=float(values.get("time", 0.0)),
        )


class WorldArchive:
    """Serializable, independently writable checkpoint for a whole world."""

    def __init__(self, data: Mapping[str, Any]) -> None:
        self._data = deepcopy(dict(data))

    @property
    def data(self) -> dict[str, Any]:
        return deepcopy(self._data)

    @property
    def branch(self) -> BranchInfo:
        return BranchInfo.from_dict(self._data["branch"])

    @property
    def actor_ids(self) -> tuple[str, ...]:
        return tuple(entry["snapshot"]["actor_id"] for entry in self._data["actors"])

    @classmethod
    def capture(
        cls,
        world: World,
        runtimes: Iterable[ActorRuntime],
        *,
        checkpoint_id: str,
        branch_id: str = "main",
        parent: "WorldArchive | BranchInfo | None" = None,
    ) -> "WorldArchive":
        if isinstance(runtimes, Mapping):
            runtime_by_actor = dict(runtimes)
        else:
            runtime_by_actor = {runtime.actor_id: runtime for runtime in runtimes}
        registered_actors = set(world.actor_ids)
        missing = registered_actors.difference(runtime_by_actor)
        if missing:
            raise ValueError(f"missing runtimes for actors: {sorted(missing)}")
        foreign_runtimes = [
            actor_id
            for actor_id, runtime in runtime_by_actor.items()
            if actor_id in registered_actors and runtime.world is not world
        ]
        if foreign_runtimes:
            raise ValueError(
                f"runtimes belong to another world: {sorted(foreign_runtimes)}"
            )

        if isinstance(parent, WorldArchive):
            parent_info = parent.branch
        else:
            parent_info = parent
        info = BranchInfo(
            branch_id=branch_id,
            checkpoint_id=checkpoint_id,
            parent_branch_id=parent_info.branch_id if parent_info else None,
            parent_checkpoint_id=parent_info.checkpoint_id if parent_info else None,
            time=world.clock.now(),
        )
        actors = []
        for actor_id in world.actor_ids:
            runtime = runtime_by_actor[actor_id]
            actors.append(
                {
                    "snapshot": runtime.snapshot().to_dict(),
                    "action_types": sorted(runtime._action_types),
                }
            )
        return cls(
            {
                "version": 1,
                "branch": info.to_dict(),
                "world": world.snapshot(),
                "actors": actors,
            }
        )

    def restore_world(self, clock: Any | None = None) -> World:
        world_data = deepcopy(self._data["world"])
        world = World(clock=clock or VirtualClock())
        world.restore(world_data)
        return world

    def restore(
        self,
        policies: Mapping[str, Any],
        *,
        action_types: Mapping[str, Iterable[type[Action]]] | None = None,
        world: World | None = None,
    ) -> tuple[World, dict[str, ActorRuntime]]:
        missing_policies = [
            actor_id for actor_id in self.actor_ids if actor_id not in policies
        ]
        if missing_policies:
            raise ValueError(f"missing policies for actors: {missing_policies}")

        restored_world = world or self.restore_world()
        action_types = action_types or {}
        runtimes: dict[str, ActorRuntime] = {}
        for entry in self._data["actors"]:
            snapshot_data = deepcopy(entry["snapshot"])
            actor_id = snapshot_data["actor_id"]
            registered_types = list(action_types.get(actor_id, ()))
            known = {action_type.action_name for action_type in registered_types}
            for action_name in entry.get("action_types", ()):
                if action_name not in known:
                    raise KeyError(
                        f"missing action type {action_name!r} for actor {actor_id!r}"
                    )
            runtime = ActorRuntime(
                actor_id,
                policies[actor_id],
                restored_world,
                action_types=registered_types,
            )
            runtime.restore(RuntimeSnapshot.from_dict(snapshot_data))
            runtimes[actor_id] = runtime
        return restored_world, runtimes

    def to_dict(self) -> dict[str, Any]:
        return self.data

    @classmethod
    def from_dict(cls, values: Mapping[str, Any]) -> "WorldArchive":
        return cls(values)
