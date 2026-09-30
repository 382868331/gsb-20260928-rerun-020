"""Whole-world save points and isolated branching.

A :class:`WorldCheckpoint` captures everything needed to rebuild a running
simulation independently of the process that produced it: the shared world
state and clock, delayed events that have not arrived yet, dispatched events
sitting unread in actor mailboxes, and each actor's in-flight action wait and
private memory.

Two branches created from the same checkpoint receive deep copies, so writable
memory, world state and unprocessed messages are never shared.  Each checkpoint
records lineage metadata describing the point it branched from and which actor
decisions changed afterwards.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from .actions import Action
from .runtime import ActorRuntime, RuntimeSnapshot
from .world import World


@dataclass(frozen=True, slots=True)
class Lineage:
    """Provenance of a checkpoint inside a branch tree."""

    checkpoint_id: str
    parent_id: str | None
    branch_name: str
    root_id: str
    depth: int
    reason: str | None = None

    def to_data(self) -> dict[str, Any]:
        return {
            "checkpoint_id": self.checkpoint_id,
            "parent_id": self.parent_id,
            "branch_name": self.branch_name,
            "root_id": self.root_id,
            "depth": self.depth,
            "reason": self.reason,
        }

    @classmethod
    def from_data(cls, data: dict[str, Any]) -> "Lineage":
        return cls(
            checkpoint_id=data["checkpoint_id"],
            parent_id=data.get("parent_id"),
            branch_name=data["branch_name"],
            root_id=data["root_id"],
            depth=int(data["depth"]),
            reason=data.get("reason"),
        )


@dataclass(frozen=True, slots=True)
class WorldCheckpoint:
    """Immutable serializable image of an entire world and its actors."""

    world: dict[str, Any]
    actors: tuple[RuntimeSnapshot, ...]
    lineage: Lineage
    actor_order: tuple[str, ...] = field(default=())

    def to_dict(self) -> dict[str, Any]:
        return {
            "world": self.world,
            "actors": [snapshot.to_dict() for snapshot in self.actors],
            "lineage": self.lineage.to_data(),
            "actor_order": list(self.actor_order),
            "content_hash": self.content_hash,
        }

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> "WorldCheckpoint":
        return cls(
            world=copy.deepcopy(values["world"]),
            actors=tuple(
                RuntimeSnapshot.from_dict(snapshot)
                for snapshot in values.get("actors", ())
            ),
            lineage=Lineage.from_data(values["lineage"]),
            actor_order=tuple(values.get("actor_order") or ()),
        )

    @property
    def content_hash(self) -> str:
        payload = {
            "world": self.world,
            "actors": [snapshot.to_dict() for snapshot in self.actors],
            "actor_order": list(self.actor_order),
        }
        encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


class WorldArchive:
    """Captures, forks and rebuilds whole-world checkpoints."""

    def __init__(self) -> None:
        self._seen = 0

    def capture(
        self,
        world: World,
        runtimes: Iterable[ActorRuntime],
        *,
        branch_name: str = "root",
        parent: WorldCheckpoint | Lineage | None = None,
        reason: str | None = None,
    ) -> WorldCheckpoint:
        runtimes = list(runtimes)
        for runtime in runtimes:
            if runtime.world is not world:
                raise ValueError(
                    f"actor {runtime.actor_id!r} is bound to a different world"
                )
        snapshots = tuple(runtime.snapshot() for runtime in runtimes)
        actor_order = tuple(runtime.actor_id for runtime in runtimes)

        lineage_data = self._child_lineage(parent)
        lineage = Lineage(
            checkpoint_id=lineage_data["checkpoint_id"],
            parent_id=lineage_data["parent_id"],
            branch_name=branch_name,
            root_id=lineage_data["root_id"],
            depth=lineage_data["depth"],
            reason=reason,
        )
        return WorldCheckpoint(
            world=world.to_data(),
            actors=snapshots,
            lineage=lineage,
            actor_order=actor_order,
        )

    def branch(
        self,
        checkpoint: WorldCheckpoint,
        *,
        branch_name: str,
        policies: dict[str, Any] | None = None,
        action_types: Iterable[type[Action]] | None = None,
        mutate: Callable[[World, dict[str, ActorRuntime]], None] | None = None,
        reason: str | None = None,
    ) -> WorldCheckpoint:
        """Fork a checkpoint without disturbing the source branch."""
        world, runtimes = self.restore(
            checkpoint, policies=policies, action_types=action_types
        )
        if mutate is not None:
            mutate(world, runtimes)
        return self.capture(
            world,
            self._ordered_runtimes(runtimes, checkpoint.actor_order),
            branch_name=branch_name,
            parent=checkpoint,
            reason=reason,
        )

    def restore(
        self,
        checkpoint: WorldCheckpoint | dict[str, Any],
        *,
        policies: dict[str, Any] | None = None,
        action_types: Iterable[type[Action]] | None = None,
    ) -> tuple[World, dict[str, ActorRuntime]]:
        """Rebuild independent world and actor instances from a checkpoint."""
        if isinstance(checkpoint, dict):
            checkpoint = WorldCheckpoint.from_dict(checkpoint)
        policies = policies or {}
        action_type_list = list(action_types or ())
        world = World.from_data(copy.deepcopy(checkpoint.world))
        runtimes: dict[str, ActorRuntime] = {}
        for snapshot in checkpoint.actors:
            actor_id = snapshot.actor_id
            policy = policies.get(actor_id)
            if policy is None:
                raise KeyError(f"no policy supplied for actor {actor_id!r}")
            runtime = ActorRuntime(
                actor_id,
                policy,
                world,
                action_types=[action_type for action_type in action_type_list],
            )
            runtime.restore(snapshot)
            runtimes[actor_id] = runtime
        return world, runtimes

    def _ordered_runtimes(
        self, runtimes: dict[str, ActorRuntime], order: tuple[str, ...]
    ) -> list[ActorRuntime]:
        if not order:
            return list(runtimes.values())
        return [runtimes[actor_id] for actor_id in order if actor_id in runtimes]

    def _child_lineage(
        self, parent: WorldCheckpoint | Lineage | None
    ) -> dict[str, Any]:
        self._seen += 1
        checkpoint_id = f"cp-{self._seen:04d}"
        if parent is None:
            return {
                "checkpoint_id": checkpoint_id,
                "parent_id": None,
                "root_id": checkpoint_id,
                "depth": 0,
            }
        lineage = parent.lineage if isinstance(parent, WorldCheckpoint) else parent
        return {
            "checkpoint_id": checkpoint_id,
            "parent_id": lineage.checkpoint_id,
            "root_id": lineage.root_id,
            "depth": lineage.depth + 1,
        }


def decisions_since(
    checkpoint: WorldCheckpoint, runtimes: dict[str, ActorRuntime]
) -> dict[str, list[dict[str, Any]]]:
    """Return actor history entries produced after a checkpoint was taken."""
    base_lengths = {
        snapshot.actor_id: len(snapshot.history)
        for snapshot in checkpoint.actors
    }
    result: dict[str, list[dict[str, Any]]] = {}
    for actor_id, runtime in runtimes.items():
        base = base_lengths.get(actor_id, 0)
        result[actor_id] = copy.deepcopy(runtime.history[base:])
    return result


def diff_decisions(
    left: dict[str, list[dict[str, Any]]],
    right: dict[str, list[dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    """Compare post-branch decisions for two restored branches."""
    diff: dict[str, dict[str, Any]] = {}
    for actor_id in sorted(set(left) | set(right)):
        left_entries = left.get(actor_id, [])
        right_entries = right.get(actor_id, [])
        diff[actor_id] = {
            "identical": left_entries == right_entries,
            "left": copy.deepcopy(left_entries),
            "right": copy.deepcopy(right_entries),
        }
    return diff
