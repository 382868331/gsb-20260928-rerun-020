from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Iterable

from .actions import Action, ActionContext, ActionStatus
from .events import Event
from .memory import Memory
from .promises import WAITING
from .policy import Decision, KeepCurrent, StartAction, StopCurrent
from .world import Observation, World


@dataclass(frozen=True, slots=True)
class RuntimeSnapshot:
    actor_id: str
    run_id: int
    status: str | None
    action_name: str | None
    action_config: dict[str, Any] | None
    memory: dict[str, Any]
    stale_results_ignored: int
    interrupted_runs: tuple[int, ...]
    history: tuple[dict[str, Any], ...]
    tick_records: tuple[dict[str, Any], ...]
    promise_runs: tuple[dict[str, Any], ...]
    current_promise_id: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "actor_id": self.actor_id,
            "run_id": self.run_id,
            "status": self.status,
            "action_name": self.action_name,
            "action_config": self.action_config,
            "memory": self.memory,
            "stale_results_ignored": self.stale_results_ignored,
            "interrupted_runs": list(self.interrupted_runs),
            "history": list(self.history),
            "tick_records": list(self.tick_records),
            "promise_runs": list(self.promise_runs),
            "current_promise_id": self.current_promise_id,
        }

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> "RuntimeSnapshot":
        return cls(
            actor_id=values["actor_id"],
            run_id=values["run_id"],
            status=values["status"],
            action_name=values["action_name"],
            action_config=values["action_config"],
            memory=dict(values["memory"]),
            stale_results_ignored=values["stale_results_ignored"],
            interrupted_runs=tuple(values["interrupted_runs"]),
            history=tuple(values["history"]),
            tick_records=tuple(values.get("tick_records", ())),
            promise_runs=tuple(values.get("promise_runs", ())),
            current_promise_id=values.get("current_promise_id"),
        )


class ActorRuntime:
    """Independent behavior state machine for one actor."""

    LIFECYCLE_EVENT_TYPES = frozenset(
        {"action_interrupted", "action_finished", "stale_action_result_ignored"}
    )

    def __init__(
        self,
        actor_id: str,
        policy: Any,
        world: World,
        *,
        action_types: Iterable[type[Action]] = (),
        memory: Memory | None = None,
    ) -> None:
        self.actor_id = actor_id
        self.policy = policy
        self.world = world
        self.world.register_actor(actor_id)
        self.memory = memory or Memory()
        self._action_types = {action_type.action_name: action_type for action_type in action_types}
        self.run_id = 0
        self.current_action: Action | None = None
        self.status: ActionStatus | None = None
        self.stale_results_ignored = 0
        self.interrupted_runs: list[int] = []
        self.history: list[dict[str, Any]] = []
        self.tick_records: list[dict[str, Any]] = []
        self.promise_runs: list[dict[str, Any]] = []
        self.current_promise_id: str | None = None

    def register_action_type(self, action_type: type[Action]) -> None:
        self._action_types[action_type.action_name] = action_type

    def tick(self) -> ActionStatus | None:
        observation = self.world.observe(self.actor_id)
        promise_event = self._consume_due_promise(observation)
        if promise_event is not None:
            observation = self.world.observe(self.actor_id)
        current_events, stale_events = self._partition_events(observation)
        for event in stale_events:
            self.stale_results_ignored += 1
            self.world.publish(
                "stale_action_result_ignored",
                {
                    "event_type": event.type,
                    "owner_run_id": event.action_run_id,
                    "current_run_id": self.run_id,
                },
                actor_id=self.actor_id,
                action_run_id=self.run_id,
            )

        promised_action: Action | None = None
        if promise_event is not None:
            promised_action = self._build_promised_action(promise_event)
            if promised_action is None:
                self.world.promises.abort_unknown_action(
                    promise_event.payload["promise_id"]
                )
                promise_event = None
            elif self.current_action is not None:
                # The promiser is busy: the promise keeps waiting until a later
                # tick instead of interrupting unrelated work.
                mailbox = self.world._mailboxes.setdefault(self.actor_id, [])
                mailbox.insert(0, promise_event)
                promise_event = None
                promised_action = None

        if promised_action is not None:
            self._start_promised_action(
                promised_action, promise_event, observation, current_events
            )
            current_events = (promise_event, *current_events)
        else:
            decision = self.policy.decide(
                observation, self.memory, self.current_action
            )
            self._apply_decision(decision, observation, current_events)
        self._record_tick(observation)

        if self.current_action is None:
            return self.status

        context = self._context(
            observation,
            current_events,
            promise_id=(
                promise_event.payload["promise_id"]
                if promised_action is not None
                else self.current_promise_id
            ),
        )
        status = self.current_action.tick(context)
        if status is ActionStatus.RUNNING:
            self.status = status
            return status
        if not status.terminal:
            raise ValueError(f"action returned non-terminal status: {status!r}")
        self._finish_run(status, context)
        return status

    def stop(self) -> ActionStatus | None:
        if self.current_action is None:
            return self.status
        observation = self.world.observe(self.actor_id)
        due_promise = self._consume_due_promise(observation)
        if due_promise is not None:
            mailbox = self.world._mailboxes.setdefault(self.actor_id, [])
            mailbox.insert(0, due_promise)
            observation = self.world.observe(self.actor_id)
        current_events, stale_events = self._partition_events(observation)
        self.stale_results_ignored += len(stale_events)
        context = self._context(observation, ())
        status = self._interrupt_current(context)
        self.current_action = None
        self.status = status
        return status

    def snapshot(self) -> RuntimeSnapshot:
        action_name = None
        action_config = None
        status = None
        if self.current_action is not None:
            action_name = self.current_action.action_name
            action_config = self.current_action.to_config()
            status = self.status.value if self.status is not None else None
        return RuntimeSnapshot(
            actor_id=self.actor_id,
            run_id=self.run_id,
            status=status,
            action_name=action_name,
            action_config=action_config,
            memory=self.memory.as_dict(),
            stale_results_ignored=self.stale_results_ignored,
            interrupted_runs=tuple(self.interrupted_runs),
            history=tuple(self.history),
            tick_records=tuple(self.tick_records),
            promise_runs=tuple(self.promise_runs),
            current_promise_id=self.current_promise_id,
        )

    def restore(self, snapshot: RuntimeSnapshot | dict[str, Any]) -> None:
        if isinstance(snapshot, dict):
            snapshot = RuntimeSnapshot.from_dict(snapshot)
        if snapshot.actor_id != self.actor_id:
            raise ValueError("snapshot belongs to another actor")
        self.run_id = snapshot.run_id
        self.memory.load_dict(snapshot.memory)
        self.stale_results_ignored = snapshot.stale_results_ignored
        self.interrupted_runs = list(snapshot.interrupted_runs)
        self.history = list(snapshot.history)
        self.tick_records = list(snapshot.tick_records)
        self.promise_runs = list(snapshot.promise_runs)
        self.current_promise_id = snapshot.current_promise_id
        if snapshot.action_name is None:
            self.current_action = None
            self.status = None
            return
        action_type = self._action_types.get(snapshot.action_name)
        if action_type is None:
            raise KeyError(f"unregistered action type: {snapshot.action_name}")
        self.current_action = action_type.from_config(snapshot.action_config or {})
        self.status = ActionStatus(snapshot.status) if snapshot.status else ActionStatus.RUNNING

    def _apply_decision(
        self,
        decision: Decision,
        observation: Observation,
        current_events: tuple[Event, ...],
    ) -> None:
        if isinstance(decision, KeepCurrent):
            return
        if isinstance(decision, StopCurrent):
            if self.current_action is not None:
                context = self._context(observation, ())
                status = self._interrupt_current(context)
                self.current_action = None
                self.status = status
                self.current_promise_id = None
            return
        if not isinstance(decision, StartAction):
            raise TypeError(f"unsupported decision: {decision!r}")

        if self.current_action is not None:
            context = self._context(observation, ())
            status = self._interrupt_current(context)
            self.current_action = None
            self.status = status
            self.current_promise_id = None

        self.run_id += 1
        self.current_action = decision.action
        self._action_types.setdefault(
            decision.action.action_name, type(decision.action)
        )
        self.status = ActionStatus.RUNNING
        start_context = self._context(observation, current_events)
        self.current_action.on_start(start_context)
        self._record(
            "started",
            self.current_action,
            start_context.run_id,
            observation=observation,
        )

    def _interrupt_current(self, context: ActionContext) -> ActionStatus:
        if self.current_action is None:
            raise RuntimeError("no current action to interrupt")
        action = self.current_action
        status = action.interrupt(context)
        if not status.terminal:
            status = ActionStatus.CANCELLED
        self.interrupted_runs.append(context.run_id)
        promise_id = context.promise_id
        if promise_id is None:
            promise_id = self.current_promise_id
        self._record(
            "interrupted",
            action,
            context.run_id,
            status,
            promise_id=promise_id,
        )
        self.world.publish(
            "action_interrupted",
            {"action": action.action_name, "status": status.value},
            actor_id=self.actor_id,
            action_run_id=context.run_id,
        )
        self._finish_run(
            status,
            context,
            publish_finished=False,
            promise_id=promise_id,
        )
        return status

    def _finish_run(
        self,
        status: ActionStatus,
        context: ActionContext,
        *,
        publish_finished: bool = True,
        promise_id: str | None = None,
    ) -> None:
        action = self.current_action
        if action is None:
            return
        self.status = status
        record_promise_id = promise_id
        if record_promise_id is None:
            record_promise_id = self._promise_id_for_run(context.run_id)
        self._record(
            "finished",
            action,
            context.run_id,
            status,
            promise_id=record_promise_id,
        )
        if publish_finished:
            self.world.publish(
                "action_finished",
                {
                    "action": action.action_name,
                    "status": status.value,
                    "promise_id": record_promise_id,
                },
                actor_id=self.actor_id,
                action_run_id=context.run_id,
            )
        if record_promise_id is not None:
            self.world.promises.finish_run(
                record_promise_id, context.run_id, status.value
            )
        if self.current_promise_id == record_promise_id:
            self.current_promise_id = None
        self.current_action = None

    def _partition_events(
        self, observation: Observation
    ) -> tuple[tuple[Event, ...], tuple[Event, ...]]:
        current: list[Event] = []
        stale: list[Event] = []
        for event in observation.events:
            if event.action_run_id is None:
                continue
            if event.type in self.LIFECYCLE_EVENT_TYPES:
                continue
            if self.current_action is not None and event.action_run_id == self.run_id:
                current.append(event)
            else:
                stale.append(event)
        return tuple(current), tuple(stale)

    def _context(
        self, observation: Observation, result_events: tuple[Event, ...]
        ,
        *,
        promise_id: str | None = None,
    ) -> ActionContext:
        effective_promise_id = (
            promise_id
            if promise_id is not None
            else self.current_promise_id
        )
        requester_id = None
        if effective_promise_id is not None:
            promised = self.world.promises.find(effective_promise_id)
            if promised is not None:
                requester_id = promised["requester"]
        return ActionContext(
            actor_id=self.actor_id,
            run_id=self.run_id,
            world=self.world,
            observation=observation,
            memory=self.memory,
            result_events=result_events,
            promise_id=effective_promise_id,
            requester_id=requester_id,
        )

    def _consume_due_promise(
        self, observation: Observation
    ) -> Event | None:
        """Take the oldest still-valid promise_due for this actor.

        A due event whose promise is no longer waiting was already arbitrated
        (withdrawal or participant removal); it is discarded silently.
        """
        kept: list[Event] = []
        chosen: Event | None = None
        for event in observation.events:
            if chosen is None and event.type == "promise_due":
                promise_id = event.payload.get("promise_id")
                record = self.world.promises.find(promise_id)
                if record is not None and record["status"] == WAITING:
                    chosen = event
                    continue
            kept.append(event)
        if chosen is None:
            return None
        self._requeue_events_front(kept)
        return chosen

    def _requeue_events_front(self, events: list[Event]) -> None:
        mailbox = self.world._mailboxes.setdefault(self.actor_id, [])
        mailbox[0:0] = list(events)

    def _build_promised_action(self, event: Event) -> Action | None:
        promise_id = event.payload["promise_id"]
        record = self.world.promises.find(promise_id)
        if record is None:
            return None
        action_type = self._action_types.get(record["action_name"])
        if action_type is None:
            return None
        return action_type.from_config(record["action_config"])

    def _start_promised_action(
        self,
        action: Action,
        promise_event: Event,
        observation: Observation,
        current_events: tuple[Event, ...],
    ) -> None:
        if self.current_action is not None:
            raise RuntimeError("promised action cannot replace running action")
        promise_id = promise_event.payload["promise_id"]
        record = self.world.promises.get(promise_id)
        self.run_id += 1
        self.current_action = action
        self.current_promise_id = promise_id
        self._action_types.setdefault(action.action_name, type(action))
        self.status = ActionStatus.RUNNING
        self.promise_runs.append(
            {
                "promise_id": promise_id,
                "run_id": self.run_id,
                "request_event_sequence": record["request_event_sequence"],
                "due_event_sequence": promise_event.sequence,
            }
        )
        start_context = self._context(
            observation, current_events, promise_id=promise_id
        )
        self.world.promises.start(
            promise_id, self.run_id, promise_event.sequence
        )
        action.on_start(start_context)
        self._record(
            "started",
            action,
            start_context.run_id,
            observation=observation,
            promise_id=promise_id,
        )

    def _promise_id_for_run(self, run_id: int) -> str | None:
        for entry in reversed(self.promise_runs):
            if entry["run_id"] == run_id:
                return entry["promise_id"]
        return None

    def _record(
        self,
        phase: str,
        action: Action,
        run_id: int,
        status: ActionStatus | None = None,
        *,
        observation: Observation | None = None,
        promise_id: str | None = None,
    ) -> None:
        observed_event_sequences = []
        if observation is not None:
            observed_event_sequences = [
                event.sequence for event in observation.events
            ]
        self.history.append(
            {
                "time": self.world.clock.now(),
                "actor_order": self.world.actor_order(self.actor_id),
                "phase": phase,
                "run_id": run_id,
                "action": action.action_name,
                "action_config": deepcopy(action.to_config()),
                "status": status.value if status else None,
                "promise_id": promise_id,
                "observed_event_sequences": observed_event_sequences,
            }
        )

    def _record_tick(self, observation: Observation) -> None:
        self.tick_records.append(
            {
                "time": observation.time,
                "actor_order": self.world.actor_order(self.actor_id),
                "observed_event_sequences": [
                    event.sequence for event in observation.events
                ],
            }
        )
