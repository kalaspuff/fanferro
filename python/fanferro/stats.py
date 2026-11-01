"""Typed snapshots of statistics, metrics and queue counts.

* :class:`QueueCounts` - :meth:`Queue.approximate_count() <fanferro.Queue.approximate_count>`.
* :class:`ConsumerStats` with :class:`EngineStats` - :meth:`Consumer.stats() <fanferro.Consumer.stats>`.
* :class:`ClientMetrics` with :class:`ClientCounters` - :meth:`Client.metrics() <fanferro.Client.metrics>`.
* :class:`ConsumerHealth` with :class:`ReceiveFailure` - :meth:`Consumer.health() <fanferro.Consumer.health>`,
  and :class:`ClientHealth` - :meth:`Client.health() <fanferro.Client.health>`: liveness and readiness.

Each is a frozen dataclass read by attribute (``counts.visible``), so type checkers catch misspelt
names and editors complete them; ``as_dict()`` gives the plain dictionary, e.g. to log it as JSON.
The counters of :class:`EngineStats` and :class:`ClientCounters` are the Rust core's metrics
(``crates/fanferro-core/src/metrics.rs``); ``scripts/check_consistency.py`` keeps them in step.
"""

from __future__ import annotations

import dataclasses
import datetime
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self

from fanferro._utils import Duration, seconds

if TYPE_CHECKING:
    from collections.abc import Mapping

    from fanferro.consumer import ConsumerState

__all__ = [
    "DEFAULT_MAX_DISPATCH_GAP",
    "DEFAULT_MAX_RECEIVE_GAP",
    "ClientCounters",
    "ClientHealth",
    "ClientMetrics",
    "ConsumerHealth",
    "ConsumerStats",
    "EngineState",
    "EngineStats",
    "QueueCounts",
    "ReceiveFailure",
]

DEFAULT_MAX_RECEIVE_GAP = 60.0
"""Seconds a running consumer may go without a successful receive before it counts as unhealthy.

Long enough for a long poll (``wait_time`` of at most 20 seconds) and a few retried failures (the
receive backoff starts at 1 second and doubles up to 30), short enough to catch a queue that is
gone or credentials that stopped working within a minute or two."""

DEFAULT_MAX_DISPATCH_GAP = 30.0
"""Seconds a running consumer's dispatcher may go without running before it counts as unhealthy.

The dispatcher runs at least twice a second while the event loop is responsive; a longer gap
means the loop was blocked (a CPU-bound or blocking handler)."""

type EngineState = Literal["created", "running", "stopping", "stopped"]
"""State of a consumer's receive engine."""


class _Snapshot:
    __slots__ = ()
    if TYPE_CHECKING:
        __dataclass_fields__: ClassVar[dict[str, dataclasses.Field[Any]]]

    def as_dict(self) -> dict[str, Any]:
        """The snapshot as a plain (JSON-serializable) dictionary, nested snapshots included."""
        return dataclasses.asdict(self)


def _counters[T: _Snapshot](cls: type[T], values: Mapping[str, int], **extra: Any) -> T:
    """Builds ``cls`` from the core's counters by field name (a counter it does not report is 0)."""
    names = [field.name for field in dataclasses.fields(cls) if field.name not in extra]
    return cls(**{name: values.get(name, 0) for name in names}, **extra)


@dataclass(frozen=True, slots=True, kw_only=True)
class QueueCounts(_Snapshot):
    """Approximate message counts of a queue (SQS updates them with a delay of up to a minute).

    Attributes:
        visible: Messages available for receiving (``ApproximateNumberOfMessages``).
        in_flight: Messages received and not yet deleted or made visible again
            (``ApproximateNumberOfMessagesNotVisible``).
        delayed: Messages sent with a delay that are not available yet
            (``ApproximateNumberOfMessagesDelayed``).
    """

    visible: int
    in_flight: int
    delayed: int

    @property
    def total(self) -> int:
        """All messages in the queue: visible, in flight and delayed."""
        return self.visible + self.in_flight + self.delayed

    @classmethod
    def from_attributes(cls, attributes: Mapping[str, str]) -> Self:
        """Reads the counts from the queue attributes SQS returns (``GetQueueAttributes``)."""
        return cls(
            visible=int(attributes.get("ApproximateNumberOfMessages", 0)),
            in_flight=int(attributes.get("ApproximateNumberOfMessagesNotVisible", 0)),
            delayed=int(attributes.get("ApproximateNumberOfMessagesDelayed", 0)),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class EngineStats(_Snapshot):
    """Counters and gauges of a consumer's receive engine (the Rust core).

    Counters count since the consumer was created; gauges (``in_flight``, ``settling``, ``buffered``,
    ``pollers``) are current values. Before the consumer first starts, every value is 0.

    Attributes:
        state: The engine's state.
        paused: Whether receiving is paused (:meth:`Consumer.pause() <fanferro.Consumer.pause>`).
        leases: Messages whose lease the engine holds.
        available_permits: Free in-flight slots (of ``max_in_flight``).
        receive_requests: ReceiveMessage calls.
        empty_receives: ReceiveMessage calls that returned nothing.
        receive_errors: ReceiveMessage calls that failed.
        delete_requests: DeleteMessageBatch calls made to settle messages (acks, dead-letters,
            requeues).
        visibility_requests: ChangeMessageVisibilityBatch calls made to settle messages (retries,
            releases, explicit extensions; heartbeats are counted as ``lease_extensions``).
        messages_received: Messages received.
        messages_acked: Messages deleted after processing.
        ack_failures: Deletes that failed.
        messages_retried: Messages whose visibility was changed for a retry.
        messages_requeued: Messages re-enqueued as new messages.
        messages_dead_lettered: Messages forwarded to the dead-letter queue.
        dead_letter_failures: Dead-letter forwards that failed.
        messages_released: Messages released (made visible) without processing.
        decode_failures: Messages that failed transport decoding.
        payloads_fetched: Offloaded payloads fetched.
        lease_extensions: Successful visibility extensions.
        lease_extension_failures: Failed visibility extensions.
        leases_lost: Leases whose receipt handle became invalid.
        leases_expired: Leases that hit the maximum lease duration.
        in_flight: Messages leased and not yet settled (buffered, processing or settling).
        settling: Messages whose settlement is in progress (their in-flight slot is free again).
        buffered: Messages decoded and waiting to be picked up by the dispatcher.
        pollers: Receive loops running.
    """

    state: EngineState
    paused: bool
    leases: int
    available_permits: int
    receive_requests: int
    empty_receives: int
    receive_errors: int
    delete_requests: int
    visibility_requests: int
    messages_received: int
    messages_acked: int
    ack_failures: int
    messages_retried: int
    messages_requeued: int
    messages_dead_lettered: int
    dead_letter_failures: int
    messages_released: int
    decode_failures: int
    payloads_fetched: int
    lease_extensions: int
    lease_extension_failures: int
    leases_lost: int
    leases_expired: int
    in_flight: int
    settling: int
    buffered: int
    pollers: int

    @classmethod
    def _from_core(cls, raw: Mapping[str, Any] | None) -> Self:
        """Builds the snapshot from ``_core.ConsumerEngine.stats()`` (``None``: no engine yet)."""
        if raw is None:
            return _counters(cls, {}, state="created", paused=False, leases=0, available_permits=0)
        return _counters(
            cls,
            raw.get("metrics", {}),
            state=raw["state"],
            paused=bool(raw["paused"]),
            leases=int(raw["leases"]),
            available_permits=int(raw["available_permits"]),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class ConsumerStats(_Snapshot):
    """Processing statistics of a consumer.

    Attributes:
        name: The consumer's name.
        queue: Name of the consumed queue.
        state: The consumer's state.
        active: Messages being processed by handlers right now.
        processed: Messages whose handler returned (or that were settled by an action).
        failed: Handler calls that raised.
        duplicates: Messages skipped as duplicates by the idempotency store.
        unroutable: Messages no route matched.
        decode_failures: Messages whose envelope, transport or codec could not decode them.
        handler_seconds: Total time spent in handlers, in seconds.
        engine: The receive engine's counters and gauges.
    """

    name: str
    queue: str
    state: ConsumerState
    active: int
    processed: int
    failed: int
    duplicates: int
    unroutable: int
    decode_failures: int
    handler_seconds: float
    engine: EngineStats


@dataclass(frozen=True, slots=True, kw_only=True)
class ClientCounters(_Snapshot):
    """Client-wide publishing and payload counters (since the client was created).

    Attributes:
        messages_published: Messages sent to queues or published to topics.
        publish_failures: Publish operations that failed.
        bytes_published: Encoded bytes published (body plus attributes).
        publish_retries: Batch entries retried after partial failures.
        payloads_offloaded: Payloads offloaded to external storage.
        bytes_offloaded: Bytes offloaded to external storage.
    """

    messages_published: int
    publish_failures: int
    bytes_published: int
    publish_retries: int
    payloads_offloaded: int
    bytes_offloaded: int

    @classmethod
    def _from_core(cls, raw: Mapping[str, int] | None) -> Self:
        """Builds the snapshot from ``_core.CoreClient.metrics()`` (``None``: not connected yet)."""
        return _counters(cls, raw or {})


@dataclass(frozen=True, slots=True, kw_only=True)
class ClientMetrics(_Snapshot):
    """On-demand metrics of a client and its consumers.

    Attributes:
        client: Publishing and payload counters.
        consumers: Statistics of each consumer, by consumer name.
    """

    client: ClientCounters
    consumers: Mapping[str, ConsumerStats]


def _isoformat_times(items: list[tuple[str, Any]]) -> dict[str, Any]:
    """``dataclasses.asdict`` factory that writes datetimes as ISO 8601 strings."""
    return {key: value.isoformat() if isinstance(value, datetime.datetime) else value for key, value in items}


class _HealthSnapshot(_Snapshot):
    __slots__ = ()

    def as_dict(self) -> dict[str, Any]:
        """The snapshot as a plain (JSON-serializable) dictionary; times are ISO 8601 strings."""
        return dataclasses.asdict(self, dict_factory=_isoformat_times)


@dataclass(frozen=True, slots=True, kw_only=True)
class ReceiveFailure(_HealthSnapshot):
    """A receive call of a consumer that failed (see :attr:`ConsumerHealth.last_error`).

    Attributes:
        error_type: Qualified name of the exception, e.g. ``fanferro.errors.ResourceNotFoundError``.
        category: The error's category, e.g. ``not_found``, ``access_denied``, ``throttling``.
        code: The AWS error code, e.g. ``AWS.SimpleQueueService.NonExistentQueue``, if AWS answered.
        message: The error message.
        retryable: Whether the same receive may succeed later unchanged (throttling, timeouts,
            network errors); a missing queue or denied access are not.
        at: When the receive failed (UTC).
    """

    error_type: str
    category: str
    code: str | None
    message: str
    retryable: bool
    at: datetime.datetime

    @classmethod
    def _from_error(cls, error: BaseException, at: datetime.datetime) -> Self:
        kind = type(error)
        return cls(
            error_type=f"{kind.__module__}.{kind.__qualname__}",
            category=str(getattr(error, "category", "unknown")),
            code=getattr(error, "code", None),
            message=str(error),
            retryable=bool(getattr(error, "retryable", False)),
            at=at,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class ConsumerHealth(_HealthSnapshot):
    """Liveness and readiness of a consumer, for health checks and dashboards.

    :meth:`healthy` is the liveness verdict (restart the process when it stays ``False``),
    :meth:`ready` the readiness verdict (send no work while it is ``False``), and :meth:`problems`
    says what is wrong. A paused consumer is healthy but not ready; a stopping one is healthy (it
    is shutting down as asked) but not ready; one that was never started or has stopped is
    neither.

    Attributes:
        name: The consumer's name.
        queue: Name of the consumed queue.
        state: The consumer's state.
        paused: Whether receiving is paused (:meth:`Consumer.pause() <fanferro.Consumer.pause>`).
        last_receive_at: When a receive last succeeded, an empty one included (UTC; ``None`` before
            the first).
        receive_gap: Seconds the consumer has been receiving without a success: since the last
            successful receive (or the start), leaving out the time it waited for free in-flight
            slots, the rate limiter or a resume, when it did not want messages.
        consecutive_receive_failures: Receives that failed since the last one that succeeded.
        last_error: The latest failed receive, also after later receives succeeded (``None`` if
            none failed).
        last_dispatch_at: When the dispatcher, which hands received messages to handlers on the
            event loop, last ran (UTC; ``None`` before it first ran).
        dispatch_gap: Seconds since the dispatcher last ran (``None`` before it first ran). It runs
            at least twice a second while the event loop is responsive.
        active: Messages being processed by handlers right now.
        in_flight: Messages received and not yet settled (buffered, processing or settling).
    """

    name: str
    queue: str
    state: ConsumerState
    paused: bool
    last_receive_at: datetime.datetime | None
    receive_gap: float
    consecutive_receive_failures: int
    last_error: ReceiveFailure | None
    last_dispatch_at: datetime.datetime | None
    dispatch_gap: float | None
    active: int
    in_flight: int

    def problems(
        self,
        *,
        max_receive_gap: Duration = DEFAULT_MAX_RECEIVE_GAP,
        max_dispatch_gap: Duration = DEFAULT_MAX_DISPATCH_GAP,
    ) -> tuple[str, ...]:
        """What makes the consumer unhealthy; empty when it is healthy.

        Args:
            max_receive_gap: Longest :attr:`receive_gap` of a healthy running consumer (seconds or a
                ``timedelta``; :data:`DEFAULT_MAX_RECEIVE_GAP`).
            max_dispatch_gap: Longest :attr:`dispatch_gap` of a healthy running consumer (seconds or
                a ``timedelta``; :data:`DEFAULT_MAX_DISPATCH_GAP`).
        """
        receive_limit = seconds(max_receive_gap, "max_receive_gap")
        dispatch_limit = seconds(max_dispatch_gap, "max_dispatch_gap")
        if self.state in ("created", "stopped"):
            text = f"consumer {self.name} is {self.state}"
            if self.state == "stopped" and self.last_error is not None and self.consecutive_receive_failures:
                text += f" after {self.consecutive_receive_failures} failed receives ({self._last_error_text()})"
            return (text,)
        if self.state != "running":
            return ()
        problems: list[str] = []
        if not self.paused and self.receive_gap > receive_limit:
            text = f"consumer {self.name} has received nothing from queue {self.queue} for {self.receive_gap:.0f} s"
            if self.consecutive_receive_failures:
                text += f": {self.consecutive_receive_failures} receives failed ({self._last_error_text()})"
            problems.append(text)
        if self.dispatch_gap is not None and self.dispatch_gap > dispatch_limit:
            problems.append(
                f"consumer {self.name} has not dispatched messages for {self.dispatch_gap:.0f} s;"
                " is the event loop blocked?"
            )
        return tuple(problems)

    def healthy(
        self,
        *,
        max_receive_gap: Duration = DEFAULT_MAX_RECEIVE_GAP,
        max_dispatch_gap: Duration = DEFAULT_MAX_DISPATCH_GAP,
    ) -> bool:
        """Liveness: the consumer is starting, running or stopping, and receives and dispatches.

        See :meth:`problems` for the arguments.
        """
        return not self.problems(max_receive_gap=max_receive_gap, max_dispatch_gap=max_dispatch_gap)

    def ready(
        self,
        *,
        max_receive_gap: Duration = DEFAULT_MAX_RECEIVE_GAP,
        max_dispatch_gap: Duration = DEFAULT_MAX_DISPATCH_GAP,
    ) -> bool:
        """Readiness: the consumer is running, not paused and healthy (see :meth:`problems`)."""
        return (
            self.state == "running"
            and not self.paused
            and self.healthy(max_receive_gap=max_receive_gap, max_dispatch_gap=max_dispatch_gap)
        )

    def _last_error_text(self) -> str:
        error = self.last_error
        if error is None:
            return "no error recorded"
        name = error.error_type.rpartition(".")[2]
        return f"last: {name} {error.code}" if error.code else f"last: {name}"


@dataclass(frozen=True, slots=True, kw_only=True)
class ClientHealth(_HealthSnapshot):
    """Liveness and readiness of every consumer of a client (see :class:`ConsumerHealth`).

    A client without consumers is healthy and ready.

    Attributes:
        consumers: Health of each consumer, by consumer name.
    """

    consumers: Mapping[str, ConsumerHealth]

    def problems(
        self,
        *,
        max_receive_gap: Duration = DEFAULT_MAX_RECEIVE_GAP,
        max_dispatch_gap: Duration = DEFAULT_MAX_DISPATCH_GAP,
    ) -> tuple[str, ...]:
        """What makes any consumer unhealthy (see :meth:`ConsumerHealth.problems`)."""
        return tuple(
            problem
            for health in self.consumers.values()
            for problem in health.problems(max_receive_gap=max_receive_gap, max_dispatch_gap=max_dispatch_gap)
        )

    def healthy(
        self,
        *,
        max_receive_gap: Duration = DEFAULT_MAX_RECEIVE_GAP,
        max_dispatch_gap: Duration = DEFAULT_MAX_DISPATCH_GAP,
    ) -> bool:
        """Liveness: every consumer is healthy (see :meth:`ConsumerHealth.healthy`)."""
        return not self.problems(max_receive_gap=max_receive_gap, max_dispatch_gap=max_dispatch_gap)

    def ready(
        self,
        *,
        max_receive_gap: Duration = DEFAULT_MAX_RECEIVE_GAP,
        max_dispatch_gap: Duration = DEFAULT_MAX_DISPATCH_GAP,
    ) -> bool:
        """Readiness: every consumer is ready (see :meth:`ConsumerHealth.ready`)."""
        return all(
            health.ready(max_receive_gap=max_receive_gap, max_dispatch_gap=max_dispatch_gap)
            for health in self.consumers.values()
        )
