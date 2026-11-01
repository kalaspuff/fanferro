"""Typed snapshots of statistics, metrics and queue counts.

* :class:`QueueCounts` - :meth:`Queue.approximate_count() <fanferro.Queue.approximate_count>`.
* :class:`ConsumerStats` with :class:`EngineStats` - :meth:`Consumer.stats() <fanferro.Consumer.stats>`.
* :class:`ClientMetrics` with :class:`ClientCounters` - :meth:`Client.metrics() <fanferro.Client.metrics>`.

Each is a frozen dataclass read by attribute (``counts.visible``), so type checkers catch misspelt
names and editors complete them; ``as_dict()`` gives the plain dictionary, e.g. to log it as JSON.
The counters of :class:`EngineStats` and :class:`ClientCounters` are the Rust core's metrics
(``crates/fanferro-core/src/metrics.rs``); ``scripts/check_consistency.py`` keeps them in step.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self

if TYPE_CHECKING:
    from collections.abc import Mapping

    from fanferro.consumer import ConsumerState

__all__ = ["ClientCounters", "ClientMetrics", "ConsumerStats", "EngineState", "EngineStats", "QueueCounts"]

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
