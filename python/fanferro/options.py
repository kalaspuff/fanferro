"""The keyword options of publishing, consumers, subscriptions, provisioning and clients, as TypedDicts.

Methods that pass their keyword arguments on take them as ``**options: Unpack[...]`` (PEP 692), so
type checkers and editors complete them and report a misspelt or mistyped option::

    await orders.publish(order, group_id="eu", attributes={"tenant": "acme"})
    await jobs.send(job, delay=30)  # `delay` only exists for queues
    consumer = client.consumer("orders-worker", concurrency=20, handler_timeout=30)

The dicts can also hold settings shared by several calls::

    worker_options: fanferro.ConsumerOptions = {"concurrency": 20, "retry": RetryPolicy(max_attempts=5)}
    consumer = client.consumer("orders-worker", **worker_options)

Every key is optional; the defaults are those of the parameter or field each option sets
(:class:`fanferro.OutgoingMessage`, :class:`fanferro.Consumer`, :meth:`fanferro.Client.subscription`,
:class:`fanferro.Provisioning`, :class:`fanferro.Client`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TypedDict

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence
    from datetime import datetime

    from fanferro._core import MemoryBroker
    from fanferro._utils import Duration
    from fanferro.codecs import Codec, CodecRegistry
    from fanferro.config import AwsRetry, Credentials, Timeouts
    from fanferro.envelopes import Envelope
    from fanferro.events import LoggingConfig
    from fanferro.messages import AttributeInput, HeaderValue
    from fanferro.middleware import ConsumeMiddleware, PublishMiddleware
    from fanferro.payloads import LargePayloads, PayloadFetch
    from fanferro.policies import DeadLetterPolicy, Heartbeat, RateLimit, RetryPolicy, ShutdownPolicy
    from fanferro.provisioning import FailureMode, Provisioning
    from fanferro.resources import Queue
    from fanferro.telemetry import Telemetry

__all__ = [
    "ClientOptions",
    "ConsumerOptions",
    "ProvisioningOverrides",
    "PublishOptions",
    "QueuePublishOptions",
    "SubscriptionOptions",
    "TopicPublishOptions",
]


class PublishOptions(TypedDict, total=False):
    """The :class:`fanferro.OutgoingMessage` fields that topics and queues both take.

    ``attributes`` and ``headers`` take any mapping: the message keeps its own copy as a ``dict``.

    ``publish``, ``prepare`` and ``publish_batch`` of a topic take :class:`TopicPublishOptions`, those
    of a queue (and ``send``/``send_batch``) :class:`QueuePublishOptions`.
    """

    type: str | None
    attributes: Mapping[str, AttributeInput]
    headers: Mapping[str, HeaderValue]
    id: str
    time: datetime | None
    subject: str | None
    source: str | None
    data_schema: str | None
    content_type: str | None
    group_id: str | None
    deduplication_id: str | None
    codec: Codec | str | None
    envelope: Envelope | None
    trace_header: str | None


class TopicPublishOptions(PublishOptions, total=False):
    """Options of a message published to a topic: :class:`PublishOptions` and ``sns_subject``."""

    sns_subject: str | None


class QueuePublishOptions(PublishOptions, total=False):
    """Options of a message sent to a queue: :class:`PublishOptions` and ``delay``."""

    delay: Duration | None


class ConsumerOptions(TypedDict, total=False):
    """The options of :meth:`fanferro.Client.consumer` and ``queue.consumer`` (see :class:`fanferro.Consumer`)."""

    name: str | None
    concurrency: int
    prefetch: int | None
    batch_size: int
    min_receive_batch: int | None
    receive_batch_wait: Duration
    wait_time: Duration
    pollers: int
    max_pollers: int | None
    rate_limit: RateLimit | float | None
    visibility_timeout: Duration | None
    heartbeat: Heartbeat | bool
    retry: RetryPolicy | None
    dead_letter: DeadLetterPolicy | None
    shutdown: ShutdownPolicy | None
    handler_timeout: Duration | None
    ordering: Literal["auto", "fifo", "none"]
    strict_ordering: bool
    envelopes: Sequence[Envelope] | None
    middleware: Sequence[ConsumeMiddleware]
    auto_ack: bool
    settle_linger: Duration
    sns_unwrap: bool
    auto_confirm_subscriptions: bool
    delete_offloaded_payloads: bool
    max_receive_failures: int | None


class SubscriptionOptions(TypedDict, total=False):
    """The options of ``topic.subscribe`` and ``queue.subscribe_to`` (see :meth:`fanferro.Client.subscription`)."""

    raw_message_delivery: bool | None
    filter_policy: Mapping[str, Any] | None
    filter_policy_scope: Literal["MessageAttributes", "MessageBody"] | None
    dead_letter_queue: Queue | str | None
    manage_queue_policy: bool | None
    provisioning: Provisioning | None


class ProvisioningOverrides(TypedDict, total=False):
    """The :class:`fanferro.Provisioning` fields that ``Provisioning.ensure(...)`` and the other presets override."""

    lookup: bool
    verify: bool
    create: bool
    update: bool
    on_missing: FailureMode
    on_drift: FailureMode
    on_error: FailureMode


class ClientOptions(TypedDict, total=False):
    """The options of :class:`fanferro.Client`, as :func:`fanferro.testing.memory_client` takes them."""

    region: str | None
    endpoint_url: str | None
    profile: str | None
    credentials: Credentials | None
    account_id: str | None
    queue_prefix: str
    topic_prefix: str
    bucket_prefix: str
    source: str
    envelope: Envelope | None
    accept_envelopes: Sequence[Envelope] | None
    codecs: CodecRegistry | Iterable[Codec] | None
    naming: Callable[[type], str] | None
    large_payloads: LargePayloads | None
    payload_fetch: PayloadFetch | None
    telemetry: Telemetry | None
    logging: LoggingConfig | None
    provisioning: Provisioning | None
    aws_retry: AwsRetry | None
    timeouts: Timeouts | None
    publish_retry_attempts: int
    verify_md5: bool
    split_batches_by_trace_header: bool
    publish_middleware: Sequence[PublishMiddleware]
    consume_middleware: Sequence[ConsumeMiddleware]
    broker: MemoryBroker | None
    event_buffer: int
    app_name: str | None
    auto_provision: bool
