"""The :class:`Client`: connection, resources, consumers, events and telemetry."""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
import signal
from typing import TYPE_CHECKING, Any, Literal, Self, overload

from fanferro import _core
from fanferro._core import CoreClient, MemoryBroker
from fanferro._utils import LoopLocal, maybe_await
from fanferro.codecs import Codec, CodecRegistry
from fanferro.config import AwsRetry, Credentials, Timeouts
from fanferro.envelopes import Envelope, Raw, as_tuple
from fanferro.errors import ConfigurationError, ProvisioningError, ResourceUnavailableError
from fanferro.events import Event, EventDispatcher, EventHook, EventKind, KindSelector, LoggingConfig
from fanferro.message_types import MessageTypes
from fanferro.messages import BatchFailure, BatchPublishResult, PreparedMessage, PublishResult
from fanferro.payloads import LargePayloads, PayloadFetch
from fanferro.provisioning import (
    DeadLetterQueue,
    Fallback,
    Provisioning,
    ProvisioningFailure,
    ProvisioningReport,
    QueueAttributes,
    TopicAttributes,
    attributes_to_aws,
)
from fanferro.resources import _UNSET, GroupId, Queue, Subscription, Topic
from fanferro.routing import Router
from fanferro.stats import ClientCounters, ClientHealth, ClientMetrics
from fanferro.telemetry import Telemetry, TelemetryRuntime, monotonic

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence

    from fanferro.consumer import Consumer
    from fanferro.middleware import ConsumeMiddleware, PublishMiddleware
    from fanferro.routing import Handler

__all__ = ["Client"]

_FORK_HOOK_REGISTERED = False

_NAME_LIMITS = {"queue": ("SQS", 80), "topic": ("SNS", 256)}
"""Service and maximum length (``.fifo`` suffix included) of queue and topic names."""
_VALID_NAME = re.compile(r"[A-Za-z0-9_-]+(?:\.fifo)?")
_VALID_PREFIX = re.compile(r"[A-Za-z0-9_-]*")
_VALID_BUCKET_PREFIX = re.compile(r"(?:[a-z0-9][a-z0-9.-]*)?")
_EVENTS_WAIT = 1.0
"""Seconds the event pump waits for events per call (stopping wakes it at once)."""
_PROVISION_CONCURRENCY = 8
"""Resources client.provision() resolves at once: enough to overlap the calls, few enough not to be throttled."""


def _register_fork_hook() -> None:
    global _FORK_HOOK_REGISTERED
    if not _FORK_HOOK_REGISTERED and hasattr(os, "register_at_fork"):
        os.register_at_fork(after_in_child=_core._reset_after_fork)
        _FORK_HOOK_REGISTERED = True


def _checked_prefix(argument: str, kind: Literal["queue", "topic"], prefix: str) -> str:
    """Validates ``queue_prefix``/``topic_prefix``: characters AWS allows in names, no ``.fifo``."""
    if ".fifo" in prefix:
        raise ConfigurationError(f"{argument} {prefix!r} must not contain '.fifo' (the suffix of FIFO {kind} names)")
    if not _VALID_PREFIX.fullmatch(prefix):
        raise ConfigurationError(
            f"{argument} {prefix!r} may only contain letters, digits, '-' and '_', "
            f"like the {kind} names it is prepended to"
        )
    return prefix


def _checked_bucket_prefix(prefix: str) -> str:
    """Validates ``bucket_prefix``: the start of an S3 bucket name."""
    if not _VALID_BUCKET_PREFIX.fullmatch(prefix) or ".." in prefix:
        raise ConfigurationError(
            f"bucket_prefix {prefix!r} may only contain lowercase letters, digits, '-' and '.' and must start "
            "with a letter or a digit, like the S3 bucket names it is prepended to"
        )
    return prefix


def _resource_name(kind: Literal["queue", "topic"], name: str, prefix: str = "") -> str:
    """The AWS name of a queue or topic, ``prefix + name``, checked against the AWS naming rules.

    The prefix is never guessed: ``name`` is taken as given, so a name that already starts with
    the prefix gets it twice.
    """
    if not name:
        raise ConfigurationError(f"{kind} name must not be empty")
    full = prefix + name
    described = f"{kind} name {full!r}" + (f" (prefix {prefix!r} + {name!r})" if prefix else "")
    if not _VALID_NAME.fullmatch(full):
        raise ConfigurationError(
            f"{described} may only contain letters, digits, '-' and '_' (FIFO {kind}s end in '.fifo')"
        )
    service, limit = _NAME_LIMITS[kind]
    if len(full) > limit:
        shorten = "the prefix or the name" if prefix else "the name"
        raise ConfigurationError(f"{described} is {len(full)} characters; {service} allows {limit} - shorten {shorten}")
    return full


def _conflicts(kind: str, name: str, declared: Mapping[str, Any], given: Mapping[str, Any]) -> None:
    """Raises unless every setting in ``given`` equals the one ``declared`` (redeclaring is allowed)."""
    differ = [option for option, value in given.items() if value != declared[option]]
    if differ:
        raise ConfigurationError(
            f"{kind} {name!r} is already declared with other settings ({', '.join(differ)}); configure it in one place"
        )


def _given(**options: Any) -> dict[str, Any]:
    """The options a call actually sets (``None`` and unset ones are not settings)."""
    return {option: value for option, value in options.items() if value is not None and value is not _UNSET}


class Client:
    """Entry point of fanferro: connection, resources, publishing and consumers.

    Connects to AWS (or MiniStack, or the in-memory broker), declares topics, queues and
    subscriptions, publishes, and runs consumers.

    Example::

        client = fanferro.Client(region="eu-west-1", source="urn:service:orders")
        orders = client.topic("orders", provisioning=fanferro.Provisioning.ensure())
        worker = client.queue("orders-worker", dead_letter=fanferro.DeadLetterQueue("orders-worker-dlq"))
        client.subscription(orders, worker)

        async with client:
            await orders.publish(OrderCreated(id=1))

    Args:
        region: AWS region (default: environment/profile).
        endpoint_url: Custom endpoint, e.g. ``"http://localhost:4566"`` for MiniStack.
        profile: Named AWS profile.
        credentials: Static credentials (default: the AWS provider chain).
        account_id: AWS account id (avoids an STS call when deriving topic ARNs).
        queue_prefix: Prepended as is to every queue name, dead-letter queues included, e.g.
            ``"staging-"`` when several environments share an AWS account (letters, digits, ``-``
            and ``_``). Queues are always declared and referred to without it, while
            ``queue.name``, events and metrics carry the prefixed name. Opt a queue out with
            ``prefixed=False``; a queue given by ``url`` is never prefixed.
        topic_prefix: Prepended as is to every topic name, like ``queue_prefix``; a topic given by
            ``arn`` is never prefixed.
        bucket_prefix: Prepended as is to the bucket of every :class:`~fanferro.LargePayloads`
            setting, the client's and those of topics and queues, e.g. ``"staging-"`` (lowercase
            letters, digits, ``-`` and ``.``, like S3 bucket names). Opt a bucket out with
            ``LargePayloads(prefixed=False)``. Consumers download payloads from the bucket a
            pointer names, so they need no prefix.
        source: CloudEvents ``source`` of published messages, e.g. ``"urn:service:orders"`` (only
            carried by CloudEvents envelopes).
        envelope: Envelope for publishing (default: :class:`~fanferro.envelopes.Raw`: the payload
            is the body, the message id and type travel in attributes). Pass
            ``fanferro.envelopes.CloudEvents()`` for self-describing CloudEvents.
        accept_envelopes: Envelopes consumers try, in order (default: CloudEvents JSON,
            CloudEvents protobuf, raw).
        codecs: Codec registry (default: protobuf, pydantic, msgspec, dataclass, bytes, text, JSON).
        naming: Fallback function naming message classes (default: ``cls.__name__``).
        large_payloads: Claim-check offloading of large messages to S3.
        payload_fetch: How consumers download offloaded payloads.
        telemetry: OpenTelemetry settings (no-op without ``opentelemetry-api``).
        logging: Event logging configuration.
        provisioning: Default provisioning policy of declared resources.
        aws_retry: Retries of individual AWS calls.
        timeouts: Timeouts of AWS calls.
        publish_retry_attempts: Attempts for failed batch entries and checksum mismatches.
        verify_md5: Verify SQS MD5 digests of sent and received messages (opt-in; TLS already
            protects messages in transit).
        split_batches_by_trace_header: Send the entries of a batch publish to a topic in separate
            ``PublishBatch`` calls per X-Ray trace header, so that each message keeps its
            ``X-Amzn-Trace-Id``: SNS takes one per call. Off by default: a call whose entries have
            different trace headers (prepared messages of an outbox relay) sends none, and trace
            context still travels in the ``traceparent`` attribute.
        publish_middleware: Middleware wrapping message preparation.
        consume_middleware: Middleware wrapping every handler of every consumer.
        broker: Use an in-memory broker instead of AWS (see :mod:`fanferro.testing`).
        event_buffer: Maximum buffered core events (excess events are dropped and reported).
        app_name: Application name added to the AWS user agent.
        auto_provision: Provision every declared resource when entering ``async with client``
            (fail fast on missing or misconfigured infrastructure).
    """

    def __init__(
        self,
        *,
        region: str | None = None,
        endpoint_url: str | None = None,
        profile: str | None = None,
        credentials: Credentials | None = None,
        account_id: str | None = None,
        queue_prefix: str = "",
        topic_prefix: str = "",
        bucket_prefix: str = "",
        source: str = "fanferro",
        envelope: Envelope | None = None,
        accept_envelopes: Sequence[Envelope] | None = None,
        codecs: CodecRegistry | Iterable[Codec] | None = None,
        naming: Callable[[type], str] | None = None,
        large_payloads: LargePayloads | None = None,
        payload_fetch: PayloadFetch | None = None,
        telemetry: Telemetry | None = None,
        logging: LoggingConfig | None = None,
        provisioning: Provisioning | None = None,
        aws_retry: AwsRetry | None = None,
        timeouts: Timeouts | None = None,
        publish_retry_attempts: int = 4,
        verify_md5: bool = False,
        split_batches_by_trace_header: bool = False,
        publish_middleware: Sequence[PublishMiddleware] = (),
        consume_middleware: Sequence[ConsumeMiddleware] = (),
        broker: MemoryBroker | None = None,
        event_buffer: int = 10_000,
        app_name: str | None = None,
        auto_provision: bool = True,
    ) -> None:
        self.queue_prefix = _checked_prefix("queue_prefix", "queue", queue_prefix)
        self.topic_prefix = _checked_prefix("topic_prefix", "topic", topic_prefix)
        self.bucket_prefix = _checked_bucket_prefix(bucket_prefix)
        if large_payloads is not None:
            large_payloads.bucket_name(self.bucket_prefix)  # fail fast on a name S3 rejects
        _register_fork_hook()
        self.region = region
        self.endpoint_url = endpoint_url
        self.profile = profile
        self.credentials = credentials
        self.account_id = account_id
        self.source = source
        self.envelope: Envelope = envelope or Raw()
        self.accept_envelopes: tuple[Envelope, ...] | None = as_tuple(accept_envelopes)
        if isinstance(codecs, CodecRegistry):
            self.codecs = codecs
        else:
            self.codecs = CodecRegistry(codecs) if codecs is not None else CodecRegistry()
        self.types = MessageTypes(self.codecs, naming)
        self.large_payloads = large_payloads
        self.payload_fetch = payload_fetch or PayloadFetch()
        self.provisioning = provisioning or Provisioning()
        self.aws_retry = aws_retry or AwsRetry()
        self.timeouts = timeouts or Timeouts()
        self.publish_retry_attempts = publish_retry_attempts
        self.verify_md5 = verify_md5
        self.split_batches_by_trace_header = split_batches_by_trace_header
        self.publish_middleware: list[PublishMiddleware] = list(publish_middleware)
        self.consume_middleware: list[ConsumeMiddleware] = list(consume_middleware)
        self.broker = broker
        self.event_buffer = event_buffer
        self.app_name = app_name
        self.auto_provision = auto_provision
        self.events = EventDispatcher(logging)
        self.events.on_change(self._update_event_filter)
        self._telemetry = TelemetryRuntime(telemetry, _core.__version__)
        self._core_client: CoreClient | None = None
        self._connect_lock: LoopLocal[asyncio.Lock] = LoopLocal(asyncio.Lock)
        self._queue_policy_locks: LoopLocal[dict[str, asyncio.Lock]] = LoopLocal(dict)
        """One lock per queue ARN, held while a subscription rewrites that queue's policy."""
        self._topics: dict[str, Topic] = {}
        self._queues: dict[str, Queue] = {}
        self._subscriptions: list[Subscription] = []
        self._consumers: list[Consumer] = []
        self._running_consumers: set[Consumer] = set()
        self._pump_users = 0
        self._pump_task: asyncio.Task[None] | None = None
        self._entered = False
        self._provisioned = False
        """Whether everything declared was provisioned once (later subscriptions resolve before publishing)."""

    # -- connection ------------------------------------------------------------------------

    def _client_config(self) -> dict[str, Any]:
        return {
            "offload": self.large_payloads.to_core(self.bucket_prefix) if self.large_payloads is not None else None,
            "fetch": self.payload_fetch.to_core(),
            "publish_retry": {"max_attempts": self.publish_retry_attempts, "initial_backoff": 0.1, "max_backoff": 5.0},
            "verify_md5": self.verify_md5,
            "split_batches_by_trace_header": self.split_batches_by_trace_header,
            "event_capacity": self.event_buffer,
        }

    def _aws_config(self) -> dict[str, Any]:
        return {
            "region": self.region,
            "endpoint_url": self.endpoint_url,
            "profile": self.profile,
            "credentials": self.credentials.to_core() if self.credentials is not None else None,
            "account_id": self.account_id,
            "app_name": self.app_name,
            "retry": self.aws_retry.to_core(),
            "timeouts": self.timeouts.to_core(),
        }

    async def _core(self) -> CoreClient:
        if self._core_client is not None:
            return self._core_client
        async with self._connect_lock.get():
            if self._core_client is None:
                if self.broker is not None:
                    core = CoreClient.memory(self.broker, self._client_config())
                else:
                    core = await CoreClient.connect(self._aws_config(), self._client_config())
                self._core_client = core
                self._update_event_filter()
                self._telemetry.watch_client(core.metrics)
        return self._core_client

    async def connect(self) -> None:
        """Connects to AWS eagerly (otherwise the first operation connects)."""
        await self._core()

    async def __aenter__(self) -> Self:
        await self._core()
        self._entered = True
        self._acquire_pump()
        if self.auto_provision:
            try:
                await self.provision()
            except BaseException:
                await self.close()
                raise
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    async def close(self) -> None:
        """Stops running consumers (concurrently), flushes events and waits for async hooks."""
        stops = await asyncio.gather(
            *(consumer.stop() for consumer in list(self._running_consumers)), return_exceptions=True
        )
        failure = next((result for result in stops if isinstance(result, BaseException)), None)
        if self._entered:
            self._entered = False
            self._release_pump()
        await self._stop_pump()
        self._drain_core_events()
        await self.events.flush()
        if failure is not None:
            raise failure

    # -- events ----------------------------------------------------------------------------

    @overload
    def on(self, selector: KindSelector, hook: EventHook) -> Callable[[], None]: ...
    @overload
    def on(self, selector: KindSelector) -> Callable[[EventHook], EventHook]: ...
    def on(self, selector: KindSelector, hook: EventHook | None = None) -> Any:
        """Registers an event hook (sync or async) for kinds or glob patterns like ``"message.*"``.

        Usable as a decorator (``@client.on("message.dead_lettered")``) or directly, in which case
        it returns an unsubscribe function.
        """
        if hook is not None:
            return self.events.subscribe(selector, hook)

        def decorate(function: EventHook) -> EventHook:
            self.events.subscribe(selector, function)
            return function

        return decorate

    def _update_event_filter(self) -> None:
        if self._core_client is not None:
            self._core_client.set_event_filter([kind.value for kind in self.events.enabled_kinds()])

    def _dispatch_core(self, raw_events: Iterable[Mapping[str, Any]]) -> None:
        for raw in raw_events:
            try:
                event = Event.from_core(raw)
            except (KeyError, ValueError):
                continue
            self.events.dispatch(event)

    def _drain_core_events(self) -> None:
        """Dispatches buffered core events (used when no event pump is running)."""
        core = self._core_client
        if core is None or (self._pump_task is not None and not self._pump_task.done()):
            return
        self._dispatch_core(core.drain_events())
        self._report_dropped(core)

    def _report_dropped(self, core: CoreClient) -> None:
        dropped = core.take_dropped_events()
        if dropped:
            self.events.emit(EventKind.EVENTS_DROPPED, f"{dropped} events were dropped (buffer full)", dropped=dropped)

    def _acquire_pump(self) -> None:
        self._pump_users += 1
        if self._pump_task is None or self._pump_task.done():
            self._pump_task = asyncio.get_running_loop().create_task(self._pump(), name="fanferro-events")

    def _release_pump(self) -> None:
        self._pump_users = max(0, self._pump_users - 1)
        if self._pump_users == 0 and self._core_client is not None:
            # The pump dispatches what its pending wait already took, then exits on its own
            # (cancelling it could drop events that wait had taken from the buffer).
            self._core_client.wake_events()

    async def _stop_pump(self) -> None:
        task = self._pump_task
        self._pump_task = None
        self._pump_users = 0
        if task is None:
            return
        if self._core_client is not None:
            self._core_client.wake_events()
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=5.0)
        except TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, RuntimeError):
                await task
        except RuntimeError:
            pass

    async def _pump(self) -> None:
        core = await self._core()
        current = asyncio.current_task()
        refreshed = monotonic()
        try:
            while self._pump_users > 0 and self._pump_task is current:
                events = await core.next_events(256, _EVENTS_WAIT)
                self._dispatch_core(events)
                self._report_dropped(core)
                if monotonic() - refreshed > 10:
                    refreshed = monotonic()
                    self._update_event_filter()
        finally:
            self._dispatch_core(core.drain_events())

    # -- resources -------------------------------------------------------------------------

    def topic(
        self,
        name: str | None = None,
        *,
        arn: str | None = None,
        prefixed: bool = True,
        attributes: TopicAttributes | Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        provisioning: Provisioning | None = None,
        envelope: Envelope | None = None,
        large_payloads: LargePayloads | Any | None = _UNSET,
        group_id: GroupId | None = None,
        codec: Codec | str | None = None,
    ) -> Topic:
        """Declares (or returns the already declared) SNS topic.

        Args:
            name: Topic name without the client's ``topic_prefix`` (``.fifo`` suffix for FIFO
                topics). Optional when ``arn`` is given.
            arn: Topic ARN (skips deriving it from the account id). The topic is then named as in
                the ARN (or by ``name``), without the prefix. A topic of another account or region
                is never created: provisioning fails while it does not exist.
            prefixed: Prepend the client's ``topic_prefix`` to ``name`` (``False`` for a topic
                shared by several environments).
            attributes: Desired attributes (:class:`~fanferro.TopicAttributes` or raw AWS names).
            tags: Desired tags.
            provisioning: Provisioning policy (default: the client's).
            envelope: Envelope for this topic (default: the client's).
            large_payloads: Claim-check settings for this topic (``None`` disables offloading).
            group_id: Default FIFO group id or a function computing it from the message.
            codec: Codec (or codec name) for this topic.
        """
        if arn is not None:
            # The ARN identifies the topic itself, so its name is used as is.
            name = _resource_name("topic", arn.rsplit(":", 1)[-1] if name is None else name)
        elif name is not None:
            name = _resource_name("topic", name, self.topic_prefix if prefixed else "")
        else:
            raise ConfigurationError("topic() needs a name or an arn")
        existing = self._topics.get(name)
        if existing is not None:
            given = _given(
                arn=arn,
                attributes=attributes_to_aws(attributes) if attributes is not None else None,
                tags=dict(tags) if tags is not None else None,
                provisioning=provisioning,
                envelope=envelope,
                group_id=group_id,
                codec=codec,
            )
            if large_payloads is not _UNSET:  # None (no offloading) is a setting too
                given["large_payloads"] = large_payloads
            _conflicts("topic", name, existing._settings(), given)
            return existing
        topic = Topic(
            self,
            name,
            arn=arn,
            attributes=attributes,
            tags=tags,
            provisioning=provisioning,
            envelope=envelope,
            large_payloads=large_payloads,
            group_id=group_id,
            codec=codec,
        )
        self._topics[name] = topic
        return topic

    def queue(
        self,
        name: str | None = None,
        *,
        url: str | None = None,
        owner_account_id: str | None = None,
        prefixed: bool = True,
        attributes: QueueAttributes | Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        dead_letter: DeadLetterQueue | None = None,
        provisioning: Provisioning | None = None,
        envelope: Envelope | None = None,
        large_payloads: LargePayloads | Any | None = _UNSET,
        group_id: GroupId | None = None,
        codec: Codec | str | None = None,
    ) -> Queue:
        """Declares (or returns the already declared) SQS queue.

        Args:
            name: Queue name without the client's ``queue_prefix`` (``.fifo`` suffix for FIFO
                queues). Optional when ``url`` is given.
            url: Queue URL (skips ``GetQueueUrl``). The queue is then named as in the URL (or by
                ``name``), without the prefix.
            owner_account_id: Owner account for cross-account queues.
            prefixed: Prepend the client's ``queue_prefix`` to ``name`` (``False`` for a queue
                shared by several environments).
            attributes: Desired attributes (:class:`~fanferro.QueueAttributes` or raw AWS names).
            tags: Desired tags.
            dead_letter: Dead-letter queue to provision and wire up with a redrive policy.
            provisioning: Provisioning policy (default: the client's).
            envelope: Envelope for messages sent to this queue (default: the client's).
            large_payloads: Claim-check settings for this queue (``None`` disables offloading).
            group_id: Default FIFO group id or a function computing it from the message.
            codec: Codec (or codec name) for this queue.
        """
        if url is not None:
            # The URL identifies the queue itself, so its name is used as is.
            name = _resource_name("queue", url.rstrip("/").rsplit("/", 1)[-1] if name is None else name)
        elif name is not None:
            name = _resource_name("queue", name, self.queue_prefix if prefixed else "")
        else:
            raise ConfigurationError("queue() needs a name or a url")
        existing = self._queues.get(name)
        if existing is not None:
            given = _given(
                url=url,
                owner_account_id=owner_account_id,
                attributes=attributes_to_aws(attributes) if attributes is not None else None,
                tags=dict(tags) if tags is not None else None,
                dead_letter=dead_letter,
                provisioning=provisioning,
                envelope=envelope,
                group_id=group_id,
                codec=codec,
            )
            if large_payloads is not _UNSET:  # None (no offloading) is a setting too
                given["large_payloads"] = large_payloads
            _conflicts("queue", name, existing._settings(), given)
            return existing
        queue = Queue(
            self,
            name,
            url=url,
            owner_account_id=owner_account_id,
            attributes=attributes,
            tags=tags,
            dead_letter=dead_letter,
            provisioning=provisioning,
            envelope=envelope,
            large_payloads=large_payloads,
            group_id=group_id,
            codec=codec,
        )
        self._queues[name] = queue
        return queue

    def subscription(
        self,
        topic: Topic | str,
        queue: Queue | str,
        *,
        raw_message_delivery: bool | None = None,
        filter_policy: Mapping[str, Any] | None = None,
        filter_policy_scope: Literal["MessageAttributes", "MessageBody"] | None = None,
        dead_letter_queue: Queue | str | None = None,
        manage_queue_policy: bool | None = None,
        provisioning: Provisioning | None = None,
    ) -> Subscription:
        """Declares (or returns the already declared) subscription of ``queue`` to ``topic``.

        Declaring it again returns the same subscription; settings that differ from the first
        declaration raise :class:`~fanferro.ConfigurationError`. A subscription declared after the
        client provisioned everything (``async with client`` or ``await client.provision()``) is
        resolved before the topic's next ``publish``, ``publish_batch`` or ``prepare``, so messages
        published after the declaration reach the queue.

        Args:
            topic: Topic handle, or a topic name (the client's ``topic_prefix`` is prepended).
            queue: Queue handle, or a queue name (the client's ``queue_prefix`` is prepended).
            raw_message_delivery: Deliver messages unwrapped (default ``True``, recommended; attributes
                become SQS message attributes). fanferro also understands wrapped SNS notifications.
            filter_policy: SNS filter policy, e.g. ``{"fanferro.type": ["orders.created"]}``.
            filter_policy_scope: ``"MessageAttributes"`` (default) or ``"MessageBody"``.
            dead_letter_queue: Queue (handle or name) receiving messages SNS fails to deliver.
            manage_queue_policy: Allow the topic to send to the queue (and the dead-letter queue;
                default ``True``).
            provisioning: Provisioning policy (default: the client's).
        """
        topic_handle = topic if isinstance(topic, Topic) else self.topic(topic)
        queue_handle = queue if isinstance(queue, Queue) else self.queue(queue)
        dead_letter_handle = (
            dead_letter_queue
            if isinstance(dead_letter_queue, Queue) or dead_letter_queue is None
            else self.queue(dead_letter_queue)
        )
        for existing in self._subscriptions:
            if existing.topic is topic_handle and existing.queue is queue_handle:
                given = _given(
                    raw_message_delivery=raw_message_delivery,
                    filter_policy=dict(filter_policy) if filter_policy is not None else None,
                    filter_policy_scope=filter_policy_scope,
                    dead_letter_queue=dead_letter_handle,
                    manage_queue_policy=manage_queue_policy,
                    provisioning=provisioning,
                )
                _conflicts("subscription", existing.name, existing._settings(), given)
                return existing
        subscription = Subscription(
            self,
            topic_handle,
            queue_handle,
            raw_message_delivery=True if raw_message_delivery is None else raw_message_delivery,
            filter_policy=filter_policy,
            filter_policy_scope=filter_policy_scope,
            dead_letter_queue=dead_letter_handle,
            manage_queue_policy=True if manage_queue_policy is None else manage_queue_policy,
            provisioning=provisioning,
        )
        self._subscriptions.append(subscription)
        if self._provisioned:
            # Provisioning already ran: without this, messages published to the topic before a
            # consumer of the queue starts would reach no queue and be lost.
            topic_handle._pending_subscriptions.append(subscription)
        return subscription

    def _declared_queue(self, name: str, *, prefixed: bool = True) -> Queue | None:
        """The queue already declared under ``name`` (as :meth:`queue` would name it), if any."""
        return self._queues.get(_resource_name("queue", name, self.queue_prefix if prefixed else ""))

    @property
    def topics(self) -> dict[str, Topic]:
        """Declared topics by AWS name (with the ``topic_prefix``)."""
        return dict(self._topics)

    @property
    def queues(self) -> dict[str, Queue]:
        """Declared queues by AWS name (with the ``queue_prefix``)."""
        return dict(self._queues)

    @property
    def subscriptions(self) -> list[Subscription]:
        """Declared subscriptions."""
        return list(self._subscriptions)

    async def provision(self, *resources: Topic | Queue | Subscription) -> list[ProvisioningReport | BaseException]:
        """Resolves resources according to their provisioning policies.

        Without arguments, provisions everything declared on the client (topics, queues with their
        dead-letter queues, subscriptions, and the large payload buckets whose settings set
        ``create_bucket``: the client's and those of topics and queues). Given resources, it
        provisions those and the buckets of the given topics and queues. Errors are raised according
        to each resource's failure modes; resources configured to only log failures are reported as
        exceptions in the returned list.

        Buckets, topics and queues are resolved concurrently (up to 8 at a time), then the
        subscriptions, those of the same queue one after another. When a resource raises, the
        others finish first, and the error of the first in declaration order is raised.
        """
        if not resources:
            self._provisioned = True
        targets: list[Topic | Queue | Subscription] = list(resources) or [
            *self._topics.values(),
            *self._queues.values(),
            *self._subscriptions,
        ]
        settings = [] if resources else [self.large_payloads]
        settings += [target.large_payloads for target in targets if isinstance(target, Topic | Queue)]
        # Each bucket once, in declaration order (several resources may share one).
        buckets = dict.fromkeys(
            s.bucket_name(self.bucket_prefix) for s in settings if s is not None and s.create_bucket
        )
        limit = asyncio.Semaphore(_PROVISION_CONCURRENCY)

        async def bucket(name: str) -> BaseException | None:
            async with limit:
                try:
                    core = await self._core()
                    report = ProvisioningReport.from_core(await core.ensure_bucket(name, True))
                    await self._settle_report(report, Provisioning.create_missing())
                except Exception as error:  # noqa: BLE001 - raised below, once the others have finished
                    return error
            return None

        async def resolve(resource: Topic | Queue | Subscription) -> ProvisioningReport | BaseException:
            async with limit:
                try:
                    info = await resource._resolve(force=True)
                except Exception as error:  # noqa: BLE001 - raised below, once the others have finished
                    return error
            report = resource.last_report
            return (
                report
                if report is not None
                else ProvisioningReport(kind=resource.kind, name=resource.name, status="found", resource=info)
            )

        async def in_turn(group: list[Subscription]) -> list[ProvisioningReport | BaseException]:
            return [await resolve(subscription) for subscription in group]

        queues = [target for target in targets if isinstance(target, Queue)]
        # The dead-letter queue of a queue provisioned here comes with that queue's report (its
        # ensure_queue provisions it first): looking it up on its own again would repeat the calls.
        adopted = {
            id(queue._dead_letter_queue)
            for queue in queues
            if queue.dead_letter is not None and queue._dead_letter_queue is not None
        }
        first = [target for target in targets if not isinstance(target, Subscription) and id(target) not in adopted]
        outcomes: dict[int, ProvisioningReport | BaseException] = {}
        for queue in queues:
            queue._adopted = None
        try:
            # Buckets, topics and queues at once (at most _PROVISION_CONCURRENCY of them).
            failed, resolved = await asyncio.gather(
                asyncio.gather(*(bucket(name) for name in buckets)), asyncio.gather(*map(resolve, first))
            )
            outcomes.update(zip(map(id, first), resolved, strict=True))
            for queue in queues:
                if id(queue) in adopted:
                    # None when its queue failed before provisioning it: then it is resolved alone.
                    outcome = queue._adopted
                    outcomes[id(queue)] = outcome if outcome is not None else await resolve(queue)
        finally:
            for queue in queues:
                queue._adopted = None
        _raise_first([*failed, *(outcomes[id(target)] for target in targets if id(target) in outcomes)])
        # Subscriptions after the topics and queues they need. Those sharing a queue would wait for
        # each other's policy writes anyway (_queue_policy_turn), so they run one after another in
        # declaration order, without taking the slots of subscriptions of other queues meanwhile.
        groups = _sharing_queues([target for target in targets if isinstance(target, Subscription)])
        for group, results in zip(groups, await asyncio.gather(*map(in_turn, groups)), strict=True):
            outcomes.update(zip(map(id, group), results, strict=True))
        self._drain_core_events()
        ordered = [outcomes[id(target)] for target in targets]
        _raise_first(ordered)
        return ordered

    async def _provision_subscriptions_for(self, queue: Queue) -> None:
        for subscription in self._subscriptions:
            if subscription.queue is queue:
                with contextlib.suppress(ResourceUnavailableError):
                    await subscription.resolve()

    @contextlib.asynccontextmanager
    async def _queue_policy_turn(self, *queue_arns: str | None) -> AsyncIterator[None]:
        """Holds the locks of the given queues, so one subscription at a time rewrites each queue's policy.

        Subscriptions write the ``Policy`` of their queue (and of their dead-letter queue)
        read-modify-write: two at once could drop the statement the other has just added, and SNS
        would then silently drop that topic's messages. The locks are taken in a fixed order.
        """
        locks = self._queue_policy_locks.get()
        async with contextlib.AsyncExitStack() as stack:
            for arn in sorted({arn for arn in queue_arns if arn}):
                await stack.enter_async_context(locks.setdefault(arn, asyncio.Lock()))
            yield

    async def _ensure(
        self, kind: Literal["queue", "topic", "subscription"], spec: Mapping[str, Any]
    ) -> ProvisioningReport:
        core = await self._core()
        if kind == "queue":
            raw = await core.ensure_queue(spec)
        elif kind == "topic":
            raw = await core.ensure_topic(spec)
        else:
            raw = await core.ensure_subscription(spec)
        self._drain_core_events()
        return ProvisioningReport.from_core(raw)

    async def _settle_report(self, report: ProvisioningReport, policy: Provisioning) -> Any:
        """Applies the policy's failure modes to a provisioning report; returns the resource info."""
        if report.status == "failed":
            mode, reason, kind = policy.on_error, "error", EventKind.RESOURCE_FAILED
        elif report.status == "missing":
            mode, reason, kind = policy.on_missing, "missing", EventKind.RESOURCE_MISSING
        elif report.drift:
            mode, reason, kind = policy.on_drift, "drift", EventKind.RESOURCE_DRIFT
        else:
            return report.resource
        description = self._describe_failure(report, reason)
        if mode != "ignore":
            self.events.emit(
                kind,
                description,
                exception=report.error,
                resource=report.kind,
                name=report.name,
                status=report.status,
                drift=[f"{d.attribute}: expected {d.expected!r}, actual {d.actual!r}" for d in report.drift] or None,
            )
        if mode == "raise":
            raise ProvisioningError(description, report)
        if isinstance(mode, Fallback):
            replacement = await maybe_await(mode.handler(ProvisioningFailure(reason=reason, report=report)))  # type: ignore[arg-type]
            self.events.emit(
                EventKind.RESOURCE_FALLBACK,
                f"fallback used for {report.kind} {report.name}",
                resource=report.kind,
                name=report.name,
                reason=reason,
                replacement=getattr(replacement, "name", None),
            )
            if replacement is not None:
                return replacement
        return report.resource if reason == "drift" else None

    @staticmethod
    def _describe_failure(report: ProvisioningReport, reason: str) -> str:
        if reason == "missing":
            return f"{report.kind} {report.name} does not exist (enable Provisioning(create=True) to create it)"
        if reason == "drift":
            details = ", ".join(d.attribute for d in report.drift)
            return (
                f"{report.kind} {report.name} differs from its spec ({details}); "
                "enable Provisioning(update=True) to fix it"
            )
        return f"provisioning {report.kind} {report.name} failed: {report.error}"

    # -- consumers -------------------------------------------------------------------------

    def consumer(self, queue: Queue | str, handler: Handler | Router | None = None, /, **options: Any) -> Consumer:
        """Creates a consumer for ``queue``.

        Args:
            queue: Queue handle, or a queue name (the client's ``queue_prefix`` is prepended).
            handler: A handler (receives every message), a :class:`~fanferro.Router`, or ``None``
                to add routes later with ``@consumer.route(...)``.
            **options: :class:`~fanferro.Consumer` options: ``concurrency``, ``prefetch``,
                ``batch_size``, ``min_receive_batch``, ``receive_batch_wait``, ``wait_time``,
                ``pollers``, ``max_pollers``, ``rate_limit``, ``visibility_timeout``, ``heartbeat``,
                ``retry``, ``dead_letter``, ``shutdown``, ``handler_timeout``, ``ordering``,
                ``strict_ordering``, ``envelopes``, ``middleware``, ``auto_ack``, ``settle_linger``,
                ``sns_unwrap``, ``auto_confirm_subscriptions``, ``delete_offloaded_payloads``,
                ``max_receive_failures``, ``name``.
        """
        from fanferro.consumer import Consumer

        queue_handle = queue if isinstance(queue, Queue) else self.queue(queue)
        if isinstance(handler, Router):
            router = handler
        else:
            router = Router()
            if handler is not None:
                router.add(handler, fallback=True)
        consumer = Consumer(self, queue_handle, router, **options)
        self._consumers.append(consumer)
        return consumer

    @property
    def consumers(self) -> list[Consumer]:
        """Consumers created by this client."""
        return list(self._consumers)

    def _register_consumer(self, consumer: Consumer) -> None:
        self._running_consumers.add(consumer)
        self._telemetry.watch_consumer(consumer)

    def _unregister_consumer(self, consumer: Consumer) -> None:
        self._running_consumers.discard(consumer)

    async def run(self, *consumers: Consumer, install_signal_handlers: bool = True) -> None:
        """Runs consumers until SIGINT/SIGTERM or :meth:`stop`, then shuts them down gracefully.

        Runs every consumer created by this client unless consumers are given. A second signal (or
        a second :meth:`stop`) skips the drain: handlers are cancelled and long polls aborted. A
        consumer that stops by itself because its receives keep failing (``max_receive_failures``)
        stops the others too, and ``run()`` raises its receive error.
        """
        targets = list(consumers) or list(self._consumers)
        if not targets:
            raise ConfigurationError("no consumers to run")
        stop = asyncio.Event()
        force = False
        started: list[Consumer] = []

        def request_stop() -> None:
            nonlocal force
            if stop.is_set():
                force = True
                for consumer in started:
                    consumer._hurry_stop(0, True)
            stop.set()

        self._stop_request = request_stop
        self._run_targets = targets
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        if install_signal_handlers:
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, request_stop)
                    installed.append(sig)
                except (NotImplementedError, RuntimeError, ValueError):
                    break
        try:
            for consumer in targets:
                await consumer.start()
                started.append(consumer)
            await stop.wait()
        finally:
            # The handlers stay installed while consumers drain: a second signal cuts it short.
            try:
                await asyncio.gather(
                    *(
                        consumer.stop(drain_timeout=0, abort_receives=True) if force else consumer.stop()
                        for consumer in started
                    ),
                    return_exceptions=True,
                )
            finally:
                for sig in installed:
                    loop.remove_signal_handler(sig)
                self._stop_request = None
                self._run_targets = []
        for consumer in started:
            if consumer._failure is not None:
                raise consumer._failure

    _stop_request: Callable[[], None] | None = None
    _run_targets: list[Consumer] = []  # noqa: RUF012 - replaced, never mutated

    def _consumer_failed(self, consumer: Consumer) -> None:
        """A consumer stopped by itself (``max_receive_failures``): :meth:`run` stops the others."""
        if self._stop_request is not None and consumer in self._run_targets:
            self._stop_request()

    def stop(self) -> None:
        """Asks :meth:`run` to shut down."""
        if self._stop_request is not None:
            self._stop_request()

    # -- outbox ----------------------------------------------------------------------------

    async def publish_prepared(self, prepared: PreparedMessage) -> PublishResult:
        """Sends a message produced by ``topic.prepare(...)``/``queue.prepare(...)`` (e.g. from an outbox)."""
        core = await self._core()
        transport = await self._transport_for_destination(prepared)
        started = monotonic()
        kind = prepared.destination.get("kind", "queue")
        with self._telemetry.publish_span(prepared.destination_name, kind, prepared.message_id) as span:
            try:
                receipt = await core.publish(
                    prepared.destination,
                    prepared.wire,
                    prepared.message_id,
                    group_id=prepared.group_id,
                    deduplication_id=prepared.deduplication_id,
                    delay=prepared.delay,
                    subject=prepared.sns_subject,
                    trace_header=prepared.trace_header,
                    transport=transport,
                )
            except Exception as error:
                self._telemetry.mark_error(span, error)
                self._telemetry.record_publish(prepared.destination_name, kind, monotonic() - started, 1, error)
                self.events.emit(
                    EventKind.PUBLISH_FAILED,
                    f"failed to publish prepared message to {prepared.destination_name}",
                    exception=error,
                    destination=prepared.destination_name,
                    message_id=prepared.message_id,
                )
                self._drain_core_events()
                raise
        self._telemetry.record_publish(prepared.destination_name, kind, monotonic() - started, 1, None)
        result = PublishResult.from_core(receipt, prepared.destination_name)
        if self.events.is_enabled(EventKind.PUBLISH_SUCCEEDED):
            self.events.emit(
                EventKind.PUBLISH_SUCCEEDED,
                f"published prepared message to {prepared.destination_name}",
                destination=prepared.destination_name,
                message_id=result.message_id,
                transport_message_id=result.transport_message_id,
            )
        self._drain_core_events()
        return result

    async def publish_prepared_batch(
        self, messages: Sequence[PreparedMessage], *, raise_on_failure: bool = False
    ) -> BatchPublishResult:
        """Sends prepared messages, batching those with the same destination.

        Like ``publish_batch``, each destination's batch emits ``publish.succeeded`` (with the
        count) and ``publish.failed`` per failed entry, and records the publish metrics.
        """
        core = await self._core()
        results: list[PublishResult | BatchFailure | None] = [None] * len(messages)
        groups: dict[str, list[int]] = {}
        for index, prepared in enumerate(messages):
            key = repr(sorted(prepared.destination.items()))
            groups.setdefault(key, []).append(index)
        for indexes in groups.values():
            first = messages[indexes[0]]
            name = first.destination_name
            kind = first.destination.get("kind", "queue")
            started = monotonic()
            with self._telemetry.publish_span(name, kind, None, batch_size=len(indexes)) as span:
                try:
                    transport = await self._transport_for_destination(first)
                    receipts = await core.publish_batch(
                        first.destination, [messages[i].core_entry() for i in indexes], transport=transport
                    )
                except Exception as error:  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
                    self._telemetry.mark_error(span, error)
                    receipts = [error] * len(indexes)
            duration = monotonic() - started
            succeeded = 0
            for index, receipt in zip(indexes, receipts, strict=True):
                if isinstance(receipt, BaseException):
                    results[index] = BatchFailure(index, messages[index], receipt)
                    self._telemetry.record_publish(name, kind, duration, 1, receipt)
                    self.events.emit(
                        EventKind.PUBLISH_FAILED,
                        f"failed to publish prepared batch entry {index} to {kind} {name}",
                        exception=receipt,
                        destination=name,
                        kind=kind,
                        index=index,
                        message_id=messages[index].message_id,
                    )
                else:
                    results[index] = PublishResult.from_core(receipt, name)
                    succeeded += 1
            if succeeded:
                self._telemetry.record_publish(name, kind, duration, succeeded, None)
                if self.events.is_enabled(EventKind.PUBLISH_SUCCEEDED):
                    self.events.emit(
                        EventKind.PUBLISH_SUCCEEDED,
                        f"published {succeeded} prepared messages to {kind} {name}",
                        destination=name,
                        kind=kind,
                        count=succeeded,
                        duration=round(duration, 6),
                    )
        batch = BatchPublishResult(tuple(r for r in results if r is not None))
        self._drain_core_events()
        if raise_on_failure:
            batch.raise_for_failures()
        return batch

    async def _transport_for_destination(self, prepared: PreparedMessage) -> Any:
        # A topic and a queue may share a name (``orders`` and its subscriber queue ``orders``),
        # with different large payload settings: the destination's kind picks the right one.
        resources = self._topics if prepared.destination.get("kind", "queue") == "topic" else self._queues
        resource = resources.get(prepared.destination_name)
        if resource is None:
            return None
        return await resource._transport_for()

    # -- introspection ---------------------------------------------------------------------

    def metrics(self) -> ClientMetrics:
        """On-demand metrics: client counters (from Rust) and per-consumer statistics.

        ``client.metrics().client.messages_published``, ``client.metrics().consumers["orders"].processed``;
        ``as_dict()`` gives the plain dictionary.
        """
        return ClientMetrics(
            client=ClientCounters._from_core(self._core_client.metrics() if self._core_client is not None else None),
            consumers={consumer.name: consumer.stats() for consumer in self._consumers},
        )

    def health(self) -> ClientHealth:
        """Liveness and readiness of every consumer this client created, for health checks.

        ``client.health().healthy()`` (liveness), ``ready()`` (readiness) and ``problems()`` (what
        is wrong); see :class:`~fanferro.stats.ClientHealth`. Cheap: no AWS call; safe to call from
        another thread.
        """
        return ClientHealth(consumers={consumer.name: consumer.health() for consumer in self._consumers})

    async def account(self) -> str:
        """The AWS account id of the configured credentials."""
        core = await self._core()
        return await core.account_id()

    def __repr__(self) -> str:
        backend = "memory" if self.broker is not None else (self.endpoint_url or "aws")
        return f"Client(backend={backend!r}, region={self.region!r})"


def _raise_first(outcomes: Iterable[ProvisioningReport | BaseException | None]) -> None:
    """Raises the first error that the failure modes do not let pass (in declaration order)."""
    for outcome in outcomes:
        if isinstance(outcome, BaseException) and not isinstance(outcome, ResourceUnavailableError):
            raise outcome


def _sharing_queues(subscriptions: list[Subscription]) -> list[list[Subscription]]:
    """Groups subscriptions (in declaration order) that touch the policy of a common queue."""
    groups: dict[int, list[Subscription]] = {}  # by the id of the group's first subscription
    group_of: dict[int, int] = {}  # queue id -> group key
    for subscription in subscriptions:
        queues = [id(q) for q in (subscription.queue, subscription.dead_letter_queue) if q is not None]
        keys = sorted({group_of[queue] for queue in queues if queue in group_of}, key=list(groups).index)
        key = keys[0] if keys else id(subscription)
        group = groups.setdefault(key, [])
        for other in keys[1:]:
            group.extend(groups.pop(other))
            group_of.update({queue: key for queue, owner in group_of.items() if owner == other})
        group.append(subscription)
        group_of.update(dict.fromkeys(queues, key))
    order = {id(subscription): index for index, subscription in enumerate(subscriptions)}
    return [sorted(group, key=lambda s: order[id(s)]) for group in groups.values()]
