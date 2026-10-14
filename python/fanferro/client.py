"""The :class:`Client`: connection, resources, consumers, events and telemetry."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from typing import TYPE_CHECKING, Any, Literal, Self, overload

from fanferro import _core
from fanferro._core import CoreClient, MemoryBroker
from fanferro._utils import LoopLocal, maybe_await
from fanferro.codecs import Codec, CodecRegistry
from fanferro.config import AwsRetry, Credentials, Timeouts
from fanferro.envelopes import CloudEvents, Envelope, as_tuple
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
)
from fanferro.resources import _UNSET, GroupId, Queue, Subscription, Topic
from fanferro.routing import Router
from fanferro.telemetry import Telemetry, TelemetryRuntime, monotonic

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from fanferro.consumer import Consumer
    from fanferro.middleware import ConsumeMiddleware, PublishMiddleware
    from fanferro.routing import Handler

__all__ = ["Client"]

_FORK_HOOK_REGISTERED = False


def _register_fork_hook() -> None:
    global _FORK_HOOK_REGISTERED
    if not _FORK_HOOK_REGISTERED and hasattr(os, "register_at_fork"):
        os.register_at_fork(after_in_child=_core._reset_after_fork)
        _FORK_HOOK_REGISTERED = True


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
        source: CloudEvents ``source`` of published messages, e.g. ``"urn:service:orders"``.
        envelope: Envelope for publishing (default: CloudEvents JSON).
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
        verify_md5: Verify SQS MD5 digests of sent and received messages.
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
        verify_md5: bool = True,
        publish_middleware: Sequence[PublishMiddleware] = (),
        consume_middleware: Sequence[ConsumeMiddleware] = (),
        broker: MemoryBroker | None = None,
        event_buffer: int = 10_000,
        app_name: str | None = None,
        auto_provision: bool = True,
    ) -> None:
        _register_fork_hook()
        self.region = region
        self.endpoint_url = endpoint_url
        self.profile = profile
        self.credentials = credentials
        self.account_id = account_id
        self.source = source
        self.envelope: Envelope = envelope or CloudEvents(default_source=source)
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
        self._topics: dict[str, Topic] = {}
        self._queues: dict[str, Queue] = {}
        self._subscriptions: list[Subscription] = []
        self._consumers: list[Consumer] = []
        self._running_consumers: set[Consumer] = set()
        self._pump_users = 0
        self._pump_task: asyncio.Task[None] | None = None
        self._entered = False

    # -- connection ------------------------------------------------------------------------

    def _client_config(self) -> dict[str, Any]:
        return {
            "offload": self.large_payloads.to_core() if self.large_payloads is not None else None,
            "fetch": self.payload_fetch.to_core(),
            "publish_retry": {"max_attempts": self.publish_retry_attempts, "initial_backoff": 0.1, "max_backoff": 5.0},
            "verify_md5": self.verify_md5,
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
        """Stops running consumers, flushes events and waits for async hooks."""
        for consumer in list(self._running_consumers):
            await consumer.stop()
        if self._entered:
            self._entered = False
            self._release_pump()
        await self._stop_pump()
        self._drain_core_events()
        await self.events.flush()

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
                events = await core.next_events(256, 1.0)
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
            name: Topic name (``.fifo`` suffix for FIFO topics). Optional when ``arn`` is given.
            arn: Topic ARN (skips deriving it from the account id).
            attributes: Desired attributes (:class:`~fanferro.TopicAttributes` or raw AWS names).
            tags: Desired tags.
            provisioning: Provisioning policy (default: the client's).
            envelope: Envelope for this topic (default: the client's).
            large_payloads: Claim-check settings for this topic (``None`` disables offloading).
            group_id: Default FIFO group id or a function computing it from the message.
            codec: Codec (or codec name) for this topic.
        """
        if name is None:
            if arn is None:
                raise ConfigurationError("topic() needs a name or an arn")
            name = arn.rsplit(":", 1)[-1]
        existing = self._topics.get(name)
        if existing is not None:
            if any(
                value not in (None, _UNSET)
                for value in (arn, attributes, tags, provisioning, envelope, group_id, codec)
            ) or (large_payloads is not _UNSET):
                raise ConfigurationError(f"topic {name!r} is already declared; configure it in one place")
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
            name: Queue name (``.fifo`` suffix for FIFO queues). Optional when ``url`` is given.
            url: Queue URL (skips ``GetQueueUrl``).
            owner_account_id: Owner account for cross-account queues.
            attributes: Desired attributes (:class:`~fanferro.QueueAttributes` or raw AWS names).
            tags: Desired tags.
            dead_letter: Dead-letter queue to provision and wire up with a redrive policy.
            provisioning: Provisioning policy (default: the client's).
            envelope: Envelope for messages sent to this queue (default: the client's).
            large_payloads: Claim-check settings for this queue (``None`` disables offloading).
            group_id: Default FIFO group id or a function computing it from the message.
            codec: Codec (or codec name) for this queue.
        """
        if name is None:
            if url is None:
                raise ConfigurationError("queue() needs a name or a url")
            name = url.rstrip("/").rsplit("/", 1)[-1]
        existing = self._queues.get(name)
        if existing is not None:
            configured = (url, owner_account_id, attributes, tags, dead_letter, provisioning, envelope, group_id, codec)
            if any(value is not None for value in configured) or large_payloads is not _UNSET:
                raise ConfigurationError(f"queue {name!r} is already declared; configure it in one place")
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
        raw_message_delivery: bool = True,
        filter_policy: Mapping[str, Any] | None = None,
        filter_policy_scope: Literal["MessageAttributes", "MessageBody"] | None = None,
        dead_letter_queue: Queue | str | None = None,
        manage_queue_policy: bool = True,
        provisioning: Provisioning | None = None,
    ) -> Subscription:
        """Declares a subscription of ``queue`` to ``topic`` (SNS fan-out to SQS).

        Args:
            topic: Topic handle or name.
            queue: Queue handle or name.
            raw_message_delivery: Deliver messages unwrapped (recommended; attributes become SQS
                message attributes). fanferro also understands wrapped SNS notifications.
            filter_policy: SNS filter policy, e.g. ``{"fanferro.type": ["orders.created"]}``.
            filter_policy_scope: ``"MessageAttributes"`` (default) or ``"MessageBody"``.
            dead_letter_queue: Queue receiving messages SNS fails to deliver.
            manage_queue_policy: Allow the topic to send to the queue (and the dead-letter queue).
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
                return existing
        subscription = Subscription(
            self,
            topic_handle,
            queue_handle,
            raw_message_delivery=raw_message_delivery,
            filter_policy=filter_policy,
            filter_policy_scope=filter_policy_scope,
            dead_letter_queue=dead_letter_handle,
            manage_queue_policy=manage_queue_policy,
            provisioning=provisioning,
        )
        self._subscriptions.append(subscription)
        return subscription

    @property
    def topics(self) -> dict[str, Topic]:
        """Declared topics by name."""
        return dict(self._topics)

    @property
    def queues(self) -> dict[str, Queue]:
        """Declared queues by name."""
        return dict(self._queues)

    @property
    def subscriptions(self) -> list[Subscription]:
        """Declared subscriptions."""
        return list(self._subscriptions)

    async def provision(self, *resources: Topic | Queue | Subscription) -> list[ProvisioningReport | BaseException]:
        """Resolves resources according to their provisioning policies.

        Without arguments, provisions everything declared on the client (topics, queues with their
        dead-letter queues, subscriptions, and the large payload bucket when ``create_bucket`` is
        set). Errors are raised according to each resource's failure modes; resources configured
        to only log failures are reported as exceptions in the returned list.
        """
        targets: list[Topic | Queue | Subscription] = list(resources) or [
            *self._topics.values(),
            *self._queues.values(),
            *self._subscriptions,
        ]
        if not resources and self.large_payloads is not None and self.large_payloads.create_bucket:
            core = await self._core()
            report = ProvisioningReport.from_core(await core.ensure_bucket(self.large_payloads.bucket, True))
            await self._settle_report(report, Provisioning.create_missing())
        outcomes: list[ProvisioningReport | BaseException] = []
        for resource in targets:
            try:
                info = await resource._resolve(force=True)
            except ResourceUnavailableError as error:
                outcomes.append(error)
                continue
            report = resource.last_report
            outcomes.append(
                report
                if report is not None
                else ProvisioningReport(kind=resource.kind, name=resource.name, status="found", resource=info)
            )
        self._drain_core_events()
        return outcomes

    async def _provision_subscriptions_for(self, queue: Queue) -> None:
        for subscription in self._subscriptions:
            if subscription.queue is queue:
                with contextlib.suppress(ResourceUnavailableError):
                    await subscription.resolve()

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
            queue: Queue handle or name.
            handler: A handler (receives every message), a :class:`~fanferro.Router`, or ``None``
                to add routes later with ``@consumer.route(...)``.
            **options: :class:`~fanferro.Consumer` options: ``concurrency``, ``prefetch``,
                ``batch_size``, ``wait_time``, ``pollers``, ``rate_limit``, ``visibility_timeout``,
                ``heartbeat``, ``retry``, ``dead_letter``, ``shutdown``, ``handler_timeout``,
                ``ordering``, ``envelopes``, ``middleware``, ``auto_ack``, ``settle_linger``,
                ``sns_unwrap``, ``auto_confirm_subscriptions``, ``delete_offloaded_payloads``,
                ``name``.
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

        Runs every consumer created by this client unless consumers are given. A second signal
        skips the drain.
        """
        targets = list(consumers) or list(self._consumers)
        if not targets:
            raise ConfigurationError("no consumers to run")
        stop = asyncio.Event()
        force = False

        def request_stop() -> None:
            nonlocal force
            if stop.is_set():
                force = True
            stop.set()

        self._stop_request = request_stop
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        if install_signal_handlers:
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, request_stop)
                    installed.append(sig)
                except (NotImplementedError, RuntimeError, ValueError):
                    break
        started: list[Consumer] = []
        try:
            for consumer in targets:
                await consumer.start()
                started.append(consumer)
            await stop.wait()
        finally:
            for sig in installed:
                loop.remove_signal_handler(sig)
            await asyncio.gather(
                *(
                    consumer.stop(drain_timeout=0, abort_receives=True) if force else consumer.stop()
                    for consumer in started
                ),
                return_exceptions=True,
            )
            self._stop_request = None

    _stop_request: Callable[[], None] | None = None

    def stop(self) -> None:
        """Asks :meth:`run` to shut down."""
        if self._stop_request is not None:
            self._stop_request()

    # -- outbox ----------------------------------------------------------------------------

    async def publish_prepared(self, prepared: PreparedMessage) -> PublishResult:
        """Sends a message produced by ``topic.prepare(...)``/``queue.prepare(...)`` (e.g. from an outbox)."""
        core = await self._core()
        transport = await self._transport_for_destination(prepared.destination_name)
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
        """Sends prepared messages, batching those with the same destination."""
        core = await self._core()
        results: list[PublishResult | BatchFailure | None] = [None] * len(messages)
        groups: dict[str, list[int]] = {}
        for index, prepared in enumerate(messages):
            key = repr(sorted(prepared.destination.items()))
            groups.setdefault(key, []).append(index)
        for indexes in groups.values():
            first = messages[indexes[0]]
            transport = await self._transport_for_destination(first.destination_name)
            try:
                receipts = await core.publish_batch(
                    first.destination, [messages[i].core_entry() for i in indexes], transport=transport
                )
            except Exception as error:  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
                receipts = [error] * len(indexes)
            for index, receipt in zip(indexes, receipts, strict=True):
                if isinstance(receipt, BaseException):
                    results[index] = BatchFailure(index, messages[index], receipt)
                else:
                    results[index] = PublishResult.from_core(receipt, messages[index].destination_name)
        batch = BatchPublishResult(tuple(r for r in results if r is not None))
        self._drain_core_events()
        if raise_on_failure:
            batch.raise_for_failures()
        return batch

    async def _transport_for_destination(self, name: str) -> Any:
        resource = self._topics.get(name) or self._queues.get(name)
        if resource is None:
            return None
        return await resource._transport_for()

    # -- introspection ---------------------------------------------------------------------

    def metrics(self) -> dict[str, Any]:
        """On-demand metrics: client counters (from Rust) and per-consumer statistics."""
        client_metrics = self._core_client.metrics() if self._core_client is not None else {}
        return {
            "client": client_metrics,
            "consumers": {consumer.name: consumer.stats() for consumer in self._consumers},
        }

    async def account(self) -> str:
        """The AWS account id of the configured credentials."""
        core = await self._core()
        return await core.account_id()

    def __repr__(self) -> str:
        backend = "memory" if self.broker is not None else (self.endpoint_url or "aws")
        return f"Client(backend={backend!r}, region={self.region!r})"
