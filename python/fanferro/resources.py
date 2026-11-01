"""Handles to topics, queues and subscriptions.

Handles are declared on a :class:`fanferro.Client` (``client.topic(...)``, ``client.queue(...)``,
``client.subscription(...)``) and resolved lazily according to their provisioning policy - on
first use, or up front with ``await client.provision()``.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any, Literal

from fanferro._core import AttributeValue, Payload, Transport, is_sqs_safe_text, validate_wire
from fanferro._utils import LoopLocal, optional_seconds
from fanferro.errors import ConfigurationError, ResourceUnavailableError, ValidationError
from fanferro.events import EventKind
from fanferro.messages import (
    BatchFailure,
    BatchPublishResult,
    OutgoingMessage,
    PreparedMessage,
    PublishResult,
    to_attribute_value,
)
from fanferro.middleware import PublishContext, chain_publish
from fanferro.provisioning import (
    DEFAULT_DEAD_LETTER_ATTRIBUTES,
    DeadLetterQueue,
    Provisioning,
    ProvisioningReport,
    QueueAttributes,
    QueueInfo,
    SubscriptionInfo,
    TopicAttributes,
    TopicInfo,
    attributes_to_aws,
)
from fanferro.telemetry import monotonic

if TYPE_CHECKING:
    from fanferro.client import Client
    from fanferro.codecs import Codec
    from fanferro.consumer import Consumer
    from fanferro.envelopes import Envelope
    from fanferro.payloads import LargePayloads
    from fanferro.routing import Handler, Router

__all__ = ["Queue", "Subscription", "Topic"]

_UNSET: Any = object()
_RETRY_AFTER_FAILURE = (1.0, 60.0)
_MAX_ATTRIBUTES = 10
"""Message attributes SQS and SNS accept per message."""

type GroupId = str | Callable[[OutgoingMessage], str]
"""A FIFO message group id, or a function deriving it from the message."""


_PLAIN_BODIES = (str, bytes, bytearray, memoryview, int, float, bool, dict, list, tuple, type(None))


def _structured(body: Any) -> bool:
    return not isinstance(body, _PLAIN_BODIES)


def _is_text(body: bytes) -> bool:
    """Whether the transport sends ``body`` as it is (no base64 and no encoding attribute)."""
    try:
        return is_sqs_safe_text(body.decode())
    except UnicodeDecodeError:
        return False


class _Resource:
    """Shared resolution logic (lookup/verify/create with failure modes and back-off)."""

    @property
    def kind(self) -> Literal["queue", "topic", "subscription"]:
        """The resource kind."""
        raise NotImplementedError

    def __init__(self, client: Client, name: str, provisioning: Provisioning | None) -> None:
        self._client = client
        self.name = name
        self._provisioning = provisioning
        self._info: Any = None
        self._lock: LoopLocal[asyncio.Lock] = LoopLocal(asyncio.Lock)
        self._failures = 0
        self._retry_at = 0.0
        self._last_error: BaseException | None = None
        self.last_report: ProvisioningReport | None = None
        """The most recent provisioning report (after resolution)."""
        # What the resolution of another resource (a queue provisioning its dead-letter queue)
        # settled for this one, while client.provision() waits for it instead of resolving again.
        self._adopted: ProvisioningReport | BaseException | None = None

    @property
    def provisioning(self) -> Provisioning:
        """The effective provisioning policy."""
        return self._provisioning or self._client.provisioning

    @property
    def resolved(self) -> bool:
        """Whether the resource has been resolved."""
        return self._info is not None

    async def _provision(self) -> Any:  # pragma: no cover - implemented by subclasses
        raise NotImplementedError

    async def _resolve(self, *, force: bool = False) -> Any:
        if self._info is not None and not force:
            return self._info
        async with self._lock.get():
            if self._info is not None and not force:
                return self._info
            if not force and time.monotonic() < self._retry_at:
                raise ResourceUnavailableError(
                    f"{self.kind} {self.name!r} is unavailable (retrying resolution shortly): {self._last_error}"
                )
            try:
                info = await self._provision()
            except Exception as error:  # not a cancellation: the caller went away, nothing failed
                self._note_failure(error)
                raise
            if info is None:
                error = ResourceUnavailableError(f"{self.kind} {self.name!r} is not available")
                self._note_failure(error)
                raise error
            self._info = info
            self._failures = 0
            self._retry_at = 0.0
            return info

    def _note_failure(self, error: BaseException) -> None:
        self._failures += 1
        self._last_error = error
        initial, maximum = _RETRY_AFTER_FAILURE
        self._retry_at = time.monotonic() + min(maximum, initial * 2 ** (self._failures - 1))

    def _reset(self) -> None:
        self._info = None
        self._failures = 0
        self._retry_at = 0.0


class _Destination(_Resource):
    """Publishing logic shared by topics and queues."""

    def __init__(
        self,
        client: Client,
        name: str,
        provisioning: Provisioning | None,
        *,
        envelope: Envelope | None,
        large_payloads: LargePayloads | Any | None,
        group_id: GroupId | None,
        codec: Codec | str | None,
    ) -> None:
        super().__init__(client, name, provisioning)
        if large_payloads is not None and large_payloads is not _UNSET:
            large_payloads.bucket_name(client.bucket_prefix)  # fail fast on a name S3 rejects
        self._envelope = envelope
        self._large_payloads = large_payloads
        self._group_id = group_id
        self._codec = codec
        self._transport: Transport | None = None
        self._destination_cache: tuple[Any, dict[str, Any]] | None = None
        self._native_destination_cache: tuple[Mapping[str, Any], Any] | None = None
        self._pending_subscriptions: list[Subscription] = []
        """Subscriptions of this topic declared after the client provisioned, resolved before publishing."""
        self._trace_budget_warned = False

    async def _resolve_pending_subscriptions(self) -> None:
        """Resolves subscriptions declared after the client provisioned, so the next message reaches them.

        A subscription that is unavailable (its provisioning policy logs the failure) stays pending
        and is tried again, after the usual back-off, by a later publish.
        """
        for subscription in list(self._pending_subscriptions):
            with contextlib.suppress(ResourceUnavailableError):
                await subscription.resolve()
        self._pending_subscriptions = [s for s in self._pending_subscriptions if not s.resolved]

    @property
    def envelope(self) -> Envelope:
        """The envelope used for publishing."""
        return self._envelope or self._client.envelope

    @property
    def large_payloads(self) -> LargePayloads | None:
        """Effective large payload settings."""
        return self._client.large_payloads if self._large_payloads is _UNSET else self._large_payloads

    @property
    def fifo(self) -> bool:
        """Whether the destination is FIFO (``.fifo`` suffix)."""
        return self.name.endswith(".fifo")

    def _destination(self, info: Any) -> dict[str, Any]:  # pragma: no cover - implemented by subclasses
        raise NotImplementedError

    def _destination_for(self, info: Any) -> dict[str, Any]:
        """The core destination of ``info``, built once per resolved resource (read-only)."""
        cache = self._destination_cache
        if cache is None or cache[0] is not info:
            cache = self._destination_cache = (info, self._destination(info))
        return cache[1]

    def _native_destination(self, core: Any, destination: Mapping[str, Any]) -> Any:
        """The core's converted ``destination``.

        Converted once for this resource's own destination (what :meth:`_destination_for` returns),
        on every call for any other mapping.
        """
        cache = self._native_destination_cache
        if cache is not None and cache[0] is destination:
            return cache[1]
        native = core.destination(destination)
        own = self._destination_cache
        if own is not None and own[1] is destination:
            self._native_destination_cache = (destination, native)
        return native

    async def _transport_for(self) -> Transport | None:
        if self._large_payloads is _UNSET:
            return None
        if self._transport is None:
            core = await self._client._core()
            settings = self.large_payloads
            self._transport = core.transport(
                settings.to_core(self._client.bucket_prefix) if settings is not None else None
            )
        return self._transport

    def _message(self, body: Any, options: Mapping[str, Any]) -> OutgoingMessage:
        try:
            return OutgoingMessage(body=body, **options)
        except TypeError as error:
            raise TypeError(f"invalid publish option: {error}") from None

    async def _encode(self, context: PublishContext, info: Any) -> PreparedMessage:
        return self._encode_now(context.message, info)

    def _encode_now(self, message: OutgoingMessage, info: Any) -> PreparedMessage:
        client = self._client
        if message.group_id is None and self._group_id is not None:
            message.group_id = self._group_id(message) if callable(self._group_id) else self._group_id
        if self.fifo and not message.group_id:
            raise ValidationError(f"publishing to FIFO {self.kind} {self.name!r} requires group_id")
        envelope = message.envelope or self.envelope
        trace_attributes = client._telemetry.inject(message, envelope)
        codec = message.codec if message.codec is not None else self._codec
        if isinstance(codec, str):
            codec = client.codecs.by_name(codec)
        encoded = client.codecs.encode(message.body, codec)
        message_type = message.type
        if message_type is None and _structured(message.body):
            message_type = client.types.name_of(message.body)
        payload = Payload(
            encoded.data,
            content_type=message.content_type or encoded.content_type,
            message_type=message_type,
            id=message.id,
            time=message.time,
            source=message.source or client.source,
            subject=message.subject,
            data_schema=message.data_schema,
            headers=message.headers,
        )
        wire = envelope.wrap(payload)
        if message.attributes or trace_attributes:
            attributes = dict(wire.attributes)
            for name, value in message.attributes.items():
                if name.startswith("fanferro."):
                    raise ValidationError(f"attribute {name!r} uses the reserved 'fanferro.' prefix")
                attributes[name] = to_attribute_value(value)
            if trace_attributes:
                self._add_trace_attributes(attributes, trace_attributes, wire.body, message)
            wire = wire.replace(attributes=attributes)
        return PreparedMessage(
            destination=self._destination_for(info),
            destination_name=self.name,
            wire=wire,
            message_id=message.id,
            message_type=message_type,
            group_id=message.group_id,
            deduplication_id=message.deduplication_id,
            # Middleware may have set a timedelta after the message was created.
            delay=optional_seconds(message.delay, "delay"),
            sns_subject=message.sns_subject,
            trace_header=message.trace_header,
        )

    def _add_trace_attributes(
        self,
        attributes: dict[str, AttributeValue],
        trace: dict[str, str],
        body: bytes,
        message: OutgoingMessage,
    ) -> None:
        """Adds the trace context attributes that fit into the attribute limit.

        Propagation must never make a message fail that would be sent without it: ``tracestate``
        (and other keys) go first, ``traceparent`` last, with a ``config.warning`` the first time
        for this destination. One slot stays free when the transport may need one (the encoding
        of a binary body, the pointer of an offloaded payload).
        """
        free = _MAX_ATTRIBUTES - len(attributes)
        if len(trace) > free - 1 and (self.large_payloads is not None or not _is_text(body)):
            free -= 1
        for key, value in trace.items():
            if free <= 0:
                break
            attributes[key] = AttributeValue.string(value)
            free -= 1
        dropped = [key for key in trace if key not in attributes]
        if dropped and not self._trace_budget_warned and len(attributes) <= _MAX_ATTRIBUTES:
            self._trace_budget_warned = True
            fallback = (
                "the trace continues through the AWS trace header"
                if message.trace_header is not None
                else "the trace context is not propagated"
            )
            self._client.events.emit(
                EventKind.CONFIG_WARNING,
                f"left trace context attributes {', '.join(dropped)} out of a message to {self.kind} {self.name!r}: "
                f"a message carries at most {_MAX_ATTRIBUTES} attributes and {fallback}; "
                "reduce the attributes of its messages to propagate it (reported once per destination)",
                destination=self.name,
                attributes=dropped,
            )

    async def _prepare_message(self, message: OutgoingMessage, info: Any) -> PreparedMessage:
        if not self._client.publish_middleware:
            return self._encode_now(message, info)
        context = PublishContext(message=message, destination=self.name, kind=self.kind, fifo=self.fifo)  # type: ignore[arg-type]

        async def final(ctx: PublishContext) -> PreparedMessage:
            return await self._encode(ctx, info)

        return await chain_publish(self._client.publish_middleware, final)(context)

    async def prepare(self, body: Any, /, **options: Any) -> PreparedMessage:
        """Encodes a message without sending it (e.g. to store it in a transactional outbox).

        Accepts the same options as :meth:`publish`. Send it later with ``client.publish_prepared``.
        Its attributes are checked as a publish checks them before sending (count, names, values),
        so that a message that could never be sent fails here, before it is stored.

        Raises:
            ValidationError: The message could not be published as it is.
        """
        message = body if isinstance(body, OutgoingMessage) else self._message(body, options)
        info = await self._resolve()
        if self._pending_subscriptions:
            await self._resolve_pending_subscriptions()
        prepared = await self._prepare_message(message, info)
        validate_wire(prepared.wire)
        return prepared

    async def publish(self, body: Any, /, **options: Any) -> PublishResult:
        """Publishes one message.

        Args:
            body: The application object (pydantic model, dataclass, protobuf message, dict, str,
                bytes...) or a fully specified :class:`fanferro.OutgoingMessage`.
            **options: :class:`fanferro.OutgoingMessage` fields: ``type``, ``attributes``,
                ``headers``, ``id``, ``time``, ``subject``, ``source``, ``data_schema``,
                ``content_type``, ``group_id``, ``deduplication_id``, ``delay`` (queues),
                ``sns_subject`` (topics), ``codec``, ``envelope``.

        Returns:
            The message ids and, for offloaded payloads, where the payload was stored.
        """
        message = body if isinstance(body, OutgoingMessage) else self._message(body, options)
        client = self._client
        info = self._info if self._info is not None else await self._resolve()
        if self._pending_subscriptions:
            await self._resolve_pending_subscriptions()
        started = monotonic()
        core = client._core_client if client._core_client is not None else await client._core()
        transport = None if self._large_payloads is _UNSET else await self._transport_for()
        with client._telemetry.publish_span(self.name, self.kind, message.id) as span:
            try:
                if client.publish_middleware:
                    prepared = await self._prepare_message(message, info)
                else:
                    prepared = self._encode_now(message, info)
                receipt = await core.publish(
                    self._native_destination(core, prepared.destination),
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
                duration = monotonic() - started
                client._telemetry.mark_error(span, error)
                client._telemetry.record_publish(self.name, self.kind, duration, 1, error)
                client.events.emit(
                    EventKind.PUBLISH_FAILED,
                    f"failed to publish to {self.kind} {self.name}",
                    exception=error,
                    destination=self.name,
                    kind=self.kind,
                    message_id=message.id,
                    message_type=message.type,
                )
                client._drain_core_events()
                raise
        result = PublishResult.from_core(receipt, self.name)
        duration = monotonic() - started
        client._telemetry.record_publish(self.name, self.kind, duration, 1, None)
        if client.events.is_enabled(EventKind.PUBLISH_SUCCEEDED):
            client.events.emit(
                EventKind.PUBLISH_SUCCEEDED,
                f"published to {self.kind} {self.name}",
                destination=self.name,
                kind=self.kind,
                message_id=result.message_id,
                transport_message_id=result.transport_message_id,
                message_type=prepared.message_type,
                size=result.size,
                offloaded=result.offloaded is not None,
                duration=round(duration, 6),
            )
        client._drain_core_events()
        return result

    async def publish_batch(
        self,
        messages: Iterable[Any],
        /,
        *,
        raise_on_failure: bool = True,
        **defaults: Any,
    ) -> BatchPublishResult:
        """Publishes many messages with batch API calls (10 per call), retrying failed entries.

        Args:
            messages: Bodies or :class:`fanferro.OutgoingMessage` objects.
            raise_on_failure: Raise :class:`fanferro.errors.BatchPublishError` if any entry failed
                (the others were still published). With ``False``, inspect ``result.failed``.
            **defaults: Options applied to plain bodies (as for :meth:`publish`).
        """
        outgoing = [m if isinstance(m, OutgoingMessage) else self._message(m, defaults) for m in messages]
        if not outgoing:
            return BatchPublishResult(())
        client = self._client
        info = await self._resolve()
        if self._pending_subscriptions:
            await self._resolve_pending_subscriptions()
        core = await client._core()
        transport = await self._transport_for()
        started = monotonic()
        results: list[PublishResult | BatchFailure | None] = [None] * len(outgoing)
        with client._telemetry.publish_span(self.name, self.kind, None, batch_size=len(outgoing)) as span:
            entries: list[tuple[int, PreparedMessage]] = []
            prepare_now = not client.publish_middleware
            for index, message in enumerate(outgoing):
                try:
                    if prepare_now:
                        entries.append((index, self._encode_now(message, info)))
                    else:
                        entries.append((index, await self._prepare_message(message, info)))
                except Exception as error:  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
                    results[index] = BatchFailure(index, message, error)
            if entries:
                try:
                    receipts = await core.publish_batch(
                        self._native_destination(core, self._destination_for(info)),
                        [prepared.core_entry() for _, prepared in entries],
                        transport=transport,
                    )
                except Exception as error:  # noqa: BLE001 - deliberate catch-all; the error is reported, never swallowed silently
                    client._telemetry.mark_error(span, error)
                    receipts = [error] * len(entries)
                for (index, _prepared), receipt in zip(entries, receipts, strict=True):
                    if isinstance(receipt, BaseException):
                        results[index] = BatchFailure(index, outgoing[index], receipt)
                    else:
                        results[index] = PublishResult.from_core(receipt, self.name)
        batch = BatchPublishResult(tuple(r for r in results if r is not None))
        duration = monotonic() - started
        if batch.successful:
            client._telemetry.record_publish(self.name, self.kind, duration, len(batch.successful), None)
            if client.events.is_enabled(EventKind.PUBLISH_SUCCEEDED):
                client.events.emit(
                    EventKind.PUBLISH_SUCCEEDED,
                    f"published {len(batch.successful)} messages to {self.kind} {self.name}",
                    destination=self.name,
                    kind=self.kind,
                    count=len(batch.successful),
                    duration=round(duration, 6),
                )
        for failure in batch.failed:
            client._telemetry.record_publish(self.name, self.kind, duration, 1, failure.error)
            client.events.emit(
                EventKind.PUBLISH_FAILED,
                f"failed to publish batch entry {failure.index} to {self.kind} {self.name}",
                exception=failure.error,
                destination=self.name,
                kind=self.kind,
                index=failure.index,
                message_id=getattr(failure.message, "id", None),
            )
        client._drain_core_events()
        if raise_on_failure:
            batch.raise_for_failures()
        return batch


class Topic(_Destination):
    """An SNS topic. Its ``name`` is the AWS name, including the client's ``topic_prefix``."""

    @property
    def kind(self) -> Literal["topic"]:
        """``"topic"``."""
        return "topic"

    def __init__(
        self,
        client: Client,
        name: str,
        *,
        arn: str | None = None,
        attributes: TopicAttributes | Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        provisioning: Provisioning | None = None,
        envelope: Envelope | None = None,
        large_payloads: LargePayloads | Any | None = _UNSET,
        group_id: GroupId | None = None,
        codec: Codec | str | None = None,
    ) -> None:
        super().__init__(
            client,
            name,
            provisioning,
            envelope=envelope,
            large_payloads=large_payloads,
            group_id=group_id,
            codec=codec,
        )
        self._arn = arn
        self.attributes = attributes_to_aws(attributes)
        self.tags = dict(tags or {})

    def _spec(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "arn": self._arn,
            "attributes": self.attributes,
            "tags": self.tags,
            "policy": self.provisioning.to_core(),
        }

    def _settings(self) -> dict[str, Any]:
        """What the declaration set (compared when the topic is declared again)."""
        return {
            "arn": self._arn,
            "attributes": self.attributes,
            "tags": self.tags,
            "provisioning": self._provisioning,
            "envelope": self._envelope,
            "group_id": self._group_id,
            "codec": self._codec,
            "large_payloads": self._large_payloads,
        }

    async def _provision(self) -> TopicInfo | None:
        report = await self._client._ensure("topic", self._spec())
        self.last_report = report
        return await self._client._settle_report(report, self.provisioning)  # type: ignore[return-value]

    async def resolve(self, *, force: bool = False) -> TopicInfo:
        """Resolves the topic per its provisioning policy (cached)."""
        return await self._resolve(force=force)

    @property
    def arn(self) -> str:
        """The topic ARN (requires the topic to be resolved)."""
        if self._info is None:
            if self._arn:
                return self._arn
            raise ResourceUnavailableError(f"topic {self.name!r} has not been resolved yet; await topic.resolve()")
        return self._info.arn

    def _destination(self, info: TopicInfo) -> dict[str, Any]:
        return {
            "kind": "topic",
            "arn": info.arn,
            "fifo": info.fifo,
            "content_based_deduplication": info.content_based_deduplication,
            "max_message_size": info.max_message_size,
        }

    def subscribe(self, queue: Queue | str, **options: Any) -> Subscription:
        """Declares a subscription of ``queue`` to this topic (see :meth:`fanferro.Client.subscription`)."""
        return self._client.subscription(self, queue, **options)

    async def get_attributes(self) -> dict[str, str]:
        """Reads the topic attributes from SNS."""
        info = await self.resolve()
        core = await self._client._core()
        return await core.topic_attributes(info.arn)

    async def delete(self) -> None:
        """Deletes the topic (teardown/tests)."""
        info = await self.resolve()
        core = await self._client._core()
        await core.delete_topic(info.arn)
        self._reset()
        self._client._drain_core_events()

    def __repr__(self) -> str:
        return f"Topic({self.name!r})"


class Queue(_Destination):
    """An SQS queue. Its ``name`` is the AWS name, including the client's ``queue_prefix``."""

    @property
    def kind(self) -> Literal["queue"]:
        """``"queue"``."""
        return "queue"

    def __init__(
        self,
        client: Client,
        name: str,
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
    ) -> None:
        super().__init__(
            client,
            name,
            provisioning,
            envelope=envelope,
            large_payloads=large_payloads,
            group_id=group_id,
            codec=codec,
        )
        self._url = url
        self.owner_account_id = owner_account_id
        self.attributes = attributes_to_aws(attributes)
        self.tags = dict(tags or {})
        self.dead_letter = dead_letter
        self._dead_letter_queue: Queue | None = None
        if dead_letter is not None:
            if dead_letter.name.endswith(".fifo") != self.fifo:
                raise ConfigurationError(
                    f"the dead-letter queue of {name!r} must be {'FIFO' if self.fifo else 'standard'}"
                )
            if client._declared_queue(dead_letter.name, prefixed=dead_letter.prefixed) is not None:
                # Shared with another queue, or declared on its own: that declaration stands, and only
                # what this one sets explicitly has to agree with it.
                self._dead_letter_queue = client.queue(
                    dead_letter.name,
                    prefixed=dead_letter.prefixed,
                    attributes=None
                    if dead_letter.attributes is DEFAULT_DEAD_LETTER_ATTRIBUTES
                    else dead_letter.attributes,
                    tags=dead_letter.tags or None,
                    provisioning=dead_letter.provisioning,
                )
            else:
                self._dead_letter_queue = client.queue(
                    dead_letter.name,
                    prefixed=dead_letter.prefixed,
                    attributes=dead_letter.attributes,
                    tags=dead_letter.tags,
                    provisioning=dead_letter.provisioning or provisioning,
                )

    @property
    def dead_letter_queue(self) -> Queue | None:
        """The declared dead-letter queue (consume it to inspect or redrive failed messages)."""
        return self._dead_letter_queue

    def _settings(self) -> dict[str, Any]:
        """What the declaration set (compared when the queue is declared again)."""
        return {
            "url": self._url,
            "owner_account_id": self.owner_account_id,
            "attributes": self.attributes,
            "tags": self.tags,
            "dead_letter": self.dead_letter,
            "provisioning": self._provisioning,
            "envelope": self._envelope,
            "group_id": self._group_id,
            "codec": self._codec,
            "large_payloads": self._large_payloads,
        }

    def _spec(self) -> dict[str, Any]:
        spec: dict[str, Any] = {
            "name": self.name,
            "url": self._url,
            "owner_account_id": self.owner_account_id,
            "attributes": self.attributes,
            "tags": self.tags,
            "policy": self.provisioning.to_core(),
        }
        if self.dead_letter is not None and self._dead_letter_queue is not None:
            spec["dead_letter"] = {
                "queue": self._dead_letter_queue._spec(),
                "max_receive_count": self.dead_letter.max_receive_count,
            }
        return spec

    async def _provision(self) -> QueueInfo | None:
        report = await self._client._ensure("queue", self._spec())
        self.last_report = report
        if report.dead_letter is not None and self._dead_letter_queue is not None:
            await self._dead_letter_queue._adopt(report.dead_letter)
        return await self._client._settle_report(report, self.provisioning)  # type: ignore[return-value]

    async def _adopt(self, report: ProvisioningReport) -> None:
        """Settles the report of this queue that the queue it is the dead-letter queue of got.

        The outcome is what a forced resolution of this queue would have given: the report, a
        :class:`ResourceUnavailableError` when the failure mode let it pass, or the error raised.
        """
        self.last_report = report
        if report.dead_letter is not None and self._dead_letter_queue is not None:
            await self._dead_letter_queue._adopt(report.dead_letter)
        # A failure leaves this queue's own retry back-off alone: the next use of it looks it up again.
        try:
            info = await self._client._settle_report(report, self.provisioning)
        except Exception as error:
            self._adopted = error
            raise
        if info is None:
            self._adopted = ResourceUnavailableError(f"{self.kind} {self.name!r} is not available")
            return
        self._info = info
        self._failures = 0
        self._retry_at = 0.0
        self._adopted = report

    async def resolve(self, *, force: bool = False) -> QueueInfo:
        """Resolves the queue per its provisioning policy (cached)."""
        return await self._resolve(force=force)

    @property
    def url(self) -> str:
        """The queue URL (requires the queue to be resolved)."""
        if self._info is None:
            if self._url:
                return self._url
            raise ResourceUnavailableError(f"queue {self.name!r} has not been resolved yet; await queue.resolve()")
        return self._info.url

    @property
    def arn(self) -> str:
        """The queue ARN (requires the queue to be resolved)."""
        if self._info is None:
            raise ResourceUnavailableError(f"queue {self.name!r} has not been resolved yet; await queue.resolve()")
        return self._info.arn

    def _destination(self, info: QueueInfo) -> dict[str, Any]:
        return {
            "kind": "queue",
            "url": info.url,
            "fifo": info.fifo,
            "content_based_deduplication": info.content_based_deduplication,
            "max_message_size": info.max_message_size,
        }

    async def send(self, body: Any, /, **options: Any) -> PublishResult:
        """Sends one message (alias of :meth:`publish`, in SQS terminology)."""
        return await self.publish(body, **options)

    async def send_batch(self, messages: Iterable[Any], /, **options: Any) -> BatchPublishResult:
        """Sends many messages (alias of :meth:`publish_batch`)."""
        return await self.publish_batch(messages, **options)

    def consumer(self, handler: Handler | Router | None = None, /, **options: Any) -> Consumer:
        """Creates a consumer for this queue (see :meth:`fanferro.Client.consumer`)."""
        return self._client.consumer(self, handler, **options)

    def subscribe_to(self, topic: Topic | str, **options: Any) -> Subscription:
        """Declares a subscription of this queue to ``topic``."""
        return self._client.subscription(topic, self, **options)

    async def get_attributes(self) -> dict[str, str]:
        """Reads the queue attributes from SQS."""
        info = await self.resolve()
        core = await self._client._core()
        return await core.queue_attributes(info.url)

    async def approximate_count(self) -> dict[str, int]:
        """Approximate visible, in-flight and delayed message counts."""
        attributes = await self.get_attributes()
        return {
            "visible": int(attributes.get("ApproximateNumberOfMessages", 0)),
            "in_flight": int(attributes.get("ApproximateNumberOfMessagesNotVisible", 0)),
            "delayed": int(attributes.get("ApproximateNumberOfMessagesDelayed", 0)),
        }

    async def purge(self) -> None:
        """Deletes every message in the queue."""
        info = await self.resolve()
        core = await self._client._core()
        await core.purge_queue(info.url)

    async def delete(self) -> None:
        """Deletes the queue (teardown/tests). The dead-letter queue is not deleted."""
        info = await self.resolve()
        core = await self._client._core()
        await core.delete_queue(info.url)
        self._reset()
        self._client._drain_core_events()

    def __repr__(self) -> str:
        return f"Queue({self.name!r})"


class Subscription(_Resource):
    """An SNS-to-SQS subscription.

    Attributes:
        topic: The topic.
        queue: The subscribed queue.
        raw_message_delivery: Deliver the original message and attributes (recommended) instead of
            an SNS notification document (fanferro handles both).
        filter_policy: SNS filter policy.
        filter_policy_scope: ``"MessageAttributes"`` or ``"MessageBody"``.
        dead_letter_queue: Subscription-level dead-letter queue for messages SNS cannot deliver.
        manage_queue_policy: Add a queue policy statement allowing the topic to send messages.
    """

    @property
    def kind(self) -> Literal["subscription"]:
        """``"subscription"``."""
        return "subscription"

    def __init__(
        self,
        client: Client,
        topic: Topic,
        queue: Queue,
        *,
        raw_message_delivery: bool = True,
        filter_policy: Mapping[str, Any] | None = None,
        filter_policy_scope: Literal["MessageAttributes", "MessageBody"] | None = None,
        dead_letter_queue: Queue | None = None,
        manage_queue_policy: bool = True,
        provisioning: Provisioning | None = None,
    ) -> None:
        if queue.fifo and not topic.fifo:
            raise ConfigurationError(
                f"the FIFO queue {queue.name!r} cannot be subscribed to the standard topic {topic.name!r}: SNS "
                "delivers to FIFO queues from FIFO topics only (subscribe it to a FIFO topic, or use a standard queue)"
            )
        super().__init__(client, f"{topic.name} -> {queue.name}", provisioning)
        self.topic = topic
        self.queue = queue
        self.raw_message_delivery = raw_message_delivery
        self.filter_policy = dict(filter_policy) if filter_policy is not None else None
        self.filter_policy_scope = filter_policy_scope
        self.dead_letter_queue = dead_letter_queue
        self.manage_queue_policy = manage_queue_policy

    def _settings(self) -> dict[str, Any]:
        """What the declaration set (compared when the subscription is declared again)."""
        return {
            "raw_message_delivery": self.raw_message_delivery,
            "filter_policy": self.filter_policy,
            "filter_policy_scope": self.filter_policy_scope,
            "dead_letter_queue": self.dead_letter_queue,
            "manage_queue_policy": self.manage_queue_policy,
            "provisioning": self._provisioning,
        }

    def _attributes(self, dead_letter_arn: str | None) -> dict[str, str]:
        import json

        attributes = {"RawMessageDelivery": "true" if self.raw_message_delivery else "false"}
        if self.filter_policy is not None:
            attributes["FilterPolicy"] = json.dumps(self.filter_policy, separators=(",", ":"), sort_keys=True)
        if self.filter_policy_scope is not None:
            attributes["FilterPolicyScope"] = self.filter_policy_scope
        if dead_letter_arn is not None:
            attributes["RedrivePolicy"] = json.dumps({"deadLetterTargetArn": dead_letter_arn}, separators=(",", ":"))
        return attributes

    async def _provision(self) -> SubscriptionInfo | None:
        topic = await self.topic.resolve()
        queue = await self.queue.resolve()
        dead_letter = await self.dead_letter_queue.resolve() if self.dead_letter_queue is not None else None
        spec = {
            "topic_arn": topic.arn,
            "queue_arn": queue.arn,
            "queue_url": queue.url,
            "attributes": self._attributes(dead_letter.arn if dead_letter else None),
            "manage_queue_policy": self.manage_queue_policy,
            "dead_letter_queue_url": dead_letter.url if dead_letter else None,
            "dead_letter_queue_arn": dead_letter.arn if dead_letter else None,
            "policy": self.provisioning.to_core(),
        }
        async with self._client._queue_policy_turn(queue.arn, dead_letter.arn if dead_letter else None):
            report = await self._client._ensure("subscription", spec)
        self.last_report = report
        return await self._client._settle_report(report, self.provisioning)  # type: ignore[return-value]

    async def resolve(self, *, force: bool = False) -> SubscriptionInfo:
        """Resolves the subscription per its provisioning policy (cached)."""
        return await self._resolve(force=force)

    async def delete(self) -> None:
        """Removes the subscription (teardown/tests).

        Raises:
            ConfigurationError: The subscription is assumed (``Provisioning.assume()``), so its ARN
                is unknown.
        """
        info = await self.resolve()
        if not info.arn:
            raise ConfigurationError(
                f"{self!r} is assumed (Provisioning.assume()), so its ARN is unknown: delete it through a "
                "subscription declared with a provisioning policy that looks it up, such as Provisioning.existing()"
            )
        core = await self._client._core()
        await core.unsubscribe(info.arn)
        self._reset()
        self._client._drain_core_events()

    def __repr__(self) -> str:
        return f"Subscription({self.topic.name!r} -> {self.queue.name!r})"
