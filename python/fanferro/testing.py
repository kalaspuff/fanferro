"""Testing helpers.

* :func:`memory_client` - a :class:`fanferro.Client` backed by an in-process broker with SQS/SNS/S3
  semantics (visibility timeouts, redrive, FIFO, fan-out, filter policies). Fast and
  deterministic; no AWS or emulator needed. Resources are created on demand.
* :func:`build_message` - a :class:`fanferro.Message` for unit-testing a handler directly, with
  the transport metadata a received message has (``attempt``, ``receive_count``, ``group_id``,
  the SNS topic, ``sent_at``...); the message records how the handler settled it
  (``message.outcome``, ``settler.calls``).
* :func:`dispatch` - runs a consumer's processing (routing, decoding into the route's type,
  middleware, the handler, failure handling and settlement) for one message, without a broker,
  and returns its outcome and the settlement calls.
* :func:`receive_sqs_event` - receives messages of a queue as the SQS event an AWS Lambda event
  source mapping would invoke a function with; :func:`invoke_sqs_handler` also invokes a
  :func:`fanferro.aws_lambda.sqs_handler` function with it and deletes the records it did not
  report as failed, as the event source mapping does.
* Flows: :func:`wait_for` waits for a condition, :func:`consume_until` runs a consumer until one
  holds, :func:`drain` receives (and acks) everything a queue holds,
  :meth:`Consumer.run_until_idle(pending=True) <fanferro.Consumer.run_until_idle>` also waits for
  retried and delayed messages.
* :func:`record_published` - records every message a client publishes (destination, body, type,
  attributes, group id) for assertions.
* :func:`peek` - what a queue of the in-memory broker holds, without receiving it.

Example::

    async def test_order_flow() -> None:
        client = fanferro.testing.memory_client()
        orders = client.topic("orders")
        worker = client.queue("worker")
        client.subscription(orders, worker)
        consumer = client.consumer(worker)
        seen: list[OrderCreated] = []

        @consumer.route(OrderCreated)
        async def handle(order: OrderCreated) -> None:
            seen.append(order)

        async with client:
            await orders.publish(OrderCreated(id=1))
            await consumer.run_until_idle()
        assert seen == [OrderCreated(id=1)]
"""

from __future__ import annotations

import asyncio
import datetime
import inspect
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Self, Unpack, cast

from fanferro._core import AttributeValue, Delivery, MemoryBroker, Payload, new_message_id
from fanferro._utils import Duration, optional_seconds, seconds
from fanferro.client import Client
from fanferro.codecs import CodecRegistry
from fanferro.consumer import Consumer
from fanferro.errors import LeaseError
from fanferro.message_types import MessageTypes
from fanferro.messages import (
    _HIDDEN_ATTRIBUTES,
    _INTERNAL_PREFIX,
    AttributeInput,
    DeadLetterInfo,
    HeaderValue,
    Message,
    Outcome,
    PreparedMessage,
    to_attribute_value,
)
from fanferro.provisioning import Provisioning
from fanferro.routing import Router

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterator, Mapping

    from fanferro.aws_lambda import LambdaContext, SqsEventHandler
    from fanferro.middleware import PrepareNext, PublishContext
    from fanferro.options import ClientOptions, ConsumerOptions
    from fanferro.resources import Queue, Topic
    from fanferro.routing import Handler

__all__ = [
    "DispatchResult",
    "FakeDelivery",
    "MemoryBroker",
    "PublishRecorder",
    "PublishedMessage",
    "QueuedMessage",
    "RecordingSettler",
    "build_message",
    "consume_until",
    "dispatch",
    "drain",
    "invoke_sqs_handler",
    "memory_client",
    "peek",
    "receive_sqs_event",
    "record_published",
    "wait_for",
]


def memory_client(**options: Unpack[ClientOptions]) -> Client:
    """A client backed by an in-memory broker; resources are created on demand by default.

    Args:
        **options: Any :class:`fanferro.Client` option (:class:`fanferro.ClientOptions`):
            ``broker`` shares a broker between clients (default: a new one), ``provisioning``
            defaults to :meth:`Provisioning.ensure` and ``region`` to ``"us-east-1"``.
    """
    options.setdefault("provisioning", Provisioning.ensure())
    options.setdefault("region", "us-east-1")
    broker = options.pop("broker", None)
    return Client(broker=broker if broker is not None else MemoryBroker(), **options)


@dataclass
class RecordingSettler:
    """A settler that records calls instead of talking to SQS."""

    calls: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    async def ack(self, message: Message[Any]) -> None:
        self.calls.append(("ack", {}))

    async def retry(self, message: Message[Any], delay: float | None, error: BaseException | None) -> None:
        self.calls.append(("retry", {"delay": delay, "error": error}))

    async def requeue(self, message: Message[Any], delay: float | None, error: BaseException | None) -> None:
        self.calls.append(("requeue", {"delay": delay, "error": error}))

    async def dead_letter(self, message: Message[Any], reason: str, error: BaseException | None) -> None:
        self.calls.append(("dead_letter", {"reason": reason, "error": error}))

    async def release(self, message: Message[Any]) -> None:
        self.calls.append(("release", {}))

    async def discard(self, message: Message[Any], reason: str) -> None:
        self.calls.append(("discard", {"reason": reason}))

    async def extend(self, message: Message[Any], timeout: float) -> None:
        self.calls.append(("extend", {"timeout": timeout}))


_TEST_ACCOUNT = "000000000000"
_TEST_REGION = "us-east-1"


class FakeDelivery:
    """The transport side of a message built by :func:`build_message` (stands in for a delivery).

    It reports the transport metadata it was built with (``receive_count``, ``group_id``, the SNS
    topic, ``sent_at``...) and records the delivery calls a consumer makes to settle the message
    in :attr:`calls` (``("ack", {})``, ``("nack", {"delay": 2.0})``, ``("requeue", {"attempt": 1,
    "delay": 1.0})``, ``("dead_letter", {"reason": ..., "details": {...}})``, ``("release", {})``,
    ``("extend", {"timeout": 30.0})``, ``("leave", {})``). A handler that settles a built message
    itself goes through the message's :class:`RecordingSettler` instead; :func:`dispatch` settles
    through the consumer, which calls this delivery.
    """

    def __init__(
        self,
        *,
        raw_body: str = "",
        raw_attributes: Mapping[str, AttributeValue] | None = None,
        receive_count: int = 1,
        sqs_message_id: str | None = None,
        group_id: str | None = None,
        deduplication_id: str | None = None,
        sequence_number: str | None = None,
        topic_arn: str | None = None,
        sent_at: datetime.datetime | None = None,
        received_at: datetime.datetime | None = None,
        visibility_timeout: float = 30.0,
    ) -> None:
        if receive_count < 1:
            raise ValueError(f"receive_count must be at least 1, got {receive_count}")
        now = datetime.datetime.now(datetime.UTC)
        self.sqs_message_id: str = sqs_message_id or new_message_id()
        self.receipt_handle: str = f"test-receipt-{self.sqs_message_id}"
        self.raw_body: str = raw_body
        self.raw_attributes: dict[str, AttributeValue] = dict(raw_attributes or {})
        self.receive_count: int = receive_count
        self.sent_at: datetime.datetime | None = sent_at or now
        self.first_received_at: datetime.datetime | None = received_at or now
        self.received_at: datetime.datetime = received_at or now
        self.group_id: str | None = group_id
        self.deduplication_id: str | None = deduplication_id
        self.sequence_number: str | None = sequence_number
        self.sender_id: str | None = _TEST_ACCOUNT
        self.aws_trace_header: str | None = None
        self.dead_letter_source_arn: str | None = None
        self.sns: dict[str, Any] | None = (
            None
            if topic_arn is None
            else {
                "message_id": new_message_id(),
                "topic_arn": topic_arn,
                "subject": None,
                "timestamp": self.sent_at,
                "sequence_number": None,
            }
        )
        self.wire: Any = None
        self.decode_error: Any = None
        self.pointer: dict[str, Any] | None = None
        self.remaining_visibility: float = visibility_timeout
        self.lease_status: str = "active"
        self.calls: list[tuple[str, dict[str, Any]]] = []
        """The delivery calls made to settle the message, in order."""

    @property
    def settled(self) -> bool:
        """Whether a settling call was made."""
        return self.lease_status == "settled"

    def _record(self, operation: str, settles: bool = True, **arguments: Any) -> None:
        self.calls.append((operation, arguments))
        if settles:
            self.lease_status = "settled"

    def mark_active(self) -> None:
        """No-op (a built message is already being processed)."""

    def mark_awaited(self, waiting_handlers: int | None = None) -> None:
        """No-op (there is no settlement batch to send early)."""

    def leave(self) -> None:
        """Records that the message was left to time out (``("leave", {})``)."""
        self._record("leave", settles=False)

    def ack_in_background(self) -> None:
        """Records an ack."""
        self._record("ack")

    async def ack(self) -> None:
        """Records an ack."""
        self._record("ack")

    async def nack(self, delay: float) -> None:
        """Records a visibility change for a retry after ``delay`` seconds."""
        self._record("nack", delay=delay)

    async def release(self) -> None:
        """Records a release."""
        self._record("release")

    async def extend(self, timeout: float) -> None:
        """Records an extension of the visibility timeout."""
        self._record("extend", settles=False, timeout=timeout)
        self.remaining_visibility = timeout

    async def dead_letter(self, reason: str, details: Mapping[str, str] | None = None) -> None:
        """Records a forward to the dead-letter queue."""
        self._record("dead_letter", reason=reason, details=dict(details or {}))

    async def requeue(self, attempt: int, delay: float | None = None) -> None:
        """Records a re-enqueued copy (whose next receive is attempt ``attempt + 1``)."""
        self._record("requeue", attempt=attempt, delay=delay)

    def __repr__(self) -> str:
        return f"FakeDelivery(sqs_message_id={self.sqs_message_id!r}, receive_count={self.receive_count})"


def _topic_arn(topic: str) -> str:
    return topic if topic.startswith("arn:") else f"arn:aws:sns:{_TEST_REGION}:{_TEST_ACCOUNT}:{topic}"


def build_message[T](
    body: T,
    *,
    type: str | None = None,
    attributes: dict[str, AttributeInput] | None = None,
    headers: dict[str, HeaderValue] | None = None,
    id: str | None = None,
    content_type: str | None = None,
    queue: str = "test-queue",
    codecs: CodecRegistry | None = None,
    settler: RecordingSettler | None = None,
    attempt: int | None = None,
    receive_count: int | None = None,
    group_id: str | None = None,
    deduplication_id: str | None = None,
    sequence_number: str | None = None,
    topic: str | None = None,
    sent_at: datetime.datetime | None = None,
) -> Message[T]:
    """Builds a decoded :class:`fanferro.Message` around ``body`` for handler unit tests.

    The message looks received: its :class:`FakeDelivery` reports the transport metadata given here
    (an SQS message id, ``sent_at`` and ``received_at`` of now by default), and settlements the
    handler makes are recorded by ``settler`` (``message.outcome`` tells which one it made).

    Args:
        body: The body; encoded with ``codecs`` and kept as the decoded ``message.body``.
        type: Message type name (default: the registered name of the body's class).
        attributes: Message attributes.
        headers: Envelope headers.
        id: Message id (default: a new one).
        content_type: Content type (default: the codec's).
        queue: Name of the queue the message is received from.
        codecs: Codecs to encode the body with (default: :class:`~fanferro.codecs.CodecRegistry`).
        settler: Records the handler's settlements (default: a new :class:`RecordingSettler`).
        attempt: The processing attempt (``message.attempt``). Without ``receive_count`` the
            message was received ``attempt`` times, as retries with the default visibility
            strategy do; with it, the earlier attempts were requeued copies (the
            ``fanferro.attempt`` attribute).
        receive_count: How many times SQS delivered this copy (default: ``attempt``, else 1).
        group_id: FIFO message group id.
        deduplication_id: FIFO deduplication id.
        sequence_number: FIFO sequence number.
        topic: The SNS topic the message was published to, as a name or an ARN (``m.topic()``
            matches either); a name becomes an ARN in account ``000000000000``, ``us-east-1``.
        sent_at: When SQS accepted the message (default: now).

    Raises:
        ValueError: If ``attempt`` or ``receive_count`` is below 1, or ``attempt`` is below
            ``receive_count``.
    """
    registry = codecs or CodecRegistry()
    encoded = registry.encode(body)
    message_type = type
    if message_type is None and not isinstance(body, str | bytes | dict | list | int | float | bool | None):
        message_type = MessageTypes(registry).name_of(body)
    payload = Payload(
        encoded.data,
        content_type=content_type or encoded.content_type,
        message_type=message_type,
        id=id or new_message_id(),
        headers=headers or {},
    )
    raw = {name: to_attribute_value(value) for name, value in (attributes or {}).items()}
    if receive_count is None:
        receive_count = attempt if attempt is not None else 1
    if receive_count < 1:
        raise ValueError(f"receive_count must be at least 1, got {receive_count}")
    if attempt is not None:
        if attempt < receive_count:
            raise ValueError(f"attempt ({attempt}) must be at least receive_count ({receive_count})")
        raw.pop("fanferro.attempt", None)
        if attempt > receive_count:
            raw["fanferro.attempt"] = to_attribute_value(attempt - receive_count)
    delivery = FakeDelivery(
        raw_body=encoded.data.decode("utf-8", "replace"),
        raw_attributes=raw,
        receive_count=receive_count,
        group_id=group_id,
        deduplication_id=deduplication_id,
        sequence_number=sequence_number,
        topic_arn=_topic_arn(topic) if topic is not None else None,
        sent_at=sent_at,
    )
    return Message(
        payload=payload,
        attributes=raw,
        envelope="test",
        delivery=cast("Delivery", delivery),
        queue=queue,
        settler=settler or RecordingSettler(),
        body=body,
    )


@dataclass(frozen=True, slots=True)
class DispatchResult:
    """What :func:`dispatch` did with a message."""

    outcome: Outcome | None
    """How the consumer settled the message (``None``: left to time out, or a settlement failed)."""
    message: Message[Any]
    """The message, with ``message.body`` decoded for the route that handled it."""
    calls: list[tuple[str, dict[str, Any]]]
    """The delivery calls the consumer made (see :class:`FakeDelivery`)."""


async def dispatch(
    target: Consumer | Router | Handler,
    message: Message[Any] | object,
    *,
    client: Client | None = None,
    dead_letter_queue: bool | None = None,
    **options: Unpack[ConsumerOptions],
) -> DispatchResult:
    """Runs a consumer's processing for one message, without a broker or a running consumer.

    Does what a consumer does with a message it received, using the consumer's own code: applies
    ``RetryPolicy.max_attempts``, selects the route, decodes the body into the route's type, runs
    the middleware and the handler (with ``handler_timeout``), and settles the message as the
    retry and dead-letter policies say (handler actions such as :class:`~fanferro.Retry`,
    :class:`~fanferro.Reject`, :class:`~fanferro.Requeue` and :class:`~fanferro.Discard`, errors,
    unroutable messages and decode errors included). The settlement is not sent anywhere: the
    delivery calls are returned. Events are emitted on the consumer's client.

    Example::

        result = await dispatch(router, build_message(OrderCreated(id=1), attempt=3))
        assert result.outcome == "retried"
        assert result.calls == [("nack", {"delay": 4.0})]

    Args:
        target: A :class:`~fanferro.Consumer` (its client, queue, policies and middleware apply;
            it need not be started), a :class:`~fanferro.Router`, or a handler (the only route).
        message: A message from :func:`build_message` (its transport metadata applies), or a body
            to build one from with the client's codecs.
        client: The client of a consumer created for a router or handler (default:
            :func:`memory_client`); ignored for a consumer.
        dead_letter_queue: Whether the queue has a dead-letter queue, which decides whether
            dead-lettering forwards the message or applies ``DeadLetterPolicy.on_missing_queue``.
            Default: whether the queue or the dead-letter policy declares one (a started consumer
            keeps the one it found).
        **options: :class:`~fanferro.Consumer` options (:class:`fanferro.ConsumerOptions`) of a
            consumer created for a router or handler (``retry``, ``dead_letter``, ``middleware``,
            ``handler_timeout``...).

    Returns:
        The outcome, the message and the delivery calls.

    Raises:
        TypeError: If ``message`` is a received message (it has a real delivery), or ``options``
            are given with a consumer.
    """
    if isinstance(target, Consumer):
        if options:
            raise TypeError("consumer options apply to a router or handler, not to a consumer")
        consumer = target
    else:
        client = client if client is not None else memory_client()
        router = target if isinstance(target, Router) else Router()
        if not isinstance(target, Router):
            router.add(target, fallback=True)
        queue = message.queue if isinstance(message, Message) and message.queue else "test-queue"
        consumer = Consumer(client, client.queue(queue), router, **options)
    if isinstance(message, Message):
        built = cast("Message[Any]", message)
        if built.delivery is None:
            built.delivery = cast("Delivery", FakeDelivery(raw_attributes=built.raw_attributes))
        elif not isinstance(built.delivery, FakeDelivery):
            raise TypeError("dispatch() takes a message from build_message(), not a received message")
        if built.settled:
            raise LeaseError(f"message {built.id} was already settled ({built.outcome})")
    else:
        codecs = consumer._client.codecs
        built = build_message(message, codecs=codecs, queue=consumer.queue.name)
    delivery = cast("FakeDelivery", built.delivery)
    if dead_letter_queue is None and consumer.state == "created":
        policy_queue = consumer.dead_letter_policy.queue
        dead_letter_queue = policy_queue is not None or consumer.queue.dead_letter_queue is not None
    # The message gets a settler of its own that knows about the dead-letter queue: the consumer
    # is left as it is, also when dispatches on it overlap.
    outcome = await consumer._dispatch(built, dead_letter_queue=dead_letter_queue)
    return DispatchResult(outcome=outcome, message=built, calls=list(delivery.calls))


async def receive_sqs_event(
    queue: Queue,
    *,
    max_messages: int = 10,
    wait_time: Duration = 0.0,
    visibility_timeout: Duration | None = None,
) -> dict[str, Any]:
    """Receives messages of ``queue`` as the SQS event Lambda would invoke a function with.

    The records carry real receipt handles: a handler from :func:`fanferro.aws_lambda.sqs_handler`
    can retry and dead-letter them. Lambda deletes the records a function does not report as
    failed; a test that wants the same deletes them itself (or checks the response).

    Args:
        queue: The queue (resolved if needed).
        max_messages: Messages to receive at most (1-10).
        wait_time: Long polling wait time in seconds (0-20).
        visibility_timeout: Visibility timeout of the received messages (default: the queue's).

    Returns:
        ``{"Records": [...]}``, possibly with no records.
    """
    info = await queue.resolve()
    core = await queue._client._core()
    records = await core.receive_records(
        info.url,
        info.arn,
        max_messages,
        seconds(wait_time, "wait_time"),
        optional_seconds(visibility_timeout, "visibility_timeout"),
    )
    return {"Records": records}


async def invoke_sqs_handler(
    handler: SqsEventHandler,
    queue: Queue,
    *,
    max_messages: int = 10,
    wait_time: Duration = 0.0,
    context: LambdaContext | None = None,
) -> dict[str, Any]:
    """Does what an SQS event source mapping does once: receives messages, invokes ``handler``.

    Receives up to ``max_messages`` messages of ``queue`` (:func:`receive_sqs_event`), awaits
    :meth:`~fanferro.aws_lambda.SqsEventHandler.handle` with them, and deletes the records the
    response does not list in ``batchItemFailures``. An invocation that raises deletes nothing.

    Returns:
        The handler's response (``{"batchItemFailures": []}`` when no message was received).
    """
    event = await receive_sqs_event(queue, max_messages=max_messages, wait_time=wait_time)
    if not event["Records"]:
        return {"batchItemFailures": []}
    response = await handler.handle(event, context)
    failed = {item["itemIdentifier"] for item in response["batchItemFailures"]}
    processed = [record["receiptHandle"] for record in event["Records"] if record["messageId"] not in failed]
    if processed:
        core = await queue._client._core()
        await core.delete_records(queue.url, processed)
    return response


# -- flows ------------------------------------------------------------------------------------


async def wait_for(
    condition: Callable[[], bool | Awaitable[bool]],
    *,
    timeout: Duration = 15.0,
    interval: Duration = 0.05,
) -> None:
    """Waits until ``condition()`` is true, checking it every ``interval`` seconds.

    Example::

        await wait_for(lambda: len(seen) == 3)
        await wait_for(lambda: consumer.stats().engine.receive_requests >= 1, timeout=5)

    Args:
        condition: Returns whether the wait is over (or an awaitable of it, e.g. an ``async def``
            that reads a queue's counts).
        timeout: How long to wait at most (seconds or a ``timedelta``).
        interval: How long to sleep between checks (seconds or a ``timedelta``).

    Raises:
        TimeoutError: If the condition is still false after ``timeout``.
    """
    limit = seconds(timeout, "timeout")
    pause = seconds(interval, "interval")
    deadline = time.monotonic() + limit
    while True:
        result = condition()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(f"condition not met within {limit} seconds")
        await asyncio.sleep(pause)


async def consume_until(
    consumer: Consumer,
    condition: Callable[[], bool | Awaitable[bool]],
    *,
    timeout: Duration = 20.0,
) -> None:
    """Runs ``consumer`` until ``condition()`` is true, then stops it (also when the wait fails).

    The consumer is started unless it is running. Unlike :meth:`Consumer.run_until_idle
    <fanferro.Consumer.run_until_idle>`, this waits for something to happen (a handler saw three
    messages, a hook got an event), not for the queue to be empty.

    Example::

        await consume_until(consumer, lambda: len(seen) == 3)

    Raises:
        TimeoutError: If the condition is still false after ``timeout`` (seconds or a ``timedelta``).
    """
    if consumer.state != "running":
        await consumer.start()
    try:
        await wait_for(condition, timeout=timeout)
    finally:
        await consumer.stop()


async def drain(
    queue: Queue,
    *,
    timeout: Duration = 15.0,
    idle_for: Duration | None = None,
    pending: bool = False,
) -> list[Message[bytes]]:
    """Receives everything ``queue`` holds, acks it and returns the messages in the order received.

    Runs a consumer of the queue's client with a handler that keeps every message, until the queue
    is idle (:meth:`Consumer.run_until_idle <fanferro.Consumer.run_until_idle>`). The bodies are not
    decoded into classes: read them with ``message.json()`` or ``message.data`` (the encoded bytes),
    ``message.type`` and ``message.attributes``. Useful to assert on what reached a queue, a
    dead-letter queue included::

        dead = await drain(orders.dead_letter_queue)
        assert [m.json() for m in dead] == [{"id": 1}]
        assert dead[0].dead_letter_info.reason == "handler_error"

    Args:
        queue: The queue (it is resolved if needed).
        timeout: How long to wait at most (seconds or a ``timedelta``).
        idle_for: How long the queue must be quiet (default: 0.3 seconds on the in-memory broker,
            1 second elsewhere, where a receive can come back empty while messages are on their
            way).
        pending: Also wait for messages in flight (held by another consumer, or waiting for a
            retry) and delayed ones, and receive them too.

    Raises:
        TimeoutError: If the queue does not go idle within ``timeout``.
    """
    client = queue._client
    received: list[Message[bytes]] = []

    async def collect(message: Message[bytes]) -> None:
        received.append(message)

    consumer = client.consumer(queue, collect, wait_time=1, name=f"drain-{queue.name}")
    quiet = idle_for if idle_for is not None else (0.3 if client.broker is not None else 1.0)
    await consumer.run_until_idle(idle_for=quiet, timeout=timeout, pending=pending)
    return received


# -- published messages -----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PublishedMessage:
    """A message :func:`record_published` saw being prepared for sending, as it goes out."""

    destination: str
    """The queue or topic name (with the client's ``queue_prefix``/``topic_prefix``)."""
    kind: Literal["queue", "topic"]
    """Whether it went to a queue or a topic."""
    body: Any
    """The body as passed to ``publish`` (not encoded)."""
    type: str | None
    """The message type name (the registered name of the body's class unless given)."""
    id: str
    """The fanferro message id."""
    attributes: dict[str, AttributeInput]
    """The application's message attributes (publish middleware's included, fanferro's own not)."""
    headers: dict[str, HeaderValue]
    """The envelope headers."""
    group_id: str | None
    """The FIFO message group id."""
    deduplication_id: str | None
    """The FIFO deduplication id, if one was given."""
    delay: float | None
    """The delivery delay in seconds."""
    prepared: PreparedMessage
    """The encoded message (``prepared.wire`` holds the body and attributes as sent)."""


class PublishRecorder:
    """Records the messages a client publishes; made by :func:`record_published`.

    It is a publish middleware of the client while it records, the last (innermost) one when it
    started: it sees each message after the other middleware changed it. A message is recorded
    once it was encoded for sending, by ``publish``, ``send``, ``publish_batch`` and ``prepare``
    (a prepared message is recorded when it is prepared, not when ``publish_prepared`` sends it);
    one whose send then fails stays recorded.

    The recorder is a sequence of :class:`PublishedMessage`, in the order they were prepared.
    """

    def __init__(self, client: Client) -> None:
        self._client = client
        self.messages: list[PublishedMessage] = []
        """The recorded messages, in order."""

    @property
    def recording(self) -> bool:
        """Whether the recorder is installed as a publish middleware of its client."""
        return any(item == self._record for item in self._client.publish_middleware)

    def start(self) -> None:
        """Starts recording (again); :func:`record_published` already did."""
        if not self.recording:
            self._client.publish_middleware.append(self._record)

    def stop(self) -> None:
        """Stops recording (the recorded messages stay)."""
        middleware = self._client.publish_middleware
        middleware[:] = [item for item in middleware if item != self._record]

    def clear(self) -> None:
        """Forgets the recorded messages."""
        self.messages.clear()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    async def _record(self, context: PublishContext, call_next: PrepareNext) -> PreparedMessage:
        prepared = await call_next(context)
        message = context.message
        self.messages.append(
            PublishedMessage(
                destination=context.destination,
                kind=context.kind,
                body=message.body,
                type=prepared.message_type,
                id=prepared.message_id,
                attributes=dict(message.attributes),
                headers=dict(message.headers),
                group_id=prepared.group_id,
                deduplication_id=message.deduplication_id,
                delay=prepared.delay,
                prepared=prepared,
            )
        )
        return prepared

    def to(self, destination: Topic | Queue | str) -> list[PublishedMessage]:
        """The messages sent to ``destination``: a topic or queue, or its name.

        A name matches with or without the client's ``topic_prefix``/``queue_prefix``.
        """
        if isinstance(destination, str):
            client = self._client
            names = {destination, client.topic_prefix + destination, client.queue_prefix + destination}
            return [message for message in self.messages if message.destination in names]
        return [
            message
            for message in self.messages
            if message.destination == destination.name and message.kind == destination.kind
        ]

    def bodies(self, destination: Topic | Queue | str | None = None) -> list[Any]:
        """The bodies of the recorded messages, of those sent to ``destination`` if it is given."""
        messages = self.messages if destination is None else self.to(destination)
        return [message.body for message in messages]

    def __len__(self) -> int:
        return len(self.messages)

    def __iter__(self) -> Iterator[PublishedMessage]:
        return iter(self.messages)

    def __getitem__(self, index: int) -> PublishedMessage:
        return self.messages[index]

    def __repr__(self) -> str:
        return f"PublishRecorder(messages={len(self.messages)}, recording={self.recording})"


def record_published(client: Client) -> PublishRecorder:
    """Records every message ``client`` publishes from now on, for assertions.

    Use it as a context manager to stop recording at its end, or keep the recorder for the rest
    of the test (:meth:`PublishRecorder.stop` stops it)::

        with record_published(client) as published:
            await place_order(client, order_id=1)
        assert published.bodies(orders) == [OrderPlaced(order_id=1)]
        assert published[0].attributes == {"tenant": "acme"}

    Works on any backend; nothing is published differently. See :class:`PublishRecorder` for what
    is recorded.
    """
    recorder = PublishRecorder(client)
    recorder.start()
    return recorder


# -- queue contents ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QueuedMessage:
    """A message in a queue of the in-memory broker, as :func:`peek` shows it."""

    sqs_message_id: str
    """The SQS message id."""
    body: str
    """The SQS body: the encoded message in its envelope (an SNS notification when the queue is
    subscribed without raw delivery, a claim-check pointer when the payload was offloaded)."""
    raw_attributes: dict[str, AttributeValue]
    """All message attributes, fanferro's own included (``fanferro.type``, ``fanferro.attempt``...)."""
    receive_count: int
    """How many times the message was received."""
    in_flight: bool
    """Whether it was received and is invisible until its visibility timeout runs out (a retry)."""
    delayed: bool
    """Whether it was sent with a delay that has not run out yet."""
    group_id: str | None
    """The FIFO message group id."""

    @property
    def visible(self) -> bool:
        """Whether a receive could return the message now (FIFO group order aside)."""
        return not self.in_flight and not self.delayed

    @property
    def attributes(self) -> dict[str, AttributeValue]:
        """Message attributes without fanferro's internal ones."""
        return {
            name: value
            for name, value in self.raw_attributes.items()
            if not name.startswith(_INTERNAL_PREFIX) and name not in _HIDDEN_ATTRIBUTES
        }

    @property
    def type(self) -> str | None:
        """The message type fanferro sent in the ``fanferro.type`` attribute (``None`` without one)."""
        value = self.raw_attributes.get("fanferro.type")
        return None if value is None else str(value.value)

    @property
    def dead_letter_info(self) -> DeadLetterInfo | None:
        """Why fanferro dead-lettered the message (``None`` unless it is a dead-lettered copy)."""
        value = self.raw_attributes.get("fanferro.dead-letter")
        return None if value is None else DeadLetterInfo.from_json(str(value.value))


async def peek(queue: Queue) -> list[QueuedMessage]:
    """Every message a queue of the in-memory broker holds, in send order, without receiving it.

    Unlike :meth:`Queue.peek <fanferro.Queue.peek>` (any backend), nothing is received: receive
    counts and visibility stay as they are, and in-flight and delayed messages are listed too.
    Messages that were deleted (acked, dead-lettered) are gone::

        await consumer.run_until_idle()
        [retried] = await peek(orders)
        assert retried.in_flight and retried.receive_count == 1

    Raises:
        TypeError: If the queue's client is not backed by a :class:`MemoryBroker` (see
            :func:`memory_client`).
    """
    broker = queue._client.broker
    if broker is None:
        raise TypeError("peek() reads the in-memory broker; use a client from memory_client(), or Queue.peek() on SQS")
    info = await queue.resolve()
    return [
        QueuedMessage(
            sqs_message_id=message["message_id"],
            body=message["body"],
            raw_attributes=message["attributes"],
            receive_count=message["receive_count"],
            in_flight=message["in_flight"],
            delayed=message["delayed"],
            group_id=message["group_id"],
        )
        for message in broker.queue_messages(info.url)
    ]
