"""Testing helpers.

* :func:`memory_client` - a :class:`fanferro.Client` backed by an in-process broker with SQS/SNS/S3
  semantics (visibility timeouts, redrive, FIFO, fan-out, filter policies). Fast and
  deterministic; no AWS or emulator needed. Resources are created on demand.
* :func:`build_message` - a :class:`fanferro.Message` for unit-testing a handler directly; the
  message records how the handler settled it (``message.outcome``, ``settler.calls``).
* :func:`receive_sqs_event` - receives messages of a queue as the SQS event an AWS Lambda event
  source mapping would invoke a function with; :func:`invoke_sqs_handler` also invokes a
  :func:`fanferro.aws_lambda.sqs_handler` function with it and deletes the records it did not
  report as failed, as the event source mapping does.

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

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from fanferro._core import MemoryBroker, Payload, new_message_id
from fanferro._utils import Duration, optional_seconds, seconds
from fanferro.client import Client
from fanferro.codecs import CodecRegistry
from fanferro.message_types import MessageTypes
from fanferro.messages import AttributeInput, HeaderValue, Message, to_attribute_value
from fanferro.provisioning import Provisioning

if TYPE_CHECKING:
    from fanferro.aws_lambda import LambdaContext, SqsEventHandler
    from fanferro.resources import Queue

__all__ = [
    "MemoryBroker",
    "RecordingSettler",
    "build_message",
    "invoke_sqs_handler",
    "memory_client",
    "receive_sqs_event",
]


def memory_client(*, broker: MemoryBroker | None = None, **options: Any) -> Client:
    """A client backed by an in-memory broker; resources are created on demand by default.

    Args:
        broker: Share a broker between clients (default: a new one).
        **options: Any :class:`fanferro.Client` option (``provisioning`` defaults to
            :meth:`Provisioning.ensure`).
    """
    options.setdefault("provisioning", Provisioning.ensure())
    options.setdefault("region", "us-east-1")
    return Client(broker=broker or MemoryBroker(), **options)


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
) -> Message[T]:
    """Builds a decoded :class:`fanferro.Message` around ``body`` for handler unit tests."""
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
    return Message(
        payload=payload,
        attributes={name: to_attribute_value(value) for name, value in (attributes or {}).items()},
        envelope="test",
        queue=queue,
        settler=settler or RecordingSettler(),
        body=body,
    )


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
