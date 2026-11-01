"""SQS events of an AWS Lambda event source mapping, processed by fanferro.aws_lambda handlers."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

import fanferro
from fanferro.envelopes import CloudEvents
from fanferro.testing import invoke_sqs_handler, receive_sqs_event
from tests.conftest import ClientFactory
from tests.helpers import OrderCreated, drain_queue

FAST_RETRY = fanferro.RetryPolicy(max_attempts=5, backoff=fanferro.Backoff.constant(0))


class _Context:
    """A Lambda context with ``remaining`` seconds left."""

    def __init__(self, remaining: float) -> None:
        self.remaining = remaining

    def get_remaining_time_in_millis(self) -> int:
        return int(self.remaining * 1000)


def _failed(response: dict[str, Any]) -> list[str]:
    return [item["itemIdentifier"] for item in response["batchItemFailures"]]


async def test_records_are_decoded_routed_and_failures_reported(client_factory: ClientFactory, name: Any) -> None:
    bucket = name("payloads")
    client = client_factory(large_payloads=fanferro.LargePayloads(bucket, threshold=2048, create_bucket=True))
    topic = client.topic(name("orders"), envelope=CloudEvents())
    queue = client.queue(name("orders-fn"))
    client.subscription(topic, queue, raw_message_delivery=False)  # SNS notifications
    seen: list[tuple[int, int, bool]] = []
    handler = fanferro.aws_lambda.sqs_handler(client, queue=queue, retry=FAST_RETRY)

    @handler.route(OrderCreated)
    async def on_order(message: fanferro.Message[OrderCreated]) -> None:
        seen.append((message.body.order_id, message.attempt, message.pointer is not None))
        if message.body.order_id == 2 and message.attempt == 1:
            raise ConnectionError("downstream unavailable")

    @handler.default
    async def on_other(message: fanferro.Message[dict[str, str]]) -> None:
        seen.append((len(message.body["blob"]), message.attempt, message.pointer is not None))

    async with client:
        await topic.publish(OrderCreated(order_id=1))
        failing = await topic.publish(OrderCreated(order_id=2))
        assert (await topic.publish({"blob": "x" * 5000})).offloaded is not None
        event: dict[str, Any] = {"Records": []}
        for _ in range(20):
            more = await receive_sqs_event(queue, wait_time=1)
            event["Records"].extend(more["Records"])
            if len(event["Records"]) == 3:
                break
        assert len(event["Records"]) == 3
        response = await handler.handle(event)
        assert len(_failed(response)) == 1
        failed_record = next(r for r in event["Records"] if r["messageId"] == _failed(response)[0])
        assert json.loads(failed_record["body"])["MessageId"]  # an SNS notification
        # Lambda deletes the rest; the retried record comes back (backoff 0) and succeeds.
        core = await client._core()
        await core.delete_records(
            queue.url, [r["receiptHandle"] for r in event["Records"] if r["messageId"] not in _failed(response)]
        )
        again = await invoke_sqs_handler(handler, queue, wait_time=2)
        assert again == {"batchItemFailures": []}
        assert sum((await queue.approximate_count()).values()) == 0
    assert failing.message_id
    assert sorted(seen) == [(1, 1, False), (2, 1, False), (2, 2, False), (5000, 1, True)]


async def test_dead_lettered_records_are_forwarded_and_not_reported(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("fn"), dead_letter=fanferro.DeadLetterQueue(name("fn-dlq"), max_receive_count=50))
    handler = fanferro.aws_lambda.sqs_handler(client)

    @handler.route(OrderCreated)
    async def on_order(order: OrderCreated) -> None:
        if order.order_id == 1:
            raise fanferro.Reject("invalid order")
        if order.order_id == 2:
            raise fanferro.Discard("not ours")

    async with client:
        for order_id in (1, 2, 3):
            await queue.send(OrderCreated(order_id=order_id))
        responses = [await invoke_sqs_handler(handler, queue, wait_time=1) for _ in range(3)]
        assert all(response == {"batchItemFailures": []} for response in responses)
        assert sum((await queue.approximate_count()).values()) == 0
        assert queue.dead_letter_queue is not None
        dead = await drain_queue(client, queue.dead_letter_queue)
    assert [message.json() for message in dead] == [{"order_id": 1, "total": 0.0}]
    info = json.loads(str(dead[0].raw_attributes["fanferro.dead-letter"].value))
    assert info["reason"] == "invalid order"
    assert info["source"].endswith(queue.name)
    # The handler resolved the queue it was handed records of, with its declared dead-letter queue.
    assert next(iter(handler.consumers.values())).queue is queue


async def test_fifo_groups_after_a_failure_are_reported_unprocessed(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(f"{name('fn')}.fifo")
    handled: list[str] = []

    async def on_message(message: fanferro.Message[dict[str, str]]) -> None:
        handled.append(message.body["n"])
        if message.body["n"] == "a2":
            raise ConnectionError("downstream unavailable")

    handler = fanferro.aws_lambda.sqs_handler(client, on_message, retry=FAST_RETRY)

    async with client:
        for n in ("a1", "a2", "a3", "b1", "b2"):
            await queue.send({"n": n}, group_id=n[0])
        event: dict[str, Any] = {"Records": []}
        for _ in range(10):
            event["Records"].extend((await receive_sqs_event(queue, wait_time=1))["Records"])
            if len(event["Records"]) == 5:
                break
        assert len(event["Records"]) == 5
        response = await handler.handle(event)
    by_id = {record["messageId"]: json.loads(record["body"])["n"] for record in event["Records"]}
    assert sorted(by_id[message_id] for message_id in _failed(response)) == ["a2", "a3"]
    assert sorted(handled) == ["a1", "a2", "b1", "b2"]


async def test_handlers_still_running_at_the_deadline_are_reported(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("fn"))
    errors: list[fanferro.Event] = []
    client.on("consumer.error", errors.append)
    handler = fanferro.aws_lambda.sqs_handler(client, timeout_margin=0.5)

    @handler.route(OrderCreated)
    async def on_order(message: fanferro.Message[OrderCreated]) -> None:
        if message.body.order_id == 2 and message.attempt == 1:
            await asyncio.sleep(30)

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await queue.send(OrderCreated(order_id=2))
        event = await receive_sqs_event(queue, wait_time=1)
        while len(event["Records"]) < 2:
            event["Records"].extend((await receive_sqs_event(queue, wait_time=1))["Records"])
        response = await handler.handle(event, _Context(remaining=1.5))
        slow = next(r["messageId"] for r in event["Records"] if json.loads(r["body"])["order_id"] == 2)
        assert _failed(response) == [slow]
        # The cancelled handler's message was released; the next invocation of the warm handler
        # starts afresh and processes it.
        core = await client._core()
        await core.delete_records(queue.url, [r["receiptHandle"] for r in event["Records"] if r["messageId"] != slow])
        assert await invoke_sqs_handler(handler, queue, wait_time=1) == {"batchItemFailures": []}
        assert sum((await queue.approximate_count()).values()) == 0
    assert any("cancelled 1 unfinished" in event.message for event in errors)


async def test_without_batch_item_failures_a_failure_fails_the_invocation(
    client_factory: ClientFactory, name: Any
) -> None:
    client = client_factory()
    queue = client.queue(name("fn"))

    async def handle(order: OrderCreated) -> None:
        raise ConnectionError("downstream unavailable")

    handler = fanferro.aws_lambda.sqs_handler(client, handle, report_batch_item_failures=False, retry=FAST_RETRY)
    async with client:
        await queue.send(OrderCreated(order_id=1))
        event = await receive_sqs_event(queue, wait_time=1)
        with pytest.raises(fanferro.BatchItemFailuresError) as raised:
            await handler.handle(event)
    assert raised.value.item_identifiers == [event["Records"][0]["messageId"]]
