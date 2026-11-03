"""Batch routes: handlers that process several messages in one call (fanferro.batching)."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

import fanferro
from fanferro.testing import consume_until, drain, wait_for
from tests.conftest import ClientFactory
from tests.helpers import OrderCreated

FAST_RETRY = fanferro.RetryPolicy(max_attempts=5, backoff=fanferro.Backoff.constant(0))


def queue_with_dlq(client: fanferro.Client, base: str) -> fanferro.Queue:
    return client.queue(base, dead_letter=fanferro.DeadLetterQueue(f"{base}-dlq", max_receive_count=50))


def orders(*ids: int) -> list[OrderCreated]:
    return [OrderCreated(order_id=i) for i in ids]


async def test_batches_fill_up_to_max_size(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("by-size"))
    consumer = client.consumer(queue, wait_time=1)
    batches: list[list[int]] = []

    @consumer.route(OrderCreated, batch=fanferro.Batch(max_size=10, max_wait=1))
    async def store(messages: list[fanferro.Message[OrderCreated]]) -> None:
        batches.append([message.body.order_id for message in messages])

    async with client:
        await queue.send_batch(orders(*range(25)))
        await consume_until(consumer, lambda: sum(map(len, batches)) == 25)
        counts = await queue.approximate_count()
    assert sorted(i for batch in batches for i in batch) == list(range(25))
    assert all(len(batch) <= 10 for batch in batches)
    assert max(map(len, batches)) == 10
    assert counts.visible == counts.in_flight == 0
    stats = consumer.stats()
    assert stats.processed == 25
    assert stats.engine.messages_acked == 25


async def test_batches_wait_at_most_max_wait(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("by-time"))
    consumer = client.consumer(queue, wait_time=1)
    batches: list[tuple[float, list[int]]] = []

    @consumer.route(batch=fanferro.Batch(max_size=100, max_wait=0.5))
    async def store(bodies: list[OrderCreated]) -> None:
        batches.append((time.monotonic(), [order.order_id for order in bodies]))

    async with client:
        await consumer.start()
        # Without an explicit prefetch, a full batch fits in flight.
        assert consumer.concurrency + consumer.prefetch >= 100
        await queue.send_batch(orders(1, 2, 3))
        sent = time.monotonic()
        await consume_until(consumer, lambda: sum(len(ids) for _, ids in batches) == 3)
    assert sorted(i for _, ids in batches for i in ids) == [1, 2, 3]
    if client.broker is not None:
        # The in-memory broker hands the three over in one receive.
        assert [ids for _, ids in batches] == [[1, 2, 3]]
    assert batches[0][0] - sent >= 0.4, "the batch was handed over before max_wait"


async def test_partial_failures_settle_each_message(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = queue_with_dlq(client, name("partial"))
    consumer = client.consumer(queue, wait_time=1, retry=FAST_RETRY)
    calls: list[list[tuple[int, int]]] = []
    failed: list[fanferro.Event] = []
    client.on("message.failed", failed.append)

    @consumer.route(OrderCreated, batch=fanferro.Batch(max_size=4, max_wait=0.5))
    async def store(messages: list[fanferro.Message[OrderCreated]]) -> fanferro.BatchResult:
        calls.append([(message.body.order_id, message.attempt) for message in messages])
        result = fanferro.BatchResult()
        for message in messages:
            if message.body.order_id == 2 and message.attempt == 1:
                result.fail(message)  # retried
            elif message.body.order_id == 3:
                result.fail(message, fanferro.Reject("invalid order"))
        return result

    async with client:
        await queue.send_batch(orders(1, 2, 3, 4))
        await consume_until(
            consumer,
            lambda: consumer.stats().processed == 3 and consumer.stats().engine.messages_dead_lettered == 1,
        )
        assert queue.dead_letter_queue is not None
        dead = await drain(queue.dead_letter_queue)
    handled = sorted(item for batch in calls for item in batch)
    assert handled == [(1, 1), (2, 1), (2, 2), (3, 1), (4, 1)]
    assert [message.json()["order_id"] for message in dead] == [3]
    assert {event.fields["error_type"] for event in failed} == {"BatchItemFailedError", "Reject"}
    stats = consumer.stats()
    assert stats.failed == 2
    assert stats.engine.messages_acked == 3
    assert stats.engine.messages_dead_lettered == 1


async def test_a_raising_handler_retries_every_message(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = queue_with_dlq(client, name("raising"))
    consumer = client.consumer(queue, wait_time=1, retry=FAST_RETRY)
    attempts: dict[int, list[int]] = {}

    @consumer.route(OrderCreated, batch=fanferro.Batch(max_size=3, max_wait=0.5))
    async def store(messages: list[fanferro.Message[OrderCreated]]) -> None:
        for message in messages:
            attempts.setdefault(message.body.order_id, []).append(message.attempt)
        if any(message.attempt == 1 for message in messages):
            raise ConnectionError("database unavailable")

    async with client:
        await queue.send_batch(orders(1, 2, 3))
        await consume_until(consumer, lambda: consumer.stats().processed == 3)
    assert attempts == {1: [1, 2], 2: [1, 2], 3: [1, 2]}
    assert consumer.stats().failed == 3


async def test_messages_settled_by_the_handler_are_left_alone(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = queue_with_dlq(client, name("explicit"))
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(OrderCreated, batch=fanferro.Batch(max_size=3, max_wait=0.5))
    async def store(messages: list[fanferro.Message[OrderCreated]]) -> None:
        await asyncio.gather(*(message.dead_letter("invalid") for message in messages if message.body.order_id == 2))

    async with client:
        await queue.send_batch(orders(1, 2, 3))
        await consume_until(consumer, lambda: consumer.stats().processed == 3)
        assert queue.dead_letter_queue is not None
        dead = await drain(queue.dead_letter_queue)
    assert [message.json()["order_id"] for message in dead] == [2]
    assert consumer.stats().engine.messages_acked == 2


async def test_fifo_batches_take_the_next_message_of_each_group(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(f"{name('fifo-batches')}.fifo")
    consumer = client.consumer(queue, wait_time=1)
    batches: list[list[tuple[str, int]]] = []

    @consumer.route(OrderCreated, batch=fanferro.Batch(max_size=10, max_wait=0.2))
    async def store(messages: list[fanferro.Message[OrderCreated]]) -> None:
        batches.append([(message.group_id or "", message.body.order_id) for message in messages])

    async with client:
        await queue.send_batch(
            [
                fanferro.OutgoingMessage(body=OrderCreated(order_id=i), group_id=f"g{i % 3}", id=f"id-{i}")
                for i in range(9)
            ]
        )
        await consume_until(consumer, lambda: sum(map(len, batches)) == 9)
    for batch in batches:
        groups = [group for group, _ in batch]
        assert len(groups) == len(set(groups)), f"a batch holds two messages of one group: {batch}"
    for group in ("g0", "g1", "g2"):
        ids = [i for batch in batches for g, i in batch if g == group]
        assert ids == sorted(ids)
        assert len(ids) == 3


async def test_concurrency_counts_batch_calls(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("calls"))
    consumer = client.consumer(queue, wait_time=1, concurrency=1)
    running = 0
    most = 0
    sizes: list[int] = []

    @consumer.route(OrderCreated, batch=fanferro.Batch(max_size=5, max_wait=0.1))
    async def store(messages: list[fanferro.Message[OrderCreated]]) -> None:
        nonlocal running, most
        running += 1
        most = max(most, running)
        sizes.append(len(messages))
        await asyncio.sleep(0.2)
        running -= 1

    async with client:
        await queue.send_batch(orders(*range(10)))
        await queue.send_batch(orders(*range(10, 20)))
        await consume_until(consumer, lambda: sum(sizes) == 20)
    assert most == 1
    assert max(sizes) <= 5
    if client.broker is not None:
        assert 5 in sizes  # receives of the in-memory broker fill a batch at once


async def test_an_explicit_prefetch_must_fit_the_batch(memory_client: fanferro.Client, name: Any) -> None:
    consumer = memory_client.consumer(memory_client.queue(name("small")), concurrency=2, prefetch=3)

    @consumer.route(OrderCreated, batch=fanferro.Batch(max_size=10))
    async def store(messages: list[fanferro.Message[OrderCreated]]) -> None: ...

    with pytest.raises(fanferro.ConfigurationError, match="raise prefetch to at least 8"):
        await consumer.start()
    assert consumer.state == "stopped"


async def test_handler_timeout_applies_to_the_batch(memory_client: fanferro.Client, name: Any) -> None:
    client = memory_client
    queue = client.queue(name("slow"))
    consumer = client.consumer(queue, wait_time=1, handler_timeout=0.3, retry=FAST_RETRY)
    calls: list[int] = []

    @consumer.route(OrderCreated, batch=fanferro.Batch(max_size=2, max_wait=5))
    async def store(messages: list[fanferro.Message[OrderCreated]]) -> None:
        calls.append(len(messages))
        if len(calls) == 1:
            await asyncio.sleep(5)

    failed: list[fanferro.Event] = []
    client.on("message.failed", failed.append)
    async with client:
        await queue.send_batch(orders(1, 2))
        await consume_until(consumer, lambda: consumer.stats().processed == 2)
    assert calls == [2, 2]
    assert [event.fields["error_type"] for event in failed] == ["HandlerTimeoutError"] * 2


@pytest.mark.parametrize("release_prefetched", [True, False])
async def test_stopping_with_a_partial_batch(
    memory_client: fanferro.Client, name: Any, release_prefetched: bool
) -> None:
    client = memory_client
    queue = client.queue(name("partial-batch"))
    shutdown = fanferro.ShutdownPolicy(release_prefetched=release_prefetched)
    consumer = client.consumer(queue, wait_time=1, shutdown=shutdown)
    batches: list[int] = []

    @consumer.route(OrderCreated, batch=fanferro.Batch(max_size=100, max_wait=60))
    async def store(messages: list[fanferro.Message[OrderCreated]]) -> None:
        batches.append(len(messages))

    async with client:
        await queue.send_batch(orders(1, 2, 3))
        await consumer.start()
        await wait_for(lambda: len(consumer._batchers) == 1 and len(next(iter(consumer._batchers.values()))) == 3)
        started = time.monotonic()
        await consumer.stop()
        assert time.monotonic() - started < 5
        counts = await queue.approximate_count()
    if release_prefetched:
        # Collected messages never reached the handler: they are back in the queue.
        assert batches == []
        assert counts.visible == 3
    else:
        # Stopping hands the batch being collected to the handler.
        assert batches == [3]
        assert counts.visible == counts.in_flight == 0


async def test_collected_messages_keep_their_leases(memory_client: fanferro.Client, name: Any) -> None:
    client = memory_client
    queue = client.queue(name("leases"))
    # Visibility of 2 s: heartbeats keep collected messages invisible while they wait.
    consumer = client.consumer(queue, wait_time=1, visibility_timeout=2)
    batches: list[list[int]] = []

    @consumer.route(OrderCreated, batch=fanferro.Batch(max_size=3, max_wait=3.5))
    async def store(messages: list[fanferro.Message[OrderCreated]]) -> None:
        batches.append([message.receive_count for message in messages])

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await consume_until(consumer, lambda: len(batches) == 1, timeout=10)
        counts = await queue.approximate_count()
    assert batches == [[1]]
    assert counts.visible == counts.in_flight == 0


async def test_lambda_records_form_one_batch_at_once(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("batch-fn"))
    handler = fanferro.aws_lambda.sqs_handler(client, queue=queue, retry=FAST_RETRY)
    batches: list[list[int]] = []

    @handler.route(OrderCreated, batch=fanferro.Batch(max_size=10, max_wait=60))
    async def store(messages: list[fanferro.Message[OrderCreated]]) -> fanferro.BatchResult:
        batches.append([message.body.order_id for message in messages])
        return fanferro.BatchResult(message for message in messages if message.body.order_id == 2)

    async with client:
        sent = await queue.send_batch(orders(1, 2, 3))
        event: dict[str, Any] = {"Records": []}
        for _ in range(20):
            more = await fanferro.testing.receive_sqs_event(queue, wait_time=1)
            event["Records"].extend(more["Records"])
            if len(event["Records"]) == 3:
                break
        started = time.monotonic()
        response = await handler.handle(event, None)
        assert time.monotonic() - started < 5, "the batch waited for max_wait"
    assert [sorted(batch) for batch in batches] == [[1, 2, 3]]
    failing = sent.successful[1]
    assert [item["itemIdentifier"] for item in response["batchItemFailures"]] == [failing.transport_message_id]
