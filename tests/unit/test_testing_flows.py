"""fanferro.testing flow helpers: wait_for, consume_until, drain, record_published, peek, pending run_until_idle."""

from __future__ import annotations

import asyncio
import dataclasses
import time

import pytest

import fanferro
from fanferro import AttributeValue
from fanferro.messages import PreparedMessage
from fanferro.middleware import PrepareNext, PublishContext
from fanferro.testing import (
    PublishedMessage,
    consume_until,
    drain,
    memory_client,
    peek,
    receive_sqs_event,
    record_published,
    wait_for,
)


@fanferro.message_type("tests.flows.order.v1")
@dataclasses.dataclass
class Order:
    id: int


# -- wait_for and consume_until -------------------------------------------------------------------


async def test_wait_for_returns_once_the_condition_holds() -> None:
    calls = 0

    def third_time() -> bool:
        nonlocal calls
        calls += 1
        return calls == 3

    await wait_for(third_time, interval=0)
    assert calls == 3


async def test_wait_for_awaits_async_conditions() -> None:
    client = memory_client()
    queue = client.queue("waited")

    async def has_messages() -> bool:
        return (await queue.approximate_count()).visible > 0

    async with client:
        waiting = asyncio.ensure_future(wait_for(has_messages, timeout=5))
        await asyncio.sleep(0.1)
        assert not waiting.done()
        await queue.send(Order(1))
        await waiting


async def test_wait_for_times_out() -> None:
    started = time.monotonic()
    with pytest.raises(TimeoutError, match=r"within 0\.2 seconds"):
        await wait_for(lambda: False, timeout=0.2, interval=0.01)
    assert time.monotonic() - started >= 0.2


async def test_consume_until_stops_the_consumer_also_when_it_times_out() -> None:
    client = memory_client()
    queue = client.queue("until")
    seen: list[Order] = []

    async def handle(order: Order) -> None:
        seen.append(order)

    consumer = client.consumer(queue, handle, wait_time=1)
    async with client:
        await queue.send_batch([Order(1), Order(2)])
        await consume_until(consumer, lambda: len(seen) == 2, timeout=10)
        assert consumer.state == "stopped"
        assert sorted(order.id for order in seen) == [1, 2]

        with pytest.raises(TimeoutError):
            await consume_until(consumer, lambda: len(seen) == 3, timeout=0.3)
        assert consumer.state == "stopped"


# -- drain -------------------------------------------------------------------------------------


async def test_drain_returns_and_acks_everything_in_a_queue() -> None:
    client = memory_client()
    queue = client.queue("drained")
    async with client:
        await queue.send_batch([Order(1), Order(2), Order(3)], attributes={"tenant": "acme"})
        messages = await drain(queue)
        assert sorted(message.json()["id"] for message in messages) == [1, 2, 3]
        assert {message.type for message in messages} == {"tests.flows.order.v1"}
        assert all(message.attribute("tenant") == "acme" for message in messages)
        assert all(message.outcome == "acked" for message in messages)
        assert (await queue.approximate_count()).total == 0
        assert await drain(queue) == []


async def test_drain_of_a_dead_letter_queue() -> None:
    client = memory_client()
    queue = client.queue("failing", dead_letter=fanferro.DeadLetterQueue("failing-dlq"))

    async def handle(order: Order) -> None:
        raise fanferro.Reject("invalid order")

    consumer = client.consumer(queue, handle, wait_time=1)
    async with client:
        await queue.send(Order(7))
        await consumer.run_until_idle()
        assert queue.dead_letter_queue is not None
        [dead] = await drain(queue.dead_letter_queue)
    assert dead.json() == {"id": 7}
    assert dead.dead_letter_info is not None
    assert dead.dead_letter_info.reason == "invalid order"


async def test_drain_with_pending_waits_for_delayed_messages() -> None:
    client = memory_client()
    queue = client.queue("drained-later")
    async with client:
        await queue.send(Order(1), delay=1)
        assert await drain(queue) == []
        [message] = await drain(queue, pending=True, timeout=10)
    assert message.json() == {"id": 1}


# -- run_until_idle(pending=True) ---------------------------------------------------------------


async def test_run_until_idle_with_pending_waits_for_retries() -> None:
    client = memory_client()
    queue = client.queue("retried", dead_letter=fanferro.DeadLetterQueue("retried-dlq"))
    attempts: list[int] = []

    async def handle(message: fanferro.Message[Order]) -> None:
        attempts.append(message.attempt)
        if message.attempt < 3:
            raise ConnectionError("downstream unavailable")

    retry = fanferro.RetryPolicy(backoff=fanferro.Backoff.constant(0.5))
    async with client:
        await queue.send(Order(1))
        # Without pending, it returns while the message waits for its retry.
        await client.consumer(queue, handle, wait_time=1, retry=retry).run_until_idle()
        assert attempts == [1]
        [waiting] = await peek(queue)
        assert waiting.in_flight
        assert waiting.receive_count == 1

        await client.consumer(queue, handle, wait_time=1, retry=retry).run_until_idle(pending=True, timeout=15)
        assert attempts == [1, 2, 3]
        assert await peek(queue) == []


async def test_run_until_idle_with_pending_ends_with_dead_lettered_messages() -> None:
    client = memory_client()
    queue = client.queue("exhausted", dead_letter=fanferro.DeadLetterQueue("exhausted-dlq"))

    async def handle(order: Order) -> None:
        raise ConnectionError("downstream unavailable")

    retry = fanferro.RetryPolicy(max_attempts=2, backoff=fanferro.Backoff.constant(0))
    consumer = client.consumer(queue, handle, wait_time=1, retry=retry)
    async with client:
        await queue.send(Order(1))
        await consumer.run_until_idle(pending=True, timeout=15)
        assert consumer.stats().failed == 2
        assert queue.dead_letter_queue is not None
        [dead] = await peek(queue.dead_letter_queue)
    assert dead.dead_letter_info is not None
    assert dead.dead_letter_info.attempt == 2


async def test_run_until_idle_with_pending_times_out_on_messages_held_elsewhere() -> None:
    client = memory_client()
    queue = client.queue("held")
    seen: list[Order] = []

    async def handle(order: Order) -> None:
        seen.append(order)

    async with client:
        await queue.send(Order(1))
        held = await receive_sqs_event(queue, visibility_timeout=30)
        assert len(held["Records"]) == 1
        consumer = client.consumer(queue, handle, wait_time=1)
        with pytest.raises(TimeoutError):
            await consumer.run_until_idle(pending=True, idle_for=0.1, timeout=0.5)
        assert consumer.state == "stopped"
    assert seen == []


# -- record_published ----------------------------------------------------------------------------


async def test_record_published_records_every_way_of_publishing() -> None:
    client = memory_client()
    orders = client.topic("orders")
    jobs = client.queue("jobs")
    async with client:
        with record_published(client) as published:
            await orders.publish(Order(1), attributes={"tenant": "acme"}, headers={"region": "eu"})
            await jobs.send(Order(2), delay=1)
            await jobs.send_batch([Order(3), {"raw": True}])
            prepared = await orders.prepare(Order(4), subject="order-4")
            await client.publish_prepared(prepared)
        await orders.publish(Order(5))

    assert len(published) == 5
    assert published.bodies() == [Order(1), Order(2), Order(3), {"raw": True}, Order(4)]
    assert published.bodies(orders) == [Order(1), Order(4)]
    assert published.bodies("jobs") == [Order(2), Order(3), {"raw": True}]
    first = published[0]
    assert isinstance(first, PublishedMessage)
    assert (first.destination, first.kind, first.type) == ("orders", "topic", "tests.flows.order.v1")
    assert first.attributes == {"tenant": "acme"}
    assert first.headers == {"region": "eu"}
    assert first.id == first.prepared.message_id
    assert "tenant" in first.prepared.wire.attributes
    assert published[1].delay == 1.0
    assert published[3].type is None
    assert [message.id for message in published.to(orders)][1] == prepared.message_id
    assert not published.recording
    assert client.publish_middleware == []


async def test_record_published_sees_what_other_middleware_did() -> None:
    async def add_tenant(context: PublishContext, call_next: PrepareNext) -> PreparedMessage:
        context.message.attributes["tenant"] = "acme"
        return await call_next(context)

    client = memory_client(publish_middleware=[add_tenant], topic_prefix="staging-", queue_prefix="staging-")
    orders = client.topic("orders.fifo")
    jobs = client.queue("orders")
    published = record_published(client)
    async with client:
        await orders.publish(Order(1), group_id="customer-1", deduplication_id="d-1")
        await jobs.send(Order(2))
    published.stop()
    assert published.recording is False
    assert client.publish_middleware == [add_tenant]

    [fifo] = published.to(orders)
    assert fifo.destination == "staging-orders.fifo"
    assert (fifo.group_id, fifo.deduplication_id) == ("customer-1", "d-1")
    assert fifo.attributes == {"tenant": "acme"}
    # Names match with or without the prefix; a topic and a queue of the same name are told apart.
    assert published.bodies("orders.fifo") == published.bodies("staging-orders.fifo") == [Order(1)]
    assert published.bodies(jobs) == [Order(2)]

    published.clear()
    assert len(published) == 0
    published.start()
    published.start()
    assert client.publish_middleware.count(published._record) == 1
    async with client:
        await jobs.send(Order(3))
    assert [message.body for message in published] == [Order(3)]
    assert repr(published) == "PublishRecorder(messages=1, recording=True)"


async def test_record_published_keeps_messages_whose_encoding_failed_out() -> None:
    client = memory_client()
    jobs = client.queue("strict")
    async with client:
        with record_published(client) as published, pytest.raises(fanferro.ValidationError):
            await jobs.send(Order(1), attributes={"fanferro.secret": "x"})
    assert len(published) == 0


# -- peek --------------------------------------------------------------------------------------


async def test_peek_shows_messages_without_receiving_them() -> None:
    client = memory_client()
    queue = client.queue("peeked.fifo")
    async with client:
        await queue.send(Order(1), group_id="a", attributes={"tenant": "acme"})
        await queue.send(Order(2), group_id="b")
        first, second = await peek(queue)
        assert await peek(queue) == [first, second]

    assert first.body == '{"id":1}'
    assert first.type == "tests.flows.order.v1"
    assert first.attributes == {"tenant": AttributeValue.string("acme")}
    assert "fanferro.type" in first.raw_attributes
    assert (first.group_id, second.group_id) == ("a", "b")
    assert first.receive_count == 0
    assert first.visible
    assert not first.in_flight
    assert not first.delayed
    assert first.dead_letter_info is None


async def test_peek_needs_the_memory_broker() -> None:
    client = fanferro.Client(endpoint_url="http://localhost:1", region="us-east-1")
    with pytest.raises(TypeError, match="memory_client"):
        await peek(client.queue("elsewhere"))
