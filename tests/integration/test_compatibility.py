"""Messages between fanferro 0.1 and the lean defaults of 0.2 (raw envelope, no MD5 checks).

fanferro 0.1 published CloudEvents with a ``content-type`` attribute by default and read raw
messages without assuming a content type; both directions must keep working (typed routing,
message ids, SNS filter policies on ``fanferro.type``).
"""

from __future__ import annotations

import asyncio
from typing import Any

import fanferro
from fanferro.envelopes import CloudEvents, CloudEventsProtobuf, Raw
from tests.conftest import ClientFactory
from tests.helpers import OrderCancelled, OrderCreated, consume_until, wait_for

# How fanferro 0.1 published and read messages by default.
LEGACY_PRODUCER: dict[str, Any] = {"envelope": CloudEvents(), "verify_md5": True}
LEGACY_CONSUMER: dict[str, Any] = {
    "accept_envelopes": [CloudEvents(), CloudEventsProtobuf(), Raw(implicit_content_type=None)],
    "verify_md5": True,
}


async def _roundtrip(
    producer: fanferro.Client, consumer_client: fanferro.Client, name: Any
) -> tuple[list[fanferro.Message[OrderCreated]], list[fanferro.Message[Any]], str]:
    topic_name, queue_name = name("compat-events"), name("compat-orders")
    topic = producer.topic(topic_name)
    producer.queue(queue_name)
    producer.subscription(topic_name, queue_name, filter_policy={"fanferro.type": ["test.order_created.v1"]})
    typed: list[fanferro.Message[OrderCreated]] = []
    untyped: list[fanferro.Message[Any]] = []
    consumer = consumer_client.consumer(consumer_client.queue(queue_name), wait_time=1)

    @consumer.route(OrderCreated)
    async def created(message: fanferro.Message[OrderCreated]) -> None:
        typed.append(message)

    @consumer.default
    async def other(message: fanferro.Message) -> None:
        untyped.append(message)

    async with producer, consumer_client:
        sent = await topic.publish(OrderCreated(order_id=1, total=2.5))
        await topic.publish(OrderCancelled(order_id=2))  # filtered out by the subscription
        await topic.publish({"untyped": True})  # no type: filtered out too
        await consumer.start()
        await wait_for(lambda: len(typed) == 1)
        await asyncio.sleep(1.0)  # anything the filter policy let through arrives meanwhile
        await consumer.stop()
    return typed, untyped, sent.message_id


async def test_legacy_cloudevents_producers_reach_default_consumers(client_factory: ClientFactory, name: Any) -> None:
    typed, untyped, message_id = await _roundtrip(client_factory(**LEGACY_PRODUCER), client_factory(), name)
    message = typed[0]
    assert message.envelope == "cloudevents"
    assert message.id == message_id
    assert message.type == "test.order_created.v1"
    assert message.body == OrderCreated(order_id=1, total=2.5)
    assert untyped == [], "the filter policy on fanferro.type still applies"


async def test_default_producers_reach_legacy_consumers(client_factory: ClientFactory, name: Any) -> None:
    typed, untyped, message_id = await _roundtrip(client_factory(), client_factory(**LEGACY_CONSUMER), name)
    message = typed[0]
    assert message.envelope == "raw"
    assert message.id == message_id
    assert message.type == "test.order_created.v1"
    assert message.body == OrderCreated(order_id=1, total=2.5)
    assert message.content_type is None, "0.1 consumers do not imply JSON; typed routes decode it anyway"
    assert untyped == []


async def test_legacy_consumers_read_untyped_json(client_factory: ClientFactory, name: Any) -> None:
    producer = client_factory()
    reader = client_factory(**LEGACY_CONSUMER)
    queue_name = name("compat-plain")
    queue = producer.queue(queue_name)
    bodies: list[Any] = []
    consumer = reader.consumer(reader.queue(queue_name), wait_time=1)

    @consumer.default
    async def handle(message: fanferro.Message) -> None:
        bodies.append(message.body)

    async with producer, reader:
        await queue.send({"order": 1})
        await queue.send([1, 2])
        await queue.send("text")
        await consume_until(consumer, lambda: len(bodies) == 3)
    assert sorted(map(repr, bodies)) == sorted(map(repr, [{"order": 1}, [1, 2], "text"]))
