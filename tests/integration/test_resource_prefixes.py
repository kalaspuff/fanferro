"""Environment prefixes for queue and topic names, on the memory broker and MiniStack."""

from __future__ import annotations

from typing import Any

import pytest

import fanferro
from fanferro.outbox import InMemoryOutbox, OutboxRelay
from tests.conftest import ClientFactory
from tests.helpers import OrderCreated, consume_until, drain_queue

STAGING = {"queue_prefix": "staging-", "topic_prefix": "staging-"}


async def test_publish_subscribe_and_consume_with_prefixes(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory(**STAGING)
    base = name("orders")
    topic = client.topic(base)
    client.subscription(base, f"{base}-worker")
    consumer = client.consumer(f"{base}-worker", wait_time=1)
    received: list[OrderCreated] = []

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        received.append(order)

    async with client:
        result = await topic.publish(OrderCreated(order_id=1))
        await consume_until(consumer, lambda: len(received) == 1)
    assert received == [OrderCreated(order_id=1)]
    assert result.destination == f"staging-{base}"
    assert topic.arn.endswith(f":staging-{base}")
    worker = consumer.queue
    assert worker is client.queues[f"staging-{base}-worker"]
    assert worker.url.endswith(f"/staging-{base}-worker")
    assert worker.last_report is not None
    assert worker.last_report.name == f"staging-{base}-worker"
    assert consumer.name == f"staging-{base}-worker"
    # Only the prefixed resources exist: nothing was declared under the logical names.
    other = client_factory(provisioning=fanferro.Provisioning.existing())
    assert (await other.topic(f"staging-{base}").resolve()).arn == topic.arn
    for missing in (other.topic(base), other.queue(f"{base}-worker")):
        with pytest.raises(fanferro.ProvisioningError, match="does not exist"):
            await missing.resolve()


async def test_failures_dead_letter_into_the_prefixed_queue(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory(**STAGING)
    base = name("poison")
    queue = client.queue(base, dead_letter=fanferro.DeadLetterQueue(f"{base}-dlq", max_receive_count=50))
    consumer = client.consumer(base, wait_time=1, retry=fanferro.RetryPolicy(max_attempts=1))
    dead: list[fanferro.Event] = []
    client.on("message.dead_lettered", dead.append)

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        raise ValueError(f"cannot process {order.order_id}")

    async with client:
        await queue.send(OrderCreated(order_id=9))
        await consume_until(consumer, lambda: len(dead) == 1)
    assert queue.dead_letter_queue is not None
    assert queue.dead_letter_queue.name == f"staging-{base}-dlq"
    assert dead[0].fields["dead_letter_queue"].endswith(f"/staging-{base}-dlq")
    redrive = (await queue.resolve()).redrive_policy
    assert redrive is not None
    assert redrive["deadLetterTargetArn"].endswith(f":staging-{base}-dlq")
    plain = client_factory()  # no prefix: finds the dead-letter queue by its AWS name
    async with plain:
        messages = await drain_queue(plain, plain.queue(f"staging-{base}-dlq"))
    assert [message.json()["order_id"] for message in messages] == [9]
    info = messages[0].dead_letter_info
    assert info is not None
    assert info["queue"] == f"staging-{base}"


async def test_outbox_relay_publishes_to_the_prefixed_destination(client_factory: ClientFactory, name: Any) -> None:
    publisher = client_factory(**STAGING)
    base = name("outbox")
    topic = publisher.topic(base)
    queue = publisher.queue(f"{base}-worker")
    publisher.subscription(topic, queue)
    outbox = InMemoryOutbox()
    async with publisher:
        prepared = await topic.prepare(OrderCreated(order_id=3))
        outbox.add(fanferro.PreparedMessage.from_json(prepared.to_json()))
        relay_client = client_factory(**STAGING)  # the relay process, configured like the publisher
        assert relay_client.topic(base).name == prepared.destination_name
        async with relay_client:
            assert await OutboxRelay(relay_client, outbox).run_once() == 1
        messages = await drain_queue(publisher, queue)
    assert prepared.destination_name == f"staging-{base}"
    assert prepared.destination["arn"].endswith(f":staging-{base}")
    assert [message.json()["order_id"] for message in messages] == [3]


async def test_environments_with_different_prefixes_are_isolated(client_factory: ClientFactory, name: Any) -> None:
    base = name("orders")
    dev = client_factory(queue_prefix="dev-", topic_prefix="dev-")
    staging = client_factory(**STAGING)
    for client in (dev, staging):
        client.subscription(base, f"{base}-worker")
    async with dev, staging:
        await dev.topic(base).publish(OrderCreated(order_id=1))
        await staging.topic(base).publish(OrderCreated(order_id=2))
        await staging.queue(f"{base}-worker").send(OrderCreated(order_id=3))
        dev_messages = await drain_queue(dev, dev.queue(f"{base}-worker"))
        staging_messages = await drain_queue(staging, staging.queue(f"{base}-worker"))
    assert [message.json()["order_id"] for message in dev_messages] == [1]
    assert sorted(message.json()["order_id"] for message in staging_messages) == [2, 3]
    assert dev.queue(f"{base}-worker").name == f"dev-{base}-worker"
    assert staging.queue(f"{base}-worker").name == f"staging-{base}-worker"
