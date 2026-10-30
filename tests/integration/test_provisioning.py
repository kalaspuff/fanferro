from __future__ import annotations

import asyncio
from typing import Any

import pytest

import fanferro
from tests.conftest import ClientFactory
from tests.helpers import OrderCreated, drain_queue, wait_for

EXISTING = fanferro.Provisioning.existing()


async def test_missing_resources_raise_by_default(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory(provisioning=EXISTING)
    queue = client.queue(name("missing"))
    with pytest.raises(fanferro.ProvisioningError) as raised:
        await queue.send({"x": 1})
    assert raised.value.report.status == "missing"
    assert "does not exist" in str(raised.value)
    with pytest.raises(fanferro.ProvisioningError):
        async with client:
            pass


async def test_log_mode_and_lazy_recovery(client_factory: ClientFactory, name: Any) -> None:
    events: list[fanferro.Event] = []
    client = client_factory(provisioning=fanferro.Provisioning(on_missing="log"))
    client.on("resource.missing", events.append)
    queue_name = name("later")
    queue = client.queue(queue_name)
    async with client:
        assert len(events) == 1, "entering the client provisions declared resources"
        outcomes = await client.provision()
        assert isinstance(outcomes[0], fanferro.ResourceUnavailableError)
        assert len(events) == 2
        with pytest.raises(fanferro.ResourceUnavailableError):
            await queue.send({"x": 1})
        creator = client_factory()
        await creator.queue(queue_name).resolve()
        info = await queue.resolve(force=True)
        assert info.name == queue_name
        await queue.send({"x": 1})


async def test_fallback(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory(provisioning=EXISTING)
    fallback_queue = client.queue(name("fallback"), provisioning=fanferro.Provisioning.ensure())
    failures: list[fanferro.ProvisioningFailure] = []

    async def use_fallback(failure: fanferro.ProvisioningFailure) -> fanferro.QueueInfo:
        failures.append(failure)
        return await fallback_queue.resolve()

    primary = client.queue(
        name("primary"), provisioning=fanferro.Provisioning(on_missing=fanferro.Fallback(use_fallback))
    )
    async with client:
        await primary.send(OrderCreated(order_id=1))
        messages = await drain_queue(client, fallback_queue)
    assert failures[0].reason == "missing"
    assert [m.json()["order_id"] for m in messages] == [1]


async def test_drift_detection_and_repair(client_factory: ClientFactory, name: Any) -> None:
    queue_name = name("drift")
    owner = client_factory()
    await owner.queue(queue_name, attributes=fanferro.QueueAttributes(visibility_timeout=30)).resolve()

    drifted: list[fanferro.Event] = []
    observer = client_factory(provisioning=fanferro.Provisioning(on_drift="log"))
    observer.on("resource.drift", drifted.append)
    info = await observer.queue(queue_name, attributes=fanferro.QueueAttributes(visibility_timeout=45)).resolve()
    assert info.visibility_timeout == 30
    assert len(drifted) == 1
    assert "VisibilityTimeout" in str(drifted[0].fields["drift"])

    strict = client_factory(provisioning=fanferro.Provisioning(on_drift="raise"))
    with pytest.raises(fanferro.ProvisioningError) as raised:
        await strict.queue(queue_name, attributes=fanferro.QueueAttributes(visibility_timeout=45)).resolve()
    assert raised.value.report.drift[0].attribute == "VisibilityTimeout"

    fixer = client_factory(provisioning=fanferro.Provisioning.ensure())
    fixed = await fixer.queue(queue_name, attributes=fanferro.QueueAttributes(visibility_timeout=45)).resolve()
    assert fixed.visibility_timeout == 45
    queue = fixer.queue(queue_name)
    assert queue.last_report is not None
    assert queue.last_report.status == "updated"


async def test_dead_letter_queue_wiring(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("work"), dead_letter=fanferro.DeadLetterQueue(f"{name('work')}-dlq", max_receive_count=4))
    async with client:
        info = await queue.resolve()
        assert queue.dead_letter_queue is not None
        dlq = await queue.dead_letter_queue.resolve()
    assert info.redrive_policy is not None
    assert info.redrive_policy["deadLetterTargetArn"] == dlq.arn
    assert int(info.redrive_policy["maxReceiveCount"]) == 4
    assert dlq.attributes["MessageRetentionPeriod"] == "1209600"


async def test_fifo_resources(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    topic = client.topic(
        f"{name('fifo-topic')}.fifo", attributes=fanferro.TopicAttributes(content_based_deduplication=True)
    )
    queue = client.queue(f"{name('fifo-queue')}.fifo")
    client.subscription(topic, queue)
    async with client:
        topic_info = await topic.resolve()
        assert topic_info.fifo
        assert topic_info.content_based_deduplication
        await topic.publish(OrderCreated(order_id=1), group_id="g")
        messages = await drain_queue(client, queue)
    assert messages[0].group_id == "g"
    with pytest.raises(fanferro.ConfigurationError):
        client.queue(name("mixed"), dead_letter=fanferro.DeadLetterQueue("dlq.fifo"))


async def test_assume_mode(client_factory: ClientFactory, name: Any, backend: str) -> None:
    queue_name = name("assumed")
    creator = client_factory()
    await creator.queue(queue_name).resolve()
    client = client_factory(provisioning=fanferro.Provisioning.assume())
    queue = client.queue(queue_name)
    info = await queue.resolve()
    assert queue.last_report is not None
    assert queue.last_report.status == "assumed"
    assert info.name == queue_name
    await queue.send({"x": 1})
    assert len(await drain_queue(client, queue)) == 1


async def test_assumed_subscriptions_are_not_looked_up(client_factory: ClientFactory, name: Any) -> None:
    # assume() makes no API calls: a subscription managed elsewhere (or one the credentials may not
    # list) must not fail the consumer that declares it.
    topic_name, queue_name = name("assumed-topic"), name("assumed-sub")
    creator = client_factory()
    await creator.topic(topic_name).resolve()
    await creator.queue(queue_name).resolve()
    client = client_factory(provisioning=fanferro.Provisioning.assume())
    queue = client.queue(queue_name)
    subscription = client.subscription(client.topic(topic_name), queue)

    async def handle(message: fanferro.Message[bytes]) -> None:
        pass

    consumer = client.consumer(queue, handle, wait_time=1)
    async with client:
        await consumer.start()
        await consumer.stop()
    assert subscription.last_report is not None
    assert subscription.last_report.status == "assumed"
    assert subscription.last_report.actions == ()
    with pytest.raises(fanferro.ConfigurationError, match=r"assumed .* ARN is unknown"):
        await subscription.delete()


async def test_tags_and_attributes(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(
        name("tagged"),
        attributes=fanferro.QueueAttributes(
            visibility_timeout=60, message_retention=3600, encryption=fanferro.SqsManagedEncryption()
        ),
        tags={"team": "payments"},
    )
    topic = client.topic(
        name("tagged-topic"), attributes=fanferro.TopicAttributes(display_name="Orders"), tags={"team": "payments"}
    )
    async with client:
        attributes = await queue.get_attributes()
        topic_attributes = await topic.get_attributes()
    assert attributes["VisibilityTimeout"] == "60"
    assert attributes["MessageRetentionPeriod"] == "3600"
    assert topic_attributes["DisplayName"] == "Orders"


async def test_kms_attributes(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(
        name("kms"), attributes=fanferro.QueueAttributes(encryption=fanferro.Kms("alias/aws/sqs", data_key_reuse=300))
    )
    topic = client.topic(
        name("kms-topic"), attributes=fanferro.TopicAttributes(encryption=fanferro.Kms("alias/aws/sns"))
    )
    async with client:
        attributes = await queue.get_attributes()
        topic_attributes = await topic.get_attributes()
        await queue.send({"secret": True})
        messages = await drain_queue(client, queue)
    assert attributes["KmsMasterKeyId"] == "alias/aws/sqs"
    assert attributes["KmsDataKeyReusePeriodSeconds"] == "300"
    assert topic_attributes["KmsMasterKeyId"] == "alias/aws/sns"
    assert messages[0].json() == {"secret": True}


async def test_teardown(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    topic = client.topic(name("gone"))
    queue = client.queue(name("gone-queue"))
    subscription = client.subscription(topic, queue)
    async with client:
        await subscription.delete()
        await queue.delete()
        await topic.delete()
    observer = client_factory(provisioning=fanferro.Provisioning(on_missing="ignore"))
    with pytest.raises(fanferro.ResourceUnavailableError):
        await observer.queue(queue.name).resolve()


async def test_topic_by_arn_and_duplicates(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    topic = client.topic(name("by-arn"))
    info = await topic.resolve()
    other = client_factory(provisioning=EXISTING)
    by_arn = other.topic(arn=info.arn)
    assert (await by_arn.resolve()).arn == info.arn
    assert client.topic(topic.name) is topic
    with pytest.raises(fanferro.ConfigurationError, match="already declared"):
        client.topic(topic.name, tags={"x": "y"})


async def test_queues_share_a_dead_letter_queue(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    shared = name("shared-dlq")
    first = client.queue(name("first"), dead_letter=fanferro.DeadLetterQueue(shared, max_receive_count=50))
    second = client.queue(name("second"), dead_letter=fanferro.DeadLetterQueue(shared, max_receive_count=50))

    async def fail(order: OrderCreated) -> None:
        raise ValueError("poison")

    no_retries = fanferro.RetryPolicy(max_attempts=1)
    consumers = [client.consumer(queue, fail, wait_time=1, retry=no_retries) for queue in (first, second)]
    dead: list[fanferro.Event] = []
    client.on("message.dead_lettered", dead.append)
    async with client:
        assert first.dead_letter_queue is not None
        assert second.dead_letter_queue is first.dead_letter_queue
        await first.send(OrderCreated(order_id=1))
        await second.send(OrderCreated(order_id=2))
        for consumer in consumers:
            await consumer.start()
        try:
            await wait_for(lambda: len(dead) == 2)
        finally:
            await asyncio.gather(*(consumer.stop() for consumer in consumers))
        messages = await drain_queue(client, first.dead_letter_queue)
    assert sorted(message.json()["order_id"] for message in messages) == [1, 2]
