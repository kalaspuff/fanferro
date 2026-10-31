from __future__ import annotations

import asyncio
import json
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


async def test_a_cancelled_resolution_is_not_a_failure(memory_client: fanferro.Client, name: Any) -> None:
    # A publish cancelled while its queue was being looked up (a request timeout, a client that went
    # away): the next publishes must look the queue up again instead of failing for a while.
    queue = memory_client.queue(name("cancelled"), provisioning=fanferro.Provisioning.create_missing())
    lookup = queue._provision
    looking_up = asyncio.Event()

    async def slow_lookup() -> Any:
        looking_up.set()
        await asyncio.sleep(0.5)
        return await lookup()

    queue._provision = slow_lookup  # type: ignore[method-assign]
    sending = asyncio.ensure_future(queue.send({"n": 1}))
    await looking_up.wait()
    sending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sending
    await queue.send({"n": 2})
    assert (await queue.approximate_count())["visible"] == 1


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


async def test_resources_create_their_own_payload_buckets(client_factory: ClientFactory, name: Any) -> None:
    # Only the client's LargePayloads created its bucket: a queue's own one with create_bucket=True
    # failed its first send with NoSuchBucket.
    client = client_factory()
    topic = client.topic(name("big"), large_payloads=fanferro.LargePayloads(name("topic-payloads"), create_bucket=True))
    shared = fanferro.LargePayloads(name("queue-payloads"), always=True, create_bucket=True)
    queue = client.queue(name("big-worker"), large_payloads=shared)
    other = client.queue(name("big-other"), large_payloads=shared)
    client.subscription(topic, queue)
    async with client:
        sent = await queue.send({"n": 1})
        assert sent.offloaded is not None
        assert sent.offloaded.bucket == shared.bucket
        assert (await other.send({"n": 2})).offloaded is not None
        assert (await topic.publish({"blob": "x" * 300_000})).offloaded is not None
        bodies = [message.json() for message in await drain_queue(client, queue)]
    assert sorted(bodies, key=lambda body: "blob" in body) == [{"n": 1}, {"blob": "x" * 300_000}]


async def test_provisioning_a_queue_creates_its_payload_bucket(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory(auto_provision=False)
    queue = client.queue(
        name("jobs"), large_payloads=fanferro.LargePayloads(name("jobs-payloads"), always=True, create_bucket=True)
    )
    async with client:
        await client.provision(queue)
        assert (await queue.send({"n": 1})).offloaded is not None
        assert [message.json() for message in await drain_queue(client, queue)] == [{"n": 1}]


async def test_queues_are_created_again_and_purged_again_only_after_a_minute(
    memory_client: fanferro.Client, name: Any
) -> None:
    # docs/parity Q7, Q8: SQS refuses both for 60 seconds, and retrying later succeeds. MiniStack
    # does neither, so this runs on the in-memory broker only.
    queue = memory_client.queue(name("phoenix"), provisioning=fanferro.Provisioning.ensure())
    await queue.purge()
    with pytest.raises(fanferro.ThrottledError) as purged:
        await queue.purge()
    assert purged.value.code == "AWS.SimpleQueueService.PurgeQueueInProgress"
    assert purged.value.status == 403
    assert purged.value.retryable
    await queue.delete()
    with pytest.raises(fanferro.ProvisioningError) as created:
        await queue.send({"n": 1})
    error = created.value.report.error
    assert isinstance(error, fanferro.ThrottledError)
    assert error.code == "AWS.SimpleQueueService.QueueDeletedRecently"
    assert created.value.retryable


async def test_subscribing_a_queue_encrypted_with_the_aws_managed_key_warns(
    memory_client: fanferro.Client, name: Any
) -> None:
    # docs/parity S8: SNS cannot use the AWS managed key of SQS and drops the messages silently.
    # MiniStack delivers them, so this runs on the in-memory broker only.
    warnings: list[fanferro.Event] = []
    memory_client.on("config.warning", warnings.append)
    topic = memory_client.topic(name("orders"))
    managed = memory_client.queue(
        name("managed"), attributes=fanferro.QueueAttributes(encryption=fanferro.Kms("alias/aws/sqs"))
    )
    plain = memory_client.queue(name("plain"))
    memory_client.subscription(topic, managed)
    memory_client.subscription(topic, plain)
    async with memory_client:
        await topic.publish({"n": 1})
        assert (await managed.approximate_count())["visible"] == 0
        assert (await plain.approximate_count())["visible"] == 1
    assert [event.fields["queue"] for event in warnings] == [managed.arn]
    assert "SNS cannot deliver" in warnings[0].message


async def test_subscriptions_declared_after_entering_resolve_before_publishing(
    client_factory: ClientFactory, name: Any
) -> None:
    client = client_factory()
    topic = client.topic(name("orders"))
    async with client:
        single, batch, prepared = (client.queue(name(suffix)) for suffix in ("single", "batch", "prepared"))
        client.subscription(topic, single)
        await topic.publish({"id": 1})
        client.subscription(topic, batch)
        await topic.publish_batch([{"id": 2}, {"id": 3}])
        client.subscription(topic, prepared)
        await client.publish_prepared(await topic.prepare({"id": 4}))
        assert topic._pending_subscriptions == []
        assert [json.loads(m.body)["id"] for m in await drain_queue(client, single)] == [1, 2, 3, 4]
        assert sorted(json.loads(m.body)["id"] for m in await drain_queue(client, batch)) == [2, 3, 4]
        assert [json.loads(m.body)["id"] for m in await drain_queue(client, prepared)] == [4]


async def test_an_unavailable_late_subscription_stays_pending(client_factory: ClientFactory, name: Any) -> None:
    events: list[fanferro.Event] = []
    client = client_factory()
    client.on("resource.missing", events.append)
    topic = client.topic(name("orders"))
    async with client:
        queue = client.queue(name("work"))
        subscription = client.subscription(topic, queue, provisioning=fanferro.Provisioning(on_missing="log"))
        await topic.publish({"id": 1})  # the subscription is missing: logged, the publish goes ahead
        assert len(events) == 1
        assert topic._pending_subscriptions == [subscription]
        await client_factory().subscription(topic.name, queue.name).resolve()
        await subscription.resolve(force=True)
        await topic.publish({"id": 2})
        assert topic._pending_subscriptions == []
        assert [json.loads(m.body)["id"] for m in await drain_queue(client, queue)] == [2]


async def test_subscriptions_declared_without_provisioning_are_not_resolved_by_publishing(
    memory_client: fanferro.Client, name: Any
) -> None:
    topic = memory_client.topic(name("orders"))
    subscription = memory_client.subscription(topic, memory_client.queue(name("work")))
    await topic.publish({"id": 1})
    assert not subscription.resolved


async def test_a_late_subscription_that_raises_fails_the_publish(memory_client: fanferro.Client, name: Any) -> None:
    topic = memory_client.topic(name("orders"))
    async with memory_client:
        queue = memory_client.queue(name("work"))
        memory_client.subscription(topic, queue, provisioning=EXISTING)
        with pytest.raises(fanferro.ProvisioningError):
            await topic.publish({"id": 1})
