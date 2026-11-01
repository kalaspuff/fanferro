from __future__ import annotations

import asyncio
import datetime
import json
import time
from typing import Any, Literal

import pytest
from google.protobuf import timestamp_pb2

import fanferro
from fanferro import matchers as m
from fanferro.envelopes import CloudEvents
from fanferro.outbox import InMemoryOutbox, OutboxRelay
from tests.conftest import ClientFactory
from tests.helpers import OrderCancelled, OrderCreated, consume_until, drain_queue


async def test_queue_roundtrip_with_metadata(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory(source="urn:service:test", envelope=fanferro.envelopes.CloudEvents())
    queue = client.queue(name("roundtrip"))
    received: list[fanferro.Message[OrderCreated]] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(OrderCreated)
    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        received.append(message)

    async with client:
        result = await queue.send(
            OrderCreated(order_id=1, total=9.5),
            attributes={"tenant": "acme", "priority": 3},
            headers={"version": 2},
            subject="order/1",
        )
        assert result.transport_message_id
        await consume_until(consumer, lambda: len(received) == 1)
    message = received[0]
    assert message.body == OrderCreated(order_id=1, total=9.5)
    assert message.id == result.message_id
    assert message.type == "test.order_created.v1"
    assert message.source == "urn:service:test"
    assert message.subject == "order/1"
    assert message.headers == {"version": 2}
    assert message.attribute("tenant") == "acme"
    assert message.attribute("priority") == 3
    assert message.receive_count == 1
    assert message.envelope == "cloudevents"
    assert isinstance(message.time, datetime.datetime)
    assert message.outcome == "acked"
    assert (await queue.approximate_count()).visible == 0


async def test_default_raw_envelope(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("lean"))
    typed: list[fanferro.Message[OrderCreated]] = []
    untyped: list[fanferro.Message[Any]] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(OrderCreated)
    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        typed.append(message)

    @consumer.default
    async def other(message: fanferro.Message) -> None:
        untyped.append(message)

    async with client:
        result = await queue.send(OrderCreated(order_id=1, total=9.5), attributes={"tenant": "acme"}, headers={"v": 2})
        for body in ({"plain": True}, [1, 2], 42, None, "text", b"\x00\x01"):
            await queue.send(body)
        await consume_until(consumer, lambda: len(typed) == 1 and len(untyped) == 6)
    message = typed[0]
    assert message.envelope == "raw"
    assert message.body == OrderCreated(order_id=1, total=9.5)
    assert message.id == result.message_id
    assert message.type == "test.order_created.v1"
    assert message.content_type == "application/json"
    assert message.attribute("tenant") == "acme"
    assert message.attribute("v") == 2, "without an envelope, headers travel as attributes"
    assert {"fanferro.id", "fanferro.type", "tenant", "v"} <= set(message.raw_attributes)
    assert "content-type" not in message.raw_attributes, "JSON is implied"
    bodies = {repr(m.body): m.content_type for m in untyped}
    assert bodies == {
        "{'plain': True}": "application/json",
        "[1, 2]": "application/json",
        "42": "application/json",
        "None": "application/json",
        "'text'": "text/plain; charset=utf-8",
        "b'\\x00\\x01'": "application/octet-stream",
    }
    assert all(m.envelope == "raw" and m.id for m in untyped)


async def test_raw_messages_whose_body_is_a_cloudevent(client_factory: ClientFactory, name: Any) -> None:
    # A relayed CloudEvent published with the default raw envelope: default consumers tried the
    # CloudEvents envelope first and unwrapped the relayed event (its id, type and data).
    client = client_factory()
    queue = client.queue(name("relay"))
    received: list[fanferro.Message[Any]] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route("relay.v1")
    async def handle(message: fanferro.Message[dict]) -> None:
        received.append(message)

    event = {"specversion": "1.0", "id": "evt-1", "source": "upstream", "type": "relayed", "data": {"x": 1}}
    async with client:
        result = await queue.send(event, type="relay.v1")
        await consume_until(consumer, lambda: len(received) == 1)
    message = received[0]
    assert (message.envelope, message.id, message.type, message.body) == ("raw", result.message_id, "relay.v1", event)


async def test_null_payloads_in_cloudevents(client_factory: ClientFactory, name: Any) -> None:
    # The CloudEvents JSON envelope read its own `"data": null` as no data: handlers got an empty
    # body, which a typed route failed to decode (and dead-lettered).
    client = client_factory()
    queue = client.queue(name("nulls"), envelope=CloudEvents())
    received: list[fanferro.Message[Any]] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route("pings.v1")
    async def handle(message: fanferro.Message[dict[str, int] | None]) -> None:
        received.append(message)

    async with client:
        await queue.send(None, type="pings.v1")
        await queue.send({"n": 1}, type="pings.v1")
        await consume_until(consumer, lambda: len(received) == 2)
    assert sorted((message.data, message.body is None) for message in received) == [
        (b"null", True),
        (b'{"n":1}', False),
    ]
    assert {message.envelope for message in received} == {"cloudevents"}


async def test_topic_fanout_raw_and_wrapped(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    topic = client.topic(name("events"))
    raw_queue = client.queue(name("raw"))
    wrapped_queue = client.queue(name("wrapped"))
    client.subscription(topic, raw_queue, raw_message_delivery=True)
    client.subscription(topic, wrapped_queue, raw_message_delivery=False)
    async with client:
        await topic.publish(
            OrderCancelled(order_id=7, reason="customer"), attributes={"region": "eu"}, sns_subject="hi"
        )
        for queue in (raw_queue, wrapped_queue):
            messages = await drain_queue(client, queue)
            assert len(messages) == 1
            message = messages[0]
            assert message.json() == {"order_id": 7, "reason": "customer"}
            assert message.type == "test.order_cancelled.v1"
            assert message.attribute("region") == "eu"
            if queue is raw_queue:
                assert message.topic_arn is None, "raw delivery carries no SNS metadata"
            else:
                assert message.topic_arn is not None
                assert message.topic_arn.endswith(topic.name)


async def test_filter_policies(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    topic = client.topic(name("orders"))
    created_only = client.queue(name("created"))
    client.subscription(topic, created_only, filter_policy={"fanferro.type": ["test.order_created.v1"]})
    async with client:
        await topic.publish(OrderCreated(order_id=1))
        await topic.publish(OrderCancelled(order_id=2))
        messages = await drain_queue(client, created_only)
    assert [message.type for message in messages] == ["test.order_created.v1"]


async def test_filter_policy_operators(client_factory: ClientFactory, name: Any) -> None:
    """Wildcards, CIDR ranges, anything-but, numeric ranges and nested body keys filter like SNS."""
    client = client_factory()
    topic = client.topic(name("events"))
    by_attributes = client.queue(name("attributes"))
    by_body = client.queue(name("body"))
    client.subscription(
        topic,
        by_attributes,
        filter_policy={
            "region": [{"wildcard": "eu-*-1"}],
            "client_ip": [{"cidr": "10.0.0.0/8"}],
            "tier": [{"anything-but": {"prefix": "free"}}],
        },
    )
    client.subscription(
        topic,
        by_body,
        filter_policy={"order": {"total": [{"numeric": [">=", 100]}], "status": [{"equals-ignore-case": "paid"}]}},
        filter_policy_scope="MessageBody",
    )
    async with client:
        for total, status, region, client_ip, tier in [
            (150, "PAID", "eu-west-1", "10.1.2.3", "pro"),
            (50, "paid", "eu-west-2", "10.1.2.3", "pro"),
            (100, "Paid", "eu-west-1", "192.168.0.1", "free-trial"),
        ]:
            await topic.publish(
                {"order": {"total": total, "status": status}},
                attributes={"region": region, "client_ip": client_ip, "tier": tier},
            )
        by_attributes_messages = await drain_queue(client, by_attributes)
        by_body_messages = await drain_queue(client, by_body)
    assert [message.json()["order"]["total"] for message in by_attributes_messages] == [150]
    assert sorted(message.json()["order"]["total"] for message in by_body_messages) == [100, 150]


async def test_invalid_filter_policies_are_refused(memory_client: fanferro.Client, name: Any) -> None:
    """Like SNS, the in-memory broker refuses a policy it cannot evaluate when it is set."""
    topic = memory_client.topic(name("events"))
    queue = memory_client.queue(name("events"))
    memory_client.subscription(topic, queue, filter_policy={"region": [{"glob": "eu-*"}]})
    with pytest.raises(fanferro.ProvisioningError, match="Unrecognized match type glob"):
        async with memory_client:
            pass


async def test_batches(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("batch"))
    async with client:
        result = await queue.send_batch([OrderCreated(order_id=i) for i in range(25)], attributes={"batch": "yes"})
        assert result.ok
        assert len(result.successful) == 25
        messages = await drain_queue(client, queue)
    assert sorted(message.json()["order_id"] for message in messages) == list(range(25))
    assert all(message.attribute("batch") == "yes" for message in messages)


async def test_batch_with_invalid_entry(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("partial"))
    async with client:
        messages = [
            fanferro.OutgoingMessage(body={"n": 1}),
            fanferro.OutgoingMessage(body={"n": 2}, attributes={"bad name": "x"}),
        ]
        with pytest.raises(fanferro.BatchPublishError) as raised:
            await queue.send_batch(messages)
        result = raised.value.result
        assert len(result.successful) == 1
        assert result.failed[0].index == 1
        assert isinstance(result.failed[0].error, fanferro.ValidationError)
        partial = await queue.send_batch(messages, raise_on_failure=False)
        assert not partial.ok


async def test_fifo_order_and_deduplication(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(f"{name('fifo')}.fifo")
    seen: list[tuple[str, int]] = []
    consumer = client.consumer(queue, concurrency=4, wait_time=1)

    @consumer.route(OrderCreated)
    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        await asyncio.sleep(0.01 * (message.body.order_id % 3))
        seen.append((message.group_id or "", message.body.order_id))

    async with client:
        for i in range(12):
            group = f"g{i % 3}"
            outgoing = fanferro.OutgoingMessage(body=OrderCreated(order_id=i), group_id=group, id=f"id-{i}")
            await queue.send(outgoing)
        await queue.send(fanferro.OutgoingMessage(body=OrderCreated(order_id=0), group_id="g0", id="id-0"))
        with pytest.raises(fanferro.ValidationError, match="group_id"):
            await queue.send(OrderCreated(order_id=99))
        await consume_until(consumer, lambda: len(seen) >= 12)
    for group in ("g0", "g1", "g2"):
        ids = [order for g, order in seen if g == group]
        assert ids == sorted(ids), f"group {group} out of order: {ids}"
    assert len(seen) == 12, "the duplicate send was deduplicated"


async def test_fifo_ids_are_validated_before_sending(client_factory: ClientFactory, name: Any) -> None:
    # docs/parity M8: SQS refuses group and deduplication ids that are not 1-128 ASCII letters,
    # digits and punctuation; the message id is the deduplication id unless one is given.
    client = client_factory()
    queue = client.queue(f"{name('fifo-ids')}.fifo")
    async with client:
        for options, field in [
            ({"group_id": "tenant a"}, "group_id"),
            ({"group_id": "g" * 129}, "group_id"),
            ({"group_id": "g", "deduplication_id": "order 1"}, "deduplication_id"),
            ({"group_id": "g", "id": "order 1"}, "message id"),
        ]:
            with pytest.raises(fanferro.ValidationError, match=field):
                await queue.send({"x": 1}, **options)
        await queue.send({"x": 1}, group_id="tenant-a/eu:1", id="order#1")
        assert [m.group_id for m in await drain_queue(client, queue)] == ["tenant-a/eu:1"]


@pytest.mark.parametrize(("scope", "delivered"), [("messageGroup", ["a", "b"]), ("queue", ["a"])])
async def test_fifo_deduplication_scope(
    client_factory: ClientFactory, name: Any, scope: Literal["queue", "messageGroup"], delivered: list[str]
) -> None:
    # The memory broker deduplicated by the deduplication id alone, also with the messageGroup scope.
    client = client_factory()
    queue = client.queue(
        f"{name('dedup-scope')}.fifo",
        attributes=fanferro.QueueAttributes(deduplication_scope=scope, fifo_throughput_limit="perQueue"),
    )
    async with client:
        for group in ("a", "a", "b"):
            await queue.send({"group": group}, group_id=group, deduplication_id="same")
        messages = await drain_queue(client, queue)
    assert sorted(message.json()["group"] for message in messages) == delivered


@pytest.mark.parametrize(("strict", "acked_before"), [(True, [0, 1, 2]), (False, [0, 0, 0])])
async def test_fifo_groups_wait_for_each_ack(
    client_factory: ClientFactory, name: Any, strict: bool, acked_before: list[int]
) -> None:
    """A group's next message starts once the previous delete is confirmed (strict_ordering)."""
    client = client_factory()
    queue = client.queue(f"{name('strict')}.fifo")
    # Background acks wait for this long linger; an awaited delete goes out at once when nothing
    # else could join its batch.
    consumer = client.consumer(queue, wait_time=1, settle_linger=0.5, strict_ordering=strict)
    started: list[tuple[float, int]] = []

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        started.append((time.monotonic(), consumer.stats().engine.messages_acked))

    async with client:
        await queue.send_batch(
            [fanferro.OutgoingMessage(body=OrderCreated(order_id=i), group_id="g") for i in range(3)]
        )
        await consume_until(consumer, lambda: len(started) == 3)
    assert [acked for _, acked in started] == acked_before
    assert started[-1][0] - started[0][0] < 0.5, "the confirmed deletes waited for settle_linger"


async def test_protobuf_envelope_and_codec(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("proto"), envelope=fanferro.envelopes.CloudEventsProtobuf())
    async with client:
        await queue.send(timestamp_pb2.Timestamp(seconds=42, nanos=7))
        received: list[timestamp_pb2.Timestamp] = []
        consumer = client.consumer(queue, wait_time=1)

        @consumer.route(timestamp_pb2.Timestamp)
        async def handle(stamp: timestamp_pb2.Timestamp) -> None:
            received.append(stamp)

        await consume_until(consumer, lambda: len(received) == 1)
    assert received[0] == timestamp_pb2.Timestamp(seconds=42, nanos=7)


async def test_raw_envelope_interop(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("raw"), envelope=fanferro.envelopes.Raw(type_attribute=None, id_attribute=None))
    async with client:
        await queue.send({"order_id": 5, "total": 1.0}, attributes={"source": "legacy"})
        received: list[OrderCreated] = []
        consumer = client.consumer(queue, wait_time=1)

        @consumer.default
        async def handle(message: fanferro.Message[OrderCreated]) -> None:
            received.append(message.body)
            assert message.envelope in {"raw", "cloudevents"}

        await consume_until(consumer, lambda: len(received) == 1)
    assert received == [OrderCreated(order_id=5, total=1.0)]


async def test_routing_by_version(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("versions"))
    calls: list[str] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(OrderCreated, when=m.header("version") >= 2)
    async def v2(order: OrderCreated) -> None:
        calls.append(f"v2:{order.order_id}")

    @consumer.route(OrderCreated)
    async def v1(order: OrderCreated) -> None:
        calls.append(f"v1:{order.order_id}")

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await queue.send(OrderCreated(order_id=2), headers={"version": 2})
        await consume_until(consumer, lambda: len(calls) == 2)
    assert sorted(calls) == ["v1:1", "v2:2"]


async def test_a_route_without_a_type_for_a_whole_message_handler_receives_every_message(
    client_factory: ClientFactory, name: Any
) -> None:
    # Regression test for #176: such a route took typing.Any as its type and dead-lettered everything.
    client = client_factory()
    queue = client.queue(name("untyped-route"))
    received: list[Any] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route
    async def handle(message: fanferro.Message[Any]) -> None:
        received.append(message.body)

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await queue.send({"plain": True}, type="something.else")
        await consume_until(consumer, lambda: len(received) == 2)
    assert sorted(received, key=str) == sorted([{"order_id": 1, "total": 0.0}, {"plain": True}], key=str)
    assert consumer.stats().unroutable == 0


async def test_large_payloads(client_factory: ClientFactory, name: Any) -> None:
    bucket = name("payloads")
    client = client_factory(
        large_payloads=fanferro.LargePayloads(bucket, threshold=2048, reference="both", create_bucket=True)
    )
    topic = client.topic(name("big"))
    queue = client.queue(name("big-worker"))
    client.subscription(topic, queue)
    big = {"blob": "x" * 50_000}
    async with client:
        result = await topic.publish(big)
        assert result.offloaded is not None
        assert result.offloaded.bucket == bucket
        assert result.offloaded.url is not None
        small = await topic.publish({"blob": "tiny"})
        assert small.offloaded is None
        messages = await drain_queue(client, queue)
    bodies = sorted((message.json() for message in messages), key=lambda body: len(body["blob"]))
    assert bodies == [{"blob": "tiny"}, big]
    offloaded = [message for message in messages if message.pointer is not None]
    assert len(offloaded) == 1


async def test_a_message_published_again_keeps_its_offloaded_payload(client_factory: ClientFactory, name: Any) -> None:
    # Sending a message again with the same id (an application retry): the FIFO queue deduplicates
    # it, but its CloudEvent has a new time, and its payload replaced the first one's, which then
    # failed its integrity check, so the only stored copy was dead-lettered.
    client = client_factory(large_payloads=fanferro.LargePayloads(name("payloads"), threshold=1024, create_bucket=True))
    queue = client.queue(f"{name('resent')}.fifo", envelope=CloudEvents())
    message = fanferro.OutgoingMessage(body={"blob": "x" * 5000}, id="order-1", group_id="g")
    async with client:
        first = await queue.send(message)
        second = await queue.send(message)
        assert first.message_id == second.message_id == "order-1"
        assert first.offloaded is not None
        assert second.offloaded is not None
        assert first.offloaded.key != second.offloaded.key
        received = await drain_queue(client, queue)
    assert [(m.id, m.json()) for m in received] == [("order-1", {"blob": "x" * 5000})]


async def test_a_copy_whose_offloaded_payload_was_deleted_is_skipped_as_a_duplicate(
    client_factory: ClientFactory, name: Any
) -> None:
    # Two copies of a message (a publisher retry) share their payload object; with
    # delete_offloaded_payloads, the ack of one deletes it. The other copy was dead-lettered as
    # undecodable (NoSuchKey) instead of skipped as already processed.
    bucket = name("payloads")
    client = client_factory(
        large_payloads=fanferro.LargePayloads(bucket, threshold=1024, reference="both", create_bucket=True)
    )
    queue = client.queue(name("deleted-payload"))
    processed: list[str] = []
    duplicates: list[fanferro.Event] = []
    decode_failures: list[fanferro.Event] = []
    client.on("message.duplicate", duplicates.append)
    client.on("message.decode_failed", decode_failures.append)

    async def handle(message: fanferro.Message[bytes]) -> None:
        processed.append(message.id)

    consumer = client.consumer(queue, handle, wait_time=1, delete_offloaded_payloads=True)
    message = fanferro.OutgoingMessage(body={"blob": "x" * 5000}, id="order-1")
    async with client:
        first = await queue.send(message)
        second = await queue.send(message)
        assert first.offloaded is not None
        assert first.offloaded.key is not None
        assert second.offloaded is not None
        assert second.offloaded.key == first.offloaded.key
        # What the ack of a copy processed elsewhere does.
        core = await client._core()
        await core.delete_payload(bucket, first.offloaded.key)
        await consume_until(consumer, lambda: len(duplicates) == 2)
        counts = await queue.approximate_count()
    assert counts.visible == counts.in_flight == 0
    assert processed == []
    assert decode_failures == []
    assert consumer.stats().duplicates == 2
    assert consumer.stats().decode_failures == 0
    assert duplicates[0].fields["reason"] == "payload_deleted"


async def test_payloads_outside_the_allowed_buckets_are_not_deleted(client_factory: ClientFactory, name: Any) -> None:
    # With delete_offloaded_payloads, the consumer deleted the bucket and key a pointer names even
    # when allowed_buckets refused to read from it and the payload came from the pointer's URL: a
    # crafted pointer could have it delete any object it may delete.
    bucket = name("payloads")
    publisher = client_factory(
        large_payloads=fanferro.LargePayloads(bucket, threshold=1024, reference="both", create_bucket=True)
    )
    topic = publisher.topic(name("shared-payload"))
    restricted_name = name("restricted")
    other_queue = publisher.queue(name("other"))
    publisher.subscription(topic, publisher.queue(restricted_name))
    publisher.subscription(topic, other_queue)
    restricted = client_factory(payload_fetch=fanferro.PayloadFetch(allowed_buckets=[name("elsewhere")]))
    restricted_queue = restricted.queue(restricted_name)
    processed: list[Any] = []
    errors: list[fanferro.Event] = []
    deleted: list[fanferro.Event] = []
    restricted.on("consumer.error", errors.append)
    restricted.on("payload.deleted", deleted.append)

    async def handle(message: fanferro.Message[Any]) -> None:
        processed.append(message.json())

    consumer = restricted.consumer(restricted_queue, handle, wait_time=1, delete_offloaded_payloads=True)
    big = {"blob": "x" * 5000}
    async with publisher, restricted:
        result = await topic.publish(big)
        assert result.offloaded is not None
        await consume_until(consumer, lambda: len(errors) + len(deleted) == 1)
        # The other subscriber still finds the payload both queues point to.
        received = await drain_queue(publisher, other_queue)
    assert processed == [big], "read through the URL"
    assert deleted == []
    assert "refusing to delete a payload from bucket" in str(errors[0].error.message)  # type: ignore[union-attr]
    assert [message.json() for message in received] == [big]


async def test_messages_over_the_queue_limit_are_offloaded_below_the_threshold(
    client_factory: ClientFactory, name: Any
) -> None:
    # The queue takes at most 2 KiB, less than the offload threshold: offloading is enabled, so the
    # message is offloaded instead of refused with MessageTooLargeError.
    bucket = name("payloads")
    client = client_factory(large_payloads=fanferro.LargePayloads(bucket, create_bucket=True))
    queue = client.queue(name("small-limit"), attributes=fanferro.QueueAttributes(max_message_size=2048))
    body = {"blob": "x" * 5000}
    async with client:
        result = await queue.send(body)
        assert result.offloaded is not None
        messages = await drain_queue(client, queue)
    assert [message.json() for message in messages] == [body]


async def test_outbox_prepare_and_relay(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    topic = client.topic(name("outbox"))
    queue = client.queue(name("outbox-worker"))
    fifo_queue = client.queue(f"{name('outbox-fifo')}.fifo")
    client.subscription(topic, queue)
    outbox = InMemoryOutbox()
    async with client:
        for i in range(3):
            prepared = await topic.prepare(OrderCreated(order_id=i))
            outbox.add(fanferro.PreparedMessage.from_json(prepared.to_json()))
        for i in range(2):
            outbox.add(await fifo_queue.prepare(OrderCreated(order_id=i), group_id="g"))
        relay = OutboxRelay(client, outbox, batch_size=10)
        assert await relay.run_once() == 5
        assert await relay.run_once() == 0
        assert outbox.pending == []
        messages = await drain_queue(client, queue)
        fifo_messages = await drain_queue(client, fifo_queue)
    assert sorted(message.json()["order_id"] for message in messages) == [0, 1, 2]
    assert [message.json()["order_id"] for message in fifo_messages] == [0, 1]


async def test_prepare_refuses_attributes_a_publish_refuses(memory_client: fanferro.Client, name: Any) -> None:
    # An outbox row with attributes SQS refuses was committed, and the relay gave it up later.
    queue = memory_client.queue(name("prepare-attributes"))
    own = len((await queue.prepare({"x": 1})).wire.attributes)
    own_binary = len((await queue.prepare(b"\xff")).wire.attributes)
    many = {f"a{i}": "x" for i in range(10 - own)}
    await queue.prepare({"x": 1}, attributes=many)  # exactly at the limit
    refused: list[tuple[Any, dict[str, Any]]] = [
        ({"x": 1}, {**many, "one-more": "x"}),
        (b"\xff", {f"a{i}": "x" for i in range(10 - own_binary)}),  # the base64 encoding needs one
        ({"x": 1}, {"empty": ""}),
        ({"x": 1}, {"AWS.trace": "x"}),
        ({"x": 1}, {"with space": "x"}),
        ({"x": 1}, {"fanferro.encoding": "base64"}),
    ]
    for body, attributes in refused:
        with pytest.raises(fanferro.ValidationError):
            await queue.prepare(body, attributes=attributes)
        with pytest.raises(fanferro.ValidationError):
            await queue.send(body, attributes=attributes)


async def test_prepared_messages_use_the_large_payload_settings_of_their_destination(
    client_factory: ClientFactory, name: Any
) -> None:
    # A topic and its subscriber queue share a name: prepared messages for the queue were sent with
    # the topic's settings (no offloading here), and refused as too large.
    client = client_factory(large_payloads=fanferro.LargePayloads(name("payloads"), threshold=1024, create_bucket=True))
    shared = name("orders")
    topic = client.topic(shared, large_payloads=None)
    queue = client.queue(shared)
    big = {"blob": "x" * 300_000}
    async with client:
        single = await client.publish_prepared(await queue.prepare(big))
        assert single.offloaded is not None
        batch = await client.publish_prepared_batch([await queue.prepare(big)], raise_on_failure=True)
        assert batch.successful[0].offloaded is not None
        with pytest.raises(fanferro.MessageTooLargeError):
            await client.publish_prepared(await topic.prepare(big))
        assert [message.json() for message in await drain_queue(client, queue)] == [big, big]


async def test_the_outbox_relay_gives_up_on_records_it_cannot_publish(client_factory: ClientFactory, name: Any) -> None:
    # Records for a deleted queue at the head of the outbox were claimed again every round, so the
    # relay never got to the records behind them.
    client = client_factory()
    gone = client.queue(name("outbox-gone"))
    worker = client.queue(name("outbox-worker"))
    outbox = InMemoryOutbox()
    async with client:
        lost = [outbox.add(await gone.prepare({"n": i})) for i in range(3)]
        kept = outbox.add(await worker.prepare({"n": 3}))
        await gone.delete()
        relay = OutboxRelay(client, outbox, batch_size=2, max_attempts=3)
        for _ in range(10):
            await relay.run_once()
            if not outbox.pending:
                break
        assert outbox.published == [kept]
        assert [(record.id, record.attempts) for record in outbox.abandoned] == [(i, 3) for i in lost]
        assert [message.json() for message in await drain_queue(client, worker)] == [{"n": 3}]


async def test_the_outbox_relay_keeps_held_back_fifo_records(client_factory: ClientFactory, name: Any) -> None:
    # A FIFO record held back behind a failing earlier one of its group did not fail itself.
    client = client_factory()
    queue = client.queue(f"{name('outbox-fifo')}.fifo", attributes=fanferro.QueueAttributes(max_message_size=1024))
    outbox = InMemoryOutbox()
    async with client:
        too_large = outbox.add(await queue.prepare({"blob": "x" * 2000}, group_id="g"))
        later = outbox.add(await queue.prepare({"n": 1}, group_id="g"))
        relay = OutboxRelay(client, outbox, max_attempts=2)
        for _ in range(5):
            await relay.run_once()
        assert outbox.published == [later]
        assert [record.id for record in outbox.abandoned] == [too_large]
        assert [message.json() for message in await drain_queue(client, queue)] == [{"n": 1}]


async def test_publish_middleware(client_factory: ClientFactory, name: Any) -> None:
    async def tenant(context: fanferro.PublishContext, call_next: fanferro.PrepareNext) -> fanferro.PreparedMessage:
        context.message.attributes.setdefault("tenant", "acme")
        prepared = await call_next(context)
        assert prepared.destination_name == context.destination
        return prepared

    client = client_factory(publish_middleware=[tenant])
    queue = client.queue(name("middleware"))
    async with client:
        await queue.send({"x": 1})
        messages = await drain_queue(client, queue)
    assert messages[0].attribute("tenant") == "acme"


async def test_publish_options_are_not_mutated(client_factory: ClientFactory, name: Any) -> None:
    tenant = {"current": "acme"}

    async def add_tenant(context: fanferro.PublishContext, call_next: fanferro.PrepareNext) -> fanferro.PreparedMessage:
        context.message.attributes.setdefault("tenant", tenant["current"])
        context.message.headers.setdefault("tenant", tenant["current"])
        return await call_next(context)

    client = client_factory(publish_middleware=[add_tenant])
    queue = client.queue(name("shared-options"))
    common_attributes = {"source": "importer"}
    common_headers: dict[str, fanferro.messages.HeaderValue] = {}
    async with client:
        await queue.send({"n": 1}, attributes=common_attributes, headers=common_headers)
        tenant["current"] = "globex"
        await queue.send({"n": 2}, attributes=common_attributes, headers=common_headers)
        tenant["current"] = "initech"
        await queue.send_batch([{"n": 3}, {"n": 4}], attributes=common_attributes)
        messages = await drain_queue(client, queue)
    assert common_attributes == {"source": "importer"}
    assert common_headers == {}
    tenants = {message.json()["n"]: message.attribute("tenant") for message in messages}
    assert tenants == {1: "acme", 2: "globex", 3: "initech", 4: "initech"}
    # Optional arguments passed through as None still mean "none".
    message = fanferro.OutgoingMessage({"n": 5}, attributes=None, headers=None)  # type: ignore[arg-type]
    assert (message.attributes, message.headers) == ({}, {})


async def test_out_of_range_options_are_validation_errors(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("invalid-options"))
    topic = client.topic(name("invalid-options"))
    async with client:
        with pytest.raises(fanferro.ValidationError, match="out of range"):
            await queue.send({"n": 1}, delay=1e30)
        for destination in (queue, topic):
            with pytest.raises(fanferro.ValidationError, match="trace_header"):
                await destination.publish({"n": 1}, trace_header="Root=1-5759e988-bd862e3fe1be46a994272793\nX=1")
        assert (await queue.approximate_count()).visible == 0


async def test_reserved_attributes_are_rejected(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("reserved"))
    async with client:
        with pytest.raises(fanferro.ValidationError, match="reserved"):
            await queue.send({"x": 1}, attributes={"fanferro.type": "spoof"})
        with pytest.raises(fanferro.ValidationError, match="at most 10"):
            await queue.send({"x": 1}, attributes={f"a{i}": "v" for i in range(10)})


async def test_numbers_of_more_than_38_digits_are_refused_before_sending(
    client_factory: ClientFactory, name: Any
) -> None:
    # docs/parity M11: SQS refuses Number values of more than 38 significant digits.
    client = client_factory()
    queue = client.queue(name("digits"))
    async with client:
        with pytest.raises(fanferro.ValidationError, match="39 significant digits"):
            await queue.send({"x": 1}, attributes={"n": int("1" * 39)})
        await queue.send({"x": 1}, attributes={"n": int("1" * 38)})
        messages = await drain_queue(client, queue)
    assert [message.attribute("n") for message in messages] == [int("1" * 38)]


class LegacyEnvelope:
    """A custom (Python) envelope: consumers unwrap it in Python, built-in ones in the engine."""

    name = "legacy"

    def wrap(self, payload: fanferro.Payload) -> fanferro.WireMessage:
        document = {"kind": payload.message_type, "data": payload.data.decode()}
        return fanferro.WireMessage(json.dumps(document), {"legacy": "1"})

    def unwrap(self, wire: fanferro.WireMessage) -> fanferro.Payload | None:
        if "legacy" not in wire.attributes:
            return None
        document = json.loads(wire.body)
        return fanferro.Payload(document["data"], message_type=document["kind"], content_type="application/json")


async def test_custom_and_builtin_envelopes(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("envelopes"))
    received: list[tuple[str, int, dict[str, str]]] = []

    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        attributes = {key: str(value.value) for key, value in message.attributes.items()}
        received.append((message.envelope, message.body.order_id, attributes))

    custom = client.consumer(
        queue,
        handle,
        wait_time=1,
        envelopes=[LegacyEnvelope(), fanferro.envelopes.CloudEvents(), fanferro.envelopes.Raw()],
    )
    async with client:
        await queue.send(OrderCreated(order_id=1), envelope=LegacyEnvelope())
        await queue.send(OrderCreated(order_id=2), envelope=fanferro.envelopes.CloudEvents(), attributes={"a": "1"})
        await queue.send(OrderCreated(order_id=3), attributes={"b": "2"})
        await consume_until(custom, lambda: len(received) == 3)
        # Built-in envelopes only: the engine unwraps the messages while decoding them.
        builtin = client.consumer(queue, handle, wait_time=1, name="builtin")
        await queue.send(OrderCreated(order_id=4), envelope=fanferro.envelopes.CloudEvents(), attributes={"c": "3"})
        await queue.send(OrderCreated(order_id=5))
        await consume_until(builtin, lambda: len(received) == 5)
    assert sorted(received, key=lambda item: item[1]) == [
        ("legacy", 1, {"legacy": "1"}),
        ("cloudevents", 2, {"a": "1", "content-type": "application/cloudevents+json"}),
        ("raw", 3, {"b": "2"}),
        ("cloudevents", 4, {"c": "3", "content-type": "application/cloudevents+json"}),
        ("raw", 5, {}),
    ]


@pytest.mark.parametrize("split", [False, True])
async def test_batch_publishes_to_topics_split_by_trace_header(split: bool, name: Any) -> None:
    # SNS takes one X-Ray trace header per PublishBatch call: entries with different ones keep
    # theirs only when the client splits such batches.
    from fanferro.testing import memory_client

    client = memory_client(split_batches_by_trace_header=split)
    topic = client.topic(name("traced"))
    queue = client.queue(name("traced-worker"))
    client.subscription(topic, queue)
    headers = [
        "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1",
        "Root=1-5759e988-bd862e3fe1be46a994272794;Parent=53995c3f42cd8ad9;Sampled=1",
    ]
    async with client:
        await topic.publish_batch(
            [fanferro.OutgoingMessage({"n": i}, trace_header=header) for i, header in enumerate(headers)]
        )
        messages = await drain_queue(client, queue)
    received = {message.json()["n"]: message.trace_header for message in messages}
    assert received == ({0: headers[0], 1: headers[1]} if split else {0: None, 1: None})
