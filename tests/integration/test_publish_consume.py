from __future__ import annotations

import asyncio
import datetime
import json
from typing import Any

import pytest
from google.protobuf import timestamp_pb2

import fanferro
from fanferro import matchers as m
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
    assert (await queue.approximate_count())["visible"] == 0


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
        assert (await queue.approximate_count())["visible"] == 0


async def test_reserved_attributes_are_rejected(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("reserved"))
    async with client:
        with pytest.raises(fanferro.ValidationError, match="reserved"):
            await queue.send({"x": 1}, attributes={"fanferro.type": "spoof"})
        with pytest.raises(fanferro.ValidationError, match="at most 10"):
            await queue.send({"x": 1}, attributes={f"a{i}": "v" for i in range(10)})


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
