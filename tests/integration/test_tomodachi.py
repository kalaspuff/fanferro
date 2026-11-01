"""Messages exchanged with tomodachi services (``fanferro.envelopes.compat``).

The tomodachi side is played by the exact bodies tomodachi 0.29 writes (fixtures built with its
``JsonBase``/``ProtobufBase`` and ``send_message()``) and by :class:`TomodachiReader`, which reads
SQS messages the way tomodachi's consumer does.
"""

from __future__ import annotations

import base64
import json
import zlib
from dataclasses import dataclass
from typing import Any

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory, wrappers_pb2

import fanferro
from fanferro.envelopes import Raw, default_envelopes
from fanferro.envelopes.compat import TomodachiJSON, TomodachiProtobuf, tomodachi_topic
from tests.conftest import ClientFactory
from tests.helpers import consume_until, drain_queue

# Built by tomodachi 0.29: JsonBase/ProtobufBase.build_message(service, "order.created", data).
TOMODACHI_JSON = (
    '{"service": {"name": "legacy-orders", "uuid": "1b2c3d4e-0000-4000-8000-000000000001"}, "metadata": '
    '{"message_uuid": "1b2c3d4e-0000-4000-8000-000000000001.3090053a-24be-4b65-847c-91b951ff0074", '
    '"protocol_version": "tomodachi-json-base--1.0.0", "compatible_protocol_versions": ["json_base-wip"], '
    '"timestamp": 1793547225.2425017, "topic": "order.created", "data_encoding": "raw"}, '
    '"data": {"order_id": 7, "total": 1.1}}'
)
TOMODACHI_PROTOBUF = (
    "CjUKDWxlZ2FjeS1vcmRlcnMSJDFiMmMzZDRlLTAwMDAtNDAwMC04MDAwLTAwMDAwMDAwMDAwMRKKAQpJMWIyYzNkNGUtMDAwMC00MDAwLTgwMD"
    "AtMDAwMDAwMDAwMDAxLmYzZDQ5NWM3LTJhZWItNGZmYy1iYThmLTdhZDY2ZWZkYzk0NxIedG9tb2RhY2hpLXByb3RvYnVmLWJhc2UtLTEuMC4w"
    "GY6FT/bWudpBIg1vcmRlci5jcmVhdGVkKgVwcm90bxoFCgNoZWo="
)
NO_ATTRIBUTES = Raw(content_type_attribute=None, type_attribute=None, id_attribute=None)
"""Sends bodies as they are, without attributes, like a tomodachi service."""


@dataclass
class Order:
    order_id: int
    total: float


def _sns_sqs_message_class() -> Any:
    """tomodachi's ``proto.SNSSQSMessage`` (``sns_sqs_message.proto``), in a pool of its own."""
    file = descriptor_pb2.FileDescriptorProto(name="tomodachi_sns_sqs_message.proto", package="proto", syntax="proto3")
    string, double, message, data = (
        descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
        descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,
        descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        descriptor_pb2.FieldDescriptorProto.TYPE_BYTES,
    )
    optional = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    for name, fields in (
        ("Service", [("name", string, None), ("uuid", string, None)]),
        (
            "Metadata",
            [
                ("message_uuid", string, None),
                ("protocol_version", string, None),
                ("timestamp", double, None),
                ("topic", string, None),
                ("data_encoding", string, None),
            ],
        ),
        (
            "SNSSQSMessage",
            [("service", message, ".proto.Service"), ("metadata", message, ".proto.Metadata"), ("data", data, None)],
        ),
    ):
        descriptor = file.message_type.add(name=name)
        for number, (field, kind, type_name) in enumerate(fields, start=1):
            descriptor.field.add(name=field, number=number, type=kind, label=optional, type_name=type_name)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("proto.SNSSQSMessage"))


SNSSQSMessage = _sns_sqs_message_class()


class TomodachiReader:
    """Reads an SQS message body like tomodachi 0.29's consumer and envelopes do."""

    @staticmethod
    def payload(body: bytes) -> str:
        # The SQS consumer: json.loads(body)["Message"] (SNS notifications and queue documents alike).
        return json.loads(body)["Message"]

    @classmethod
    def json(cls, body: bytes) -> dict[str, Any]:
        message = json.loads(cls.payload(body))
        metadata = message["metadata"]
        if metadata["data_encoding"] == "base64_gzip_json":
            message["data"] = json.loads(zlib.decompress(base64.b64decode(message["data"])))
        return message

    @classmethod
    def protobuf(cls, body: bytes) -> tuple[Any, wrappers_pb2.StringValue]:
        message = SNSSQSMessage()
        message.ParseFromString(base64.b64decode(cls.payload(body)))
        data = message.data if message.metadata.data_encoding == "proto" else zlib.decompress(message.data)
        value = wrappers_pb2.StringValue()
        value.ParseFromString(data)
        return message, value


async def test_consumes_what_tomodachi_publishes_and_sends(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory(accept_envelopes=[TomodachiJSON(), TomodachiProtobuf(), *default_envelopes()])
    client.types.register(Order, "order.created")
    client.types.register(wrappers_pb2.StringValue, "order.created.proto")
    topic_name, queue_name = name("tomodachi-topic"), name("tomodachi-in")
    topic = client.topic(topic_name)
    queue = client.queue(queue_name)
    # tomodachi subscribes its queues without raw message delivery.
    client.subscription(topic_name, queue_name, raw_message_delivery=False)
    orders: list[fanferro.Message[Order]] = []
    values: list[fanferro.Message[wrappers_pb2.StringValue]] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(Order)
    async def on_order(message: fanferro.Message[Order]) -> None:
        orders.append(message)

    @consumer.route(wrappers_pb2.StringValue)
    async def on_value(message: fanferro.Message[wrappers_pb2.StringValue]) -> None:
        values.append(message)

    compressed = json.loads(TOMODACHI_JSON)
    compressed["metadata"]["data_encoding"] = "base64_gzip_json"
    compressed["data"] = base64.b64encode(zlib.compress(json.dumps({"order_id": 8, "total": 2.0}).encode())).decode()
    protobuf = SNSSQSMessage()
    protobuf.ParseFromString(base64.b64decode(TOMODACHI_PROTOBUF))
    protobuf.metadata.topic = "order.created.proto"
    async with client:
        await topic.publish(TOMODACHI_JSON, envelope=NO_ATTRIBUTES)
        await topic.publish(json.dumps(compressed), envelope=NO_ATTRIBUTES)
        await topic.publish(base64.b64encode(protobuf.SerializeToString()).decode(), envelope=NO_ATTRIBUTES)
        # tomodachi's send_message(): the envelope in a document shaped like an SNS notification.
        document = {"Type": "Message", "Version": "0", "Message": TOMODACHI_JSON, "MessageAttributes": {}}
        await queue.send(json.dumps(document), envelope=NO_ATTRIBUTES)
        await consume_until(consumer, lambda: len(orders) == 3 and len(values) == 1)

    assert sorted(message.body.order_id for message in orders) == [7, 7, 8]
    first = next(message for message in orders if message.body.order_id == 7)
    assert first.envelope == "tomodachi-json"
    assert first.type == "order.created"
    assert first.id == "3090053a-24be-4b65-847c-91b951ff0074"
    assert first.source == "legacy-orders"
    assert first.time is not None
    assert first.time.year == 2026
    assert values[0].body.value == "hej"
    assert values[0].envelope == "tomodachi-protobuf"


async def test_publishes_what_tomodachi_consumes(client_factory: ClientFactory, name: Any) -> None:
    # tomodachi's topic_prefix is the client's: metadata.topic names the topic without it.
    client = client_factory(topic_prefix="env-")
    reader = client_factory(accept_envelopes=[NO_ATTRIBUTES])
    # A dotted topic name of older tomodachi versions (0.29 allows letters, digits, - and _ only).
    topic_name, proto_name, queue_name = name("tomodachi.out"), name("tomodachi-proto"), name("tomodachi-out")
    json_topic = client.topic(tomodachi_topic(topic_name), envelope=TomodachiJSON(service_name="billing"))
    proto_topic = client.topic(proto_name, envelope=TomodachiProtobuf(service_uuid="svc"))
    assert json_topic.name == "env-" + tomodachi_topic(topic_name)
    queue = client.queue(queue_name, envelope=TomodachiJSON())
    client.subscription(tomodachi_topic(topic_name), queue_name, raw_message_delivery=False)
    client.subscription(proto_name, queue_name, raw_message_delivery=False)
    async with client, reader:
        sent = await json_topic.publish(Order(order_id=1, total=9.5), type="orders.created.v1")
        await json_topic.publish({"blob": "x" * 70_000})
        await proto_topic.publish(wrappers_pb2.StringValue(value="hej"))
        await queue.send({"direct": True})
        consumer_queue = reader.queue(queue_name)
        received = []
        for _ in range(10):
            received += await _bodies(reader, consumer_queue)
            if len(received) == 4:
                break

    documents = [message for message in received if not message.startswith(b'{"Type":"Message"')]
    by_kind = {_kind(body): body for body in documents}
    order = TomodachiReader.json(by_kind["order"])
    assert order["data"] == {"order_id": 1, "total": 9.5}
    assert order["service"]["name"] == "billing"
    assert order["metadata"]["topic"] == topic_name
    assert order["metadata"]["message_uuid"].endswith("." + sent.message_id)
    assert order["metadata"]["data_encoding"] == "raw"
    large = TomodachiReader.json(by_kind["large"])
    assert large["metadata"]["data_encoding"] == "base64_gzip_json"
    assert large["data"] == {"blob": "x" * 70_000}
    message, value = TomodachiReader.protobuf(by_kind["proto"])
    assert value.value == "hej"
    assert message.service.uuid == "svc"
    assert message.metadata.message_uuid.startswith("svc.")
    assert message.metadata.topic == proto_name
    direct = TomodachiReader.json(next(body for body in received if body.startswith(b'{"Type":"Message"')))
    assert direct["data"] == {"direct": True}
    assert direct["metadata"]["topic"] == ""


async def _bodies(client: fanferro.Client, queue: fanferro.Queue) -> list[bytes]:
    """The bodies of the SQS messages in ``queue`` as received, SNS notifications not unwrapped."""
    received: list[bytes] = []

    async def collect(message: fanferro.Message[bytes]) -> None:
        received.append(message.data)

    consumer = client.consumer(queue, collect, wait_time=1, sns_unwrap=False)
    await consumer.run_until_idle(idle_for=0.3 if client.broker is not None else 1.0, timeout=15)
    return received


def _kind(body: bytes) -> str:
    """Which message an SNS notification body holds."""
    message = json.loads(body)["Message"]
    if not message.startswith("{"):
        return "proto"
    return "large" if '"base64_gzip_json"' in message else "order"


async def test_round_trips_between_fanferro_services(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory(accept_envelopes=[TomodachiJSON(), *default_envelopes()])
    queue = client.queue(name("tomodachi-roundtrip"), envelope=TomodachiJSON())
    async with client:
        sent = await queue.send(Order(order_id=3, total=1.0), type="orders.v1")
        [message] = await drain_queue(client, queue)
    assert message.id == sent.message_id
    assert message.type == "orders.v1"
    assert message.json() == {"order_id": 3, "total": 1.0}
