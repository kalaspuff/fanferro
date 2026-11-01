"""Messages exchanged with tomodachi services in a metadata protobuf envelope (``compat.MetadataProtobuf``).

The other side is played by :class:`Reference`, which builds and reads messages of the envelope's
protobuf schema with Python's protobuf, the way such a service's envelope does.
"""

from __future__ import annotations

import base64
import contextvars
import json
import zlib
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory, wrappers_pb2
from pydantic import BaseModel

import fanferro
from fanferro.envelopes import Raw, default_envelopes
from fanferro.envelopes.compat import MetadataProtobuf
from tests.conftest import ClientFactory
from tests.helpers import consume_until, drain_queue

if TYPE_CHECKING:
    from fanferro.middleware import HandlerNext

NO_ATTRIBUTES = Raw(content_type_attribute=None, type_attribute=None, id_attribute=None)
"""Sends bodies as they are, without attributes, like a tomodachi service."""

METADATA_FIELDS = (
    "uuid",
    "envelope",
    "topic",
    "origin",
    "timestamp",
    "encoding",
    "external_path",
    "correlation_id",
    "data_definition",
    "data_class",
)

correlation: contextvars.ContextVar[str | None] = contextvars.ContextVar("correlation", default=None)
"""The correlation id of the work at hand, as an application's logging context keeps it."""


def current_correlation_id() -> str | None:
    return correlation.get()


def _message_class() -> Any:
    """The envelope's ``Message`` (data and ``Metadata``), in a pool of its own."""
    file = descriptor_pb2.FileDescriptorProto(name="metadata_envelope.proto", package="envelope", syntax="proto3")
    optional = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
    metadata = file.message_type.add(name="Metadata")
    for number, field in enumerate(METADATA_FIELDS, start=1):
        metadata.field.add(
            name=field, number=number, type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING, label=optional
        )
    message = file.message_type.add(name="Message")
    message.field.add(name="data", number=1, type=descriptor_pb2.FieldDescriptorProto.TYPE_BYTES, label=optional)
    message.field.add(
        name="metadata",
        number=2,
        type=descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        label=optional,
        type_name=".envelope.Metadata",
    )
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("envelope.Message"))


EnvelopeMessage = _message_class()


class Reference:
    """Builds and reads messages like a service's own implementation of the envelope."""

    @staticmethod
    def build(data: bytes, encoding: str, topic: str, **metadata: str) -> str:
        message = EnvelopeMessage()
        message.data = data
        message.metadata.uuid = metadata.pop("uuid", "3090053a-24be-4b65-847c-91b951ff0074")
        message.metadata.envelope = "example-envelope--1.0.0"
        message.metadata.topic = topic
        message.metadata.origin = "legacy-orders"
        message.metadata.timestamp = "2026-11-01T15:33:45.242502Z"
        message.metadata.encoding = encoding
        for field, value in metadata.items():
            setattr(message.metadata, field, value)
        return base64.b64encode(message.SerializeToString()).decode("ascii")

    @staticmethod
    def read(body: bytes) -> tuple[Any, Any]:
        """The metadata and decoded data of an SQS message body (an SNS notification or queue document)."""
        message = EnvelopeMessage()
        message.ParseFromString(base64.b64decode(json.loads(body)["Message"]))
        encoding, data = message.metadata.encoding, message.data
        if encoding.startswith("gzip_"):
            data, encoding = zlib.decompress(data), encoding.removeprefix("gzip_")
        if encoding == "json":
            data = json.loads(data)
        elif encoding == "string":
            data = data.decode()
        return message.metadata, data


@fanferro.message_type("order-created")
class OrderCreated(BaseModel):
    order_id: int
    total: float


@dataclass
class Invoice:
    invoice_id: int


def _envelope(**options: Any) -> MetadataProtobuf:
    return MetadataProtobuf("example-envelope", version="1.0.0", **options)


async def test_consumes_what_services_publish_and_send(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory(accept_envelopes=[_envelope(), *default_envelopes()])
    client.types.register(wrappers_pb2.StringValue, "order-noted")
    topic_name, queue_name = name("metadata-topic"), name("metadata-in")
    topic = client.topic(topic_name)
    queue = client.queue(queue_name)
    client.subscription(topic_name, queue_name, raw_message_delivery=False)
    orders: list[fanferro.Message[OrderCreated]] = []
    notes: list[fanferro.Message[wrappers_pb2.StringValue]] = []
    direct: list[fanferro.Message[str]] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(OrderCreated)
    async def on_order(message: fanferro.Message[OrderCreated]) -> None:
        orders.append(message)

    @consumer.route(wrappers_pb2.StringValue)
    async def on_note(message: fanferro.Message[wrappers_pb2.StringValue]) -> None:
        notes.append(message)

    @consumer.default
    async def on_direct(message: fanferro.Message[str]) -> None:
        direct.append(message)

    note = wrappers_pb2.StringValue(value="hej")
    large = json.dumps({"order_id": 8, "total": 2.0, "padding": "x" * 70_000}).encode()
    async with client:
        await topic.publish(
            Reference.build(b'{"order_id": 7, "total": 1.5}', "json", "order-created", correlation_id="c-7"),
            envelope=NO_ATTRIBUTES,
        )
        await topic.publish(
            Reference.build(zlib.compress(large), "gzip_json", "order-created", uuid="u-8"), envelope=NO_ATTRIBUTES
        )
        await topic.publish(
            Reference.build(
                note.SerializeToString(),
                "proto",
                "order-noted",
                uuid="u-9",
                data_definition=note.DESCRIPTOR.file.name,
                data_class=note.DESCRIPTOR.name,
            ),
            envelope=NO_ATTRIBUTES,
        )
        # Sent straight to the queue: the envelope in a document shaped like an SNS notification.
        document = {"Type": "Message", "Message": Reference.build(b"hello", "string", "", uuid="u-10")}
        await queue.send(json.dumps(document), envelope=NO_ATTRIBUTES)
        await consume_until(consumer, lambda: len(orders) == 2 and len(notes) == 1 and len(direct) == 1)

    by_id = {message.body.order_id: message for message in orders}
    first = by_id[7]
    assert first.envelope == "metadata-protobuf"
    assert first.type == "order-created"
    assert first.id == "3090053a-24be-4b65-847c-91b951ff0074"
    assert first.source == "legacy-orders"
    assert first.time is not None
    assert first.time.microsecond == 242502
    assert first.headers["correlationid"] == "c-7"
    # Without a correlation id: one derived from the message uuid, the same on every delivery.
    assert by_id[8].headers["correlationid"] not in {"", "u-8"}
    assert notes[0].body.value == "hej"
    assert notes[0].payload.data_schema == "google/protobuf/wrappers.proto"
    assert notes[0].headers["dataclass"] == "StringValue"
    assert direct[0].body == "hello"
    assert direct[0].type is None


async def test_publishes_what_services_consume(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory(topic_prefix="env-")
    reader = client_factory(accept_envelopes=[NO_ATTRIBUTES])
    envelope = _envelope(service_name="billing", correlation_id=current_correlation_id)
    topic_name, queue_name, direct_name = name("metadata-out"), name("metadata-out"), name("metadata-direct")
    topic = client.topic(topic_name, envelope=envelope)
    direct_queue = client.queue(direct_name, envelope=envelope)
    client.subscription(topic_name, queue_name, raw_message_delivery=False)
    async with client, reader:
        token = correlation.set("c-request")
        try:
            sent = await topic.publish(OrderCreated(order_id=1, total=9.5))
            await topic.publish(wrappers_pb2.StringValue(value="hej"), headers={"correlationid": "c-explicit"})
        finally:
            correlation.reset(token)
        await topic.publish({"blob": "x" * 70_000})
        await direct_queue.send("hello")
        received = await _bodies(reader, reader.queue(queue_name), expected=3)
        [queued] = await _bodies(reader, reader.queue(direct_name), expected=1)

    messages = [Reference.read(body) for body in received]
    by_encoding = {metadata.encoding: (metadata, data) for metadata, data in messages}
    assert set(by_encoding) == {"json", "proto", "gzip_json"}
    metadata, data = by_encoding["json"]
    assert data == {"order_id": 1, "total": 9.5}
    assert metadata.uuid == sent.message_id
    assert metadata.envelope == "example-envelope--1.0.0"
    assert metadata.topic == topic_name, "the topic without the client's prefix"
    assert metadata.origin == "billing"
    assert metadata.correlation_id == "c-request"
    assert (metadata.data_definition, metadata.data_class) == ("", "")
    metadata, data = by_encoding["proto"]
    assert wrappers_pb2.StringValue.FromString(data).value == "hej"
    assert metadata.correlation_id == "c-explicit"
    assert (metadata.data_definition, metadata.data_class) == ("google/protobuf/wrappers.proto", "StringValue")
    metadata, data = by_encoding["gzip_json"]
    assert data == {"blob": "x" * 70_000}
    assert len(metadata.correlation_id) == 36, "a generated uuid when nothing gives one"
    metadata, data = Reference.read(queued)
    assert (metadata.encoding, metadata.topic, data) == ("string", "", "hello")


async def test_correlation_ids_follow_handlers(client_factory: ClientFactory, name: Any) -> None:
    envelope = _envelope(correlation_id=current_correlation_id)
    client = client_factory(accept_envelopes=[envelope, *default_envelopes()])

    async def correlated(message: fanferro.Message[Any], call_next: HandlerNext) -> None:
        value = message.headers.get("correlationid")
        token = correlation.set(value if isinstance(value, str) else None)
        try:
            await call_next(message)
        finally:
            correlation.reset(token)

    client.consume_middleware.append(correlated)
    orders = client.queue(name("metadata-orders"), envelope=envelope)
    invoices = client.queue(name("metadata-invoices"), envelope=envelope)
    consumer = client.consumer(orders, wait_time=1)

    @consumer.route(OrderCreated)
    async def on_order(message: fanferro.Message[OrderCreated]) -> None:
        await invoices.send(Invoice(invoice_id=message.body.order_id), type="invoice-created")

    async with client:
        sent = await orders.send(OrderCreated(order_id=4, total=1.0), headers={"correlationid": "c-chain"})
        await consume_until(consumer, lambda: consumer.stats().processed == 1)
        [invoice] = await drain_queue(client, invoices)

    assert invoice.type == "invoice-created"
    assert invoice.json() == {"invoice_id": 4}
    assert invoice.headers["correlationid"] == "c-chain"
    assert invoice.id != sent.message_id


async def _bodies(client: fanferro.Client, queue: fanferro.Queue, expected: int) -> list[bytes]:
    """The bodies of the SQS messages in ``queue`` as received, SNS notifications not unwrapped."""
    received: list[bytes] = []

    async def collect(message: fanferro.Message[bytes]) -> None:
        received.append(message.data)

    for _ in range(10):
        consumer = client.consumer(queue, collect, wait_time=1, sns_unwrap=False)
        await consumer.run_until_idle(idle_for=0.3 if client.broker is not None else 1.0, timeout=15)
        if len(received) >= expected:
            break
    return received
