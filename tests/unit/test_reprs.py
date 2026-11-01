"""The reprs of native types and of the routing and codec registries read as Python."""

from __future__ import annotations

import dataclasses
import datetime

import pytest

import fanferro
import fanferro.testing
from fanferro import _core
from fanferro import matchers as m
from fanferro.codecs import CodecRegistry, JsonCodec
from fanferro.message_types import MessageTypes

NAMESPACE = {name: getattr(_core, name) for name in dir(_core)} | {"datetime": datetime}


@dataclasses.dataclass
class OrderCreated:
    id: int


@pytest.mark.parametrize(
    "envelope",
    [
        _core.RawEnvelope(),
        _core.RawEnvelope(content_type_attribute=None, type_attribute='it\'s "quoted"', implicit_content_type=None),
        _core.CloudEventsEnvelope(),
        _core.CloudEventsEnvelope(content_type_attribute=False, type_attribute=None, default_source="urn:svc"),
        _core.CloudEventsProtobufEnvelope(),
        _core.CloudEventsProtobufEnvelope(type_attribute=None, default_source="ünïcode\n"),
    ],
)
def test_envelope_reprs_rebuild_the_envelope(envelope: object) -> None:
    assert eval(repr(envelope), NAMESPACE) == envelope


def test_envelope_reprs_use_python_literals() -> None:
    assert repr(_core.RawEnvelope(content_type_attribute=None)) == (
        "RawEnvelope(content_type_attribute=None, type_attribute='fanferro.type', id_attribute='fanferro.id', "
        "implicit_content_type='application/json')"
    )
    assert repr(_core.CloudEventsEnvelope()) == (
        "CloudEventsEnvelope(content_type_attribute=True, type_attribute='fanferro.type', default_source='fanferro')"
    )


def test_attribute_value_repr() -> None:
    text = fanferro.AttributeValue("String", "it's")
    assert repr(text) == "AttributeValue('String', \"it's\")"
    assert eval(repr(text), NAMESPACE) == text
    assert repr(fanferro.AttributeValue("Number.int", "42")) == "AttributeValue('Number.int', '42')"
    assert repr(fanferro.AttributeValue("Binary", b"secret")) == "AttributeValue('Binary', <6 bytes>)"


def test_payload_repr_shows_metadata_but_not_the_body() -> None:
    assert repr(fanferro.Payload(b"{}", content_type="application/json")) == (
        "Payload(<2 bytes>, content_type='application/json', message_type=None, id=None)"
    )
    payload = fanferro.Payload(
        b"secret",
        message_type="orders.created",
        id="m-1",
        time=datetime.datetime(2026, 11, 1, tzinfo=datetime.UTC),
        source="urn:svc",
        subject="o-1",
        data_schema="urn:schema",
        headers={"tenant": "acme"},
    )
    assert repr(payload) == (
        "Payload(<6 bytes>, content_type=None, message_type='orders.created', id='m-1', "
        "time=datetime.datetime(2026, 11, 1, 0, 0, tzinfo=datetime.timezone.utc), source='urn:svc', "
        "subject='o-1', data_schema='urn:schema', headers=['tenant'])"
    )
    assert "secret" not in repr(payload)
    assert "acme" not in repr(payload)


def test_wire_message_repr_names_attributes_only() -> None:
    wire = fanferro.WireMessage(b"x", {"tenant": "acme", "fanferro.type": "orders.created"})
    assert repr(wire) == "WireMessage(<1 byte>, attributes=['fanferro.type', 'tenant'])"
    assert repr(fanferro.WireMessage("")) == "WireMessage(<0 bytes>, attributes=[])"


async def test_memory_broker_repr_counts_resources() -> None:
    broker = fanferro.MemoryBroker("eu-north-1", "123456789012")
    assert repr(broker) == (
        "MemoryBroker('eu-north-1', '123456789012', <0 queues, 0 topics, 0 subscriptions, 0 buckets>)"
    )
    async with fanferro.testing.memory_client(broker=broker) as client:
        topic = client.topic("orders")
        client.queue("billing").subscribe_to(topic)
        await client.provision()
    assert repr(broker) == (
        "MemoryBroker('eu-north-1', '123456789012', <1 queues, 1 topics, 1 subscriptions, 0 buckets>)"
    )
    assert repr(fanferro.MemoryBroker(orphaned_polls=True)) == (
        "MemoryBroker('us-east-1', '000000000000', <0 queues, 0 topics, 0 subscriptions, 0 buckets>, "
        "orphaned_polls=True)"
    )


def test_codec_registry_repr_lists_codecs_in_lookup_order() -> None:
    assert repr(CodecRegistry()) == (
        "CodecRegistry(['protobuf', 'pydantic', 'msgspec', 'dataclass', 'bytes', 'text', 'json'])"
    )
    assert repr(CodecRegistry([JsonCodec()])) == "CodecRegistry(['json'])"


def test_message_types_repr_lists_names_and_aliases() -> None:
    types = MessageTypes()
    assert repr(types) == "MessageTypes({})"
    types.register(OrderCreated, "orders.created", "orders.created.v1")
    assert repr(types) == "MessageTypes({'orders.created': OrderCreated, 'orders.created.v1': OrderCreated})"


def test_router_repr_lists_routes_in_evaluation_order() -> None:
    router = fanferro.Router(name="orders")
    assert repr(router) == "Router(name='orders', routes=[])"

    @router.route(OrderCreated)
    async def created(order: OrderCreated) -> None: ...

    @router.route("orders.cancelled", when=m.header("version") >= 2, priority=3, name="cancelled-v2")
    async def cancelled(message: fanferro.Message) -> None: ...  # type: ignore[type-arg]

    @router.default
    async def unknown(message: fanferro.Message) -> None: ...  # type: ignore[type-arg]

    prefix = "test_router_repr_lists_routes_in_evaluation_order.<locals>."
    assert repr(router) == (
        "Router(name='orders', routes=["
        "Route('cancelled-v2', message_type='orders.cancelled', when=header('version') >= 2, priority=3), "
        f"Route('{prefix}created', message_type=OrderCreated), "
        f"Route('{prefix}unknown', fallback=True)])"
    )
    assert repr(fanferro.Router()) == "Router(routes=[])"
