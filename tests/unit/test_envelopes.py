from __future__ import annotations

import json

import pytest

import fanferro
from fanferro import envelopes


def payload(data: bytes = b'{"a":1}', content_type: str | None = "application/json") -> fanferro.Payload:
    return fanferro.Payload(data, content_type=content_type, message_type="orders.created", id="m-1", source="urn:svc")


class TestCloudEvents:
    def test_roundtrip(self) -> None:
        envelope = envelopes.CloudEvents()
        wire = envelope.wrap(payload())
        document = json.loads(wire.body)
        assert document["specversion"] == "1.0"
        assert document["data"] == {"a": 1}
        assert wire.attributes["content-type"].value == "application/cloudevents+json"
        assert wire.attributes["fanferro.type"].value == "orders.created"
        decoded = envelope.unwrap(wire)
        assert decoded is not None
        assert decoded.data == b'{"a":1}'
        assert decoded.message_type == "orders.created"

    def test_options(self) -> None:
        envelope = envelopes.CloudEvents(content_type_attribute=False, type_attribute=None, default_source="urn:x")
        wire = envelope.wrap(fanferro.Payload(b"x", content_type="text/plain"))
        assert wire.attributes == {}
        assert json.loads(wire.body)["source"] == "urn:x"

    def test_does_not_claim_other_messages(self) -> None:
        assert envelopes.CloudEvents().unwrap(fanferro.WireMessage(b'{"order": 1}')) is None

    def test_malformed(self) -> None:
        wire = fanferro.WireMessage(b'{"specversion":"1.0"}', {"content-type": "application/cloudevents+json"})
        with pytest.raises(fanferro.EnvelopeError):
            envelopes.CloudEvents().unwrap(wire)

    def test_invalid_header_names(self) -> None:
        with pytest.raises(fanferro.EnvelopeError):
            envelopes.CloudEvents().wrap(fanferro.Payload(b"{}", headers={"Bad-Name": "x"}))


def test_protobuf_envelope_roundtrip() -> None:
    envelope = envelopes.CloudEventsProtobuf()
    wire = envelope.wrap(payload(b"\x08\x96\x01", "application/x-protobuf"))
    assert wire.attributes["content-type"].value == "application/cloudevents+protobuf"
    decoded = envelope.unwrap(wire)
    assert decoded is not None
    assert decoded.data == b"\x08\x96\x01"


def test_raw_envelope() -> None:
    envelope = envelopes.Raw()
    wire = envelope.wrap(payload())
    assert wire.body == b'{"a":1}'
    assert wire.attributes["fanferro.id"].value == "m-1"
    decoded = envelope.unwrap(fanferro.WireMessage(b"anything"))
    assert decoded is not None
    assert decoded.data == b"anything"


def test_unwrap_first_and_protocol() -> None:
    chain = envelopes.default_envelopes()
    wire = envelopes.CloudEventsProtobuf().wrap(payload())
    decoded, envelope = envelopes.unwrap_first(chain, wire)
    assert envelope.name == "cloudevents-protobuf"
    assert decoded.id == "m-1"
    assert all(isinstance(e, envelopes.Envelope) for e in chain)
    with pytest.raises(fanferro.EnvelopeError):
        envelopes.unwrap_first([envelopes.CloudEvents()], fanferro.WireMessage(b"plain"))


class Legacy:
    name = "legacy"

    def wrap(self, p: fanferro.Payload) -> fanferro.WireMessage:
        return fanferro.WireMessage(json.dumps({"kind": p.message_type, "data": p.data.decode()}), {"legacy": "1"})

    def unwrap(self, wire: fanferro.WireMessage) -> fanferro.Payload | None:
        if "legacy" not in wire.attributes:
            return None
        document = json.loads(wire.body)
        return fanferro.Payload(document["data"], message_type=document["kind"], content_type="application/json")


def test_custom_envelope() -> None:
    legacy = Legacy()
    wire = legacy.wrap(payload())
    decoded, matched = envelopes.unwrap_first([legacy, *envelopes.default_envelopes()], wire)
    assert matched is legacy
    assert decoded.message_type == "orders.created"
    assert not envelopes.supports_headers(legacy)
    assert envelopes.supports_headers(envelopes.CloudEvents())


def test_broken_custom_envelope_is_wrapped() -> None:
    class Broken:
        name = "broken"

        def wrap(self, p: fanferro.Payload) -> fanferro.WireMessage:
            raise NotImplementedError

        def unwrap(self, wire: fanferro.WireMessage) -> fanferro.Payload | None:
            raise KeyError("boom")

    with pytest.raises(fanferro.EnvelopeError, match="broken"):
        envelopes.unwrap_first([Broken()], fanferro.WireMessage(b"x"))


class TestTomodachi:
    def test_importable_from_the_package(self) -> None:
        assert fanferro.envelopes.compat.TomodachiJSON is fanferro._core.TomodachiJSONEnvelope
        assert envelopes.compat.TomodachiProtobuf is fanferro._core.TomodachiProtobufEnvelope

    def test_options_equality_and_repr(self) -> None:
        envelope = envelopes.compat.TomodachiJSON(service_name="billing", service_uuid="u-1", compress_threshold=None)
        assert envelope.name == "tomodachi-json"
        assert (envelope.service_name, envelope.service_uuid, envelope.type_attribute) == (
            "billing",
            "u-1",
            "fanferro.type",
        )
        assert envelope.compress_threshold is None
        assert envelopes.compat.TomodachiProtobuf().compress_threshold == 60_000
        assert envelopes.compat.TomodachiProtobuf().name == "tomodachi-protobuf"
        # Equal settings, equal envelopes (declaring a resource again compares them).
        assert envelopes.compat.TomodachiJSON() == envelopes.compat.TomodachiJSON()
        assert hash(envelopes.compat.TomodachiJSON()) == hash(envelopes.compat.TomodachiJSON())
        assert envelopes.compat.TomodachiJSON() != envelopes.compat.TomodachiJSON(service_uuid="x")
        assert repr(envelope) == (
            "TomodachiJSONEnvelope(service_name='billing', service_uuid='u-1', type_attribute='fanferro.type', "
            "compress_threshold=None)"
        )
        assert not envelopes.supports_headers(envelope)

    def test_invalid_options(self) -> None:
        with pytest.raises(fanferro.ConfigurationError, match="compress_threshold"):
            envelopes.compat.TomodachiProtobuf(compress_threshold=-1)
        with pytest.raises(fanferro.ConfigurationError, match="'topic' or 'queue'"):
            envelopes.compat.TomodachiJSON().wrap_for(payload(), "bucket", "x")  # type: ignore[arg-type]

    def test_wraps_for_topics_and_queues(self) -> None:
        envelope = envelopes.compat.TomodachiJSON(service_uuid="u")
        document = json.loads(envelope.wrap_for(payload(), "topic", "order___2e_created").body)
        assert document["metadata"]["topic"] == "order.created"
        assert document["metadata"]["message_uuid"] == "u.m-1"
        assert document["service"]["name"] == "urn:svc"
        queued = json.loads(envelope.wrap_for(payload(), "queue", "orders").body)
        assert queued["Type"] == "Message"
        assert json.loads(queued["Message"])["data"] == {"a": 1}
        decoded = envelope.unwrap(fanferro.WireMessage(json.dumps(queued)))
        assert decoded is not None
        assert (decoded.id, decoded.data) == ("m-1", b'{"a":1}')

    def test_refuses_payloads_it_cannot_carry(self) -> None:
        with pytest.raises(fanferro.EnvelopeError, match="protobuf envelope"):
            envelopes.compat.TomodachiJSON().wrap(payload(b"\x00", "application/x-protobuf"))
        with pytest.raises(fanferro.EnvelopeError, match="JSON envelope"):
            envelopes.compat.TomodachiProtobuf().wrap(payload())

    @pytest.mark.parametrize(
        ("topic", "encoded"),
        [
            # What tomodachi 0.29's AWSSNSSQSTransport.encode_topic() returns.
            ("orders", "orders"),
            ("order.created", "order___2e_created"),
            ("order.created/v1.fifo", "order___2e_created___2f_v1.fifo"),
            ("åäö.x", "___c3a5____c3a4____c3b6____2e_x"),
            ("a_.b-c", "a____2e_b-c"),
        ],
    )
    def test_tomodachi_topic_names(self, topic: str, encoded: str) -> None:
        assert envelopes.compat.tomodachi_topic(topic) == encoded
