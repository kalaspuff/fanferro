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
