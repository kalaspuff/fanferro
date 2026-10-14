"""Envelopes: how a message and its metadata are laid out on the wire.

Built-in envelopes are implemented in Rust:

* :class:`Raw` - no envelope (default for publishing): the payload is the body; the message id
  and type travel in message attributes (``fanferro.id``, ``fanferro.type``), the content type
  too unless it is JSON. Any SQS/SNS client can read and write these messages.
* :class:`CloudEvents` - CloudEvents 1.0 structured JSON. JSON payloads are embedded verbatim,
  text as a string, binary as ``data_base64``. Headers become CloudEvents extension attributes.
* :class:`CloudEventsProtobuf` - CloudEvents 1.0 protobuf format (compact binary).

Consumers try their envelopes in order (default: CloudEvents, CloudEventsProtobuf, Raw), so a
queue can receive a mix of formats. Custom envelopes implement the :class:`Envelope` protocol::

    class LegacyEnvelope:
        name = "legacy"

        def wrap(self, payload: Payload) -> WireMessage:
            body = json.dumps({"kind": payload.message_type, "data": json.loads(payload.data)})
            return WireMessage(body, {"legacy": "1"})

        def unwrap(self, wire: WireMessage) -> Payload | None:
            if "legacy" not in wire.attributes:
                return None  # not ours: let the next envelope try
            document = json.loads(wire.body)
            return Payload(json.dumps(document["data"]), message_type=document["kind"], content_type="application/json")
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Protocol, runtime_checkable

from fanferro._core import CloudEventsEnvelope, CloudEventsProtobufEnvelope, Payload, RawEnvelope, WireMessage
from fanferro.errors import EnvelopeError

__all__ = [
    "CloudEvents",
    "CloudEventsProtobuf",
    "Envelope",
    "Raw",
    "default_envelopes",
    "supports_headers",
    "unwrap_first",
]

CloudEvents = CloudEventsEnvelope
"""CloudEvents 1.0 structured JSON envelope (``application/cloudevents+json``)."""

CloudEventsProtobuf = CloudEventsProtobufEnvelope
"""CloudEvents 1.0 protobuf envelope (``application/cloudevents+protobuf``)."""

Raw = RawEnvelope
"""No envelope (the default): payload bytes as the body, metadata in attributes."""


@runtime_checkable
class Envelope(Protocol):
    """Wraps payloads for transport and recognises/unwraps its own format."""

    @property
    def name(self) -> str:
        """Stable identifier of the format."""
        ...

    def wrap(self, payload: Payload, /) -> WireMessage:
        """Produces the wire representation of ``payload``."""
        ...

    def unwrap(self, wire: WireMessage, /) -> Payload | None:
        """Parses ``wire`` if it is in this format; returns ``None`` if it is not.

        Raise :class:`fanferro.errors.EnvelopeError` for messages in this format that are malformed.
        """
        ...


def default_envelopes() -> tuple[Envelope, ...]:
    """The envelopes consumers accept by default, in detection order."""
    return (CloudEvents(), CloudEventsProtobuf(), Raw())


def supports_headers(envelope: Envelope) -> bool:
    """Whether the envelope carries payload headers inside the body (CloudEvents extensions)."""
    explicit = getattr(envelope, "supports_headers", None)
    if isinstance(explicit, bool):
        return explicit
    return envelope.name in {"cloudevents", "cloudevents-protobuf"}


def unwrap_first(envelopes: Iterable[Envelope], wire: WireMessage) -> tuple[Payload, Envelope]:
    """Unwraps ``wire`` with the first envelope that recognises it.

    Raises:
        EnvelopeError: When no envelope recognises the message, or the matching one fails.
    """
    tried: list[str] = []
    for envelope in envelopes:
        try:
            payload = envelope.unwrap(wire)
        except EnvelopeError:
            raise
        except Exception as error:
            raise EnvelopeError(f"envelope {envelope.name!r} failed to unwrap the message: {error}") from error
        if payload is not None:
            return payload, envelope
        tried.append(envelope.name)
    raise EnvelopeError(f"no configured envelope recognised the message (tried {', '.join(tried) or 'none'})")


def as_tuple(envelopes: Sequence[Envelope] | Envelope | None) -> tuple[Envelope, ...] | None:
    """Normalizes a single envelope or a sequence (``None`` stays ``None``)."""
    if envelopes is None:
        return None
    if isinstance(envelopes, Sequence):
        return tuple(envelopes)
    return (envelopes,)
