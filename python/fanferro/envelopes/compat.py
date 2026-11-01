"""Envelopes of other messaging frameworks, to exchange messages with services built on them.

`tomodachi <https://github.com/kalaspuff/tomodachi>`_ services wrap the messages they publish to
SNS and send to SQS in an envelope of their own, ``tomodachi.envelope.JsonBase`` or
``tomodachi.envelope.ProtobufBase``. With these envelopes, implemented in Rust like the built-in
ones, fanferro services consume those messages and publish messages tomodachi services consume:

* :class:`TomodachiJSON` - ``JsonBase``: JSON data in a JSON document.
* :class:`TomodachiProtobuf` - ``ProtobufBase``: a protobuf message in a base64-encoded protobuf
  document.
* :class:`MetadataProtobuf` - envelopes that tomodachi services bring of their own, laid out as a
  protobuf message of the data and a metadata message (uuid, envelope name, topic, origin,
  timestamp, data encoding, correlation id, ...), base64-encoded. The envelope's name is a setting.

Both read messages that tomodachi published to a topic (delivered with or without raw message
delivery) or sent to a queue, data that tomodachi compressed included. The tomodachi topic is the
message type: an incoming message's type is its ``metadata.topic``, so routes for tomodachi
messages are declared for the topic name (``client.types.register(OrderCreated, "order-created")``
or ``@fanferro.message_type("order-created")``), and messages published to a topic name it.
Messages sent to a queue are wrapped in the document tomodachi's consumer expects there::

    from fanferro.envelopes import default_envelopes
    from fanferro.envelopes.compat import TomodachiJSON, tomodachi_topic

    client = fanferro.Client(
        topic_prefix="production-",  # tomodachi's aws_sns_sqs.topic_prefix
        accept_envelopes=[TomodachiJSON(), *default_envelopes()],  # before Raw, which accepts anything
    )
    orders = client.topic(tomodachi_topic("order.created"), envelope=TomodachiJSON(service_name="billing"))

A :class:`MetadataProtobuf` envelope recognises its messages by the name in ``metadata.envelope``
and carries the correlation id in the ``correlationid`` header; ``correlation_id`` gives it to
published messages from the application's context (a logging context, a request)::

    envelope = MetadataProtobuf("example-envelope", version="1.0.0", correlation_id=current_correlation_id)
"""

from __future__ import annotations

import re
import sys
from typing import Any

from fanferro._core import MetadataProtobufEnvelope, TomodachiJSONEnvelope, TomodachiProtobufEnvelope
from fanferro.errors import ConfigurationError

__all__ = [
    "CORRELATION_ID_HEADER",
    "DATA_CLASS_HEADER",
    "MetadataProtobuf",
    "TomodachiJSON",
    "TomodachiProtobuf",
    "tomodachi_topic",
]

TomodachiJSON = TomodachiJSONEnvelope
"""tomodachi's JSON envelope (``tomodachi.envelope.JsonBase``)."""

TomodachiProtobuf = TomodachiProtobufEnvelope
"""tomodachi's protobuf envelope (``tomodachi.envelope.ProtobufBase``)."""

MetadataProtobuf = MetadataProtobufEnvelope
"""A protobuf envelope of data and a metadata message, named by ``metadata.envelope``."""

CORRELATION_ID_HEADER = "correlationid"
"""The header holding the correlation id of :class:`MetadataProtobuf` messages."""

DATA_CLASS_HEADER = "dataclass"
"""The header holding the protobuf message name of :class:`MetadataProtobuf` data."""

_PROTOBUF_CONTENT_TYPE = "application/x-protobuf"

_TOPIC_CHARACTER = re.compile(r"(?!\.fifo$)([^a-zA-Z0-9_*#-])")


def tomodachi_topic(topic: str) -> str:
    """The SNS topic name tomodachi uses for ``topic``, to declare it with ``client.topic()``.

    tomodachi encodes each character SNS does not allow in topic names (``.``, ``/``, ...) as
    ``___``, its UTF-8 bytes in hex and ``_``: ``tomodachi_topic("order.created")`` is
    ``"order___2e_created"``. A ``.fifo`` suffix stays as it is. The topic prefix is not part of it:
    give the client tomodachi's prefix as ``topic_prefix``.
    """
    return _TOPIC_CHARACTER.sub(lambda match: "___" + match.group(1).encode().hex() + "_", topic)


def _outgoing_metadata(
    envelope: MetadataProtobufEnvelope,
    body: object,
    content_type: str | None,
    headers: dict[str, Any],
    data_schema: str | None,
) -> tuple[dict[str, Any], str | None]:
    """The headers and data schema of a message published with a :class:`MetadataProtobuf` envelope.

    The correlation id comes from the message's ``correlationid`` header, else from the envelope's
    ``correlation_id`` function (the envelope generates one when neither gives one). Protobuf data
    names its ``.proto`` file (the data schema) and message (the ``dataclass`` header), unless the
    message names them.
    """
    added: dict[str, Any] = {}
    function = envelope.correlation_id
    if function is not None and not headers.get(CORRELATION_ID_HEADER):
        correlation_id = function()
        if correlation_id is not None and not isinstance(correlation_id, str):
            raise ConfigurationError(
                f"the correlation_id function of {envelope.envelope!r} envelopes must return a string or None, "
                f"not {type(correlation_id).__name__}"
            )
        if correlation_id:
            added[CORRELATION_ID_HEADER] = correlation_id
    if content_type == _PROTOBUF_CONTENT_TYPE and _is_protobuf_message(body):
        descriptor: Any = getattr(body, "DESCRIPTOR")  # noqa: B009 - typed as Any
        if data_schema is None:
            data_schema = descriptor.file.name
        if DATA_CLASS_HEADER not in headers:
            added[DATA_CLASS_HEADER] = descriptor.name
    return ({**headers, **added} if added else headers), data_schema


def _is_protobuf_message(value: object) -> bool:
    module = sys.modules.get("google.protobuf.message")
    message = getattr(module, "Message", None)
    return message is not None and isinstance(value, message)
