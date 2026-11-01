"""Envelopes of other messaging frameworks, to exchange messages with services built on them.

`tomodachi <https://github.com/kalaspuff/tomodachi>`_ services wrap the messages they publish to
SNS and send to SQS in an envelope of their own, ``tomodachi.envelope.JsonBase`` or
``tomodachi.envelope.ProtobufBase``. With these envelopes, implemented in Rust like the built-in
ones, fanferro services consume those messages and publish messages tomodachi services consume:

* :class:`TomodachiJSON` - ``JsonBase``: JSON data in a JSON document.
* :class:`TomodachiProtobuf` - ``ProtobufBase``: a protobuf message in a base64-encoded protobuf
  document.

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
"""

from __future__ import annotations

import re

from fanferro._core import TomodachiJSONEnvelope, TomodachiProtobufEnvelope

__all__ = ["TomodachiJSON", "TomodachiProtobuf", "tomodachi_topic"]

TomodachiJSON = TomodachiJSONEnvelope
"""tomodachi's JSON envelope (``tomodachi.envelope.JsonBase``)."""

TomodachiProtobuf = TomodachiProtobufEnvelope
"""tomodachi's protobuf envelope (``tomodachi.envelope.ProtobufBase``)."""

_TOPIC_CHARACTER = re.compile(r"(?!\.fifo$)([^a-zA-Z0-9_*#-])")


def tomodachi_topic(topic: str) -> str:
    """The SNS topic name tomodachi uses for ``topic``, to declare it with ``client.topic()``.

    tomodachi encodes each character SNS does not allow in topic names (``.``, ``/``, ...) as
    ``___``, its UTF-8 bytes in hex and ``_``: ``tomodachi_topic("order.created")`` is
    ``"order___2e_created"``. A ``.fifo`` suffix stays as it is. The topic prefix is not part of it:
    give the client tomodachi's prefix as ``topic_prefix``.
    """
    return _TOPIC_CHARACTER.sub(lambda match: "___" + match.group(1).encode().hex() + "_", topic)
