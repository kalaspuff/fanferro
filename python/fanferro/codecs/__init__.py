"""Codecs: converting application objects to message bytes and back.

Built in (tried in this order):

=================  ==============================================  ====================
Codec              Encodes / decodes                                Content type
=================  ==============================================  ====================
``ProtobufCodec``  ``google.protobuf`` messages                     application/x-protobuf
``PydanticCodec``  pydantic models (and ``list[Model]``, unions...)  application/json
``MsgspecCodec``   ``msgspec.Struct``                               application/json
``DataclassCodec`` dataclasses (nested, typed)                      application/json
``BytesCodec``     ``bytes``                                        application/octet-stream
``TextCodec``      ``str``                                          text/plain
``JsonCodec``      dicts, lists, numbers, ``None``                  application/json
=================  ==============================================  ====================

Implement :class:`Codec` to support anything else (Avro, CBOR, MessagePack, ...) and register it
with ``Client(codecs=CodecRegistry([MyCodec(), *default_codecs()]))`` or
``client.codecs.register(MyCodec())``.
"""

from fanferro.codecs._aws import AwsNotificationCodec
from fanferro.codecs._base import (
    JSON_CONTENT_TYPE,
    OCTET_STREAM,
    PROTOBUF_CONTENT_TYPE,
    TEXT_CONTENT_TYPE,
    Codec,
    EncodedBody,
    is_json_content_type,
    media_type,
    to_jsonable,
)
from fanferro.codecs._dataclass import DataclassCodec
from fanferro.codecs._json import JsonCodec
from fanferro.codecs._msgspec import MsgspecCodec
from fanferro.codecs._protobuf import ProtobufCodec
from fanferro.codecs._pydantic import PydanticCodec
from fanferro.codecs._raw import BytesCodec, TextCodec
from fanferro.codecs._registry import CodecRegistry, default_codecs

__all__ = [
    "JSON_CONTENT_TYPE",
    "OCTET_STREAM",
    "PROTOBUF_CONTENT_TYPE",
    "TEXT_CONTENT_TYPE",
    "AwsNotificationCodec",
    "BytesCodec",
    "Codec",
    "CodecRegistry",
    "DataclassCodec",
    "EncodedBody",
    "JsonCodec",
    "MsgspecCodec",
    "ProtobufCodec",
    "PydanticCodec",
    "TextCodec",
    "default_codecs",
    "is_json_content_type",
    "media_type",
    "to_jsonable",
]
