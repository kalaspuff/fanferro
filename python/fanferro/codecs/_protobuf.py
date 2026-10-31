"""Protocol Buffers codec (binary by default, protobuf JSON optionally)."""

from __future__ import annotations

import sys
from typing import Any

from fanferro.codecs._base import (
    JSON_CONTENT_TYPE,
    PROTOBUF_CONTENT_TYPE,
    EncodedBody,
    SettingsEquality,
    is_json_content_type,
)
from fanferro.errors import DecodeError, EncodeError

__all__ = ["ProtobufCodec"]

_PROTOBUF_TYPES = {"application/x-protobuf", "application/protobuf", "application/vnd.google.protobuf"}


def _message_class() -> Any:
    module = sys.modules.get("google.protobuf.message")
    return getattr(module, "Message", None)


class ProtobufCodec(SettingsEquality):
    """Encodes protobuf messages with ``SerializeToString`` (or protobuf JSON).

    The default message type name of a protobuf class is its fully qualified name
    (``DESCRIPTOR.full_name``, e.g. ``acme.orders.v1.OrderCreated``). Decoding accepts both binary
    and JSON payloads, selected by content type.

    Args:
        json: Encode with ``google.protobuf.json_format`` instead of the binary wire format.
        deterministic: Use deterministic binary serialization (stable map ordering).
    """

    name = "protobuf"

    def __init__(self, *, json: bool = False, deterministic: bool = True) -> None:
        self.json = json
        self.deterministic = deterministic

    def can_encode(self, value: object) -> bool:
        message = _message_class()
        return message is not None and isinstance(value, message)

    def encode(self, value: Any) -> EncodedBody:
        try:
            if self.json:
                from google.protobuf import json_format

                return EncodedBody(json_format.MessageToJson(value, indent=None).encode(), JSON_CONTENT_TYPE)
            return EncodedBody(value.SerializeToString(deterministic=self.deterministic), PROTOBUF_CONTENT_TYPE)
        except Exception as error:
            raise EncodeError(f"cannot encode {type(value).__name__}: {error}") from error

    def can_decode(self, target: Any, content_type: str | None) -> bool:
        message = _message_class()
        return message is not None and isinstance(target, type) and issubclass(target, message)

    def decode(self, data: bytes, target: Any, content_type: str | None) -> Any:
        try:
            if is_json_content_type(content_type):
                from google.protobuf import json_format

                return json_format.Parse(data, target(), ignore_unknown_fields=True)
            return target.FromString(data)
        except Exception as error:
            raise DecodeError(f"cannot decode {target.__name__}: {error}") from error

    def type_name(self, target: Any) -> str | None:
        descriptor = getattr(target, "DESCRIPTOR", None)
        full_name = getattr(descriptor, "full_name", None)
        return full_name if isinstance(full_name, str) and self.can_decode(target, None) else None
