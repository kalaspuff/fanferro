"""Codecs for raw bytes and text."""

from __future__ import annotations

from typing import Any

from fanferro.codecs._base import OCTET_STREAM, TEXT_CONTENT_TYPE, EncodedBody, SettingsEquality, is_json_content_type
from fanferro.errors import DecodeError

__all__ = ["BytesCodec", "TextCodec"]


class BytesCodec(SettingsEquality):
    """Passes ``bytes`` through unchanged (``application/octet-stream``)."""

    name = "bytes"

    def __init__(self, content_type: str = OCTET_STREAM) -> None:
        self.content_type = content_type

    def can_encode(self, value: object) -> bool:
        return isinstance(value, bytes | bytearray | memoryview)

    def encode(self, value: Any) -> EncodedBody:
        return EncodedBody(bytes(value), self.content_type)

    def can_decode(self, target: Any, content_type: str | None) -> bool:
        return target in (bytes, bytearray, memoryview)

    def decode(self, data: bytes, target: Any, content_type: str | None) -> Any:
        return target(data) if target is not bytes else bytes(data)

    def type_name(self, target: Any) -> str | None:
        return None


class TextCodec(SettingsEquality):
    """Encodes ``str`` as UTF-8 text (``text/plain; charset=utf-8``)."""

    name = "text"

    def __init__(self, content_type: str = TEXT_CONTENT_TYPE) -> None:
        self.content_type = content_type

    def can_encode(self, value: object) -> bool:
        return isinstance(value, str)

    def encode(self, value: Any) -> EncodedBody:
        return EncodedBody(value.encode(), self.content_type)

    def can_decode(self, target: Any, content_type: str | None) -> bool:
        return target is str and not is_json_content_type(content_type)

    def decode(self, data: bytes, target: Any, content_type: str | None) -> Any:
        try:
            return data.decode()
        except UnicodeDecodeError as error:
            raise DecodeError(f"message body is not valid UTF-8 text: {error}") from error

    def type_name(self, target: Any) -> str | None:
        return None
