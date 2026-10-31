"""msgspec codec (``msgspec.Struct`` types, and containers of them)."""

from __future__ import annotations

import sys
from typing import Any

from fanferro.codecs._base import (
    JSON_CONTENT_TYPE,
    EncodedBody,
    SettingsEquality,
    is_json_content_type,
    media_type,
    referenced_types,
)
from fanferro.errors import DecodeError, EncodeError

__all__ = ["MsgspecCodec"]


def _struct() -> Any:
    module = sys.modules.get("msgspec")
    return getattr(module, "Struct", None)


class MsgspecCodec(SettingsEquality):
    """Encodes ``msgspec.Struct`` values as JSON and decodes JSON into them (fast and validated)."""

    name = "msgspec"

    def __init__(self) -> None:
        self.content_type = JSON_CONTENT_TYPE

    def can_encode(self, value: object) -> bool:
        struct = _struct()
        return struct is not None and isinstance(value, struct)

    def encode(self, value: Any) -> EncodedBody:
        import msgspec

        try:
            return EncodedBody(msgspec.json.encode(value), self.content_type)
        except Exception as error:
            raise EncodeError(f"cannot encode {type(value).__name__}: {error}") from error

    def can_decode(self, target: Any, content_type: str | None) -> bool:
        struct = _struct()
        if struct is None:
            return False
        is_struct = isinstance(target, type) and issubclass(target, struct)
        # Containers of structs too (`list[Point]`): msgspec decodes typing constructs.
        if not is_struct and not any(
            isinstance(referenced, type) and issubclass(referenced, struct) for referenced in referenced_types(target)
        ):
            return False
        return content_type is None or is_json_content_type(content_type) or media_type(content_type) == "text/plain"

    def decode(self, data: bytes, target: Any, content_type: str | None) -> Any:
        import msgspec

        try:
            return msgspec.json.decode(data, type=target)
        except msgspec.DecodeError as error:
            raise DecodeError(f"cannot decode {getattr(target, '__name__', target)}: {error}") from error

    def type_name(self, target: Any) -> str | None:
        return None
