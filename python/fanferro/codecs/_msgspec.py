"""msgspec codec (``msgspec.Struct`` types)."""

from __future__ import annotations

import sys
from typing import Any

from fanferro.codecs._base import JSON_CONTENT_TYPE, EncodedBody, is_json_content_type, media_type
from fanferro.errors import DecodeError, EncodeError

__all__ = ["MsgspecCodec"]


def _struct() -> Any:
    module = sys.modules.get("msgspec")
    return getattr(module, "Struct", None)


class MsgspecCodec:
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
        if struct is None or not (isinstance(target, type) and issubclass(target, struct)):
            return False
        return content_type is None or is_json_content_type(content_type) or media_type(content_type) == "text/plain"

    def decode(self, data: bytes, target: Any, content_type: str | None) -> Any:
        import msgspec

        try:
            return msgspec.json.decode(data, type=target)
        except msgspec.DecodeError as error:
            raise DecodeError(f"cannot decode {target.__name__}: {error}") from error

    def type_name(self, target: Any) -> str | None:
        return None
