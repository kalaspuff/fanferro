"""JSON codec for plain Python data (dicts, lists, primitives and common value types)."""

from __future__ import annotations

import json
import types
import typing
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from fanferro.codecs._base import (
    JSON_CONTENT_TYPE,
    EncodedBody,
    is_json_content_type,
    loads_json,
    media_type,
    to_jsonable,
)
from fanferro.errors import DecodeError, EncodeError

__all__ = ["JsonCodec"]

_PLAIN_TARGETS: tuple[Any, ...] = (Any, object, dict, list, str, int, float, bool, type(None), None)


_ENCODER = json.JSONEncoder(separators=(",", ":"), ensure_ascii=False, default=to_jsonable)


def _default_dumps(value: Any) -> bytes:
    # One encoder for every call: json.dumps would create one per call for these options.
    return _ENCODER.encode(value).encode()


class JsonCodec:
    """Encodes plain data as JSON and decodes JSON into plain data.

    Args:
        dumps: Custom serializer returning ``bytes`` or ``str`` (e.g. ``orjson.dumps``).
        loads: Custom parser accepting ``bytes`` (e.g. ``orjson.loads``).
        content_type: Content type written for encoded values.
    """

    name = "json"

    def __init__(
        self,
        *,
        dumps: Callable[[Any], bytes | str] | None = None,
        loads: Callable[[bytes], Any] | None = None,
        content_type: str = JSON_CONTENT_TYPE,
    ) -> None:
        self._dumps = dumps or _default_dumps
        self._loads = loads or loads_json
        self.content_type = content_type

    def can_encode(self, value: object) -> bool:
        return value is None or isinstance(value, Mapping | list | tuple | int | float | bool | str)

    def encode(self, value: Any) -> EncodedBody:
        try:
            data = self._dumps(value)
        except (TypeError, ValueError) as error:
            raise EncodeError(f"cannot encode {type(value).__name__} as JSON: {error}") from error
        return EncodedBody(data.encode() if isinstance(data, str) else data, self.content_type)

    def can_decode(self, target: Any, content_type: str | None) -> bool:
        if (
            content_type is not None
            and not is_json_content_type(content_type)
            and media_type(content_type) != "text/plain"
        ):
            return False
        origin = typing.get_origin(target)
        if target in _PLAIN_TARGETS or origin in (dict, list, Mapping, Sequence):
            return True
        if origin in (typing.Union, types.UnionType):
            return all(arg in _PLAIN_TARGETS for arg in typing.get_args(target))
        return False

    def decode(self, data: bytes, target: Any, content_type: str | None) -> Any:
        try:
            value = self._loads(data)
        except (ValueError, TypeError) as error:
            raise DecodeError(f"message body is not valid JSON: {error}") from error
        expected = typing.get_origin(target) or target
        if expected in (dict, Mapping) and not isinstance(value, dict):
            raise DecodeError(f"expected a JSON object, got {type(value).__name__}")
        if expected in (list, Sequence) and not isinstance(value, list):
            raise DecodeError(f"expected a JSON array, got {type(value).__name__}")
        int_as_float = expected is float and isinstance(value, int) and not isinstance(value, bool)
        if expected in (str, int, float, bool) and not isinstance(value, expected) and not int_as_float:
            raise DecodeError(f"expected {expected.__name__}, got {type(value).__name__}")
        return value

    def type_name(self, target: Any) -> str | None:
        return None
