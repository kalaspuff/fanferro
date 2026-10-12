"""The codec protocol and shared helpers."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import uuid
from typing import Any, NamedTuple, Protocol, runtime_checkable

__all__ = [
    "JSON_CONTENT_TYPE",
    "OCTET_STREAM",
    "PROTOBUF_CONTENT_TYPE",
    "TEXT_CONTENT_TYPE",
    "Codec",
    "EncodedBody",
    "is_json_content_type",
    "media_type",
    "to_jsonable",
]

JSON_CONTENT_TYPE = "application/json"
TEXT_CONTENT_TYPE = "text/plain; charset=utf-8"
OCTET_STREAM = "application/octet-stream"
PROTOBUF_CONTENT_TYPE = "application/x-protobuf"


class EncodedBody(NamedTuple):
    """The result of encoding a value."""

    data: bytes
    """Encoded bytes."""
    content_type: str
    """Media type of ``data``."""


@runtime_checkable
class Codec(Protocol):
    """Converts application objects to bytes and back.

    Codecs are tried in registry order: the first whose ``can_encode`` accepts a value encodes
    it, and the first whose ``can_decode`` accepts a target type decodes into it. Implement this
    protocol to support additional serialization libraries.
    """

    @property
    def name(self) -> str:
        """Short identifier, e.g. ``"json"``."""
        ...

    def can_encode(self, value: object, /) -> bool:
        """Whether this codec can encode ``value``."""
        ...

    def encode(self, value: Any, /) -> EncodedBody:
        """Encodes ``value``."""
        ...

    def can_decode(self, target: Any, content_type: str | None, /) -> bool:
        """Whether this codec can decode data of ``content_type`` into ``target``."""
        ...

    def decode(self, data: bytes, target: Any, content_type: str | None, /) -> Any:
        """Decodes ``data`` into an instance of ``target``."""
        ...

    def type_name(self, target: Any, /) -> str | None:
        """A canonical message type name for a class (or ``None`` to use the default naming)."""
        ...


def media_type(content_type: str | None) -> str | None:
    """Lowercased media type without parameters."""
    if content_type is None:
        return None
    return content_type.split(";", 1)[0].strip().lower()


def is_json_content_type(content_type: str | None) -> bool:
    """Whether ``content_type`` is JSON (``application/json``, ``*/*+json``, ``text/json``)."""
    essence = media_type(content_type)
    return essence is not None and (essence in {"application/json", "text/json"} or essence.endswith("+json"))


def to_jsonable(value: Any) -> Any:
    """Converts common Python values into JSON-compatible structures.

    Handles dataclasses, enums, datetimes (ISO 8601), dates, times, UUIDs, Decimals (as strings to
    preserve precision), bytes (UTF-8 or a list of ints), sets, tuples, mappings and objects with
    ``model_dump()`` (pydantic) or ``_asdict()`` (named tuples).
    """
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, enum.Enum):
        return to_jsonable(value.value)
    if isinstance(value, datetime.datetime | datetime.date | datetime.time):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, decimal.Decimal):
        return str(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {f.name: to_jsonable(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, dict):
        return {str(to_jsonable(key)): to_jsonable(item) for key, item in value.items()}
    if hasattr(value, "_asdict"):
        return to_jsonable(value._asdict())
    if isinstance(value, list | tuple | set | frozenset):
        return [to_jsonable(item) for item in value]
    if isinstance(value, bytes):
        try:
            return value.decode()
        except UnicodeDecodeError:
            return list(value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return model_dump(mode="json")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
