"""The codec protocol and shared helpers."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import json
import typing
import uuid
from typing import Any, NamedTuple, Protocol, runtime_checkable

__all__ = [
    "JSON_CONTENT_TYPE",
    "OCTET_STREAM",
    "PROTOBUF_CONTENT_TYPE",
    "TEXT_CONTENT_TYPE",
    "Codec",
    "EncodedBody",
    "SettingsEquality",
    "is_json_content_type",
    "loads_json",
    "media_type",
    "to_jsonable",
]

JSON_CONTENT_TYPE = "application/json"
TEXT_CONTENT_TYPE = "text/plain; charset=utf-8"
OCTET_STREAM = "application/octet-stream"
PROTOBUF_CONTENT_TYPE = "application/x-protobuf"


class SettingsEquality:
    """Makes codecs of the same class with the same settings equal.

    Declaring a topic or queue again compares its settings, its codec too: two ``JsonCodec()``
    are the same codec, not a conflict. Settings are the instance attributes.
    """

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        return vars(self) == vars(other)

    def __hash__(self) -> int:
        return hash(type(self))


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


# First bytes for which `json.loads` detects an encoding other than UTF-8 (BOMs, UTF-16/32).
_NOT_UTF8_LEADS = frozenset({0x00, 0xEF, 0xFE, 0xFF})


def loads_json(data: bytes | bytearray | str) -> Any:
    """``json.loads(data)``, without its encoding detection when that would choose UTF-8 anyway.

    Same results and errors as ``json.loads`` for every input.
    """
    if type(data) is bytes and len(data) > 1 and data[0] not in _NOT_UTF8_LEADS and data[1]:
        return json.loads(data.decode("utf-8", "surrogatepass"))
    return json.loads(data)


def referenced_types(target: Any) -> list[Any]:
    """``target`` and everything its typing constructs refer to, each once.

    That is the arguments of generic types and unions, the metadata of ``Annotated``, the values
    of ``type`` aliases (recursive ones too) and the supertypes of ``NewType``.
    """
    seen: list[Any] = []
    pending = [target]
    while pending:
        current = pending.pop()
        if any(current is known for known in seen):
            continue
        seen.append(current)
        if isinstance(current, typing.TypeAliasType):
            pending.append(current.__value__)
        elif isinstance(current, typing.NewType):
            pending.append(current.__supertype__)
        origin = typing.get_origin(current)
        if origin is not None:
            pending.append(origin)
            pending.extend(typing.get_args(current))
    return seen


# Type checks of `to_jsonable`, built once (a `X | Y` union would be built anew for every value).
_JSON_SCALARS = (bool, int, float, str)
_EXACT_JSON_SCALARS = frozenset({str, int, float, bool, type(None)})
_DATE_TIMES = (datetime.datetime, datetime.date, datetime.time)
_SEQUENCES = (list, tuple, set, frozenset)


def _json_key(key: Any) -> str:
    """A JSON object key; enum members by their value.

    ``str()`` of a member of a ``(str, Enum)`` class is its qualified name, not its value.
    """
    return str(to_jsonable(key.value if isinstance(key, enum.Enum) else key))


def to_jsonable(value: Any) -> Any:
    """Converts common Python values into JSON-compatible structures.

    Handles dataclasses, enums, datetimes (ISO 8601), dates, times, UUIDs, Decimals (as strings to
    preserve precision), bytes (UTF-8 or a list of ints), sets, tuples, mappings and objects with
    ``model_dump()`` (pydantic) or ``_asdict()`` (named tuples).
    """
    if type(value) in _EXACT_JSON_SCALARS or isinstance(value, _JSON_SCALARS):
        return value
    if isinstance(value, enum.Enum):
        return to_jsonable(value.value)
    if isinstance(value, _DATE_TIMES):
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
        return {_json_key(key): to_jsonable(item) for key, item in value.items()}
    if hasattr(value, "_asdict"):
        return to_jsonable(value._asdict())
    if isinstance(value, _SEQUENCES):
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
