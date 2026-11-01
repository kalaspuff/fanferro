"""Parsing and encoding of AWS's JSON documents into frozen dataclasses (tolerant, as AWS asks)."""

from __future__ import annotations

import dataclasses
import email.utils
import functools
import re
import types
import typing
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar, Literal, Self
from urllib.parse import quote_plus, unquote_plus

from fanferro.errors import DecodeError

_METADATA = "fanferro.aws"

_INTEGER = re.compile(r"-?[0-9]+")
"""An integer as a string (ASCII digits only: ``str.isdigit()`` accepts ``²``, which ``int()`` rejects)."""


type _TimeFormat = Literal["iso", "rfc2822", "millis"]


@dataclass(frozen=True, slots=True)
class _Key:
    path: tuple[str, ...]
    url_encoded: bool = False
    time_format: _TimeFormat = "iso"


def _at(*path: str, url_encoded: bool = False, time_format: _TimeFormat = "iso") -> dict[str, Any]:
    """Field metadata: where the field is in AWS's document (default: its name in camel case).

    ``time_format`` is how a time is written: ISO 8601 (read in any format AWS uses), RFC 2822
    (``Fri, 27 Jan 2023 06:02:51 GMT``, AWS Health) or epoch milliseconds (Step Functions).
    """
    return {_METADATA: _Key(path, url_encoded, time_format)}


_SKIP: dict[str, Any] = {_METADATA: None}
"""Field metadata of fields that are not in the document."""


class AwsDocument:
    """Base of the classes of AWS notification documents (decoded and encoded by fanferro's codec)."""

    __slots__ = ()

    aws_message_type: ClassVar[str | None] = None
    """The message type fanferro gives the notification (``None``: not a notification of its own)."""

    @classmethod
    def from_json(cls, document: Mapping[str, Any]) -> Self:
        """Parses AWS's JSON document (as ``json.loads`` returns it).

        Raises:
            DecodeError: When the document lacks a required field or has a value of the wrong type.
        """
        return _parse(cls, document, ())

    def to_json(self) -> dict[str, Any]:
        """The JSON document AWS sends for this notification (fields AWS leaves out omitted)."""
        return _dump(self)


def _camel(name: str) -> str:
    head, *rest = name.rstrip("_").split("_")
    return head + "".join(part[:1].upper() + part[1:] for part in rest)


@functools.cache
def _fields(cls: type) -> tuple[tuple[str, _Key, Any, bool], ...]:
    """Each document field of ``cls``: name, key, type and whether it is required."""
    hints = typing.get_type_hints(cls)
    result = []
    for f in dataclasses.fields(cls):
        key = f.metadata.get(_METADATA, _Key((_camel(f.name),)))
        if key is None:
            continue
        required = f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING
        result.append((f.name, key, hints[f.name], required))
    return tuple(result)


def _render(path: tuple[str, ...]) -> str:
    return ".".join(path) or "document"


def _parse[T](cls: type[T], document: Any, path: tuple[str, ...]) -> T:
    custom = getattr(cls, "_from_document", None)
    return custom(document, path) if custom is not None else _parse_fields(cls, document, path)


def _parse_fields[T](cls: type[T], document: Any, path: tuple[str, ...], **values: Any) -> T:
    if not isinstance(document, Mapping):
        raise DecodeError(f"{_render(path)}: expected an object for {cls.__name__}, got {type(document).__name__}")
    for name, key, hint, required in _fields(cls):
        if name in values:
            continue
        value: Any = document
        for part in key.path:
            value = value.get(part) if isinstance(value, Mapping) else None
        if value is None:
            if required:
                raise DecodeError(f"{_render((*path, *key.path))}: missing required field for {cls.__name__}")
            continue
        if key.url_encoded and isinstance(value, str):
            value = unquote_plus(value)
        values[name] = _convert(value, hint, (*path, *key.path))
    return cls(**values)


def _convert(value: Any, hint: Any, path: tuple[str, ...]) -> Any:
    origin = typing.get_origin(hint)
    if origin in (types.UnionType, typing.Union):
        options = [arg for arg in typing.get_args(hint) if arg is not type(None)]
        return _convert(value, options[0], path)
    if hint is Any:
        return value
    if isinstance(hint, type) and issubclass(hint, AwsDocument):
        return _parse(hint, value, path)
    if origin is tuple:
        if not isinstance(value, list):
            raise DecodeError(f"{_render(path)}: expected a list, got {type(value).__name__}")
        (item,) = typing.get_args(hint)[:1]
        return tuple(_convert(entry, item, (*path, str(index))) for index, entry in enumerate(value))
    if origin is Mapping:
        if not isinstance(value, Mapping):
            raise DecodeError(f"{_render(path)}: expected an object, got {type(value).__name__}")
        item = typing.get_args(hint)[1]
        return {str(name): _convert(entry, item, (*path, str(name))) for name, entry in value.items()}
    if hint is datetime:
        return _time(value, path)
    if hint is bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.lower() in ("true", "false"):  # SES documents it as a string too
            return value.lower() == "true"
    elif hint is float:
        if isinstance(value, int | float) and not isinstance(value, bool):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                pass
    elif hint is int:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str) and _INTEGER.fullmatch(value.strip()):
            return int(value)
    elif hint is str:
        if isinstance(value, str):
            return value
        if isinstance(value, int | float) and not isinstance(value, bool):
            return str(value)
    raise DecodeError(f"{_render(path)}: expected {getattr(hint, '__name__', hint)}, got {value!r}")


def _time(value: Any, path: tuple[str, ...]) -> datetime:
    """A time in any format AWS writes: ISO 8601, RFC 2822 or epoch milliseconds."""
    if isinstance(value, int | float) and not isinstance(value, bool):
        try:
            return datetime.fromtimestamp(value / 1000, UTC)
        except (OverflowError, OSError, ValueError):  # out of range, or NaN
            raise DecodeError(f"{_render(path)}: expected a time, got {value!r}") from None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            try:
                parsed = email.utils.parsedate_to_datetime(value)
            except (TypeError, ValueError):
                parsed = None
        if parsed is not None:
            return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
    raise DecodeError(f"{_render(path)}: expected a time, got {value!r}")


def _dump(document: AwsDocument) -> dict[str, Any]:
    custom = getattr(document, "_to_document", None)
    return custom() if custom is not None else _dump_fields(document)


def _dump_fields(document: AwsDocument) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, key, _, _ in _fields(type(document)):
        value = getattr(document, name)
        if value is None:
            continue
        if key.url_encoded and isinstance(value, str):
            value = quote_plus(value, safe="/")
        elif isinstance(value, datetime) and key.time_format != "iso":
            value = (
                round(value.timestamp() * 1000)
                if key.time_format == "millis"
                else email.utils.format_datetime(value.astimezone(UTC), usegmt=True)
            )
        target = result
        for part in key.path[:-1]:
            target = target.setdefault(part, {})
        target[key.path[-1]] = _to_jsonable(value)
    return result


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, AwsDocument):
        return _dump(value)
    if isinstance(value, tuple):
        return [_to_jsonable(entry) for entry in value]
    if isinstance(value, Mapping):
        return {name: _to_jsonable(entry) for name, entry in value.items()}
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return value
