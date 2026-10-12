"""Dataclass codec: dataclasses as JSON, decoded with a small type-directed converter."""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import functools
import json
import types
import typing
import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from fanferro.codecs._base import JSON_CONTENT_TYPE, EncodedBody, is_json_content_type, media_type, to_jsonable
from fanferro.errors import DecodeError, EncodeError

__all__ = ["DataclassCodec", "convert"]


@functools.cache
def _hints(cls: type) -> dict[str, Any]:
    return typing.get_type_hints(cls)


def convert(value: Any, target: Any, path: str = "$") -> Any:
    """Converts JSON data into ``target`` (dataclasses, containers, enums, dates, UUIDs, ...).

    Raises:
        DecodeError: When ``value`` does not fit ``target``; the message includes a JSON path.
    """
    if target is Any or target is object:
        return value
    origin = typing.get_origin(target)
    args = typing.get_args(target)
    if origin is typing.Annotated:
        return convert(value, args[0], path)
    if origin in (typing.Union, types.UnionType):
        if value is None and type(None) in args:
            return None
        errors = []
        for arg in args:
            if arg is type(None):
                continue
            try:
                return convert(value, arg, path)
            except DecodeError as error:
                errors.append(str(error))
        raise DecodeError(f"{path}: value matches no member of {target}: {'; '.join(errors)}")
    if origin is typing.Literal:
        if value in args:
            return value
        raise DecodeError(f"{path}: expected one of {args!r}, got {value!r}")
    if origin in (list, Sequence, set, frozenset, tuple):
        if not isinstance(value, list):
            raise DecodeError(f"{path}: expected an array, got {type(value).__name__}")
        if origin is tuple and args and args[-1] is not Ellipsis:
            if len(args) != len(value):
                raise DecodeError(f"{path}: expected {len(args)} items, got {len(value)}")
            return tuple(
                convert(item, arg, f"{path}[{i}]") for i, (item, arg) in enumerate(zip(value, args, strict=True))
            )
        item_type = args[0] if args else Any
        items = [convert(item, item_type, f"{path}[{i}]") for i, item in enumerate(value)]
        if origin in (set, frozenset):
            return origin(items)
        return tuple(items) if origin is tuple else items
    if origin in (dict, Mapping):
        if not isinstance(value, dict):
            raise DecodeError(f"{path}: expected an object, got {type(value).__name__}")
        key_type, value_type = args or (Any, Any)
        return {convert(k, key_type, f"{path}.{k}"): convert(v, value_type, f"{path}.{k}") for k, v in value.items()}
    if isinstance(target, type):
        return _convert_class(value, target, path)
    return value


def _convert_class(value: Any, target: type, path: str) -> Any:
    if dataclasses.is_dataclass(target):
        if not isinstance(value, dict):
            raise DecodeError(f"{path}: expected an object for {target.__name__}, got {type(value).__name__}")
        hints = _hints(target)
        kwargs = {}
        for f in dataclasses.fields(target):
            if not f.init:
                continue
            if f.name in value:
                kwargs[f.name] = convert(value[f.name], hints.get(f.name, Any), f"{path}.{f.name}")
            elif f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING:
                raise DecodeError(f"{path}: missing required field {f.name!r} for {target.__name__}")
        return target(**kwargs)
    if issubclass(target, enum.Enum):
        try:
            return target(value)
        except ValueError as error:
            raise DecodeError(f"{path}: {error}") from error
    if target is bool:
        if isinstance(value, bool):
            return value
        raise DecodeError(f"{path}: expected a boolean, got {type(value).__name__}")
    if target is int:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        raise DecodeError(f"{path}: expected an integer, got {type(value).__name__}")
    if target is float:
        if isinstance(value, int | float) and not isinstance(value, bool):
            return float(value)
        raise DecodeError(f"{path}: expected a number, got {type(value).__name__}")
    if target is str:
        if isinstance(value, str):
            return value
        raise DecodeError(f"{path}: expected a string, got {type(value).__name__}")
    try:
        if target is datetime.datetime:
            return datetime.datetime.fromisoformat(value)
        if target is datetime.date:
            return datetime.date.fromisoformat(value)
        if target is datetime.time:
            return datetime.time.fromisoformat(value)
        if target is datetime.timedelta:
            return datetime.timedelta(seconds=float(value))
        if target is uuid.UUID:
            return uuid.UUID(str(value))
        if target is decimal.Decimal:
            return decimal.Decimal(str(value))
    except (TypeError, ValueError, decimal.InvalidOperation) as error:
        raise DecodeError(f"{path}: cannot convert {value!r} to {target.__name__}: {error}") from error
    if target is bytes:
        if isinstance(value, str):
            return value.encode()
        if isinstance(value, list):
            return bytes(value)
    expected: Any = target
    if isinstance(value, expected):
        return value
    raise DecodeError(f"{path}: cannot convert {type(value).__name__} to {target.__name__}")


class DataclassCodec:
    """Encodes dataclass instances as JSON and decodes JSON into dataclasses.

    Nested dataclasses, lists, dicts, tuples, sets, optionals/unions, literals, enums, datetimes,
    dates, UUIDs and Decimals are converted according to the field type hints.
    """

    name = "dataclass"

    def __init__(self, content_type: str = JSON_CONTENT_TYPE) -> None:
        self.content_type = content_type

    def can_encode(self, value: object) -> bool:
        return dataclasses.is_dataclass(value) and not isinstance(value, type)

    def encode(self, value: Any) -> EncodedBody:
        try:
            data = json.dumps(to_jsonable(value), separators=(",", ":"), ensure_ascii=False)
        except (TypeError, ValueError) as error:
            raise EncodeError(f"cannot encode {type(value).__name__}: {error}") from error
        return EncodedBody(data.encode(), self.content_type)

    def can_decode(self, target: Any, content_type: str | None) -> bool:
        if (
            content_type is not None
            and not is_json_content_type(content_type)
            and media_type(content_type) != "text/plain"
        ):
            return False
        return isinstance(target, type) and dataclasses.is_dataclass(target)

    def decode(self, data: bytes, target: Any, content_type: str | None) -> Any:
        try:
            value = json.loads(data)
        except ValueError as error:
            raise DecodeError(f"message body is not valid JSON: {error}") from error
        return convert(value, target)

    def type_name(self, target: Any) -> str | None:
        return None
