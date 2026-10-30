"""Dataclass codec: dataclasses as JSON, decoded with a small type-directed converter."""

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import decimal
import enum
import functools
import json
import types
import typing
import uuid
from collections.abc import (
    Callable,
    Collection,
    Iterable,
    Mapping,
    MutableMapping,
    MutableSequence,
    MutableSet,
    Sequence,
)
from collections.abc import Set as AbstractSet
from typing import Any

from fanferro.codecs._base import (
    JSON_CONTENT_TYPE,
    EncodedBody,
    is_json_content_type,
    loads_json,
    media_type,
    referenced_types,
    to_jsonable,
)
from fanferro.errors import DecodeError, EncodeError

__all__ = ["DataclassCodec", "convert"]


@functools.cache
def _hints(cls: type) -> dict[str, Any]:
    try:
        return typing.get_type_hints(cls)
    except NameError:
        parameters = getattr(cls, "__type_params__", ())
        if not parameters:
            raise
        # Early Python 3.12 releases (3.12.3, for one) do not resolve the type parameters of
        # `class Box[T]` in string annotations (`from __future__ import annotations`).
        return typing.get_type_hints(cls, localns={parameter.__name__: parameter for parameter in parameters})


# Converters are compiled once per target type: a function per type that checks and converts a
# value without inspecting the type again. JSON paths for error messages are built lazily, only
# when a value does not fit: a path is the root string or `(parent path, segment, kind)`.
type _Path = str | tuple[Any, Any, int]
type _Converter = Callable[[Any, _Path], Any]
_FIELD, _INDEX, _KEY = 0, 1, 2
_CONVERTERS: dict[Any, _Converter] = {}


def _render(path: _Path) -> str:
    parts: list[str] = []
    while isinstance(path, tuple):
        path, segment, kind = path
        parts.append(segment if kind == _FIELD else f"[{segment}]" if kind == _INDEX else f".{segment}")
    parts.append(path)
    parts.reverse()
    return "".join(parts)


def convert(value: Any, target: Any, path: str = "$") -> Any:
    """Converts JSON data into ``target`` (dataclasses, containers, enums, dates, UUIDs, ...).

    Raises:
        DecodeError: When ``value`` does not fit ``target``; the message includes a JSON path.
    """
    return _converter(target)(value, path)


def _converter(target: Any) -> _Converter:
    # Equal typing constructs can be spelled differently (`int | str` and `str | int`, or
    # `Optional[int]` and `int | None` before Python 3.14), and error messages show the spelling,
    # so they are cached by it too. Classes are equal only to themselves.
    key = target if isinstance(target, type) else (target, repr(target))
    try:
        return _CONVERTERS[key]
    except KeyError:
        return _compile(target, key)
    except TypeError:  # unhashable typing construct: compiled for every use
        return _compile(target, None)


def _compile(target: Any, key: Any) -> _Converter:
    if isinstance(target, type) and dataclasses.is_dataclass(target):
        # Registered before its fields are compiled (on first use), so recursive types work.
        converter = _dataclass_converter(target)
    else:
        converter = _type_converter(target)
    if key is not None:
        _CONVERTERS[key] = converter
    return converter


def _identity(value: Any, path: _Path) -> Any:
    return value


# Abstract collection types are decoded into the concrete type JSON arrays and objects map to.
_ABSTRACT_SEQUENCES: dict[Any, Any] = {
    Sequence: list,
    MutableSequence: list,
    Collection: list,
    Iterable: list,
    AbstractSet: set,
    MutableSet: set,
}


def _type_parameters(cls: Any) -> tuple[Any, ...]:
    """The type parameters of a generic class (``class Box[T]`` or ``Generic[T]``)."""
    return getattr(cls, "__type_params__", ()) or getattr(cls, "__parameters__", ())


def _substitute(hint: Any, types_by_parameter: Mapping[Any, Any]) -> Any:
    """``hint`` with the type variables of a generic type replaced by its arguments."""
    if not types_by_parameter:
        return hint
    if isinstance(hint, typing.TypeVar):
        return types_by_parameter.get(hint, hint)
    parameters = getattr(hint, "__parameters__", ())
    if parameters:
        try:
            return hint[tuple(types_by_parameter.get(parameter, parameter) for parameter in parameters)]
        except TypeError:
            return hint
    return hint


def _lazy(resolve: Callable[[], Any]) -> _Converter:
    """The converter of the type ``resolve()`` returns, compiled on first use.

    A type alias may refer to itself (``type Json = dict[str, Json] | list[Json] | str | ...``): its
    converter is cached before that reference is compiled.
    """
    compiled: _Converter | None = None

    def convert_resolved(value: Any, path: _Path) -> Any:
        nonlocal compiled
        if compiled is None:
            compiled = _converter(resolve())
        return compiled(value, path)

    return convert_resolved


def _type_converter(target: Any) -> _Converter:
    if target is Any or target is object:
        return _identity
    if isinstance(target, typing.TypeAliasType):  # `type When = datetime` (PEP 695)
        return _lazy(lambda: target.__value__)
    if isinstance(target, typing.NewType):
        return _converter(target.__supertype__)
    origin = typing.get_origin(target)
    args = typing.get_args(target)
    if isinstance(origin, typing.TypeAliasType):  # a generic alias: `type Pair[T] = ...` as `Pair[int]`
        parameters = dict(zip(origin.__type_params__, args, strict=False))
        return _lazy(lambda: _substitute(origin.__value__, parameters))
    if origin is typing.Annotated:
        return _converter(args[0])
    if origin in (typing.Union, types.UnionType):
        return _union_converter(target, args)
    if origin is typing.Literal:
        return _literal_converter(args)
    if origin in (list, set, frozenset, tuple) or origin in _ABSTRACT_SEQUENCES:
        return _sequence_converter(_ABSTRACT_SEQUENCES.get(origin, origin), args)
    if origin in (dict, Mapping, MutableMapping):
        return _mapping_converter(args)
    if isinstance(origin, type) and dataclasses.is_dataclass(origin):  # a generic dataclass: `Box[Item]`
        return _dataclass_converter(origin, dict(zip(_type_parameters(origin), args, strict=False)))
    if isinstance(target, type):
        return _class_converter(target)
    return _identity


def _union_converter(target: Any, args: tuple[Any, ...]) -> _Converter:
    nullable = type(None) in args
    members = [_converter(arg) for arg in args if arg is not type(None)]

    def convert_union(value: Any, path: _Path) -> Any:
        if value is None and nullable:
            return None
        errors = []
        for member in members:
            try:
                return member(value, path)
            except DecodeError as error:
                errors.append(str(error))
        raise DecodeError(f"{_render(path)}: value matches no member of {target}: {'; '.join(errors)}")

    return convert_union


def _literal_converter(args: tuple[Any, ...]) -> _Converter:
    def convert_literal(value: Any, path: _Path) -> Any:
        if value in args:
            return value
        raise DecodeError(f"{_render(path)}: expected one of {args!r}, got {value!r}")

    return convert_literal


def _sequence_converter(origin: Any, args: tuple[Any, ...]) -> _Converter:
    if origin is tuple and args and args[-1] is not Ellipsis:
        members = [_converter(arg) for arg in args]

        def convert_fixed_tuple(value: Any, path: _Path) -> Any:
            if not isinstance(value, list):
                raise DecodeError(f"{_render(path)}: expected an array, got {type(value).__name__}")
            if len(members) != len(value):
                raise DecodeError(f"{_render(path)}: expected {len(members)} items, got {len(value)}")
            pairs = enumerate(zip(value, members, strict=True))
            return tuple(member(item, (path, i, _INDEX)) for i, (item, member) in pairs)

        return convert_fixed_tuple

    item = _converter(args[0] if args else Any)
    container: Any = origin if origin in (set, frozenset, tuple) else None

    def convert_items(value: Any, path: _Path) -> Any:
        if not isinstance(value, list):
            raise DecodeError(f"{_render(path)}: expected an array, got {type(value).__name__}")
        items = [item(entry, (path, i, _INDEX)) for i, entry in enumerate(value)]
        return items if container is None else container(items)

    return convert_items


# JSON object keys are strings (the codecs write other keys with str()): parsed back for these types.
_KEY_PARSERS: dict[type, Callable[[str], Any]] = {
    int: int,
    float: float,
    bool: {"True": True, "False": False, "true": True, "false": False}.__getitem__,
}


def _key_converter(target: Any) -> _Converter:
    convert_key = _converter(target)
    parse = _KEY_PARSERS.get(target) if isinstance(target, type) else None
    if isinstance(target, type) and issubclass(target, enum.Enum):
        # By value; fanferro 0.5 wrote members of (str, Enum) and (int, Enum) classes by name.
        by_text = {str(member): member for member in target} | {str(member.value): member for member in target}
        parse = by_text.__getitem__
    if parse is None:
        return convert_key

    def convert_string_key(value: Any, path: _Path) -> Any:
        if isinstance(value, str):
            with contextlib.suppress(KeyError, ValueError):  # not parseable: reported by `convert_key`
                value = parse(value)
        return convert_key(value, path)

    return convert_string_key


def _mapping_converter(args: tuple[Any, ...]) -> _Converter:
    key_type, value_type = args or (Any, Any)
    key, item = _key_converter(key_type), _converter(value_type)

    def convert_mapping(value: Any, path: _Path) -> Any:
        if not isinstance(value, dict):
            raise DecodeError(f"{_render(path)}: expected an object, got {type(value).__name__}")
        return {key(k, (path, k, _KEY)): item(v, (path, k, _KEY)) for k, v in value.items()}

    return convert_mapping


def _dataclass_converter(target: type, types_by_parameter: Mapping[Any, Any] | None = None) -> _Converter:
    compiled: list[tuple[str, str, _Converter, bool]] | None = None

    def fields() -> list[tuple[str, str, _Converter, bool]]:
        # On first use: the hints may refer to classes defined later, or to this class itself.
        nonlocal compiled
        hints = _hints(target)
        result = []
        for f in dataclasses.fields(target):
            if f.init:
                required = f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING
                hint = _substitute(hints.get(f.name, Any), types_by_parameter or {})
                result.append((f.name, f".{f.name}", _converter(hint), required))
        compiled = result
        return result

    def convert_dataclass(value: Any, path: _Path) -> Any:
        if not isinstance(value, dict):
            raise DecodeError(f"{_render(path)}: expected an object for {target.__name__}, got {type(value).__name__}")
        kwargs = {}
        for name, segment, convert_field, required in fields() if compiled is None else compiled:
            if name in value:
                kwargs[name] = convert_field(value[name], (path, segment, _FIELD))
            elif required:
                raise DecodeError(f"{_render(path)}: missing required field {name!r} for {target.__name__}")
        return target(**kwargs)

    return convert_dataclass


_PARSERS: dict[type, Callable[[Any], Any]] = {
    datetime.datetime: datetime.datetime.fromisoformat,
    datetime.date: datetime.date.fromisoformat,
    datetime.time: datetime.time.fromisoformat,
    datetime.timedelta: lambda value: datetime.timedelta(seconds=float(value)),
    uuid.UUID: lambda value: uuid.UUID(str(value)),
    decimal.Decimal: lambda value: decimal.Decimal(str(value)),
}


def _class_converter(target: type) -> _Converter:
    if issubclass(target, enum.Enum):

        def convert_enum(value: Any, path: _Path) -> Any:
            try:
                return target(value)
            except ValueError as error:
                raise DecodeError(f"{_render(path)}: {error}") from error

        return convert_enum
    if target is bool:

        def convert_bool(value: Any, path: _Path) -> Any:
            if isinstance(value, bool):
                return value
            raise DecodeError(f"{_render(path)}: expected a boolean, got {type(value).__name__}")

        return convert_bool
    if target is int:

        def convert_int(value: Any, path: _Path) -> Any:
            if isinstance(value, int) and not isinstance(value, bool):
                return value
            raise DecodeError(f"{_render(path)}: expected an integer, got {type(value).__name__}")

        return convert_int
    if target is float:

        def convert_float(value: Any, path: _Path) -> Any:
            if isinstance(value, int | float) and not isinstance(value, bool):
                return float(value)
            raise DecodeError(f"{_render(path)}: expected a number, got {type(value).__name__}")

        return convert_float
    if target is str:

        def convert_str(value: Any, path: _Path) -> Any:
            if isinstance(value, str):
                return value
            raise DecodeError(f"{_render(path)}: expected a string, got {type(value).__name__}")

        return convert_str
    parse = _PARSERS.get(target)
    if parse is not None:

        def convert_parsed(value: Any, path: _Path) -> Any:
            try:
                return parse(value)
            except (TypeError, ValueError, decimal.InvalidOperation) as error:
                raise DecodeError(f"{_render(path)}: cannot convert {value!r} to {target.__name__}: {error}") from error

        return convert_parsed

    def convert_instance(value: Any, path: _Path) -> Any:
        if target is bytes:
            if isinstance(value, str):
                return value.encode()
            if isinstance(value, list):
                return bytes(value)
        expected: Any = target
        if isinstance(value, expected):
            return value
        raise DecodeError(f"{_render(path)}: cannot convert {type(value).__name__} to {target.__name__}")

    return convert_instance


def _converted(target: Any) -> bool:
    """Whether the converter turns JSON data into something else for ``target``."""
    if typing.get_origin(target) in (tuple, set, frozenset) or target in (tuple, set, frozenset):
        return True
    return isinstance(target, type) and (
        dataclasses.is_dataclass(target) or issubclass(target, enum.Enum) or target in _PARSERS
    )


# One encoder for every call: json.dumps would create one per call for these options.
_ENCODER = json.JSONEncoder(separators=(",", ":"), ensure_ascii=False)


class DataclassCodec:
    """Encodes dataclass instances as JSON and decodes JSON into dataclasses.

    Nested dataclasses (generic ones too), lists, dicts (with int, float, bool or enum keys too),
    tuples, sets, optionals/unions, literals, enums, datetimes, dates, UUIDs, Decimals, ``NewType``
    and ``type`` aliases are converted according to the field type hints.
    """

    name = "dataclass"

    def __init__(self, content_type: str = JSON_CONTENT_TYPE) -> None:
        self.content_type = content_type

    def can_encode(self, value: object) -> bool:
        return dataclasses.is_dataclass(value) and not isinstance(value, type)

    def encode(self, value: Any) -> EncodedBody:
        try:
            data = _ENCODER.encode(to_jsonable(value))
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
        if isinstance(target, type) and dataclasses.is_dataclass(target):
            return True
        # Other targets built from what the converter makes of JSON (`list[Item]`, `Item | None`,
        # enums, datetimes, tuples and sets): left to the JSON codec, they stayed plain data.
        return any(_converted(referenced) for referenced in referenced_types(target))

    def decode(self, data: bytes, target: Any, content_type: str | None) -> Any:
        try:
            value = loads_json(data)
        except ValueError as error:
            raise DecodeError(f"message body is not valid JSON: {error}") from error
        return convert(value, target)

    def type_name(self, target: Any) -> str | None:
        return None
