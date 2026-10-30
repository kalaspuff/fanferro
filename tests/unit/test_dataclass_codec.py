"""The dataclass codec's compiled converters behave exactly like the converter they replaced.

`reference_convert` (end of this file) is the converter of fanferro 0.3: it inspected the target
type for every value and built every JSON path eagerly. The compiled converters must give the same
results and the same errors, messages included, except where the reference was wrong: it returned
parametrized generic dataclasses (`Box[int]`) and type aliases unconverted, could not resolve the
hints of `Box` itself on Python 3.12.3, and refused the string keys of `dict[int, ...]` that
encoding writes (tested separately below).
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import json
import types
import typing
import uuid
from collections.abc import Callable, Mapping, Sequence
from typing import Annotated, Any, Literal

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from fanferro.codecs._dataclass import convert
from fanferro.errors import DecodeError


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class Level(enum.IntEnum):
    LOW = 1
    HIGH = 2


@dataclasses.dataclass
class Item:
    sku: str
    qty: int = 1


@dataclasses.dataclass
class Order:
    id: uuid.UUID
    items: list[Item]
    color: Color
    created: datetime.datetime
    total: decimal.Decimal
    tags: set[str] = dataclasses.field(default_factory=set)
    note: str | None = None
    kind: Literal["a", "b"] = "a"
    pair: tuple[int, str] = (0, "")
    computed: int = dataclasses.field(default=0, init=False)


@dataclasses.dataclass
class Node:
    name: str
    children: list[Node] = dataclasses.field(default_factory=list)
    parent: Node | None = None


@dataclasses.dataclass
class Box[T]:
    value: T


TARGETS: list[Any] = [
    Any,
    object,
    None,
    int,
    float,
    str,
    bool,
    bytes,
    list,
    dict,
    list[int],
    list[str],
    list[Any],
    tuple[int, str],
    tuple[int, ...],
    tuple[()],
    set[str],
    frozenset[int],
    Sequence[int],
    dict[str, int],
    Mapping[str, Any],
    typing.List[int],  # noqa: UP006 - old spellings must keep working
    typing.Optional[int],  # noqa: UP045
    int | None,
    str | int,
    int | str,  # equal to the one before, spelled differently (as some below)
    typing.Union[str, int],  # noqa: UP007
    int | list[int] | None,
    list[typing.Optional[int]],  # noqa: UP045
    list[int | None],
    Literal["a", "b"],
    Literal["b", "a"],
    Literal[1, True],
    Annotated[int, "meta"],
    Annotated[list[Item], "meta"],
    Color,
    Level,
    datetime.datetime,
    datetime.date,
    datetime.time,
    datetime.timedelta,
    uuid.UUID,
    decimal.Decimal,
    Item,
    Order,
    Node,
    list[Node],
    dict[str, list[Item]],
]

KNOWN: list[tuple[Any, Any]] = [
    ({"sku": "a"}, Item),
    ({"sku": "a", "qty": "2"}, Item),
    ({"qty": 2}, Item),
    (
        {
            "id": "2c6f6c0e-6b3b-4f1d-9c5e-4b8c2a1f0e11",
            "items": [{"sku": "a", "qty": 2}, {"sku": "b"}],
            "color": "red",
            "created": "2026-01-01T10:00:00+00:00",
            "total": "10.50",
            "tags": ["x", "y"],
            "note": None,
            "kind": "b",
            "pair": [1, "one"],
            "computed": 5,
        },
        Order,
    ),
    ({"id": "nope", "items": [], "color": "red", "created": "x", "total": "1"}, Order),
    ({"name": "root", "children": [{"name": "a", "children": [{"name": "b"}]}]}, Node),
    ({"name": "root", "children": [{"name": 1}]}, Node),
    ({"name": "a", "parent": {"name": "b", "parent": 3}}, Node),
    ([1, "a"], tuple[int, str]),
    ([1, "a", 2], tuple[int, str]),
    ([1, 2, 3], tuple[int, ...]),
    ([], tuple[()]),
    ([1, 2], set[str]),
    (["a", "a"], set[str]),
    ({"a": [{"sku": "x"}]}, dict[str, list[Item]]),
    ({"a": [{"sku": "x"}, {"sku": "y", "qty": "no"}]}, dict[str, list[Item]]),
    ("blue", Color),
    ("green", Color),
    (2, Level),
    ("12:30:00", datetime.time),
    ("2026-01-01", datetime.date),
    (90.5, datetime.timedelta),
    ("x", datetime.timedelta),
    ("1.5e3", decimal.Decimal),
    ("abc", decimal.Decimal),
    ("text", bytes),
    ([104, 105], bytes),
    ([300], bytes),
    ([1.5], bytes),
    ("a", Literal["a", "b"]),
    (True, Literal[1, True]),
    ([1, "x"], int | list[int] | None),
]

json_values = st.recursive(
    st.none() | st.booleans() | st.integers() | st.floats(allow_nan=False) | st.text(max_size=8),
    lambda children: st.lists(children, max_size=4) | st.dictionaries(st.text(max_size=6), children, max_size=4),
    max_leaves=10,
)


def outcome(function: Callable[..., Any], value: Any, target: Any, path: str = "$") -> tuple[Any, ...]:
    try:
        return ("value", function(value, target, path))
    except DecodeError as error:
        return ("decode error", str(error))
    except Exception as error:  # other exceptions must be the same too
        return ("raised", type(error), str(error))


@pytest.mark.parametrize(("value", "target"), KNOWN)
def test_known_values_convert_as_before(value: Any, target: Any) -> None:
    assert outcome(convert, value, target) == outcome(reference_convert, value, target)
    assert outcome(convert, value, target, "body") == outcome(reference_convert, value, target, "body")


@settings(max_examples=1000)
@given(json_values, st.sampled_from(TARGETS))
def test_any_value_converts_as_before(value: Any, target: Any) -> None:
    assert outcome(convert, value, target) == outcome(reference_convert, value, target)


type When = datetime.datetime
type Items = list[Item]
type Pair[T] = tuple[T, T]
type Json = dict[str, Json] | list[Json] | str | int | float | bool | None
type Tree[T] = dict[str, Tree[T]] | T
UserId = typing.NewType("UserId", int)


class Shade(str, enum.Enum):  # noqa: UP042 - str() of such a member is its name, unlike StrEnum
    DARK = "dark"


class Size(int, enum.Enum):
    SMALL = 1


@dataclasses.dataclass
class Shipment:
    placed: When
    items: Items
    route: Pair[str]
    boxes: list[Box[Item]]
    owner: UserId
    labels: typing.Iterable[str] = ()
    extra: typing.MutableMapping[str, Item] = dataclasses.field(default_factory=dict)


def test_type_aliases_and_generic_dataclasses_are_converted() -> None:
    # They were returned as the JSON data (strings, dicts) without an error.
    value = {
        "placed": "2026-01-01T10:00:00+00:00",
        "items": [{"sku": "a"}],
        "route": ["here", "there"],
        "boxes": [{"value": {"sku": "b", "qty": 2}}],
        "owner": 7,
        "labels": ["x"],
        "extra": {"e": {"sku": "e"}},
    }
    shipment = convert(value, Shipment)
    assert shipment == Shipment(
        placed=datetime.datetime(2026, 1, 1, 10, tzinfo=datetime.UTC),
        items=[Item("a")],
        route=("here", "there"),
        boxes=[Box(Item("b", 2))],
        owner=UserId(7),
        labels=["x"],
        extra={"e": Item("e")},
    )
    with pytest.raises(DecodeError, match=r"^\$\.boxes\[0\]\.value\.qty: expected an integer, got str$"):
        convert({**value, "boxes": [{"value": {"sku": "b", "qty": "2"}}]}, Shipment)
    with pytest.raises(DecodeError, match=r"^\$\.owner: expected an integer, got str$"):
        convert({**value, "owner": "7"}, Shipment)
    # Without arguments, a type parameter accepts anything.
    assert convert({"value": {"sku": "b"}}, Box) == Box({"sku": "b"})


def test_recursive_type_aliases_are_converted() -> None:
    document = {"a": [1, "x", {"b": None, "c": 2.5}], "d": True}
    assert convert(document, Json) == document
    assert convert({"a": {"b": 1}, "c": 2}, Tree[int]) == {"a": {"b": 1}, "c": 2}
    with pytest.raises(DecodeError, match="matches no member"):
        convert({"a": {"b": "x"}}, Tree[int])


def test_keys_that_encoding_writes_as_strings_are_converted_back() -> None:
    @dataclasses.dataclass
    class Counts:
        by_id: dict[int, str]
        by_ratio: dict[float, int]
        by_flag: dict[bool, int]
        by_level: dict[Level, int]
        by_color: dict[Color, int]
        by_shade: dict[Shade, int] = dataclasses.field(default_factory=dict)
        by_size: dict[Size, int] = dataclasses.field(default_factory=dict)

    counts = Counts(
        {1: "a"}, {0.5: 1}, {True: 1, False: 0}, {Level.HIGH: 2}, {Color.RED: 1}, {Shade.DARK: 3}, {Size.SMALL: 4}
    )
    from fanferro.codecs import CodecRegistry

    registry = CodecRegistry()
    encoded = registry.encode(counts)
    assert json.loads(encoded.data)["by_shade"] == {"dark": 3}, "enum keys are written by value"
    assert registry.decode(encoded.data, Counts, encoded.content_type) == counts
    # Earlier versions wrote members of (str, Enum) and (int, Enum) classes by name.
    old = {"by_id": {}, "by_ratio": {}, "by_flag": {}, "by_level": {}, "by_color": {}, "by_shade": {"Shade.DARK": 3}}
    assert convert(old, Counts).by_shade == {Shade.DARK: 3}
    with pytest.raises(DecodeError, match=r"^\$\.by_id\.x: expected an integer, got str$"):
        convert({"by_id": {"x": "a"}, "by_ratio": {}, "by_flag": {}, "by_level": {}, "by_color": {}}, Counts)


def test_errors_name_the_json_path() -> None:
    with pytest.raises(DecodeError, match=r"^body\.a\[1\]\.qty: expected an integer, got str$"):
        convert({"a": [{"sku": "x"}, {"sku": "y", "qty": "no"}]}, dict[str, list[Item]], "body")


def test_equal_targets_keep_their_spelling() -> None:
    # Equal typing constructs can be spelled differently; errors name the target as it was written
    # and list the members in its order, whichever spelling was converted first.
    spellings = [
        (int | str, str | int, typing.Union[str, int]),  # noqa: UP007
        (typing.Optional[int], int | None),  # noqa: UP045
        (Literal[1, 2], Literal[2, 1]),
        (list[typing.Optional[str]], list[str | None]),  # noqa: UP045
    ]
    for targets in spellings:
        for target in [*targets, *reversed(targets)]:
            assert outcome(convert, [[]], target) == outcome(reference_convert, [[]], target), target


# The converter of fanferro 0.3, kept as the reference.


def reference_convert(value: Any, target: Any, path: str = "$") -> Any:
    if target is Any or target is object:
        return value
    origin = typing.get_origin(target)
    args = typing.get_args(target)
    if origin is typing.Annotated:
        return reference_convert(value, args[0], path)
    if origin in (typing.Union, types.UnionType):
        if value is None and type(None) in args:
            return None
        errors = []
        for arg in args:
            if arg is type(None):
                continue
            try:
                return reference_convert(value, arg, path)
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
                reference_convert(item, arg, f"{path}[{i}]")
                for i, (item, arg) in enumerate(zip(value, args, strict=True))
            )
        item_type = args[0] if args else Any
        items = [reference_convert(item, item_type, f"{path}[{i}]") for i, item in enumerate(value)]
        if origin in (set, frozenset):
            return origin(items)
        return tuple(items) if origin is tuple else items
    if origin in (dict, Mapping):
        if not isinstance(value, dict):
            raise DecodeError(f"{path}: expected an object, got {type(value).__name__}")
        key_type, value_type = args or (Any, Any)
        return {
            reference_convert(k, key_type, f"{path}.{k}"): reference_convert(v, value_type, f"{path}.{k}")
            for k, v in value.items()
        }
    if isinstance(target, type):
        return _reference_convert_class(value, target, path)
    return value


def _reference_convert_class(value: Any, target: type, path: str) -> Any:
    if dataclasses.is_dataclass(target):
        if not isinstance(value, dict):
            raise DecodeError(f"{path}: expected an object for {target.__name__}, got {type(value).__name__}")
        hints = typing.get_type_hints(target)
        kwargs = {}
        for f in dataclasses.fields(target):
            if not f.init:
                continue
            if f.name in value:
                kwargs[f.name] = reference_convert(value[f.name], hints.get(f.name, Any), f"{path}.{f.name}")
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
