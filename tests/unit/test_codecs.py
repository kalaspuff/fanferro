from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import json
import uuid
from collections.abc import Callable
from typing import Annotated, Any, Literal, NamedTuple

import msgspec
import pydantic
import pytest
from google.protobuf import struct_pb2, timestamp_pb2
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic.alias_generators import to_camel

import fanferro
from fanferro.codecs import (
    BytesCodec,
    CodecRegistry,
    DataclassCodec,
    EncodedBody,
    JsonCodec,
    ProtobufCodec,
    PydanticCodec,
    TextCodec,
    is_json_content_type,
    to_jsonable,
)
from fanferro.codecs._base import loads_json


class Color(enum.Enum):
    RED = "red"


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


class Model(pydantic.BaseModel):
    id: int
    name: str


class Cat(pydantic.BaseModel):
    kind: Literal["cat"] = "cat"
    lives: int


class Dog(pydantic.BaseModel):
    kind: Literal["dog"] = "dog"
    good: bool


class Point(msgspec.Struct):
    x: int
    y: int


@pytest.fixture
def registry() -> CodecRegistry:
    return CodecRegistry()


def test_dataclass_roundtrip(registry: CodecRegistry) -> None:
    order = Order(
        id=uuid.uuid4(),
        items=[Item("a", 2), Item("b")],
        color=Color.RED,
        created=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        total=decimal.Decimal("10.50"),
        tags={"x"},
        pair=(1, "one"),
    )
    encoded = registry.encode(order)
    assert encoded.content_type == "application/json"
    assert registry.decode(encoded.data, Order, encoded.content_type) == order


def test_dataclass_errors(registry: CodecRegistry) -> None:
    with pytest.raises(fanferro.DecodeError, match="missing required field"):
        registry.decode(b'{"sku": "x", "qty": 1}', Order, "application/json")
    with pytest.raises(fanferro.DecodeError, match=r"\$\.qty"):
        registry.decode(b'{"sku": "x", "qty": "many"}', Item, "application/json")
    with pytest.raises(fanferro.DecodeError):
        registry.decode(b"not json", Item, "application/json")


def test_pydantic(registry: CodecRegistry) -> None:
    encoded = registry.encode(Model(id=1, name="n"))
    assert registry.decode(encoded.data, Model, encoded.content_type) == Model(id=1, name="n")
    assert registry.decode(b'[{"id": 1, "name": "a"}]', list[Model], "application/json") == [Model(id=1, name="a")]
    animal = Annotated[Cat | Dog, pydantic.Field(discriminator="kind")]
    assert registry.decode(b'{"kind": "dog", "good": true}', animal, "application/json") == Dog(good=True)
    with pytest.raises(fanferro.DecodeError, match="validation failed"):
        registry.decode(b'{"id": "x"}', Model, "application/json")
    assert isinstance(registry.decoder_for(Model, None), PydanticCodec)


class Account(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(alias_generator=to_camel)

    user_id: int
    display_name: str = ""


class Renamed(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(serialize_by_alias=True)

    user_id: int = pydantic.Field(alias="uid")


def test_pydantic_models_with_aliases(registry: CodecRegistry) -> None:
    # Bodies carry field names unless the codec or the model says otherwise; validation accepted
    # only aliases, so a model with aliases could not decode its own messages.
    encoded = registry.encode(Account(userId=7, displayName="Ada"))
    assert json.loads(encoded.data) == {"user_id": 7, "display_name": "Ada"}
    assert registry.decode(encoded.data, Account, encoded.content_type) == Account(userId=7, displayName="Ada")
    by_alias = PydanticCodec(by_alias=True).encode(Account(userId=7))
    assert json.loads(by_alias.data) == {"userId": 7, "displayName": ""}
    assert registry.decode(by_alias.data, Account, "application/json") == Account(userId=7)
    assert registry.decode(b'[{"userId": 1}, {"user_id": 2}]', list[Account], None) == [
        Account(userId=1),
        Account(userId=2),
    ]
    # The model's own serialization setting is kept unless the codec overrides it.
    assert json.loads(registry.encode(Renamed(uid=3)).data) == {"uid": 3}
    assert json.loads(PydanticCodec(by_alias=False).encode(Renamed(uid=3)).data) == {"user_id": 3}


def test_msgspec(registry: CodecRegistry) -> None:
    encoded = registry.encode(Point(1, 2))
    assert registry.decode(encoded.data, Point, encoded.content_type) == Point(1, 2)
    with pytest.raises(fanferro.DecodeError):
        registry.decode(b'{"x": "no"}', Point, "application/json")


def test_protobuf(registry: CodecRegistry) -> None:
    message = timestamp_pb2.Timestamp(seconds=12, nanos=5)
    encoded = registry.encode(message)
    assert encoded.content_type == "application/x-protobuf"
    assert registry.decode(encoded.data, timestamp_pb2.Timestamp, encoded.content_type) == message
    assert registry.type_name(timestamp_pb2.Timestamp) == "google.protobuf.Timestamp"
    json_codec = ProtobufCodec(json=True)
    encoded_json = json_codec.encode(struct_pb2.Value(string_value="hi"))
    assert encoded_json.content_type == "application/json"
    assert json_codec.decode(encoded_json.data, struct_pb2.Value, "application/json").string_value == "hi"
    with pytest.raises(fanferro.DecodeError):
        registry.decode(b"\xff\xff", timestamp_pb2.Timestamp, "application/x-protobuf")


def test_plain_values(registry: CodecRegistry) -> None:
    assert registry.encode({"a": 1}) == EncodedBody(b'{"a":1}', "application/json")
    assert registry.encode("hi") == EncodedBody(b"hi", "text/plain; charset=utf-8")
    assert registry.encode(b"\x00") == EncodedBody(b"\x00", "application/octet-stream")
    assert registry.encode([1, "x"]).data == b'[1,"x"]'
    assert registry.decode(b'{"a": 1}', dict, "application/json") == {"a": 1}
    assert registry.decode(b'{"a": 1}', dict[str, int], "application/json") == {"a": 1}
    assert registry.decode(b'"text"', str, "application/json") == "text"
    assert registry.decode(b"text", str, "text/plain") == "text"
    assert registry.decode(b"\x01", bytes, None) == b"\x01"
    assert registry.decode(b"3", float, "application/json") == 3
    with pytest.raises(fanferro.DecodeError):
        registry.decode(b"[1]", dict, "application/json")
    with pytest.raises(fanferro.DecodeError):
        registry.decode(b"\xff", str, "text/plain")


def test_no_codec(registry: CodecRegistry) -> None:
    class Opaque:
        pass

    with pytest.raises(fanferro.EncodeError):
        registry.encode(Opaque())
    with pytest.raises(fanferro.DecodeError):
        registry.decode(b"{}", Opaque, "application/json")


def test_custom_codec_takes_precedence() -> None:
    class Upper:
        name = "upper"

        def can_encode(self, value: object) -> bool:
            return isinstance(value, str)

        def encode(self, value: Any) -> EncodedBody:
            return EncodedBody(value.upper().encode(), "text/x-upper")

        def can_decode(self, target: Any, content_type: str | None) -> bool:
            return target is str and content_type == "text/x-upper"

        def decode(self, data: bytes, target: Any, content_type: str | None) -> Any:
            return data.decode().lower()

        def type_name(self, target: Any) -> str | None:
            return None

    registry = CodecRegistry()
    registry.register(Upper())
    assert registry.encode("abc") == EncodedBody(b"ABC", "text/x-upper")
    assert registry.decode(b"ABC", str, "text/x-upper") == "abc"
    assert registry.by_name("upper").name == "upper"
    with pytest.raises(KeyError):
        registry.by_name("nope")


def test_encoders_are_cached_per_type_only_for_builtin_codecs() -> None:
    registry = CodecRegistry()
    assert registry.encoder_for({"a": 1}) is registry.encoder_for({"b": 2})

    class CsvRows:
        """Encodes only dicts that contain rows (a decision based on the value)."""

        name = "csv"

        def can_encode(self, value: object) -> bool:
            return isinstance(value, dict) and "rows" in value

        def encode(self, value: Any) -> EncodedBody:
            return EncodedBody(b"\n".join(row.encode() for row in value["rows"]), "text/csv")

        def can_decode(self, target: Any, content_type: str | None) -> bool:
            return False

        def decode(self, data: bytes, target: Any, content_type: str | None) -> Any:
            raise NotImplementedError

        def type_name(self, target: Any) -> str | None:
            return None

    registry.register(CsvRows())
    assert registry.encode({"rows": ["a", "b"]}) == EncodedBody(b"a\nb", "text/csv")
    assert registry.encode({"plain": 1}).content_type == "application/json"
    assert registry.encode({"rows": ["c"]}).content_type == "text/csv"


def test_registry_order_and_helpers() -> None:
    names = [codec.name for codec in CodecRegistry().codecs]
    assert names == ["protobuf", "pydantic", "msgspec", "dataclass", "bytes", "text", "json"]
    assert is_json_content_type("application/vnd.api+json")
    assert not is_json_content_type(None)
    assert to_jsonable({"d": decimal.Decimal("1.1"), "e": Color.RED, "t": datetime.date(2026, 1, 1)}) == {
        "d": "1.1",
        "e": "red",
        "t": "2026-01-01",
    }
    custom = JsonCodec(dumps=lambda value: "custom", loads=lambda data: {"parsed": True})
    assert custom.encode({}).data == b"custom"
    assert custom.decode(b"x", dict, None) == {"parsed": True}
    assert isinstance(
        CodecRegistry([TextCodec(), BytesCodec(), DataclassCodec()]).decoder_for(Item, None), DataclassCodec
    )


_JSON_VALUES = st.recursive(
    st.none() | st.booleans() | st.integers() | st.floats(allow_nan=False) | st.text(),
    lambda children: st.lists(children, max_size=4) | st.dictionaries(st.text(), children, max_size=4),
    max_leaves=8,
)


def _parsed(loads: Callable[[bytes], Any], data: bytes) -> tuple[Any, ...]:
    try:
        return ("value", loads(data))
    except ValueError as error:  # JSONDecodeError, UnicodeDecodeError
        return (type(error), str(error))


@given(
    _JSON_VALUES,
    st.sampled_from(["utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be", "utf-32", "utf-32-le", "utf-32-be"]),
)
def test_json_bodies_parse_like_json_loads(value: Any, encoding: str) -> None:
    data = json.dumps(value, ensure_ascii=False).encode(encoding, "surrogatepass")
    assert _parsed(loads_json, data) == _parsed(json.loads, data)


@given(st.binary(max_size=24))
def test_any_body_parses_like_json_loads(data: bytes) -> None:
    assert _parsed(loads_json, data) == _parsed(json.loads, data)


class Shade(enum.IntEnum):
    DARK = 1


class Mood(enum.StrEnum):
    CALM = "calm"


class Pair(NamedTuple):
    left: int
    right: str


@dataclasses.dataclass
class Wrapper:
    color: Color
    when: datetime.date
    amount: decimal.Decimal
    tags: frozenset[str]


_JSONABLE_LEAVES = (
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False)
    | st.text(max_size=6)
    | st.sampled_from([Color.RED, Shade.DARK, Mood.CALM])
    | st.datetimes()
    | st.dates()
    | st.times()
    | st.timedeltas()
    | st.uuids()
    | st.decimals(allow_nan=False)
    | st.binary(max_size=6)
    | st.builds(Pair, st.integers(), st.text(max_size=4))
    | st.builds(
        Wrapper, st.just(Color.RED), st.dates(), st.decimals(allow_nan=False), st.frozensets(st.text(max_size=3))
    )
)
_JSONABLE_VALUES = st.recursive(
    _JSONABLE_LEAVES,
    lambda children: (
        st.lists(children, max_size=3)
        | st.tuples(children, children)
        | st.frozensets(st.integers() | st.text(max_size=3), max_size=3)
        | st.dictionaries(
            st.text(max_size=4) | st.integers() | st.sampled_from([Color.RED, Mood.CALM]), children, max_size=3
        )
    ),
    max_leaves=8,
)


def _jsonable(function: Callable[[Any], Any], value: Any) -> tuple[Any, ...]:
    try:
        return ("value", function(value))
    except TypeError as error:
        return ("error", str(error))


@settings(max_examples=500)
@given(_JSONABLE_VALUES)
def test_to_jsonable_converts_as_before(value: Any) -> None:
    assert _jsonable(to_jsonable, value) == _jsonable(reference_to_jsonable, value)
    assert _jsonable(to_jsonable, [value, object()]) == _jsonable(reference_to_jsonable, [value, object()])


# The to_jsonable of fanferro 0.3, kept as the reference.


def reference_to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, enum.Enum):
        return reference_to_jsonable(value.value)
    if isinstance(value, datetime.datetime | datetime.date | datetime.time):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return value.total_seconds()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, decimal.Decimal):
        return str(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {f.name: reference_to_jsonable(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, dict):
        return {str(reference_to_jsonable(key)): reference_to_jsonable(item) for key, item in value.items()}
    if hasattr(value, "_asdict"):
        return reference_to_jsonable(value._asdict())
    if isinstance(value, list | tuple | set | frozenset):
        return [reference_to_jsonable(item) for item in value]
    if isinstance(value, bytes):
        try:
            return value.decode()
        except UnicodeDecodeError:
            return list(value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return model_dump(mode="json")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
