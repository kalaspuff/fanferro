from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import uuid
from typing import Annotated, Any, Literal

import msgspec
import pydantic
import pytest
from google.protobuf import struct_pb2, timestamp_pb2

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
