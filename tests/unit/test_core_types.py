from __future__ import annotations

import datetime
import pickle
import traceback
from decimal import Decimal

import pytest

import fanferro
import fanferro.errors
from fanferro import _core, testing

NATIVE_ERRORS = [
    value for value in vars(_core).values() if isinstance(value, type) and issubclass(value, BaseException)
]


class TestAttributeValue:
    def test_constructors(self) -> None:
        assert fanferro.AttributeValue.string("x").data_type == "String"
        number = fanferro.AttributeValue.number(Decimal("1.50"))
        assert number.base_type == "Number"
        assert number.value == "1.50"
        binary = fanferro.AttributeValue.binary(b"\x00\x01")
        assert binary.value == b"\x00\x01"
        custom = fanferro.AttributeValue("Number.int", 5)
        assert custom.data_type == "Number.int"
        assert custom.value == "5"

    def test_equality_and_hashing(self) -> None:
        assert fanferro.AttributeValue.string("a") == fanferro.AttributeValue.string("a")
        assert len({fanferro.AttributeValue.string("a"), fanferro.AttributeValue.string("a")}) == 1
        assert fanferro.AttributeValue.string("a") != fanferro.AttributeValue.number("1")

    @pytest.mark.parametrize(("data_type", "value"), [("Weird", "x"), ("Binary", 1), ("String", b"bytes")])
    def test_invalid(self, data_type: str, value: object) -> None:
        with pytest.raises((fanferro.ValidationError, TypeError)):
            fanferro.AttributeValue(data_type, value)  # type: ignore[arg-type]


class TestPayloadAndWire:
    def test_payload_fields(self) -> None:
        now = datetime.datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=datetime.UTC)
        payload = fanferro.Payload(
            b"data",
            content_type="application/json",
            message_type="t",
            id="1",
            time=now,
            source="s",
            subject="sub",
            data_schema="schema",
            headers={"a": "x", "b": 2, "c": True},
        )
        assert payload.data == b"data"
        assert payload.time == now
        assert payload.headers == {"a": "x", "b": 2, "c": True}
        replaced = payload.replace(id="2", headers={})
        assert replaced.id == "2"
        assert replaced.headers == {}
        assert payload.id == "1"
        assert payload == fanferro.Payload(
            b"data",
            content_type="application/json",
            message_type="t",
            id="1",
            time=now,
            source="s",
            subject="sub",
            data_schema="schema",
            headers={"a": "x", "b": 2, "c": True},
        )

    def test_naive_datetimes_are_utc(self) -> None:
        payload = fanferro.Payload(b"", time=datetime.datetime(2026, 1, 1, 12, 0))
        assert payload.time == datetime.datetime(2026, 1, 1, 12, 0, tzinfo=datetime.UTC)

    def test_unknown_keyword(self) -> None:
        with pytest.raises(TypeError):
            fanferro.Payload(b"", nope=1)  # type: ignore[call-arg]

    def test_wire_message(self) -> None:
        wire = fanferro.WireMessage("hi", {"a": "x", "n": 3, "b": b"\x01", "flag": True})
        assert wire.body == b"hi"
        assert wire.body_text == "hi"
        assert wire.attributes["n"] == fanferro.AttributeValue.number("3")
        assert wire.attributes["b"] == fanferro.AttributeValue.binary(b"\x01")
        assert wire.attributes["flag"] == fanferro.AttributeValue.string("true")
        assert fanferro.WireMessage(b"\xff").body_text is None
        assert wire.replace(body="x").body == b"x"


def test_helpers() -> None:
    assert len(fanferro.new_message_id()) == 36
    assert _core.is_sqs_safe_text("hello")
    assert not _core.is_sqs_safe_text("\x00")
    xray = _core.xray_from_traceparent("00-5759e988bd862e3fe1be46a994272793-53995c3f42cd8ad8-01")
    assert xray == "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"
    assert _core.traceparent_from_xray(xray) == "00-5759e988bd862e3fe1be46a994272793-53995c3f42cd8ad8-01"
    assert "fanferro.type" in _core.reserved_attributes()


def test_error_hierarchy() -> None:
    assert issubclass(fanferro.ResourceNotFoundError, fanferro.AwsError)
    assert issubclass(fanferro.MessageTooLargeError, fanferro.ValidationError)
    assert issubclass(fanferro.ProvisioningError, fanferro.FanferroError)
    assert issubclass(fanferro.HandlerTimeoutError, TimeoutError)
    assert fanferro.FanferroError.retryable is False


def test_native_errors_are_exported_by_the_errors_module() -> None:
    assert {fanferro.FanferroError, fanferro.AccessDeniedError, fanferro.ShutdownError} <= set(NATIVE_ERRORS)
    assert {error_type.__name__ for error_type in NATIVE_ERRORS} <= set(fanferro.errors.__all__)


@pytest.mark.parametrize("error_type", NATIVE_ERRORS, ids=lambda error_type: error_type.__name__)
def test_native_errors_name_their_module(error_type: type[Exception]) -> None:
    assert error_type.__module__ == "fanferro.errors"
    assert getattr(fanferro.errors, error_type.__name__) is error_type
    assert traceback.format_exception_only(error_type("boom")) == [f"fanferro.errors.{error_type.__name__}: boom\n"]
    assert fanferro.errors.error_details(error_type("boom"))["error_type"] == f"fanferro.errors.{error_type.__name__}"
    copy = pickle.loads(pickle.dumps(error_type("boom")))
    assert (type(copy), str(copy)) == (error_type, "boom")


async def test_aws_errors_pickle_with_their_attributes() -> None:
    async with testing.memory_client() as client:
        queue = client.queue("missing", provisioning=fanferro.Provisioning.assume())
        with pytest.raises(fanferro.ResourceNotFoundError) as raised:
            await queue.approximate_count()
    error = raised.value
    copy = pickle.loads(pickle.dumps(error))
    assert (type(copy), str(copy)) == (fanferro.ResourceNotFoundError, str(error))
    assert vars(copy) == vars(error)
    assert (copy.code, copy.operation, copy.status) == (
        "AWS.SimpleQueueService.NonExistentQueue",
        "GetQueueAttributes",
        400,
    )
