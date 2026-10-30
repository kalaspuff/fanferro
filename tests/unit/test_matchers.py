from __future__ import annotations

from typing import Any

import pytest

import fanferro
from fanferro import matchers as m
from fanferro.testing import build_message


def message(**overrides: Any) -> fanferro.Message[Any]:
    options: dict[str, Any] = {
        "type": "orders.created",
        "attributes": {"tenant": "acme", "version": 2, "blob": b"\x01"},
        "headers": {"schema": "v3", "priority": 5, "replay": True},
    }
    options.update(overrides)
    return build_message({"order": {"status": "paid", "items": [{"sku": "a"}], "total": 12.5}}, **options)


def test_attributes() -> None:
    msg = message()
    assert (m.attribute("tenant") == "acme")(msg)
    assert (m.attribute("version") == 2)(msg)
    assert (m.attribute("version") == "2")(msg)
    assert (m.attribute("version") >= 2)(msg)
    assert not (m.attribute("version") > 2)(msg)
    assert (m.attribute("tenant").is_in("x", "acme"))(msg)
    assert (m.attribute("missing").missing())(msg)
    assert not (m.attribute("missing") == "x")(msg)
    assert not (m.attribute("missing") != "x")(msg)
    assert (m.attribute("tenant") != "other")(msg)
    assert (m.attribute("blob") == b"\x01")(msg)


def test_fractional_numbers_compare_by_value() -> None:
    # Number attributes arrive as Decimal, and Decimal("0.1") != 0.1 in Python: equality, is_in and
    # != missed what the ordering comparisons matched.
    from decimal import Decimal

    msg = message(attributes={"rate": 0.1, "price": Decimal("19.99"), "count": 3})
    assert (m.attribute("rate") == 0.1)(msg)
    assert not (m.attribute("rate") != 0.1)(msg)
    assert (m.attribute("rate") != 0.2)(msg)
    assert (m.attribute("rate").is_in(0.1, 0.2))(msg)
    assert (m.attribute("price") == 19.99)(msg)
    assert (m.attribute("price") == Decimal("19.990"))(msg)
    assert (m.attribute("count") == 3.0)(msg)
    assert not (m.attribute("count") == True)(msg)  # noqa: E712 - a boolean is not the number 1
    assert (m.body("order.total") == Decimal("12.5"))(msg)
    assert not (m.attribute("rate") == float("nan"))(msg)


def test_headers_body_and_type() -> None:
    msg = message()
    assert (m.header("schema") == "v3")(msg)
    assert (m.header("priority") > 3)(msg)
    assert (m.header("replay") == "true")(msg)
    assert (m.header("tenant") == "acme")(msg), "falls back to attributes"
    assert (m.body("order.status") == "paid")(msg)
    assert (m.body("order.items.0.sku") == "a")(msg)
    assert (m.body("order.total") < 20)(msg)
    assert (m.body("order.nothing").missing())(msg)
    assert (m.body("order.items.5.sku").missing())(msg)
    assert (m.message_type() == "orders.created")(msg)
    assert (m.message_type().startswith("orders."))(msg)
    assert (m.message_type().matches(r"created$"))(msg)
    assert (m.content_type() == "application/json")(msg)
    assert (m.envelope() == "test")(msg)
    assert (m.body("order").contains("status"))(msg)


def test_combinators() -> None:
    msg = message()
    both = (m.attribute("tenant") == "acme") & (m.header("schema") == "v3")
    either = (m.attribute("tenant") == "x") | (m.header("schema") == "v3")
    assert both(msg)
    assert either(msg)
    assert not (~both)(msg)
    assert m.any_of(lambda _: False, m.attribute("tenant") == "acme")(msg)
    assert not m.all_of(lambda _: True, m.attribute("tenant") == "x")(msg)
    assert m.predicate(lambda msg: msg.type == "orders.created", "is created")(msg)
    assert "tenant" in repr(both)


def test_body_on_non_json() -> None:
    msg = build_message(b"\x00binary")
    assert not (m.body("a") == 1)(msg)


def test_invalid_matcher() -> None:
    with pytest.raises(TypeError):
        m.as_matcher(42)  # type: ignore[arg-type]


def test_comparisons_with_nan_values_do_not_match() -> None:
    # Values from other producers can be "NaN": comparisons must not match rather than raise
    # (an exception while routing would fail the message).
    msg = build_message({"score": "NaN", "ratio": float("nan")}, attributes={"score": "nan"})
    for field in (m.attribute("score"), m.body("score"), m.body("ratio")):
        assert not (field > 1)(msg)
        assert not (field <= 1)(msg)
        assert not (field == 1)(msg)
    assert (m.attribute("score") == "nan")(msg)
