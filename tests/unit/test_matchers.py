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
