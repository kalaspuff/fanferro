from __future__ import annotations

import dataclasses
from typing import Any

import pytest

import fanferro
from fanferro import matchers as m
from fanferro.message_types import MessageTypes
from fanferro.testing import build_message


@fanferro.message_type("orders.created.v1", "orders.created")
@dataclasses.dataclass
class Created:
    id: int


@dataclasses.dataclass
class Cancelled:
    id: int


async def on_created(order: Created) -> None: ...


async def on_cancelled(message: fanferro.Message[Cancelled]) -> None: ...


async def on_any(message: fanferro.Message) -> None:  # type: ignore[type-arg]
    ...


async def on_unannotated(message) -> None:  # type: ignore[no-untyped-def]
    ...


@pytest.fixture
def types() -> MessageTypes:
    return MessageTypes()


def test_handler_inspection() -> None:
    from fanferro.routing import inspect_handler

    assert inspect_handler(on_created) == (Created, False)
    assert inspect_handler(on_cancelled) == (Cancelled, True)
    assert inspect_handler(on_any) == (Any, True)
    assert inspect_handler(on_unannotated) == (Any, True)


def test_invalid_handlers() -> None:
    router = fanferro.Router()

    def sync_handler(message: fanferro.Message[Any]) -> None: ...

    async def no_args() -> None: ...

    with pytest.raises(fanferro.RoutingError, match="async"):
        router.add(sync_handler)  # type: ignore[arg-type]
    with pytest.raises(fanferro.RoutingError, match="first argument"):
        router.add(no_args)


def test_type_routing(types: MessageTypes) -> None:
    router = fanferro.Router()
    router.route()(on_created)
    router.route(Cancelled)(on_cancelled)
    assert router.resolve(build_message(Created(1)), types).handler is on_created  # type: ignore[union-attr]
    legacy = build_message({"id": 1}, type="orders.created")
    assert router.resolve(legacy, types).handler is on_created  # type: ignore[union-attr]
    assert router.resolve(build_message(Cancelled(1)), types).handler is on_cancelled  # type: ignore[union-attr]
    assert router.resolve(build_message({"x": 1}, type="unknown"), types) is None


def test_matchers_priorities_and_default(types: MessageTypes) -> None:
    router = fanferro.Router()

    @router.route(when=m.attribute("version") == "2")
    async def v2(message: fanferro.Message[Any]) -> None: ...

    @router.route(when=m.attribute("version").exists(), priority=10)
    async def any_version(message: fanferro.Message[Any]) -> None: ...

    @router.default
    async def fallback(message: fanferro.Message[Any]) -> None: ...

    assert router.resolve(build_message({}, attributes={"version": "2"}), types).handler is any_version  # type: ignore[union-attr]
    assert router.resolve(build_message({}), types).handler is fallback  # type: ignore[union-attr]
    assert [route.name for route in router.routes][-1].endswith("fallback")


def test_include_and_invoke(types: MessageTypes) -> None:
    calls: list[Any] = []
    inner = fanferro.Router()

    @inner.route(Created)
    async def created(order: Created) -> None:
        calls.append(order)

    outer = fanferro.Router()
    outer.include(inner)
    assert len(outer) == 1
    message = build_message(Created(5))
    route = outer.resolve(message, types)
    assert route is not None
    import asyncio

    asyncio.run(route.invoke(message))
    assert calls == [Created(5)]


def test_message_types_registry() -> None:
    types = MessageTypes()
    assert types.name_of(Created) == "orders.created.v1"
    assert types.names_of(Created) == ("orders.created.v1", "orders.created")
    assert types.name_of(Cancelled(1)) == "Cancelled"
    assert types.names_of(Cancelled) == ("Cancelled",)
    types.register(Cancelled, "orders.cancelled", "orders.cancel")
    assert types.name_of(Cancelled) == "orders.cancelled"
    assert types.names_of(Cancelled) == ("orders.cancelled", "orders.cancel"), "registering refreshes cached names"
    assert types.class_for("orders.cancel") is Cancelled
    assert MessageTypes(naming=lambda cls: f"x.{cls.__name__}").name_of(Cancelled) == "x.Cancelled"
