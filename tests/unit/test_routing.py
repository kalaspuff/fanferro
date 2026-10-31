from __future__ import annotations

import asyncio
import dataclasses
import functools
from typing import Any

import pytest

import fanferro
from fanferro import matchers as m
from fanferro.message_types import MessageTypes
from fanferro.routing import Handler
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


async def created_with(order: Created, db: list[Any]) -> None:
    db.append(order)


async def cancelled_with(db: list[Any], message: fanferro.Message[Cancelled], *, audit: bool = False) -> None:
    db.append((message.body, audit))


def test_partial_handlers(types: MessageTypes) -> None:
    # The signature of a partial's __call__ is (*args, **keywords): partials were refused as
    # handlers that do not accept the message.
    from fanferro.routing import inspect_handler

    db: list[Any] = []
    keyword = functools.partial(created_with, db=db)
    positional = functools.partial(cancelled_with, db)
    nested = functools.partial(functools.partial(cancelled_with, db), audit=True)
    assert inspect_handler(keyword) == (Created, False)
    assert inspect_handler(positional) == (Cancelled, True)
    assert inspect_handler(nested) == (Cancelled, True)
    router = fanferro.Router()
    created_route = router.add(keyword)
    cancelled_route = router.add(nested)
    assert created_route.name == "created_with"
    assert cancelled_route.name == "cancelled_with"
    assert router.resolve(build_message(Created(1)), types) is created_route
    assert router.resolve(build_message(Cancelled(2)), types) is cancelled_route
    asyncio.run(created_route.invoke(build_message(Created(1))))
    asyncio.run(cancelled_route.invoke(build_message(Cancelled(2))))
    assert db == [Created(1), (Cancelled(2), True)]
    with pytest.raises(fanferro.RoutingError, match="created_with must accept the message"):
        router.add(functools.partial(created_with, Created(1), db))


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


async def on_any_message(message: fanferro.Message[Any]) -> None: ...


async def on_any_body(body: Any) -> None: ...


async def on_object_body(body: object) -> None: ...


@pytest.mark.parametrize("handler", [on_any, on_any_message, on_unannotated, on_any_body, on_object_body])
def test_routes_without_a_message_type_for_untyped_handlers_match_every_message(
    handler: Handler, types: MessageTypes
) -> None:
    # Regression test for #176: these routes took typing.Any as their message type and matched nothing.
    decorated, routed, added = fanferro.Router(), fanferro.Router(), fanferro.Router()
    decorated.route(handler)
    routed.route()(handler)
    added.add(handler)
    for router in (decorated, routed, added):
        assert router.routes[0].message_type is None
        for message in (build_message(Created(1)), build_message({"x": 1}, type="unknown"), build_message(b"raw")):
            assert router.resolve(message, types) is router.routes[0]


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
