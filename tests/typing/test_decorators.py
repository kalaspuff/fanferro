"""The decorators of fanferro return the functions they decorate with their own types.

pyrefly checks the ``assert_type`` calls here (CI runs it over ``tests``): a decorator that erased a
handler to ``Any`` or ``(...) -> Awaitable[None]`` would fail them. pytest runs them too, and checks
that the decorators registered what they decorated.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, assert_type

import fanferro
import fanferro.aws_lambda
from fanferro.testing import build_message, memory_client


@dataclass
class OrderPlaced:
    order_id: str


async def test_router_decorators_keep_handler_types() -> None:
    message = build_message({"n": 1})
    router = fanferro.Router()

    @router.route(OrderPlaced)
    async def on_order(order: OrderPlaced) -> None: ...

    @router.route
    async def on_message(message: fanferro.Message[Any]) -> None: ...

    @router.default
    async def fallback(message: fanferro.Message[Any]) -> None: ...

    call = on_order(OrderPlaced("1"))
    assert_type(call, Coroutine[Any, Any, None])
    await call
    call = on_message(message)
    assert_type(call, Coroutine[Any, Any, None])
    await call
    call = fallback(message)
    assert_type(call, Coroutine[Any, Any, None])
    await call
    assert [route.handler for route in router.routes] == [on_order, on_message, fallback]


async def test_consumer_decorators_keep_handler_types() -> None:
    client = memory_client()
    try:
        message = build_message({"n": 1})
        consumer = client.consumer(client.queue("typed-routes"))

        @consumer.route(OrderPlaced, priority=1, name="orders")
        async def on_order(order: OrderPlaced) -> None: ...

        @consumer.route
        async def on_message(message: fanferro.Message[Any]) -> None: ...

        @consumer.default
        async def fallback(message: fanferro.Message[Any]) -> None: ...

        call = on_order(OrderPlaced("1"))
        assert_type(call, Coroutine[Any, Any, None])
        await call
        call = on_message(message)
        assert_type(call, Coroutine[Any, Any, None])
        await call
        call = fallback(message)
        assert_type(call, Coroutine[Any, Any, None])
        await call
        routes = consumer.router.routes
        assert [route.handler for route in routes] == [on_order, on_message, fallback]
        assert (routes[0].name, routes[0].priority) == ("orders", 1)
        assert routes[2].fallback
    finally:
        await client.close()


async def test_lambda_handler_decorators_keep_handler_types() -> None:
    client = memory_client()
    try:
        message = build_message({"n": 1})
        handler = fanferro.aws_lambda.sqs_handler(client, queue="typed-lambda")

        @handler.route(OrderPlaced)
        async def on_order(order: OrderPlaced) -> None: ...

        @handler.default
        async def fallback(message: fanferro.Message[Any]) -> None: ...

        call = on_order(OrderPlaced("1"))
        assert_type(call, Coroutine[Any, Any, None])
        await call
        call = fallback(message)
        assert_type(call, Coroutine[Any, Any, None])
        await call
        assert [route.handler for route in handler.router.routes] == [on_order, fallback]
    finally:
        await client.close()


async def test_client_on_keeps_hook_types() -> None:
    client = memory_client()
    try:
        seen: list[fanferro.Event] = []
        event = fanferro.Event(fanferro.EventKind.MESSAGE_ACKED, logging.INFO, "acked", datetime.now(UTC))

        @client.on("message.*")
        async def alert(event: fanferro.Event) -> None:
            seen.append(event)

        @client.on(fanferro.EventKind.MESSAGE_ACKED)
        def count(event: fanferro.Event) -> int:
            return len(seen)

        unsubscribe = client.on("publish.*", seen.append)

        call = alert(event)
        assert_type(call, Coroutine[Any, Any, None])
        await call
        assert_type(count(event), int)
        assert_type(unsubscribe, Callable[[], None])
        unsubscribe()
        assert seen == [event]
    finally:
        await client.close()
