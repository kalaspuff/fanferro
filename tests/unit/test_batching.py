"""Batch routes without a broker: options, handler signatures, results (fanferro.testing.dispatch_batch)."""

from __future__ import annotations

import asyncio
import contextvars
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest

import fanferro
from fanferro.idempotency import InMemoryIdempotencyStore, idempotent
from fanferro.testing import build_message, dispatch, dispatch_batch, memory_client


@fanferro.message_type("unit.batch_order.v1")
@dataclass
class Order:
    id: int


def built(*ids: int, **options: Any) -> list[fanferro.Message[Any]]:
    return [build_message(Order(i), **options) for i in ids]


def test_batch_options_are_validated() -> None:
    assert fanferro.Batch() == fanferro.Batch(10, 1.0)
    assert fanferro.Batch(max_wait=timedelta(milliseconds=250)).max_wait == 0.25
    assert fanferro.Batch(max_size=1, max_wait=0).max_wait == 0.0
    with pytest.raises(fanferro.ConfigurationError, match="max_size must be between 1 and 10,000"):
        fanferro.Batch(max_size=0)
    with pytest.raises(fanferro.ConfigurationError, match="max_size"):
        fanferro.Batch(max_size=10_001)
    with pytest.raises(TypeError, match="max_size must be an integer"):
        fanferro.Batch(max_size=2.5)  # type: ignore[arg-type]
    with pytest.raises(fanferro.ConfigurationError, match="max_wait must be between 0 and 900"):
        fanferro.Batch(max_wait=901)
    with pytest.raises(fanferro.ConfigurationError, match="max_wait"):
        fanferro.Batch(max_wait=-1)
    assert repr(fanferro.Batch(5, 0.5)) == "Batch(max_size=5, max_wait=0.5)"


def test_batch_handler_signatures() -> None:
    router = fanferro.Router()
    batch = fanferro.Batch()

    @router.route(Order, batch=batch)
    async def messages(items: list[fanferro.Message[Order]]) -> None: ...

    @router.route(batch=batch)
    async def bodies(items: Sequence[Order]) -> None: ...

    @router.route("any", batch=batch)
    async def untyped(items: list[fanferro.Message]) -> None: ...  # type: ignore[type-arg]

    @router.route("bare", batch=batch)
    async def bare(items) -> None: ...  # type: ignore[no-untyped-def]

    routes = {route.name.rsplit(".", 1)[-1]: route for route in router.routes}
    assert (routes["messages"].body_type, routes["messages"].pass_message) == (Order, True)
    assert (routes["bodies"].body_type, routes["bodies"].pass_message) == (Order, False)
    assert routes["bodies"].message_type is Order  # typed batch handlers route by their type
    assert (routes["untyped"].body_type, routes["untyped"].pass_message) == (Any, True)
    assert (routes["bare"].body_type, routes["bare"].pass_message) == (Any, True)
    assert all(route.batch is batch for route in router.routes)
    assert "batch=Batch(max_size=10, max_wait=1.0)" in repr(routes["messages"])
    assert "batch=" not in repr(fanferro.Router().add(messages))


def test_batch_handlers_must_take_a_list() -> None:
    router = fanferro.Router()

    async def single(message: fanferro.Message[Order]) -> None: ...

    with pytest.raises(fanferro.RoutingError, match=r"takes a list: annotate the first parameter .* as list\[Message"):
        router.add(single, batch=fanferro.Batch())
    with pytest.raises(TypeError, match=r"batch must be a fanferro\.Batch"):
        router.add(single, batch=10)  # type: ignore[arg-type]


def test_batch_results() -> None:
    first, second = built(1, 2)
    error = ValueError("bad")
    result = fanferro.BatchResult([first])
    result.fail(1, error)
    result.fail(first, error)  # reported again: the last error counts
    assert result.failed == [(first, error), (1, error)]
    assert len(result) == 2
    assert repr(result) == "BatchResult(failed=2)"
    assert fanferro.BatchResult({second: None, -1: error}).failed == [(second, None), (-1, error)]
    with pytest.raises(TypeError, match="error must be an exception"):
        result.fail(first, "bad")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="got a bool"):
        result.fail(True)
    with pytest.raises(TypeError, match="error must be an exception"):
        result.fail(first, KeyboardInterrupt())  # type: ignore[arg-type]


async def test_a_batch_handler_gets_the_messages_together() -> None:
    router = fanferro.Router()
    calls: list[list[int]] = []

    @router.route(Order, batch=fanferro.Batch(max_size=3, max_wait=60))
    async def store(orders: list[Order]) -> fanferro.BatchResult:
        calls.append([order.id for order in orders])
        return fanferro.BatchResult({0: fanferro.Reject("invalid"), -1: None})

    results = await dispatch_batch(router, built(1, 2, 3, 4, 5), dead_letter_queue=True)
    assert calls == [[1, 2, 3], [4, 5]]
    assert [result.outcome for result in results] == ["dead_lettered", "acked", "retried", "dead_lettered", "retried"]
    assert [result.message.body.id for result in results] == [1, 2, 3, 4, 5]
    assert results[1].calls == [("ack", {})]
    assert results[0].calls[0][0] == "dead_letter"


async def test_dispatch_hands_a_batch_of_one_over_at_once() -> None:
    router = fanferro.Router()

    @router.route(Order, batch=fanferro.Batch(max_size=10, max_wait=60))
    async def store(orders: list[fanferro.Message[Order]]) -> None:
        assert len(orders) == 1

    result = await asyncio.wait_for(dispatch(router, build_message(Order(1))), timeout=5)
    assert result.outcome == "acked"
    assert await dispatch_batch(router, []) == []


@pytest.mark.parametrize(
    ("returned", "error"),
    [
        ([0], "returned list: return None"),
        ("bad", "returned str"),
        (fanferro.BatchResult([7]), "position 7 of a batch of 2 messages"),
        (fanferro.BatchResult([build_message(Order(9))]), "a message that is not in the batch"),
    ],
)
async def test_invalid_results_fail_the_whole_batch(returned: object, error: str) -> None:
    router = fanferro.Router()
    failed: list[fanferro.Event] = []

    @router.route(Order, batch=fanferro.Batch(max_size=2))
    async def store(orders: list[Order]) -> Any:
        return returned

    client = memory_client()
    client.on("message.failed", failed.append)
    results = await dispatch_batch(router, built(1, 2), client=client)
    assert [result.outcome for result in results] == ["retried", "retried"]
    assert all(error in event.fields["error"] for event in failed), [event.fields["error"] for event in failed]
    await client.close()


async def test_a_raising_batch_handler_applies_the_retry_policy_to_each_message() -> None:
    router = fanferro.Router()

    @router.route(Order, batch=fanferro.Batch(max_size=3))
    async def store(orders: list[fanferro.Message[Order]]) -> None:
        raise LookupError("not found")

    retry = fanferro.RetryPolicy(max_attempts=3, dead_letter_on=(LookupError,))
    messages = [build_message(Order(1)), build_message(Order(2), attempt=3), build_message(Order(3))]
    results = await dispatch_batch(router, messages, retry=retry, dead_letter_queue=True)
    assert [result.outcome for result in results] == ["dead_lettered"] * 3
    reasons = [dict(result.calls[0][1])["reason"] for result in results]
    assert reasons == ["not found"] * 3


async def test_batch_handler_timeout() -> None:
    router = fanferro.Router()

    @router.route(Order, batch=fanferro.Batch(max_size=2))
    async def store(orders: list[Order]) -> None:
        await asyncio.sleep(10)

    results = await dispatch_batch(router, built(1, 2), handler_timeout=0.05)
    assert [result.outcome for result in results] == ["retried", "retried"]


async def test_middleware_runs_for_each_message_of_a_batch() -> None:
    store = InMemoryIdempotencyStore()
    seen: list[int] = []
    calls: list[list[int]] = []

    async def record(message: fanferro.Message[Any], call_next: fanferro.HandlerNext) -> None:
        seen.append(message.body.id)
        await call_next(message)

    router = fanferro.Router()

    @router.route(Order, batch=fanferro.Batch(max_size=10))
    async def handle(orders: list[Order]) -> None:
        calls.append([order.id for order in orders])

    first = await dispatch_batch(router, built(1, 2), middleware=[idempotent(store), record])
    assert [result.outcome for result in first] == ["acked", "acked"]
    again = [build_message(Order(i), id=result.message.id) for i, result in zip((1, 2), first, strict=True)]
    second = await dispatch_batch(router, [*again, build_message(Order(3))], middleware=[idempotent(store), record])
    assert [result.outcome for result in second] == ["acked", "acked", "acked"]
    # Duplicates were skipped before they joined a batch.
    assert calls == [[1, 2], [3]]
    assert seen == [1, 2, 3]


async def test_messages_of_other_routes_are_not_batched() -> None:
    router = fanferro.Router()
    calls: list[Any] = []

    @router.route(Order, batch=fanferro.Batch(max_size=10))
    async def orders(items: list[Order]) -> None:
        calls.append([item.id for item in items])

    @router.default
    async def other(message: fanferro.Message[Any]) -> None:
        calls.append(message.body)

    results = await dispatch_batch(router, [build_message(Order(1)), build_message({"x": 1}), build_message(Order(2))])
    assert [result.outcome for result in results] == ["acked"] * 3
    assert calls == [{"x": 1}, [1, 2]]


async def test_batch_handlers_run_in_the_consumer_context() -> None:
    variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable", default="unset")
    seen: list[str] = []

    async def set_variable(message: fanferro.Message[Any], call_next: fanferro.HandlerNext) -> None:
        variable.set(f"message {message.body.id}")
        await call_next(message)

    router = fanferro.Router()

    @router.route(Order, batch=fanferro.Batch(max_size=2))
    async def handle(orders: list[Order]) -> None:
        seen.append(variable.get())

    variable.set("dispatcher")
    await dispatch_batch(router, built(1, 2), middleware=[set_variable])
    # Not the context of the message that completed the batch.
    assert seen == ["dispatcher"]


async def test_dispatch_batch_rejects_a_message_given_twice() -> None:
    message = build_message(Order(1))
    with pytest.raises(TypeError, match="each message once"):
        await dispatch_batch(fanferro.Router(), [message, message])
