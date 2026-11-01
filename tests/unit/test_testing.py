"""fanferro.testing: built messages with transport metadata, and dispatch() through a consumer's processing."""

from __future__ import annotations

import dataclasses
import datetime
from typing import Any

import pytest

import fanferro
from fanferro import matchers as m
from fanferro.testing import FakeDelivery, RecordingSettler, build_message, dispatch, memory_client


@fanferro.message_type("tests.testing.order.v1")
@dataclasses.dataclass
class Order:
    id: int
    action: str = "ok"


class BoomError(Exception):
    pass


async def handle(order: Order) -> None:
    """Behaves according to the order's action, so that every outcome can be produced."""
    if order.action == "reject":
        raise fanferro.Reject("rejected")
    if order.action == "retry":
        raise fanferro.Retry("later", delay=3)
    if order.action == "requeue":
        raise fanferro.Requeue("again", delay=2)
    if order.action == "discard":
        raise fanferro.Discard("not needed")
    if order.action == "fail":
        raise BoomError("handler failed")


def order_router() -> fanferro.Router:
    router = fanferro.Router()
    router.add(handle)
    return router


# -- build_message ---------------------------------------------------------------------------


def test_built_message_reports_transport_metadata() -> None:
    sent = datetime.datetime(2026, 11, 1, 12, 0, tzinfo=datetime.UTC)
    message = build_message(
        Order(1),
        attempt=4,
        group_id="customer-1",
        deduplication_id="dedup-1",
        sequence_number="18888",
        topic="orders",
        sent_at=sent,
        queue="worker",
    )
    assert message.attempt == 4
    assert message.receive_count == 4
    assert message.group_id == "customer-1"
    assert message.sequence_number == "18888"
    assert message.delivery is not None
    assert message.delivery.deduplication_id == "dedup-1"
    assert message.topic_arn == "arn:aws:sns:us-east-1:000000000000:orders"
    assert message.sent_at == sent
    assert message.time == sent
    assert message.received_at is not None
    assert message.sqs_message_id
    assert message.queue == "worker"
    assert "fanferro.attempt" not in message.raw_attributes


def test_built_message_defaults_to_a_first_receive() -> None:
    message = build_message({"a": 1})
    assert message.attempt == 1
    assert message.receive_count == 1
    assert message.group_id is None
    assert message.topic_arn is None
    assert message.sent_at is not None
    assert isinstance(message.delivery, FakeDelivery)


def test_attempt_with_receive_count_comes_from_requeued_copies() -> None:
    message = build_message({"a": 1}, attempt=5, receive_count=2)
    assert message.attempt == 5
    assert message.receive_count == 2
    assert message.raw_attributes["fanferro.attempt"].value == "3"
    assert "fanferro.attempt" not in message.attributes
    assert build_message({"a": 1}, receive_count=3).attempt == 3


@pytest.mark.parametrize(
    ("options", "error"),
    [
        ({"attempt": 0}, "receive_count must be at least 1"),
        ({"receive_count": 0}, "receive_count must be at least 1"),
        ({"attempt": 2, "receive_count": 3}, r"attempt \(2\) must be at least receive_count \(3\)"),
    ],
)
def test_invalid_attempts_are_refused(options: dict[str, Any], error: str) -> None:
    with pytest.raises(ValueError, match=error):
        build_message({"a": 1}, **options)


def test_topic_matchers_match_a_built_message() -> None:
    by_name = build_message({"a": 1}, topic="orders")
    by_arn = build_message({"a": 1}, topic="arn:aws:sns:eu-north-1:123456789012:orders")
    assert (m.topic() == "orders")(by_name)
    assert (m.topic() == "orders")(by_arn)
    assert (m.topic() == "arn:aws:sns:eu-north-1:123456789012:orders")(by_arn)
    assert not (m.topic() == "payments")(by_name)
    assert not (m.topic() == "orders")(build_message({"a": 1}))


async def test_handler_settlements_still_go_to_the_settler() -> None:
    settler = RecordingSettler()
    message = build_message({"a": 1}, attempt=4, settler=settler)

    async def late(message: fanferro.Message[Any]) -> None:
        if message.attempt > 3:
            await message.discard("too late")

    await late(message)
    assert message.outcome == "discarded"
    assert settler.calls == [("discard", {"reason": "too late"})]
    assert isinstance(message.delivery, FakeDelivery)
    assert message.delivery.calls == []


# -- dispatch --------------------------------------------------------------------------------


async def test_dispatch_acks_a_handled_message_decoded_into_the_route_type() -> None:
    seen: list[fanferro.Message[Order]] = []
    router = fanferro.Router()

    @router.route(Order)
    async def on_order(message: fanferro.Message[Order]) -> None:
        seen.append(message)

    result = await dispatch(router, build_message({"id": 7}, type="tests.testing.order.v1"))
    assert result.outcome == "acked"
    assert result.calls == [("ack", {})]
    assert seen[0].body == Order(7)
    assert result.message is seen[0]


async def test_dispatch_builds_a_message_from_a_body_and_takes_a_handler() -> None:
    seen: list[Order] = []

    async def on_order(order: Order) -> None:
        seen.append(order)

    result = await dispatch(on_order, Order(1))
    assert result.outcome == "acked"
    assert seen == [Order(1)]


@pytest.mark.parametrize(
    ("action", "outcome", "call"),
    [
        ("ok", "acked", "ack"),
        ("retry", "retried", "nack"),
        ("requeue", "requeued", "requeue"),
        ("discard", "discarded", "ack"),
        ("reject", "dead_lettered", "dead_letter"),
        ("fail", "retried", "nack"),
    ],
)
async def test_dispatch_maps_handler_results_to_outcomes(action: str, outcome: str, call: str) -> None:
    result = await dispatch(order_router(), Order(1, action), dead_letter_queue=True)
    assert result.outcome == outcome
    assert [name for name, _ in result.calls] == [call]


async def test_dispatch_passes_delays_and_dead_letter_details() -> None:
    retried = await dispatch(order_router(), Order(1, "retry"))
    assert retried.calls == [("nack", {"delay": 3.0})]
    requeued = await dispatch(order_router(), build_message(Order(1, "requeue"), attempt=2))
    assert requeued.calls == [("requeue", {"attempt": 2, "delay": 2.0})]
    rejected = await dispatch(order_router(), Order(1, "reject"), dead_letter_queue=True)
    ((_, arguments),) = rejected.calls
    assert arguments["reason"] == "rejected"
    assert arguments["details"]["error_type"] == "fanferro.actions.Reject"
    assert arguments["details"]["message_type"] == "tests.testing.order.v1"


async def test_dispatch_without_a_dead_letter_queue_applies_on_missing_queue() -> None:
    result = await dispatch(
        order_router(), Order(1, "reject"), dead_letter=fanferro.DeadLetterPolicy(on_missing_queue="discard")
    )
    assert result.outcome == "discarded"
    assert result.calls == [("ack", {})]


async def test_dispatch_applies_max_attempts_and_the_retry_policy() -> None:
    calls = 0

    async def counting(order: Order) -> None:
        nonlocal calls
        calls += 1

    retry = fanferro.RetryPolicy(max_attempts=3)
    exhausted = await dispatch(counting, build_message(Order(1), attempt=4), retry=retry, dead_letter_queue=True)
    assert exhausted.outcome == "dead_lettered"
    assert calls == 0
    failing = await dispatch(order_router(), build_message(Order(1, "fail"), attempt=3), retry=retry,
                             dead_letter_queue=True)  # fmt: skip
    assert failing.outcome == "dead_lettered"


async def test_dispatch_handles_unroutable_messages_and_decode_errors() -> None:
    policy = fanferro.DeadLetterPolicy(on_unroutable="discard", on_decode_error="dead_letter")
    unroutable = await dispatch(order_router(), build_message({"x": 1}, type="other"), dead_letter=policy)
    assert unroutable.outcome == "discarded"
    undecodable = await dispatch(
        order_router(), build_message({"id": "x"}, type="tests.testing.order.v1"), dead_letter=policy,
        dead_letter_queue=True,
    )  # fmt: skip
    assert undecodable.outcome == "dead_lettered"
    assert undecodable.calls[0][1]["reason"] == "decode failed"


async def test_dispatch_runs_middleware_and_matches_the_topic() -> None:
    order: list[str] = []

    async def middleware(message: fanferro.Message[Any], call_next: Any) -> None:
        order.append(f"middleware:{message.attempt}")
        await call_next(message)

    router = fanferro.Router()

    @router.route(when=m.topic() == "orders")
    async def from_orders(message: fanferro.Message[Any]) -> None:
        order.append("orders")

    @router.default
    async def other(message: fanferro.Message[Any]) -> None:
        order.append("other")

    await dispatch(router, build_message({"a": 1}, topic="orders", attempt=2), middleware=[middleware])
    await dispatch(router, build_message({"a": 1}, topic="payments"), middleware=[middleware])
    assert order == ["middleware:2", "orders", "middleware:1", "other"]


async def test_dispatch_through_a_consumer_uses_its_policies() -> None:
    client = memory_client()
    queue = client.queue("orders", dead_letter=fanferro.DeadLetterQueue("orders-dlq"))
    consumer = client.consumer(queue, order_router(), retry=fanferro.RetryPolicy(max_attempts=2))
    result = await dispatch(consumer, build_message(Order(1, "fail"), attempt=2))
    assert result.outcome == "dead_lettered"
    assert result.calls[0][1]["details"]["queue"] == "orders"
    assert consumer.stats().failed == 1
    assert consumer.state == "created"
    with pytest.raises(TypeError, match="consumer options"):
        await dispatch(consumer, Order(1), retry=fanferro.RetryPolicy())


async def test_dispatch_records_explicit_settlements_and_left_messages() -> None:
    async def settles(message: fanferro.Message[Any]) -> None:
        await message.extend(60)
        await message.release()

    released = await dispatch(settles, {"a": 1})
    assert released.outcome == "released"
    assert released.calls == [("extend", {"timeout": 60.0}), ("release", {})]

    async def leaves(message: fanferro.Message[Any]) -> None:
        pass

    left = await dispatch(leaves, {"a": 1}, auto_ack=False)
    assert left.outcome is None
    assert left.calls == [("leave", {})]


async def test_dispatch_refuses_settled_messages() -> None:
    message = build_message({"a": 1})
    await message.ack()
    with pytest.raises(RuntimeError, match="already settled"):
        await dispatch(order_router(), message)


@pytest.mark.parametrize("action", ["ok", "retry", "requeue", "discard", "reject", "fail", "unroutable", "undecodable"])
async def test_dispatch_gives_the_outcome_of_a_consumer_on_the_memory_broker(action: str) -> None:
    def message_for(action: str) -> tuple[Any, str]:
        if action == "unroutable":
            return {"x": 1}, "other"
        if action == "undecodable":
            return {"id": "x"}, "tests.testing.order.v1"
        return {"id": 1, "action": action}, "tests.testing.order.v1"

    body, message_type = message_for(action)
    expected = await dispatch(order_router(), build_message(body, type=message_type), dead_letter_queue=True)

    client = memory_client()
    queue = client.queue("orders", dead_letter=fanferro.DeadLetterQueue("orders-dlq"))
    consumer = client.consumer(queue, order_router(), wait_time=1)
    outcomes: list[Any] = []
    client.on("message.processed", lambda event: outcomes.append(event.fields.get("outcome")))
    async with client:
        await queue.send(body, type=message_type)
        await consumer.run_until_idle()
        counts = await queue.approximate_count()
        assert queue.dead_letter_queue is not None
        dead = (await queue.dead_letter_queue.approximate_count()).visible
    remaining = counts.visible + counts.in_flight + counts.delayed
    if expected.outcome == "acked":
        assert outcomes == ["acked"]
    if expected.outcome in ("acked", "discarded"):
        assert (remaining, dead) == (0, 0)
    elif expected.outcome == "dead_lettered":
        assert (remaining, dead) == (0, 1)
    else:
        assert expected.outcome in ("retried", "requeued")
        assert (remaining, dead) == (1, 0)
