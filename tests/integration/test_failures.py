from __future__ import annotations

import asyncio
from typing import Any

import fanferro
from fanferro.idempotency import idempotent
from tests.conftest import ClientFactory
from tests.helpers import OrderCreated, consume_until, drain_queue

FAST_RETRY = fanferro.Backoff.constant(0)


def queue_with_dlq(client: fanferro.Client, base: str) -> fanferro.Queue:
    return client.queue(base, dead_letter=fanferro.DeadLetterQueue(f"{base}-dlq", max_receive_count=50))


async def test_retry_then_succeed(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = queue_with_dlq(client, name("flaky"))
    attempts: list[int] = []
    consumer = client.consumer(queue, wait_time=1, retry=fanferro.RetryPolicy(max_attempts=5, backoff=FAST_RETRY))

    @consumer.route(OrderCreated)
    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        attempts.append(message.attempt)
        if message.attempt < 3:
            raise ConnectionError("downstream unavailable")

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await consume_until(consumer, lambda: consumer.stats()["processed"] == 1)
    assert attempts == [1, 2, 3]
    assert consumer.stats()["failed"] == 2


async def test_exhausted_retries_dead_letter_with_metadata(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = queue_with_dlq(client, name("poison"))
    consumer = client.consumer(queue, wait_time=1, retry=fanferro.RetryPolicy(max_attempts=2, backoff=FAST_RETRY))
    dead: list[fanferro.Event] = []
    client.on("message.dead_lettered", dead.append)

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        raise ValueError(f"cannot process {order.order_id}")

    async with client:
        await queue.send(OrderCreated(order_id=9))
        await consume_until(consumer, lambda: len(dead) == 1)
        assert queue.dead_letter_queue is not None
        dead_messages = await drain_queue(client, queue.dead_letter_queue)
    assert len(dead_messages) == 1
    message = dead_messages[0]
    assert message.json() == {"order_id": 9, "total": 0.0}
    info = message.dead_letter_info
    assert info is not None
    assert info["reason"] == "retries exhausted"
    assert info["error_type"].endswith("ValueError")
    assert info["error_message"] == "cannot process 9"
    assert info["attempt"] == "2"
    assert info["message_type"] == "test.order_created.v1"


async def test_handler_actions(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = queue_with_dlq(client, name("actions"))
    outcomes: dict[int, list[int]] = {}
    consumer = client.consumer(
        queue,
        wait_time=1,
        retry=fanferro.RetryPolicy(max_attempts=5, backoff=FAST_RETRY, strategy="visibility"),
    )

    @consumer.route(OrderCreated)
    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        order_id = message.body.order_id
        outcomes.setdefault(order_id, []).append(message.attempt)
        if order_id == 1:
            raise fanferro.Reject("invalid order")
        if order_id == 2:
            raise fanferro.Discard("test order")
        if order_id == 3 and message.attempt == 1:
            raise fanferro.Requeue("try again", delay=0)
        if order_id == 4 and message.attempt == 1:
            raise fanferro.Retry("not yet", delay=0)

    async with client:
        for order_id in (1, 2, 3, 4):
            await queue.send(OrderCreated(order_id=order_id))
        await consume_until(consumer, lambda: len(outcomes.get(3, [])) == 2 and len(outcomes.get(4, [])) == 2)
        assert queue.dead_letter_queue is not None
        dead = await drain_queue(client, queue.dead_letter_queue)
    assert outcomes[1] == [1]
    assert outcomes[2] == [1]
    assert outcomes[3] == [1, 2], "requeue increments the attempt counter"
    assert outcomes[4] == [1, 2]
    assert [message.json()["order_id"] for message in dead] == [1]
    assert dead[0].dead_letter_info["reason"] == "invalid order"  # type: ignore[index]
    assert (await queue.approximate_count())["visible"] == 0


async def test_explicit_settlement(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("manual"))
    seen: list[int] = []
    consumer = client.consumer(queue, wait_time=1, auto_ack=False)

    @consumer.route(OrderCreated)
    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        seen.append(message.attempt)
        if message.attempt == 1:
            await message.extend(30)
            await message.retry(delay=0)
        else:
            await message.ack()

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await consume_until(consumer, lambda: len(seen) == 2)
        await asyncio.sleep(0.2)
    assert seen == [1, 2]
    assert (await queue.approximate_count())["visible"] == 0


async def test_unroutable_and_undecodable(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = queue_with_dlq(client, name("bad"))
    events: list[fanferro.Event] = []
    client.on(["message.unroutable", "message.decode_failed"], events.append)
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        raise AssertionError("must not be called")

    async with client:
        await queue.send({"what": "ever"}, type="unknown.type")
        await queue.send({"order_id": "not-a-number"}, type="test.order_created.v1")
        await queue.send(b"\x00\x01", type="test.order_created.v1", content_type="application/json")
        await consume_until(consumer, lambda: len(events) == 3)
        assert queue.dead_letter_queue is not None
        dead = await drain_queue(client, queue.dead_letter_queue)
    kinds = sorted(event.kind.value for event in events)
    assert kinds == ["message.decode_failed", "message.decode_failed", "message.unroutable"]
    reasons = sorted(message.dead_letter_info["reason"] for message in dead)  # type: ignore[index]
    assert reasons == ["decode failed", "decode failed", "no route matched"]


async def test_handler_timeout_retries(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("slow"))
    attempts: list[int] = []
    consumer = client.consumer(
        queue, wait_time=1, handler_timeout=0.2, retry=fanferro.RetryPolicy(max_attempts=3, backoff=FAST_RETRY)
    )

    @consumer.route(OrderCreated)
    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        attempts.append(message.attempt)
        if message.attempt == 1:
            await asyncio.sleep(5)

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await consume_until(consumer, lambda: consumer.stats()["processed"] == 1)
    assert attempts == [1, 2]


async def test_idempotent_consumer(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("idempotent"))
    processed: list[str] = []
    duplicates: list[fanferro.Event] = []
    client.on("message.duplicate", duplicates.append)
    consumer = client.consumer(queue, wait_time=1, middleware=[idempotent()])

    @consumer.route(OrderCreated)
    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        processed.append(message.id)

    async with client:
        for _ in range(3):
            await queue.send(OrderCreated(order_id=1), id="order-1-created")
        await consume_until(consumer, lambda: len(processed) + len(duplicates) == 3)
    assert processed == ["order-1-created"]
    assert len(duplicates) == 2
    assert consumer.stats()["duplicates"] == 2


async def test_idempotent_consumer_runs_explicit_retries_again(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("idempotent-retry"))
    attempts: list[int] = []
    duplicates: list[fanferro.Event] = []
    client.on("message.duplicate", duplicates.append)
    consumer = client.consumer(queue, wait_time=1, middleware=[idempotent()])

    @consumer.route(OrderCreated)
    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        attempts.append(message.attempt)
        if message.attempt == 1:
            # Settled explicitly: not processed yet. (Without a delay the message could come back
            # before this handler returned, while its key is still claimed.)
            await message.retry(delay=1)

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await consume_until(consumer, lambda: len(attempts) == 2 or bool(duplicates))
    assert attempts == [1, 2]
    assert not duplicates


async def test_out_of_range_extensions_are_validation_errors(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("extend-range"))
    raised: list[BaseException] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(OrderCreated)
    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        try:
            await message.extend(1e30)
        except BaseException as error:  # a panic would surface as a BaseException
            raised.append(error)

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await consume_until(consumer, lambda: consumer.stats()["processed"] == 1)
    assert len(raised) == 1
    assert isinstance(raised[0], fanferro.ValidationError), repr(raised[0])


async def test_attempts_beyond_limit_skip_the_handler(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = queue_with_dlq(client, name("limit"))
    calls: list[int] = []
    slow_retry = fanferro.RetryPolicy(max_attempts=None, backoff=fanferro.Backoff.constant(1))
    # The next consumer reuses the queue right away: an aborted long poll could hide the retried
    # message for a visibility timeout, so this one lets its last poll finish.
    polite = fanferro.ShutdownPolicy(abort_receives=False)
    first = client.consumer(queue, wait_time=1, retry=slow_retry, shutdown=polite)

    @first.route(OrderCreated)
    async def fail(message: fanferro.Message[OrderCreated]) -> None:
        calls.append(message.attempt)
        raise RuntimeError("boom")

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await consume_until(first, lambda: len(calls) >= 3)
        strict = client.consumer(queue, wait_time=1, retry=fanferro.RetryPolicy(max_attempts=2), name="strict")

        @strict.route(OrderCreated)
        async def never(order: OrderCreated) -> None:
            raise AssertionError("must not run")

        assert queue.dead_letter_queue is not None
        dlq = queue.dead_letter_queue
        await consume_until(strict, lambda: strict.stats()["engine"].get("messages_dead_lettered", 0) == 1)
        dead_messages = await drain_queue(client, dlq)
    assert dead_messages[0].dead_letter_info["reason"] == "maximum attempts exceeded"  # type: ignore[index]


class _BrokenCloudEvent:
    """Sends a malformed CloudEvent (declared by its content type, missing required attributes)."""

    name = "broken"

    def wrap(self, payload: fanferro.Payload) -> fanferro.WireMessage:
        return fanferro.WireMessage(b'{"specversion":"1.0"}', {"content-type": "application/cloudevents+json"})

    def unwrap(self, wire: fanferro.WireMessage) -> fanferro.Payload | None:
        return None


async def test_envelope_failures_are_decode_failures(client_factory: ClientFactory, name: Any) -> None:
    # The engine unwraps built-in envelopes; failures surface exactly as when Python unwraps them.
    client = client_factory()
    queue = queue_with_dlq(client, name("envelope-failures"))
    events: list[fanferro.Event] = []
    client.on("message.decode_failed", events.append)
    consumer = client.consumer(queue, wait_time=1, envelopes=[fanferro.envelopes.CloudEvents()])

    @consumer.default
    async def handle(message: fanferro.Message[Any]) -> None:
        raise AssertionError("must not be called")

    async with client:
        await queue.send(OrderCreated(order_id=1))  # raw: no configured envelope recognises it
        await queue.send(OrderCreated(order_id=2), envelope=_BrokenCloudEvent())
        await consume_until(consumer, lambda: len(events) == 2)
    errors = sorted(str(event.exception) for event in events)
    assert errors == [
        'envelope error: CloudEvents attribute "id" is required',
        "no configured envelope recognised the message (tried cloudevents)",
    ]
    assert all(isinstance(event.exception, fanferro.EnvelopeError) for event in events)
