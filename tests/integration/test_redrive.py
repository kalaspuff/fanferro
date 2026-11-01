"""Inspecting (peek) and redriving dead-letter queues, from Python and from the ``fanferro dlq`` command."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

import fanferro
from fanferro import __main__ as cli
from fanferro.testing import memory_client
from tests.conftest import ClientFactory
from tests.helpers import OrderCreated, consume_until

FAST_RETRY = fanferro.Backoff.constant(0)
# A stopped consumer's aborted long poll would take the redriven messages on MiniStack (SQS keeps
# serving it); finishing the poll on stop releases what it returns.
POLITE = fanferro.ShutdownPolicy(abort_receives=False)


def queue_with_dlq(client: fanferro.Client, base: str) -> fanferro.Queue:
    return client.queue(base, dead_letter=fanferro.DeadLetterQueue(f"{base}-dlq", max_receive_count=50))


@pytest.mark.parametrize("strategy", ["visibility", "requeue"])
async def test_redriven_messages_get_a_fresh_retry_budget(
    client_factory: ClientFactory, name: Any, strategy: str
) -> None:
    client = client_factory()
    queue = queue_with_dlq(client, name("orders"))
    dlq = queue.dead_letter_queue
    assert dlq is not None
    broken = True
    attempts: list[tuple[int, int]] = []
    retry = fanferro.RetryPolicy(max_attempts=3, backoff=FAST_RETRY, strategy=strategy)  # type: ignore[arg-type]
    dead: list[fanferro.Event] = []
    client.on("message.dead_lettered", dead.append)

    def consumer() -> fanferro.Consumer:
        consumer = client.consumer(queue, wait_time=1, retry=retry, shutdown=POLITE)

        @consumer.route(OrderCreated)
        async def handle(message: fanferro.Message[OrderCreated]) -> None:
            attempts.append((message.body.order_id, message.attempt))
            if broken:
                raise ValueError(f"cannot process {message.body.order_id}")

        return consumer

    async with client:
        await queue.send_batch([OrderCreated(order_id=1), OrderCreated(order_id=2)])
        await consume_until(consumer(), lambda: len(dead) == 2)

        peeked = await dlq.peek()
        assert sorted(message.json()["order_id"] for message in peeked) == [1, 2]
        for message in peeked:
            info = message.dead_letter_info
            assert info is not None
            assert info.reason == "retries exhausted"
            assert info.error_type is not None
            assert info.error_type.endswith("ValueError")
            assert message.source == info.source == queue.arn
            assert (message.requeued_attempts > 0) == (strategy == "requeue")
            assert "fanferro.dead-letter" in message.raw_attributes
            assert not any(name.startswith("fanferro.") for name in message.attributes)
        # Peeking leaves the messages in the queue.
        assert len(await dlq.peek(limit=5)) == 2

        broken = False
        attempts.clear()
        result = await dlq.redrive()
        assert result.ok, result
        assert result.moved == 2
        assert result.destinations == {queue.url: 2}
        assert await dlq.peek() == []

        await consume_until(consumer(), lambda: len(attempts) == 2)
    # Each redriven message is processed at its first attempt (it would have been dead-lettered
    # again after one attempt with requeue's attempt count kept).
    assert sorted(attempts) == [(1, 1), (2, 1)]


async def test_redrive_can_keep_the_attempt_count(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = queue_with_dlq(client, name("kept"))
    dlq = queue.dead_letter_queue
    assert dlq is not None
    attempts: list[int] = []
    retry = fanferro.RetryPolicy(max_attempts=3, backoff=FAST_RETRY, strategy="requeue")
    consumer = client.consumer(queue, wait_time=1, retry=retry, shutdown=POLITE)
    dead: list[fanferro.Event] = []
    client.on("message.dead_lettered", dead.append)

    @consumer.route(OrderCreated)
    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        attempts.append(message.attempt)
        raise ValueError("still broken")

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await consume_until(consumer, lambda: len(dead) == 1)
        result = await dlq.redrive(reset_attempts=False)
        assert result.moved == 1
        await consume_until(consumer, lambda: len(dead) == 2)
    # The kept count makes the redriven message's first receive its last attempt.
    assert attempts == [1, 2, 3, 3]


async def test_messages_without_a_source_are_left_until_redriven_to_a_queue(
    client_factory: ClientFactory, name: Any
) -> None:
    client = client_factory()
    dlq = client.queue(name("manual-dlq"))
    target = client.queue(name("target"))
    async with client:
        await dlq.send_batch([OrderCreated(order_id=i) for i in range(3)])
        result = await dlq.redrive()
        assert result.moved == 0
        assert not result.ok
        assert len(result.not_moved) == 3
        assert all(isinstance(failure.error, fanferro.ValidationError) for failure in result.not_moved)
        assert all(message.source is None and message.dead_letter_info is None for message in await dlq.peek())

        result = await dlq.redrive(target, limit=2)
        assert (result.moved, result.destinations) == (2, {target.url: 2})
        assert len(await dlq.peek()) == 1
        assert len(await target.peek()) == 2

        with pytest.raises(fanferro.ValidationError):
            await dlq.redrive(dlq)
        with pytest.raises(fanferro.ValidationError):
            await dlq.redrive(limit=0)
        with pytest.raises(fanferro.ValidationError):
            await dlq.redrive(max_per_second=0)
        with pytest.raises(fanferro.ValidationError):
            await dlq.peek(0)


def test_dlq_command(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], name: Any) -> None:
    broker = fanferro.MemoryBroker()
    base = name("cli")

    async def setup() -> tuple[str, str]:
        client = memory_client(broker=broker)
        queue = queue_with_dlq(client, base)
        dlq = queue.dead_letter_queue
        assert dlq is not None
        consumer = client.consumer(queue, wait_time=1, retry=fanferro.RetryPolicy(max_attempts=1))
        dead: list[fanferro.Event] = []
        client.on("message.dead_lettered", dead.append)

        @consumer.route(OrderCreated)
        async def handle(order: OrderCreated) -> None:
            raise ValueError(f"cannot process {order.order_id}")

        async with client:
            await queue.send_batch([OrderCreated(order_id=1), OrderCreated(order_id=2)])
            await consume_until(consumer, lambda: len(dead) == 2)
            urls = (queue.url, dlq.url)
        await client.close()
        return urls

    queue_url, dlq_url = asyncio.run(setup())
    monkeypatch.setattr(cli, "_client", lambda args: memory_client(broker=broker))

    assert cli.main(["dlq", "stats", "--queue-url", dlq_url, "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == {"visible": 2, "in_flight": 0, "delayed": 0}

    assert cli.main(["dlq", "peek", "--queue-url", dlq_url]) == 0
    out = capsys.readouterr().out
    assert out.count("reason:        retries exhausted") == 2
    assert "ValueError: cannot process 1" in out
    assert cli.main(["dlq", "peek", "--queue-url", dlq_url, "--json", "--limit", "1"]) == 0
    [peeked] = json.loads(capsys.readouterr().out)
    assert peeked["dead_letter_info"]["reason"] == "retries exhausted"
    assert peeked["attributes"]["fanferro.dead-letter"]["type"] == "String"

    assert cli.main(["dlq", "redrive", "--queue-url", dlq_url, "--max-per-second", "100"]) == 0
    out = capsys.readouterr().out
    assert "moved 2 message(s)" in out
    assert f"2 to {queue_url}" in out
    assert cli.main(["dlq", "peek", "--queue-url", dlq_url]) == 0
    assert "no visible messages" in capsys.readouterr().out
    assert len(broker.queue_messages(queue_url)) == 2

    # Messages without a source are reported, and the command fails.
    async def manual() -> None:
        client = memory_client(broker=broker)
        await client.queue(url=dlq_url).send(OrderCreated(order_id=3))
        await client.close()

    asyncio.run(manual())
    assert cli.main(["dlq", "redrive", "--queue-url", dlq_url, "--json"]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["moved"] == 0
    assert report["not_moved"][0]["error_type"] == "ValidationError"
    assert cli.main(["dlq", "redrive", "--queue-url", dlq_url, "--to-url", queue_url]) == 0
    assert "moved 1 message(s)" in capsys.readouterr().out

    assert cli.main(["dlq", "stats", "--queue-url", "memory://us-east-1/000000000000/missing"]) == 2
    assert "queue missing does not exist" in capsys.readouterr().err
