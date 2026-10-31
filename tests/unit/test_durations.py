"""Options in seconds take a number or a ``timedelta`` alike and are validated where they are given."""

from __future__ import annotations

import dataclasses
from datetime import timedelta
from typing import Any

import pytest

import fanferro
from fanferro.idempotency import IdempotencyMiddleware, InMemoryIdempotencyStore
from fanferro.outbox import InMemoryOutbox, OutboxRelay
from fanferro.testing import RecordingSettler, build_message

SECONDS = (2.5, timedelta(seconds=2.5))


async def _noop(message: fanferro.Message) -> None: ...


@pytest.mark.parametrize("value", SECONDS, ids=["float", "timedelta"])
@pytest.mark.parametrize(
    "option", ["receive_batch_wait", "wait_time", "visibility_timeout", "handler_timeout", "settle_linger"]
)
def test_consumer_options_take_timedelta(option: str, value: float | timedelta) -> None:
    client = fanferro.testing.memory_client()
    scale = 0.1 if option == "settle_linger" else 1  # at most a second
    given = value * scale
    consumer = client.consumer(client.queue("durations"), _noop, **{option: given})
    stored = getattr(consumer, option)
    assert type(stored) is float
    assert stored == pytest.approx(2.5 * scale)


@pytest.mark.parametrize(
    ("options", "error"),
    [
        ({"handler_timeout": 0}, "handler_timeout must be positive"),
        ({"handler_timeout": -5}, "handler_timeout"),
        ({"handler_timeout": timedelta(seconds=-5)}, "handler_timeout"),
        ({"handler_timeout": float("nan")}, "handler_timeout"),
        ({"visibility_timeout": -3}, "visibility_timeout"),
        ({"visibility_timeout": 43201}, "visibility_timeout must be between 0 and 43200"),
        ({"visibility_timeout": timedelta(hours=13)}, "visibility_timeout"),
        ({"wait_time": timedelta(seconds=21)}, "wait_time must be between 0 and 20"),
        ({"wait_time": -1}, "wait_time"),
        ({"receive_batch_wait": timedelta(seconds=-1)}, "receive_batch_wait"),
        ({"settle_linger": timedelta(seconds=2)}, "settle_linger must be between 0 and 1"),
    ],
)
def test_consumer_options_out_of_range_fail_at_construction(options: dict[str, Any], error: str) -> None:
    client = fanferro.testing.memory_client()
    with pytest.raises(ValueError, match=error):
        client.consumer(client.queue("durations"), _noop, **options)


def test_consumer_options_of_the_wrong_type_name_the_option() -> None:
    client = fanferro.testing.memory_client()
    with pytest.raises(TypeError, match="handler_timeout must be a number of seconds or a timedelta"):
        client.consumer(client.queue("durations"), _noop, handler_timeout="5")  # type: ignore[arg-type]


@pytest.mark.parametrize("value", SECONDS, ids=["float", "timedelta"])
def test_policies_take_timedelta(value: float | timedelta) -> None:
    backoff = fanferro.Backoff(initial=value, maximum=value * 4, multiplier=2, jitter=False)
    assert (backoff.initial, backoff.maximum) == (2.5, 10.0)
    assert type(backoff.initial) is float
    assert backoff.delay(3) == 10.0
    assert dataclasses.replace(backoff, jitter=True).maximum == 10.0
    assert backoff == fanferro.Backoff(2.5, 10.0, 2, False)
    shutdown = fanferro.ShutdownPolicy(drain_timeout=value, cancel_timeout=value, progress_interval=value)
    assert (shutdown.drain_timeout, shutdown.cancel_timeout, shutdown.progress_interval) == (2.5, 2.5, 2.5)
    assert fanferro.ShutdownPolicy(drain_timeout=None).drain_timeout is None
    assert fanferro.ShutdownPolicy() == fanferro.ShutdownPolicy(30.0, 10.0, 5.0, True, True, True)


@pytest.mark.parametrize(
    ("make", "error"),
    [
        (lambda: fanferro.Backoff(initial=timedelta(seconds=-1)), "initial"),
        (lambda: fanferro.Backoff(maximum=-1), "maximum"),
        (lambda: fanferro.Backoff(multiplier=0.5), "multiplier"),
        (lambda: fanferro.Backoff(multiplier=float("nan")), "multiplier"),
        (lambda: fanferro.ShutdownPolicy(drain_timeout=timedelta(seconds=-1)), "drain_timeout"),
        (lambda: fanferro.ShutdownPolicy(cancel_timeout=-1), "cancel_timeout"),
        (lambda: fanferro.ShutdownPolicy(progress_interval=timedelta(0)), "progress_interval must be positive"),
    ],
)
def test_policies_out_of_range(make: Any, error: str) -> None:
    with pytest.raises(ValueError, match=error):
        make()


@pytest.mark.parametrize("value", SECONDS, ids=["float", "timedelta"])
async def test_actions_and_settlements_take_timedelta(value: float | timedelta) -> None:
    assert fanferro.Retry(delay=value).delay == 2.5
    assert fanferro.Requeue(delay=value).delay == 2.5
    for method in ("retry", "requeue"):
        settler = RecordingSettler()
        message = build_message({"a": 1}, settler=settler)
        await getattr(message, method)(delay=value)
        assert settler.calls[-1][1]["delay"] == 2.5
    settler = RecordingSettler()
    message = build_message({"a": 1}, settler=settler)
    await message.extend(value)
    assert settler.calls[-1] == ("extend", {"timeout": 2.5})
    with pytest.raises(ValueError, match="timeout"):
        await message.extend(timedelta(seconds=-1))


@pytest.mark.parametrize("value", SECONDS, ids=["float", "timedelta"])
def test_outgoing_message_delay_takes_timedelta(value: float | timedelta) -> None:
    message = fanferro.OutgoingMessage({"a": 1}, delay=value)
    assert message.delay == 2.5
    with pytest.raises(ValueError, match="delay"):
        fanferro.OutgoingMessage({"a": 1}, delay=timedelta(seconds=-1))


async def test_publish_delay_takes_timedelta(memory_client: fanferro.Client) -> None:
    queue = memory_client.queue("delayed-publish")
    async with memory_client:
        await queue.send({"a": 1}, delay=timedelta(seconds=1))
        prepared = await queue.prepare({"a": 1}, delay=timedelta(seconds=3))
        assert prepared.delay == 3.0
        counts = await queue.approximate_count()
        assert counts["delayed"] == 1


@pytest.mark.parametrize("value", SECONDS, ids=["float", "timedelta"])
def test_idempotency_durations_take_timedelta(value: float | timedelta) -> None:
    middleware = IdempotencyMiddleware(InMemoryIdempotencyStore(), ttl=value, lock_ttl=value, retry_delay=value)
    assert (middleware.ttl, middleware.lock_ttl, middleware.retry_delay) == (2.5, 2.5, 2.5)


@pytest.mark.parametrize("value", SECONDS, ids=["float", "timedelta"])
def test_outbox_and_store_intervals_take_timedelta(value: float | timedelta) -> None:
    client = fanferro.testing.memory_client()
    relay = OutboxRelay(client, InMemoryOutbox(), poll_interval=value)
    assert relay.poll_interval == 2.5
    assert InMemoryIdempotencyStore(sweep_interval=value)._sweep_interval == 2.5
    with pytest.raises(ValueError, match="poll_interval"):
        OutboxRelay(client, InMemoryOutbox(), poll_interval=timedelta(seconds=-1))


async def test_run_until_idle_takes_timedelta(memory_client: fanferro.Client) -> None:
    queue = memory_client.queue("idle-durations")
    handled: list[object] = []

    async def handle(message: fanferro.Message) -> None:
        handled.append(message.body)

    consumer = memory_client.consumer(queue, handle, wait_time=1)
    async with memory_client:
        await queue.send({"a": 1})
        await consumer.run_until_idle(idle_for=timedelta(milliseconds=200), timeout=timedelta(seconds=10))
    assert handled == [{"a": 1}]
    with pytest.raises(ValueError, match="idle_for"):
        await consumer.run_until_idle(idle_for=timedelta(seconds=-1))
