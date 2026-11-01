"""Consumer and client health: receive failures, gaps, readiness (consumer.health(), client.health())."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest

import fanferro
from fanferro import testing
from tests.conftest import ClientFactory
from tests.helpers import wait_for


async def test_a_running_consumer_is_healthy_and_ready(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("healthy"))
    consumer = client.consumer(queue, _ignore, wait_time=1)

    before = consumer.health()
    assert before.state == "created"
    assert not before.healthy()
    assert not before.ready()
    assert before.problems() == (f"consumer {consumer.name} is created",)
    assert (before.last_receive_at, before.last_dispatch_at, before.dispatch_gap) == (None, None, None)

    async with client:
        await consumer.start()
        await wait_for(lambda: consumer.health().last_receive_at is not None)
        health = consumer.health()
        assert health.state == "running"
        assert health.consecutive_receive_failures == 0
        assert health.last_error is None
        assert health.last_dispatch_at is not None
        assert health.dispatch_gap is not None
        assert health.dispatch_gap < 5
        assert health.receive_gap < 5
        assert health.healthy()
        assert health.ready()
        assert client.health().healthy()
        assert client.health().ready()

        # Paused: alive, but takes no work.
        consumer.pause()
        paused = consumer.health()
        assert paused.paused
        assert paused.healthy(max_receive_gap=0)
        assert not paused.ready()
        assert not client.health().ready()
        consumer.resume()
        await consumer.stop()

    stopped = consumer.health()
    assert stopped.state == "stopped"
    assert not stopped.healthy()
    assert client.health().problems() == (f"consumer {consumer.name} is stopped",)


async def test_a_deleted_queue_shows_in_health(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("deleted"))
    consumer = client.consumer(queue, _ignore, wait_time=1)
    async with client:
        await consumer.start()
        await wait_for(lambda: consumer.health().last_receive_at is not None)
        await queue.delete()
        await wait_for(lambda: consumer.health().consecutive_receive_failures >= 2, timeout=20)
        await wait_for(lambda: consumer.health().receive_gap > 1.5, timeout=20)
        health = consumer.health()
        assert health.state == "running", "the consumer keeps retrying by default (the queue may come back)"
        error = health.last_error
        assert error is not None
        assert error.error_type == "fanferro.errors.ResourceNotFoundError"
        assert error.category == "not_found"
        assert not error.retryable
        assert error.at >= health.last_receive_at  # type: ignore[operator]
        assert consumer.stats().engine.receive_errors >= health.consecutive_receive_failures
        assert health.receive_gap > 1
        problems = health.problems(max_receive_gap=1)
        assert len(problems) == 1
        assert f"has received nothing from queue {queue.name}" in problems[0]
        assert "receives failed (last: ResourceNotFoundError" in problems[0]
        assert not health.healthy(max_receive_gap=1)
        assert not health.ready(max_receive_gap=1)
        assert health.as_dict()["last_error"]["at"] == error.at.isoformat()
        await consumer.stop()


async def test_max_receive_failures_stops_the_consumer_and_run_raises(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("gone"))
    errors: list[fanferro.Event] = []
    client.on("consumer.error", errors.append)
    consumer = client.consumer(queue, _ignore, wait_time=1, max_receive_failures=2)
    async with client:
        run = asyncio.ensure_future(consumer.run(install_signal_handlers=False))
        await wait_for(lambda: consumer.health().last_receive_at is not None)
        await queue.delete()
        with pytest.raises(fanferro.ResourceNotFoundError):
            await asyncio.wait_for(run, 30)
    assert consumer.state == "stopped"
    assert len(errors) == 1
    assert errors[0].fields["consecutive_failures"] >= 2
    assert errors[0].error is not None
    assert errors[0].error.category == "not_found"
    (problem,) = consumer.health().problems()
    assert problem.startswith(f"consumer {consumer.name} is stopped after ")
    assert "failed receives (last: ResourceNotFoundError" in problem


async def test_retryable_receive_failures_do_not_stop_the_consumer() -> None:
    client = testing.memory_client()
    queue = client.queue("throttled")
    consumer = client.consumer(queue, _ignore, wait_time=1, max_receive_failures=1)
    async with client:
        await consumer.start()
        await wait_for(lambda: consumer.health().last_receive_at is not None)
        engine = consumer._engine
        # A throttled (retryable) receive, as the engine would report it.
        throttled = fanferro.ThrottledError("sqs:ReceiveMessage failed (throttling)")
        throttled.retryable = True  # type: ignore[attr-defined]
        consumer._engine = _FailingHealth(engine, {"consecutive_failures": 5, "last_error": throttled})
        await asyncio.sleep(1.2)
        assert consumer.state == "running"
        consumer._engine = engine
        await consumer.stop()


async def _ignore(message: fanferro.Message[Any]) -> None:
    pass


class _FailingHealth:
    """Delegates to an engine but reports the given receive health."""

    def __init__(self, engine: Any, health: dict[str, Any]) -> None:
        self._engine = engine
        self._health = health

    def receive_health(self) -> dict[str, Any]:
        return {**self._engine.receive_health(), **self._health}

    def __getattr__(self, attribute: str) -> Any:
        return getattr(self._engine, attribute)


def test_max_receive_failures_must_be_positive() -> None:
    client = testing.memory_client()
    with pytest.raises(ValueError, match="max_receive_failures must be at least 1"):
        client.consumer("q", _ignore, max_receive_failures=0)


async def test_a_blocked_event_loop_shows_in_the_dispatch_gap() -> None:
    client = testing.memory_client()
    queue = client.queue("blocking")
    consumer = client.consumer(queue, wait_time=1)
    handled = asyncio.Event()

    @consumer.default
    async def handle(message: fanferro.Message[Any]) -> None:
        time.sleep(1.5)  # noqa: ASYNC251 - deliberately blocks the event loop
        handled.set()

    gaps: list[float] = []
    problems: list[str] = []
    sampling = threading.Event()

    def sample() -> None:
        # From another thread, like a probe that must also catch a blocked loop.
        while not sampling.is_set():
            health = consumer.health()
            if health.dispatch_gap is not None:
                gaps.append(health.dispatch_gap)
                problems.extend(health.problems(max_dispatch_gap=1))
            time.sleep(0.05)

    async with client:
        await consumer.start()
        thread = threading.Thread(target=sample)
        thread.start()
        try:
            await queue.send({"n": 1})
            await asyncio.wait_for(handled.wait(), 15)
        finally:
            sampling.set()
            thread.join()
        await wait_for(lambda: (consumer.health().dispatch_gap or 0) < 1)
        await consumer.stop()
    assert max(gaps) > 1.2
    assert any("has not dispatched messages for" in problem for problem in problems)


async def test_client_run_stops_every_consumer_and_raises_when_one_fails() -> None:
    client = testing.memory_client()
    gone = client.queue("client-run-gone")
    other = client.queue("client-run-other")
    failing = client.consumer(gone, _ignore, wait_time=1, max_receive_failures=1)
    healthy = client.consumer(other, _ignore, wait_time=1)
    async with client:
        run = asyncio.ensure_future(client.run(install_signal_handlers=False))
        await wait_for(lambda: failing.health().last_receive_at is not None)
        await gone.delete()
        with pytest.raises(fanferro.ResourceNotFoundError):
            await asyncio.wait_for(run, 30)
    assert failing.state == healthy.state == "stopped"
