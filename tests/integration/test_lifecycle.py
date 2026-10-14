from __future__ import annotations

import asyncio
import os
import signal
import time
from typing import Any

import pytest

import fanferro
from tests.conftest import ClientFactory
from tests.helpers import OrderCreated, consume_until, wait_for


async def test_heartbeats_protect_slow_handlers(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("slow"), attributes=fanferro.QueueAttributes(visibility_timeout=2))
    receive_counts: list[int] = []
    extended: list[fanferro.Event] = []
    client.on("lease.extended", extended.append)
    consumer = client.consumer(queue, wait_time=1, concurrency=2, heartbeat=fanferro.Heartbeat(interval=0.5))

    @consumer.route(OrderCreated)
    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        receive_counts.append(message.receive_count)
        await asyncio.sleep(5)

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await consume_until(consumer, lambda: consumer.stats()["processed"] == 1, timeout=30)
    assert receive_counts == [1], "the message must not be redelivered while it is being processed"
    assert len(extended) >= 3
    assert consumer.stats()["engine"]["lease_extensions"] >= 3


async def test_graceful_shutdown_waits_for_in_flight(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("drain"))
    started = asyncio.Event()
    finished: list[int] = []
    draining: list[fanferro.Event] = []
    client.on("consumer.draining", draining.append)
    consumer = client.consumer(
        queue, wait_time=1, shutdown=fanferro.ShutdownPolicy(drain_timeout=10, progress_interval=0.2)
    )

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        started.set()
        await asyncio.sleep(1)
        finished.append(order.order_id)

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await consumer.start()
        await asyncio.wait_for(started.wait(), 15)
        await consumer.stop()
        assert finished == [1]
        assert consumer.state == "stopped"
        assert draining, "progress events are emitted while draining"
        assert (await queue.approximate_count()) == {"visible": 0, "in_flight": 0, "delayed": 0}


async def test_drain_timeout_cancels_and_releases(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("stuck"), attributes=fanferro.QueueAttributes(visibility_timeout=60))
    started = asyncio.Event()
    cancelled: list[bool] = []
    consumer = client.consumer(
        queue, wait_time=1, shutdown=fanferro.ShutdownPolicy(drain_timeout=0.3, cancel_timeout=2)
    )

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await consumer.start()
        await asyncio.wait_for(started.wait(), 15)
        began = time.monotonic()
        await consumer.stop()
        assert time.monotonic() - began < 5
        assert cancelled == [True]
        # The unfinished message was released: another consumer gets it right away.
        second: list[int] = []
        again = client.consumer(queue, wait_time=1, name="second")

        @again.route(OrderCreated)
        async def redo(message: fanferro.Message[OrderCreated]) -> None:
            second.append(message.receive_count)

        await consume_until(again, lambda: len(second) == 1)
    assert second == [2]


async def test_stop_returns_immediately_when_idle(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("idle-stop"))
    consumer = client.consumer(queue, wait_time=20, shutdown=fanferro.ShutdownPolicy(abort_receives=True))

    @consumer.default
    async def handle(message: fanferro.Message[Any]) -> None: ...

    async with client:
        await consumer.start()
        await wait_for(lambda: consumer.stats()["engine"].get("receive_requests", 0) >= 1)
        await asyncio.sleep(0.2)  # a long poll is in flight
        began = time.monotonic()
        await consumer.stop()
        elapsed = time.monotonic() - began
    assert consumer.state == "stopped"
    assert elapsed < 0.25, f"stop() took {elapsed:.3f} s"


async def test_stop_waits_for_running_handlers(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("busy-stop"))
    started = asyncio.Event()
    finished: list[int] = []
    consumer = client.consumer(queue, wait_time=20, shutdown=fanferro.ShutdownPolicy(abort_receives=True))

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        started.set()
        await asyncio.sleep(1)
        finished.append(order.order_id)

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await consumer.start()
        await asyncio.wait_for(started.wait(), 15)
        began = time.monotonic()
        await consumer.stop()
        elapsed = time.monotonic() - began
        assert finished == [1]
        assert 0.5 < elapsed < 2.5, f"stop() took {elapsed:.3f} s"
        assert consumer.stats()["engine"]["messages_acked"] == 1
        assert (await queue.approximate_count()) == {"visible": 0, "in_flight": 0, "delayed": 0}


async def test_stop_mid_stream_loses_nothing(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("restart"), attributes=fanferro.QueueAttributes(visibility_timeout=2))
    seen: set[int] = set()
    first = client.consumer(
        queue, wait_time=2, concurrency=2, prefetch=4, shutdown=fanferro.ShutdownPolicy(abort_receives=True)
    )

    @first.route(OrderCreated)
    async def slow(order: OrderCreated) -> None:
        await asyncio.sleep(0.05)
        seen.add(order.order_id)

    async with client:
        await queue.send_batch([OrderCreated(order_id=i) for i in range(30)])
        await first.start()
        await wait_for(lambda: len(seen) >= 5)
        await first.stop()
        assert len(seen) < 30
        second = client.consumer(queue, wait_time=1, name="second")

        @second.route(OrderCreated)
        async def rest(order: OrderCreated) -> None:
            seen.add(order.order_id)

        # Messages handed to the aborted long poll reappear after their visibility timeout.
        await consume_until(second, lambda: len(seen) == 30, timeout=60)
    assert seen == set(range(30))


async def test_client_close_is_immediate_and_keeps_events(memory_client: fanferro.Client, name: Any) -> None:
    queue = memory_client.queue(name("close"))
    kinds: list[str] = []
    memory_client.on("consumer.*", lambda event: kinds.append(event.kind.value))
    consumer = memory_client.consumer(queue, wait_time=20, shutdown=fanferro.ShutdownPolicy(abort_receives=True))

    @consumer.default
    async def handle(message: fanferro.Message[Any]) -> None: ...

    await memory_client.__aenter__()
    await consumer.start()
    await asyncio.sleep(0.1)
    began = time.monotonic()
    await memory_client.close()  # stops the consumer and the event pump
    elapsed = time.monotonic() - began
    assert consumer.state == "stopped"
    assert elapsed < 0.25, f"close() took {elapsed:.3f} s"
    assert "consumer.stopped" in kinds, "the engine's last event is dispatched before close() returns"


async def test_concurrency_limit(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("concurrency"))
    active = 0
    peak = 0
    done: list[int] = []
    consumer = client.consumer(queue, wait_time=1, concurrency=3, prefetch=2)

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.2)
        active -= 1
        done.append(order.order_id)

    async with client:
        await queue.send_batch([OrderCreated(order_id=i) for i in range(12)])
        await consume_until(consumer, lambda: len(done) == 12, timeout=30)
    assert peak == 3


async def test_rate_limit(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("rate"))
    done: list[float] = []
    consumer = client.consumer(queue, wait_time=1, concurrency=10, rate_limit=fanferro.RateLimit(per_second=4, burst=2))

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        done.append(time.monotonic())

    async with client:
        await queue.send_batch([OrderCreated(order_id=i) for i in range(8)])
        began = time.monotonic()
        await consume_until(consumer, lambda: len(done) == 8, timeout=30)
    assert time.monotonic() - began >= 1.2


async def test_pause_and_resume(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("pause"))
    done: list[int] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        done.append(order.order_id)

    async with client:
        await consumer.start()
        consumer.pause()
        await asyncio.sleep(1.5)
        await queue.send(OrderCreated(order_id=1))
        await asyncio.sleep(1.5)
        assert done == []
        consumer.resume()
        await wait_for(lambda: done == [1])
        await consumer.stop()


async def test_run_until_request_stop_and_client_run(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("run"))
    done: list[int] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        done.append(order.order_id)
        if order.order_id == 1:
            consumer.request_stop()

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await asyncio.wait_for(consumer.run(install_signal_handlers=False), 30)
        assert done == [1]
        assert consumer.state == "stopped"

        other = client.consumer(queue, wait_time=1, name="other")

        @other.route(OrderCreated)
        async def again(order: OrderCreated) -> None:
            done.append(order.order_id)
            client.stop()

        await queue.send(OrderCreated(order_id=2))
        await asyncio.wait_for(client.run(other, install_signal_handlers=False), 30)
    assert done == [1, 2]


@pytest.mark.skipif(not hasattr(signal, "SIGTERM") or os.name == "nt", reason="POSIX signals")
async def test_signals_stop_run(memory_client: fanferro.Client, name: Any) -> None:
    queue = memory_client.queue(name("signals"))
    consumer = memory_client.consumer(queue, wait_time=1)

    @consumer.default
    async def handle(message: fanferro.Message[Any]) -> None: ...

    async with memory_client:
        task = asyncio.create_task(consumer.run())
        await wait_for(lambda: consumer.state == "running")
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.wait_for(task, 15)
    assert consumer.state == "stopped"


async def test_events_and_metrics(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    queue = client.queue(name("observed"))
    kinds: list[str] = []
    client.on("*", lambda event: kinds.append(event.kind.value))
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None: ...

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await consume_until(consumer, lambda: consumer.stats()["processed"] == 1)
        await asyncio.sleep(1.2)
    for expected in (
        "resource.created",
        "publish.succeeded",
        "consumer.starting",
        "consumer.started",
        "message.received",
        "message.processed",
        "message.acked",
        "consumer.stopping",
        "consumer.stopped",
    ):
        assert expected in kinds, expected
    metrics = client.metrics()
    assert metrics["client"]["messages_published"] == 1
    stats = metrics["consumers"][consumer.name]
    assert stats["processed"] == 1
    assert stats["engine"]["messages_acked"] == 1
    assert stats["engine"]["in_flight"] == 0


async def test_consumer_state_errors(memory_client: fanferro.Client, name: Any) -> None:
    queue = memory_client.queue(name("state"))
    empty = memory_client.consumer(queue, wait_time=1)
    with pytest.raises(fanferro.ConsumerStateError, match="no routes"):
        await empty.start()

    @empty.default
    async def handle(message: fanferro.Message[Any]) -> None: ...

    async with memory_client:
        await empty.start()
        with pytest.raises(fanferro.ConsumerStateError):
            await empty.start()
        await empty.stop()
        await empty.stop()
        await empty.start()
        await empty.stop()
