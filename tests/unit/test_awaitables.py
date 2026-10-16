"""Awaitables of the native module: results, cancellation and event loops (crates/fanferro-py/src/runtime.rs)."""

from __future__ import annotations

import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

import fanferro
from fanferro import testing


async def test_cancelling_an_awaitable_drops_its_work() -> None:
    client = testing.memory_client()
    core = await client._core()
    # A pending next_events holds the event buffer: cancelling it must drop that wait at once.
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(core.next_events(10, 30.0), 0.05)
    started = time.monotonic()
    assert await core.next_events(10, 0.01) == []
    assert time.monotonic() - started < 1.0


async def test_many_concurrent_awaitables() -> None:
    client = testing.memory_client()
    queue = client.queue("many")
    async with client:
        results = await asyncio.gather(*(queue.send({"n": n}) for n in range(500)))
        assert len({result.message_id for result in results}) == 500
        assert (await queue.approximate_count())["visible"] == 500


async def roundtrip(client: fanferro.Client, name: str) -> list[Any]:
    """Publishes, consumes and acks a few messages (awaitables, settlements, events)."""
    queue = client.queue(name)
    received: list[Any] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.default
    async def handle(message: fanferro.Message[Any]) -> None:
        received.append(message.body)
        await message.ack()

    async with client:
        await queue.send_batch([{"n": n} for n in range(5)])
        await asyncio.gather(*(queue.send({"n": n}) for n in range(5, 10)))
        await consumer.run_until_idle(idle_for=0.2, timeout=15)
    return received


class NoFileDescriptors(asyncio.SelectorEventLoop):
    """A loop that cannot watch file descriptors (like the proactor event loop on Windows)."""

    def add_reader(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError


def test_loops_without_file_descriptors_get_results_too() -> None:
    loop = NoFileDescriptors()
    try:
        received = loop.run_until_complete(roundtrip(testing.memory_client(), "no-fds"))
    finally:
        loop.close()
    assert sorted(item["n"] for item in received) == list(range(10))


def test_event_loops_on_several_threads() -> None:
    broker = fanferro.MemoryBroker()

    def run(index: int) -> list[Any]:
        return asyncio.run(roundtrip(testing.memory_client(broker=broker), f"thread-{index}"))

    with ThreadPoolExecutor(4) as pool:
        results = list(pool.map(run, range(4)))
    assert [sorted(item["n"] for item in received) for received in results] == [list(range(10))] * 4


@pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="counts file descriptors with /proc")
def test_closed_event_loops_release_their_file_descriptors() -> None:
    async def once() -> None:
        core = await testing.memory_client()._core()
        await core.account_id()

    asyncio.run(once())
    before = len(os.listdir("/proc/self/fd"))
    for _ in range(20):
        asyncio.run(once())
    # Each loop that awaited something has a socket pair; the last one is released by the next loop.
    assert len(os.listdir("/proc/self/fd")) <= before + 2
