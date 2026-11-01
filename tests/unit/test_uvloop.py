"""fanferro on uvloop's event loop (which uvicorn and many servers pick when it is installed).

These tests create uvloop loops themselves, so they run in every test session where uvloop is
installed. ``FANFERRO_TEST_EVENT_LOOP=uvloop`` runs every other async test on uvloop as well
(see tests/conftest.py).
"""

from __future__ import annotations

import asyncio
import os
import signal
from collections.abc import Callable, Coroutine
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest

import fanferro
from fanferro import testing
from tests.helpers import wait_for
from tests.unit.test_awaitables import roundtrip

uvloop = pytest.importorskip("uvloop", reason="uvloop is not installed (it does not support Windows)")

has_proc_fds = pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="counts file descriptors with /proc")


def run_on_uvloop[T](main: Callable[[], Coroutine[Any, Any, T]]) -> T:
    with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
        assert isinstance(runner.get_loop(), uvloop.Loop)
        return runner.run(main())


def open_fds() -> int:
    return len(os.listdir("/proc/self/fd"))


async def test_async_tests_run_on_the_configured_event_loop() -> None:
    # Guards FANFERRO_TEST_EVENT_LOOP: a CI job that asks for uvloop must really get it.
    on_uvloop = isinstance(asyncio.get_running_loop(), uvloop.Loop)
    assert on_uvloop == (os.environ.get("FANFERRO_TEST_EVENT_LOOP") == "uvloop")


def test_publish_consume_and_settle() -> None:
    received = run_on_uvloop(lambda: roundtrip(testing.memory_client(), "uvloop"))
    assert sorted(item["n"] for item in received) == list(range(10))


def test_settlements_start_eagerly() -> None:
    async def main() -> None:
        client = testing.memory_client()
        queue = client.queue("uvloop-settlements")
        settlements: list[fanferro.Settlement] = []
        consumer = client.consumer(queue, wait_time=1)

        @consumer.default
        async def handle(message: fanferro.Message[Any]) -> None:
            settlement = message.ack()
            settlements.append(settlement)
            await settlement

        async with client:
            await queue.send_batch([{"n": n} for n in range(3)])
            await consumer.run_until_idle(idle_for=0.2, timeout=15)
            assert (await queue.approximate_count())["visible"] == 0
        assert len(settlements) == 3
        assert all(settlement.done() for settlement in settlements)

    run_on_uvloop(main)


def test_cancelling_an_awaitable_drops_its_work() -> None:
    async def main() -> None:
        core = await testing.memory_client()._core()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(core.next_events(10, 30.0), 0.05)
        loop = asyncio.get_running_loop()
        started = loop.time()
        assert await core.next_events(10, 0.01) == []
        assert loop.time() - started < 1.0

    run_on_uvloop(main)


def test_results_arrive_through_the_loops_socket() -> None:
    # uvloop watches file descriptors, so the bridge must wake it through its socket pair rather
    # than the call_soon_threadsafe fallback of loops without add_reader.
    wakes: list[str] = []

    class Recording(uvloop.Loop):
        def add_reader(self, fd: Any, callback: Callable[..., object], *args: Any) -> None:
            wakes.append("add_reader")
            super().add_reader(fd, callback, *args)

        def call_soon_threadsafe(self, callback: Callable[..., object], *args: Any, **kwargs: Any) -> Any:
            wakes.append("call_soon_threadsafe")
            return super().call_soon_threadsafe(callback, *args, **kwargs)

    async def main() -> None:
        core = await testing.memory_client()._core()
        await asyncio.gather(*(core.account_id() for _ in range(10)))

    with asyncio.Runner(loop_factory=Recording) as runner:
        runner.run(main())
    assert wakes == ["add_reader"]


@has_proc_fds
def test_closed_loops_release_their_file_descriptors() -> None:
    async def once() -> None:
        core = await testing.memory_client()._core()
        await core.account_id()

    run_on_uvloop(once)
    before = open_fds()
    for _ in range(20):
        run_on_uvloop(once)
    # The last loop's socket pair is released when the next loop registers.
    assert open_fds() <= before + 2


def test_loops_on_several_threads() -> None:
    broker = fanferro.MemoryBroker()

    def run(index: int) -> list[Any]:
        return run_on_uvloop(lambda: roundtrip(testing.memory_client(broker=broker), f"uvloop-thread-{index}"))

    with ThreadPoolExecutor(4) as pool:
        results = list(pool.map(run, range(4)))
    assert [sorted(item["n"] for item in received) for received in results] == [list(range(10))] * 4


def test_asyncio_and_uvloop_loops_side_by_side() -> None:
    # One process can run both kinds of loop (a server on uvloop, a script on asyncio.run).
    broker = fanferro.MemoryBroker()
    with ThreadPoolExecutor(2) as pool:
        on_uvloop = pool.submit(run_on_uvloop, lambda: roundtrip(testing.memory_client(broker=broker), "mixed-uv"))
        on_asyncio = pool.submit(asyncio.run, roundtrip(testing.memory_client(broker=broker), "mixed-asyncio"))
        results = [on_uvloop.result(), on_asyncio.result()]
    assert [sorted(item["n"] for item in received) for received in results] == [list(range(10))] * 2


@pytest.mark.skipif(not hasattr(signal, "SIGTERM"), reason="POSIX signals")
def test_signals_stop_run() -> None:
    async def main() -> str:
        client = testing.memory_client()
        consumer = client.consumer(client.queue("uvloop-signals"), wait_time=1)

        @consumer.default
        async def handle(message: fanferro.Message[Any]) -> None: ...

        async with client:
            task = asyncio.create_task(consumer.run())
            await wait_for(lambda: consumer.state == "running")
            os.kill(os.getpid(), signal.SIGTERM)
            await asyncio.wait_for(task, 15)
        return consumer.state

    assert run_on_uvloop(main) == "stopped"
