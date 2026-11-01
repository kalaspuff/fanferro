"""Test helpers shared by integration and e2e tests."""

from __future__ import annotations

import asyncio
import dataclasses
import time
from collections.abc import Callable
from typing import Any

import pydantic

import fanferro


@fanferro.message_type("test.order_created.v1")
class OrderCreated(pydantic.BaseModel):
    order_id: int
    total: float = 0.0


@fanferro.message_type("test.order_cancelled.v1")
@dataclasses.dataclass
class OrderCancelled:
    order_id: int
    reason: str = ""


async def wait_for(condition: Callable[[], bool], timeout: float = 15.0, interval: float = 0.05) -> None:
    """Waits until ``condition()`` is true."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise TimeoutError("condition not met in time")
        await asyncio.sleep(interval)


async def consume_until(consumer: fanferro.Consumer, condition: Callable[[], bool], timeout: float = 20.0) -> None:
    """Runs ``consumer`` until ``condition()`` is true, then stops it."""
    if consumer.state != "running":
        await consumer.start()
    try:
        await wait_for(condition, timeout)
    finally:
        await consumer.stop()


async def drain_queue(
    client: fanferro.Client, queue: fanferro.Queue, timeout: float = 15.0
) -> list[fanferro.Message[Any]]:
    """Receives everything currently in ``queue`` (acks it) and returns the messages.

    The in-memory broker hands out what a queue holds at once, so a short quiet spell is enough
    there; an emulator gets a second.
    """
    received: list[fanferro.Message[Any]] = []

    async def collect(message: fanferro.Message[bytes]) -> None:
        received.append(message)

    consumer = client.consumer(queue, collect, wait_time=1, name=f"drain-{queue.name}")
    idle_for = 0.3 if client.broker is not None else 1.0
    await consumer.run_until_idle(idle_for=idle_for, timeout=timeout)
    return received
