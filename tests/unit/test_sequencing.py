"""FIFO group sequencing: the consumer's scheduler that runs a group's messages in order."""

from __future__ import annotations

import asyncio
from typing import Any, cast

from fanferro.consumer import _GroupSequencer


class _Consumer:
    """The parts of a consumer the sequencer uses; outcomes and redeliveries are scripted."""

    def __init__(self, outcomes: dict[str, str | None]) -> None:
        self.outcomes = outcomes
        self.redeliveries: dict[str, list[str]] = {}  # released message -> what arrives meanwhile
        self.sequencer = _GroupSequencer(cast(Any, self))
        self.tasks: list[asyncio.Task[None]] = []
        self.ran: list[str] = []
        self.released: list[str] = []
        self._waiting = 0

    def _spawn(self, coroutine: Any) -> None:
        self.tasks.append(asyncio.ensure_future(coroutine))

    async def _run(self, delivery: str, *, counted: bool = False) -> str | None:
        if counted:
            self._waiting -= 1  # as the consumer does once the message has a handler slot
        self.ran.append(delivery)
        return self.outcomes.get(delivery, "acked")

    async def _release_quietly(self, delivery: str) -> None:
        self.released.append(delivery)
        for redelivered in self.redeliveries.get(delivery, []):
            self.sequencer.submit("g", cast(Any, redelivered))
        await asyncio.sleep(0)

    def _task_ended(self) -> None: ...

    def _task_failed(self, error: Exception) -> None:
        raise error

    async def run(self, *deliveries: str) -> None:
        for delivery in deliveries:
            self.sequencer.submit("g", cast(Any, delivery))
        while not all(task.done() for task in self.tasks):
            await asyncio.gather(*self.tasks)


async def test_a_message_that_comes_back_hands_back_the_rest_of_its_group() -> None:
    consumer = _Consumer({"m1": "retried"})
    await consumer.run("m0", "m1", "m2", "m3")
    assert consumer.ran == ["m0", "m1"]
    assert consumer.released == ["m2", "m3"]


async def test_a_redelivery_that_arrives_while_the_group_is_handed_back_is_processed() -> None:
    """The group's redelivery (the retried message first) runs instead of being handed back too."""
    consumer = _Consumer({"m0": "retried"})
    consumer.redeliveries["m2"] = ["m0 again", "m1 again", "m2 again"]
    await consumer.run("m0", "m1", "m2")
    assert consumer.released == ["m1", "m2"]
    assert consumer.ran == ["m0", "m0 again", "m1 again", "m2 again"]
    assert consumer._waiting == 0
