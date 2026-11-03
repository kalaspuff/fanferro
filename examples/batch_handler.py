"""Batch handler: one database round trip for many messages, with per-message failures.

A batch route collects the messages it matches (up to ``max_size``, waiting at most ``max_wait``
after the first one) and calls its handler once per batch. The handler inserts the whole batch in
one transaction and reports the rows it could not store in a ``BatchResult``: those messages are
retried or dead-lettered on their own, and the rest of the batch is acked.
Run: uv run python -m examples.batch_handler
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass

import fanferro
from examples._common import make_client, unique


@fanferro.message_type("examples.reading_recorded.v1")
@dataclass
class ReadingRecorded:
    sensor: str
    value: float


async def main() -> int:
    client = make_client()
    queue = client.queue(unique("readings"), dead_letter=fanferro.DeadLetterQueue(unique("readings-dlq")))
    db = sqlite3.connect(":memory:", check_same_thread=False)
    db.execute("CREATE TABLE readings (sensor TEXT NOT NULL, value REAL CHECK (value >= 0))")
    # One batch call at a time (concurrency counts handler calls): the batches share a connection.
    consumer = client.consumer(queue, wait_time=1, concurrency=1)
    batch_sizes: list[int] = []

    @consumer.route(ReadingRecorded, batch=fanferro.Batch(max_size=50, max_wait=0.5))
    async def store(messages: list[fanferro.Message[ReadingRecorded]]) -> fanferro.BatchResult:
        batch_sizes.append(len(messages))
        result = fanferro.BatchResult()

        def insert() -> None:
            with db:  # one transaction for the batch
                for message in messages:
                    try:
                        db.execute("INSERT INTO readings VALUES (?, ?)", (message.body.sensor, message.body.value))
                    except sqlite3.IntegrityError as error:
                        # A row that can never be stored: dead-letter this message only.
                        result.fail(message, fanferro.Reject(f"invalid reading: {error}"))

        await asyncio.to_thread(insert)
        return result

    async with client:
        await queue.send_batch([ReadingRecorded("s1", float(i)) for i in range(99)])
        await queue.send(ReadingRecorded("s2", -1.0))  # violates the CHECK constraint
        await consumer.run_until_idle(timeout=30)

    stored = db.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
    db.close()
    stats = consumer.stats()
    print(f"batches: {batch_sizes}; stored {stored} readings, dead-lettered {stats.engine.messages_dead_lettered}")
    assert stored == 99
    assert stats.engine.messages_dead_lettered == 1
    return int(stored)


if __name__ == "__main__":
    asyncio.run(main())
