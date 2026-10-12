"""Idempotent consumer ("inbox"): side effects and the processed-message record in one transaction.

SQS delivers at least once and producers may publish twice, so the handler records the message id
in the same database transaction as its own writes. A unique constraint makes duplicates harmless.
For a ready-made middleware (Redis/DynamoDB/SQL store of your choice) see
``fanferro.idempotency.IdempotencyMiddleware``. Run: uv run python examples/idempotent_consumer.py
"""

from __future__ import annotations

import asyncio
import sqlite3

import fanferro
from examples._common import make_client, unique


async def main() -> int:
    client = make_client()
    queue = client.queue(unique("payments"))
    db = sqlite3.connect(":memory:", check_same_thread=False)
    db.executescript(
        "CREATE TABLE processed (message_id TEXT PRIMARY KEY);CREATE TABLE ledger (payment_id TEXT, amount INTEGER);"
    )
    consumer = client.consumer(queue, wait_time=1, concurrency=1)

    @consumer.default
    async def record_payment(message: fanferro.Message[dict[str, int]]) -> None:
        def apply() -> None:
            try:
                with db:  # one transaction: dedup record + business write
                    db.execute("INSERT INTO processed (message_id) VALUES (?)", (message.id,))
                    db.execute("INSERT INTO ledger VALUES (?, ?)", (message.id, message.body["amount"]))
            except sqlite3.IntegrityError:
                print(f"skipping duplicate {message.id}")

        await asyncio.to_thread(apply)

    async with client:
        # The same logical message published three times (e.g. producer retries).
        for _ in range(3):
            await queue.send({"amount": 100}, id="payment-123")
        await consumer.run_until_idle(timeout=30)

    total = db.execute("SELECT COALESCE(SUM(amount), 0) FROM ledger").fetchone()[0]
    db.close()
    print(f"ledger total: {total}")
    return int(total)


if __name__ == "__main__":
    asyncio.run(main())
