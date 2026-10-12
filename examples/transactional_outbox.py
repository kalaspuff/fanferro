"""Transactional outbox with SQLite: the business write and the message commit together.

1. In one database transaction, insert the order and the encoded message (``topic.prepare``).
2. A relay publishes pending outbox rows and marks them published (at-least-once; the message id
   is stable, so consumers deduplicate with the idempotency middleware or FIFO deduplication).

The same shape works with PostgreSQL (``SELECT ... FOR UPDATE SKIP LOCKED``), see
docs/guides/patterns.md. Run: uv run python examples/transactional_outbox.py
"""

from __future__ import annotations

import asyncio
import dataclasses
import sqlite3
from collections.abc import Sequence

import fanferro
from examples._common import make_client, unique
from fanferro.outbox import OutboxRecord, OutboxRelay


@fanferro.message_type("shop.order_created.v1")
@dataclasses.dataclass
class OrderCreated:
    order_id: str
    total: int


class SqliteOutbox:
    """An :class:`fanferro.outbox.OutboxStore` on SQLite (blocking calls run in a thread)."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.db = connection
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS outbox (id TEXT PRIMARY KEY, message TEXT NOT NULL, "
            "attempts INTEGER NOT NULL DEFAULT 0, published INTEGER NOT NULL DEFAULT 0)"
        )

    def add(self, cursor: sqlite3.Cursor, message: fanferro.PreparedMessage) -> None:
        """Called inside the business transaction."""
        cursor.execute("INSERT INTO outbox (id, message) VALUES (?, ?)", (message.message_id, message.to_json()))

    async def claim_batch(self, limit: int) -> Sequence[OutboxRecord]:
        def query() -> list[OutboxRecord]:
            rows = self.db.execute(
                "SELECT id, message, attempts FROM outbox WHERE published = 0 ORDER BY rowid LIMIT ?", (limit,)
            ).fetchall()
            return [OutboxRecord(row[0], fanferro.PreparedMessage.from_json(row[1]), row[2]) for row in rows]

        return await asyncio.to_thread(query)

    async def mark_published(self, ids: Sequence[str]) -> None:
        def update() -> None:
            with self.db:
                self.db.executemany("UPDATE outbox SET published = 1 WHERE id = ?", [(i,) for i in ids])

        await asyncio.to_thread(update)

    async def mark_failed(self, record: OutboxRecord, error: BaseException) -> None:
        def update() -> None:
            with self.db:
                self.db.execute("UPDATE outbox SET attempts = attempts + 1 WHERE id = ?", (record.id,))

        await asyncio.to_thread(update)


async def place_order(db: sqlite3.Connection, outbox: SqliteOutbox, topic: fanferro.Topic, order_id: str) -> None:
    """The unit of work: the order row and its event are committed atomically."""
    message = await topic.prepare(OrderCreated(order_id=order_id, total=42))
    with db:  # one transaction
        cursor = db.cursor()
        cursor.execute("INSERT INTO orders (id) VALUES (?)", (order_id,))
        outbox.add(cursor, message)


async def main() -> list[str]:
    client = make_client()
    topic = client.topic(unique("orders"))
    queue = client.queue(unique("order-events"))
    client.subscription(topic, queue)
    db = sqlite3.connect(":memory:", check_same_thread=False)
    db.execute("CREATE TABLE orders (id TEXT PRIMARY KEY)")
    outbox = SqliteOutbox(db)
    received: list[str] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        received.append(order.order_id)

    async with client:
        for order_id in ("a-1", "a-2", "a-3"):
            await place_order(db, outbox, topic, order_id)
        relay = OutboxRelay(client, outbox, batch_size=50)
        published = await relay.run_once()  # in production: asyncio.create_task(relay.run())
        print(f"relayed {published} messages")
        await consumer.run_until_idle(timeout=30)

    db.close()
    print(f"received {sorted(received)}")
    return sorted(received)


if __name__ == "__main__":
    asyncio.run(main())
