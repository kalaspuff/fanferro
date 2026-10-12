"""FIFO: per-entity ordering with a FIFO topic fanning out to a FIFO queue.

Messages with the same group id (here: the account) are processed strictly in order, one at a
time; different groups are processed concurrently. Publishing the same message id twice within
five minutes is deduplicated by SNS/SQS.
Run: uv run python examples/fifo_ordering.py
"""

from __future__ import annotations

import asyncio
import dataclasses

import fanferro
from examples._common import make_client, unique


@fanferro.message_type("bank.transaction.v1")
@dataclasses.dataclass
class Transaction:
    account: str
    sequence: int
    amount: int


async def main() -> dict[str, list[int]]:
    client = make_client()
    ledger = client.topic(
        f"{unique('ledger')}.fifo",
        group_id=lambda message: message.body.account,  # group id derived from each message
    )
    projections = client.queue(f"{unique('balances')}.fifo")
    client.subscription(ledger, projections)
    order: dict[str, list[int]] = {}
    consumer = client.consumer(projections, concurrency=8, wait_time=1)

    @consumer.route(Transaction)
    async def apply(tx: Transaction) -> None:
        await asyncio.sleep(0.01)
        order.setdefault(tx.account, []).append(tx.sequence)

    async with client:
        for sequence in range(5):
            for account in ("alice", "bob"):
                await ledger.publish(Transaction(account, sequence, 10), id=f"{account}-{sequence}")
        await ledger.publish(Transaction("alice", 0, 10), id="alice-0")  # duplicate: dropped
        await consumer.run_until_idle(timeout=60)

    print(order)
    return order


if __name__ == "__main__":
    asyncio.run(main())
