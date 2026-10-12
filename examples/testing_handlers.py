"""Testing: unit-test handlers with ``build_message`` and flows with the in-memory broker.

In your project, put functions like these in pytest test modules (async tests need
``pytest-asyncio`` or ``anyio``). Run: uv run python examples/testing_handlers.py
"""

from __future__ import annotations

import asyncio
import dataclasses

import fanferro
from fanferro.testing import build_message, memory_client


@fanferro.message_type("billing.invoice_due.v1")
@dataclasses.dataclass
class InvoiceDue:
    invoice_id: str
    amount: int


async def remind(message: fanferro.Message[InvoiceDue]) -> None:
    """The handler under test."""
    if message.body.amount <= 0:
        raise fanferro.Reject("nothing to collect")
    if message.attempt > 3:
        await message.discard("too late to remind")


async def test_handler_in_isolation() -> None:
    message = build_message(InvoiceDue("inv-1", 0))
    try:
        await remind(message)
    except fanferro.Reject as rejected:
        assert str(rejected) == "nothing to collect"
    else:
        raise AssertionError("expected a rejection")


async def test_full_flow_in_memory() -> None:
    client = memory_client()
    invoices = client.queue("invoices", dead_letter=fanferro.DeadLetterQueue("invoices-dlq"))
    consumer = client.consumer(invoices, remind, wait_time=1)
    async with client:
        await invoices.send(InvoiceDue("inv-1", 100))
        await invoices.send(InvoiceDue("inv-2", 0))
        await consumer.run_until_idle()
        assert invoices.dead_letter_queue is not None
        assert (await invoices.dead_letter_queue.approximate_count())["visible"] == 1
        assert consumer.stats()["processed"] == 1


async def main() -> None:
    await test_handler_in_isolation()
    await test_full_flow_in_memory()
    print("all example tests passed")


if __name__ == "__main__":
    asyncio.run(main())
