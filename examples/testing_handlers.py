"""Testing: unit-test handlers with ``build_message`` and ``dispatch``, and flows with the in-memory broker.

In your project, put functions like these in pytest test modules (async tests need
``pytest-asyncio`` or ``anyio``). Run: uv run python examples/testing_handlers.py
"""

from __future__ import annotations

import asyncio
import dataclasses

import fanferro
from fanferro.testing import build_message, dispatch, memory_client


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


async def test_late_attempts_are_discarded() -> None:
    # Transport metadata a received message has: attempt, receive_count, group_id, topic, sent_at...
    message = build_message(InvoiceDue("inv-1", 100), attempt=4)
    await remind(message)
    assert message.outcome == "discarded"


async def test_outcomes_as_the_consumer_settles_them() -> None:
    # dispatch() runs the consumer's routing, decoding, middleware and failure handling for one
    # message, without a broker: Reject dead-letters the message when the queue has a DLQ.
    result = await dispatch(remind, InvoiceDue("inv-2", 0), dead_letter_queue=True)
    assert result.outcome == "dead_lettered"
    assert result.calls[0][0] == "dead_letter"
    assert (await dispatch(remind, InvoiceDue("inv-3", 100))).outcome == "acked"


async def test_full_flow_in_memory() -> None:
    client = memory_client()
    invoices = client.queue("invoices", dead_letter=fanferro.DeadLetterQueue("invoices-dlq"))
    consumer = client.consumer(invoices, remind, wait_time=1)
    async with client:
        await invoices.send(InvoiceDue("inv-1", 100))
        await invoices.send(InvoiceDue("inv-2", 0))
        await consumer.run_until_idle()
        assert invoices.dead_letter_queue is not None
        assert (await invoices.dead_letter_queue.approximate_count()).visible == 1
        assert consumer.stats().processed == 1


async def main() -> None:
    await test_handler_in_isolation()
    await test_late_attempts_are_discarded()
    await test_outcomes_as_the_consumer_settles_them()
    await test_full_flow_in_memory()
    print("all example tests passed")


if __name__ == "__main__":
    asyncio.run(main())
