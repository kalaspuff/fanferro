"""An AWS Lambda function that consumes an SQS queue through an event source mapping.

Lambda receives the messages and invokes ``handler(event, context)`` with them; fanferro decodes
each record as a consumer would (SNS notifications, claim checks, envelopes), routes it, applies
the retry and dead-letter policies, and returns the records that are to come back as
``batchItemFailures``. Deploy it with the function's handler set to ``lambda_function.handler`` and
an event source mapping with ``FunctionResponseTypes: [ReportBatchItemFailures]``.

Run: uv run python -m examples.lambda_function (plays the event source mapping on the configured
backend with ``fanferro.testing.invoke_sqs_handler``)
"""

from __future__ import annotations

import asyncio
import dataclasses

import fanferro
from examples._common import make_client, unique
from fanferro.testing import invoke_sqs_handler


@fanferro.message_type("billing.payment_captured.v1")
@dataclasses.dataclass
class PaymentCaptured:
    payment_id: str
    amount: int


# Created once per execution environment: warm invocations reuse the client and its connections.
client = make_client()
payments = client.queue(unique("payments"), dead_letter=fanferro.DeadLetterQueue(unique("payments-dlq")))
router = fanferro.Router()
booked: list[str] = []


@router.route(PaymentCaptured)
async def book(payment: PaymentCaptured) -> None:
    if payment.amount <= 0:
        raise fanferro.Reject("invalid amount")  # forwarded to the dead-letter queue, not reported
    booked.append(payment.payment_id)


handler = fanferro.aws_lambda.sqs_handler(
    client,
    router,
    queue=payments,
    retry=fanferro.RetryPolicy(max_attempts=3, backoff=fanferro.Backoff.exponential(initial=5, maximum=60)),
)


async def main() -> list[str]:
    await payments.resolve()
    for payment_id, amount in (("p-1", 100), ("p-2", 0), ("p-3", 250)):
        await payments.send(PaymentCaptured(payment_id, amount))
    # What the event source mapping does: receive a batch, invoke the function, delete the records
    # it did not report as failed.
    responses = []
    while sorted(booked) != ["p-1", "p-3"]:
        responses.append(await invoke_sqs_handler(handler, payments, wait_time=1))
    print(f"booked {sorted(booked)}; responses: {responses}")
    assert all(response == {"batchItemFailures": []} for response in responses)
    assert payments.dead_letter_queue is not None
    assert (await payments.dead_letter_queue.approximate_count())["visible"] == 1
    await client.close()
    return sorted(booked)


if __name__ == "__main__":
    asyncio.run(main())
