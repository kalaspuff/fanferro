# Testing

## Handlers in isolation

```python
from fanferro.testing import build_message

async def test_rejects_empty_invoices() -> None:
    message = build_message(InvoiceDue("inv-1", amount=0), attributes={"tenant": "acme"})
    with pytest.raises(fanferro.Reject):
        await remind(message)
```

`build_message(body, type=, attributes=, headers=, id=, content_type=, queue=)` encodes the body
with the default codecs and returns a decoded `Message`. Explicit settlements are recorded:
`message.outcome` and `settler.calls` (pass `settler=RecordingSettler()`).

## Flows with the in-memory broker

```python
from fanferro.testing import memory_client

async def test_signup_flow() -> None:
    client = memory_client()                     # resources are created on demand
    events = client.topic("events")
    emails = client.queue("emails", dead_letter=fanferro.DeadLetterQueue("emails-dlq"))
    client.subscription(events, emails)
    sent: list[str] = []

    async def send_email(event: SignedUp) -> None:
        sent.append(event.email)

    consumer = client.consumer(emails, send_email, wait_time=1)

    async with client:
        await events.publish(SignedUp(email="a@example.com"))
        await consumer.run_until_idle()          # process until the queue is empty, then stop
    assert sent == ["a@example.com"]
```

The broker models SQS/SNS/S3 behaviour that matters for processing: visibility timeouts, receive
counts, redrive to DLQs, FIFO groups and deduplication, delays, SNS fan-out with filter policies,
raw and wrapped delivery, presigned URLs. Share one `MemoryBroker` between clients with
`memory_client(broker=broker)`; inspect queues with `broker.queue_messages(url)`.

## Against an emulator

[MiniStack](https://github.com/ministackorg/ministack) runs without Docker:

```bash
uv run ministack &           # http://localhost:4566
```

```python
client = fanferro.Client(endpoint_url="http://localhost:4566", region="us-east-1",
                         credentials=fanferro.Credentials("test", "test"), account_id="000000000000")
```

fanferro's own test suite starts MiniStack automatically (see `tests/conftest.py`) and runs every
integration scenario against both the in-memory broker and MiniStack.

## Tips

* Use unique resource names per test when sharing an emulator.
* Lower `wait_time` (e.g. 1) in tests so consumers stop quickly.
* Assert on events: `client.on("message.dead_lettered", seen.append)`.
* `consumer.stats()` and `client.metrics()` expose counters for assertions.
