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

The message looks received, with the transport metadata a handler may depend on: `attempt=`,
`receive_count=`, `group_id=`, `deduplication_id=`, `sequence_number=`, `topic=` (a name or an
ARN, so that `m.topic() == "orders"` matches) and `sent_at=`. `attempt=4` alone means the fourth
receive (`receive_count == 4`, as visibility retries give); with `receive_count=` the earlier
attempts were requeued copies.

```python
async def test_gives_up_on_late_reminders() -> None:
    message = build_message(InvoiceDue("inv-1", amount=100), attempt=4)
    await remind(message)
    assert message.outcome == "discarded"
```

## Routing and outcomes without a broker

`dispatch(target, message_or_body)` runs what a consumer does with one received message, with the
consumer's own code: `RetryPolicy.max_attempts`, route selection, decoding into the route's type,
middleware, the handler (with `handler_timeout`), and the settlement the retry and dead-letter
policies choose for its result (`Retry`, `Reject`, `Requeue`, `Discard`, other errors, unroutable
messages, decode errors). Nothing is sent: it returns the outcome and the delivery calls the
consumer made.

```python
from fanferro.testing import build_message, dispatch

async def test_router() -> None:
    result = await dispatch(router, build_message(OrderCreated(id=1), topic="orders"))
    assert result.outcome == "acked"
    assert result.message.body == OrderCreated(id=1)

    rejected = await dispatch(router, OrderCreated(id=-1), dead_letter_queue=True)
    assert rejected.outcome == "dead_lettered"
    assert rejected.calls[0][0] == "dead_letter"   # ("dead_letter", {"reason": ..., "details": {...}})
```

* `target` is a `Router`, a handler, or a `Consumer` (not started; its client, queue, policies and
  middleware apply). For a router or handler, consumer options go in as keywords
  (`retry=RetryPolicy(max_attempts=3)`, `middleware=[...]`, `auto_ack=False`...) and the client is
  `memory_client()` unless `client=` is given.
* `calls` lists the delivery calls: `("ack", {})` (an ack or a discard), `("nack", {"delay": s})`
  (a retry), `("requeue", {"attempt": n, "delay": s})`, `("dead_letter", {"reason": ..., "details":
  {...}})`, `("release", {})`, `("extend", {"timeout": s})` and `("leave", {})` (left to time
  out, outcome `None`).
* Dead-lettering forwards a message only when the queue has a dead-letter queue; otherwise
  `DeadLetterPolicy.on_missing_queue` applies (by default the message is retried after the
  backoff's maximum), as on a broker. `dispatch` assumes a dead-letter queue when the queue or the
  dead-letter policy declares one; `dead_letter_queue=True` or `False` says so explicitly.

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

The broker models SQS/SNS/S3 behaviour that matters for processing, as real AWS behaves (see
[parity with AWS](../parity/README.md)): visibility timeouts, receive counts, receipt handles of
earlier receives, redrive to DLQs (the receive count carries over, and a `RedrivePolicy` SQS would
refuse fails when it is set), FIFO groups and deduplication, delays, queue attributes SQS would
refuse, the 60 seconds before a deleted queue's name can be used again and between two purges,
SNS fan-out with filter policies (none to queues encrypted with `alias/aws/sqs`, which SNS cannot
use), raw and wrapped delivery, X-Ray headers of active topics, presigned URLs. Filter policies support what SNS supports (exact values,
`prefix`, `suffix`, `equals-ignore-case`, `wildcard`, `cidr`, `anything-but`, `numeric`, `exists`,
`$or`, nested keys of `MessageBody` policies), with its limits, and a policy SNS would refuse fails
when it is set. Share one `MemoryBroker` between clients with `memory_client(broker=broker)`;
inspect queues with `broker.queue_messages(url)`.

A long poll that a stopping consumer aborts ends with the connection on the broker. SQS goes on
serving it until its wait time ends, and a message sent meanwhile goes to it and stays invisible for
its visibility timeout. `MemoryBroker(orphaned_polls=True)` models that, to test shutdowns and
deployments as they behave on SQS; it is off by default, so that a test that stops a consumer and
goes on at once does not wait for visibility timeouts. fanferro's consumers keep the messages they
release while stopping out of such polls, on SQS and on such a broker.

## Lambda functions

A function from `fanferro.aws_lambda.sqs_handler` (see [deployment](deployment.md#lambda)) is
tested against the queue it consumes: `invoke_sqs_handler` does what the event source mapping does
once (receives up to ten messages, invokes the handler with them as an SQS event, deletes the
records the response does not list), and `receive_sqs_event` only builds the event, with real
receipt handles, to inspect or invoke yourself.

```python
from fanferro.testing import invoke_sqs_handler, memory_client

async def test_function() -> None:
    client = memory_client()
    payments = client.queue("payments", dead_letter=fanferro.DeadLetterQueue("payments-dlq"))
    handler = fanferro.aws_lambda.sqs_handler(client, book, queue=payments)
    await payments.send(PaymentCaptured("p-1", 100))
    assert await invoke_sqs_handler(handler, payments) == {"batchItemFailures": []}
```

Hand-written events work too (`await handler.handle({"Records": [...]})`): settling them needs no
receipt handle, except for retries and releases, whose visibility change then fails (the record
is still reported as failed).

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
* `consumer.stats()`, `client.metrics()` and `queue.approximate_count()` expose counters for
  assertions (`consumer.stats().engine.messages_acked == 1`, `(await queue.approximate_count()).visible == 0`).
