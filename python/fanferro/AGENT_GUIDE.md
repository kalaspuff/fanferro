# fanferro usage guide (for developers and AI agents)

fanferro is an asyncio library for Amazon SNS + SQS with a Rust core. This guide is the canonical,
compact reference for writing applications with it. Print it with `fanferro guide` (or
`python -m fanferro guide`). Full docs: https://github.com/kalaspuff/fanferro/tree/main/docs

## Rules of thumb

1. **One `fanferro.Client` per process**, created after any `fork()`, used as `async with client:`.
2. **Declare resources once** (`client.topic`, `client.queue`, `client.subscription`) next to the
   client, with an explicit `Provisioning` policy. Use `Provisioning.ensure()` only where the
   application owns the infrastructure (dev, tests, self-contained services); otherwise
   `Provisioning.existing()` (default) or `Provisioning.assume()` with infrastructure as code.
3. **Name message types explicitly and version them**: `@fanferro.message_type("billing.invoice_paid.v1")`.
4. **Handlers must be idempotent.** Delivery is at least once. Use `message.id` (stable across
   producer retries) as the idempotency key; prefer the inbox pattern (dedup row in the same DB
   transaction) or `fanferro.idempotency.IdempotencyMiddleware`.
5. **Publish from DB transactions with the outbox** (`topic.prepare()` + `OutboxRelay`), never
   "commit then publish" for business-critical events.
6. **Always give queues a dead-letter queue** (`dead_letter=fanferro.DeadLetterQueue(...)`) and keep
   its `max_receive_count` above `RetryPolicy.max_attempts`.
7. **Return to ack, raise to retry.** Raise `fanferro.Reject` for permanent errors (dead-letter now),
   `fanferro.Discard` for irrelevant messages, `fanferro.Retry(delay=...)` for "not yet".
8. **Do not block the event loop** in handlers: use async clients or `await asyncio.to_thread(...)`.
9. **Stay within 10 message attributes**; fanferro uses 2 by default (+1-2 for trace context).
   Prefer envelope headers for metadata that needs no SNS filtering.
10. **Test with `fanferro.testing.memory_client()`**, end to end with MiniStack.

## Minimal service

```python
import asyncio

from pydantic import BaseModel

import fanferro


@fanferro.message_type("billing.invoice_paid.v1")
class InvoicePaid(BaseModel):
    invoice_id: str
    amount_cents: int


client = fanferro.Client(source="urn:service:billing")  # region/credentials from the environment
events = client.topic("billing-events")
ledger = client.queue(
    "ledger-updates",
    dead_letter=fanferro.DeadLetterQueue("ledger-updates-dlq", max_receive_count=10),
)
client.subscription(events, ledger, filter_policy={"fanferro.type": ["billing.invoice_paid.v1"]})

consumer = client.consumer(
    ledger,
    concurrency=20,
    retry=fanferro.RetryPolicy(max_attempts=5, backoff=fanferro.Backoff.exponential(initial=2, maximum=300)),
)


@consumer.route(InvoicePaid)
async def record_payment(event: InvoicePaid) -> None: ...  # idempotent side effects


async def main() -> None:
    async with client:  # connects and provisions declared resources
        await client.run()  # runs all consumers; SIGTERM → graceful drain


if __name__ == "__main__":
    asyncio.run(main())
```

Publishing from a web handler (client created at startup, shared):

```python
result = await events.publish(InvoicePaid(invoice_id="inv-1", amount_cents=1299), attributes={"tenant": "acme"})
```

## API map

| Task | API |
| --- | --- |
| Connect | `fanferro.Client(region=..., endpoint_url=..., credentials=fanferro.Credentials(...))` |
| Declare | `client.topic(name, attributes=TopicAttributes(...))`, `client.queue(name, attributes=QueueAttributes(...), dead_letter=DeadLetterQueue(...))`, `client.subscription(topic, queue, filter_policy=...)` |
| Provision now | `async with client:` or `await client.provision()` |
| Publish | `await topic.publish(obj, **options)`, `await queue.send(obj, **options)` |
| Batch | `await topic.publish_batch([...])`, `await queue.send_batch([...])` |
| Prepare (outbox) | `prepared = await topic.prepare(obj)`, `prepared.to_json()`, `await client.publish_prepared(prepared)` |
| Consume | `consumer = client.consumer(queue, ...)`, `@consumer.route(Type)`, `await consumer.run()` |
| Route | `fanferro.Router()`, `router.route(Type, when=matchers..., priority=...)`, `router.default` |
| Settle manually | `await message.ack() / retry(delay=) / requeue() / dead_letter(reason) / release() / discard(reason)` |
| Observe | `client.on("message.*", hook)`, `LoggingConfig(...)`, `Telemetry(...)`, `client.metrics()` |
| Test | `fanferro.testing.memory_client()`, `fanferro.testing.build_message(obj)`, `consumer.run_until_idle()` |

Publish options (keywords or `fanferro.OutgoingMessage`): `type`, `attributes`, `headers`, `id`,
`time`, `subject`, `source`, `data_schema`, `content_type`, `group_id` (FIFO), `deduplication_id`,
`delay` (queues), `sns_subject` (topics), `codec`, `envelope`.

Consumer options: `concurrency`, `prefetch`, `batch_size`, `wait_time`, `pollers`, `rate_limit`,
`visibility_timeout`, `heartbeat`, `retry`, `dead_letter`, `shutdown`, `handler_timeout`,
`ordering`, `envelopes`, `middleware`, `auto_ack`, `sns_unwrap`, `auto_confirm_subscriptions`,
`delete_offloaded_payloads`, `name`.

## Handler signatures

```python
@consumer.route(OrderPlaced)
async def a(order: OrderPlaced) -> None: ...  # body only


@consumer.route(OrderPlaced)
async def b(message: fanferro.Message[OrderPlaced]) -> None:  # message + metadata
    message.body, message.id, message.type, message.attempt, message.attribute("tenant"), message.headers


@consumer.default
async def c(message: fanferro.Message[bytes]) -> None: ...  # anything; never fails to decode
```

Supported body types: pydantic models (and `list[Model]`, discriminated unions), dataclasses,
`msgspec.Struct`, protobuf messages, `dict`/`list` (JSON), `str`, `bytes`; anything else via a
custom `Codec`.

## Routing recipes

```python
from fanferro import matchers as m

router = fanferro.Router()
router.route(OrderV2, when=m.header("schemaversion") >= 2, priority=10)(handle_v2)
router.route(OrderV1)(handle_v1)
router.route(when=(m.attribute("tenant") == "acme") & m.body("order.express").exists())(express)
router.route(when=m.topic() == "returns")(from_returns_topic)
router.default(log_unknown)
consumer = client.consumer(queue, router)
```

Typed routes match by message type name (and aliases). Messages without a type (from boto3 or
other producers) only match `when=` routes and the default route.

## Failure handling recipes

| Situation | Do |
| --- | --- |
| Downstream temporarily down | let the exception propagate (retried with backoff) or `raise fanferro.Retry(delay=30)` |
| Invalid message forever | `raise fanferro.Reject("reason")` → dead-letter queue with metadata |
| Irrelevant message | `raise fanferro.Discard("reason")` |
| Many transient errors | `consumer.pause()`; `consumer.resume()` later |
| Retry long sequences without SQS redrive interfering | `RetryPolicy(strategy="requeue")` |
| Certain exception types are permanent | `RetryPolicy(dead_letter_on=(ValidationError,))` |
| Slow handlers | nothing: heartbeats keep the message invisible (up to 12 h); set `handler_timeout` to bound it |

## Ordering

Use FIFO resources (`"name.fifo"`) and a group id per entity:
`client.topic("ledger.fifo", group_id=lambda m: m.body.account_id)`. Same-group messages are
processed sequentially; publishing the same `id` twice within 5 minutes is deduplicated.

## Large messages

`fanferro.Client(large_payloads=fanferro.LargePayloads(bucket="...", reference="both"))` offloads
bodies above 256 KiB to S3 and sends a pointer (bucket/key and/or presigned URL). Consumers fetch
transparently. Add an S3 lifecycle rule on the prefix.

## Transactional outbox

```python
prepared = await events.prepare(InvoicePaid(...))  # inside the request
async with db.transaction():
    await db.execute("INSERT INTO invoices ...")
    await db.execute("INSERT INTO outbox (id, message) VALUES ($1, $2)", prepared.message_id, prepared.to_json())

relay = fanferro.outbox.OutboxRelay(client, MyPostgresOutbox(pool))  # implements OutboxStore
asyncio.create_task(relay.run())  # separate worker
```

`OutboxStore.claim_batch` must lock rows (`FOR UPDATE SKIP LOCKED`). See `docs/guides/patterns.md`.

## Observability

```python
client = fanferro.Client(
    logging=fanferro.LoggingConfig(levels={"message.processed": logging.INFO}, disabled=["receive.*"]),
    telemetry=fanferro.Telemetry(),  # with opentelemetry-sdk configured: spans + metrics + propagation
)


@client.on("message.dead_lettered")
async def alert(event: fanferro.Event) -> None: ...
```

Event kinds: `fanferro events`. Trace context carriers: message attributes (default), CloudEvents
envelope, AWS X-Ray trace header (`fanferro.Propagation(inject=..., extract=...)`).

## Testing

```python
from fanferro.testing import build_message, memory_client

async def test_handler() -> None:
    await record_payment(InvoicePaid(invoice_id="i", amount_cents=1))      # plain function call
    message = build_message(InvoicePaid(invoice_id="i", amount_cents=0))
    with pytest.raises(fanferro.Reject):
        await validating_handler(message)

async def test_flow() -> None:
    client = memory_client()            # in-memory SQS/SNS/S3; creates resources on demand
    ...declare resources and consumers like production code...
    async with client:
        await topic.publish(event)
        await consumer.run_until_idle()
```

## Common mistakes

* Publishing before subscriptions exist: declare subscriptions on the client and enter it (`async with
  client:`) before publishing, or messages published to a topic with no subscribers are lost.
* Sync handlers: handlers must be `async def`.
* Creating a client per request: create it once and share it.
* `max_receive_count` (SQS redrive) lower than `RetryPolicy.max_attempts`: SQS moves messages before
  fanferro's retries finish (fanferro logs a `config.warning`).
* Using more than 10 attributes, empty attribute values, or names starting with `fanferro.`.
* Combining matchers without parentheses: `(m.topic() == "a") | (m.topic() == "b")`.
* Expecting exactly-once delivery: design for at-least-once.
* Very long `wait_time` in tests: stopping waits for the last long poll; use `wait_time=1`.
