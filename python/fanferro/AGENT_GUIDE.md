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
9. **Stay within 10 message attributes**; fanferro uses 1-2 by default (`fanferro.id`, plus
   `fanferro.type` for typed messages and `content-type` for non-JSON payloads; +1-2 for trace
   context). Messages are raw by default (the body is the JSON document); use
   `Client(envelope=fanferro.envelopes.CloudEvents())` when consumers need CloudEvents metadata
   (`source`, `subject`, headers inside the body).
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

Consumer options: `concurrency`, `prefetch`, `batch_size`, `min_receive_batch`,
`receive_batch_wait`, `wait_time`, `pollers`, `rate_limit`, `visibility_timeout`, `heartbeat`,
`retry`, `dead_letter`, `shutdown`, `handler_timeout`, `ordering`, `strict_ordering`, `envelopes`,
`middleware`, `auto_ack`, `settle_linger`, `sns_unwrap`, `auto_confirm_subscriptions`,
`delete_offloaded_payloads`, `name`. Options and policy fields in seconds (and `delay=` in
publishes, `Retry`, `Requeue` and settlements) also take a `timedelta`.

## Credentials and region

Leave `credentials=` unset in production: fanferro then uses the AWS SDK default provider chain,
which reads the same environment variables and files as boto3 and the AWS CLI. First match wins:

1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN` environment variables
2. `~/.aws/config` and `~/.aws/credentials`: profile from `profile=`, else `AWS_PROFILE`, else
   `default` (SSO, `credential_process` and role profiles work)
3. Web identity: `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN` (EKS IRSA)
4. Container credentials: ECS task role, EKS Pod Identity
5. EC2 instance metadata (instance profile)

Temporary credentials are cached and refreshed automatically. Environment credentials win over
`profile=`. Region: `region=`, else `AWS_REGION` / `AWS_DEFAULT_REGION`, else the profile, else
instance metadata; a missing region fails when the client connects (`async with client:` or the
first operation), not in `Client(...)`. `account_id` is looked up with STS when unset. Use
`credentials=fanferro.Credentials("test", "test")` only for emulators and tests.

## Several environments in one AWS account

```python
env = os.environ["APP_ENV"]  # e.g. "staging"
client = fanferro.Client(queue_prefix=f"{env}-", topic_prefix=f"{env}-")
orders = client.topic("orders")  # AWS name: staging-orders
worker = client.queue("orders-worker", dead_letter=fanferro.DeadLetterQueue("orders-worker-dlq"))
client.subscription("orders", "orders-worker")  # names given as strings get the prefix too
audit = client.topic("shared-audit", prefixed=False)  # shared by all environments
```

Write every name without the prefix: fanferro adds it to every queue and topic name, dead-letter
queues and names passed as strings included, and never checks whether a name already has it.
Queues given by `url=` and topics given by `arn=` are never prefixed. Pass `prefixed=False`
resources as handles, and give a shared queue's `DeadLetterQueue` `prefixed=False` too (otherwise
every environment rewires the queue's redrive policy). `queue.name`, `client.queues`, events,
metrics and outbox records carry the prefixed name: compare with `topic.name` in matchers
(`m.topic() == orders.name`) and give an outbox relay the same prefixes. Names are checked when
declared: letters, digits, `-`, `_`, a `.fifo` suffix, at most 80 characters for queues and 256
for topics, prefix included. `bucket_prefix=f"{env}-"` does the same for the buckets of
`LargePayloads` (`prefixed=False` for a shared bucket; lowercase, at most 63 characters).

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
processed sequentially, each after the previous one's delete is confirmed (`strict_ordering=False`
acks in the background: faster, but a failed delete reorders the group); publishing the same `id`
twice within 5 minutes is deduplicated. Group ids, deduplication ids and the message `id` (the
deduplication id by default) must be 1-128 ASCII letters, digits and punctuation (no spaces);
others raise `ValidationError` before sending.

## Large messages

`fanferro.Client(large_payloads=fanferro.LargePayloads(bucket="...", reference="both"))` offloads
bodies above 256 KiB to S3 and sends a pointer (bucket/key and/or presigned URL). Consumers fetch
transparently. Add an S3 lifecycle rule on the prefix (expiring well after the queue's retention
period). `delete_offloaded_payloads=True` (point-to-point queues only) deletes the object on ack and
acks a copy whose object is gone as a duplicate (`message.duplicate`): it would also drop a message
whose object expired or was deleted for another reason.

## Transactional outbox

```python
prepared = await events.prepare(InvoicePaid(...))  # inside the request
async with db.transaction():
    await db.execute("INSERT INTO invoices ...")
    await db.execute("INSERT INTO outbox (id, message) VALUES ($1, $2)", prepared.message_id, prepared.to_json())

relay = fanferro.outbox.OutboxRelay(client, MyPostgresOutbox(pool))  # implements OutboxStore
asyncio.create_task(relay.run())  # separate worker
```

`OutboxStore.claim_batch` must lock rows (`FOR UPDATE SKIP LOCKED`) and skip rows given up with
`mark_abandoned` (records whose attempts failed for good `OutboxRelay(max_attempts=10)` times). See
`docs/guides/patterns.md`.

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
  fanferro's retries finish (fanferro logs a `config.warning`). On FIFO queues keep it well above:
  SQS also counts the receives of messages handed back while an earlier message of their group was
  retried.
* Using more than 10 attributes, empty attribute values, or names starting with `fanferro.`.
* Combining matchers without parentheses: `(m.topic() == "a") | (m.topic() == "b")`.
* Building environment names by hand (`f"{env}-orders"`) instead of `Client(queue_prefix=...,
  topic_prefix=...)`: one forgotten prefix silently declares (and may create) an unprefixed
  resource. With the client prefixes set, a hand-built name gets the prefix twice.
* Expecting exactly-once delivery: design for at-least-once.
* Long `wait_time` in tests: stopping aborts the long poll in flight, but SQS keeps serving it for
  up to `wait_time` and may hide a message sent meanwhile for a visibility timeout; use
  `wait_time=1` (or `ShutdownPolicy(abort_receives=False)`) where a test reuses the queue.
