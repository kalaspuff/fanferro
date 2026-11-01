---
name: fanferro
description: Build SNS/SQS messaging with the fanferro Python library (Rust core) - publishing, consumers, routing, retries and dead-letter queues, FIFO ordering, large payloads, transactional outbox, idempotent consumers, OpenTelemetry, testing with the in-memory broker or MiniStack. Use when code imports fanferro or the task involves SQS queues or SNS topics in a Python service that uses fanferro.
---

# Using fanferro

Before writing code, read the full usage guide that ships with the installed version:

```bash
uv run fanferro guide        # or: python -m fanferro guide
```

It is authoritative for the installed version (this skill only summarizes it). Also useful:
`fanferro events` (event kinds), `fanferro attributes` (reserved message attributes),
`fanferro dlq stats|peek|redrive --queue-url URL` (inspect a dead-letter queue, move its messages back;
in code: `await dlq.peek()`, `await dlq.redrive()`).

## Non-negotiables

1. One `fanferro.Client` per process, created after `fork()`, used as `async with client:`.
2. Declare topics/queues/subscriptions on the client with an explicit `Provisioning` policy.
3. Explicit, versioned message type names: `@fanferro.message_type("domain.event_name.v1")`.
4. Every queue gets `dead_letter=fanferro.DeadLetterQueue(...)`; `max_receive_count` > `RetryPolicy.max_attempts`.
5. Handlers are `async def`, idempotent (at-least-once delivery; key on `message.id`), and non-blocking.
6. Return to ack; raise to retry; `fanferro.Reject` = dead-letter now; `fanferro.Discard` = drop.
7. Business events from DB writes go through the transactional outbox (`topic.prepare()` + `fanferro.outbox.OutboxRelay`).
8. ≤ 10 message attributes (fanferro uses 1-2, +1-2 for tracing); `fanferro.*` names are reserved.
   Messages are raw JSON bodies by default; opt into CloudEvents with
   `Client(envelope=fanferro.envelopes.CloudEvents())`. tomodachi services: envelopes
   `fanferro.envelopes.compat.TomodachiJSON()`/`TomodachiProtobuf()` (also in `accept_envelopes`,
   before `Raw`); the tomodachi topic is the message type. S3 events and SES bounces, complaints
   and deliveries are recognised by default: annotate handlers with `fanferro.aws.S3Event`,
   `SesBounceNotification`, `SesComplaintNotification`, `SesDeliveryNotification`. EventBridge
   events: the body is the `detail`; type `aws.<service>.<event>`, or the `detail-type` for other
   sources.
9. Tests: `fanferro.testing.memory_client()` + `consumer.run_until_idle()`; handler unit tests with
   `fanferro.testing.build_message(obj, attempt=..., topic=...)`, routers and outcomes with
   `await fanferro.testing.dispatch(router, obj)`; end-to-end against MiniStack (`endpoint_url=http://localhost:4566`).
   Lambda functions: `fanferro.testing.invoke_sqs_handler(handler, queue)`.
10. Environments sharing an AWS account: `fanferro.Client(queue_prefix=f"{env}-", topic_prefix=f"{env}-")`,
    never hand-built `f"{env}-..."` names. Write names without the prefix everywhere (strings passed to
    `subscription`/`consumer`/`DeadLetterQueue` get it too); `prefixed=False` for shared resources, passed
    as handles; `queue.name` and events carry the prefixed name. `bucket_prefix=f"{env}-"` prefixes
    `LargePayloads` buckets the same way.
11. Long-running consumers get liveness/readiness probes from `client.health().healthy()` / `.ready()`
    (`.problems()` says why not; see the deployment guide's health checks), or
    `client.consumer(..., max_receive_failures=N)` so `run()` raises once receives keep failing with an
    error that is not retryable (deleted queue, denied access) and the orchestrator restarts the task.

## Skeleton

```python
client = fanferro.Client(source="urn:service:<name>")
topic = client.topic("<domain>-events")
queue = client.queue("<service>-<purpose>", dead_letter=fanferro.DeadLetterQueue("<service>-<purpose>-dlq"))
client.subscription(topic, queue, filter_policy={"fanferro.type": ["<domain>.<event>.v1"]})
consumer = client.consumer(queue, concurrency=20, retry=fanferro.RetryPolicy(max_attempts=5))

@consumer.route(MyEvent)
async def handle(event: MyEvent) -> None: ...

async def main() -> None:
    async with client:
        await client.run()
```

In AWS Lambda (SQS event source mapping with `FunctionResponseTypes: ["ReportBatchItemFailures"]`),
the same routes run per record, and failed records are returned as `batchItemFailures`:

```python
router = fanferro.Router()

@router.route(MyEvent)
async def handle(event: MyEvent) -> None: ...

handler = fanferro.aws_lambda.sqs_handler(client, router, queue=queue)  # the function's handler
```
