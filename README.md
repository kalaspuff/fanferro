<h1>
  <picture>
    <img
src="https://raw.githubusercontent.com/kalaspuff/fanferro/main/docs/assets/logo/fanferro.svg"
      alt="" width="44" height="44" align="absmiddle">
  </picture>
  fanferro
</h1>

**Fast, robust SNS + SQS messaging for Python, powered by a Rust core.**

*fan-out* (SNS → SQS) · *fanfare* (announcing to a topic) · *ferro* (iron: the Rust core)

```python
import fanferro
from pydantic import BaseModel              # or dataclasses, msgspec, protobuf, ...


@fanferro.message_type("checkout.order_placed.v1")
class OrderPlaced(BaseModel):
    order_id: str
    amount_cents: int


client = fanferro.Client(
    source="urn:service:checkout",
    # create missing resources (default: they must exist)
    provisioning=fanferro.Provisioning.ensure(),
)
orders = client.topic("orders")
billing = client.queue("billing", dead_letter=fanferro.DeadLetterQueue("billing-dlq"))
client.subscription(orders, billing)
consumer = client.consumer(billing, concurrency=20)


@consumer.route(OrderPlaced)
async def charge(order: OrderPlaced) -> None:
    ...                                     # return → ack · raise → retry with backoff → DLQ


async def main() -> None:
    async with client:                      # connect + provision declared resources
        await orders.publish(OrderPlaced(order_id="o-1", amount_cents=1299))
        await consumer.run()                # SIGTERM → graceful drain
```

> Status: **0.x** - the API may still change between minor versions.

## Install

```bash
uv add fanferro                   # pip install fanferro
uv add "fanferro[all]"            # + pydantic, protobuf, msgspec, opentelemetry-api
```

## AWS Lambda

Consume a queue in a Lambda function with an SQS event source mapping: each record is
decoded, routed and retried like a message a consumer received, and failed records come back
as `batchItemFailures`.

```python
# at module level, so it is reused while the function is warm
client = fanferro.Client(source="urn:service:billing")
billing = client.queue("billing", dead_letter=fanferro.DeadLetterQueue("billing-dlq"))
router = fanferro.Router()


@router.route(OrderPlaced)
async def charge(order: OrderPlaced) -> None:
    ...                                     # return → done · raise → retried · Reject → DLQ


# the function Lambda calls: handler(event, context)
handler = fanferro.aws_lambda.sqs_handler(client, router, queue=billing)
```

Turn on `ReportBatchItemFailures` on the event source mapping. Permissions, outcomes and
testing with `fanferro.testing.invoke_sqs_handler`:
[Deployment → Lambda](docs/guides/deployment.md#lambda).

## Events of AWS services

Notifications and events that AWS services send to SNS and SQS are recognised by their body
and decoded into typed classes of `fanferro.aws`: annotate a handler with one and it is
routed there.

```python
import fanferro
from fanferro import aws

client = fanferro.Client()
# In this example, the queue is assumed to be subscribed to the topics S3 and SES send
# to by your infrastructure as code (Terraform, say).
consumer = client.consumer(client.queue("aws-events"))


@consumer.route                             # an S3 event notification
async def on_upload(event: aws.S3Event) -> None:
    for record in event.records:            # none in S3's test event
        if record.created:                  # object keys come URL-decoded
            await index(record.bucket.name, record.object.key, record.object.size)


@consumer.route                             # an SES identity's notification
async def on_bounce(notification: aws.SesBounceNotification) -> None:
    if notification.bounce.permanent:
        await suppress([r.email_address for r in notification.bounce.bounced_recipients])


@consumer.route("aws.ses.open")             # an SES configuration set's event
async def on_open(message: fanferro.Message[aws.SesNotification]) -> None:
    await record_open(message.body.mail.message_id, message.json()["open"]["ipAddress"])


async def main() -> None:
    async with client:
        await consumer.run()
```

Also typed: SES complaints and deliveries, CloudWatch alarms, Lambda destination records and
dead letters, and EventBridge events (decoded into your own class, or typed for scheduled
rules, ECS tasks, Step Functions, EC2, Auto Scaling and AWS Health):
[Notifications and events of AWS services](docs/guides/aws-notifications.md).

## Testing

Test routing and handlers without a broker: `fanferro.testing.dispatch` runs one message
through the consumer's routing, decoding, middleware, handler and retry and dead-letter
policies, and returns how it was settled. `build_message` gives a message the transport
metadata a handler may depend on (`attempt`, `group_id`, the SNS `topic`, `sent_at`...).

```python
from fanferro.testing import build_message, dispatch


async def test_charge() -> None:
    message = build_message(OrderPlaced(order_id="o-1", amount_cents=1299), attempt=3)
    result = await dispatch(consumer, message)
    assert result.outcome == "acked"        # or "retried", "requeued", "dead_lettered", ...
```

Flows run on an in-memory broker (`fanferro.testing.memory_client()`) or an emulator:
[Testing](docs/guides/testing.md).

## Highlights

* **Handlers are plain typed functions.** Return to ack, raise to retry. No receipt handles.
* **Your own types.** Pydantic, dataclasses, msgspec, protobuf, JSON or raw bytes.
* **Routing on anything.** Message type, attribute, body field or topic.
* **Safe by default.** Heartbeats, retries with backoff, dead-letter queues, graceful
  shutdown.
* **Infrastructure in code.** Declared queues, topics and subscriptions are checked or
  created.
* **Long-running workers or AWS Lambda.** The same handlers and policies for SQS event
  sources.
* **AWS's events, typed.** S3, SES, CloudWatch alarms, Lambda and EventBridge events,
  decoded.
* **Talks to tomodachi services.** Their envelopes, read and written in Rust.
* **Tests without AWS or Docker.** An in-memory broker runs the same engine you deploy.
* **Mistakes caught early.** Bad names, sizes and signatures fail before any network call.
* **Observable.** Typed events, structured logs, hooks and OpenTelemetry.
* **The hard parts included.** Large payloads via S3, outbox, idempotency, FIFO ordering.
* **Fast.** A Rust core, full batches and little Python work per message.
* **No required dependencies.** Fully typed, Python 3.12-3.15, wheels for Linux, macOS,
  Windows.

## Performance

* **Rust does the heavy lifting.** Signing, HTTP, the SQS and SNS protocols and batching.
* **Full batches, overlapping.** Messages are received ten at a time, the next batch
  prefetched.
* **Acks don't block handlers.** Deletes are batched in the background while the next
  message runs.
* **Little Python per message.** Only your codecs, routing and handlers run on the event
  loop.
* **Lean defaults.** Small messages, no redundant checksums, no tracing work unless enabled.

Benchmarks and how to run them: [Performance](docs/performance.md).

## Real AWS, emulators and the in-memory broker

fanferro runs on real AWS, on the AWS emulators MiniStack and LocalStack, and on its
in-memory broker. Real AWS is the reference: probes run the same scenarios and raw API calls
on each target, and the in-memory broker follows AWS wherever they found a difference, with
AWS's error codes and texts.

* **Real AWS**, for production: it is the reference.
* **In-memory broker** (`fanferro.testing.memory_client()`), for tests without AWS or Docker:
  no credentials, and other accounts' queues and topics are just missing; filter policy
  errors leave out the parser position SNS appends; long polls that a stopping consumer
  aborts end at once (`MemoryBroker(orphaned_polls=True)` serves them as SQS does).
* **MiniStack**, for end-to-end tests without Docker: accepts many queue and SNS attributes,
  names and FIFO ids that AWS refuses; differs in receipt handles of earlier receives,
  `Number` values, numeric filters on `String` attributes, and X-Ray headers and `Binary`
  values through SNS.
* **LocalStack**, for end-to-end tests with Docker: accepts out-of-range queue attributes and
  a dead-letter queue of the other type; differs in FIFO receipt handles and duplicates,
  `Number` values, `wildcard` filters and presigned URL expiry.

Neither emulator enforces SQS's 60 seconds between deleting a queue and creating it again or
between two purges, drops SNS deliveries to queues encrypted with `alias/aws/sqs` as AWS
does, or refuses unknown credentials. Every finding, the recorded runs and how to probe an
account of your own: [Parity with AWS](docs/parity/README.md).

## tomodachi services

fanferro services exchange messages with [tomodachi](https://github.com/kalaspuff/tomodachi)
services both ways: `fanferro.envelopes.compat` reads and writes tomodachi's envelopes
(`TomodachiJSON`, `TomodachiProtobuf` and `MetadataProtobuf`) in Rust, and the data decodes
into and encodes from your own models:
[tomodachi services](docs/guides/message-formats.md#tomodachi-services).

## Learn more

* [Getting started](docs/guides/getting-started.md): first publisher and consumer, local
  development.
* [Publishing](docs/guides/publishing.md) · [Consuming](docs/guides/consuming.md): options,
  batches, FIFO, routing, concurrency, shutdown.
* [Failure handling](docs/guides/failure-handling.md): retries, dead-letter queues, poison
  messages.
* [Provisioning](docs/guides/provisioning.md): declaring resources, failure modes, IAM
  permissions.
* [Message formats](docs/guides/message-formats.md) ·
  [Large payloads](docs/guides/large-payloads.md): codecs, envelopes, tomodachi services,
  claim checks.
* [AWS notifications](docs/guides/aws-notifications.md): S3, SES, CloudWatch, Lambda and
  EventBridge events as typed messages.
* [Observability](docs/guides/observability.md): events, logging, hooks, OpenTelemetry,
  metrics.
* [Patterns](docs/guides/patterns.md): transactional outbox, inbox, fan-out, versioning,
  ordering.
* [Testing](docs/guides/testing.md) · [Deployment](docs/guides/deployment.md): in-memory
  broker, MiniStack, production setup.
* [Examples](examples/README.md): runnable examples for every feature.
* [Reference](docs/reference/configuration.md): configuration,
  [events](docs/reference/events.md), [attributes](docs/reference/attributes.md),
  [errors](docs/reference/errors.md).
* [Architecture](docs/architecture.md): how it is built and why, with
  [decision records](docs/adr/README.md).

**Using an AI coding agent?** Point it at `fanferro guide` (the usage guide shipped with the
package) or copy [`skills/fanferro`](skills/fanferro/SKILL.md) into your project's skills.

## Contributing

See [AGENTS.md](AGENTS.md) (for humans and AI agents): `uv sync`, `make check`, conventional
commits.

## License

[MIT](LICENSE)
