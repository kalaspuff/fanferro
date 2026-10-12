# fanferro

**Fast, robust SNS + SQS messaging for Python, powered by a Rust core.**

*fan-out* (SNS → SQS) · *fanfare* (announcing to a topic) · *ferro* (iron: the Rust core)

```python
import fanferro
from pydantic import BaseModel                  # or dataclasses, msgspec, protobuf, ...


@fanferro.message_type("checkout.order_placed.v1")
class OrderPlaced(BaseModel):
    order_id: str
    amount_cents: int


client = fanferro.Client(
    source="urn:service:checkout",
    provisioning=fanferro.Provisioning.ensure(),  # create missing resources (default: must exist)
)
orders = client.topic("orders")
billing = client.queue("billing", dead_letter=fanferro.DeadLetterQueue("billing-dlq"))
client.subscription(orders, billing)
consumer = client.consumer(billing, concurrency=20)


@consumer.route(OrderPlaced)
async def charge(order: OrderPlaced) -> None:
    ...                                    # return → ack · raise → retry with backoff → DLQ


async def main() -> None:
    async with client:                     # connect + provision declared resources
        await orders.publish(OrderPlaced(order_id="o-1", amount_cents=1299))
        await consumer.run()               # SIGTERM → graceful drain
```

> Status: **0.x** - the API may still change between minor versions.

## Why fanferro

* **A consumer you do not have to write.** Long polling with in-flight limits, prefetch and rate
  limiting; visibility **heartbeats** that run on Rust threads (a busy event loop never loses a
  lease); batched acks; **graceful shutdown** with drain timeouts, progress events and no
  messages stuck behind orphaned long polls.
* **Failure handling built in.** Retry policies with backoff (change visibility or re-enqueue),
  max-attempt limits, dead-letter forwarding with error metadata, `Retry` / `Requeue` / `Reject` /
  `Discard` handler actions, policies for undecodable and unroutable messages.
* **Any message type.** Pydantic, dataclasses, msgspec, protobuf, JSON, text, bytes - or your own
  codec. **CloudEvents** envelopes (JSON or binary protobuf), raw messages for interop, or your own
  envelope.
* **Routing** by message type, version header, attributes, JSON body fields or SNS topic.
* **Large payloads** go through S3 automatically (claim checks with bucket/key or **presigned URLs**,
  SHA-256 verified, SSRF-safe, AWS Extended Client compatible).
* **Declarative infrastructure.** Topics, queues, DLQs, subscriptions and queue policies are looked
  up, verified for drift, created or updated per resource - with `raise` / `log` / `ignore` /
  custom-fallback failure modes. KMS supported, never required.
* **Observable.** Typed events with per-kind logging control and sync/async hooks; OpenTelemetry
  spans, metrics and trace propagation through message attributes, CloudEvents or the AWS X-Ray
  header; on-demand metrics.
* **Patterns included.** Transactional outbox relay, idempotent consumers, FIFO per-entity ordering.
* **Testable.** An in-memory SQS/SNS/S3 broker for fast deterministic tests; end-to-end tests run
  against [MiniStack](https://github.com/ministackorg/ministack) without Docker.
* **Typed, async, dependency-free.** Python 3.12-3.15, fully typed, no required dependencies; wheels
  for Linux (x86_64, aarch64; glibc and musl), macOS and Windows.

## Install

```bash
uv add fanferro                   # pip install fanferro
uv add "fanferro[all]"            # + pydantic, protobuf, msgspec, opentelemetry-api
```

## Learn more

| | |
| --- | --- |
| [Getting started](docs/guides/getting-started.md) | First publisher and consumer, local development |
| [Publishing](docs/guides/publishing.md) · [Consuming](docs/guides/consuming.md) | Options, batches, FIFO, routing, concurrency, shutdown |
| [Failure handling](docs/guides/failure-handling.md) | Retries, dead-letter queues, poison messages |
| [Provisioning](docs/guides/provisioning.md) | Declaring resources, failure modes, IAM permissions |
| [Message formats](docs/guides/message-formats.md) · [Large payloads](docs/guides/large-payloads.md) | Codecs, envelopes, claim checks |
| [Observability](docs/guides/observability.md) | Events, logging, hooks, OpenTelemetry, metrics |
| [Patterns](docs/guides/patterns.md) | Transactional outbox, inbox, fan-out, versioning, ordering |
| [Testing](docs/guides/testing.md) · [Deployment](docs/guides/deployment.md) | In-memory broker, MiniStack, production setup |
| [Examples](examples/README.md) | Runnable examples for every feature |
| [Reference](docs/reference/configuration.md) | Configuration, [events](docs/reference/events.md), [attributes](docs/reference/attributes.md), [errors](docs/reference/errors.md) |

**Using an AI coding agent?** Point it at `fanferro guide` (the usage guide shipped with the package)
or copy [`skills/fanferro`](skills/fanferro/SKILL.md) into your project's skills.

## How it works

The Rust core (`crates/fanferro-core`) speaks to AWS through the AWS SDK for Rust and owns envelopes,
transport encoding, claim checks, provisioning and the consumer engine. The Python package wraps it
with an asyncio API and everything that touches your objects: codecs, routing, handlers, middleware,
hooks and OpenTelemetry. See [docs/architecture.md](docs/architecture.md).

## Contributing

See [AGENTS.md](AGENTS.md) (for humans and AI agents): `uv sync`, `make check`, conventional commits.

## License

[MIT](LICENSE)
