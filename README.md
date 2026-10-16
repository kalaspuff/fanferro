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

## Install

```bash
uv add fanferro                   # pip install fanferro
uv add "fanferro[all]"            # + pydantic, protobuf, msgspec, opentelemetry-api
```

## Highlights

* **Handlers are plain typed functions.** Return to ack, raise to retry. No receipt handles.
* **Your own types.** Pydantic, dataclasses, msgspec, protobuf, JSON or raw bytes.
* **Routing on anything.** Message type, attribute, body field or topic.
* **Safe by default.** Heartbeats, retries with backoff, dead-letter queues, graceful shutdown.
* **Infrastructure in code.** Declared queues, topics and subscriptions are checked or created.
* **Tests without AWS or Docker.** An in-memory broker runs the same engine you deploy.
* **Mistakes caught early.** Bad names, sizes and signatures fail before any network call.
* **Observable.** Typed events, structured logs, hooks and OpenTelemetry.
* **The hard parts included.** Large payloads via S3, outbox, idempotency, FIFO ordering.
* **Fast.** A Rust core, full batches and little Python work per message.
* **No required dependencies.** Fully typed, Python 3.12-3.15, wheels for Linux, macOS, Windows.

## Performance

* **Rust does the heavy lifting.** Signing, HTTP, the SQS and SNS protocols and batching.
* **Full batches, overlapping.** Messages are received ten at a time, the next batch prefetched.
* **Acks don't block handlers.** Deletes are batched in the background while the next message runs.
* **Little Python per message.** Only your codecs, routing and handlers run on the event loop.
* **Lean defaults.** Small messages, no redundant checksums, no tracing work unless enabled.

Benchmarks and how to run them: [Performance](docs/performance.md).

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
| [Architecture](docs/architecture.md) | How it is built and why, with [decision records](docs/adr/README.md) |

**Using an AI coding agent?** Point it at `fanferro guide` (the usage guide shipped with the package)
or copy [`skills/fanferro`](skills/fanferro/SKILL.md) into your project's skills.

## Contributing

See [AGENTS.md](AGENTS.md) (for humans and AI agents): `uv sync`, `make check`, conventional commits.

## License

[MIT](LICENSE)
