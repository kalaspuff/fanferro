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

## Highlights

* **Handlers are typed functions.** The first parameter's annotation decides what a handler
  receives: `order: OrderPlaced` the decoded body, `message: fanferro.Message[OrderPlaced]` the
  message with its id, attributes and attempt number, `Message[bytes]` the raw bytes. Return to ack,
  raise to retry; receipt handles and visibility timeouts stay out of your code.
* **Bring your own types, route on anything.** Pydantic models, dataclasses, msgspec structs,
  protobuf messages, JSON, text and bytes work without glue code (an integration loads only if your
  application imports its library). Routes match by type, version header, attribute, JSON body
  field or SNS topic: `when=(m.attribute("tenant") == "acme") & m.body("order.express").exists()`.
* **Production behaviour by default.** Heartbeats keep messages invisible while handlers run;
  failures are retried with jittered exponential backoff and, after the fifth attempt, forwarded to
  the dead-letter queue with the error attached; SIGTERM stops receiving and drains running
  handlers. Raise `Retry`, `Requeue`, `Reject` or `Discard` to decide otherwise.
* **Infrastructure declared in code.** Entering the client looks up the declared topics, queues,
  dead-letter queues and subscriptions and checks them for drift; `Provisioning.ensure()` also
  creates and updates them, redrive and queue policies included, and a missing resource can raise,
  log or fall back. `existing()` and `assume()` suit least-privilege deployments, and
  `queue_prefix`/`topic_prefix` keep environments apart in one account.
* **Tests without AWS or Docker.** `fanferro.testing.memory_client()` runs the real engine against
  an in-memory SQS/SNS/S3 broker (visibility timeouts, redrive, FIFO groups, filter policies),
  `consumer.run_until_idle()` stops once the queue is empty, and `build_message()` unit-tests a
  handler. End-to-end tests run against [MiniStack](https://github.com/ministackorg/ministack),
  installed with pip.
* **Mistakes surface before the network.** Names, attribute limits, reserved attributes, FIFO group
  ids, message sizes and handler signatures are checked locally, and errors say what to change:
  `queue name '...' (prefix 'staging-' + '...') is 83 characters; SQS allows 80 - shorten the prefix
  or the name`. Every error says whether retrying can help, and AWS errors keep their code and
  request id.
* **Observable from the first message.** Every notable action is a typed event, logged with
  structured fields at levels you choose per kind and passed to sync or async hooks
  (`client.on("message.dead_lettered", alert)`). With an OpenTelemetry SDK, spans, metrics and trace
  propagation (message attributes, CloudEvents or the X-Ray header) need no extra code;
  `consumer.stats()` and `client.metrics()` work without one.
* **Fast without tuning.** Full receive batches, pipelined acks and lean messages are the defaults:
  a consumed message costs about 20 µs of CPU against the in-memory broker and 55 µs over HTTP
  ([Performance](#performance)).
* **The hard parts included.** Large payloads move through S3 (claim checks with bucket/key or
  presigned URLs, SHA-256 verified, SSRF-safe, AWS Extended Client compatible); a transactional
  outbox relay, idempotent consumers, FIFO per-entity ordering and CloudEvents (JSON or protobuf)
  are built in.
* **Typed, dependency-free, prebuilt.** Fully typed (`Message[T]`, `py.typed`), no required
  dependencies, Python 3.12-3.15, wheels for Linux (x86_64, aarch64; glibc and musl), macOS and
  Windows.

## Install

```bash
uv add fanferro                   # pip install fanferro
uv add "fanferro[all]"            # + pydantic, protobuf, msgspec, opentelemetry-api
```

## Design

How fanferro is built and why: [architecture](docs/architecture.md),
[decision records](docs/adr/README.md). The notable parts:

* **Rust for protocol, timing and I/O; Python for your objects.** The core (no Python dependency)
  talks to AWS through the AWS SDK for Rust and owns envelopes, transport encoding, claim checks,
  provisioning and the consumer engine. Python owns codecs, routing, handlers, middleware, hooks
  and OpenTelemetry; the binding in between only converts
  ([ADR 0001](docs/adr/0001-rust-core-python-api.md)).
* **Leases do not depend on the event loop.** Heartbeats run on the core's threads, ten messages
  per call, from receipt until settlement: a handler that blocks the event loop keeps its message,
  and an extension call that hangs is retried at the next tick instead of stalling the others.
* **Python runs on the event loop only.** Handlers, codecs and hooks never run on the core's
  threads. Results of Rust futures reach asyncio in batches, one wake-up per batch, without worker
  threads waiting for the GIL ([ADR 0009](docs/adr/0009-results-reach-asyncio-in-batches.md));
  events are pulled from a bounded buffer, so hooks may be async
  ([ADR 0004](docs/adr/0004-events-pulled-from-the-core.md)).
* **At least once, never silently dropped, always bounded.** Every received message is acked,
  retried, requeued, dead-lettered, released or discarded with an event; a delivery dropped
  without a settlement releases its message. A consumer holds at most `concurrency + prefetch`
  unsettled messages, and settlements, events and payload downloads are bounded too.
* **Codec, envelope and transport are separate layers.** Codecs turn objects into bytes, envelopes
  lay out bytes and metadata on the wire, and the transport base64-encodes binary bodies and moves
  large ones to S3. Codecs and envelopes are pluggable; handlers never see base64, claim checks or
  SNS notification JSON ([ADR 0003](docs/adr/0003-claim-checks-in-the-transport-layer.md)).
* **Plain messages on the wire.** By default the body is the encoded payload, and the id and type
  travel as message attributes: any SQS or SNS client reads fanferro's messages, and fanferro
  consumes theirs. CloudEvents are opt-in ([ADR 0008](docs/adr/0008-lean-defaults.md)).
* **Stable message ids.** A message's id is generated once and kept on every retry: FIFO
  deduplication ids, idempotency keys and claim-check object keys derive from it, so retries
  deduplicate instead of multiplying.
* **One engine, two backends.** AWS access goes through ports implemented by the AWS SDK adapter
  and by an in-memory broker on Tokio's clock: the engine's timing is tested on paused,
  deterministic time, and your tests exercise the engine you deploy
  ([ADR 0005](docs/adr/0005-in-memory-backend.md)).
* **Well-behaved in real processes.** Importing fanferro does no I/O and clients connect on first
  use. One Tokio runtime per process; clients created before `fork()` refuse to work in the child,
  and an `atexit` hook stops the runtime before the interpreter finalizes.

## Performance

Per message, everything that needs no Python objects runs in Rust: request signing, HTTP, the SQS
and SNS protocols, built-in envelopes, base64, leases and batching. The event loop is left with
codecs, routing and your handlers.

| Scenario | CPU per message, in-memory broker | CPU per message, MiniStack over HTTP | Messages per second, in-memory broker |
| --- | ---: | ---: | ---: |
| `publish`: `await queue.send(...)`, one after another | 31 µs | 140 µs | 30,000 |
| `publish-concurrent`: 64 sends at a time | 28 µs | 140 µs | 51,000 |
| `publish-batch-10`: `send_batch()` of 10 messages | 13 µs | 30 µs | 79,000 |
| `publish-batch-100`: `send_batch()` of 100 messages | 10 µs | 21 µs | 97,000 |
| `consume`: handler taking `fanferro.Message`, concurrency 100 | 18 µs | 55 µs | 86,000 |
| `consume-typed`: body decoded into a dataclass | 21 µs | 61 µs | 68,000 |

[`scripts/benchmark.py`](scripts/benchmark.py), medians of ten runs: fanferro 0.4.0, Python 3.14,
Linux aarch64 (10 cores, a VM on Apple silicon), a small JSON body, an empty handler. CPU is that of
the whole process: the event loop and the Rust threads. The in-memory broker answers at once, so its
columns are fanferro's own cost; over HTTP the CPU adds signing and the SQS protocol, while the
elapsed time is MiniStack's (an emulator written in Python). Against SQS, round trips of a few
milliseconds dominate, and batches, prefetch and concurrency overlap them.

What keeps the cost down:

* **Full batches, overlapping.** Pollers receive full batches of ten and prefetch the next one
  while handlers run. Acks and visibility changes go out ten per call, also while earlier calls are
  in flight, and heartbeats extend ten leases per call. `send_batch()` and `publish_batch()` keep
  up to eight calls in flight (standard queues and topics).
* **Acks do not hold handler slots.** A handler's slot goes to the next message as soon as it
  returns, while the delete is batched with others.
* **Little Python per message.** Built-in envelopes are unwrapped in Rust while a batch is decoded,
  dataclass converters are compiled once per type, and a trivial awaitable costs about 14 µs of CPU
  instead of 75 µs with `pyo3-async-runtimes`
  ([ADR 0009](docs/adr/0009-results-reach-asyncio-in-batches.md)).
* **Lean defaults.** Raw bodies with few attributes, no MD5 digests on top of TLS, events built only
  for kinds that something logs or hooks, and no span work without an OpenTelemetry SDK
  ([ADR 0008](docs/adr/0008-lean-defaults.md)).

Run it yourself: `uv run python scripts/benchmark.py --repeat 5` (in-memory broker) or
`uv run python scripts/benchmark.py --endpoint http://localhost:4566 -n 2000 --repeat 3`
(MiniStack, started with `uv run ministack`).

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

## Contributing

See [AGENTS.md](AGENTS.md) (for humans and AI agents): `uv sync`, `make check`, conventional commits.

## License

[MIT](LICENSE)
