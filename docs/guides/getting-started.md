# Getting started

## Install

```bash
uv add fanferro                     # or: pip install fanferro
uv add "fanferro[pydantic,otel]"    # optional integrations: pydantic, protobuf, msgspec, otel, all
```

fanferro supports Python 3.12-3.15. Wheels (one abi3 wheel per platform) are published for Linux
x86_64 and aarch64 (manylinux2014 / glibc 2.17+, and musllinux 1.2 / Alpine), macOS 11+ (Apple
silicon and Intel) and Windows x64 (and Windows on Arm, best effort); other platforms build from the
source distribution with a Rust toolchain. It has no required dependencies; the Rust core ships
inside the wheel.

## Hello, fan-out

```python
import asyncio

from pydantic import BaseModel

import fanferro


@fanferro.message_type("shop.order_placed.v1")   # the logical name on the wire
class OrderPlaced(BaseModel):
    order_id: str
    amount_cents: int


client = fanferro.Client(region="eu-west-1", source="urn:service:checkout")
orders = client.topic("orders", provisioning=fanferro.Provisioning.ensure())
billing = client.queue(
    "billing",
    dead_letter=fanferro.DeadLetterQueue("billing-dlq", max_receive_count=5),
    provisioning=fanferro.Provisioning.ensure(),
)
client.subscription(orders, billing)            # SNS → SQS, raw delivery, queue policy managed

consumer = client.consumer(billing, concurrency=20)


@consumer.route(OrderPlaced)
async def charge(order: OrderPlaced) -> None:
    ...                                         # return = ack; raise = retry per policy


async def main() -> None:
    async with client:                          # connect + provision declared resources
        await orders.publish(OrderPlaced(order_id="o-1", amount_cents=1299))
        await consumer.run()                    # until SIGINT/SIGTERM, then graceful shutdown


asyncio.run(main())
```

What happened:

* `client.topic/queue/subscription` declare resources; entering the client provisions them
  according to their [provisioning policy](provisioning.md) (here: create if missing, fix drift).
* `publish` encoded the model as JSON (the message body), added the `fanferro.id` and
  `fanferro.type` attributes (the type is usable in SNS filter policies), and published to SNS.
  `Client(envelope=fanferro.envelopes.CloudEvents())` wraps messages in CloudEvents instead.
* The consumer long-polls SQS from Rust, keeps messages invisible with heartbeats while the handler
  runs, decodes the body into `OrderPlaced` and acks it when the handler returns.

## Local development without AWS

Use the in-memory broker (`fanferro.testing.memory_client()`), or MiniStack, a free AWS emulator:

```bash
uv add --dev ministack
uv run ministack                                # listens on http://localhost:4566
```

```python
client = fanferro.Client(
    endpoint_url="http://localhost:4566",
    region="us-east-1",
    credentials=fanferro.Credentials("test", "test"),
)
```

## Next steps

* Route different message types and versions: [Consuming](consuming.md#routing)
* Decide what happens on failure: [Failure handling](failure-handling.md)
* Publish reliably from a database transaction: [Patterns → outbox](patterns.md#transactional-outbox)
* Run the [examples](../../examples/README.md)
