"""Smoke test for an installed wheel (no optional dependencies, no network).

python -I scripts/smoke_test.py
"""

from __future__ import annotations

import asyncio
import dataclasses
import platform
import sys

import fanferro
from fanferro.testing import memory_client


@fanferro.message_type("smoke.ping.v1")
@dataclasses.dataclass
class Ping:
    n: int


async def main() -> None:
    client = memory_client()
    topic = client.topic("smoke")
    queue = client.queue("smoke-queue", dead_letter=fanferro.DeadLetterQueue("smoke-dlq"))
    client.subscription(topic, queue)
    received: list[int] = []
    other: list[bytes] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(Ping)
    async def handle(ping: Ping) -> None:
        received.append(ping.n)

    @consumer.default
    async def handle_other(message: fanferro.Message[bytes]) -> None:
        other.append(message.body)

    async with client:
        await topic.publish_batch([Ping(n) for n in range(5)])
        await topic.publish(b"\x00" * 10, type="binary")
        await consumer.run_until_idle(timeout=30)
    assert sorted(received) == [0, 1, 2, 3, 4], received
    assert other == [b"\x00" * 10], other
    payload = fanferro.Payload(b"\x01\x02", content_type="application/x-protobuf", message_type="t", id="1")
    for envelope in (fanferro.envelopes.CloudEvents(), fanferro.envelopes.CloudEventsProtobuf()):
        decoded = envelope.unwrap(envelope.wrap(payload))
        assert decoded is not None, envelope.name
        assert (decoded.data, decoded.message_type, decoded.id) == (b"\x01\x02", "t", "1"), decoded
    python = sys.version.split()[0]
    print(f"fanferro {fanferro.__version__} OK on {platform.system()} {platform.machine()} Python {python}")


if __name__ == "__main__":
    asyncio.run(main())
