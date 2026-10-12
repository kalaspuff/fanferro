"""Protobuf messages in a binary CloudEvents envelope.

Any generated protobuf class works the same way (here: well-known types, so no codegen is needed).
The message type defaults to the protobuf full name, e.g. ``google.protobuf.Struct``.
Run: uv run python examples/protobuf_binary.py
"""

from __future__ import annotations

import asyncio

from google.protobuf import struct_pb2, timestamp_pb2

import fanferro
from examples._common import make_client, unique


async def main() -> list[str]:
    # Every message published by this client uses the compact protobuf CloudEvents format.
    client = make_client(envelope=fanferro.envelopes.CloudEventsProtobuf())
    queue = client.queue(unique("telemetry"))
    seen: list[str] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(timestamp_pb2.Timestamp)
    async def on_timestamp(message: fanferro.Message[timestamp_pb2.Timestamp]) -> None:
        seen.append(f"{message.type}: {message.body.seconds}s via {message.envelope}")

    @consumer.route(struct_pb2.Struct)
    async def on_struct(document: struct_pb2.Struct) -> None:
        seen.append(f"struct: {dict(document)}")

    async with client:
        await queue.send(timestamp_pb2.Timestamp(seconds=1_700_000_000))
        document = struct_pb2.Struct()
        document.update({"sensor": "t-1", "celsius": 21.5})
        await queue.send(document)
        await consumer.run_until_idle(timeout=30)

    for line in sorted(seen):
        print(line)
    return sorted(seen)


if __name__ == "__main__":
    asyncio.run(main())
