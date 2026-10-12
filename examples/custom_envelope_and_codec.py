"""Plugging in your own envelope and codec.

* ``LegacyEnvelope`` reads/writes the JSON format of an older system (``{"event": ..., "payload": ...}``).
* ``CsvCodec`` encodes rows of a dataclass as CSV text.
Run: uv run python examples/custom_envelope_and_codec.py
"""

from __future__ import annotations

import asyncio
import csv
import dataclasses
import io
import json
from typing import Any

import fanferro
from examples._common import make_client, unique
from fanferro.codecs import CodecRegistry, EncodedBody, default_codecs


class LegacyEnvelope:
    """``{"event": "<type>", "id": "...", "payload": <json>}`` with no message attributes."""

    name = "legacy"

    def wrap(self, payload: fanferro.Payload) -> fanferro.WireMessage:
        body = {"event": payload.message_type, "id": payload.id, "payload": json.loads(payload.data)}
        return fanferro.WireMessage(json.dumps(body))

    def unwrap(self, wire: fanferro.WireMessage) -> fanferro.Payload | None:
        try:
            document = json.loads(wire.body)
        except ValueError:
            return None
        if not isinstance(document, dict) or "event" not in document:
            return None  # not ours: the next envelope gets a chance
        return fanferro.Payload(
            json.dumps(document["payload"]),
            message_type=document["event"],
            id=document.get("id"),
            content_type="application/json",
        )


@dataclasses.dataclass
class Row:
    sku: str
    quantity: int


class CsvCodec:
    """Encodes ``list[Row]`` as CSV."""

    name = "csv"

    def can_encode(self, value: object) -> bool:
        return isinstance(value, list) and bool(value) and all(isinstance(item, Row) for item in value)

    def encode(self, value: Any) -> EncodedBody:
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerows((row.sku, row.quantity) for row in value)
        return EncodedBody(buffer.getvalue().encode(), "text/csv")

    def can_decode(self, target: Any, content_type: str | None) -> bool:
        return target == list[Row] and content_type == "text/csv"

    def decode(self, data: bytes, target: Any, content_type: str | None) -> Any:
        return [Row(sku, int(quantity)) for sku, quantity in csv.reader(io.StringIO(data.decode()))]

    def type_name(self, target: Any) -> str | None:
        return None


async def main() -> list[str]:
    client = make_client(
        codecs=CodecRegistry([CsvCodec(), *default_codecs()]),
        accept_envelopes=[LegacyEnvelope(), *fanferro.envelopes.default_envelopes()],
    )
    legacy_queue = client.queue(unique("legacy"), envelope=LegacyEnvelope())
    seen: list[str] = []
    consumer = client.consumer(legacy_queue, wait_time=1)

    @consumer.route("inventory.adjusted")
    async def adjusted(message: fanferro.Message[dict[str, int]]) -> None:
        seen.append(f"{message.envelope}: {message.body}")

    @consumer.route("inventory.snapshot")
    async def snapshot(message: fanferro.Message[list[Row]]) -> None:
        seen.append(f"{message.envelope}: {len(message.body)} rows")

    async with client:
        await legacy_queue.send({"sku-1": -2}, type="inventory.adjusted")
        # A CSV payload inside the default CloudEvents envelope (envelope overridden per message).
        await legacy_queue.send(
            [Row("a", 1), Row("b", 2)], type="inventory.snapshot", envelope=fanferro.envelopes.CloudEvents()
        )
        await consumer.run_until_idle(timeout=30)

    for line in sorted(seen):
        print(line)
    return sorted(seen)


if __name__ == "__main__":
    asyncio.run(main())
