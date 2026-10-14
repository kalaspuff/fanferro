"""Observability: event logging configuration, event hooks, OpenTelemetry and on-demand metrics.

Run: uv run python examples/observability.py
"""

from __future__ import annotations

import asyncio
import logging

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import fanferro
from examples._common import make_client, unique


async def main() -> list[str]:
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)-7s %(name)s: %(message)s")
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)

    client = make_client(
        envelope=fanferro.envelopes.CloudEvents(),  # trace context can travel inside the CloudEvent
        logging=fanferro.LoggingConfig(
            levels={"message.processed": logging.INFO, "lease.*": None},  # promote / silence kinds
            disabled=["receive.*"],  # never log polling chatter
        ),
        telemetry=fanferro.Telemetry(
            propagation=fanferro.Propagation(
                inject=(fanferro.Carrier.ENVELOPE, fanferro.Carrier.AWS_TRACE_HEADER),  # no attribute slots used
            )
        ),
    )
    queue = client.queue(unique("observed"), dead_letter=fanferro.DeadLetterQueue(unique("observed-dlq")))
    alerts: list[str] = []

    @client.on("message.dead_lettered")
    async def page_someone(event: fanferro.Event) -> None:  # async hooks run on the event loop
        alerts.append(f"dead letter on {event.fields.get('queue')}: {event.fields.get('reason')}")

    consumer = client.consumer(queue, wait_time=1)

    @consumer.default
    async def handle(message: fanferro.Message[dict[str, str]]) -> None:
        with trace.get_tracer("example").start_as_current_span("business logic"):
            if message.body.get("fail"):
                raise fanferro.Reject("invalid input")

    async with client:
        with trace.get_tracer("example").start_as_current_span("http request"):
            await queue.send({"hello": "world"})
            await queue.send({"fail": "yes"})
        await consumer.run_until_idle(timeout=30)
        print("metrics:", client.metrics()["consumers"][consumer.name])

    spans = sorted(span.name for span in exporter.get_finished_spans())
    print("spans:", spans)
    print("alerts:", alerts)
    provider.shutdown()
    return spans


if __name__ == "__main__":
    asyncio.run(main())
