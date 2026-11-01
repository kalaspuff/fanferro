from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode

import fanferro
from fanferro import testing
from fanferro.telemetry import Carrier, Propagation
from tests.helpers import OrderCreated, consume_until, drain_queue


@pytest.fixture
def otel() -> Iterator[tuple[TracerProvider, InMemorySpanExporter, MeterProvider, InMemoryMetricReader]]:
    exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader])
    yield tracer_provider, exporter, meter_provider, reader
    tracer_provider.shutdown()
    meter_provider.shutdown()


def metric_names(reader: InMemoryMetricReader) -> set[str]:
    data = reader.get_metrics_data()
    names: set[str] = set()
    if data is None:
        return names
    for resource_metrics in data.resource_metrics:
        for scope in resource_metrics.scope_metrics:
            names.update(metric.name for metric in scope.metrics)
    return names


@pytest.mark.parametrize(
    "carrier",
    [Carrier.MESSAGE_ATTRIBUTES, Carrier.ENVELOPE, Carrier.AWS_TRACE_HEADER],
)
async def test_trace_propagation(otel: Any, carrier: Carrier, name: Any) -> None:
    tracer_provider, exporter, meter_provider, reader = otel
    telemetry = fanferro.Telemetry(
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
        propagation=Propagation(inject=(carrier,), extract=(carrier,)),
    )
    # The envelope carrier is the CloudEvents distributed tracing extension.
    envelope = fanferro.envelopes.CloudEvents() if carrier is Carrier.ENVELOPE else None
    client = testing.memory_client(telemetry=telemetry, envelope=envelope)
    queue = client.queue(name("traced"))
    handler_spans: list[Any] = []
    consumer = client.consumer(queue, wait_time=1)
    tracer = tracer_provider.get_tracer("test")

    @consumer.route(OrderCreated)
    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        with tracer.start_as_current_span("handler work") as span:
            handler_spans.append(span)

    async with client:
        with tracer.start_as_current_span("request"):
            await queue.send(OrderCreated(order_id=1))
        await consume_until(consumer, lambda: len(handler_spans) == 1)
    spans = {span.name: span for span in exporter.get_finished_spans()}
    producer = spans[f"send {queue.name}"]
    process = spans[f"process {queue.name}"]
    work = spans["handler work"]
    assert producer.kind is SpanKind.PRODUCER
    assert process.kind is SpanKind.CONSUMER
    assert process.context.trace_id == producer.context.trace_id
    assert process.parent is not None
    assert process.parent.span_id == producer.context.span_id
    assert work.parent is not None
    assert work.parent.span_id == process.context.span_id
    assert producer.attributes["messaging.system"] == "aws_sqs"
    assert process.attributes["messaging.message.id"]
    names = metric_names(reader)
    assert {
        "messaging.client.sent.messages",
        "messaging.process.duration",
        "messaging.client.consumed.messages",
    } <= names
    assert "fanferro.consumer.messages_acked" in names
    await client.close()


async def test_each_publish_carries_its_own_trace_context(otel: Any, name: Any) -> None:
    """A shared attributes dict must not keep the trace context of the first publish."""
    tracer_provider, exporter, meter_provider, _ = otel
    telemetry = fanferro.Telemetry(tracer_provider=tracer_provider, meter_provider=meter_provider)
    client = testing.memory_client(telemetry=telemetry)
    queue = client.queue(name("shared-attributes"))
    tracer = tracer_provider.get_tracer("test")
    common = {"tenant": "acme"}
    async with client:
        for request in ("first", "second"):
            with tracer.start_as_current_span(request):
                await queue.send(OrderCreated(order_id=1), attributes=common)
        received = await drain_queue(client, queue)
    assert common == {"tenant": "acme"}
    requests = {span.name: span for span in exporter.get_finished_spans() if span.name in ("first", "second")}
    trace_ids = {message.attribute("traceparent").split("-")[1] for message in received}
    assert trace_ids == {f"{span.context.trace_id:032x}" for span in requests.values()}


async def test_link_mode_and_errors(otel: Any, name: Any) -> None:
    tracer_provider, exporter, meter_provider, _ = otel
    client = testing.memory_client(
        telemetry=fanferro.Telemetry(
            tracer_provider=tracer_provider, meter_provider=meter_provider, consumer_span_parent="link"
        )
    )
    queue = client.queue(name("linked"))
    consumer = client.consumer(queue, wait_time=1, retry=fanferro.RetryPolicy(max_attempts=1))

    @consumer.route(OrderCreated)
    async def handle(order: OrderCreated) -> None:
        raise RuntimeError("traced failure")

    async with client:
        await queue.send(OrderCreated(order_id=1))
        await consume_until(consumer, lambda: consumer.stats()["failed"] == 1)
    spans = {span.name: span for span in exporter.get_finished_spans()}
    process = spans[f"process {queue.name}"]
    producer = spans[f"send {queue.name}"]
    assert process.context.trace_id != producer.context.trace_id
    assert process.links[0].context.span_id == producer.context.span_id
    assert process.status.status_code is StatusCode.ERROR
    assert any(event.name == "exception" for event in process.events)


async def test_failed_publish_records_the_exception_once(otel: Any, name: Any) -> None:
    tracer_provider, exporter, meter_provider, _ = otel
    telemetry = fanferro.Telemetry(tracer_provider=tracer_provider, meter_provider=meter_provider)
    client = testing.memory_client(telemetry=telemetry)
    queue = client.queue(name("failed-publish"))
    with pytest.raises(fanferro.EncodeError):
        await queue.send(object())  # no codec can encode a plain object
    producers = [span for span in exporter.get_finished_spans() if span.kind is SpanKind.PRODUCER]
    assert len(producers) == 1
    # mark_error() owns error recording on producer spans: the SDK must not add a second event.
    assert [event.name for event in producers[0].events] == ["exception"]
    assert producers[0].status.status_code is StatusCode.ERROR
    await client.close()


async def test_propagation_without_tracing(name: Any) -> None:
    provider = TracerProvider()
    tracer = provider.get_tracer("test")
    client = testing.memory_client(telemetry=fanferro.Telemetry(tracing=False, metrics=False))
    queue = client.queue(name("propagate"))
    async with client:
        with tracer.start_as_current_span("outer") as span, trace.use_span(span):
            await queue.send({"x": 1})
        prepared = await queue.prepare({"x": 1})
        assert "traceparent" not in prepared.wire.attributes, "no active span: nothing to propagate"
    messages = client.broker.queue_messages((await queue.resolve()).url)  # type: ignore[union-attr]
    traceparent = messages[0]["attributes"]["traceparent"].value
    assert traceparent.split("-")[1] == format(span.get_span_context().trace_id, "032x")
    provider.shutdown()


async def test_disabled_telemetry(name: Any) -> None:
    client = testing.memory_client(telemetry=fanferro.Telemetry.disabled())
    queue = client.queue(name("quiet"))
    async with client:
        provider = TracerProvider()
        with provider.get_tracer("t").start_as_current_span("outer"):
            prepared = await queue.prepare({"x": 1})
        provider.shutdown()
    assert "traceparent" not in prepared.wire.attributes
    assert prepared.trace_header is None


async def test_trace_context_passes_through_without_an_sdk(name: Any) -> None:
    # Without an SDK, spans record nothing, but a handler still runs in the producer's trace
    # context, so messages it publishes carry that context on.
    # No test leaves a global tracer provider behind (tests/conftest.py checks it).
    assert isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider)
    client = testing.memory_client()
    queue = client.queue(name("no-sdk"))
    forward = client.queue(name("no-sdk-forward"))
    contexts: list[trace.SpanContext] = []
    consumer = client.consumer(queue, wait_time=1)

    @consumer.route(OrderCreated)
    async def handle(message: fanferro.Message[OrderCreated]) -> None:
        contexts.append(trace.get_current_span().get_span_context())
        await forward.send(message.body)

    traceparent = "00-5759e988bd862e3fe1be46a994272793-53995c3f42cd8ad8-01"
    async with client:
        await queue.send(OrderCreated(order_id=1), attributes={"traceparent": traceparent})
        await consume_until(consumer, lambda: len(contexts) == 1)
        forwarded = await drain_queue(client, forward)
    assert trace.format_trace_id(contexts[0].trace_id) == "5759e988bd862e3fe1be46a994272793"
    assert trace.format_span_id(contexts[0].span_id) == "53995c3f42cd8ad8"
    assert [message.raw_attributes["traceparent"].value for message in forwarded] == [traceparent]
