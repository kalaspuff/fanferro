"""Telemetry without an OpenTelemetry SDK costs little per message, and notices an SDK configured later (#201)."""

from __future__ import annotations

from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.metrics import _internal as metrics_internal
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

import fanferro
from fanferro.envelopes import Raw
from fanferro.messages import OutgoingMessage
from fanferro.telemetry import _NO_SPAN, TelemetryRuntime


class Recorder:
    """Stands in for a metric instrument."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, float]] = []

    def add(self, value: float, attributes: Any = None) -> None:
        self.calls.append(("add", value))

    def record(self, value: float, attributes: Any = None) -> None:
        self.calls.append(("record", value))


def recording(runtime: TelemetryRuntime) -> Recorder:
    recorder = Recorder()
    runtime._instruments = dict.fromkeys(runtime._instruments, recorder)
    return recorder


@pytest.fixture
def no_sdk() -> None:
    # No test leaves a global provider behind (tests/conftest.py checks it).
    assert trace._TRACER_PROVIDER is None
    assert metrics_internal._METER_PROVIDER is None


@pytest.mark.usefixtures("no_sdk")
def test_without_an_sdk_spans_and_metrics_are_skipped() -> None:
    runtime = TelemetryRuntime(fanferro.Telemetry(), "0")
    recorder = recording(runtime)
    assert runtime.publish_span("orders", "queue", "id") is _NO_SPAN
    runtime.record_publish("orders", "queue", 0.1, 1, None)
    runtime.record_consumed("orders", 1)
    runtime.record_process("orders", 0.1, None)
    assert recorder.calls == []


@pytest.mark.usefixtures("no_sdk")
def test_an_sdk_configured_after_the_client_is_used(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = TelemetryRuntime(fanferro.Telemetry(), "0")
    recorder = recording(runtime)
    tracer_provider, meter_provider = TracerProvider(), MeterProvider()
    # What trace.set_tracer_provider() and metrics.set_meter_provider() set (undone by monkeypatch).
    monkeypatch.setattr(trace, "_TRACER_PROVIDER", tracer_provider)
    monkeypatch.setattr(metrics_internal, "_METER_PROVIDER", meter_provider)
    assert not runtime._noop_tracing()
    assert runtime.publish_span("orders", "queue", "id") is not _NO_SPAN
    runtime.record_publish("orders", "queue", 0.1, 1, None)
    runtime.record_consumed("orders", 1)
    runtime.record_process("orders", 0.1, None)
    assert recorder.calls == [("add", 1), ("record", 0.1), ("add", 1), ("record", 0.1)]
    tracer_provider.shutdown()
    meter_provider.shutdown()


@pytest.mark.usefixtures("no_sdk")
def test_explicit_providers_are_always_used() -> None:
    tracer_provider, meter_provider = TracerProvider(), MeterProvider()
    runtime = TelemetryRuntime(fanferro.Telemetry(tracer_provider=tracer_provider, meter_provider=meter_provider), "0")
    recorder = recording(runtime)
    assert not runtime._noop_tracing()
    runtime.record_publish("orders", "queue", 0.1, 1, None)
    assert recorder.calls == [("add", 1), ("record", 0.1)]
    tracer_provider.shutdown()
    meter_provider.shutdown()


@pytest.mark.usefixtures("no_sdk")
def test_a_provider_named_by_the_environment_is_looked_up(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_PYTHON_TRACER_PROVIDER", "configured")
    looked_up: list[bool] = []

    def get_tracer_provider() -> Any:
        looked_up.append(True)
        return trace.ProxyTracerProvider()

    monkeypatch.setattr(trace, "get_tracer_provider", get_tracer_provider)
    runtime = TelemetryRuntime(fanferro.Telemetry(), "0")
    looked_up.clear()
    assert runtime._noop_tracing()
    assert looked_up == [True], "the public function loads the provider the environment names"


@pytest.mark.usefixtures("no_sdk")
def test_inject_needs_an_active_span_but_no_sdk() -> None:
    runtime = TelemetryRuntime(fanferro.Telemetry(), "0")
    message = OutgoingMessage(body={})
    assert runtime.inject(message, Raw()) is None
    assert message.trace_header is None
    context = SpanContext(
        trace_id=0x5759E988BD862E3FE1BE46A994272793,
        span_id=0x53995C3F42CD8AD8,
        is_remote=True,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    with trace.use_span(NonRecordingSpan(context)):
        attributes = runtime.inject(message, Raw())
    assert attributes == {"traceparent": "00-5759e988bd862e3fe1be46a994272793-53995c3f42cd8ad8-01"}
    assert message.trace_header is not None
