"""OpenTelemetry integration: tracing, context propagation and metrics.

Install ``fanferro[otel]`` (``opentelemetry-api``) and configure an OpenTelemetry SDK as usual;
fanferro then creates producer spans when publishing and consumer spans around handlers (so spans
your handler creates are nested under them), propagates trace context through messages and records
messaging metrics. Without OpenTelemetry installed, telemetry is a no-op.

Trace context can travel through three carriers (configure them with :class:`Propagation`):

* ``MESSAGE_ATTRIBUTES``: W3C ``traceparent``/``tracestate`` message attributes. The convention
  used by OpenTelemetry instrumentations of the AWS SDKs; costs 1-2 of the 10 attribute slots.
* ``ENVELOPE``: CloudEvents distributed-tracing extension inside the envelope (no attribute slots;
  only for CloudEvents envelopes).
* ``AWS_TRACE_HEADER``: AWS X-Ray format in the ``AWSTraceHeader`` system attribute (SQS) or the
  ``X-Amzn-Trace-Id`` header (SNS, with active tracing). Understood by AWS Lambda and X-Ray.

Consumers extract from the first carrier that has a context (default order: message attributes,
envelope, AWS trace header).

Metrics follow the OpenTelemetry messaging semantic conventions
(``messaging.client.sent.messages``, ``messaging.client.operation.duration``,
``messaging.client.consumed.messages``, ``messaging.process.duration``) plus observable
``fanferro.*`` instruments fed from the Rust core's counters (leases, retries, dead letters...).
The same counters are available on demand from ``client.metrics()`` and ``consumer.stats()``.
"""

from __future__ import annotations

import contextlib
import importlib
import time
import weakref
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

from fanferro._core import traceparent_from_xray, xray_from_traceparent

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping
    from contextlib import AbstractContextManager

    from fanferro.envelopes import Envelope
    from fanferro.messages import Message, OutgoingMessage

__all__ = ["Carrier", "Propagation", "Telemetry", "TelemetryRuntime"]

INSTRUMENTATION_NAME = "fanferro"

# Returned instead of a span context when tracing is off: no generator or span per message.
_NO_SPAN: AbstractContextManager[None] = contextlib.nullcontext()


class Carrier(StrEnum):
    """Where trace context is written to / read from."""

    MESSAGE_ATTRIBUTES = "message_attributes"
    ENVELOPE = "envelope"
    AWS_TRACE_HEADER = "aws_trace_header"


@dataclass(frozen=True, slots=True)
class Propagation:
    """Trace context propagation settings.

    Attributes:
        inject: Carriers written when publishing.
        extract: Carriers read when consuming, in priority order.
        attribute_keys: Propagator fields allowed into message attributes (``baggage`` is excluded by
            default to save attribute slots; add it if you rely on baggage).
        propagator: A ``TextMapPropagator``; defaults to OpenTelemetry's global propagator.
    """

    inject: tuple[Carrier, ...] = (Carrier.MESSAGE_ATTRIBUTES, Carrier.AWS_TRACE_HEADER)
    extract: tuple[Carrier, ...] = (Carrier.MESSAGE_ATTRIBUTES, Carrier.ENVELOPE, Carrier.AWS_TRACE_HEADER)
    attribute_keys: tuple[str, ...] = ("traceparent", "tracestate")
    propagator: Any = None


@dataclass(frozen=True, slots=True)
class Telemetry:
    """OpenTelemetry settings.

    Attributes:
        tracing: Create producer and consumer spans.
        metrics: Record messaging metrics.
        propagation: How trace context travels with messages.
        consumer_span_parent: ``"child"`` makes the processing span a child of the producer's span;
            ``"link"`` starts a new trace linked to it (preferable for very long-lived workflows).
        tracer_provider: Explicit tracer provider (default: the global one).
        meter_provider: Explicit meter provider (default: the global one).
    """

    tracing: bool = True
    metrics: bool = True
    propagation: Propagation = field(default_factory=Propagation)
    consumer_span_parent: Literal["child", "link"] = "child"
    tracer_provider: Any = None
    meter_provider: Any = None

    @classmethod
    def disabled(cls) -> Telemetry:
        """No spans, metrics or propagation."""
        return cls(tracing=False, metrics=False, propagation=Propagation(inject=(), extract=()))


def _import_otel() -> dict[str, Any] | None:
    try:
        return {
            "trace": importlib.import_module("opentelemetry.trace"),
            "propagate": importlib.import_module("opentelemetry.propagate"),
            "metrics": importlib.import_module("opentelemetry.metrics"),
            "context": importlib.import_module("opentelemetry.context"),
        }
    except ImportError:
        return None


def _system(kind: str) -> str:
    return "aws.sns" if kind == "topic" else "aws_sqs"


def _error_type(error: BaseException | None) -> str | None:
    return None if error is None else f"{type(error).__module__}.{type(error).__qualname__}"


class TelemetryRuntime:
    """The live telemetry objects of a client (internal)."""

    def __init__(self, config: Telemetry | None, version: str) -> None:
        self.config = config or Telemetry()
        self._otel = _import_otel()
        self._version = version
        self._tracer: Any = None
        self._meter: Any = None
        self._instruments: dict[str, Any] = {}
        # Metric attributes of successful operations, by destination (built once, never mutated).
        self._attributes: dict[tuple[str, str], dict[str, str]] = {}
        self._consumers: weakref.WeakSet[Any] = weakref.WeakSet()
        self._client_metrics: Callable[[], Mapping[str, int]] | None = None
        self._proxy_provider: type | None = None
        if self._otel is None:
            return
        trace = self._otel["trace"]
        # The API's provider until an SDK is configured: its spans record nothing (see _noop_tracing).
        self._proxy_provider = getattr(trace, "ProxyTracerProvider", None)
        if self.config.tracing:
            self._tracer = trace.get_tracer(INSTRUMENTATION_NAME, version, tracer_provider=self.config.tracer_provider)
        if self.config.metrics:
            self._setup_metrics()

    @property
    def available(self) -> bool:
        """Whether OpenTelemetry is installed."""
        return self._otel is not None

    def _noop_tracing(self) -> bool:
        """Whether spans record nothing: no OpenTelemetry SDK is configured (yet).

        The API then creates non-recording spans that only re-activate the span of their parent
        context, which :meth:`process_span` and :meth:`publish_span` do directly.
        """
        if self.config.tracer_provider is not None or self._proxy_provider is None:
            return False
        assert self._otel is not None
        return isinstance(self._otel["trace"].get_tracer_provider(), self._proxy_provider)

    def _propagator(self) -> Any:
        if self.config.propagation.propagator is not None:
            return self.config.propagation.propagator
        assert self._otel is not None
        return self._otel["propagate"].get_global_textmap()

    # -- metrics ---------------------------------------------------------------------------

    def _setup_metrics(self) -> None:
        assert self._otel is not None
        metrics = self._otel["metrics"]
        self._meter = metrics.get_meter(INSTRUMENTATION_NAME, self._version, meter_provider=self.config.meter_provider)
        meter = self._meter
        self._instruments = {
            "sent": meter.create_counter(
                "messaging.client.sent.messages", unit="{message}", description="Messages sent or published."
            ),
            "operation_duration": meter.create_histogram(
                "messaging.client.operation.duration", unit="s", description="Duration of publish operations."
            ),
            "consumed": meter.create_counter(
                "messaging.client.consumed.messages", unit="{message}", description="Messages received."
            ),
            "process_duration": meter.create_histogram(
                "messaging.process.duration", unit="s", description="Duration of message processing."
            ),
        }
        observation = metrics.Observation

        def consumer_callback(metric: str) -> Callable[[Any], list[Any]]:
            def callback(_options: Any) -> list[Any]:
                observations = []
                for consumer in list(self._consumers):
                    try:
                        stats = consumer.stats()
                    except Exception:  # noqa: BLE001, S112 - never break metric collection
                        continue
                    if stats.engine.state == "created":
                        continue  # not started yet: nothing to report
                    observations.append(
                        observation(
                            getattr(stats.engine, metric),
                            {"messaging.destination.name": consumer.queue_name, "messaging.system": "aws_sqs"},
                        )
                    )
                return observations

            return callback

        for metric, description in (
            ("messages_acked", "Messages deleted after processing."),
            ("messages_retried", "Messages scheduled for retry."),
            ("messages_requeued", "Messages re-enqueued."),
            ("messages_dead_lettered", "Messages forwarded to a dead-letter queue."),
            ("messages_released", "Messages released without processing."),
            ("receive_errors", "Failed receive calls."),
            ("lease_extensions", "Visibility extensions."),
            ("lease_extension_failures", "Failed visibility extensions."),
            ("leases_lost", "Leases whose receipt handle became invalid."),
            ("decode_failures", "Messages that could not be decoded."),
            ("payloads_fetched", "Offloaded payloads downloaded."),
        ):
            meter.create_observable_counter(
                f"fanferro.consumer.{metric}",
                callbacks=[consumer_callback(metric)],
                unit="{message}",
                description=description,
            )
        for metric, description in (
            ("in_flight", "Messages leased (buffered, processing or settling)."),
            ("buffered", "Messages waiting for a free handler slot."),
            ("settling", "Messages whose settlement (delete, retry, dead-letter) is in progress."),
        ):
            meter.create_observable_up_down_counter(
                f"fanferro.consumer.{metric}",
                callbacks=[consumer_callback(metric)],
                unit="{message}",
                description=description,
            )

        def client_callback(metric: str) -> Callable[[Any], list[Any]]:
            def callback(_options: Any) -> list[Any]:
                if self._client_metrics is None:
                    return []
                try:
                    return [observation(self._client_metrics()[metric], {})]
                except Exception:  # noqa: BLE001 - never break metric collection
                    return []

            return callback

        for metric in ("payloads_offloaded", "publish_retries", "bytes_published"):
            meter.create_observable_counter(f"fanferro.client.{metric}", callbacks=[client_callback(metric)])

    def watch_consumer(self, consumer: Any) -> None:
        """Exposes a consumer's statistics through observable instruments."""
        self._consumers.add(consumer)

    def watch_client(self, metrics: Callable[[], Mapping[str, int]]) -> None:
        """Exposes client statistics through observable instruments."""
        self._client_metrics = metrics

    def record_publish(
        self, destination: str, kind: str, duration: float, count: int, error: BaseException | None
    ) -> None:
        """Records a publish operation."""
        if not self._instruments:
            return
        attributes = self._attributes.get((kind, destination)) if error is None else None
        if attributes is None:
            attributes = {
                "messaging.system": _system(kind),
                "messaging.destination.name": destination,
                "messaging.operation.name": "publish" if kind == "topic" else "send",
            }
            if error is None:
                self._attributes[kind, destination] = attributes
            else:
                attributes["error.type"] = _error_type(error) or ""
        self._instruments["sent"].add(count, attributes)
        self._instruments["operation_duration"].record(duration, attributes)

    def record_consumed(self, queue: str, count: int) -> None:
        """Records received messages."""
        if self._instruments:
            attributes = self._attributes.get(("consumed", queue))
            if attributes is None:
                attributes = {"messaging.system": "aws_sqs", "messaging.destination.name": queue}
                self._attributes["consumed", queue] = attributes
            self._instruments["consumed"].add(count, attributes)

    def record_process(self, queue: str, duration: float, error: BaseException | None) -> None:
        """Records message processing."""
        if not self._instruments:
            return
        attributes = self._attributes.get(("process", queue)) if error is None else None
        if attributes is None:
            attributes = {
                "messaging.system": "aws_sqs",
                "messaging.destination.name": queue,
                "messaging.operation.name": "process",
            }
            if error is None:
                self._attributes["process", queue] = attributes
            else:
                attributes["error.type"] = _error_type(error) or ""
        self._instruments["process_duration"].record(duration, attributes)

    # -- tracing ---------------------------------------------------------------------------

    def publish_span(
        self, destination: str, kind: str, message_id: str | None, batch_size: int | None = None
    ) -> AbstractContextManager[Any]:
        """A producer span (no-op without tracing)."""
        if self._tracer is None or self._otel is None or self._noop_tracing():
            # Without an SDK the span would only re-activate the current span: nothing to do.
            return _NO_SPAN
        return self._publish_span(destination, kind, message_id, batch_size)

    @contextlib.contextmanager
    def _publish_span(
        self, destination: str, kind: str, message_id: str | None, batch_size: int | None
    ) -> Iterator[Any]:
        assert self._tracer is not None
        assert self._otel is not None
        trace = self._otel["trace"]
        attributes: dict[str, Any] = {
            "messaging.system": _system(kind),
            "messaging.destination.name": destination,
            "messaging.operation.name": "publish" if kind == "topic" else "send",
            "messaging.operation.type": "send",
        }
        if message_id is not None:
            attributes["messaging.message.id"] = message_id
        if batch_size is not None:
            attributes["messaging.batch.message_count"] = batch_size
        operation = "publish" if kind == "topic" else "send"
        with self._tracer.start_as_current_span(
            f"{operation} {destination}",
            kind=trace.SpanKind.PRODUCER,
            attributes=attributes,
            # mark_error() records the exception on failure paths: the SDK must not add a
            # second exception event (as consumer spans, _process_span already leaves it out).
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            yield span

    def inject(self, message: OutgoingMessage, envelope: Envelope) -> dict[str, str] | None:
        """Writes the current trace context into ``message`` according to the propagation settings.

        Envelope headers and the AWS trace header are written into ``message``; the message
        attributes are returned instead (``traceparent`` first, then the other keys in the order of
        ``attribute_keys``), for the caller to add as far as the attribute limit allows.
        """
        if self._otel is None or not self.config.propagation.inject:
            return None
        carrier: dict[str, str] = {}
        self._propagator().inject(carrier)
        traceparent = carrier.get("traceparent")
        if not traceparent:
            return None
        from fanferro.envelopes import supports_headers

        attributes: dict[str, str] | None = None
        for target in self.config.propagation.inject:
            if target is Carrier.MESSAGE_ATTRIBUTES:
                keys = self.config.propagation.attribute_keys
                attributes = {
                    key: carrier[key]
                    for key in sorted(keys, key=lambda key: key != "traceparent")
                    if carrier.get(key) and key not in message.attributes
                }
            elif target is Carrier.ENVELOPE and supports_headers(envelope):
                for key, value in carrier.items():
                    if value and key.isalnum() and key.islower():
                        message.headers.setdefault(key, value)
            elif target is Carrier.AWS_TRACE_HEADER and message.trace_header is None:
                message.trace_header = xray_from_traceparent(traceparent)
        return attributes

    def extract(self, message: Message[Any]) -> Any:
        """The trace context carried by ``message`` (``None`` if absent)."""
        if self._otel is None:
            return None
        for source in self.config.propagation.extract:
            carrier: dict[str, str] = {}
            if source is Carrier.MESSAGE_ATTRIBUTES:
                for key in ("traceparent", "tracestate", "baggage"):
                    value = message.raw_attributes.get(key)
                    if value is not None and isinstance(value.value, str):
                        carrier[key] = value.value
            elif source is Carrier.ENVELOPE:
                for key in ("traceparent", "tracestate", "baggage"):
                    value = message.headers.get(key)
                    if isinstance(value, str):
                        carrier[key] = value
            elif source is Carrier.AWS_TRACE_HEADER and message.trace_header:
                traceparent = traceparent_from_xray(message.trace_header)
                if traceparent:
                    carrier["traceparent"] = traceparent
            if "traceparent" in carrier:
                context = self._propagator().extract(carrier)
                span_context = self._otel["trace"].get_current_span(context).get_span_context()
                if span_context.is_valid:
                    return context
        return None

    def process_span(self, message: Message[Any], queue: str) -> AbstractContextManager[Any]:
        """A consumer span around processing ``message`` (no-op without tracing)."""
        if self._tracer is None or self._otel is None:
            return _NO_SPAN
        if self._noop_tracing():
            # Without an SDK the span records nothing and only makes the span of the extracted
            # producer context the current one (propagation continues through the handler).
            if self.config.consumer_span_parent == "child" and (parent := self.extract(message)) is not None:
                trace = self._otel["trace"]
                return trace.use_span(trace.get_current_span(parent))
            return _NO_SPAN
        return self._process_span(message, queue)

    @contextlib.contextmanager
    def _process_span(self, message: Message[Any], queue: str) -> Iterator[Any]:
        assert self._tracer is not None
        assert self._otel is not None
        trace = self._otel["trace"]
        parent = self.extract(message)
        attributes: dict[str, Any] = {
            "messaging.system": "aws_sqs",
            "messaging.destination.name": queue,
            "messaging.operation.name": "process",
            "messaging.operation.type": "process",
            "messaging.message.id": message.id,
            "messaging.message.body.size": len(message.data),
            "fanferro.receive_count": message.receive_count,
        }
        if message.type:
            attributes["fanferro.message_type"] = message.type
        if message.group_id:
            attributes["messaging.message.group.id"] = message.group_id
        links = []
        context = parent
        if parent is not None and self.config.consumer_span_parent == "link":
            links = [trace.Link(trace.get_current_span(parent).get_span_context())]
            context = None
        with self._tracer.start_as_current_span(
            f"process {queue}",
            context=context,
            kind=trace.SpanKind.CONSUMER,
            attributes=attributes,
            links=links,
            record_exception=False,
            set_status_on_exception=False,
        ) as span:
            yield span

    def mark_error(self, span: Any, error: BaseException) -> None:
        """Records ``error`` on ``span``."""
        if span is None or self._otel is None:
            return
        trace = self._otel["trace"]
        span.record_exception(error)
        span.set_status(trace.Status(trace.StatusCode.ERROR, str(error) or type(error).__name__))
        span.set_attribute("error.type", _error_type(error) or "")


monotonic = time.perf_counter
"""Monotonic clock used for durations."""
