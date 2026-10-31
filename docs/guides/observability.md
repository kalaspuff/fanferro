# Observability

## Events

Everything notable is an `Event` of a stable `EventKind` (`"message.acked"`, `"lease.extended"`,
`"resource.created"`, ... - full list in the [events reference](../reference/events.md) or
`fanferro events`). Events carry `level`, `message`, `time`, structured `fields` (`queue`,
`message_id`, `sqs_message_id`, `message_type`, `attempt`, `duration`, ...), `error` details and,
for Python-side failures, the `exception`.

Kinds that nobody logs or hooks are never produced (the Rust core checks a flag per kind).

## Logging

Events are logged with the standard `logging` module to `fanferro.<category>` loggers
(`fanferro.message`, `fanferro.consumer`, `fanferro.resource`, `fanferro.lease`, ...).

```python
client = fanferro.Client(
    logging=fanferro.LoggingConfig(
        levels={"message.processed": logging.INFO, "lease.*": None},   # change levels; None = never log
        disabled=["receive.*"],                                         # never log these kinds
        include_fields=True,                                            # "text (queue=... message_id=...)"
        include_tracebacks=True,                                        # exc_info on failures
        formatter=None,                                                 # or (event) -> str
    )
)
```

Every record has `record.fanferro = {"kind", "fields", "error"}` for structured log processors
(e.g. a JSON formatter or structlog's stdlib integration). Logger levels still apply: enable DEBUG
on `fanferro.message` to see per-message events.

## Hooks

```python
@client.on("message.dead_lettered")
async def alert(event: fanferro.Event) -> None:
    await pager.trigger(f"dead letter on {event.fields['queue']}: {event.fields['reason']}")

unsubscribe = client.on(["resource.drift", "resource.missing"], lambda event: audit.append(event))
client.on("*", metrics_bridge)        # glob patterns: "message.*", "*.failed", ...
```

Hooks may be sync or async; async hooks run as tasks on the event loop, and so does an awaitable
that a sync hook returns (`lambda event: notify(event)` with an async `notify`). A failing hook emits
`hook.failed` and never affects message processing. Core events reach hooks within milliseconds.

## OpenTelemetry

Install `fanferro[otel]` and configure the OpenTelemetry SDK as usual. Then fanferro:

* creates `send <queue>` / `publish <topic>` producer spans and `process <queue>` consumer spans
  (messaging semantic conventions; handler spans nest under the consumer span);
* propagates trace context through the carriers you choose:

```python
fanferro.Telemetry(
    propagation=fanferro.Propagation(
        inject=(fanferro.Carrier.MESSAGE_ATTRIBUTES, fanferro.Carrier.AWS_TRACE_HEADER),           # default
        extract=(fanferro.Carrier.MESSAGE_ATTRIBUTES, fanferro.Carrier.ENVELOPE, fanferro.Carrier.AWS_TRACE_HEADER),
        attribute_keys=("traceparent", "tracestate"),     # add "baggage" if you use it
    ),
    consumer_span_parent="child",       # or "link": new trace linked to the producer
)
```

| Carrier | Where | Cost / notes |
| --- | --- | --- |
| `MESSAGE_ATTRIBUTES` | `traceparent`/`tracestate` attributes | 1-2 of 10 attribute slots; what OpenTelemetry's AWS SDK instrumentations use |
| `ENVELOPE` | CloudEvents distributed-tracing extension | free; CloudEvents envelopes only |
| `AWS_TRACE_HEADER` | `AWSTraceHeader` system attribute (SQS), `X-Amzn-Trace-Id` (SNS) | free; X-Ray format; understood by AWS Lambda/X-Ray |

SNS takes one `X-Amzn-Trace-Id` per `PublishBatch` call, not one per entry, so a batch publish to
a topic makes one call per trace header among its entries: messages of one `publish_batch()` share
the batch's span and need one call, prepared messages with their own trace contexts (an outbox
relay) one call each. They are sent at once on standard topics, one after the other on FIFO topics.

Context is injected from the current span (your web request's span if tracing is off for
fanferro), so `Telemetry(tracing=False)` still propagates. `Telemetry.disabled()` turns everything off.

## Metrics

With an OpenTelemetry `MeterProvider`, fanferro records the messaging semantic convention metrics
`messaging.client.sent.messages`, `messaging.client.operation.duration`,
`messaging.client.consumed.messages`, `messaging.process.duration`, and observable
`fanferro.consumer.*` instruments (acked, retried, requeued, dead_lettered, released,
receive_errors, lease_extensions, leases_lost, decode_failures, payloads_fetched, in_flight,
buffered, settling) plus `fanferro.client.*` counters.

The same numbers are available on demand without OpenTelemetry:

```python
client.metrics()        # {"client": {...}, "consumers": {name: stats}}
consumer.stats()        # processed, failed, duplicates, unroutable, decode_failures, handler_seconds, engine: {...}
```
