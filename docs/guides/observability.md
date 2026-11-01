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

Async hooks are bounded. At most `client.events.max_pending_hooks` (1,000) calls run at once: past
that, hooks are skipped for further events, a warning is logged, and an `events.dropped` event with
`reason="hooks"` and the count follows once the calls catch up (`client.events.hooks_skipped` keeps
the total). `client.close()` waits `client.events.flush_timeout` (5 s) for running hooks, then
cancels them and logs how many, so a hook that hangs (an HTTP call without a timeout) cannot keep a
shutting-down process alive. Give such calls their own timeout anyway.

An exception from the application's logging setup (a `logging.Filter`, a handler or its formatter)
never reaches fanferro's operations: the first one is reported once on stderr (through
`logging.lastResort`) and the event's log record is skipped.

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

SNS takes one `X-Amzn-Trace-Id` per `PublishBatch` call, not one per entry. A call whose entries
share a trace header sends it: messages of one `publish_batch()` share the batch's span, so they
keep it. A call whose entries differ (prepared messages of an outbox relay, each with the trace
context it was prepared in) sends none, and those messages carry their context in the `traceparent`
attribute only. `Client(split_batches_by_trace_header=True)` sends such batches in one call per
trace header instead (at once on standard topics, one after the other on FIFO topics): up to ten
calls per ten messages, for consumers that rely on the X-Ray header alone.

Trace context attributes never make a message fail. A message carries at most 10 attributes,
and fanferro's own (`fanferro.id`, `fanferro.type`, `content-type`...) count too; when the
propagation attributes would push a message over the limit, `tracestate` (and any other key) is
left out first, then `traceparent`, and a `config.warning` event names the destination (once per
destination). The trace then continues through the AWS trace header, which takes no attribute slot
(left out of `inject`, the context is not propagated). fanferro keeps one slot free for the
transport where it may need one: a binary body's `fanferro.encoding` attribute, or the pointer of a
payload offloaded to S3 when the destination has large payloads enabled. To keep `traceparent` on
every message, leave a slot free for it (two with `tracestate`, one more for binary bodies or
offloaded payloads).

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
metrics = client.metrics()          # fanferro.ClientMetrics
metrics.client.messages_published   # ClientCounters: publishing and payload counters
metrics.consumers["orders"]         # ConsumerStats of each consumer, by name

stats = consumer.stats()            # fanferro.ConsumerStats
stats.processed, stats.failed       # also duplicates, unroutable, decode_failures, handler_seconds
stats.engine.messages_acked         # EngineStats: the receive engine's counters and gauges
stats.engine.in_flight

counts = await queue.approximate_count()  # fanferro.QueueCounts
counts.visible, counts.in_flight, counts.delayed, counts.total
```

They are frozen dataclasses (`fanferro.stats`), so type checkers catch a misspelt name and editors
complete them; `as_dict()` gives the plain dictionary, e.g. to log it as JSON
(`json.dumps(client.metrics().as_dict())`). Their docstrings describe every counter.

For liveness and readiness (when a receive last succeeded, the failures since, whether the event
loop still dispatches), use `client.health()` and `consumer.health()`; see
[health checks](deployment.md#health-checks).
