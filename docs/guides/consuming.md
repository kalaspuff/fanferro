# Consuming

```python
consumer = client.consumer(queue, concurrency=20)

@consumer.route(OrderPlaced)
async def handle(order: OrderPlaced) -> None:
    ...

await consumer.run()          # until SIGINT/SIGTERM or consumer.request_stop()
```

## Handlers

A handler is an `async def` whose first parameter's annotation decides what it receives:

| Annotation | Receives | Body decoded as |
| --- | --- | --- |
| `order: OrderPlaced` | the body only | `OrderPlaced` |
| `message: fanferro.Message[OrderPlaced]` | the message | `OrderPlaced` (`message.body`) |
| `message: fanferro.Message` / none | the message | JSON → Python data, text → `str`, else `bytes` |
| `message: fanferro.Message[bytes]` | the message | raw bytes (never fails to decode) |

`Message` exposes the decoded `body`, envelope metadata (`id`, `type`, `time`, `source`,
`subject`, `headers`, `content_type`), user `attributes` (`message.attribute("tenant")` returns
`str`/`int`/`Decimal`/`bytes`), transport metadata (`receive_count`, `attempt`, `group_id`,
`sent_at`, `topic_arn`, `trace_header`, `pointer`) and settlement methods.

Return normally to **ack** (delete) the message. Raise to apply the [retry policy](failure-handling.md).
Automatic acks are not awaited: the delete is batched with others and completes in the background
(`consumer.stop()` waits for it; a failed delete is reported as a `message.ack_failed` event and
the message is redelivered). On FIFO queues, the next message of the group waits for the delete
instead (see [FIFO queues](#fifo-queues)). `consumer.stats()["processed"]` counts messages whose
handler succeeded, `stats()["engine"]["messages_acked"]` the confirmed deletes.

## Routing

Each consumer has a `Router`; `consumer.route(...)` is `consumer.router.route(...)`. Routers can
be built separately and shared: `client.consumer(queue, router)`, `router.include(other)`.

```python
from fanferro import matchers as m

router = fanferro.Router()

@router.route(OrderPlaced)                                   # by message type (incl. aliases)
async def placed(order: OrderPlaced) -> None: ...

@router.route(OrderPlacedV2, when=m.header("schemaversion") >= 2, priority=10)
async def placed_v2(order: OrderPlacedV2) -> None: ...

@router.route("orders.cancelled")                            # by type name
async def cancelled(message: fanferro.Message[dict]) -> None: ...

@router.route(when=(m.attribute("tenant") == "acme") & m.body("order.express").exists())
async def express(message: fanferro.Message[dict]) -> None: ...

@router.default                                               # anything else
async def other(message: fanferro.Message[bytes]) -> None: ...
```

Rules: routes are evaluated by `priority` (higher first), then registration order; the first match
wins; `default` routes run last. A typed route without `when` matches messages whose type is the
class's type name (or an alias from `@message_type("name", "alias")`). Messages without a type
(foreign producers) only match `when=` routes and the default route. `client.consumer(queue, handler)`
with a single handler uses it as the default route, so it receives everything.

Matchers (all in `fanferro.matchers`): `attribute(name)`, `header(name)` (falls back to attributes),
`body("dotted.path.0.field")` (JSON bodies), `message_type()`, `content_type()`, `source()`,
`subject()`, `envelope()`, `topic()` (ARN, or `== "name"` with the AWS name: `== orders.name`
when the client has a `topic_prefix`), `group_id()`; comparisons `==`, `!=`, `<`, `<=`, `>`,
`>=`, `.is_in()`, `.exists()`, `.missing()`, `.startswith()`, `.endswith()`, `.contains()`,
`.matches(regex)`, `.test(fn)`; combine with `&`, `|`, `~` (parenthesize comparisons:
`(m.topic() == "orders") | ...`). Any `(Message) -> bool` callable works too.

No route → the dead-letter policy's `on_unroutable` action (default: dead-letter).

## Settling explicitly

```python
@consumer.route(Job)
async def handle(message: fanferro.Message[Job]) -> None:
    if not ready():
        await message.retry(delay=60)          # or raise fanferro.Retry(delay=60)
        return
    await message.extend(600)                  # rarely needed: heartbeats do this
    await process(message.body)
    await message.ack()                        # optional: returning acks
```

Also `requeue(delay=...)`, `dead_letter(reason)`, `release()`, `discard(reason)`. With
`auto_ack=False`, unsettled messages are left to time out: heartbeats stop and the handler slot is
free when the handler ends, and the message can still be settled (by a task the handler started)
until its visibility timeout runs out.

An explicit `await message.ack()` (or any other settlement) inside a handler resolves once SQS
confirmed it, and the handler keeps its concurrency slot until then. Returning instead hands the
slot to the next message right away; the automatic ack completes in the background.

## Throughput and flow control

| Option | Default | Meaning |
| --- | --- | --- |
| `concurrency` | 10 | handlers running at the same time |
| `prefetch` | `min(batch_size, concurrency) × pollers` | extra messages leased ahead (kept invisible by heartbeats) |
| `batch_size` | 10 | messages per `ReceiveMessage` (1-10) |
| `min_receive_batch` | `batch_size` (FIFO: 1) | free slots a poller waits for before receiving |
| `receive_batch_wait` | 0.1 | seconds to wait for `min_receive_batch` free slots before receiving fewer |
| `wait_time` | 20 | long polling wait (0-20 s; fractions round up to whole seconds, as SQS counts them; `0` short-polls and pauses a second while the queue is empty) |
| `pollers` | 1 | concurrent receive loops (raise for high-throughput queues) |
| `rate_limit` | `None` | `RateLimit(per_second, burst)` or a number: max messages received per second |
| `handler_timeout` | `None` | cancel handlers after N seconds (`HandlerTimeoutError`, retried) |
| `settle_linger` | 0.005 | seconds a batch of acks or retries waits for more before it is sent |

Acks and retries are sent in batch calls (`DeleteMessageBatch`, `ChangeMessageVisibilityBatch`)
of up to ten messages: a batch goes out as soon as it is full, also while earlier calls are still in
flight, or `settle_linger` after its first settlement. When every running handler awaits an
explicit settlement (`await message.ack()`) and the batch holds all of them, nothing else can join
it, so it is sent right away.
`consumer.stats()["engine"]` counts the calls (`delete_requests`, `visibility_requests`).

Receives are full by default: a poller waits until `min_receive_batch` handler slots are free
(after the first one, for at most `receive_batch_wait`), so a busy consumer asks SQS for ten
messages per call instead of whatever single slot just became free. FIFO queues receive as soon as
one slot is free (waiting would hold back message groups). The default `prefetch` leases one more
batch per poller (at most `concurrency` messages), so the next receive overlaps with the handlers.

The engine never leases more than `concurrency + prefetch` unsettled messages. A message gives
its slot back as soon as its settlement starts (the handler returned or raised), so receiving and
the next handlers do not wait for delete round trips; settlements in progress are bounded
separately. `consumer.pause()` / `resume()` stop and restart receiving without affecting
in-flight messages (back-pressure, circuit breaking).

## Visibility and heartbeats

The visibility timeout defaults to the queue's attribute (override with `visibility_timeout=`).
While a message is buffered or processed, the engine extends its visibility before it expires
(`Heartbeat(interval=..., extend_by=..., max_duration=...)`; default interval: a third of the
timeout), batching ten messages per call. Heartbeats run on Rust threads, so a handler blocking the
event loop does not lose its lease. If a receipt handle becomes invalid, a `lease.lost` event is
emitted (the message may be processed twice; make handlers idempotent). Pass `heartbeat=False` to
disable heartbeats. SQS counts visibility timeouts and delays in whole seconds: fanferro rounds
fractions up (0.5 s becomes 1 s, never 0), and so does the in-memory broker.

## FIFO queues

Messages of the same group are processed one at a time in order; groups run concurrently. A
group's next message starts once the automatic ack of the previous one is confirmed. When a message
is retried or released, or its delete fails, the group's later messages in the same batch are
released, so SQS redelivers the group in order, starting with that message. Force or disable the
sequencing with `ordering="fifo"|"none"`.

Waiting for the delete adds its round trip to each message of a group (the delete does not wait for
`settle_linger` when nothing else could join its batch). `strict_ordering=False` acks in the
background instead: faster per group, but a message whose delete fails is redelivered after the
group's later messages.

## SNS subscriptions

Messages from SNS arrive raw (default for subscriptions declared with fanferro) or wrapped in
SNS notification JSON; fanferro unwraps both, so handlers see the original body and attributes.
`message.topic_arn` is set for wrapped deliveries. Cross-account subscriptions send a
confirmation message to the queue: set `auto_confirm_subscriptions=True` to confirm it.

## Lifecycle

```python
await consumer.run()                  # start, wait for a stop request or signal, stop gracefully
async with consumer: ...              # start on enter, stop on exit
await consumer.start(); ...; await consumer.stop(drain_timeout=10)
await consumer.run_until_idle()       # tests and batch jobs: stop once the queue is empty
await client.run()                    # all consumers of the client, with signal handling
```

Shutdown follows `ShutdownPolicy`:

1. receiving stops at once: the long poll in flight is aborted, and messages released during
   shutdown stay invisible until SQS ended the abandoned poll, so it cannot capture them (SQS may
   still hand that poll up to `batch_size` new messages; they are redelivered after their
   visibility timeout). With `abort_receives=False` the poll may finish instead (≤ `wait_time`) and
   its messages are released right away - slower to stop, nothing delayed (ADR 0007, ADR 0008);
2. prefetched messages are released at once (`release_prefetched`), also those that wait for a
   handler slot; in-flight handlers get `drain_timeout` seconds (`None` = forever) with
   `consumer.draining` progress events every `progress_interval`;
3. remaining handlers are cancelled and get `cancel_timeout` seconds to unwind;
4. unfinished messages are released (`release_unfinished`) or left to time out;
5. pending acks are flushed: automatic acks run in the background and complete, and so does a
   settlement that started (`await message.ack()`...) even if its handler was cancelled.

A second SIGINT/SIGTERM (or a second `request_stop()`/`client.stop()`) skips the drain: handlers
are cancelled at once and the long poll in flight is aborted. `stop()` called while a stop is in
progress waits for it (and `stop(drain_timeout=0)` cuts it short); called while the consumer is
starting, it stops the consumer once it started. `client.close()` stops all consumers at once.

## Consume middleware

```python
async def timing(message: fanferro.Message, call_next: fanferro.HandlerNext) -> None:
    started = time.perf_counter()
    try:
        await call_next(message)
    finally:
        histogram.record(time.perf_counter() - started, {"type": message.type})

client = fanferro.Client(consume_middleware=[timing])          # every consumer
consumer = client.consumer(queue, middleware=[idempotent(store)])  # one consumer
```

Middleware runs after routing and decoding (the body is available) and may raise handler actions.
