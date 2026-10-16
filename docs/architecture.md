# Architecture

fanferro is a Rust core with a Python API on top. This document explains how the pieces fit,
which layer owns what, and the invariants every change must preserve. Read it before changing
anything non-trivial; update it in the same change when you move a responsibility.

## Layers

```text
 Python application code
 ───────────────────────────────────────────────────────────────────────────────────────────
 python/fanferro            Client · Topic/Queue/Subscription · Consumer · Router/matchers
 (asyncio, user objects)    codecs (pydantic, dataclasses, protobuf, msgspec, JSON...)
                            envelopes protocol · middleware · events/logging/hooks
                            OpenTelemetry · idempotency · outbox · testing helpers
 ───────────────────────── PyO3 boundary: fanferro._core (crates/fanferro-py) ─────────────
 crates/fanferro-py         conversions, fork-aware Tokio runtime, asyncio bridge,
                            exception mapping. No business logic.
 ───────────────────────────────────────────────────────────────────────────────────────────
 crates/fanferro-core       envelope/      CloudEvents JSON · CloudEvents protobuf · raw
 (Tokio, no Python)         transport/     base64 for binary bodies · claim checks (S3,
                                           presigned URLs, SSRF allow-list) · SNS unwrapping
                            publish.rs     send/publish, batches, FIFO dedup, MD5 checks
                            consumer/      pollers · leases · heartbeats · settlement · shutdown
                            provision/     lookup → verify → update/create, drift, policies
                            events.rs      typed events (filtered, bounded buffer)
                            metrics.rs     lock-free counters, on-demand snapshots
                            backend/       ports: SqsApi · SnsApi · ObjectStoreApi · HttpFetch
                                           · IdentityApi, with two adapters:
                                             aws/    AWS SDK for Rust (AWS, MiniStack...)
                                             memory/ in-process broker for tests
```

**Why this split.** Everything that touches application objects (pydantic models, protobuf classes,
handler functions, OpenTelemetry context) lives in Python, because only Python can do it well.
Everything that is protocol, timing or I/O heavy (SigV4 signing, retries, long polling,
visibility heartbeats, batching, base64/JSON/protobuf envelopes, S3 transfers) lives in Rust:
it is fast, runs on its own threads (heartbeats keep working even if a handler blocks the event
loop), and is testable without Python. See [ADR 0001](adr/0001-rust-core-python-api.md).

## Message pipeline

Publishing (`topic.publish(obj)`):

```text
Python  obj ─ codec ─▶ Payload(data, content_type, type, id, time, source, headers)
        middleware (PublishContext) · telemetry injects trace context
        Envelope.wrap ─▶ WireMessage(body bytes, attributes)     [builtin = Rust, custom = Python]
        user attributes merged (fanferro.* is reserved)            ─▶ PreparedMessage (outbox-ready)
Rust    transport: non-text body → base64 + fanferro.encoding
                   size > threshold → S3 + pointer + fanferro.claim-check   (or AWS extended format)
                   size > limit without offloading → MessageTooLargeError (fail fast)
        FIFO: group id required, dedup id defaults to the message id
        SendMessage/Publish (+ batches, partial-failure retries, optional MD5 verification)
```

Consuming (`consumer.run()`):

```text
Rust    pollers: acquire in-flight permits (+ rate limiter) ─▶ ReceiveMessage (long poll)
        lease each message (heartbeats start now) ─▶ MD5 check (opt-in) ─▶ SNS notification unwrap
        ─▶ transport decode (claim-check fetch, base64) ─▶ envelope unwrap (built-in envelopes)
        ─▶ Delivery into a bounded channel
Python  dispatcher: next_batch() ─▶ per-message task (FIFO: sequenced per group)
        envelope unwrap (custom envelopes, in the configured order) ─▶ Message ─▶ attempts check
        router.resolve ─▶ codec decode into the route's type ─▶ middleware ─▶ handler
        success → ack (batched delete)   failure → RetryPolicy/DeadLetterPolicy
Rust    settle loop batches DeleteMessage / ChangeMessageVisibility (ten at once, also while
        earlier calls are in flight; fewer after the linger, or at once when every handler awaits a
        settlement of the batch); DLQ forward / requeue send a copy (original body + attributes +
        fanferro.dead-letter / fanferro.attempt)
```

## Invariants

These are relied upon across layers; tests cover each of them.

1. **At-least-once, never silently dropped.** A received message is always settled: acked,
   retried (visibility), requeued, dead-lettered, released, or explicitly discarded with an event.
   Dropping a `Delivery` without settling releases the message (Rust `Drop`).
2. **Bounded in-flight.** A consumer never holds more than `concurrency + prefetch` unsettled
   messages; pollers acquire one in-flight permit per requested message before calling
   `ReceiveMessage`. A settlement exchanges the message's permit for one of `max_settling`
   settling permits (default: four times the in-flight limit, at least 20) when it starts, so
   receiving continues during the settlement's round trip and the settlement backlog is bounded
   too.
3. **Leases are protected until settled.** Heartbeats extend visibility (batched by ten) while a
   message is buffered, processing or waiting for a settling permit, capped at 12 hours after
   receipt and `max_duration`. An extension call that gets no answer within half the heartbeat
   interval (0.5-10 s) is retried at the next tick, so a stuck connection cannot stall the others.
   The lease stays tracked until its settlement completed.
4. **Stable message ids.** The fanferro message id is generated once per message and reused on
   retries; FIFO destinations use it as the deduplication id; idempotency keys default to it.
5. **Transport details are invisible to handlers.** Base64, claim checks and SNS wrapping are
   undone before envelopes see the message; `fanferro.*` attributes are hidden from
   `message.attributes` (still in `message.raw_attributes`).
6. **Fail fast before the network.** Queue and topic names (AWS naming rules, environment prefix
   included), attribute limits (10, names, empty values), reserved names, FIFO group ids and size
   limits are validated locally with actionable messages.
7. **No hidden I/O on import.** Importing fanferro does nothing; the client connects lazily or
   on `async with`. Optional libraries (pydantic, protobuf, msgspec, OpenTelemetry) are never
   imported by fanferro unless the application uses them.
8. **Graceful shutdown leaks nothing.** Stopping aborts the long poll in flight and keeps
   messages it releases invisible until SQS ended that poll (or waits for the poll, see
   [ADR 0007](adr/0007-shutdown-and-orphaned-long-polls.md) and [ADR 0008](adr/0008-lean-defaults.md)),
   drains handlers within the policy, releases or abandons what is left, flushes settlements.
9. **Events are cheap when unused.** Rust only builds events of kinds that logging or a hook
   wants (atomic flag check); the buffer is bounded and overflow is reported (`events.dropped`).
10. **Fork safety.** The Tokio runtime is per process; clients created before `fork()` refuse to
    work in the child instead of corrupting connections.
11. **Clean interpreter exit.** An `atexit` hook stops the runtime (GIL released, bounded wait)
    before the interpreter finalizes, so no Rust thread touches Python during finalization
    (CPython 3.12 kills such threads, which crashes the process). Runtime threads hand results to
    the event loop without attaching to the interpreter at all, except on loops that cannot watch
    file descriptors (see below). Afterwards awaitables raise `ShutdownError`. Tests:
    `tests/unit/test_interpreter_lifecycle.py`.

## Concurrency model

* One Tokio runtime per process (`crates/fanferro-py/src/runtime.rs`), 2-4 worker threads by
  default (`FANFERRO_WORKER_THREADS`). An awaitable is an asyncio future (`RustFuture`) driven by
  one Tokio task; cancelling it aborts the task, which drops the Rust future at its next await
  point. Outcomes are converted into Python objects on the event loop's thread, in batches: a
  finished task queues its outcome on the bridge of its loop, and the first outcome of a batch
  wakes the loop by writing a byte to a socket the loop watches (`loop.add_reader`), so runtime
  threads never wait for the GIL. Loops that cannot watch file descriptors (the proactor loop on
  Windows) get one `call_soon_threadsafe` per batch from a blocking task instead
  ([ADR 0009](adr/0009-results-reach-asyncio-in-batches.md)). Settlements (`delivery.ack()`,
  `nack()`...) run as tasks the engine tracks (`close()` waits for them; cancelling their
  awaitable does not stop a started delete).
* Per consumer engine: `pollers` receive loops, one heartbeat loop, one settle loop and one
  decode task per received batch (decoded and unwrapped concurrently, handed out together and in
  order). Lease state is in a `parking_lot` mutex that is never held across an `.await`. An ack
  queues its delete synchronously when a settling permit is free, so acks reach the settle loop in
  the order they were requested; queueing wakes the loop only when it can then send something
  sooner (the first pending settlement, a full batch, an urgent or awaited settlement). The loop
  sends full batches while earlier calls are in flight and finishes each lease when its batch call
  returns.
* Per Python consumer: one dispatcher task, one task per message (or per FIFO group), a
  semaphore of `concurrency` permits held while decoding, middleware and the handler run; the
  task settles the message after giving its permit back. Automatic acks are not awaited: the
  engine deletes the message in the background (`message.ack_failed` events report failures).
* Events flow Rust → Python through a bounded channel drained by an event pump task (started
  while a consumer runs or the client is entered) or synchronously after operations. The pump
  is stopped by waking its pending wait, not by cancelling it, so events that wait already took
  are still dispatched.

## Extension points

| Extension | How | Where |
| --- | --- | --- |
| Message formats | implement `Codec` (Python protocol) | `python/fanferro/codecs/_base.py` |
| Wire layouts | implement `Envelope` (Python protocol) or add a Rust envelope | `python/fanferro/envelopes.py`, `crates/fanferro-core/src/envelope/` |
| Behaviour around publishing/handling | publish/consume middleware | `python/fanferro/middleware.py` |
| Observability | event hooks, `LoggingConfig`, OpenTelemetry providers | `python/fanferro/events.py`, `telemetry.py` |
| Failure decisions | `RetryPolicy(classify=...)`, handler actions, `Fallback` | `policies.py`, `actions.py`, `provisioning.py` |
| Idempotency/outbox storage | implement `IdempotencyStore` / `OutboxStore` | `idempotency.py`, `outbox.py` |
| New AWS operations | extend a backend trait + both adapters | `crates/fanferro-core/src/backend/` |

## Where things live

| Concern | Rust | Python |
| --- | --- | --- |
| Errors | `error.rs` (classification) | `errors.py` (hierarchy re-export + Python-only errors) |
| Configuration | serde structs next to their module | dataclasses with `to_core()` |
| Events | `events.rs` (`EventKind`) | `events.py` (`EventKind`, must match) |
| Reserved attributes | `types/reserved.rs` | `messages.py` hides `fanferro.*` |
| Tests | `#[cfg(test)]` modules, `tests/aws_emulator.rs` | `tests/unit`, `tests/integration`, `tests/e2e` |

The rules for keeping these in sync are in [AGENTS.md](../AGENTS.md) and enforced by
`scripts/check_consistency.py`.
