# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/) (0.x: minor versions may contain breaking changes).
Entries are generated from conventional commits with [git-cliff](https://git-cliff.org)
(`cliff.toml`) by the release workflow; edit the generated text if it needs polishing.

## [Unreleased]

## [0.3.0] - 2026-10-15

### Queue and topic names are checked when they are declared

Names must follow the rules of SQS and SNS: letters, digits, `-` and `_`, a `.fifo` suffix for
FIFO resources, and at most 80 characters for a queue or 256 for a topic, including the new
`queue_prefix`/`topic_prefix` (see
[Deployment](docs/guides/deployment.md#several-environments-in-one-aws-account)). Other names
raise `ConfigurationError` before any network call. AWS and the in-memory broker never accepted
them, but MiniStack does: a name such as `orders.v1` that worked against MiniStack is now
rejected.

### Features

- Queue_prefix and topic_prefix for per-environment resource names (#12) (#12)

### Documentation

- Document AWS credential and region resolution (#10) (#10)
- A skill for pull requests, stacks and publish kits (#11) (#11)

## [0.2.0] - 2026-10-14

### Lean, fast defaults (breaking)

Without any tuning, clients and consumers now perform like a hand-tuned 0.1 configuration
([ADR 0008](docs/adr/0008-lean-defaults.md)):

- Messages are published with the **raw envelope**: the body is the encoded payload (e.g. the
  JSON document); `fanferro.id` and `fanferro.type` (typed messages) travel as message attributes,
  `content-type` only for payloads that are not JSON. CloudEvents is opt-in:
  `Client(envelope=fanferro.envelopes.CloudEvents())`. Consumers still read CloudEvents (JSON and
  protobuf), so messages from 0.1 producers keep working.
- **MD5 verification** of sent and received messages is opt-in: `Client(verify_md5=True)`.
- **`consumer.stop()` returns at once**: `ShutdownPolicy.abort_receives` defaults to `True` (running
  handlers still drain). SQS may still hand the aborted long poll up to `batch_size` messages per
  poller; they are redelivered after their visibility timeout.
- **Full receives**: a poller waits until `min_receive_batch` handler slots are free (default:
  `batch_size`; FIFO queues: 1), for at most `receive_batch_wait` (0.1 s) after the first one, and
  `prefetch` defaults to one receive batch per poller (`min(batch_size, concurrency) × pollers`).
- **Slots are freed when settlement starts**: a handler that returns hands its slot to the next
  message while the delete is in flight; settlements in progress are bounded separately.
- **Batched acks**: deletes and visibility changes wait up to `settle_linger` (5 ms) to fill a
  batch of ten, and are sent at once when every running handler awaits an explicit settlement
  in that batch.
- **Automatic acks are not awaited per message**: the engine deletes the message in the
  background (`consumer.stop()` waits for it). `consumer.stats()["processed"]` and the
  `message.processed` event count handlers that succeeded; `stats()["engine"]["messages_acked"]`
  counts confirmed deletes, and a failed delete is reported as a `message.ack_failed` event. Explicit
  settlements (`await message.ack()`...) still resolve once SQS confirmed them.
- `consumer.stop()` and `client.close()` no longer wait up to half a second for the dispatcher or
  the event pump, and events emitted just before `close()` are no longer lost.
- Per-message work in the consumer and publisher is lighter: settlement awaitables are resolved
  in batches, event checks, type names and encoders are cached, and messages are no longer copied
  while being decoded.

Migrating from 0.1:

- Upgrade consumers before producers. 0.1 consumers read 0.2 messages (typed routes, message ids
  and filter policies on `fanferro.type` work), but they do not imply JSON for raw messages:
  untyped routes see JSON scalars as `bytes`, and CloudEvents metadata (`message.source`,
  `subject`, headers) is absent.
- Services that parse fanferro messages as CloudEvents themselves keep producing them with
  `Client(envelope=fanferro.envelopes.CloudEvents())`.
- The 0.1 behaviour is one configuration away:

  ```python
  client = fanferro.Client(envelope=fanferro.envelopes.CloudEvents(), verify_md5=True)
  consumer = client.consumer(
      queue,
      min_receive_batch=1,
      prefetch=0,
      settle_linger=0,
      shutdown=fanferro.ShutdownPolicy(abort_receives=False),
  )
  ```

### Features

- **consumer:** Full receive batches (min_receive_batch, receive_batch_wait, default prefetch) (#5) (#5)
- [**breaking**] Lean defaults - raw envelope, opt-in CloudEvents and MD5 checks, immediate stop (#6) (#6)

### Bug fixes

- **consumer:** Wake the dispatcher and the event pump immediately when stopping (#2) (#2)

### Performance

- **consumer:** Free handler slots when settlement starts and bound the settling backlog (#3) (#3)
- **consumer:** Batch acks and retries (full batches at once, 5 ms linger) (#4) (#4)
- Cheaper per-message consume and publish paths (#7) (#7)

## [0.1.0] - 2026-10-12

First release.

### Features

- Rust core (`fanferro-core`) with Python bindings (`fanferro._core`, abi3 for CPython 3.12+).
- Publishing to SNS topics and SQS queues: single and batch, FIFO group ids and deduplication,
  delays, attributes, envelope headers, partial-failure retries, MD5 verification.
- Envelopes: CloudEvents 1.0 JSON (default), CloudEvents protobuf, raw, and custom Python envelopes.
- Codecs: pydantic, dataclasses, msgspec, protobuf, JSON, text, bytes, and custom codecs.
- Large payloads (claim checks) in S3 with bucket/key and/or presigned URL pointers, SHA-256
  verification, SSRF-safe URL allow-list, and AWS Extended Client format compatibility.
- Consumers: long polling with in-flight limits, prefetch, rate limiting, multiple pollers, visibility
  heartbeats, batched settlement, routing (types, headers, attributes, JSON body, topic), FIFO
  per-group ordering, handler timeouts, middleware, pause/resume.
- Failure handling: retry policies (visibility or requeue strategies, backoff), handler actions
  (`Retry`, `Requeue`, `Reject`, `Discard`), dead-letter forwarding with metadata, max-attempt limits,
  decode/unroutable policies.
- Graceful shutdown with drain/cancel timeouts, progress events, release of unfinished messages and
  protection against orphaned long polls; SIGINT/SIGTERM handling.
- Declarative provisioning of topics, queues (with dead-letter queues), subscriptions (with queue
  policies) and payload buckets: lookup, verify, create, update; drift detection; failure modes
  raise/log/ignore/fallback; KMS settings.
- Events with per-kind logging configuration and sync/async hooks; OpenTelemetry tracing, metrics
  and configurable context propagation (message attributes, CloudEvents envelope, AWS X-Ray header);
  on-demand metrics.
- Transactional outbox relay, idempotency middleware, in-memory broker for tests, `fanferro` CLI
  (`guide`, `events`, `attributes`, `version`).
- abi3 wheels for CPython 3.12+ on Linux x86_64/aarch64 (manylinux2014 and musllinux_1_2), macOS
  (arm64, x86_64) and Windows (x64; arm64 best effort), plus the sdist.
- Usage guide for AI agents shipped in the wheel (`fanferro guide`) and a copyable agent skill.
