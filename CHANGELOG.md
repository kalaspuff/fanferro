# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/) (0.x: minor versions may contain breaking changes).
Entries are generated from conventional commits with [git-cliff](https://git-cliff.org)
(`cliff.toml`) by the release workflow; edit the generated text if it needs polishing.

## [Unreleased]

### Features

- **memory:** `MemoryBroker(orphaned_polls=True)` serves long polls that a stopping consumer
  aborted until their wait time ends, as SQS does: a message sent meanwhile stays invisible for its
  visibility timeout. Off by default; consumers keep the messages they release while stopping out
  of such polls on any backend that serves them (#155)

### Bug fixes

- **memory:** The in-memory broker refuses queue attributes and names that SQS refuses, with SQS's
  error codes and texts: out-of-range values, unknown attribute names, FIFO attributes of standard
  queues, `FifoQueue` on an existing queue, a per-message-group throughput limit without the
  message group deduplication scope; queues encrypted with a KMS key report
  `SqsManagedSseEnabled=false` (#146)
- **memory:** Receipt handles of the in-memory broker behave like SQS's: on standard queues the
  handle of an earlier receive still deletes the message or changes its visibility, and a message
  that is visible again can be hidden with its latest handle; FIFO queues refuse earlier handles
  with "The receipt handle has expired"; visibility changes past 12 hours after the receive and for
  deleted messages fail with SQS's codes and texts (#147)
- SQS's `QueueDeletedRecently` (creating a queue within 60 seconds of deleting one of the same
  name) and `PurgeQueueInProgress` (purging a queue twice within 60 seconds) are retryable
  `ThrottledError`s instead of invalid requests; the in-memory broker refuses both for 60 seconds,
  like SQS (#148)
- **memory:** `Number` attributes come back from the in-memory broker without leading and trailing
  zeros (`1.50` as `1.5`), with the attributes' MD5 of the values as returned, as from SQS (#149)
- **memory:** Errors of the in-memory broker carry what AWS sends: status 400 for a missing queue,
  the service's own text in failed batch entries (`EmptyValue` for an empty body), and AWS's texts
  for too large messages, invalid characters and a missing payload behind a presigned URL (#150)
- FIFO group ids, deduplication ids and message ids used as deduplication ids must be 1-128 ASCII
  letters, digits and punctuation, as SQS and SNS require: others raise `ValidationError` before
  sending instead of failing at AWS (and passing on the in-memory broker) (#151)
- **memory:** SNS topics and subscriptions of the in-memory broker are validated and reported like
  SNS's: the code `InvalidParameter` with SNS's texts (also for the filter policies SNS refuses), FIFO
  attributes and invalid `SignatureVersion` values of standard topics refused, topic ARNs of other
  regions refused, and the default `Policy`, `EffectiveDeliveryPolicy`, `SubscriptionPrincipal` and
  `FilterPolicyScope` reported (#152)
- Provisioning a subscription to a queue encrypted with the AWS managed key `alias/aws/sqs` emits a
  `config.warning`: SNS cannot use that key and drops the messages silently. The in-memory broker
  drops such deliveries too, and gives deliveries of topics with active X-Ray tracing a parent of
  SNS's own, as SNS does (#153)
- **payloads:** Provisioning a payload bucket with credentials S3 does not know fails with
  `AccessDeniedError` instead of reporting the bucket as found (S3 answers `HeadBucket` with 403 for
  both) (#154)

## [0.6.0] - 2026-10-31

### Behaviour changes

- With `auto_ack=False`, a message the handler leaves unsettled stops being extended by heartbeats
  when the handler returns, as documented, and comes back after its visibility timeout. It was
  released at once when its delivery was garbage-collected (and redelivered in a loop), or extended
  for up to 12 hours while the application kept a reference to it. Settle such a message before
  its visibility timeout runs out, or extend it with `message.extend()`.
- Dicts keyed by members of `(str, Enum)` or `(int, Enum)` classes are encoded with the members'
  values as keys (`{"dark": 3}`) instead of their names (`{"Shade.DARK": 3}`), as for other enums;
  the dataclass codec reads both.
- `OutboxRelay` gives up a record after `max_attempts` (10) attempts that failed with an error
  retrying cannot fix, through the new `OutboxStore.mark_abandoned()`, instead of retrying it
  forever ahead of every record behind it. Implement `mark_abandoned` in your store (keep the
  record, never claim it again); without it the relay warns and keeps retrying, as before.
- Offloaded payloads are stored under `{prefix}{message_id}/{sha256 of the body}` instead of
  `{prefix}{message_id}`, so that publishing a message again with another body cannot replace the
  payload an earlier copy points to. Consumers read the key from the pointer, so messages published
  before the change stay readable; S3 lifecycle rules on the prefix keep working.

### Bug fixes

- **payloads:** Payload URL downloads no longer follow redirects, which could lead a crafted pointer
  past `allowed_url_hosts` (#58)
- Forked processes no longer hang in `fork()` while awaitables complete (#59)
- **consumer:** Unsettled messages are left to time out with `auto_ack=False` or
  `release_unfinished=False` instead of being released at once (#60)
- **payloads:** An S3 download that breaks off is retried instead of dead-lettered (#61)
- **codecs:** Pydantic models with field aliases decode their own messages (#62)
- **consumer:** Stopping hides released messages only while an aborted long poll can take them (a
  busy consumer on SQS hid them for about 20 s) (#63)
- **consumer:** Idle consumers with a `wait_time` under a second no longer poll SQS
  continuously (#64)
- **provisioning:** `Provisioning.assume()` makes no API calls for subscriptions either (#65)
- Missing AWS credentials raise `AccessDeniedError` instead of a retryable network error (#66)
- **provisioning:** Queue policies that only allow other accounts or topics get the subscription's
  permission (#67)
- **payloads:** Messages over a destination's size limit are offloaded, also below the
  threshold (#68)
- **codecs:** The dataclass codec decodes type aliases, generic dataclasses and non-string dict
  keys (#69)
- **envelopes:** Raw messages whose JSON body is a CloudEvent are no longer unwrapped as one (#70)
- A publish cancelled while its destination is looked up no longer makes the next ones fail (#71)
- **routing:** `==` and `is_in()` match fractional `Number` attributes against float literals (#72)
- **consumer:** `pause()` also stops a poller that waits for a free slot (#106)
- **consumer:** Heartbeats no longer cut short an explicit `message.extend()` (#107)
- **consumer:** Messages can be dead-lettered to a queue of the other type (FIFO or standard) (#108)
- **provisioning:** Subscriptions can move a nested filter policy to the message body scope (#109)
- **provisioning:** A topic declared by the ARN of another account or region is never created in the
  client's (#110)
- **consumer:** FIFO messages handed back unprocessed keep their retry budget (#112)
- **outbox:** The relay gives up records that can never be published instead of stalling behind
  them (#113)
- **codecs:** Containers of dataclasses and msgspec structs decode into their item types (#114)
- **payloads:** Publishing a message again with another body no longer breaks its offloaded
  payload (#115)
- **payloads:** A large message's attributes are validated before its payload is uploaded (#116)
- ARNs, assumed queue URLs and presigned URL hosts follow every AWS partition (#117)
- **routing:** `m.topic() !=` and `.is_in()` accept topic names like `==` (#118)
- **outbox:** Prepared messages for a queue named like a topic use the queue's large payload
  settings (#121)
- **outbox:** Prepared message bodies with control characters such as NUL are stored as base64, so
  that a `jsonb` outbox column takes them (#122)
- **events:** Awaitables returned by sync event hooks (a lambda calling an async function) are
  awaited (#123)
- **routing:** `functools.partial` handlers are accepted (#124)
- **consumer:** A negative or NaN retry delay raises `ValueError` before the message is marked
  settled, so that it can still be settled (#125)
- Declaring a topic or queue again with an equal envelope or codec (`CloudEvents()`, `JsonCodec()`)
  returns it instead of raising `ConfigurationError` (#126)
- **provisioning:** `create_bucket` of a topic's or queue's own `LargePayloads` creates its bucket (#127)
- **envelopes:** CloudEvents JSON keeps a JSON `null` payload instead of turning it into an empty
  body (#128)
- **payloads:** Legacy AWS extended client pointers (`SQSLargePayloadSize`, `MessageS3Pointer`) are read,
  and `SQSLargePayloadSize` is reserved when publishing (#129)
- The in-memory broker deduplicates FIFO messages per message group when the queue's
  `DeduplicationScope` is `messageGroup`, as SQS does (#130)
- The in-memory broker wakes a waiting long poll when a visibility change makes a message visible
  sooner, so a retry after 2 s arrives after 2 s instead of when the poll ends, as on SQS (#133)
- The in-memory broker refuses a `RedrivePolicy` SQS refuses (a missing target or one of the other
  queue type, a `maxReceiveCount` outside 1-1000), keeps a message in its queue when the dead-letter
  queue cannot take it, moves FIFO messages without deduplicating them, and keeps their receive
  count, so a consumer of the dead-letter queue sees the same attempt as on SQS (#134)
- Subscribing a FIFO queue to a standard topic raises `ConfigurationError` when it is declared, and
  the in-memory broker refuses it like SNS, instead of failing only when provisioning on AWS (#135)
- **consumer:** `DeadLetterPolicy(on_exhausted="retry")` runs the handler again for messages
  received past `max_attempts` instead of retrying them forever without processing them (#136)
- **consumer:** A FIFO message group no longer stays stuck after a consumer is stopped and started
  again; with `ShutdownPolicy(release_prefetched=False)`, messages still buffered once the drain
  deadline has passed are released (or left with `release_unfinished=False`) instead of being
  scheduled and cancelled (#137)
- **codecs:** The dataclass codec decodes the `NamedTuple`, `TypedDict`, enum `Literal` and bare
  `tuple`/`set` fields it writes, instead of dead-lettering such messages (#138)
- Batch publishes to SNS topics send the X-Ray trace header (`X-Amzn-Trace-Id`) when their entries
  share it, instead of leaving it out; `Client(split_batches_by_trace_header=True)` sends entries
  with different headers in separate calls. The in-memory broker keeps a batch's trace header only
  when its entries share it, like SNS (#139)
- **consumer:** A retry or a release waits for a heartbeat extension of the message that is in
  flight, which could otherwise reach SQS after it and hide the message for another visibility
  timeout (#140)
- **consumer:** `delete_offloaded_payloads` acks a copy whose payload was already deleted (#142)

### Features

- **payloads:** `Client(bucket_prefix=...)` prefixes the buckets of large payloads per environment (#132)

### Documentation

- Agents that can push open their own pull requests (#56)
- Changelog entries for the bug fixes since 0.5.0 (#119)

### Tests

- The immediate-stop tests no longer fail on slow CI runners (#105)

## [0.5.0] - 2026-10-17

### Behaviour changes

- Visibility timeouts and delays are rounded up to whole seconds, as SQS counts them: a visibility
  timeout of 0.5 s is 1 s instead of 0, and a retry after 0.7 s waits 1 s instead of none. The
  in-memory broker rounds them the same way, so sub-second timeouts behave in tests as in
  production.
- `consumer.stop()` called while a stop is in progress waits for it instead of returning at once,
  and `client.close()` stops its consumers concurrently instead of one after another.
- Declaring a queue or topic again with the same settings returns it instead of raising, and
  declaring a subscription again with different settings raises `ConfigurationError` instead of
  silently keeping the first declaration.
- On FIFO queues, the next message of a message group starts once the automatic ack of the
  previous one is confirmed, which adds a delete round trip per message of a group. A message
  whose delete failed is redelivered before the group's later messages instead of after them.
  `client.consumer(queue, strict_ordering=False)` acks in the background as before.
- Presigned URLs of offloaded payloads (`LargePayloads(reference="url" | "both")`) are valid for
  24 hours instead of 1 hour by default, so messages that wait in a queue for a while can still be
  fetched; `presign_ttl=timedelta(hours=1)` keeps the old lifetime.
- AWS calls fail (and are retried) when their response does not start within 10 seconds
  (`Timeouts(read=10)`), instead of waiting for as long as a dead connection stays open: a stuck
  delete no longer stalls a consumer, a stuck publish no longer hangs its caller. Long polls get
  at least their wait time plus 15 seconds, requests with large bodies one more second per 64 KiB
  uploaded; `Timeouts(read=None)` turns it off.
- The in-memory broker evaluates filter policies like SNS: it refuses a policy SNS would refuse
  when the subscription is created (`ProvisioningError`), and values match by type, so a numeric
  condition no longer matches a `String` attribute that holds a number, nor a string condition a
  `Number` attribute.

### Features

- **memory:** The in-memory broker filters messages like SNS (#46) (#46)

### Bug fixes

- **idempotency:** Explicit retries are no longer skipped as duplicates (#25) (#25)
- **idempotency:** The in-memory store forgets expired keys (#26) (#26)
- **publish:** FIFO batches keep their order when failed entries are retried (#27) (#27)
- **consumer:** A FIFO group's redelivery is no longer released again (#42) (#42)
- **consumer:** FIFO message groups keep their order when a delete fails (#43) (#43)
- **payloads:** Presigned URLs are valid for 24 hours by default (#44) (#44)
- AWS calls time out when no response starts within 10 seconds (#45) (#45)

### Documentation

- Highlights, design and performance sections in the README (#41) (#41)
- A shorter, easier README (#48) (#48)
- The fanferro logo in the README (#49) (#49)

## [0.4.0] - 2026-10-16

### Faster publishing and consuming

Awaitables, publishing and consuming cost much less per message (`scripts/benchmark.py` measures
it). Behaviour is unchanged, except:

- Each event loop that awaits fanferro operations holds a socket pair (two file descriptors) until
  the loop is closed; delivering results no longer starts threads.
- A panic in the Rust core raises `FanferroError` instead of `pyo3_async_runtimes.RustPanic`.
- `publish_batch()` and `send_batch()` send up to eight batch calls at once to standard queues and
  topics; FIFO destinations still get one call after another.

### Performance

- **consumer:** Pipelined settlement batches and fewer wake-ups (#15) (#15)
- **memory:** Indexed queues in the memory broker (#16) (#16)
- A leaner bridge between the Rust runtime and asyncio (#17) (#17)
- **codecs:** Faster JSON and dataclass codecs (#18) (#18)
- **consumer:** Less work per message on the event loop (#19) (#19)
- **publish:** A lighter publish path and concurrent batch calls (#20) (#20)
- **telemetry:** No span work without an OpenTelemetry SDK (#21) (#21)
- Build the HTTP client for presigned URLs on first use (#22) (#22)

### Tests

- A benchmark of publishing and consuming throughput (#14) (#14)

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
