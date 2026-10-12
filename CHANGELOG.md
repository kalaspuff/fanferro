# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/) (0.x: minor versions may contain breaking changes).
Entries are generated from conventional commits with [git-cliff](https://git-cliff.org)
(`cliff.toml`) by the release workflow; edit the generated text if it needs polishing.

## [Unreleased]

## [0.1.0] - Unreleased

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
