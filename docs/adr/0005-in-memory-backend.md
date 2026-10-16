# 0005: An in-memory backend implements the same ports as AWS

Status: Accepted

## Context

The consumer engine's correctness depends on timing (visibility, heartbeats, redrive, FIFO
groups). Testing only against an emulator is slow and makes deterministic time impossible. Users
also need fast tests for their handlers and flows.

## Decision

* All AWS access goes through backend traits (`SqsApi`, `SnsApi`, `ObjectStoreApi`, `HttpFetch`,
  `IdentityApi`). The AWS SDK adapter and an in-memory broker implement them.
* The in-memory broker models visibility timeouts, receive counts, long polling, delays, redrive,
  FIFO groups and deduplication, SNS fan-out (raw and notification JSON), filter policies (every
  operator and limit of SNS, validated when they are set) and presigned URLs. It uses
  `tokio::time`, so Rust tests run on paused, deterministic time.
* `fanferro.testing.memory_client()` exposes it to users.

## Consequences

* Most tests are fast and deterministic; end-to-end tests against MiniStack (ADR 0006) verify the
  AWS protocol mapping.
* New backend operations must be implemented twice (tests for both).
