---
name: fanferro-backend-operation
description: Add an AWS operation (SQS, SNS, S3, STS) to fanferro's backend ports and implement it in both the AWS SDK adapter and the in-memory broker, with error classification and tests. Use when the core needs a new AWS API call.
---

# Adding a backend operation

All AWS access goes through the traits in `crates/fanferro-core/src/backend/mod.rs`
(`SqsApi`, `SnsApi`, `ObjectStoreApi`, `HttpFetch`, `IdentityApi`).

1. **Trait**: add an async method using fanferro types (`StringMap`, `MessageAttributes`, request
   structs). Keep SDK types out of the trait.
2. **AWS adapter** (`backend/aws/{sqs,sns,s3}.rs`): call the SDK, convert types, map errors with
   `map_sdk_error("sqs", "OperationName", err)`. If a new error code matters, extend `classify`
   in `backend/aws/errors.rs` (+ its unit test). Long polls need timeout overrides (see `receive_messages`).
3. **Memory adapter** (`backend/memory/mod.rs`): implement realistic semantics and the same error
   codes/kinds AWS returns (`aws_error(...)` helpers). Mutate state only under the lock, never across `.await`.
4. **Tests**: `backend/memory/tests.rs` for semantics; `crates/fanferro-core/tests/aws_emulator.rs`
   or a Python e2e test for the real protocol (run with MiniStack: see the e2e skill).
5. **Use it** from the core module that needs it; expose to Python only if applications need it
   (binding + `_core.pyi`).
6. **IAM**: add the action to the permissions table in `docs/guides/provisioning.md`.
