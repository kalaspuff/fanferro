# 0006: MiniStack as the end-to-end test target

Status: Accepted

## Context

End-to-end tests must exercise the real AWS wire protocols (SQS JSON, SNS query, S3, STS). Since
March 2026 the LocalStack image requires an auth token; MiniStack is a free, MIT-licensed,
pip-installable emulator that runs without Docker.

## Decision

* `ministack` is a test dependency. The pytest fixture starts it on a free port for the session
  unless `FANFERRO_TEST_ENDPOINT_URL` points at an emulator (or anything AWS-compatible).
* Integration scenarios are parametrized over the in-memory backend and MiniStack; MiniStack-only
  tests (`tests/e2e`, `crates/fanferro-core/tests/aws_emulator.rs`) cover interoperability.

## Consequences

* `uv run pytest` runs the full suite locally and in CI with no Docker requirement.
* Behaviour that MiniStack does not emulate (IAM enforcement, KMS key policies) is covered by unit
  tests of the request mapping, not end to end.
