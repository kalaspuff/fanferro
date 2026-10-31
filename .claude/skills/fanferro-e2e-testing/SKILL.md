---
name: fanferro-e2e-testing
description: Run and write fanferro end-to-end tests against MiniStack (AWS emulator, no Docker) - starting the emulator, parametrized integration tests, Rust emulator tests, debugging protocol-level differences, and comparing fanferro on real AWS, MiniStack, LocalStack and the memory broker (scripts/probe_backends.py). Use when tests involve real SQS/SNS/S3 behaviour, when a test fails only on MiniStack, or when checking parity with real AWS.
---

# End-to-end testing with MiniStack

* `uv run pytest` starts MiniStack automatically (session fixture `ministack_endpoint` in
  `tests/conftest.py`, random free port). To reuse a running instance:
  `uv run ministack &` then `FANFERRO_TEST_ENDPOINT_URL=http://localhost:4566 uv run pytest`.
* Integration tests take `client_factory` and run on `memory` and `ministack`
  (`-k memory` / `-k ministack` to select). MiniStack-only tests live in `tests/e2e/` and are
  marked `e2e`; `uv run pytest -m "not e2e"` skips all emulator tests.
* Rust: `FANFERRO_TEST_ENDPOINT_URL=http://localhost:4566 cargo test -p fanferro-core --test aws_emulator`
  (skips itself without the variable).
* Use unique names (`name("prefix")`), `wait_time=1`, short visibility timeouts (≥ 2 s) and the
  helpers in `tests/helpers.py`. Never sleep for correctness; wait for conditions.
* Inspect MiniStack behaviour by reading its source in the virtualenv
  (`.venv/lib/python*/site-packages/ministack/services/{sqs,sns,s3}.py`) - it mirrors AWS closely
  (e.g. FIFO deduplication returns the original message's MD5).
* A test that only fails on MiniStack usually means real AWS behaves the same way: check the AWS
  docs and model it in the memory broker too (example: orphaned long polls, ADR 0007).
* For interop, boto3 is a test dependency: produce/consume with boto3 to verify wire formats.

## Parity with real AWS (`scripts/probe_backends.py`)

MiniStack, LocalStack and the in-memory broker model AWS; the probe checks how closely. Each of its
scenarios drives fanferro's public API and records facts (outcomes, exception classes, error kinds,
codes, statuses and messages, what was delivered where and in which order), normalised so that runs
on different targets compare:

```bash
uv run python scripts/probe_backends.py list                          # the scenarios
uv run python scripts/probe_backends.py run --target memory           # writes probe-memory.json
uv run python scripts/probe_backends.py run --target ministack        # starts MiniStack itself
uv run python scripts/probe_backends.py run --target localstack --endpoint http://localhost:4566
uv run python scripts/probe_backends.py compare probe-aws.json probe-memory.json probe-ministack.json
```

* Real AWS needs a person with credentials of a test account: `iam-policy --account <id>` prints
  the least privilege a run needs, `run --target aws --region <region>` (with `AWS_PROFILE` or
  `--profile`) writes `probe-aws.json`, and `cleanup --target aws --region <region>` deletes what an
  interrupted run left (every `ffprobe-*` queue, topic and bucket). Result files contain no
  credentials or account ids, so they can be shared and committed to an issue.
* LocalStack needs Docker and, since 2026.03, an auth token:
  `docker run --rm -p 4566:4566 -e LOCALSTACK_AUTH_TOKEN localstack/localstack`.
* `compare` takes real AWS as the reference (first file). A difference is a finding: fix the memory
  broker (`backend/memory`) or the error classification (`backend/aws/errors.rs`) to match AWS, or
  note why fanferro must behave differently. Facts named `info.*` (timings, URLs, request ids) are
  never counted as differences.
* Add a scenario with `@scenario("area.name")`: record facts with `probe.fact(...)` and
  `await probe.outcome(key, awaitable)`, name resources with `probe.name(...)`, and wait for
  deliveries with `collect(...)`/`Collector` (on real AWS, `warm_up` waits until a new subscription
  delivers).
