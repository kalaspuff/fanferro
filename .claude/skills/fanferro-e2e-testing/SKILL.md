---
name: fanferro-e2e-testing
description: Run and write fanferro end-to-end tests against MiniStack (AWS emulator, no Docker) - starting the emulator, parametrized integration tests, Rust emulator tests, and debugging protocol-level differences. Use when tests involve real SQS/SNS/S3 behaviour or when a test fails only on MiniStack.
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
