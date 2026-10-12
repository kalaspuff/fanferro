# Examples

Every example runs against the in-memory broker by default (no AWS, no Docker):

```bash
uv run python -m examples.quickstart
```

Run them against MiniStack (`uv run ministack` in another terminal) or AWS:

```bash
FANFERRO_ENDPOINT_URL=http://localhost:4566 uv run python -m examples.quickstart
FANFERRO_USE_AWS=1 AWS_REGION=eu-west-1 uv run python -m examples.quickstart   # creates real resources
```

| Example | Shows |
| --- | --- |
| [`quickstart.py`](quickstart.py) | Topic → queue fan-out, filter policy, dead-letter queue, typed consumer, graceful stop |
| [`routing_versions.py`](routing_versions.py) | Routing by message type, version header, attributes and JSON body fields; default route |
| [`protobuf_binary.py`](protobuf_binary.py) | Protobuf payloads in the binary CloudEvents envelope |
| [`large_payloads.py`](large_payloads.py) | Claim checks: large bodies stored in S3 with bucket/key and presigned URL pointers |
| [`transactional_outbox.py`](transactional_outbox.py) | Transactional outbox on SQLite with `topic.prepare()` and `OutboxRelay` |
| [`idempotent_consumer.py`](idempotent_consumer.py) | Inbox pattern: deduplication record and side effects in one transaction |
| [`custom_envelope_and_codec.py`](custom_envelope_and_codec.py) | A custom envelope for a legacy format and a custom (CSV) codec |
| [`observability.py`](observability.py) | Log configuration, async event hooks, OpenTelemetry spans and on-demand metrics |
| [`fifo_ordering.py`](fifo_ordering.py) | FIFO topic → FIFO queue, per-group ordering, deduplication, derived group ids |
| [`worker_service.py`](worker_service.py) | A worker with several consumers, rate limits, retry and shutdown policies, `client.run()` |
| [`testing_handlers.py`](testing_handlers.py) | Unit-testing handlers with `build_message` and flows with the in-memory broker |

`tests/unit/test_examples.py` runs all of them, so they stay in sync with the library.
