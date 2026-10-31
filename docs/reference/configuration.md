# Configuration reference

Every option of the public configuration surface. `scripts/check_consistency.py` verifies that
each parameter and field listed here exists in the code and that every one in the code is listed.

## `Client(...)`

| Parameter | Default | Description |
| --- | --- | --- |
| `region` | provider chain | AWS region |
| `endpoint_url` | `None` | Custom endpoint (MiniStack: `http://localhost:4566`) |
| `profile` | `None` | Named AWS profile |
| `credentials` | provider chain | `Credentials(access_key_id, secret_access_key, session_token)`; see [Deployment → Credentials and regions](../guides/deployment.md#credentials-and-regions) |
| `account_id` | STS lookup | Account id used to derive topic ARNs and assumed queue URLs |
| `queue_prefix` | `""` | Prepended to every queue name, dead-letter queues included (e.g. `"staging-"`); see [Deployment → Several environments in one AWS account](../guides/deployment.md#several-environments-in-one-aws-account) |
| `topic_prefix` | `""` | Prepended to every topic name |
| `bucket_prefix` | `""` | Prepended to the bucket of every `LargePayloads`, the client's and those of topics and queues (lowercase letters, digits, `-`, `.`) |
| `source` | `"fanferro"` | CloudEvents `source` of published messages (e.g. `urn:service:orders`) |
| `envelope` | `Raw()` | Envelope for publishing (`CloudEvents()` for CloudEvents) |
| `accept_envelopes` | CloudEvents, CloudEventsProtobuf, Raw | Envelopes consumers try, in order |
| `codecs` | `CodecRegistry()` | Codec registry or list of codecs |
| `naming` | class name | Fallback `(cls) -> str` for message type names |
| `large_payloads` | `None` | `LargePayloads(...)` claim-check offloading |
| `payload_fetch` | `PayloadFetch()` | How consumers download offloaded payloads |
| `telemetry` | `Telemetry()` | OpenTelemetry settings |
| `logging` | `LoggingConfig()` | Event logging |
| `provisioning` | `Provisioning.existing()` | Default provisioning policy |
| `aws_retry` | `AwsRetry()` | SDK retries per AWS call |
| `timeouts` | `Timeouts()` | AWS call timeouts |
| `publish_retry_attempts` | `4` | Attempts for failed batch entries and checksum mismatches |
| `verify_md5` | `False` | Verify SQS MD5 digests of sent and received messages (standard queues) |
| `split_batches_by_trace_header` | `False` | Batch publishes to topics: one `PublishBatch` call per X-Ray trace header among the entries, so that each keeps its `X-Amzn-Trace-Id` (SNS takes one per call); off, a call with different headers sends none |
| `publish_middleware` | `()` | Publish middleware |
| `consume_middleware` | `()` | Consume middleware applied to every consumer |
| `broker` | `None` | `MemoryBroker` to use instead of AWS |
| `event_buffer` | `10000` | Buffered core events before dropping |
| `app_name` | `"fanferro"` | Application name in the AWS user agent |
| `auto_provision` | `True` | Provision declared resources when entering `async with client` |

## `client.topic(...)`

| Parameter | Default | Description |
| --- | --- | --- |
| `name` | - | Topic name without the prefix (`.fifo` suffix for FIFO) |
| `arn` | derived | Topic ARN (never prefixed) |
| `prefixed` | `True` | Prepend the client's `topic_prefix`; `False` for a topic shared by environments |
| `attributes` | `None` | `TopicAttributes(...)` or raw AWS attributes |
| `tags` | `None` | Tags |
| `provisioning` | client default | Provisioning policy |
| `envelope` | client default | Envelope for this topic |
| `large_payloads` | client default | Claim-check settings (`None` disables) |
| `group_id` | `None` | Default FIFO group id or `(OutgoingMessage) -> str` |
| `codec` | automatic | Codec or codec name |

## `client.queue(...)`

| Parameter | Default | Description |
| --- | --- | --- |
| `name` | - | Queue name without the prefix (`.fifo` suffix for FIFO) |
| `url` | looked up | Queue URL (never prefixed) |
| `owner_account_id` | own account | Owner of a cross-account queue |
| `prefixed` | `True` | Prepend the client's `queue_prefix`; `False` for a queue shared by environments |
| `attributes` | `None` | `QueueAttributes(...)` or raw AWS attributes |
| `tags` | `None` | Tags |
| `dead_letter` | `None` | `DeadLetterQueue(name, max_receive_count, attributes, tags, provisioning, prefixed)` |
| `provisioning` | client default | Provisioning policy |
| `envelope` | client default | Envelope for messages sent to this queue |
| `large_payloads` | client default | Claim-check settings (`None` disables) |
| `group_id` | `None` | Default FIFO group id or `(OutgoingMessage) -> str` |
| `codec` | automatic | Codec or codec name |

## `client.subscription(topic, queue, ...)`

| Parameter | Default | Description |
| --- | --- | --- |
| `raw_message_delivery` | `True` | Deliver the original message and attributes |
| `filter_policy` | `None` | SNS filter policy |
| `filter_policy_scope` | `"MessageAttributes"` | Or `"MessageBody"` |
| `dead_letter_queue` | `None` | Queue (or name) receiving undeliverable messages |
| `manage_queue_policy` | `True` | Allow the topic to send to the queue (and its DLQ) |
| `provisioning` | client default | Provisioning policy |

## `client.consumer(queue, handler_or_router, ...)`

Every option in seconds (`receive_batch_wait`, `wait_time`, `visibility_timeout`, `handler_timeout`,
`settle_linger`) also takes a `timedelta`; out-of-range values raise `ValueError` when the consumer is
created.

| Parameter | Default | Description |
| --- | --- | --- |
| `name` | queue name | Name used in events and metrics |
| `concurrency` | `10` | Handlers running at the same time |
| `prefetch` | `min(batch_size, concurrency) × pollers` | Extra messages leased ahead (a receive batch per poller; by default each extra receive loop adds one batch while it runs) |
| `batch_size` | `10` | Messages per receive call (1-10) |
| `min_receive_batch` | `batch_size` (FIFO: 1) | Free slots a poller waits for before receiving (1 to `batch_size`) |
| `receive_batch_wait` | `0.1` | Seconds a poller waits for `min_receive_batch` free slots before receiving fewer |
| `wait_time` | `20` | Long polling wait in seconds (0-20; fractions round up; `0` short-polls, pausing a second while the queue is empty) |
| `pollers` | `1` | Receive loops that always run |
| `max_pollers` | `None` | Most receive loops at once (`pollers` to 64; `None`: one per full batch of `concurrency + prefetch`); extra loops run while receives come back full and a batch of slots is free |
| `rate_limit` | `None` | `RateLimit(per_second, burst)` or messages per second |
| `visibility_timeout` | queue attribute | Visibility timeout in seconds applied on receive (0-43200) |
| `heartbeat` | `True` | `Heartbeat(...)`, `True` (defaults) or `False` |
| `retry` | `RetryPolicy()` | Retry policy |
| `dead_letter` | `DeadLetterPolicy()` | Dead-letter policy |
| `shutdown` | `ShutdownPolicy()` | Shutdown policy |
| `handler_timeout` | `None` | Cancel handlers after N seconds (positive) |
| `ordering` | `"auto"` | `"fifo"` (per-group sequencing), `"none"`, or auto (FIFO queues) |
| `strict_ordering` | `True` | A group's next message waits until the automatic ack of the previous one is confirmed (`False`: background acks; a failed delete can reorder the group) |
| `envelopes` | client default | Envelopes to try |
| `middleware` | `()` | Consume middleware for this consumer |
| `auto_ack` | `True` | Ack when the handler returns |
| `settle_linger` | `0.005` | Seconds a batch of acks/retries waits to fill up (0-1; full batches of 10 go at once) |
| `sns_unwrap` | `True` | Unwrap SNS notification JSON |
| `auto_confirm_subscriptions` | `False` | Confirm SNS subscription confirmation messages |
| `delete_offloaded_payloads` | `False` | Delete S3 payload objects after acking; a message whose object is missing (`NoSuchKey`) is then acked as an already processed duplicate (`message.duplicate`), including one whose object expired or was deleted for another reason (see [large payloads](../guides/large-payloads.md#cleanup)) |

## Policies

`RetryPolicy`: `max_attempts` (5), `backoff` (`Backoff()`), `strategy` (`"visibility"`),
`retry_on` (`(Exception,)`), `dead_letter_on` (`()`), `classify` (`None`), `dead_letter_unknown` (`False`).

`Backoff`: `initial` (1.0), `maximum` (300.0), `multiplier` (2.0), `jitter` (`True`);
constructors `exponential()`, `constant()`, `immediate()`.

`DeadLetterPolicy`: `queue` (`None`; a queue name, prefixed like the others), `on_exhausted` /
`on_decode_error` / `on_unroutable` (`"dead_letter"`; or `"retry"`, `"discard"`, `"leave"`),
`on_missing_queue` (`"leave"`), `include_error` (`True`).

`DeadLetterQueue`: `name` (without the prefix), `max_receive_count` (5), `attributes` (14 days of
retention), `tags` (`{}`), `provisioning` (the queue's), `prefixed` (`True`: the client's
`queue_prefix` applies). Queues may share one; its first declaration sets its attributes.

`ShutdownPolicy`: `drain_timeout` (30.0; `None` = wait forever), `cancel_timeout` (10.0),
`progress_interval` (5.0), `release_unfinished` (`True`), `release_prefetched` (`True`),
`abort_receives` (`True`).

`Heartbeat`: `interval`, `extend_by`, `max_duration` (defaults derived from the visibility timeout;
at most 12 hours).

`RateLimit`: `per_second`, `burst`.

Durations in the policies (`Backoff`'s delays, `ShutdownPolicy`'s timeouts, `Heartbeat`), in
`Retry(delay=...)`, `Requeue(delay=...)`, `message.retry()`, `message.requeue()`,
`message.extend()`, `IdempotencyMiddleware` and a publish's `delay` are seconds or a `timedelta`;
the policies and `OutgoingMessage` keep them as float seconds.

`Provisioning`: `lookup` (`True`), `verify` (`True`), `create` (`False`), `update` (`False`),
`on_missing` (`"raise"`), `on_drift` (`"log"`), `on_error` (`"raise"`); presets `existing()`,
`create_missing()`, `ensure()`, `assume()`.

`LargePayloads`: `bucket`, `prefix` (`"fanferro/"`), `threshold` (262144), `always` (`False`),
`reference` (`"s3"`), `presign_ttl` (24 hours), `format` (`"fanferro"`), `encryption` (`None`),
`bucket_key` (`True`), `storage_class` (`None`), `create_bucket` (`False`), `prefixed` (`True`:
the client's `bucket_prefix` applies; `False` for a bucket shared by environments).

`PayloadFetch`: `max_bytes` (64 MiB), `prefer` (`"s3"`), `allowed_url_hosts` (AWS + endpoint),
`allow_http` (`False`), `verify_checksum` (`True`).

`Telemetry`: `tracing` (`True`), `metrics` (`True`), `propagation` (`Propagation()`),
`consumer_span_parent` (`"child"`), `tracer_provider`, `meter_provider`.

`Propagation`: `inject` (message attributes, AWS trace header), `extract` (message attributes,
envelope, AWS trace header), `attribute_keys` (`traceparent`, `tracestate`), `propagator`.

`LoggingConfig`: `enabled` (`True`), `logger` (`"fanferro"`), `levels` (`{}`), `disabled` (`()`),
`include_fields` (`True`), `include_tracebacks` (`True`), `formatter` (`None`).

`AwsRetry`: `max_attempts` (5), `mode` (`"standard"`), `initial_backoff` (0.2), `max_backoff` (20.0).

`Timeouts`: `connect` (3.1), `read` (10.0: until the response starts; long polls get at least
their wait time plus 15 s, large uploads one more second per 64 KiB), `operation`, `attempt`
(`None`: none).

## Environment variables

| Variable | Effect |
| --- | --- |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | Region when `region=` is not passed |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | Credentials when `credentials=` is not passed (take precedence over profiles) |
| `AWS_PROFILE` | Profile when `profile=` is not passed |
| `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_ARN`, `AWS_CONTAINER_CREDENTIALS_*` | Web identity (EKS IRSA), ECS task role and EKS Pod Identity credentials |
| `FANFERRO_WORKER_THREADS` | Tokio worker threads of the native runtime (default 2-4) |
| `FANFERRO_TEST_ENDPOINT_URL` | Test suite: use this emulator instead of starting MiniStack |
