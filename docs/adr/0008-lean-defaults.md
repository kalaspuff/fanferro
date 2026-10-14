# 0008: Lean defaults - raw messages, opt-in extras, immediate stop

Status: Accepted (supersedes the default envelope of [0002](0002-cloudevents-default-envelope.md)
and the default `abort_receives` of [0007](0007-shutdown-and-orphaned-long-polls.md))

## Context

A benchmark of fanferro 0.1 against boto3/botocore/aiobotocore (SNS publish, SQS consume, all
against MiniStack) showed that fanferro's *defaults* were slower than a hand-tuned
configuration and sent larger requests:

* every message was a CloudEvents document with two message attributes (`content-type`,
  `fanferro.type`), parsed again on the consumer side;
* MD5 digests of every sent and received message were verified, which botocore does not do and
  which TLS makes redundant for integrity in transit;
* the consumer asked each `ReceiveMessage` for as many messages as it had free handler slots,
  often 1-4 instead of 10, and a slot stayed taken until the delete completed (fixed separately:
  full receive batches, slots freed when settlement starts, batched deletes);
* `consumer.stop()` waited for the long poll in flight (up to `wait_time`, 20 s by default).

## Decision

* **Raw envelope by default.** `Client(envelope=...)` defaults to `Raw()`: the body is the
  encoded payload (a JSON document for pydantic models, dataclasses, msgspec structs and plain
  data), the message id travels in `fanferro.id` (stable across retries; idempotency keys and FIFO
  deduplication depend on it) and the message type in `fanferro.type` (routing, SNS filter
  policies) when the message has one.
* **JSON is implied.** The raw envelope writes `content-type` only for non-JSON payloads
  (`Raw(implicit_content_type="application/json")`). Consumers assume JSON for messages that
  carry fanferro's id or type attribute but no `content-type`; messages from other producers keep
  an unknown content type, as before.
* **CloudEvents is opt-in**: `Client(envelope=fanferro.envelopes.CloudEvents())`. Consumers keep
  accepting CloudEvents JSON and protobuf (`accept_envelopes` is unchanged), detected from the
  `content-type` attribute or, without it, a `specversion` key, so plain JSON bodies are not
  parsed twice.
* **MD5 verification is opt-in**: `verify_md5=False` by default (client and consumer engine).
* **Immediate stop**: `ShutdownPolicy.abort_receives=True` by default. Running handlers still get
  `drain_timeout`; only the wait for the long poll in flight is gone.

## Consequences

* Messages are smaller, cheaper to send, receive and decode, and readable by any SQS/SNS client
  without knowledge of CloudEvents. A typed raw message uses 2 of the 10 attribute slots, an
  untyped one 1.
* The id attribute stays on by default because it is cheap: preparing a message costs about
  0.25 µs more with it (encoding a dict body as JSON costs about 3.6 µs), and publish benchmarks
  show no difference beyond their noise. `Raw(id_attribute=None)` drops it; `message.id` then
  falls back to the SQS message id, which a producer retry changes.
* Raw messages carry no `source`, `subject`, `time` (consumers fall back to the SQS sent
  timestamp) or `dataschema`; envelope headers become message attributes (`matchers.header()`
  falls back to attributes). Applications that need these choose CloudEvents explicitly.
* 0.1 consumers read 0.2 messages (typed routes, ids and filter policies work); untyped routes
  see JSON objects and arrays as before, but JSON scalars arrive as bytes there because 0.1 does
  not imply a content type. 0.2 consumers read 0.1 CloudEvents and raw messages unchanged.
* After an aborted long poll SQS may still hand that poll up to `batch_size` messages per poller;
  they stay invisible until their visibility timeout ends and are then redelivered (at least once
  holds; redelivery is delayed). Messages released during shutdown are kept invisible until the
  abandoned poll is over (≤ `wait_time` + 1 s), so the poll cannot capture them. Processes that
  stop a consumer and keep using the queue right away can choose `abort_receives=False`.
* Breaking change: released as 0.2.0 with a migration section in the changelog; 0.1 behaviour is
  one configuration away:

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
