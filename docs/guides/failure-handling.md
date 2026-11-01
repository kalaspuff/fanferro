# Failure handling

## Decision order

When a handler raises:

1. **Handler actions** decide explicitly: `fanferro.Retry(delay=...)`, `fanferro.Requeue(delay=...)`,
   `fanferro.Reject(reason)` (dead-letter now), `fanferro.Discard(reason)` (delete, no retries).
   A delay is a non-negative number of seconds: `Retry(delay=-1)` and `message.retry(delay=-1)`
   raise `ValueError` and leave the message unsettled.
2. `RetryPolicy.classify(error)` (if set) may return `"retry"`, `"dead_letter"` or `"discard"`.
   A `classify` that raises is reported as a `consumer.error` event (with its own exception), and
   the next steps decide as if it had returned `None`.
3. `RetryPolicy.dead_letter_on` exceptions are dead-lettered immediately.
4. Otherwise the message is retried, unless the attempt number reached `max_attempts`, in which
   case `DeadLetterPolicy.on_exhausted` applies (default: dead-letter). `on_exhausted="retry"`
   keeps retrying past `max_attempts`, with the backoff of each attempt, until the handler succeeds
   (or SQS redrive moves the message).

A message whose processing fails outside these steps (a bug in fanferro or in a consume middleware's
failure path, reported as `consumer.error`) is retried after the backoff of its attempt, so that
`max_attempts` and the dead-letter policy still apply; it is not left leased.

```python
consumer = client.consumer(
    queue,
    retry=fanferro.RetryPolicy(
        max_attempts=5,
        backoff=fanferro.Backoff.exponential(initial=1, maximum=300),   # jittered
        strategy="visibility",              # or "requeue"
        dead_letter_on=(ValidationError, PermissionError),
    ),
    dead_letter=fanferro.DeadLetterPolicy(on_unroutable="dead_letter", on_decode_error="dead_letter"),
)
```

## Retry strategies

* `"visibility"` (default): `ChangeMessageVisibility` to the backoff delay. Cheap; the SQS receive
  count keeps growing; delays up to 12 hours; a FIFO group is blocked while its message waits.
* `"requeue"`: send a copy with `fanferro.attempt` incremented and delete the original. Resets the SQS
  receive count (a queue `RedrivePolicy` never cuts the sequence short) and supports delays up to
  15 minutes on standard queues. A crash between send and delete can duplicate the message. On a
  FIFO queue the copy goes behind the rest of its message group, which loses its order (consumers
  emit a `config.warning`); keep `"visibility"` there.

`message.attempt` counts processing attempts across both (SQS receives plus requeues; not the
receives of FIFO messages handed back unprocessed, see [FIFO queues](consuming.md#fifo-queues)).

## Dead-letter queues

Declare one with the queue:

```python
queue = client.queue("orders", dead_letter=fanferro.DeadLetterQueue("orders-dlq", max_receive_count=10))
```

fanferro provisions the DLQ first and sets the queue's `RedrivePolicy`. When fanferro dead-letters a
message it **forwards** it (original body and attributes, so SQS "redrive to source" works) and
adds `fanferro.dead-letter`: a JSON document with `reason`, `source`, `queue`, `sqs_message_id`,
`receive_count`, `attempt`, `message_type`, `message_id`, `error_type`, `error_message` and `time`.
Consuming the DLQ, `message.dead_letter_info` reads it as a `fanferro.DeadLetterInfo`
(`info.reason`, `info.attempt`, `info.time` as a `datetime`; `info.document` is the whole document).

Keep `max_receive_count` above `RetryPolicy.max_attempts` so SQS redrive is only the safety net (fanferro warns otherwise).

The consumer finds the DLQ from `DeadLetterPolicy(queue=...)`, the queue's `DeadLetterQueue`
declaration, or the queue's `RedrivePolicy`. Without one, `on_missing_queue` applies
(`"leave"`: retry with maximum backoff; `"retry"`; `"discard"`).

## Messages that never reach a handler

| Situation | Action (configurable) | Event |
| --- | --- | --- |
| Envelope/transport/codec cannot decode | `on_decode_error` (dead-letter) | `message.decode_failed` |
| Transient decode failure (S3 download error) | retried with backoff | `message.decode_failed` |
| No route matches | `on_unroutable` (dead-letter) | `message.unroutable` |
| Attempt > `max_attempts` on receipt | `on_exhausted` (dead-letter; `"retry"` runs the handler) | `message.dead_lettered` |
| Idempotency: already processed | acked | `message.duplicate` |
| Idempotency: claimed elsewhere (in progress, or its worker crashed) | postponed by `retry_delay`, not counted as an attempt | `message.requeued` (FIFO: `message.retry_scheduled`) |
| Offloaded payload missing, with `delete_offloaded_payloads=True` | acked as already processed | `message.duplicate` |

## Poison messages and redrive

Inspect a DLQ with a consumer (`queue.dead_letter_queue`) or the AWS console. After fixing the
cause, use SQS "Start DLQ redrive" (`StartMessageMoveTask`): forwarded messages are byte-identical
to the originals plus the metadata attribute.

## Robustness built in

* Every AWS call is retried by the AWS SDK (standard mode, jittered exponential backoff;
  `AwsRetry`) and has timeouts (`Timeouts`): a call whose response does not start within 10 s
  (a dead connection) fails and is retried, instead of waiting for as long as the connection
  stays open. Long polls get at least their wait time plus 15 s, large uploads more time per
  64 KiB.
* Failed batch entries are retried individually; MD5 digests of sent and received messages are
  verified (standard queues).
* The receive loop backs off on errors (1 s → 30 s) and recovers by itself (`receive.failed`).
* Settlement (deletes, visibility changes) is batched and retried; failures emit
  `message.ack_failed` (the message will be redelivered - handlers must be idempotent).
* Publishing uses stable message ids, so retries are deduplicated on FIFO destinations and by
  idempotent consumers on standard ones.
