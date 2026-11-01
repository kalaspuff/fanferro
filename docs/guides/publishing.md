# Publishing

```python
topic = client.topic("orders")            # SNS
queue = client.queue("jobs")              # SQS

await topic.publish(OrderPlaced(...))     # → PublishResult
await queue.send(ResizeImage(...))        # queue.send is queue.publish in SQS terms
```

## What can be published

Anything a [codec](message-formats.md) understands: pydantic models, dataclasses, `msgspec.Struct`,
protobuf messages, dicts/lists/numbers (JSON), `str` (text) and `bytes`. The message type defaults
to the class's registered name (`@fanferro.message_type(...)`), the protobuf full name, or the class
name; plain values (dict, str, bytes...) have no type unless you pass `type=`.

## Options

Every option is a field of `fanferro.OutgoingMessage`; pass them as keywords or build the object.
The keywords are typed (`fanferro.TopicPublishOptions` for topics, `fanferro.QueuePublishOptions`
for queues), so type checkers and editors complete them and report a misspelt option, `delay=` on
a topic or `sns_subject=` on a queue. Options given together with an `OutgoingMessage` raise
`TypeError`: set them on the message instead.

| Option | Meaning |
| --- | --- |
| `type` | Message type name (routing, filter policies) |
| `attributes` | User message attributes: `str` → String, numbers → Number (at most 38 significant digits; SQS returns them without leading and trailing zeros: `1.50` as `1.5`), `bytes` → Binary |
| `headers` | Envelope headers (CloudEvents extensions, lowercase alphanumeric names; message attributes with the raw envelope) |
| `id` | Message id; stable across retries; FIFO deduplication id by default (then 1-128 ASCII letters, digits and punctuation) |
| `time`, `source`, `subject`, `data_schema` | CloudEvents attributes (not carried by the raw envelope) |
| `content_type` | Override the codec's content type (for `bytes`/`str` bodies) |
| `group_id` | Message group id (required for FIFO destinations; 1-128 ASCII letters, digits and punctuation) |
| `deduplication_id` | FIFO deduplication id (defaults to `id`; 1-128 ASCII letters, digits and punctuation) |
| `delay` | Delivery delay in seconds (standard queues, ≤ 900) |
| `sns_subject` | SNS `Subject` (topics only) |
| `codec`, `envelope` | Override codec or envelope for this message |
| `trace_header` | AWS X-Ray trace header (set by telemetry; pass one to continue a trace) |

```python
await topic.publish(
    order,
    attributes={"tenant": "acme", "priority": 5},
    headers={"schemaversion": 2},
    group_id=order.customer_id,        # FIFO topic
)
```

At most **10 attributes** per message, including fanferro's (`content-type`, `fanferro.type`, and
`traceparent` when propagating trace context through attributes). Names starting with
`fanferro.` are reserved. Violations raise `ValidationError` before any network call.

## Batches

```python
result = await queue.send_batch([ResizeImage(id=i) for i in range(100)], attributes={"source": "import"})
```

Batches are split into calls of up to 10 entries (and by size). Up to eight calls are in flight at
once. On FIFO destinations the message groups are spread over up to eight lanes (one per ten
entries, a group always in the same lane), and each lane sends its calls one after another, with
the retries of a call before its next one, so that each message group keeps its order. Entries the
service reports as failed with retryable errors are retried, up to `publish_retry_attempts` in all.
A call that fails as a whole (throttling, a timeout, a 5xx) has been retried by the AWS SDK already
(`aws_retry`) and is not retried again: its error becomes the result of each of its entries, as
`publish()` raises it. If some entries still fail, `BatchPublishError` is raised after the others
were sent (`error.result.failed` lists them).
Pass `raise_on_failure=False` to inspect `result.failed`. The options of a batch apply to the plain
bodies; an `OutgoingMessage` in the batch keeps its own.

## FIFO

Destinations whose name ends in `.fifo` are FIFO. Every message needs a group id (pass `group_id=`
or declare `client.topic("x.fifo", group_id=lambda m: m.body.account_id)`). The message id is the
deduplication id unless content-based deduplication is enabled or you pass `deduplication_id`, so
publishing the same message again within five minutes is a no-op.

## Size limits

SQS accepts up to 1 MiB; SNS 256 KiB by default (topics can raise it with
`TopicAttributes(max_message_size=...)`). Larger messages fail with `MessageTooLargeError` unless
[large payload offloading](large-payloads.md) is enabled.

## Prepared messages

`await topic.prepare(obj, **options)` does everything except sending and returns a
`PreparedMessage` that serializes to JSON (`to_json()` / `from_json()`). Send it later with
`client.publish_prepared(...)`, or many with `client.publish_prepared_batch(...)`, which returns
failed entries in `result.failed` rather than raising (`raise_on_failure=False` by default, unlike
`publish_batch`: a relay settles each record by its own outcome). This is the basis of the
[transactional outbox](patterns.md#transactional-outbox).
`prepare()` checks the attributes as a publish does (count, names, values) and raises
`ValidationError` for a message that could not be sent, so it fails before it is stored; offloading
a payload to S3 takes one more attribute, which only the publish can tell.

## Middleware

```python
async def stamp_tenant(context: fanferro.PublishContext, call_next: fanferro.PrepareNext) -> fanferro.PreparedMessage:
    context.message.attributes.setdefault("tenant", current_tenant())
    return await call_next(context)

client = fanferro.Client(publish_middleware=[stamp_tenant])
```

Publish middleware wraps preparation (codec + envelope) for single publishes, batches and
`prepare()` alike.
