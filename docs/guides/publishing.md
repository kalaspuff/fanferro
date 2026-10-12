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

Every option is a field of `fanferro.OutgoingMessage`; pass them as keywords or build the object:

| Option | Meaning |
| --- | --- |
| `type` | Message type name (routing, filter policies) |
| `attributes` | User message attributes: `str` → String, numbers → Number, `bytes` → Binary |
| `headers` | Envelope headers (CloudEvents extensions; lowercase alphanumeric names) |
| `id` | Message id; stable across retries; FIFO deduplication id by default |
| `time`, `source`, `subject`, `data_schema` | CloudEvents attributes |
| `content_type` | Override the codec's content type (for `bytes`/`str` bodies) |
| `group_id` | Message group id (required for FIFO destinations) |
| `deduplication_id` | FIFO deduplication id (defaults to `id`) |
| `delay` | Delivery delay in seconds (standard queues, ≤ 900) |
| `sns_subject` | SNS `Subject` (topics only) |
| `codec`, `envelope` | Override codec or envelope for this message |

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

Batches are split into calls of up to 10 entries (and by size). Entries that fail with retryable
errors are retried; if some still fail, `BatchPublishError` is raised after the others were sent
(`error.result.failed` lists them). Pass `raise_on_failure=False` to inspect `result.failed`.

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
`client.publish_prepared(...)`. This is the basis of the [transactional outbox](patterns.md#transactional-outbox).

## Middleware

```python
async def stamp_tenant(context: fanferro.PublishContext, call_next: fanferro.PrepareNext) -> fanferro.PreparedMessage:
    context.message.attributes.setdefault("tenant", current_tenant())
    return await call_next(context)

client = fanferro.Client(publish_middleware=[stamp_tenant])
```

Publish middleware wraps preparation (codec + envelope) for single publishes, batches and
`prepare()` alike.
