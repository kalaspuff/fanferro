# Message formats

A message passes through two independent layers:

* a **codec** turns your object into bytes (and back): JSON, protobuf, ...;
* an **envelope** lays out those bytes plus metadata (id, type, time, headers) on the wire.

## Codecs

Built in, tried in this order: protobuf messages, pydantic models (and `list[Model]`, discriminated
unions, `TypedDict` through `TypeAdapter`), `msgspec.Struct`, dataclasses (nested, typed: enums,
datetimes, UUIDs, Decimals, optionals, literals...), `bytes`, `str`, JSON data. Library codecs only
activate when the library is imported by your application.

Decoding is driven by the handler's annotation: `Message[Order]` decodes into `Order` with the
first codec that supports that type and the message's content type. Protobuf classes accept both
binary and protobuf-JSON payloads.

Custom codec:

```python
class AvroCodec:
    name = "avro"
    def can_encode(self, value: object) -> bool: return isinstance(value, AvroRecord)
    def encode(self, value) -> fanferro.codecs.EncodedBody: return EncodedBody(value.serialize(), "avro/binary")
    def can_decode(self, target, content_type) -> bool: return isinstance(target, type) and issubclass(target, AvroRecord)
    def decode(self, data, target, content_type): return target.deserialize(data)
    def type_name(self, target) -> str | None: return getattr(target, "SCHEMA_NAME", None)

client = fanferro.Client(codecs=fanferro.CodecRegistry([AvroCodec(), *fanferro.codecs.default_codecs()]))
# or: client.codecs.register(AvroCodec())
```

Per message or destination: `publish(obj, codec="json")`, `client.topic("x", codec=MyCodec())`.

## Message types

```python
@fanferro.message_type("orders.created.v1", "orders.created")   # name + accepted aliases
class OrderCreated(BaseModel): ...
```

Resolution: `client.types.register(cls, name, *aliases)` > decorator > codec name (protobuf full
name) > `naming` function (default: class name). Prefer explicit, versioned names for anything
crossing service boundaries.

## Envelopes

| Envelope | Body | Attributes added |
| --- | --- | --- |
| `envelopes.Raw()` (default) | the payload bytes (e.g. the JSON document) | `fanferro.id`, `fanferro.type` (typed messages), `content-type` (non-JSON payloads) |
| `envelopes.CloudEvents()` | CloudEvents 1.0 JSON; JSON data embedded verbatim | `content-type`, `fanferro.type` |
| `envelopes.CloudEventsProtobuf()` | CloudEvents 1.0 protobuf (base64 on the wire) | `content-type`, `fanferro.type`, `fanferro.encoding` |

```python
client = fanferro.Client(envelope=fanferro.envelopes.CloudEvents())                  # publish default
queue = client.queue("legacy", envelope=fanferro.envelopes.Raw(type_attribute=None))  # per destination
await queue.send(obj, envelope=fanferro.envelopes.CloudEventsProtobuf())             # per message
client = fanferro.Client(accept_envelopes=[MyEnvelope(), *fanferro.envelopes.default_envelopes()])
```

**Raw (default).** The body is exactly what the codec produced, so any SQS/SNS client can read
it. Metadata travels in message attributes: the message id (`fanferro.id`, stable across
producer retries), the message type (`fanferro.type`, for routing and SNS filter policies) and the
content type. JSON (`application/json`, what pydantic, dataclass, msgspec and plain-data payloads
use) is implied: it is not written, and consumers assume it for messages that carry `fanferro.id`
or `fanferro.type` without a `content-type`. Other content types (text, protobuf, bytes, custom
media types) are written. Headers become message attributes (`m.header("x")` falls back to
attributes); `source`, `subject`, `time` and `data_schema` are not carried (`message.time` is the
SQS sent timestamp). `Raw(implicit_content_type=None)` writes every content type, and
`Raw(content_type_attribute=None, type_attribute=None, id_attribute=None)` adds no attributes at
all (message ids then fall back to the SQS message id).

**CloudEvents** makes messages self-describing (id, type, source, time, subject, headers as
extension attributes, trace context via the distributed tracing extension) and interoperable with
CloudEvents SDKs, at the cost of a larger body and an extra attribute.

Consumers try envelopes in order (default: CloudEvents JSON, CloudEvents protobuf, raw), so a
queue can receive a mix of formats, including CloudEvents from fanferro 0.1 producers. CloudEvents
are recognised by their `content-type` attribute or, without it, a `specversion` key; `Raw`
accepts anything, so messages from producers that do not use fanferro (boto3, the console, other
frameworks) are consumable: route them with a default route or `when=` matchers and annotate the
type you expect.

A custom envelope implements `name`, `wrap(payload) -> WireMessage` and
`unwrap(wire) -> Payload | None` (return `None` for messages that are not in its format). See
`examples/custom_envelope_and_codec.py`.

## Binary data on SQS

SQS bodies must be text. Binary bodies (protobuf, compressed data) are base64-encoded by the
transport and decoded transparently (`fanferro.encoding`). In CloudEvents JSON, binary payloads use
`data_base64` instead.
