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
| `envelopes.CloudEvents()` (default) | CloudEvents 1.0 JSON; JSON data embedded verbatim | `content-type`, `fanferro.type` |
| `envelopes.CloudEventsProtobuf()` | CloudEvents 1.0 protobuf (base64 on the wire) | `content-type`, `fanferro.type`, `fanferro.encoding` |
| `envelopes.Raw()` | the payload bytes | `content-type`, `fanferro.type`, `fanferro.id` (each optional) |

```python
client = fanferro.Client(envelope=fanferro.envelopes.CloudEventsProtobuf())        # publish default
queue = client.queue("legacy", envelope=fanferro.envelopes.Raw(type_attribute=None))  # per destination
await queue.send(obj, envelope=fanferro.envelopes.CloudEvents())                     # per message
client = fanferro.Client(accept_envelopes=[MyEnvelope(), *fanferro.envelopes.default_envelopes()])
```

Consumers try envelopes in order; `Raw` accepts anything, so messages from producers that do not
use fanferro (boto3, the console, other frameworks) are consumable: route them with a default
route or `when=` matchers and annotate the type you expect.

A custom envelope implements `name`, `wrap(payload) -> WireMessage` and
`unwrap(wire) -> Payload | None` (return `None` for messages that are not in its format). See
`examples/custom_envelope_and_codec.py`.

## Binary data on SQS

SQS bodies must be text. Binary bodies (protobuf, compressed data) are base64-encoded by the
transport and decoded transparently (`fanferro.encoding`). In CloudEvents JSON, binary payloads use
`data_base64` instead.
