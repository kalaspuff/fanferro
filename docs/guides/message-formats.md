# Message formats

A message passes through two independent layers:

* a **codec** turns your object into bytes (and back): JSON, protobuf, ...;
* an **envelope** lays out those bytes plus metadata (id, type, time, headers) on the wire.

## Codecs

Built in, tried in this order: protobuf messages, pydantic models (and `list[Model]`, discriminated
unions, `TypedDict` through `TypeAdapter`; bodies use field names unless the model serializes by
alias or `PydanticCodec(by_alias=True)`, and decoding accepts names and aliases with pydantic 2.11
or later), `msgspec.Struct`, the S3 and SES notifications of `fanferro.aws` ([AWS
notifications](aws-notifications.md)), dataclasses (nested, typed: enums, datetimes, UUIDs, Decimals,
optionals, literals of values or enum members, `type` aliases, generic dataclasses, named tuples
from objects or arrays, `TypedDict` fields, bare `tuple` and `set`, dicts with int, float, bool or
enum keys...), `bytes`, `str`, JSON data. Library codecs only activate when the library is imported by
your application.

Decoding is driven by the handler's annotation: `Message[Order]` decodes into `Order` with the
first codec that supports that type and the message's content type. Protobuf classes accept both
binary and protobuf-JSON payloads. Containers and unions decode with the codec of the types they
hold, whatever else the application imported: `list[Item]` or `Item | None` with the dataclass
codec, `list[Point]` with msgspec, `list[Model]` with pydantic; plain JSON shapes (`list[int]`,
`dict[str, Any]`) with pydantic when it is imported (validated), otherwise as JSON data.

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
| `envelopes.AwsNotifications()` | the document of an AWS service's notification (S3 events, SES email; see [AWS notifications](aws-notifications.md)) | none |
| `envelopes.compat.TomodachiJSON()`, `TomodachiProtobuf()` | tomodachi's envelopes (see [tomodachi services](#tomodachi-services)) | `fanferro.type` |

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

Consumers try envelopes in order (default: CloudEvents JSON, CloudEvents protobuf, AWS
notifications, raw), so a queue can receive a mix of formats, including CloudEvents from fanferro
0.1 producers and the notifications of S3 and SES, which get a message type (`aws.s3.event`,
`aws.ses.bounce`...) and decode into the classes of `fanferro.aws` ([AWS
notifications](aws-notifications.md)). CloudEvents
are recognised by their `content-type` attribute or, without it, a `specversion` key (except in
messages with the raw envelope's `fanferro.id` attribute, whose body may be a relayed event); `Raw`
accepts anything, so messages from producers that do not use fanferro (boto3, the console, other
frameworks) are consumable: route them with a default route or `when=` matchers and annotate the
type you expect.

A custom envelope implements `name`, `wrap(payload) -> WireMessage` and
`unwrap(wire) -> Payload | None` (return `None` for messages that are not in its format). See
`examples/custom_envelope_and_codec.py`. When a consumer uses only built-in envelopes, the Rust
engine unwraps messages while it decodes them, off the event loop; with a custom envelope among
them, the consumer tries them in Python, in the same order.

## tomodachi services

[tomodachi](https://github.com/kalaspuff/tomodachi) services wrap what they publish to SNS and
send to SQS in an envelope of their own: `tomodachi.envelope.JsonBase` (a JSON document with the
service, metadata and the data) or `tomodachi.envelope.ProtobufBase` (the same as a protobuf
message, base64-encoded). `fanferro.envelopes.compat` has both, implemented in Rust like the
built-in envelopes (consumers unwrap them in the engine, off the event loop):

| Envelope | tomodachi envelope | Data | Message type |
| --- | --- | --- | --- |
| `compat.TomodachiJSON()` | `JsonBase` | JSON (text payloads become JSON strings) | the tomodachi topic |
| `compat.TomodachiProtobuf()` | `ProtobufBase` | a serialized protobuf message (`application/x-protobuf`) | the tomodachi topic |

The data inside a tomodachi envelope is decoded and encoded by the codecs, like any payload:
handlers get pydantic models, dataclasses, msgspec structs or protobuf messages, and published
objects reach tomodachi handlers as the `data` they expect (a dict for JSON, the protobuf message
for `ProtobufBase`).

```python
from fanferro.envelopes import default_envelopes
from fanferro.envelopes.compat import TomodachiJSON, TomodachiProtobuf


@fanferro.message_type("order-created")  # the tomodachi topic is the message type
class OrderCreated(BaseModel):
    order_id: int
    total: Decimal
    placed_at: datetime
    lines: list[Line]


@fanferro.message_type("invoice-created")
class InvoiceCreated(BaseModel):
    invoice_id: int
    order_id: int


client = fanferro.Client(
    topic_prefix="production-",  # tomodachi's aws_sns_sqs.topic_prefix, if it has one
    # Before Raw, which accepts any message.
    accept_envelopes=[TomodachiJSON(), TomodachiProtobuf(), *default_envelopes()],
)
client.types.register(orders_pb2.OrderShipped, "order-shipped")  # a protobuf class for a topic

# A queue of this service, subscribed to the topics tomodachi services publish to.
client.subscription("order-created", "billing-orders")
client.subscription("order-shipped", "billing-orders")
consumer = client.consumer(client.queue("billing-orders"))

# Topics tomodachi services subscribe to, and a queue one consumes.
invoices = client.topic("invoice-created", envelope=TomodachiJSON(service_name="billing"))
legacy_inbox = client.queue("legacy-inbox", envelope=TomodachiJSON(service_name="billing"))


@consumer.route(OrderCreated)
async def on_order_created(message: fanferro.Message[OrderCreated]) -> None:
    order = message.body  # an OrderCreated, validated by pydantic
    await invoices.publish(InvoiceCreated(invoice_id=1, order_id=order.order_id))  # data: {"invoice_id": 1, ...}


@consumer.route(orders_pb2.OrderShipped)
async def on_order_shipped(message: fanferro.Message[orders_pb2.OrderShipped]) -> None: ...


@consumer.default  # messages tomodachi sent straight to the queue (no topic)
async def on_direct(message: fanferro.Message[OrderCreated]) -> None: ...  # decoded by the annotation
```

**Consuming.** Both envelopes recognise their messages by `metadata.protocol_version` in the body,
whether tomodachi published them to a topic (delivered with or without raw message delivery) or
sent them to a queue (`aws_sns_sqs_send_message`, which wraps the envelope in a document shaped
like an SNS notification), and read data tomodachi compressed (`base64_gzip_json`, `gzip_proto`,
and the older `base64` and `base64_gzip_proto`). A message's type is its `metadata.topic`, so
routes are declared for the topic name (`client.types.register(cls, "topic")` or
`@fanferro.message_type("topic")`); messages tomodachi sent straight to a queue have no topic and
reach the default route. `message.id` is the `message_uuid` without the service uuid in front,
`message.source` the producing service's name and `message.time` its timestamp.

**Publishing.** Messages are written as tomodachi 0.29 writes them: `service` (`service_name`,
default the message's source, and `service_uuid`, default a UUID per process like a tomodachi
service's), `message_uuid` (the service uuid, `.` and the fanferro message id, so a fanferro
consumer reads the same id back), the topic without the client's `topic_prefix` in
`metadata.topic`, and data of 60,000 bytes or more compressed like tomodachi does
(`compress_threshold`, `None` to never compress). Messages sent to a queue are wrapped in the
document tomodachi's consumer expects there. The type is promoted to `fanferro.type`
(`type_attribute`) for fanferro consumers and SNS filter policies; tomodachi ignores the
attribute. `TomodachiJSON` refuses binary payloads and `TomodachiProtobuf` JSON and text ones
(`EnvelopeError`), so a message is not published in a form the other side cannot parse.

* tomodachi reads the body of SNS notifications: queues tomodachi subscribes to a topic have no raw
  message delivery, and a queue fanferro subscribes on a tomodachi service's behalf must have none
  either (`raw_message_delivery=False`). fanferro consumers read both.
* Topic names: tomodachi 0.29 allows letters, digits, `-` and `_` only, which are the same on
  SNS. Older versions accepted other characters and encoded them (`order.created` is the topic
  `order___2e_created`): `compat.tomodachi_topic("order.created")` gives that name.
* Trace context travels in the `traceparent` message attribute, which tomodachi's OpenTelemetry
  instrumentation reads and writes too.
* tomodachi does not read claim checks: do not offload payloads (`large_payloads`) of messages for
  tomodachi services; compression keeps most of them within the SNS limit.

### Metadata protobuf envelopes

tomodachi services can bring an envelope of their own. `compat.MetadataProtobuf` reads and writes
one layout of such envelopes: a protobuf message of the data and a metadata message, base64-encoded
as the body and sent like tomodachi's envelopes.

```proto
message Metadata {
    string uuid = 1;            // message.id
    string envelope = 2;        // the envelope's name and version: "example-envelope--1.0.0"
    string topic = 3;           // message.type
    string origin = 4;          // message.source
    string timestamp = 5;       // message.time, "2026-11-01T15:33:45.242502Z"
    string encoding = 6;        // proto, json, string or raw; gzip_ in front when compressed
    string external_path = 7;   // neither read nor written
    string correlation_id = 8;  // the correlationid header
    string data_definition = 9; // protobuf data: its .proto file (the data schema)
    string data_class = 10;     // protobuf data: its message name (the dataclass header)
}

message Message {
    bytes data = 1;
    Metadata metadata = 2;
}
```

Nothing but `metadata.envelope` tells these messages apart, so the envelope's name is its first
argument: messages whose `metadata.envelope` begins with it are read, and published messages
carry it with `version`.

```python
from fanferro.envelopes import default_envelopes
from fanferro.envelopes.compat import MetadataProtobuf

envelope = MetadataProtobuf(
    "example-envelope",
    version="1.0.0",                         # metadata.envelope: "example-envelope--1.0.0"
    service_name="billing",                  # metadata.origin (default: the message's source)
    correlation_id=current_correlation_id,   # () -> str | None, called for each message published
)
client = fanferro.Client(accept_envelopes=[envelope, *default_envelopes()])  # before Raw
invoices = client.topic("invoice-created", envelope=envelope)
```

**Data.** The encoding follows the content type, so the data decodes into and encodes from
models like any payload: protobuf messages are `proto` (with their `.proto` file and message name
in `data_definition` and `data_class`), pydantic models, dataclasses, msgspec structs and dicts
`json`, text `string` and bytes `raw`. Other content types (a MessagePack codec, say) fail before
sending: the format has no name for them. Data over 60,000 bytes is compressed with zlib
(`gzip_json`, ...; `compress_threshold`, `None` to never compress), and compressed data is read
up to 64 MiB. A service that reads a topic's data with a protobuf class reads only `proto`: publish
models to topics whose consumers read JSON.

**Correlation ids.** `metadata.correlation_id` is the `correlationid` header. A published message
takes it from its own `correlationid` header, else from the envelope's `correlation_id` function,
which is called where the message is prepared (in the publishing task, so it sees its context
variables, like a logging context), else gets a random UUID. An incoming message without one gets
one derived from its uuid (`generate_missing_correlation_id=False` leaves it out), the same for
every delivery of the message. To keep the id through a chain of handlers, set the context the
function reads from the incoming message in consume middleware:

```python
correlation: ContextVar[str | None] = ContextVar("correlation", default=None)


def current_correlation_id() -> str | None:
    return correlation.get()


async def correlated(message: fanferro.Message[Any], call_next: HandlerNext) -> None:
    value = message.headers.get("correlationid")
    token = correlation.set(value if isinstance(value, str) else None)
    try:
        await call_next(message)  # what the handler publishes carries the same correlation id
    finally:
        correlation.reset(token)


client = fanferro.Client(consume_middleware=[correlated], accept_envelopes=[envelope, *default_envelopes()])
```

* Other headers travel as message attributes, next to the promoted type (`fanferro.type`).
* Data stored out of band (`encoding` starting with `oob_`) is not read: the message fails to
  decode (`EnvelopeError`). Do not offload payloads (`large_payloads`) of messages for these
  services either: they do not read claim checks.

## Binary data on SQS

SQS bodies must be text. Binary bodies (protobuf, compressed data) are base64-encoded by the
transport and decoded transparently (`fanferro.encoding`). In CloudEvents JSON, binary payloads use
`data_base64` instead. A JSON `null` payload (`publish(None, ...)`) is `"data": null` with a JSON
`datacontenttype` and reaches handlers as `null`; `"data": null` without one is an event without data.
