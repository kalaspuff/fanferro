# 0016: A metadata protobuf envelope for tomodachi services with envelopes of their own

Status: Accepted

## Context

tomodachi lets a service bring its own envelope. One layout for such envelopes is a protobuf message of the data (`bytes data = 1`) and a metadata message
(`Metadata metadata = 2`: `uuid`, `envelope`, `topic`, `origin`, `timestamp`, `encoding`,
`external_path`, `correlation_id`, `data_definition`, `data_class`, all strings),
base64-encoded as the body and sent through tomodachi's SNS/SQS transport. `metadata.encoding`
says how the data is encoded (`proto`, `json`, `string`, `raw`), with `gzip_` in front when it is
compressed with zlib. Only `metadata.envelope` tells such messages apart, and each envelope of
this layout has a name of its own.

Moving such services to fanferro one at a time needs the envelope on both sides, and a custom
envelope in Python would parse every message on the event loop.

## Decision

* `fanferro.envelopes.compat.MetadataProtobuf` is a built-in envelope (Rust core,
  `envelope/tomodachi/metadata.rs`), sent like tomodachi's envelopes (`wrap_for`: the topic in
  `metadata.topic`, tomodachi's queue message document for queues). It is opt-in.
* The envelope's name is a required setting and nothing names a particular one: messages are
  recognised by a `metadata.envelope` that begins with it, and published messages carry it with
  the optional `version` (`<name>--<version>`).
* The data encoding follows the content type: protobuf `proto`, JSON `json` (so pydantic models,
  dataclasses and msgspec structs are published as JSON, like dicts), text `string`, binary
  `raw`. Other content types fail before sending, since the format cannot name them. Data over
  60,000 bytes is compressed.
* `metadata.uuid` is the message id, `metadata.topic` the message type (as for tomodachi's
  envelopes), `metadata.origin` the source, `metadata.data_definition` the data schema.
  `correlation_id` and `data_class` become the headers `correlationid` and `dataclass` (header
  names are lowercase alphanumeric, like CloudEvents extensions, so the correlation id keeps its
  name under other envelopes); other headers travel as message attributes.
* Correlation ids: published messages take the `correlationid` header, else the result of the
  envelope's `correlation_id` function, called where the message is prepared, in the publisher's
  context (a logging context, a request), else a random UUID. The function is called in Python;
  the Rust envelope only sees the header. Incoming messages without one get one derived from
  their uuid (UUID v5) unless `generate_missing_correlation_id=False`, so that every delivery of a
  message, and every copy SNS fans out, has the same id.
* Protobuf messages name their `.proto` file and message in `data_definition` and `data_class`
  unless the message sets the data schema or the `dataclass` header.

## Consequences

* Services with such an envelope and fanferro services share topics and queues; fanferro reads
  their messages in the engine, off the event loop.
* Data stored out of band (`encoding` starting with `oob_`: the data is a key of an object stored
  elsewhere) is not read: such messages fail to decode with an `EnvelopeError` naming the message.
  fanferro's claim checks replace the whole body with a pointer, which these services do not
  read, so messages for them must not be offloaded (`large_payloads`) either.
* `external_path` is neither read nor written.
