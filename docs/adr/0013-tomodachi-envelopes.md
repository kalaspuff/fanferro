# 0013: Envelopes of tomodachi services

Status: Accepted

## Context

Services built on [tomodachi](https://github.com/kalaspuff/tomodachi) exchange messages over SNS
and SQS in envelopes of their own (`tomodachi.envelope.JsonBase` and `ProtobufBase`). Teams that
move services to fanferro one at a time need both to consume what tomodachi services publish and
to publish what they consume, and a custom envelope in Python would unwrap every message on the
event loop and get the details wrong (compression, the document tomodachi wraps queue messages
in, ids).

Two things in tomodachi's format depend on the destination, which `Envelope.wrap(payload)` does not
know: the envelope names the topic (`metadata.topic`), and a message sent straight to a queue is
wrapped in a document shaped like an SNS notification (`{"Type": "Message", "Message": ...}`),
because tomodachi's consumer reads every body as one. Published to a topic, the envelope must not
be wrapped: SNS adds the notification itself.

## Decision

* `fanferro.envelopes.compat` has `TomodachiJSON` and `TomodachiProtobuf`, built-in envelopes
  implemented in the Rust core (`envelope/tomodachi.rs`), opt-in for publishing and consuming.
  They write what tomodachi 0.29 writes, compression of data from 60,000 bytes included, and read
  every data encoding tomodachi has written.
* They have `wrap_for(payload, kind, name)` besides `wrap(payload)`, which destinations call for
  them: a topic gets its name (without the client's `topic_prefix`, decoded from tomodachi's
  encoding of topic names) in `metadata.topic`, a queue the document tomodachi expects there. The
  `Envelope` protocol stays as it is; `wrap()` writes a topic message without a topic.
* The tomodachi topic is the message type of incoming messages (unless the `fanferro.type`
  attribute is present), so routing follows tomodachi's topics.
* `message_uuid` is the service uuid, `.` and the message id. Reading, the service uuid prefix is
  removed: a message fanferro published keeps its id (architecture invariant 4); one tomodachi
  published has the uuid4 tomodachi generated. Without a `service_uuid`, envelopes use a UUID
  generated once per process, so equal settings keep envelopes equal.
* Compressed data may expand to 64 MiB at most; a message that expands further is an envelope
  error, not an allocation of unbounded size.

## Consequences

* fanferro and tomodachi services share topics and queues without code on either side; a
  fanferro consumer reads tomodachi messages in the engine, off the event loop.
* Messages tomodachi sends straight to a queue have no topic, hence no type: they reach the
  default route.
* tomodachi does not read claim checks, CloudEvents or the raw envelope's attributes: what a
  tomodachi service consumes must be published with these envelopes and without offloading.
