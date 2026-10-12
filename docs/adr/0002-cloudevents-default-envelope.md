# 0002: CloudEvents 1.0 (JSON) as the default envelope

Status: Accepted

## Context

Messages need metadata (id, type, time, source, content type, trace context). SQS/SNS allow only
ten message attributes, which applications also need, and SNS filter policies work on attributes.

## Decision

* Default envelope: CloudEvents 1.0 structured mode, JSON event format. JSON payloads are embedded
  verbatim in `data` (no re-serialization), text as a string, binary as `data_base64`. Headers
  are CloudEvents extension attributes (this is how the distributed tracing extension travels).
* Two attributes by default: `content-type: application/cloudevents+json` (format detection) and
  `fanferro.type` (SNS filter policies, routing without parsing). Both can be disabled.
* A protobuf CloudEvents envelope for compact binary messages and a raw envelope for systems that
  do not use envelopes. Consumers try envelopes in order: CloudEvents JSON, CloudEvents protobuf,
  raw (raw accepts anything, so foreign messages are still consumable).

## Consequences

* Messages are self-describing and interoperable with any CloudEvents SDK.
* Attribute budget used by fanferro is small and documented (`docs/reference/attributes.md`).
