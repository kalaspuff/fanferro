# 0015: EventBridge events are unwrapped like an envelope

Status: Accepted

## Context

EventBridge rules deliver events to SQS queues and SNS topics: the events of AWS services, of SaaS
partners and of applications (`PutEvents`), all in the same wrapper (`id`, `detail-type`,
`source`, `account`, `time`, `region`, `resources`) around the event itself, the `detail`. With
ADR 0014 only S3's and SES's own notifications were recognised; EventBridge events reached
handlers as untyped JSON, wrapper included.

A generic class around the detail (`EventBridgeEvent[InvoicePaid]`) would not route: a handler
annotated with a parameterized generic has no message type, so its route would match every
message, and decoding the detail into an application's class needs that class's codec.

## Decision

* The `AwsNotifications` envelope unwraps EventBridge events like a CloudEvent: the payload is the
  `detail` (a slice of the body), the message id the event's `id`, the time its `time`, the source
  its `source`; `detail-type`, `account`, `region` and `resources` become headers, which
  `aws.EventBridgeMetadata.of(message)` reads. Handlers are annotated with the class of the detail
  and the codecs decode it like any payload.
* The message type is `aws.<service>.<event>` for AWS services (the snake-cased `detail-type`
  without a leading `aws_` or `<service>_`), SES's types for SES's email events (whose detail is
  an SES notification, so the SES classes decode it), and the `detail-type` as it is for every
  other source, which names its events as the application or partner does.
* S3 events through EventBridge differ from S3's notifications (fields, names, keys that are not
  URL-encoded) and decode into a class of their own, `aws.S3ObjectEvent`, whose routes match every
  S3 event type.

## Consequences

* A handler annotated with an application's model receives that application's EventBridge events,
  with the event id as message id (stable across EventBridge's redeliveries).
* Two sources with the same `detail-type` share a message type; routes tell them apart with
  `when=m.source() == ...`.
* A rule's input transformer replaces the event with what its template makes, which is not
  recognised as an event.
