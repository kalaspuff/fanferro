# 0014: Notifications of AWS services are recognised and typed by default

Status: Accepted

## Context

S3 event notifications and SES email notifications (bounces, complaints, deliveries, and the
events of configuration sets) reach applications through SNS topics and SQS queues as JSON
documents without attributes. Raw delivered them with no message type, so applications routed
them with a default route or `when=` matchers and parsed AWS's camelCase documents themselves,
including details that are easy to get wrong: S3 object keys are URL-encoded, S3 sends a test
event of another shape when notifications are set up, and SES has two formats (`notificationType`
for an identity's feedback notifications, `eventType` for event publishing) and more than one
timestamp format.

## Decision

* A built-in envelope, `AwsNotifications` (`aws`, in the Rust core), recognises S3 event
  notifications (and the test event) and SES notifications by their body and gives them a message
  type (`aws.s3.event`, `aws.ses.<event type in snake case>`), a source (`aws:s3`, `aws:ses`) and
  the event's time. It is among the default envelopes consumers try, before `Raw`. Messages with
  the raw envelope's `fanferro.id` or `fanferro.type` attribute are never taken for notifications.
* `fanferro.aws` has frozen dataclasses for S3 events and SES bounce, complaint and delivery
  notifications (and the part every SES notification has), decoded and encoded by a default
  codec (`AwsNotificationCodec`, before the dataclass codec) whose type names are the envelope's
  types, so a handler annotated with one of them is routed to without `message_type=`.
* Parsing is tolerant like AWS asks: unknown fields are ignored, missing optional ones are `None`,
  booleans may be strings, timestamps are read in every format AWS documents. S3 keys are
  URL-decoded. The classes encode back into AWS's documents, for tests.

## Consequences

* Consumers receive S3 and SES notifications as typed objects with no configuration.
* A consumer's messages of these shapes now have a type: routes with conditions on a missing type
  or on the `raw` envelope no longer match them. Default routes still receive them when no route
  of their type exists.
* Other AWS notifications (EventBridge events, other services) are not recognised; the envelope
  can learn them the same way.
