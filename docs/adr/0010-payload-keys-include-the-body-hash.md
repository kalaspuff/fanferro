# 0010: Offloaded payload keys include the body's hash

Status: Accepted

## Context

[0003](0003-claim-checks-in-the-transport-layer.md) stored an offloaded payload under
`{prefix}{message_id}`, so that publish retries overwrite the same object instead of creating new
ones. The pointer in the message records the payload's size and SHA-256, and consumers verify
both. A message published again with the same id but other bytes overwrote the object that an
earlier copy pointed to: with the CloudEvents envelopes, which stamp the event time when they wrap,
every application retry of `publish(obj, id=...)` has other bytes. On a FIFO destination the
retry is deduplicated while its upload replaced the payload, so the only stored copy failed its
integrity check and was dead-lettered; on a standard one the first copy failed the same way.

## Decision

The key is `{prefix}{message_id}/{sha256 of the body}`: retries of the same bytes still overwrite
the same object, and other bytes get an object of their own. Consumers read the key from the
pointer, so messages published with the old layout stay readable.

## Consequences

* A message published again with another body leaves one more object (cleaned up by lifecycle
  rules on the prefix, like every offloaded payload).
* Tools that derived an object's key from the message id alone need the body's SHA-256 too (it is
  in the fanferro pointer).
