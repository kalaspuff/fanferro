# 0003: Claim checks and base64 live in the transport layer, below envelopes

Status: Accepted

## Context

SQS/SNS bodies must be valid XML-range text and are limited in size. Envelopes (including custom
Python ones) should not need to care about either.

## Decision

* After the envelope produced a wire message, the transport layer base64-encodes non-text bodies
  (`fanferro.encoding=base64`) and offloads bodies above the threshold to S3, replacing them with a
  pointer (`fanferro.claim-check`) that carries bucket/key and/or a presigned URL, size and SHA-256.
* The AWS SQS/SNS Extended Client pointer format (`ExtendedPayloadSize`) is supported for reading
  always and for writing on request, for interoperability with Java/Python services.
* Receiving resolves pointers before envelopes see the message. URL fetches are restricted to an
  allow-list of hosts (AWS by default) to prevent SSRF via crafted messages, and size-capped.
* The S3 key is `{prefix}{message_id}`, so publish retries overwrite the same object.

## Consequences

* Envelopes and handlers are oblivious to transport encoding.
* Routing by envelope fields requires the download; routing by attributes does not.
* Objects are not deleted by default (fan-out readers share them); lifecycle rules or
  `delete_offloaded_payloads=True` (point-to-point queues) clean up.
