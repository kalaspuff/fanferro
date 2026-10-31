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
  allow-list of hosts to prevent SSRF via crafted messages, and size-capped. By default the list
  holds the endpoints of S3 (until 0.7 any host under AWS's domains, which included load balancers
  and EC2 instances that resolve to private addresses), and URL fetches need a pointer that records
  the payload's SHA-256.
* The S3 key is `{prefix}{message_id}`, so publish retries overwrite the same object (amended by
  [0010](0010-payload-keys-include-the-body-hash.md): `{prefix}{message_id}/{sha256 of the body}`).

## Consequences

* Envelopes and handlers are oblivious to transport encoding.
* Routing by envelope fields requires the download; routing by attributes does not.
* Objects are not deleted by default (fan-out readers share them); lifecycle rules or
  `delete_offloaded_payloads=True` (point-to-point queues) clean up.
