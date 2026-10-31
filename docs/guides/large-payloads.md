# Large payloads (claim checks)

```python
client = fanferro.Client(
    large_payloads=fanferro.LargePayloads(
        bucket="my-app-payloads",
        prefix="events/",
        threshold=256 * 1024,         # offload when body + attributes exceed this
        reference="both",             # "s3", "url" (presigned) or "both"
        presign_ttl=timedelta(days=2),  # default: 24 hours
        encryption=fanferro.Kms("alias/my-key"),   # or "s3", or None (bucket default)
    )
)
```

Publishing a message whose encoded size exceeds the threshold (or the destination's size limit, when
that is lower: 256 KiB for SNS topics by default, a queue's `max_message_size`) uploads the body to
`s3://bucket/{prefix}{message_id}/{sha256 of the body}` and sends a small pointer instead. Consumers
resolve pointers before your handler runs; handlers never notice. Per destination:
`client.topic("x", large_payloads=LargePayloads(...))` or `large_payloads=None` to disable.
`create_bucket=True` creates the bucket when the client provisions (`async with client` or
`client.provision()`), the client's and those of topics and queues, each once.
`Client(bucket_prefix="staging-")` prepends an environment prefix to every bucket name (opt one
out with `prefixed=False`); see
[Deployment → Several environments in one AWS account](deployment.md#several-environments-in-one-aws-account). The
pointer takes one of the 10 message attributes, so an offloaded message can carry at most 9: a
message with more fails before anything is uploaded.

## Bucket + key or presigned URL

* `reference="s3"`: consumers need `s3:GetObject` (and KMS decrypt for SSE-KMS).
* `reference="url"`: consumers download with plain HTTPS - useful for other teams/accounts that
  should not get S3 permissions. Presigned URLs expire after `presign_ttl` (24 hours by default,
  at most 7 days, the limit of AWS signatures). A message that waits longer (a consumer outage, a
  redrive from the dead-letter queue) can no longer be fetched: it fails to decode ("payload URL
  expired at ...") and is dead-lettered by default. A URL signed with temporary credentials (an
  IAM role on EC2, ECS, EKS or Lambda) also stops working when those credentials expire, often
  within hours: choose `presign_ttl` for how long messages may wait, and `reference="both"` when
  consumers can read the bucket.
* `reference="both"`: consumers use `PayloadFetch(prefer="s3")` and fall back to the URL.

## Safety

`PayloadFetch` (client option `payload_fetch=`) limits what consumers download:
`max_bytes` (64 MiB), `allowed_url_hosts` (default: AWS hosts and your `endpoint_url`; blocks
SSRF through crafted pointers), `allow_http`, and `verify_checksum` (size + SHA-256 recorded in the
pointer). Redirects are not followed: a URL that answers with one fails to decode, so a pointer
cannot lead a consumer to a host outside the list.

## Interoperability

`format="aws_extended"` writes pointers in the format of the AWS SQS/SNS Extended Client
Libraries (`ExtendedPayloadSize` attribute, `PayloadS3Pointer` body), so Java/Python services
using them can read fanferro messages. fanferro reads both formats regardless, and the legacy
pointers of those libraries too (`SQSLargePayloadSize` attribute, `MessageS3Pointer` body: the Java
library before 2.0, the Python ones with `use_legacy_attribute=True`).

## Cleanup

Objects are kept by default because SNS fan-out delivers the same pointer to several queues. Use
an S3 lifecycle rule on the prefix, or `client.consumer(queue, delete_offloaded_payloads=True)` on
point-to-point queues. The key is derived from the message id and the body, so publish retries
of the same bytes overwrite the same object instead of creating new ones, while a message published
again with the same id but another body (CloudEvents stamp a new time) gets its own object rather
than replacing the one an earlier copy points to.

With `delete_offloaded_payloads=True`, every copy of a message (a duplicate delivery of a standard
queue, a publish retry of the same bytes) points to the same object, which the ack of the first
copy deletes. A consumer with this option therefore treats a payload object that no longer exists
(S3 answers `NoSuchKey`) as the sign of a message that was already processed: it acks the message
without running a handler and emits `message.duplicate` (with `reason="payload_deleted"`), as
`IdempotencyMiddleware` does for a completed message.

Take into account before enabling it:

* A message whose object is missing for any other reason is dropped the same way, not
  dead-lettered: an S3 lifecycle rule that expired the object before the message was consumed, a
  manual or scripted delete, a consumer of another queue (SNS fan-out) that deleted the shared
  object. Keep a lifecycle rule's expiry well above the longest time a message can wait in the
  queue (its retention period, and the time it spends in redrive from a dead-letter queue), and
  enable the option only on point-to-point queues. Watch `message.duplicate` events with
  `reason="payload_deleted"` if such losses must be noticed.
* A missing bucket (`NoSuchBucket`), denied access and other errors are not affected: they are
  decode failures as before (`on_decode_error`, or retried when transient).
* Without the option, a missing object stays a decode failure and the message is dead-lettered.
