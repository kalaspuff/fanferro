# Large payloads (claim checks)

```python
client = fanferro.Client(
    large_payloads=fanferro.LargePayloads(
        bucket="my-app-payloads",
        prefix="events/",
        threshold=256 * 1024,         # offload when body + attributes exceed this
        reference="both",             # "s3", "url" (presigned) or "both"
        presign_ttl=timedelta(hours=6),
        encryption=fanferro.Kms("alias/my-key"),   # or "s3", or None (bucket default)
    )
)
```

Publishing a message whose encoded size exceeds the threshold uploads the body to
`s3://bucket/{prefix}{message_id}` and sends a small pointer instead. Consumers resolve pointers
before your handler runs; handlers never notice. Per destination: `client.topic("x",
large_payloads=LargePayloads(...))` or `large_payloads=None` to disable.

## Bucket + key or presigned URL

* `reference="s3"`: consumers need `s3:GetObject` (and KMS decrypt for SSE-KMS).
* `reference="url"`: consumers download with plain HTTPS - useful for other teams/accounts that
  should not get S3 permissions. Presigned URLs expire (`presign_ttl`, at most 7 days; less with
  temporary credentials - the URL dies with the session).
* `reference="both"`: consumers use `PayloadFetch(prefer="s3")` and fall back to the URL.

## Safety

`PayloadFetch` (client option `payload_fetch=`) limits what consumers download:
`max_bytes` (64 MiB), `allowed_url_hosts` (default: AWS hosts and your `endpoint_url`; blocks
SSRF through crafted pointers), `allow_http`, and `verify_checksum` (size + SHA-256 recorded in the
pointer).

## Interoperability

`format="aws_extended"` writes pointers in the format of the AWS SQS/SNS Extended Client
Libraries (`ExtendedPayloadSize` attribute, `PayloadS3Pointer` body), so Java/Python services
using them can read fanferro messages. fanferro reads both formats regardless.

## Cleanup

Objects are kept by default because SNS fan-out delivers the same pointer to several queues. Use
an S3 lifecycle rule on the prefix, or `client.consumer(queue, delete_offloaded_payloads=True)` on
point-to-point queues. The key is derived from the message id, so publish retries overwrite the
same object instead of creating new ones.
