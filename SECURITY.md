# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub security advisories ("Report a vulnerability"
on the repository) or by email to hello@carloscar.com.

## Supported versions

During 0.x only the latest minor release receives fixes.

## Security-relevant behaviour

* **Payload URLs**: claim-check pointers may contain presigned URLs. Consumers only fetch URLs on an
  allow-list (S3 endpoints by default, not other hosts under AWS's domains such as load balancers or
  EC2 instances; `PayloadFetch.allowed_url_hosts`) over HTTPS, or on the configured `endpoint_url`
  (its scheme, host and port), with a size cap and without following redirects, and only for
  pointers that record the payload's SHA-256, to prevent server-side request forgery through
  crafted messages.
* **Payload buckets**: pointers with a bucket and key are read with the consumer's credentials.
  Grant consumers `s3:GetObject` (and `s3:DeleteObject` with `delete_offloaded_payloads=True`) on
  the payload buckets only, or list them in `PayloadFetch.allowed_buckets`, so that a crafted
  message cannot have them read other objects.
* **Integrity**: offloaded payloads are verified against the size and SHA-256 recorded in the pointer;
  SQS MD5 digests of message bodies and attributes are verified.
* **Secrets**: fanferro never logs message bodies or credentials; presigned URLs are redacted from
  HTTP error messages. `Credentials.__repr__` hides the secret key.
* **Queue policies**: subscriptions add a statement allowing only the subscribed topic (`aws:SourceArn`)
  to send to the queue.
* **Untrusted input**: message bodies are parsed by the configured codecs; prefer validating codecs
  (pydantic, msgspec) for messages from other trust domains.
