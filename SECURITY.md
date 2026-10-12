# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub security advisories ("Report a vulnerability"
on the repository) or by email to hello@carloscar.com.

## Supported versions

During 0.x only the latest minor release receives fixes.

## Security-relevant behaviour

* **Payload URLs**: claim-check pointers may contain presigned URLs. Consumers only fetch URLs on an
  allow-list (AWS hosts and the configured `endpoint_url` by default; `PayloadFetch.allowed_url_hosts`)
  over HTTPS, with a size cap, to prevent server-side request forgery through crafted messages.
* **Integrity**: offloaded payloads are verified against the size and SHA-256 recorded in the pointer;
  SQS MD5 digests of message bodies and attributes are verified.
* **Secrets**: fanferro never logs message bodies or credentials; presigned URLs are redacted from
  HTTP error messages. `Credentials.__repr__` hides the secret key.
* **Queue policies**: subscriptions add a statement allowing only the subscribed topic (`aws:SourceArn`)
  to send to the queue.
* **Untrusted input**: message bodies are parsed by the configured codecs; prefer validating codecs
  (pydantic, msgspec) for messages from other trust domains.
