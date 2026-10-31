# Error reference

Every exception derives from `fanferro.FanferroError` and has a `retryable` attribute (whether
retrying the same operation unchanged may succeed). Errors raised by the Rust core also have
`category`; AWS errors have `service`, `operation`, `kind`, `code`, `aws_message`, `request_id`
and `status`.

| Exception | Raised when | Retryable |
| --- | --- | --- |
| `ConfigurationError` | Invalid configuration (client, consumer, resource declarations) | no |
| `ValidationError` | A value is invalid before any network call (attributes, FIFO group id, reserved names) | no |
| `MessageTooLargeError` | Encoded message exceeds the destination limit without offloading (`.size`, `.limit`) | no |
| `AwsError` | An AWS call failed (`kind`: throttling, transient, timeout, network, not_found, access_denied, invalid_request, kms, receipt_handle_invalid, service) | by kind |
| `ResourceNotFoundError` | Queue/topic/subscription/object does not exist | no |
| `AccessDeniedError` | Missing permissions or credentials | no |
| `ThrottledError` | Throttled after SDK retries; also SQS refusing to create a queue again within 60 seconds of deleting it (`QueueDeletedRecently`) or to purge it again within 60 seconds (`PurgeQueueInProgress`) | yes |
| `TransportError` | Network/HTTP/timeout/checksum failure outside the SDK | yes |
| `PayloadError` | Storing or fetching a claim-check payload failed (also: URL not allowed) | no |
| `PayloadIntegrityError` | Downloaded payload size or SHA-256 does not match | no |
| `EnvelopeError` | Envelope could not be produced or parsed | no |
| `LeaseError` | Message already settled, or its lease is no longer valid | no |
| `ShutdownError` | Consumer/client closed, or used after `fork()` | no |
| `ProvisioningError` | Provisioning failed with failure mode `"raise"` (`.report`) | by cause |
| `ResourceUnavailableError` | A resource could not be resolved (log/ignore mode); re-resolved later | yes |
| `EncodeError` / `DecodeError` (`CodecError`) | A codec could not encode/decode | no |
| `RoutingError` | Invalid handler signature or route configuration | no |
| `BatchPublishError` | Some batch entries failed after retries (`.result`) | if all failures are |
| `HandlerTimeoutError` | A handler exceeded `handler_timeout` (also a `TimeoutError`) | yes |
| `ConsumerStateError` | Invalid consumer state transition (start twice, no routes) | no |

Handler control flow uses `fanferro.Retry`, `Requeue`, `Reject` and `Discard` (subclasses of
`HandlerAction`, not `FanferroError`).
