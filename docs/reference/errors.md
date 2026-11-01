# Error reference

Every exception derives from `fanferro.FanferroError` and has a `retryable` attribute (whether
retrying the same operation unchanged may succeed). Errors raised by the Rust core also have
`category`; AWS errors have `service`, `operation`, `kind`, `code`, `aws_message`, `request_id`
and `status`.

All of them are classes of `fanferro.errors` (also exported by `fanferro`), the module that
tracebacks, the `error_type` of events and dead-letter metadata, and OpenTelemetry's `error.type`
name: `fanferro.errors.AccessDeniedError`.

Errors pickle with their attributes, so they reach the caller of a worker process
(`ProcessPoolExecutor`, multiprocessing) or a task queue as themselves: a `ProvisioningError` with
its `.report`, a `BatchPublishError` with its `.result`. A value that does not pickle is left out
(`None` in the unpickled error), and only that value: a failed message whose body is an instance of
a class defined in a function arrives with `body=None` but with its id, type and attributes, a
failure's error that does not pickle as `error=None`. Copies (`copy.copy`, `copy.deepcopy`) keep
every attribute. `AttributeValue` and `WireMessage` pickle too.

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
| `BatchItemFailuresError` | Records of an SQS event failed in a `fanferro.aws_lambda` handler created with `report_batch_item_failures=False` (`.item_identifiers`); Lambda returns the batch to the queue | yes |

Handler control flow uses `fanferro.Retry`, `Requeue`, `Reject` and `Discard` (subclasses of
`HandlerAction`, not `FanferroError`).
