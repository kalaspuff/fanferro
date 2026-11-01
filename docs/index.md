# fanferro documentation

* [Getting started](guides/getting-started.md) - install, first publish and consumer, local development
* [Publishing](guides/publishing.md) - topics, queues, options, batches, FIFO, prepared messages
* [Consuming](guides/consuming.md) - consumers, routing, handlers, concurrency, heartbeats, shutdown
* [Failure handling](guides/failure-handling.md) - retries, dead-letter queues, requeue, poison messages
* [Provisioning](guides/provisioning.md) - declaring resources, policies, failure modes, IAM
* [Message formats](guides/message-formats.md) - codecs, envelopes, protobuf, interoperability
* [AWS notifications](guides/aws-notifications.md) - S3 events, SES bounces, complaints and deliveries, and EventBridge events as typed messages
* [Large payloads](guides/large-payloads.md) - claim checks with S3 and presigned URLs
* [Observability](guides/observability.md) - events, logging, hooks, OpenTelemetry, metrics
* [Patterns](guides/patterns.md) - transactional outbox, inbox, fan-out, versioning, ordering
* [Testing](guides/testing.md) - in-memory broker, handler unit tests, MiniStack
* [Deployment](guides/deployment.md) - permissions, environments sharing an account, graceful
  shutdown, processes, tuning

Reference: [configuration](reference/configuration.md) · [events](reference/events.md) ·
[attributes](reference/attributes.md) · [errors](reference/errors.md)

Internals: [architecture](architecture.md) · [performance](performance.md) · [parity with AWS](parity/README.md) ·
[decision records](adr/README.md) · [contributing guide for agents](../AGENTS.md)
