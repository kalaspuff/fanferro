# Deployment

## Processes, threads and fork

The Rust core runs a Tokio runtime with 2-4 worker threads per process (`FANFERRO_WORKER_THREADS`).
Create clients **after** forking (gunicorn/uvicorn workers, multiprocessing): a client created
before `fork()` raises `ShutdownError` in the child instead of sharing connections with the parent.

## Graceful shutdown

Orchestrators send SIGTERM and kill after a grace period (ECS: 30 s by default, Kubernetes:
`terminationGracePeriodSeconds`, 30 s by default). `consumer.run()` / `client.run()` handle
SIGTERM: stop receiving, let the last long poll finish (≤ `wait_time`), drain handlers, release
the rest. Make sure `ShutdownPolicy.drain_timeout` + `cancel_timeout` (and `wait_time`) fit in
the grace period:

```python
client.consumer(queue, wait_time=10, shutdown=fanferro.ShutdownPolicy(drain_timeout=15, cancel_timeout=5))
```

Kubernetes: set the grace period above that, and avoid sending traffic during termination
(preStop hooks are not needed for queue consumers).

## Visibility timeout and heartbeats

Choose a visibility timeout that covers retry delays you want SQS to apply (30-60 s is typical);
heartbeats handle long processing. A short timeout recovers crashed workers' messages sooner; a long
one tolerates longer stalls (GC, network partitions) before redelivery.

## Throughput tuning

* I/O-bound handlers: raise `concurrency` (100+ is fine on asyncio) and `pollers` (each long poll
  returns at most 10 messages).
* Keep `prefetch` low for slow handlers (prefetched messages wait while invisible).
* Use `publish_batch`/`send_batch` for bulk publishing (10 messages per API call).
* CPU-bound handlers block the event loop: offload with `asyncio.to_thread` or a process pool
  (heartbeats keep working regardless).

## Credentials and regions

fanferro uses the standard AWS provider chain (environment, profile, SSO, web identity / IRSA,
ECS task role, EC2 instance profile). Set `region=` explicitly in multi-region code. For
cross-account queues pass `owner_account_id=` to `client.queue`.

## Least privilege

Use `Provisioning.assume()` (or `existing()` with read permissions) in production services and
manage infrastructure with IaC; reserve `ensure()` for development, tests and services that own
their queues. See [Provisioning → IAM permissions](provisioning.md#iam-permissions).

## Lambda

fanferro is designed for long-running consumers. In Lambda with an SQS event source mapping,
use fanferro for publishing (create the client outside the handler and `asyncio.run(...)` the
publish per invocation) and read records directly: with the default raw envelope the record body
is the message (e.g. the JSON document) and `fanferro.id` / `fanferro.type` are in its
`messageAttributes`; with CloudEvents, parse the body with
`fanferro.envelopes.CloudEvents().unwrap(fanferro.WireMessage(record["body"]))`.
