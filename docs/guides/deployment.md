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

Without `credentials=`, fanferro uses the default credential provider chain of the AWS SDK for
Rust. It reads the same environment variables and files as boto3 and the AWS CLI, so a setup that
works for them works for fanferro. The first source that provides credentials wins:

1. **Environment variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and optionally
   `AWS_SESSION_TOKEN`.
2. **Shared config and credentials files** (`~/.aws/config`, `~/.aws/credentials`): the profile
   passed as `profile=`, else `AWS_PROFILE`, else `default`. SSO profiles, `credential_process`
   and role profiles (`role_arn` + `source_profile`) are supported.
3. **Web identity token**: `AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN` (EKS IRSA).
4. **Container credentials**: the ECS task role, EKS Pod Identity (`AWS_CONTAINER_CREDENTIALS_*`).
5. **EC2 instance metadata (IMDS)**: the instance profile role.

The SDK caches the credentials and refreshes temporary ones before they expire; long-running
consumers need no special handling.

Watch out: environment variables come before the files, so `AWS_ACCESS_KEY_ID` in the environment
wins over `profile=`. Pass `credentials=fanferro.Credentials(...)` to bypass the chain entirely
(emulators and tests; prefer the chain in production).

Without `region=`, the region comes from `AWS_REGION` (or `AWS_DEFAULT_REGION`), then the
profile, then instance metadata. If none provides one, connecting fails with
`no AWS region configured; pass region=... or set AWS_REGION`. The client connects lazily, on
`async with client:`, `await client.connect()` or the first operation, so this error appears
there, not when `Client(...)` is constructed. Set `region=` explicitly in multi-region code.

Without `account_id=`, fanferro looks the account id up once with STS `GetCallerIdentity` when it
needs it (topic ARNs, assumed queue URLs). Pass `account_id=` to skip that call. For
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
