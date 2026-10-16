# Deployment

## Processes, threads and fork

The Rust core runs a Tokio runtime with 2-4 worker threads per process (`FANFERRO_WORKER_THREADS`).
Results reach the event loop through a socket pair the loop watches: two file descriptors per
event loop that awaits fanferro operations, released after the loop is closed.
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

## Several environments in one AWS account

When environments share an account (a dev environment per developer, several dev stacks,
staging), give the client the environment's prefixes. fanferro prepends them to the name of every
queue and topic the application declares, dead-letter queues included, and to every name passed
as a string (`client.subscription`, `client.consumer`, `topic.subscribe`, `queue.subscribe_to`,
`dead_letter_queue=`, `DeadLetterPolicy(queue=...)`):

```python
env = os.environ["APP_ENV"]
client = fanferro.Client(queue_prefix=f"{env}-", topic_prefix=f"{env}-")

orders = client.topic("orders")  # AWS: staging-orders
worker = client.queue(
    "orders-worker",  # AWS: staging-orders-worker
    dead_letter=fanferro.DeadLetterQueue("orders-worker-dlq"),  # AWS: staging-orders-worker-dlq
)
client.subscription("orders", "orders-worker")  # the same handles as above
consumer = client.consumer("orders-worker", handle)  # the same queue
events = client.queue("events.fifo")  # AWS: staging-events.fifo
audit = client.topic("shared-audit", prefixed=False)  # shared by all environments
```

* The prefix is prepended as is (no separator is added) and may only contain letters, digits,
  `-` and `_`. Write names without it everywhere: fanferro never checks whether a name already
  starts with the prefix, so `client.queue("staging-orders")` becomes `staging-staging-orders`.
* `prefixed=False` on `client.queue`, `client.topic` or `DeadLetterQueue` opts a resource out:
  one that all environments share, or one named by someone else (another account's queue, for
  example). A name passed as a string always gets the prefix, so pass such resources as
  handles: `client.subscription(audit, worker)`. `DeadLetterPolicy(queue=...)` takes a name, so
  a shared dead-letter queue is declared on its queue instead.
* Every environment that declares a shared resource provisions it with its own spec. Declare
  it the same way everywhere, or let one environment own it. A shared queue's dead-letter queue
  is usually shared too, so opt both out; otherwise each environment points the queue's
  redrive policy at its own dead-letter queue:
  ```python
  archive = client.queue(
      "shared-archive",
      prefixed=False,
      dead_letter=fanferro.DeadLetterQueue("shared-archive-dlq", prefixed=False),
  )
  ```
* A queue given by `url=` or a topic given by `arn=` is never prefixed: the URL or ARN already
  names the resource.
* Everything after the declaration uses the AWS name, prefix included: `queue.name`,
  `client.queues` and `client.topics`, events and logs, provisioning reports, OpenTelemetry spans
  and metrics (`messaging.destination.name`, so dashboards show one series per environment), the
  default consumer name, dead-letter metadata, and the `destination_name` of prepared (outbox)
  messages. Matchers compare AWS names as well: `m.topic() == orders.name`.
* An outbox record holds the ARN or URL of its destination, so a relay publishes to the
  environment the message was prepared in. It finds the destination's settings (such as
  `large_payloads`) by the prefixed name, though, so give the relay's client the same prefixes as
  the publisher's.
* The prefix counts toward the AWS limits: at most 80 characters for a queue name and 256 for a
  topic name, a `.fifo` suffix included. fanferro checks every name when it is declared, before
  any network call, and the error says what to shorten:
  `queue name '...' (prefix 'staging-' + '...') is 83 characters; SQS allows 80 - shorten the prefix or the name`.
* The prefixes apply to queues and topics only. For large payloads, give each environment its own
  bucket or key prefix (`LargePayloads("my-bucket", prefix=f"{env}/fanferro/")`).

The prefix also scopes IAM permissions to one environment. A role for staging (account
`123456789012`) that publishes, consumes and looks resources up:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:GetQueueUrl", "sqs:GetQueueAttributes", "sqs:ListQueueTags", "sqs:SendMessage",
        "sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility"
      ],
      "Resource": "arn:aws:sqs:*:123456789012:staging-*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "sns:Publish", "sns:GetTopicAttributes", "sns:ListTagsForResource",
        "sns:ListSubscriptionsByTopic", "sns:GetSubscriptionAttributes"
      ],
      "Resource": "arn:aws:sns:*:123456789012:staging-*"
    }
  ]
}
```

Where an environment provisions its own resources, add the create and update actions from
[Provisioning → IAM permissions](provisioning.md#iam-permissions). Unless the client has
`account_id=`, it also needs `sts:GetCallerIdentity`, which cannot be scoped to resources.
Choose prefixes that do not start with another environment's prefix: `dev-*` also matches the
resources of `dev-eu-`.

## Lambda

fanferro is designed for long-running consumers. In Lambda with an SQS event source mapping,
use fanferro for publishing (create the client outside the handler and `asyncio.run(...)` the
publish per invocation) and read records directly: with the default raw envelope the record body
is the message (e.g. the JSON document) and `fanferro.id` / `fanferro.type` are in its
`messageAttributes`; with CloudEvents, parse the body with
`fanferro.envelopes.CloudEvents().unwrap(fanferro.WireMessage(record["body"]))`.
