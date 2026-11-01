# Deployment

## Processes, threads and fork

The Rust core runs a Tokio runtime with 2-4 worker threads per process (`FANFERRO_WORKER_THREADS`).
Results reach the event loop through a socket pair the loop watches: two file descriptors per
event loop that awaits fanferro operations, released after the loop is closed.
Create clients **after** forking (gunicorn/uvicorn workers, multiprocessing): a client created
before `fork()` raises `ShutdownError` in the child instead of sharing connections with the parent.
fanferro's errors pickle, so one raised in a process pool worker or a multiprocessing child reaches
the parent as itself, with its attributes (see the [error reference](../reference/errors.md)).

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

## Health checks

`consumer.health()` and `client.health()` (every consumer of the client) tell liveness and readiness
probes, and dashboards, how consumers are doing, without an AWS call:

```python
health = client.health()            # fanferro.ClientHealth; consumer.health() is a ConsumerHealth
health.healthy()                    # liveness: restart the process while this stays False
health.ready()                      # readiness: running, not paused, healthy
health.problems()                   # why not, e.g. ("consumer orders has received nothing from queue orders for 75 s: 9 receives failed (last: ResourceNotFoundError AWS.SimpleQueueService.NonExistentQueue)",)
health.consumers["orders"].last_error   # fanferro.ReceiveFailure: error_type, category, code, retryable, at
json.dumps(health.as_dict())        # times as ISO 8601 strings
```

A consumer is **healthy** while it is starting, running or stopping and, while it runs:

* a receive succeeded (an empty long poll counts) within `max_receive_gap` (60 s by default). The
  time it waits for free in-flight slots (handlers keep it busy), the rate limiter or a resume does
  not count, so busy and paused consumers stay healthy. Receives that keep failing (a deleted
  queue, revoked credentials, a missing `sqs:ReceiveMessage` permission) make it unhealthy.
* its dispatcher, which hands messages to handlers on the event loop, ran within `max_dispatch_gap`
  (30 s by default). It runs at least twice a second while the loop is responsive; a blocking or
  CPU-bound handler holds it up.

A consumer that was never started, or has stopped, is not healthy. A paused or stopping consumer is
healthy but not **ready**. Both limits are arguments of `healthy()`, `ready()` and `problems()`.

An HTTP endpoint for Kubernetes or ECS probes needs no web framework:

```python
async def serve_health(client: fanferro.Client, port: int = 8080) -> asyncio.Server:
    async def respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        request = await reader.readline()                      # b"GET /livez HTTP/1.1"
        health = client.health()
        ok = health.ready() if b" /readyz" in request else health.healthy()
        body = json.dumps({"ok": ok, "problems": health.problems()}).encode()
        status = b"200 OK" if ok else b"503 Service Unavailable"
        writer.write(b"HTTP/1.1 " + status + b"\r\nContent-Type: application/json\r\nContent-Length: %d"
                     b"\r\nConnection: close\r\n\r\n" % len(body) + body)
        await writer.drain()
        writer.close()

    return await asyncio.start_server(respond, "0.0.0.0", port)
```

It answers on the event loop, so a loop blocked for longer than the probe's timeout fails the probe
by not answering. Where probes run a command instead (ECS container health checks), touch a file
from a thread while the client is healthy and check its age: `health()` is safe to call from
another thread, and a thread notices a blocked loop through the dispatch gap.

```python
def touch_while_healthy(client: fanferro.Client, path: str, stop: threading.Event) -> None:
    while not stop.wait(5):
        if client.health().healthy():
            pathlib.Path(path).touch()
# healthCheck: ["CMD-SHELL", "test $(( $(date +%s) - $(stat -c %Y /tmp/healthy) )) -lt 30"]
```

By default a consumer whose receives fail keeps retrying (with backoff up to 30 s), since a queue
may be recreated or a permission granted, and a probe decides when to restart. To have the process
end instead, set `max_receive_failures`: after that many failed receives in a row, the last of which
is not retryable (the queue does not exist, access is denied; throttling and network errors are
retryable), the consumer emits `consumer.error`, stops gracefully, and `consumer.run()` and
`client.run()` (which stops its other consumers) raise that error, so the orchestrator restarts the
task:

```python
consumer = client.consumer(queue, router, max_receive_failures=5)
await consumer.run()   # raises fanferro.ResourceNotFoundError once the queue is gone for 5 receives
```

## Visibility timeout and heartbeats

Choose a visibility timeout that covers retry delays you want SQS to apply (30-60 s is typical);
heartbeats handle long processing. A short timeout recovers crashed workers' messages sooner; a long
one tolerates longer stalls (GC, network partitions) before redelivery.

## Throughput tuning

* I/O-bound handlers: raise `concurrency` (100+ is fine on asyncio). Receives follow: while they
  come back full, extra receive loops run (up to `max_pollers`), so `pollers` rarely needs
  raising; it sets the long polls that always run, also on an idle queue.
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
client = fanferro.Client(
    queue_prefix=f"{env}-",
    topic_prefix=f"{env}-",
    bucket_prefix=f"{env}-",
    large_payloads=fanferro.LargePayloads("my-app-payloads"),  # AWS: staging-my-app-payloads
)

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
  the publisher's, `bucket_prefix` included: the relay uploads large payloads.
* The prefix counts toward the AWS limits: at most 80 characters for a queue name and 256 for a
  topic name, a `.fifo` suffix included. fanferro checks every name when it is declared, before
  any network call, and the error says what to shorten:
  `queue name '...' (prefix 'staging-' + '...') is 83 characters; SQS allows 80 - shorten the prefix or the name`.
* `bucket_prefix` is prepended to the bucket of every `LargePayloads` setting, the client's and
  those of topics and queues; `LargePayloads(..., prefixed=False)` opts a shared bucket out (then
  give each environment its own key prefix: `prefix=f"{env}/fanferro/"`). S3 bucket names are
  global and stricter than queue names: the prefix may only contain lowercase letters, digits,
  `-` and `.`, and the bucket name, prefix included, is checked against the 63 characters S3
  allows when it is declared. Consumers download a payload from the bucket its pointer names, so
  they read it whatever their own prefix.

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

A Lambda function with an SQS event source mapping consumes a queue with
`fanferro.aws_lambda.sqs_handler`: Lambda receives the messages, and fanferro processes each record
of the event as a consumer processes a message it received (SNS notifications are unwrapped,
offloaded payloads downloaded, base64 undone, envelopes unwrapped; then the attempts check,
routing, codecs, middleware, the handler, and the retry and dead-letter policies), concurrently up
to `concurrency` (FIFO queues: in order per message group).

```python
import fanferro

client = fanferro.Client(source="urn:service:billing")    # at module level: reused while warm
payments = client.queue("payments", dead_letter=fanferro.DeadLetterQueue("payments-dlq"))
router = fanferro.Router()


@router.route(PaymentCaptured)
async def book(payment: PaymentCaptured) -> None: ...


handler = fanferro.aws_lambda.sqs_handler(client, router, queue=payments)  # handler(event, context)
```

The function returns `{"batchItemFailures": [...]}`, so the event source mapping must report batch
item failures (`FunctionResponseTypes: ["ReportBatchItemFailures"]`; without it Lambda ignores the
response and deletes every record of an invocation that returned, retried ones too). For a mapping
without it, pass `report_batch_item_failures=False`: an invocation with failed records then raises
`BatchItemFailuresError`, and the whole batch comes back.

| Outcome of a record | Response | Effect |
| --- | --- | --- |
| acked (handler returned), discarded, duplicate | not listed | Lambda deletes it |
| dead-lettered (`Reject`, exhausted retries, decode errors) | not listed | fanferro forwarded a copy to the dead-letter queue first (with `fanferro.dead-letter`); without one, `on_missing_queue` applies |
| requeued | not listed | fanferro sent a copy with the next attempt number first |
| retried (an exception, `Retry`) | listed | its visibility timeout is set to the retry policy's backoff (`ChangeMessageVisibility`; if that fails, it comes back after the queue's visibility timeout) |
| released, not settled (`auto_ack=False`), settlement failed | listed | it comes back |
| still running `timeout_margin` (1 s) before the invocation times out | listed | the handler is cancelled (`consumer.error`) |
| FIFO: a later record of a group with a listed or requeued record | listed, unprocessed | the group comes back in order |

* The handler keeps one event loop between invocations, and with it the client's connections; Rust
  events and running event hooks are flushed before each invocation returns. In code that runs an
  event loop already (tests, async adapters), `await handler.handle(event, context)`.
* The queue is resolved the first time records of it arrive (a cold start), like a consumer's,
  with its dead-letter queue (`DeadLetterPolicy(queue=...)`, the declaration's, or the redrive
  policy's target). Without `queue=`, the handler takes the queue each record names
  (`eventSourceARN`): a declared queue of that name, or the queue as it exists.
* Keep `RetryPolicy.max_attempts` below the redrive policy's `maxReceiveCount`, as for consumers,
  and the queue's visibility timeout at least six times the function timeout (AWS's advice:
  Lambda may retry an invocation of a batch that is still invisible).
* Besides what the event source mapping needs (`sqs:ReceiveMessage`, `sqs:DeleteMessage`,
  `sqs:GetQueueAttributes`), the function's role needs `sqs:GetQueueUrl` (queue resolution, unless
  the queue is declared with `Provisioning.assume()`), `sqs:ChangeMessageVisibility` (retry
  backoff), `sqs:SendMessage` on the dead-letter queue (and on the queue for `Requeue`),
  `s3:GetObject` on payload buckets (claim checks), and `kms:Decrypt` for encrypted queues.
* Test functions with `fanferro.testing.invoke_sqs_handler(handler, queue)`, which receives a
  batch, invokes the handler and deletes what it did not report, like the event source mapping
  (see [testing](testing.md#lambda-functions) and `examples/lambda_function.py`).

Publishing from Lambda needs nothing special: publish in a handler (it runs on the handler's
loop), or create the client outside the function's handler and `asyncio.run(...)` the publish per
invocation.
