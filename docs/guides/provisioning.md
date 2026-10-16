# Provisioning

Resources are declared on the client and resolved lazily (first use) or up front (`async with
client`, `await client.provision()`), according to a `Provisioning` policy.

```python
client = fanferro.Client(provisioning=fanferro.Provisioning.existing())   # default for all resources

orders = client.topic("orders", attributes=fanferro.TopicAttributes(encryption=fanferro.Kms("alias/aws/sns")))
worker = client.queue(
    "orders-worker",
    attributes=fanferro.QueueAttributes(visibility_timeout=60, message_retention=timedelta(days=4)),
    dead_letter=fanferro.DeadLetterQueue("orders-worker-dlq", max_receive_count=8),
    tags={"team": "orders"},
    provisioning=fanferro.Provisioning.ensure(),        # this queue is owned by the app
)
client.subscription(orders, worker, filter_policy={"fanferro.type": ["orders.created.v1"]})
```

Declaring a resource again returns the same handle; settings that differ from the first declaration
raise `ConfigurationError` (configure each resource in one place). Several queues can share a
dead-letter queue: the first declaration of it sets its attributes, and later `DeadLetterQueue`s of
the same name may only repeat them.

## What happens

For every resource: **lookup** (`GetQueueUrl`, `GetTopicAttributes` by ARN derived from the account
id, `ListSubscriptionsByTopic`) → **verify** (read attributes and tags, compute drift) →
**update** drifted mutable attributes and tags (if `update`) → **create** when missing (if
`create`). Queues provision their dead-letter queue first and set `RedrivePolicy`. Subscriptions
also make sure the queue policy lets the topic send (`manage_queue_policy`, a statement with
`aws:SourceArn`), including the subscription's own dead-letter queue. A queue `policy` in
`QueueAttributes` is compared and updated together with those statements, so provisioning the
queue never revokes a subscription's permission.

## Policies

| Preset | lookup | verify | create | update |
| --- | --- | --- | --- | --- |
| `Provisioning.existing()` (default) | ✓ | ✓ | | |
| `Provisioning.create_missing()` | ✓ | ✓ | ✓ | |
| `Provisioning.ensure()` | ✓ | ✓ | ✓ | ✓ |
| `Provisioning.assume()` | | | | |

`assume()` makes no API calls: URLs/ARNs are derived from names, region and account id (pass
`account_id=` to the client to avoid an STS call). Use it for least-privilege deployments.

## Failure modes

Each policy has `on_missing`, `on_drift` and `on_error`, each one of:

* `"raise"` - raise `ProvisioningError` (with `.report`). Default for missing resources and errors.
* `"log"` - emit `resource.missing` / `resource.drift` / `resource.failed` and continue. Missing
  resources raise `ResourceUnavailableError` on use and are looked up again later (with backoff),
  so the application recovers when infrastructure appears. Default for drift.
* `"ignore"` - like log, without the event.
* `fanferro.Fallback(handler)` - call `handler(ProvisioningFailure)` (sync or async); return a
  `QueueInfo`/`TopicInfo` to use instead (e.g. a shared fallback queue) or `None`.

```python
async def regional_fallback(failure: fanferro.ProvisioningFailure) -> fanferro.TopicInfo | None:
    return await other_region_topic.resolve()

orders = client.topic("orders", provisioning=fanferro.Provisioning(on_missing=fanferro.Fallback(regional_fallback)))
```

Reports: `await client.provision()` returns a `ProvisioningReport` per resource (status `found`,
`created`, `updated`, `assumed`, `missing`, `failed`; `drift`; `actions`) or the error;
`resource.last_report` keeps the latest. Teardown helpers: `topic.delete()`, `queue.delete()`,
`queue.purge()`, `subscription.delete()`.

## Attributes

`QueueAttributes`: `visibility_timeout`, `message_retention`, `delay`, `receive_wait_time`,
`max_message_size`, `encryption` (`Kms(key_id, data_key_reuse)` or `SqsManagedEncryption()`),
`content_based_deduplication`, `deduplication_scope`, `fifo_throughput_limit`, `policy`,
`redrive_allow_policy`, `extra` (raw AWS names).
`TopicAttributes`: `display_name`, `encryption`, `content_based_deduplication`, `tracing`
(`"Active"` for X-Ray), `signature_version`, `max_message_size`, `fifo_throughput_scope`,
`policy`, `delivery_policy`, `archive_policy`, `extra`.
Durations accept seconds or `timedelta`. JSON-valued attributes are compared semantically;
`FifoQueue`/`FifoTopic` are immutable and reported as drift instead of updated.

KMS is optional everywhere. With a customer managed key on a queue subscribed to SNS, the key
policy must allow `sns.amazonaws.com` to use it (`kms:GenerateDataKey*`, `kms:Decrypt`) - fanferro
does not modify key policies.

## IAM permissions

| Activity | Actions |
| --- | --- |
| Publish to a topic / send to a queue | `sns:Publish` / `sqs:SendMessage` |
| Consume | `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility`, `sqs:GetQueueAttributes`, `sqs:GetQueueUrl` |
| Dead-letter / requeue | `sqs:SendMessage` on the DLQ / the queue |
| Lookup and verify | `sqs:GetQueueUrl`, `sqs:GetQueueAttributes`, `sqs:ListQueueTags`, `sns:GetTopicAttributes`, `sns:ListTagsForResource`, `sns:ListSubscriptionsByTopic`, `sns:GetSubscriptionAttributes`, `sts:GetCallerIdentity` (unless `account_id` is set) |
| Create / update | `sqs:CreateQueue`, `sqs:SetQueueAttributes`, `sqs:TagQueue`, `sns:CreateTopic`, `sns:SetTopicAttributes`, `sns:TagResource`, `sns:Subscribe`, `sns:SetSubscriptionAttributes` |
| Large payloads | `s3:PutObject` (publisher), `s3:GetObject` (consumer, unless presigned URLs), `s3:DeleteObject` (if deleting), `kms:*` as required by SSE-KMS |

With `Provisioning.assume()` only the data-plane actions are needed. When several environments
share an account, `Client(queue_prefix=..., topic_prefix=...)` names their resources apart and
lets a policy grant one environment's resources only: see
[Deployment → Several environments in one AWS account](deployment.md#several-environments-in-one-aws-account).
