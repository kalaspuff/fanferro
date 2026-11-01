# Notifications and events of AWS services

S3 (bucket event notifications), SES (bounce, complaint and delivery notifications, and the events
a configuration set publishes), CloudWatch alarms, Auto Scaling lifecycle hooks and Lambda
(destinations and dead-letter queues) send JSON documents to SNS topics and SQS queues, and
EventBridge rules send the events of AWS services, SaaS partners and applications to them. fanferro
recognises them and decodes them into typed objects of `fanferro.aws` or [classes of your
own](#eventbridge-events), so receiving them is like receiving any typed message:

```python
import fanferro
from fanferro import aws

client = fanferro.Client()
consumer = client.consumer(client.queue("uploads"))


@consumer.route
async def on_upload(message: fanferro.Message[aws.S3Event]) -> None:
    for record in message.body.records:  # S3's test event has none
        if record.created:
            await index(record.bucket.name, record.object.key, record.object.size)


bounces = client.consumer(client.queue("email-feedback"))


@bounces.route
async def on_bounce(notification: aws.SesBounceNotification) -> None:
    if notification.bounce.permanent:
        await suppress([recipient.email_address for recipient in notification.bounce.bounced_recipients])


@bounces.route
async def on_complaint(notification: aws.SesComplaintNotification) -> None:
    await suppress([recipient.email_address for recipient in notification.complaint.complained_recipients])


@bounces.route
async def on_delivery(notification: aws.SesDeliveryNotification) -> None:
    await mark_delivered(notification.mail.message_id, notification.delivery.recipients)
```

## How messages are recognised

The `AwsNotifications` envelope, one of the envelopes consumers try by default (before `Raw`),
recognises the notifications by their body, whichever way they arrive: from an SNS topic with or
without raw message delivery, or sent by S3 straight to an SQS queue. It gives each a message type,
a source and the event's time:

| Notification | `message.type` | Body class | `message.source` | `message.time` |
| --- | --- | --- | --- | --- |
| S3 event (`{"Records": [...]}` of `aws:s3` records) | `aws.s3.event` | `aws.S3Event` | `aws:s3` | first record's `eventTime` |
| S3's test event (`s3:TestEvent`, when notifications are set up) | `aws.s3.event` | `aws.S3Event` (no records, `test_event` set) | `aws:s3` | its `Time` |
| SES bounce | `aws.ses.bounce` | `aws.SesBounceNotification` | `aws:ses` | the bounce's `timestamp` |
| SES complaint | `aws.ses.complaint` | `aws.SesComplaintNotification` | `aws:ses` | the complaint's `timestamp` |
| SES delivery | `aws.ses.delivery` | `aws.SesDeliveryNotification` | `aws:ses` | the delivery's `timestamp` |
| other SES events: `Send`, `Reject`, `Open`, `Click`, `Rendering Failure`, `DeliveryDelay`, `Subscription`, `Received` | `aws.ses.send`, ..., `aws.ses.rendering_failure`, `aws.ses.delivery_delay`, ... | `aws.SesNotification` (the type and the mail) or a class of your own | `aws:ses` | the event's `timestamp`, else the mail's |

Routes for these classes match their message types, so a handler annotated with one of them needs
no `message_type=`. Messages of fanferro producers (with a `fanferro.id` or `fanferro.type`
attribute) are never taken for notifications, whatever their body. `message.id` is the SQS message id:
AWS's documents have no id of their own (S3's `sequencer` orders the events of a key, SES's
`feedback_id` identifies a bounce or complaint).

SES sends the same notifications in two formats, an identity's feedback notifications
(`notificationType`) and a configuration set's event publishing (`eventType`); both decode into
the same classes (`notification.event_publishing` tells them apart). Other SES events, and
fields AWS adds later, are in `message.json()`:

```python
@consumer.route("aws.ses.open")
async def on_open(message: fanferro.Message[aws.SesNotification]) -> None:
    opened = message.json()["open"]  # {"ipAddress": ..., "timestamp": ..., "userAgent": ...}
```

## The classes

Frozen dataclasses with the fields AWS documents, in snake case. Fields AWS leaves out are `None`
(lists empty); unknown fields are ignored.

* `S3Event.records` → `S3EventRecord`: `event_name` (`ObjectCreated:Put`, `ObjectRemoved:Delete`,
  ...; `category`, `created` and `removed` read it), `event_time`, `aws_region`, `bucket`
  (`name`, `arn`), `object` (`key`, **URL-decoded**: S3 sends `red+flower.jpg` for `red flower.jpg`;
  `size`, `etag`, `version_id`, `sequencer`), `principal_id`, `source_ip_address`, `request_id`,
  `configuration_id`, `restore` (`ObjectRestore:Completed`). `record.sequence` orders the events of
  one key: the greater happened later.
* `SesBounceNotification.bounce`: `bounce_type` (`permanent`, `transient`), `bounce_sub_type`,
  `bounced_recipients` (`email_address`, `action`, `status`, `diagnostic_code`), `timestamp`,
  `feedback_id`, `reporting_mta`, `remote_mta_ip`.
* `SesComplaintNotification.complaint`: `complained_recipients` (`email_address`),
  `complaint_feedback_type`, `complaint_sub_type`, `user_agent`, `arrival_date`, `timestamp`,
  `feedback_id`.
* `SesDeliveryNotification.delivery`: `recipients`, `processing_time_millis`, `smtp_response`,
  `reporting_mta`, `remote_mta_ip`, `timestamp`.
* Every SES notification's `mail`: `message_id` (what `SendEmail` returned), `timestamp`,
  `source`, `destination`, `headers`, `common_headers` (`from_`, `to`, `subject`, ...), `tags`
  (event publishing), `source_arn`, `sending_account_id`, ...

## EventBridge events

An EventBridge rule whose target is an SQS queue or SNS topic delivers each event as EventBridge
wraps it, whatever sent it: an AWS service, a SaaS partner (Stripe, Auth0, Shopify... through a
partner event source) or an application (`PutEvents`):

```json
{"version": "0", "id": "0d079340-135a-c8c6-95c2-41fb8f496c53", "detail-type": "InvoicePaid",
 "source": "com.acme.billing", "account": "111122223333", "time": "2026-11-01T12:00:00Z",
 "region": "eu-north-1", "resources": [], "detail": {"invoice_id": "i-1", "amount": 9.5}}
```

The `AwsNotifications` envelope unwraps it like a CloudEvent: the body is the `detail`, decoded
into the class the handler is annotated with (a pydantic model, a dataclass, a msgspec struct...),
`message.id` is the event's `id` (EventBridge keeps it when it delivers an event again, so it is
a good idempotency key), `message.time` its `time` and `message.source` its `source`.
`aws.EventBridgeMetadata.of(message)` has the rest: `detail_type`, `account`, `region` and
`resources`.

```python
@fanferro.message_type("InvoicePaid")  # the detail-type
class InvoicePaid(BaseModel):
    invoice_id: str
    amount: Decimal


@consumer.route
async def on_paid(invoice: InvoicePaid) -> None:  # the event's detail
    ...
```

The message type of an event is:

| Source | Message type | Example |
| --- | --- | --- |
| an AWS service (`aws.<service>`) | `aws.<service>.<event>`: the `detail-type` in snake case, without a leading `aws_` or `<service>_` | `ECS Task State Change` from `aws.ecs`: `aws.ecs.task_state_change`; `AWS Health Event` from `aws.health`: `aws.health.event` |
| SES email events (`aws.ses`) | the types of SES's notifications, whose document the `detail` is | `Email Bounced`: `aws.ses.bounce`, decoded into `aws.SesBounceNotification` |
| any other source (applications, SaaS partners) | the `detail-type` as it is | `InvoicePaid`, `invoice.paid` |

S3 events sent through EventBridge are not S3's notifications: one event per message, other
event names and other fields (`reason` such as `PutObject` or `Lifecycle Expiration`,
`deletion-type`, `requester`), events for tags, ACLs and storage classes, and object keys that are
not URL-encoded. They decode into `aws.S3ObjectEvent`, whose routes match every S3 event type
(`aws.s3.object_created`, `aws.s3.object_deleted`, `aws.s3.object_restore_completed`...); to handle
one of them, give the type:

```python
@consumer.route("aws.s3.object_created")
async def on_created(event: aws.S3ObjectEvent) -> None:
    await index(event.bucket.name, event.object.key, event.object.size)
```

### Typed events of AWS services

These events, which applications commonly react to, have classes in `fanferro.aws` (each the
event's `detail`; `message.time` is the event's time):

| Event (`detail-type`) | Message type | Class |
| --- | --- | --- |
| a scheduled rule fired (`Scheduled Event`) | `aws.events.scheduled_event` | `ScheduledEvent` (no fields: the rule's ARN is in `EventBridgeMetadata.of(message).resources`) |
| `ECS Task State Change` | `aws.ecs.task_state_change` | `EcsTaskStateChange`: `last_status`, `stopped`, `stop_code`, `stopped_reason`, `containers` (`exit_code`, `reason`), `group`, `version`... |
| `Step Functions Execution Status Change` (standard workflows) | `aws.states.step_functions_execution_status_change` | `StepFunctionsExecutionStatusChange`: `status`, `succeeded`, `failed`, `error`, `cause`, `start_date`, `stop_date`, `input_json()`, `output_json()` |
| `EC2 Spot Instance Interruption Warning` | `aws.ec2.spot_instance_interruption_warning` | `Ec2SpotInstanceInterruptionWarning`: `instance_id`, `instance_action` |
| `EC2 Instance Rebalance Recommendation` | `aws.ec2.instance_rebalance_recommendation` | `Ec2InstanceRebalanceRecommendation`: `instance_id` |
| `EC2 Instance State-change Notification` | `aws.ec2.instance_state_change_notification` | `Ec2InstanceStateChange`: `instance_id`, `state` |
| `EC2 Instance-launch Lifecycle Action`, `EC2 Instance-terminate Lifecycle Action` | `aws.autoscaling.ec2_instance_launch_lifecycle_action`, `..._terminate_lifecycle_action` | `AutoScalingLifecycleAction`: `launching`, `terminating`, `ec2_instance_id`, `auto_scaling_group_name`, `lifecycle_hook_name`, `lifecycle_action_token` |
| `AWS Health Event`, `AWS Health Abuse Event` | `aws.health.event`, `aws.health.abuse_event` | `HealthEvent`: `service`, `event_type_code`, `event_type_category`, `status_code`, `start_time`, `description`, `affected_entities`, `actionability`... |

Auto Scaling lifecycle hooks can also notify an SQS queue or SNS topic directly instead of
EventBridge; those notifications have the same types and class (with `account_id`, `request_id`
and `time` besides). The test notification a hook sends when it is added is
`aws.autoscaling.test_notification` (`AutoScalingTestNotification`): give it a route, or a
default route, that acknowledges it.

```python
@consumer.route
async def on_terminating(action: aws.AutoScalingLifecycleAction) -> None:
    if action.terminating:
        await drain(action.ec2_instance_id)
        await autoscaling.complete_lifecycle_action(
            LifecycleHookName=action.lifecycle_hook_name,
            AutoScalingGroupName=action.auto_scaling_group_name,
            LifecycleActionToken=action.lifecycle_action_token,
            LifecycleActionResult="CONTINUE",
        )


@consumer.route
async def on_autoscaling_test(notification: aws.AutoScalingTestNotification) -> None:
    pass  # acknowledged
```

Events of other AWS services decode into classes of your own (or `Message[dict]`), routed by
their type (`@consumer.route("aws.batch.job_state_change")`). A rule with an input transformer
sends whatever its template makes, which is not an event: route such messages like any other.

## CloudWatch alarms

An alarm whose action is an SNS topic sends it a notification of its own (PascalCase, with the
metric in `Trigger`): `aws.cloudwatch.alarm`, decoded into `aws.CloudWatchAlarm` (`alarm_name`,
`new_state_value`, `alarming`, `ok`, `new_state_reason`, `old_state_value`, `state_change_time`,
`trigger.metric_name`, `trigger.dimensions`, `trigger.threshold`...). Through EventBridge, an
alarm's state change is `aws.cloudwatch.alarm_state_change`, decoded into
`aws.CloudWatchAlarmStateChange` (`state` and `previous_state` with `value`, `reason`,
`reason_data_json()`, `timestamp`; `configuration`; `alarm_tags`).

```python
@consumer.route
async def on_alarm(alarm: aws.CloudWatchAlarm) -> None:
    if alarm.alarming:
        await page_on_call(alarm.alarm_name, alarm.new_state_reason)
```

## Lambda destinations and dead-letter queues

The destinations of asynchronously invoked Lambda functions (an SQS queue, an SNS topic or
EventBridge) receive a record of each invocation: `aws.lambda.invocation_success` and
`aws.lambda.invocation_failure`, decoded into `aws.LambdaInvocationRecord` (`succeeded`,
`request_context` with `function_arn`, `condition` and `approximate_invoke_count`,
`request_payload`, the event, `response_payload` and `error_message`). The on-failure
destinations of Kinesis and DynamoDB stream event source mappings send records of the same shape,
with the batch in `kinesis_batch_info` or `ddb_stream_batch_info`. A record, lifecycle action or
alarm state change that fanferro publishes (in a test, say) gets the message type of its own kind:
`aws.lambda.invocation_success` for a successful invocation, `..._terminate_lifecycle_action` for a
terminating instance.

A function's dead-letter queue (or topic) receives the event itself, with the `RequestID`,
`ErrorCode` and `ErrorMessage` attributes Lambda adds. Such messages are `aws.lambda.dead_letter`
whatever their body, so that an event a function failed to process (an S3 notification, say) does
not reach the routes of that event again; decode the body into the event's class and read the error
with `aws.LambdaDeadLetter.of(message)`:

```python
@consumer.route("aws.lambda.dead_letter")
async def on_failed_upload(message: fanferro.Message[aws.S3Event]) -> None:
    failure = aws.LambdaDeadLetter.of(message)  # request_id, error_code, error_message
    await report(failure.error_message, message.body.records)
```

## Setting up the subscription

Subscribe the queue to the topic S3 or SES publishes to like to any topic, raw message delivery
or not; or let S3 send to the queue itself, or make it an EventBridge rule's target. The queue's
policy must allow S3 (or EventBridge) to send to it (a topic's or queue's resource policy for S3,
SES and EventBridge is not something fanferro provisions).

```python
client.topic("bucket-events", provisioning=fanferro.Provisioning.existing())
client.subscription("bucket-events", "uploads")
```

## Testing

The classes encode back into AWS's documents. `fanferro.testing.build_message()` and
`fanferro.testing.dispatch()` take them like any body, and publishing one with the
`AwsNotifications` envelope sends it as AWS does (the document, without attributes), so the
consumer under test recognises it:

```python
event = aws.S3Event.from_json(json.loads(path.read_text()))  # a notification captured from S3
await testing.dispatch(consumer, event)
await topic.publish(event, envelope=fanferro.envelopes.AwsNotifications())
```
