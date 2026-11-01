# Notifications of AWS services: S3 events and SES email

S3 (bucket event notifications) and SES (bounce, complaint and delivery notifications, and the
events a configuration set publishes) send JSON documents to SNS topics and SQS queues. fanferro
recognises them and decodes them into typed objects of `fanferro.aws`, so receiving them is like
receiving any typed message:

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

## Setting up the subscription

Subscribe the queue to the topic S3 or SES publishes to like to any topic, raw message delivery
or not; or let S3 send to the queue itself. The queue's policy must allow S3 to send to it (a
topic's or queue's resource policy for S3 and SES is not something fanferro provisions).

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
