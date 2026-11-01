"""Notifications of AWS services (S3 events, SES email notifications) reach typed handlers.

The documents are published as AWS sends them (the ``AwsNotifications`` envelope: the document,
without attributes) through SNS subscriptions with and without raw message delivery, and straight
to the queue, like S3's SQS destinations.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import fanferro
from fanferro import aws
from fanferro.envelopes import AwsNotifications
from tests.aws_samples import (
    BOUNCE,
    CLOUDWATCH_ALARM,
    COMPLAINT,
    DELIVERY,
    EVENTBRIDGE_S3_CREATED,
    MAIL,
    S3_PUT,
    S3_TEST,
    eventbridge,
    ses,
)
from tests.conftest import ClientFactory
from tests.helpers import consume_until


@fanferro.message_type("InvoicePaid")
@dataclass(frozen=True)
class InvoicePaid:
    invoice_id: str
    amount: float


async def test_typed_handlers_receive_s3_and_ses_notifications(client_factory: ClientFactory, name: Any) -> None:
    client = client_factory()
    topic_name, raw_queue, wrapped_queue = name("aws-events"), name("aws-raw"), name("aws-wrapped")
    topic = client.topic(topic_name)
    client.queue(raw_queue)
    client.queue(wrapped_queue)
    client.subscription(topic_name, raw_queue)
    client.subscription(topic_name, wrapped_queue, raw_message_delivery=False)
    uploads: list[fanferro.Message[aws.S3Event]] = []
    bounces: list[fanferro.Message[aws.SesBounceNotification]] = []
    complaints: list[aws.SesComplaintNotification] = []
    deliveries: list[aws.SesDeliveryNotification] = []
    others: list[fanferro.Message[aws.SesNotification]] = []
    router = fanferro.Router()

    @router.route
    async def on_upload(message: fanferro.Message[aws.S3Event]) -> None:
        uploads.append(message)

    @router.route
    async def on_bounce(message: fanferro.Message[aws.SesBounceNotification]) -> None:
        bounces.append(message)

    @router.route
    async def on_complaint(notification: aws.SesComplaintNotification) -> None:
        complaints.append(notification)

    @router.route
    async def on_delivery(notification: aws.SesDeliveryNotification) -> None:
        deliveries.append(notification)

    @router.default
    async def on_other(message: fanferro.Message[aws.SesNotification]) -> None:
        others.append(message)

    documents = [
        S3_PUT,
        S3_TEST,
        json.dumps(ses("Bounce", BOUNCE)),
        json.dumps(ses("Complaint", COMPLAINT, event_publishing=True)),
        json.dumps(ses("Delivery", DELIVERY)),
        json.dumps(ses("DeliveryDelay", {"delayType": "MailboxFull"}, event_publishing=True)),
    ]
    async with client:
        for document in documents:
            await topic.publish(document, envelope=AwsNotifications())
            await client.queue(raw_queue).send(document, envelope=AwsNotifications())
        # Each document twice on the raw queue (through the topic and sent), once on the other.
        for queue_name, copies in ((raw_queue, 2), (wrapped_queue, 1)):
            counts = (len(uploads), len(bounces), len(others))
            await consume_until(
                client.consumer(client.queue(queue_name), router, wait_time=1),
                lambda counts=counts, copies=copies: (
                    (len(uploads), len(bounces), len(others))
                    == (counts[0] + 2 * copies, counts[1] + copies, counts[2] + copies)
                ),
            )

    assert len(uploads) == 6
    assert len(bounces) == len(complaints) == len(deliveries) == len(others) == 3
    put = next(message for message in uploads if message.body.records)
    assert put.type == "aws.s3.event"
    assert put.envelope == "aws"
    assert put.source == "aws:s3"
    assert put.time == datetime(1970, 1, 1, tzinfo=UTC)
    assert put.body.records[0].object.key == "HappyFace.jpg"
    assert sum(1 for message in uploads if message.body.test_event is not None) == 3
    bounce = bounces[0]
    assert bounce.type == "aws.ses.bounce"
    assert bounce.time == datetime(2012, 5, 25, 14, 59, 38, 605000, tzinfo=UTC)
    assert bounce.body.bounce.permanent
    assert complaints[0].event_publishing
    assert deliveries[0].delivery.processing_time_millis == 546
    assert {message.type for message in others} == {"aws.ses.delivery_delay"}
    assert others[0].body.notification_type == "DeliveryDelay"


async def test_eventbridge_events_reach_handlers_of_their_detail(client_factory: ClientFactory, name: Any) -> None:
    """EventBridge rules deliver to topics (with or without raw delivery) and queues; the detail is the body."""
    client = client_factory()
    topic_name, raw_queue, wrapped_queue = name("eb-events"), name("eb-raw"), name("eb-wrapped")
    topic = client.topic(topic_name)
    client.queue(raw_queue)
    client.queue(wrapped_queue)
    client.subscription(topic_name, raw_queue)
    client.subscription(topic_name, wrapped_queue, raw_message_delivery=False)
    paid: list[fanferro.Message[InvoicePaid]] = []
    uploads: list[fanferro.Message[aws.S3ObjectEvent]] = []
    bounces: list[aws.SesBounceNotification] = []
    router = fanferro.Router()

    @router.route
    async def on_paid(message: fanferro.Message[InvoicePaid]) -> None:
        paid.append(message)

    @router.route
    async def on_upload(message: fanferro.Message[aws.S3ObjectEvent]) -> None:
        uploads.append(message)

    @router.route
    async def on_bounce(notification: aws.SesBounceNotification) -> None:
        bounces.append(notification)

    documents = [
        json.dumps(eventbridge("com.acme.billing", "InvoicePaid", {"invoice_id": "i-1", "amount": 9.5})),
        EVENTBRIDGE_S3_CREATED,
        json.dumps(eventbridge("aws.ses", "Email Bounced", {"eventType": "Bounce", "bounce": BOUNCE, "mail": MAIL})),
    ]
    async with client:
        for document in documents:
            await topic.publish(document, envelope=AwsNotifications())
            await client.queue(raw_queue).send(document, envelope=AwsNotifications())
        for queue_name, copies in ((raw_queue, 2), (wrapped_queue, 1)):
            counts = (len(paid), len(uploads), len(bounces))
            await consume_until(
                client.consumer(client.queue(queue_name), router, wait_time=1),
                lambda counts=counts, copies=copies: (
                    (len(paid), len(uploads), len(bounces)) == tuple(count + copies for count in counts)
                ),
            )

    assert {message.body for message in paid} == {InvoicePaid(invoice_id="i-1", amount=9.5)}
    assert {message.id for message in paid} == {"0d079340-135a-c8c6-95c2-41fb8f496c53"}
    assert all(message.envelope == "aws" for message in paid)
    assert {message.type for message in uploads} == {"aws.s3.object_created"}
    assert uploads[0].body.object.key == "example-key"
    assert all(notification.bounce.permanent for notification in bounces)


async def test_alarms_and_lambda_dead_letters(client_factory: ClientFactory, name: Any) -> None:
    """An alarm's notification through SNS, and Lambda's dead letters with their attributes."""
    client = client_factory()
    topic_name, queue_name = name("alarms"), name("alarms-dlq")
    topic = client.topic(topic_name)
    queue = client.queue(queue_name)
    client.subscription(topic_name, queue_name, raw_message_delivery=False)
    alarms: list[aws.CloudWatchAlarm] = []
    failures: list[tuple[InvoicePaid, aws.LambdaDeadLetter | None]] = []
    router = fanferro.Router()

    @router.route
    async def on_alarm(alarm: aws.CloudWatchAlarm) -> None:
        alarms.append(alarm)

    @router.route("aws.lambda.dead_letter")
    async def on_failed(message: fanferro.Message[InvoicePaid]) -> None:
        failures.append((message.body, aws.LambdaDeadLetter.of(message)))

    dead_letter = {"RequestID": "e4b46cbf", "ErrorCode": 200, "ErrorMessage": "Process exited"}
    async with client:
        await topic.publish(json.dumps(CLOUDWATCH_ALARM), envelope=AwsNotifications())
        # A dead-letter topic's subscriber gets the attributes inside the SNS notification.
        await topic.publish(
            json.dumps({"invoice_id": "i-2", "amount": 1.0}), envelope=AwsNotifications(), attributes=dead_letter
        )
        await queue.send(
            json.dumps({"invoice_id": "i-1", "amount": 9.5}), envelope=AwsNotifications(), attributes=dead_letter
        )
        await consume_until(
            client.consumer(queue, router, wait_time=1), lambda: len(alarms) == 1 and len(failures) == 2
        )

    assert alarms[0].alarming
    assert alarms[0].trigger is not None
    assert alarms[0].trigger.metric_name == "CPUUtilization"
    assert sorted(body.invoice_id for body, _ in failures) == ["i-1", "i-2"]
    assert {failure for _, failure in failures} == {
        aws.LambdaDeadLetter(request_id="e4b46cbf", error_code=200, error_message="Process exited")
    }
