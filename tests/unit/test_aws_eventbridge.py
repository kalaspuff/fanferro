"""EventBridge events: unwrapped like an envelope, detail decoded into the handler's class."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

import fanferro
from fanferro import aws, testing
from fanferro.envelopes import AwsNotifications
from tests.aws_samples import BOUNCE, EVENTBRIDGE_S3_CREATED, EVENTBRIDGE_S3_DELETED, MAIL, eventbridge


@fanferro.message_type("InvoicePaid")
@dataclass
class InvoicePaid:
    invoice_id: str
    amount: float


def unwrap(document: str | dict[str, object]) -> fanferro.Payload:
    body = document if isinstance(document, str) else json.dumps(document)
    payload = AwsNotifications().unwrap(fanferro.WireMessage(body))
    assert payload is not None
    return payload


class TestS3ObjectEvent:
    def test_parses_aws_examples(self) -> None:
        created = aws.S3ObjectEvent.from_json(json.loads(unwrap(EVENTBRIDGE_S3_CREATED).data))
        assert created.bucket == aws.S3EventBridgeBucket(
            name="amzn-s3-demo-bucket1", aws_generated_tags={"aws:cloudformation:stack-name": "amzn-s3-demo-stack"}
        )
        assert created.object == aws.S3EventBridgeObject(
            key="example-key",
            size=5,
            etag="b1946ac92492d2347c6235b4d2611184",
            version_id="IYV3p45BT0ac8hjHg1houSdS1a.Mro8e",
            sequencer="617f08299329d189",
        )
        assert (created.reason, created.requester, created.source_ip_address) == (
            "PutObject",
            "123456789012",
            "192.0.2.1",
        )
        assert (created.event_version, created.request_id) == ("1.2", "N4N7GDK58NMKJ12R")
        assert created.sequence == 0x617F08299329D189
        deleted = aws.S3ObjectEvent.from_json(json.loads(unwrap(EVENTBRIDGE_S3_DELETED).data))
        assert (deleted.reason, deleted.deletion_type) == ("Lifecycle Expiration", "Delete Marker Created")
        assert deleted.object.size is None

    def test_keys_are_literal(self) -> None:
        document = json.loads(EVENTBRIDGE_S3_CREATED)
        document["detail"]["object"]["key"] = "a b+c%20.txt"
        event = aws.S3ObjectEvent.from_json(json.loads(unwrap(document).data))
        assert event.object.key == "a b+c%20.txt"
        assert event.to_json()["object"]["key"] == "a b+c%20.txt"

    def test_round_trips(self) -> None:
        detail = json.loads(EVENTBRIDGE_S3_CREATED)["detail"]
        assert aws.S3ObjectEvent.from_json(detail).to_json() == detail

    def test_routes_match_every_s3_event_type(self) -> None:
        types = fanferro.Client(region="us-east-1").types
        names = types.names_of(aws.S3ObjectEvent)
        assert names[0] == "aws.s3.object_created"
        assert {"aws.s3.object_deleted", "aws.s3.object_restore_completed", "aws.s3.object_tags_added"} <= set(names)


class TestMetadata:
    async def test_handlers_get_the_detail_and_the_wrapper(self) -> None:
        received: list[tuple[InvoicePaid, aws.EventBridgeMetadata | None, fanferro.Message[InvoicePaid]]] = []
        router = fanferro.Router()

        @router.route
        async def on_paid(message: fanferro.Message[InvoicePaid]) -> None:
            received.append((message.body, aws.EventBridgeMetadata.of(message), message))

        client = testing.memory_client()
        queue = client.queue("events")
        document = eventbridge(
            "com.acme.billing", "InvoicePaid", {"invoice_id": "i-1", "amount": 9.5}, resources=["arn:a", "arn:b"]
        )
        async with client:
            await queue.send(json.dumps(document), envelope=AwsNotifications())
            await client.consumer(queue, router, wait_time=1).run_until_idle(idle_for=0.3, timeout=10)
        [(body, metadata, message)] = received
        assert body == InvoicePaid(invoice_id="i-1", amount=9.5)
        assert (message.id, message.type, message.source) == (document["id"], "InvoicePaid", "com.acme.billing")
        assert message.time == datetime(2026, 11, 1, 12, tzinfo=UTC)
        assert metadata == aws.EventBridgeMetadata(
            id=str(document["id"]),
            detail_type="InvoicePaid",
            source="com.acme.billing",
            time=datetime(2026, 11, 1, 12, tzinfo=UTC),
            account="111122223333",
            region="eu-north-1",
            resources=("arn:a", "arn:b"),
        )

    def test_other_messages_have_none(self) -> None:
        assert aws.EventBridgeMetadata.of(testing.build_message({"a": 1})) is None


def test_ses_email_events_decode_into_the_ses_classes() -> None:
    document = eventbridge("aws.ses", "Email Bounced", {"eventType": "Bounce", "bounce": BOUNCE, "mail": MAIL})
    payload = unwrap(document)
    assert payload.message_type == "aws.ses.bounce"
    notification = aws.SesBounceNotification.from_json(json.loads(payload.data))
    assert notification.event_publishing
    assert notification.bounce.permanent
