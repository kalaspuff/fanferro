"""fanferro.aws: typed S3 event and SES notification bodies, and their codec."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

import fanferro
from fanferro import aws, testing
from fanferro.codecs import AwsNotificationCodec, CodecRegistry
from tests.aws_samples import BOUNCE, COMPLAINT, DELIVERY, MAIL, S3_PUT, S3_TEST, ses


class TestS3:
    def test_parses_aws_example(self) -> None:
        event = aws.S3Event.from_json(json.loads(S3_PUT))
        [record] = event.records
        assert record.event_name == "ObjectCreated:Put"
        assert record.event_time == datetime(1970, 1, 1, tzinfo=UTC)
        assert (record.created, record.removed, record.category) == (True, False, "ObjectCreated")
        assert record.bucket == aws.S3Bucket(
            name="amzn-s3-demo-bucket", arn="arn:aws:s3:::amzn-s3-demo-bucket", owner_principal_id="A3NL1KOZZKExample"
        )
        assert record.object.key == "HappyFace.jpg"
        assert record.object.size == 1024
        assert record.object.etag == "d41d8cd98f00b204e9800998ecf8427e"
        assert record.object.version_id == "096fKKXTRTtl3on89fVO.nfljtsv6qko"
        assert record.sequence == 0x0055AED6DCD90281E5
        assert (record.principal_id, record.source_ip_address) == ("AIDAJDPLRKLG7UEXAMPLE", "172.16.0.1")
        assert (record.request_id, record.configuration_id, record.aws_region) == (
            "C3D13FE58DE4C810",
            "testConfigRule",
            "us-west-2",
        )
        assert event.test_event is None

    def test_round_trips_to_the_same_document(self) -> None:
        document = json.loads(S3_PUT)
        assert aws.S3Event.from_json(document).to_json() == document

    def test_keys_are_url_decoded(self) -> None:
        document = json.loads(S3_PUT)
        document["Records"][0]["s3"]["object"]["key"] = "photos/red+flower%281%29.jpg"
        event = aws.S3Event.from_json(document)
        assert event.records[0].object.key == "photos/red flower(1).jpg"
        assert event.to_json()["Records"][0]["s3"]["object"]["key"] == "photos/red+flower%281%29.jpg"

    def test_test_events_have_no_records(self) -> None:
        event = aws.S3Event.from_json(json.loads(S3_TEST))
        assert event.records == ()
        assert event.test_event == aws.S3TestEvent(
            bucket="amzn-s3-demo-bucket",
            time=datetime(2014, 10, 13, 15, 57, 2, 89000, tzinfo=UTC),
            request_id="5582815E1AEA5ADF",
            host_id="8cLeGAmw098X5cv4Zkwcmo8vvZa3eH3eKxsPzbB9wrR+YstdA6Knx4Ip8EXAMPLE",
        )
        assert event.to_json() == json.loads(S3_TEST)

    def test_deleted_objects_and_restores(self) -> None:
        document = json.loads(S3_PUT)
        record = document["Records"][0]
        record["eventName"] = "ObjectRestore:Completed"
        del record["s3"]["object"]["size"], record["s3"]["object"]["sequencer"], record["userIdentity"]
        record["glacierEventData"] = {
            "restoreEventData": {
                "lifecycleRestorationExpiryTime": "2026-11-08T00:00:00.000Z",
                "lifecycleRestoreStorageClass": "GLACIER",
            }
        }
        [parsed] = aws.S3Event.from_json(document).records
        assert (parsed.object.size, parsed.sequence, parsed.principal_id) == (None, None, None)
        assert parsed.category == "ObjectRestore"
        assert parsed.restore == aws.S3RestoreEventData(
            lifecycle_restoration_expiry_time=datetime(2026, 11, 8, tzinfo=UTC),
            lifecycle_restore_storage_class="GLACIER",
        )

    def test_invalid_documents(self) -> None:
        with pytest.raises(fanferro.DecodeError, match=r"Records\.0\.s3\.bucket: missing required field"):
            aws.S3Event.from_json({"Records": [{"eventName": "x", "eventTime": "2026-01-01T00:00:00Z"}]})
        with pytest.raises(fanferro.DecodeError, match="expected a time"):
            aws.S3Event.from_json({"Records": [{**json.loads(S3_PUT)["Records"][0], "eventTime": "yesterday"}]})
        with pytest.raises(fanferro.DecodeError, match="expected a list"):
            aws.S3Event.from_json({"Records": {}})
        for time in (1e30, -1e20, float("inf"), float("nan")):  # numbers out of datetime's range
            with pytest.raises(fanferro.DecodeError, match="expected a time"):
                aws.S3Event.from_json({"Records": [{**json.loads(S3_PUT)["Records"][0], "eventTime": time}]})
        record = json.loads(S3_PUT)["Records"][0]
        for size in ("²", "1.5", "", "--1"):
            record["s3"]["object"]["size"] = size
            with pytest.raises(fanferro.DecodeError, match="expected int"):
                aws.S3Event.from_json({"Records": [record]})
        record["s3"]["object"]["size"] = " 2048 "
        assert aws.S3Event.from_json({"Records": [record]}).records[0].object.size == 2048


class TestSes:
    def test_bounce_notification(self) -> None:
        notification = aws.SesBounceNotification.from_json(ses("Bounce", BOUNCE))
        assert notification.notification_type == "Bounce"
        assert not notification.event_publishing
        bounce = notification.bounce
        assert (bounce.permanent, bounce.transient) == (True, False)
        assert (bounce.bounce_type, bounce.bounce_sub_type) == ("Permanent", "General")
        assert bounce.bounced_recipients == (
            aws.SesBouncedRecipient(
                email_address="recipient1@example.com",
                action="failed",
                status="5.0.0",
                diagnostic_code="smtp; 550 user unknown",
            ),
            aws.SesBouncedRecipient(email_address="recipient2@example.com", action="delayed", status="4.0.0"),
        )
        assert (bounce.reporting_mta, bounce.remote_mta_ip) == ("example.com", "127.0.2.0")
        assert bounce.timestamp == datetime(2012, 5, 25, 14, 59, 38, 605000, tzinfo=UTC)

    def test_mail(self) -> None:
        mail = aws.SesBounceNotification.from_json(ses("Bounce", BOUNCE)).mail
        # The format of SES feedback notifications' example: "2018-10-08T14:05:45 +0000".
        assert mail.timestamp == datetime(2018, 10, 8, 14, 5, 45, tzinfo=UTC)
        assert mail.message_id == "000001378603177f-7a5433e7-8edb-42ae-af10-f0181f34d6ee-000000"
        assert (mail.source, mail.source_ip, mail.sending_account_id) == (
            "sender@example.com",
            "127.0.3.0",
            "123456789012",
        )
        assert mail.destination == ("recipient@example.com",)
        assert mail.headers_truncated is False
        assert mail.headers[1] == aws.SesHeader(name="Subject", value="Hello")
        assert mail.common_headers is not None
        assert mail.common_headers.from_ == ("Sender Name <sender@example.com>",)
        assert mail.common_headers.subject == "Message sent using SES"
        assert mail.tags == {}

    def test_complaint_and_delivery(self) -> None:
        complaint = aws.SesComplaintNotification.from_json(ses("Complaint", COMPLAINT)).complaint
        assert complaint.complained_recipients == (aws.SesComplainedRecipient(email_address="recipient1@example.com"),)
        assert complaint.complaint_feedback_type == "abuse"
        assert complaint.arrival_date == datetime(2009, 12, 3, 4, 24, 21, tzinfo=timezone(timedelta(hours=-5)))
        assert complaint.complaint_sub_type is None
        delivery = aws.SesDeliveryNotification.from_json(ses("Delivery", DELIVERY)).delivery
        assert delivery.processing_time_millis == 546
        assert delivery.recipients == ("success@simulator.amazonses.com",)
        assert delivery.smtp_response == "250 ok:  Message 64111812 accepted"

    def test_event_publishing_format(self) -> None:
        document = ses("Delivery", DELIVERY, event_publishing=True)
        document["mail"] = {**MAIL, "headersTruncated": "true", "tags": {"ses:configuration-set": ["marketing"]}}
        notification = aws.SesDeliveryNotification.from_json(document)
        assert notification.event_publishing
        assert notification.notification_type == "Delivery"
        assert notification.mail.headers_truncated is True  # documented as a string there
        assert notification.mail.tags == {"ses:configuration-set": ("marketing",)}
        dumped = notification.to_json()
        assert dumped["eventType"] == "Delivery"
        assert "notificationType" not in dumped
        assert aws.SesDeliveryNotification.from_json(dumped) == notification

    @pytest.mark.parametrize(
        ("cls", "kind", "details"),
        [
            (aws.SesBounceNotification, "Bounce", BOUNCE),
            (aws.SesComplaintNotification, "Complaint", COMPLAINT),
            (aws.SesDeliveryNotification, "Delivery", DELIVERY),
        ],
    )
    def test_round_trips(self, cls: type[aws.SesNotification], kind: str, details: dict[str, object]) -> None:
        notification = cls.from_json(ses(kind, details))
        assert cls.from_json(notification.to_json()) == notification

    def test_any_notification_decodes_as_ses_notification(self) -> None:
        notification = aws.SesNotification.from_json(ses("Open", {"ipAddress": "192.0.2.1"}, event_publishing=True))
        assert (notification.notification_type, notification.event_publishing) == ("Open", True)
        assert notification.mail.message_id.startswith("000001378603177f")

    def test_a_notification_of_another_kind_is_a_decode_error(self) -> None:
        with pytest.raises(fanferro.DecodeError, match="bounce: missing required field"):
            aws.SesBounceNotification.from_json(ses("Complaint", COMPLAINT))


class TestCodec:
    def test_is_a_default_codec_with_the_envelope_type_names(self) -> None:
        registry = CodecRegistry()
        assert any(isinstance(codec, AwsNotificationCodec) for codec in registry.codecs)
        codec = AwsNotificationCodec()
        assert codec.type_name(aws.S3Event) == "aws.s3.event"
        assert codec.type_name(aws.SesBounceNotification) == "aws.ses.bounce"
        assert codec.type_name(aws.SesComplaintNotification) == "aws.ses.complaint"
        assert codec.type_name(aws.SesDeliveryNotification) == "aws.ses.delivery"
        assert codec.type_name(aws.SesNotification) is None
        assert codec.type_name(aws.S3Bucket) is None

    def test_encodes_and_decodes(self) -> None:
        registry = CodecRegistry()
        event = aws.S3Event.from_json(json.loads(S3_PUT))
        encoded = registry.encode(event)
        assert encoded.content_type == "application/json"
        assert json.loads(encoded.data) == json.loads(S3_PUT)
        assert registry.decode(encoded.data, aws.S3Event, "application/json") == event
        with pytest.raises(fanferro.DecodeError, match="not valid JSON"):
            registry.decode(b"{", aws.S3Event, "application/json")

    async def test_handlers_get_typed_bodies(self) -> None:
        received: list[aws.SesBounceNotification] = []
        router = fanferro.Router()

        @router.route
        async def on_bounce(message: fanferro.Message[aws.SesBounceNotification]) -> None:
            received.append(message.body)

        notification = aws.SesBounceNotification.from_json(ses("Bounce", BOUNCE))
        message = testing.build_message(notification)
        assert message.type == "aws.ses.bounce"
        result = await testing.dispatch(router, message)
        assert result.outcome == "acked"
        assert received == [notification]
