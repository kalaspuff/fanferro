"""Notifications as AWS documents them (examples of the S3 and SES developer guides), for tests."""

from __future__ import annotations

from typing import Any

S3_PUT = '{"Records":[{"eventVersion":"2.6","eventSource":"aws:s3","awsRegion":"us-west-2","eventTime":"1970-01-01T00:00:00.000Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AIDAJDPLRKLG7UEXAMPLE"},"requestParameters":{"sourceIPAddress":"172.16.0.1"},"responseElements":{"x-amz-request-id":"C3D13FE58DE4C810","x-amz-id-2":"FMyUVURIY8/IgAtTv8xRjskZQpcIZ9KG4V5Wp6S7S/JRWeUWerMUE5JgHvANOjpD"},"s3":{"s3SchemaVersion":"1.0","configurationId":"testConfigRule","bucket":{"name":"amzn-s3-demo-bucket","ownerIdentity":{"principalId":"A3NL1KOZZKExample"},"arn":"arn:aws:s3:::amzn-s3-demo-bucket"},"object":{"key":"HappyFace.jpg","size":1024,"eTag":"d41d8cd98f00b204e9800998ecf8427e","versionId":"096fKKXTRTtl3on89fVO.nfljtsv6qko","sequencer":"0055AED6DCD90281E5"}}}]}'  # noqa: E501 - verbatim
S3_TEST = '{"Service":"Amazon S3","Event":"s3:TestEvent","Time":"2014-10-13T15:57:02.089Z","Bucket":"amzn-s3-demo-bucket","RequestId":"5582815E1AEA5ADF","HostId":"8cLeGAmw098X5cv4Zkwcmo8vvZa3eH3eKxsPzbB9wrR+YstdA6Knx4Ip8EXAMPLE"}'  # noqa: E501 - verbatim
MAIL: dict[str, Any] = {
    "timestamp": "2018-10-08T14:05:45 +0000",
    "messageId": "000001378603177f-7a5433e7-8edb-42ae-af10-f0181f34d6ee-000000",
    "source": "sender@example.com",
    "sourceArn": "arn:aws:ses:us-east-1:888888888888:identity/example.com",
    "sourceIp": "127.0.3.0",
    "sendingAccountId": "123456789012",
    "destination": ["recipient@example.com"],
    "headersTruncated": False,
    "headers": [{"name": "From", "value": '"Sender Name" <sender@example.com>'}, {"name": "Subject", "value": "Hello"}],
    "commonHeaders": {
        "from": ["Sender Name <sender@example.com>"],
        "date": "Mon, 08 Oct 2018 14:05:45 +0000",
        "to": ["Recipient Name <recipient@example.com>"],
        "messageId": " custom-message-ID",
        "subject": "Message sent using SES",
    },
}
BOUNCE: dict[str, Any] = {
    "bounceType": "Permanent",
    "bounceSubType": "General",
    "bouncedRecipients": [
        {
            "status": "5.0.0",
            "action": "failed",
            "diagnosticCode": "smtp; 550 user unknown",
            "emailAddress": "recipient1@example.com",
        },
        {"status": "4.0.0", "action": "delayed", "emailAddress": "recipient2@example.com"},
    ],
    "reportingMTA": "example.com",
    "timestamp": "2012-05-25T14:59:38.605Z",
    "feedbackId": "000001378603176d-5a4b5ad9-6f30-4198-a8c3-b1eb0c270a1d-000000",
    "remoteMtaIp": "127.0.2.0",
}
COMPLAINT: dict[str, Any] = {
    "userAgent": "ExampleCorp Feedback Loop (V0.01)",
    "complainedRecipients": [{"emailAddress": "recipient1@example.com"}],
    "complaintFeedbackType": "abuse",
    "arrivalDate": "2009-12-03T04:24:21.000-05:00",
    "timestamp": "2012-05-25T14:59:38.623Z",
    "feedbackId": "000001378603177f-18c07c78-fa81-4a58-9dd1-fedc3cb8f49a-000000",
}
DELIVERY: dict[str, Any] = {
    "timestamp": "2014-05-28T22:41:01.184Z",
    "processingTimeMillis": 546,
    "recipients": ["success@simulator.amazonses.com"],
    "smtpResponse": "250 ok:  Message 64111812 accepted",
    "reportingMTA": "a8-70.smtp-out.amazonses.com",
    "remoteMtaIp": "127.0.2.0",
}


def ses(kind: str, details: dict[str, Any] | None, *, event_publishing: bool = False) -> dict[str, Any]:
    """An SES notification: feedback notifications (``notificationType``) or event publishing (``eventType``)."""
    key = "failure" if kind == "Rendering Failure" else kind[0].lower() + kind[1:]
    document: dict[str, Any] = {("eventType" if event_publishing else "notificationType"): kind, "mail": MAIL}
    if details is not None:
        document[key] = details
    return document
