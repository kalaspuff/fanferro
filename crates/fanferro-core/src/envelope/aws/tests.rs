use super::*;
use crate::envelope::{EnvelopeSet, RawEnvelope};
use crate::types::AttributeValue;

/// AWS's example of an `s3:ObjectCreated:Put` event notification.
const S3_PUT: &str = r#"{"Records":[{"eventVersion":"2.6","eventSource":"aws:s3","awsRegion":"us-west-2","eventTime":"1970-01-01T00:00:00.000Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AIDAJDPLRKLG7UEXAMPLE"},"requestParameters":{"sourceIPAddress":"172.16.0.1"},"responseElements":{"x-amz-request-id":"C3D13FE58DE4C810","x-amz-id-2":"FMyUVURIY8/IgAtTv8xRjskZQpcIZ9KG4V5Wp6S7S/JRWeUWerMUE5JgHvANOjpD"},"s3":{"s3SchemaVersion":"1.0","configurationId":"testConfigRule","bucket":{"name":"amzn-s3-demo-bucket","ownerIdentity":{"principalId":"A3NL1KOZZKExample"},"arn":"arn:aws:s3:::amzn-s3-demo-bucket"},"object":{"key":"HappyFace.jpg","size":1024,"eTag":"d41d8cd98f00b204e9800998ecf8427e","versionId":"096fKKXTRTtl3on89fVO.nfljtsv6qko","sequencer":"0055AED6DCD90281E5"}}}]}"#;

/// The message S3 sends after a notification is configured.
const S3_TEST: &str = r#"{"Service":"Amazon S3","Event":"s3:TestEvent","Time":"2014-10-13T15:57:02.089Z","Bucket":"amzn-s3-demo-bucket","RequestId":"5582815E1AEA5ADF","HostId":"8cLeGAmw098X5cv4Zkwcmo8vvZa3eH3eKxsPzbB9wrR+YstdA6Knx4Ip8EXAMPLE"}"#;

/// An SES bounce notification (feedback notifications: `notificationType`).
const SES_BOUNCE: &str = r#"{"notificationType":"Bounce","bounce":{"bounceType":"Permanent","bounceSubType":"General","bouncedRecipients":[{"status":"5.0.0","action":"failed","diagnosticCode":"smtp; 550 user unknown","emailAddress":"recipient1@example.com"}],"reportingMTA":"example.com","timestamp":"2012-05-25T14:59:38.605Z","feedbackId":"000001378603176d-5a4b5ad9-6f30-4198-a8c3-b1eb0c270a1d-000000","remoteMtaIp":"127.0.2.0"},"mail":{"timestamp":"2012-05-25T14:59:38.000Z","source":"sender@example.com","sourceArn":"arn:aws:ses:us-east-1:888888888888:identity/example.com","sendingAccountId":"123456789012","messageId":"000001378603177f-7a5433e7-8edb-42ae-af10-f0181f34d6ee-000000","destination":["recipient1@example.com"]}}"#;

fn wire(body: &str) -> WireMessage {
    WireMessage::new(Bytes::copy_from_slice(body.as_bytes()), MessageAttributes::new())
}

fn unwrap(body: &str) -> Option<Payload> {
    AwsNotifications.unwrap(&wire(body)).unwrap()
}

#[test]
fn recognises_s3_event_notifications() {
    let payload = unwrap(S3_PUT).unwrap();
    assert_eq!(payload.message_type.as_deref(), Some("aws.s3.event"));
    assert_eq!(payload.source.as_deref(), Some("aws:s3"));
    assert_eq!(payload.content_type.as_deref(), Some("application/json"));
    assert_eq!(payload.time.unwrap().unix_timestamp(), 0);
    assert_eq!(&payload.data[..], S3_PUT.as_bytes());
    assert_eq!(payload.id, None);

    let test = unwrap(S3_TEST).unwrap();
    assert_eq!(test.message_type.as_deref(), Some("aws.s3.event"));
    assert_eq!(test.time.unwrap().unix_timestamp(), 1_413_215_822);
}

#[test]
fn recognises_ses_notifications_of_both_formats() {
    let payload = unwrap(SES_BOUNCE).unwrap();
    assert_eq!(payload.message_type.as_deref(), Some("aws.ses.bounce"));
    assert_eq!(payload.source.as_deref(), Some("aws:ses"));
    // The time of the bounce, not of the mail.
    assert_eq!(payload.time.unwrap().millisecond(), 605);

    for (kind, details, message_type) in [
        ("Complaint", "complaint", "aws.ses.complaint"),
        ("Delivery", "delivery", "aws.ses.delivery"),
        ("DeliveryDelay", "deliveryDelay", "aws.ses.delivery_delay"),
        ("Rendering Failure", "failure", "aws.ses.rendering_failure"),
        ("Open", "open", "aws.ses.open"),
        ("Send", "send", "aws.ses.send"),
    ] {
        let body = format!(
            r#"{{"eventType":"{kind}","mail":{{"timestamp":"2016-10-19T23:20:52.240Z","messageId":"m"}},"{details}":{{"timestamp":"2016-10-19T23:21:04.133Z"}}}}"#
        );
        let payload = unwrap(&body).unwrap();
        assert_eq!(payload.message_type.as_deref(), Some(message_type), "{kind}");
    }
    // SES receiving, and a timestamp in the format of SES feedback notifications' examples.
    let body = r#"{"notificationType":"Received","mail":{"timestamp":"2018-10-08T14:05:45 +0000"},"receipt":{}}"#;
    let payload = unwrap(body).unwrap();
    assert_eq!(payload.message_type.as_deref(), Some("aws.ses.received"));
    assert_eq!(payload.time.unwrap().unix_timestamp(), 1_539_007_545);
}

#[test]
fn leaves_other_messages_to_the_next_envelope() {
    for body in [
        "",
        "hello",
        r#"{"order": 1}"#,
        r#"{"Records": []}"#,
        r#"{"Records": [{"eventSource": "aws:dynamodb"}], "x": "aws:s3"}"#,
        r#"{"Records": "aws:s3"}"#,
        r#"{"Event": "s3:TestEvent"}"#,
        r#"{"notificationType": "Bounce", "mail": "x"}"#,
        r#"{"notificationType": "", "mail": {}}"#,
        r#"{"eventType": "Bounce"}"#,
        r#"{"mail": {}, "eventType": "Bounce""#,
    ] {
        assert_eq!(unwrap(body), None, "{body}");
    }
    // A fanferro producer's message keeps its own type, whatever its body.
    let mut wire = wire(S3_PUT);
    wire.attributes.insert("fanferro.type".into(), AttributeValue::string("archive.v1"));
    assert_eq!(AwsNotifications.unwrap(&wire).unwrap(), None);
}

#[test]
fn wraps_documents_as_aws_sends_them() {
    let payload = Payload {
        data: Bytes::from_static(S3_PUT.as_bytes()),
        content_type: Some("application/json".into()),
        message_type: Some("aws.s3.event".into()),
        id: Some("1".into()),
        ..Payload::default()
    };
    let wire = AwsNotifications.wrap(&payload).unwrap();
    assert!(wire.attributes.is_empty());
    assert_eq!(&wire.body[..], S3_PUT.as_bytes());
    assert_eq!(unwrap(S3_PUT), AwsNotifications.unwrap(&wire).unwrap());
    // A JSON document given as text (a `str` body) is one too; anything else is not.
    let text = Payload { content_type: Some("text/plain".into()), ..payload };
    assert_eq!(&AwsNotifications.wrap(&text).unwrap().body[..], S3_PUT.as_bytes());
    let binary = Payload { data: Bytes::from_static(b"\x00{"), content_type: None, ..text };
    assert!(AwsNotifications.wrap(&binary).is_err());
}

#[test]
fn snake_cases_ses_event_types() {
    assert_eq!(snake_case("Bounce"), "bounce");
    assert_eq!(snake_case("DeliveryDelay"), "delivery_delay");
    assert_eq!(snake_case("Rendering Failure"), "rendering_failure");
    assert_eq!(snake_case("  "), "");
}

#[test]
fn the_default_set_detects_notifications_before_raw() {
    let set = EnvelopeSet::default_set();
    assert_eq!(set.unwrap(&wire(S3_PUT)).unwrap().1, "aws");
    assert_eq!(set.unwrap(&wire(SES_BOUNCE)).unwrap().1, "aws");
    assert_eq!(set.unwrap(&wire(r#"{"order": 1}"#)).unwrap().1, "raw");
    let raw = RawEnvelope::default().wrap(&Payload { data: Bytes::from_static(b"{}"), ..Payload::default() });
    assert_eq!(set.unwrap(&raw.unwrap()).unwrap().1, "raw");
}
