use super::*;
use crate::envelope::{EnvelopeSet, RawEnvelope};
use crate::types::{AttributeValue, HeaderValue};

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

/// AWS's example of an S3 `Object Created` event sent through EventBridge.
const EVENTBRIDGE_S3: &str = r#"{"version":"0","id":"17793124-05d4-b198-2fde-7ededc63b103","detail-type":"Object Created","source":"aws.s3","account":"111122223333","time":"2021-11-12T00:00:00Z","region":"ca-central-1","resources":["arn:aws:s3:::amzn-s3-demo-bucket1"],"detail":{"version":"0","bucket":{"name":"amzn-s3-demo-bucket1"},"object":{"key":"example-key","size":5,"etag":"b1946ac92492d2347c6235b4d2611184","version-id":"IYV3p45BT0ac8hjHg1houSdS1a.Mro8e","sequencer":"617f08299329d189"},"request-id":"N4N7GDK58NMKJ12R","requester":"123456789012","source-ip-address":"192.0.2.1","reason":"PutObject"}}"#;

fn eventbridge(source: &str, detail_type: &str, detail: &str) -> String {
    format!(
        r#"{{"version":"0","id":"e-1","detail-type":"{detail_type}","source":"{source}","account":"111122223333","time":"2026-11-01T12:00:00Z","region":"eu-north-1","resources":["arn:a","arn:b"],"detail":{detail}}}"#
    )
}

#[test]
fn unwraps_eventbridge_events_like_an_envelope() {
    let wire = wire(EVENTBRIDGE_S3);
    let payload = AwsNotifications.unwrap(&wire).unwrap().unwrap();
    assert_eq!(payload.message_type.as_deref(), Some("aws.s3.object_created"));
    assert_eq!(payload.id.as_deref(), Some("17793124-05d4-b198-2fde-7ededc63b103"));
    assert_eq!(payload.source.as_deref(), Some("aws.s3"));
    assert_eq!(payload.time.unwrap().unix_timestamp(), 1_636_675_200);
    assert_eq!(payload.content_type.as_deref(), Some("application/json"));
    // The detail, sliced from the body.
    let detail: serde_json::Value = serde_json::from_slice(&payload.data).unwrap();
    assert_eq!(detail["object"]["key"], "example-key");
    let start = payload.data.as_ptr() as usize - wire.body.as_ptr() as usize;
    assert!(start < wire.body.len());
    let header = |name: &str| match payload.headers.get(name) {
        Some(HeaderValue::String(value)) => value.as_str(),
        other => panic!("{name}: {other:?}"),
    };
    assert_eq!(header("detail-type"), "Object Created");
    assert_eq!(header("account"), "111122223333");
    assert_eq!(header("region"), "ca-central-1");
    assert_eq!(header("resources"), "arn:aws:s3:::amzn-s3-demo-bucket1");
}

#[test]
fn names_eventbridge_event_types() {
    for (source, detail_type, detail, expected) in [
        ("aws.ecs", "ECS Task State Change", "{}", "aws.ecs.task_state_change"),
        (
            "aws.states",
            "Step Functions Execution Status Change",
            "{}",
            "aws.states.step_functions_execution_status_change",
        ),
        ("aws.events", "Scheduled Event", "{}", "aws.events.scheduled_event"),
        ("aws.ec2", "EC2 Spot Instance Interruption Warning", "{}", "aws.ec2.spot_instance_interruption_warning"),
        ("aws.ec2", "EC2 Instance State-change Notification", "{}", "aws.ec2.instance_state_change_notification"),
        (
            "aws.autoscaling",
            "EC2 Instance-launch Lifecycle Action",
            "{}",
            "aws.autoscaling.ec2_instance_launch_lifecycle_action",
        ),
        ("aws.health", "AWS Health Event", "{}", "aws.health.event"),
        ("aws.cloudwatch", "CloudWatch Alarm State Change", "{}", "aws.cloudwatch.alarm_state_change"),
        ("aws.s3", "Object Deleted", "{}", "aws.s3.object_deleted"),
        // SES email events keep the types of SES's notifications.
        ("aws.ses", "Email Bounced", r#"{"eventType":"Bounce","mail":{}}"#, "aws.ses.bounce"),
        ("aws.ses", "Email Delivery Delayed", r#"{"eventType":"DeliveryDelay","mail":{}}"#, "aws.ses.delivery_delay"),
        (
            "aws.ses",
            "Advisor Recommendation Status Open",
            r#"{"version":"1.0.0"}"#,
            "aws.ses.advisor_recommendation_status_open",
        ),
        // Other sources: the detail type as it is.
        ("com.acme.billing", "InvoicePaid", "{}", "InvoicePaid"),
        ("aws.partner/stripe.com/ed_123", "invoice.paid", "{}", "invoice.paid"),
    ] {
        let payload = unwrap(&eventbridge(source, detail_type, detail)).unwrap();
        assert_eq!(payload.message_type.as_deref(), Some(expected), "{source} {detail_type}");
        assert_eq!(payload.source.as_deref(), Some(source));
    }
}

#[test]
fn ses_email_events_through_eventbridge_carry_the_ses_document() {
    let detail =
        format!(r#"{{"eventType":"Bounce","bounce":{{"bounceType":"Permanent"}},"mail":{}}}"#, r#"{"messageId":"m"}"#);
    let payload = unwrap(&eventbridge("aws.ses", "Email Bounced", &detail)).unwrap();
    assert_eq!(payload.message_type.as_deref(), Some("aws.ses.bounce"));
    assert_eq!(&payload.data[..], detail.as_bytes());
    assert_eq!(payload.id.as_deref(), Some("e-1"));
}

#[test]
fn incomplete_eventbridge_wrappers_are_not_events() {
    for body in [
        r#"{"detail-type":"x","source":"y"}"#,
        r#"{"detail-type":"x","source":"y","detail":{},"id":"1"}"#,
        r#"{"detail-type":"","source":"y","detail":{},"id":"1","time":"2026-01-01T00:00:00Z"}"#,
        r#"{"detail-type":1,"source":"y","detail":{},"id":"1","time":"2026-01-01T00:00:00Z"}"#,
    ] {
        assert_eq!(unwrap(body), None, "{body}");
    }
    // An application event whose detail has no object is still one (detail is any JSON).
    let payload = unwrap(r#"{"detail-type":"Tick","source":"app","detail":null,"id":"1","time":"x"}"#).unwrap();
    assert_eq!(&payload.data[..], b"null");
    assert_eq!(payload.time, None);
}

#[test]
fn recognises_auto_scaling_lifecycle_hook_notifications() {
    let launching = r#"{"Origin":"EC2","LifecycleHookName":"my-hook","Destination":"AutoScalingGroup","AccountId":"123456789012","RequestId":"18b2ec17-3e9b-4c15-8024-ff2e8ce8786a","LifecycleTransition":"autoscaling:EC2_INSTANCE_LAUNCHING","AutoScalingGroupName":"my-asg","Service":"AWS Auto Scaling","Time":"2021-01-19T00:36:26.533Z","EC2InstanceId":"i-0598c7d356eba48d7","LifecycleActionToken":"71514b9d-6a40-4b26-8523-05e7ee35fa40","NotificationMetadata":"hook message metadata"}"#;
    let payload = unwrap(launching).unwrap();
    assert_eq!(payload.message_type.as_deref(), Some("aws.autoscaling.ec2_instance_launch_lifecycle_action"));
    assert_eq!(payload.source.as_deref(), Some("aws.autoscaling"));
    assert_eq!(payload.time.unwrap().millisecond(), 533);
    let terminating = launching.replace("EC2_INSTANCE_LAUNCHING", "EC2_INSTANCE_TERMINATING");
    assert_eq!(
        unwrap(&terminating).unwrap().message_type.as_deref(),
        Some("aws.autoscaling.ec2_instance_terminate_lifecycle_action")
    );
    let test = r#"{"AccountId":"123456789012","RequestId":"18b2ec17","AutoScalingGroupARN":"arn:aws:autoscaling:us-west-2:123456789012:autoScalingGroup:042cba90:autoScalingGroupName/my-asg","AutoScalingGroupName":"my-asg","Service":"AWS Auto Scaling","Event":"autoscaling:TEST_NOTIFICATION","Time":"2021-01-19T00:35:52.359Z"}"#;
    assert_eq!(unwrap(test).unwrap().message_type.as_deref(), Some("aws.autoscaling.test_notification"));
    let launch = test.replace("TEST_NOTIFICATION", "EC2_INSTANCE_LAUNCH");
    assert_eq!(unwrap(&launch).unwrap().message_type.as_deref(), Some("aws.autoscaling.ec2_instance_launch"));
    assert_eq!(unwrap(r#"{"Service":"AWS Auto Scaling"}"#), None);
    assert_eq!(unwrap(r#"{"Service":"Other","Event":"AWS Auto Scaling"}"#), None);
}

/// A CloudWatch alarm's notification to SNS.
const CLOUDWATCH_ALARM: &str = r#"{"AlarmName":"ServerCpuTooHigh","AlarmDescription":"CPU too high","AWSAccountId":"123456789012","AlarmConfigurationUpdatedTimestamp":"2019-10-01T13:00:00.000+0000","NewStateValue":"ALARM","NewStateReason":"Threshold Crossed: 1 out of the last 1 datapoints [99.5 (02/10/19 16:59:00)] was greater than the threshold (50.0).","StateChangeTime":"2019-10-02T17:04:40.989+0000","Region":"US East (N. Virginia)","AlarmArn":"arn:aws:cloudwatch:us-east-1:123456789012:alarm:ServerCpuTooHigh","OldStateValue":"OK","OKActions":[],"AlarmActions":["arn:aws:sns:us-east-1:123456789012:alarms"],"InsufficientDataActions":[],"Trigger":{"MetricName":"CPUUtilization","Namespace":"AWS/EC2","StatisticType":"Statistic","Statistic":"AVERAGE","Unit":null,"Dimensions":[{"value":"i-12345678901234567","name":"InstanceId"}],"Period":300,"EvaluationPeriods":1,"ComparisonOperator":"GreaterThanThreshold","Threshold":50.0,"TreatMissingData":"missing","EvaluateLowSampleCountPercentile":""}}"#;

/// AWS's example record of a failed asynchronous Lambda invocation.
const LAMBDA_FAILURE: &str = r#"{"version":"1.0","timestamp":"2019-11-14T18:16:05.568Z","requestContext":{"requestId":"e4b46cbf-b738-xmpl-8880-a18cdf61200e","functionArn":"arn:aws:lambda:us-east-1:123456789012:function:my-function:$LATEST","condition":"RetriesExhausted","approximateInvokeCount":3},"requestPayload":{"ORDER_IDS":["9e07af03-ce31-4ff3-xmpl-36dce652cb4f"]},"responseContext":{"statusCode":200,"executedVersion":"$LATEST","functionError":"Unhandled"},"responsePayload":{"errorMessage":"RequestId: e4b46cbf-b738-xmpl-8880-a18cdf61200e Process exited before completing request"}}"#;

#[test]
fn parses_timestamps_with_offsets_without_a_colon() {
    for (text, timestamp) in [
        ("2019-10-02T17:04:40.989+0000", 1_570_035_880),
        ("2018-10-08T14:05:45 +0000", 1_539_007_545),
        ("2019-10-02T19:04:40+0200", 1_570_035_880),
        ("2019-10-02T17:04:40Z", 1_570_035_880),
    ] {
        assert_eq!(parse_timestamp(text).map(OffsetDateTime::unix_timestamp), Some(timestamp), "{text}");
    }
    assert_eq!(parse_timestamp("+0000"), None);
    assert_eq!(parse_timestamp("yesterday"), None);
}

#[test]
fn recognises_cloudwatch_alarm_notifications() {
    let payload = unwrap(CLOUDWATCH_ALARM).unwrap();
    assert_eq!(payload.message_type.as_deref(), Some("aws.cloudwatch.alarm"));
    assert_eq!(payload.source.as_deref(), Some("aws.cloudwatch"));
    assert_eq!(payload.time.unwrap().unix_timestamp(), 1_570_035_880);
    assert_eq!(unwrap(r#"{"AlarmName":"x","NewStateValue":1}"#), None);
}

#[test]
fn recognises_lambda_invocation_records() {
    let payload = unwrap(LAMBDA_FAILURE).unwrap();
    assert_eq!(payload.message_type.as_deref(), Some("aws.lambda.invocation_failure"));
    assert_eq!(payload.source.as_deref(), Some("aws.lambda"));
    assert_eq!(payload.time.unwrap().unix_timestamp(), 1_573_755_365);
    let success = LAMBDA_FAILURE.replace("RetriesExhausted", "Success");
    assert_eq!(unwrap(&success).unwrap().message_type.as_deref(), Some("aws.lambda.invocation_success"));
    // Through EventBridge: source `lambda`.
    let detail = LAMBDA_FAILURE;
    let wrapped = eventbridge("lambda", "Lambda Function Invocation Result - Failure", detail);
    let payload = unwrap(&wrapped).unwrap();
    assert_eq!(payload.message_type.as_deref(), Some("aws.lambda.invocation_failure"));
    assert_eq!(&payload.data[..], detail.as_bytes());
    let wrapped = eventbridge("lambda", "Lambda Function Invocation Result - Success", detail);
    assert_eq!(unwrap(&wrapped).unwrap().message_type.as_deref(), Some("aws.lambda.invocation_success"));
}

#[test]
fn recognises_lambda_dead_letters_by_their_attributes() {
    let mut wire = wire(S3_PUT);
    wire.attributes.insert("RequestID".into(), AttributeValue::string("e4b46cbf"));
    wire.attributes.insert("ErrorCode".into(), AttributeValue::number("200"));
    wire.attributes.insert("ErrorMessage".into(), AttributeValue::string("boom"));
    let payload = AwsNotifications.unwrap(&wire).unwrap().unwrap();
    // A failed S3 event: its type says it failed, its body is the S3 event.
    assert_eq!(payload.message_type.as_deref(), Some("aws.lambda.dead_letter"));
    assert_eq!(payload.content_type.as_deref(), Some("application/json"));
    assert_eq!(&payload.data[..], S3_PUT.as_bytes());
    wire.body = Bytes::from_static(b"not json");
    let payload = AwsNotifications.unwrap(&wire).unwrap().unwrap();
    assert_eq!((payload.message_type.as_deref(), payload.content_type), (Some("aws.lambda.dead_letter"), None));
    wire.attributes.remove("ErrorMessage");
    assert_eq!(AwsNotifications.unwrap(&wire).unwrap(), None);
}
