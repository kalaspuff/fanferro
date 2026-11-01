//! Notifications of AWS services: S3 event notifications and SES email notifications.
//!
//! S3 (bucket event notifications) and SES (bounce, complaint and delivery notifications, and
//! event publishing) deliver JSON documents to SNS topics and SQS queues, without attributes. This
//! envelope recognises them by their body and gives them a message type, so that consumers route
//! them like any typed message:
//!
//! | Document | Message type |
//! | --- | --- |
//! | S3 `{"Records": [{"eventSource": "aws:s3", ...}]}` and the `s3:TestEvent` message | `aws.s3.event` |
//! | SES `{"notificationType" or "eventType": "Bounce", "mail": {...}, ...}` | `aws.ses.bounce` |
//!
//! Auto Scaling lifecycle hooks that notify SNS or SQS directly are typed like their EventBridge
//! events (`aws.autoscaling.ec2_instance_launch_lifecycle_action`, `..._terminate_...`; the hook's
//! test notification `aws.autoscaling.test_notification`).
//!
//! CloudWatch alarms that notify SNS are `aws.cloudwatch.alarm`; the records of asynchronous Lambda
//! invocations that a destination receives are `aws.lambda.invocation_success` and
//! `aws.lambda.invocation_failure` (through EventBridge too), and events a Lambda function sent to
//! its dead-letter queue (recognised by the `RequestID`, `ErrorCode` and `ErrorMessage`
//! attributes Lambda adds) `aws.lambda.dead_letter`.
//!
//! SES types are `aws.ses.` and the snake-cased event type: `aws.ses.complaint`,
//! `aws.ses.delivery`, `aws.ses.delivery_delay`, `aws.ses.rendering_failure`, `aws.ses.received`...
//! The body is the payload as it is (JSON); the event time is the message time.
//!
//! EventBridge events that a rule sends to a queue or topic (from AWS services, SaaS partners or
//! `PutEvents`) are unwrapped like an envelope: see [`eventbridge`].

use bytes::Bytes;
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::Envelope;

pub mod eventbridge;
use crate::error::{Error, Result};
use crate::types::{Headers, MessageAttributes, Payload, WireMessage, reserved};

/// Message type of S3 event notifications (and S3's test event).
pub const AWS_S3_EVENT_TYPE: &str = "aws.s3.event";

/// Prefix of the message types of SES notifications (`aws.ses.bounce`, ...).
pub const AWS_SES_TYPE_PREFIX: &str = "aws.ses.";

/// Message type of CloudWatch alarm notifications sent to SNS.
pub const AWS_CLOUDWATCH_ALARM_TYPE: &str = "aws.cloudwatch.alarm";

/// Message type of events an asynchronously invoked Lambda function sent to its dead-letter queue.
pub const AWS_LAMBDA_DEAD_LETTER_TYPE: &str = "aws.lambda.dead_letter";

/// Message attributes Lambda adds to the events it sends to a dead-letter queue.
const LAMBDA_DEAD_LETTER_ATTRIBUTES: [&str; 3] = ["RequestID", "ErrorCode", "ErrorMessage"];

const JSON_CONTENT_TYPE: &str = "application/json";

/// Recognises notifications that AWS services send to SNS and SQS (S3 events, SES notifications).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct AwsNotifications;

/// A recognised notification: its type, source and time.
struct Notification {
    message_type: String,
    source: &'static str,
    time: Option<OffsetDateTime>,
}

fn parse_time(value: Option<&Value>) -> Option<OffsetDateTime> {
    parse_timestamp(value?.as_str()?)
}

/// An RFC 3339 timestamp, or one with an offset without a colon, as SES
/// (`2018-10-08T14:05:45 +0000`) and CloudWatch (`2019-10-02T17:04:40.989+0000`) write them.
pub(crate) fn parse_timestamp(text: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(text, &Rfc3339).ok().or_else(|| {
        let split = text.len().checked_sub(5)?;
        let (stamp, offset) = (text.get(..split)?.trim_end(), text.get(split..)?);
        let sign = offset.chars().next().filter(|c| matches!(c, '+' | '-'))?;
        let (hours, minutes) = (offset.get(1..3)?, offset.get(3..)?);
        OffsetDateTime::parse(&format!("{stamp}{sign}{hours}:{minutes}"), &Rfc3339).ok()
    })
}

/// `DeliveryDelay` -> `delivery_delay`, `Rendering Failure` -> `rendering_failure`.
pub(crate) fn snake_case(kind: &str) -> String {
    let mut snake = String::with_capacity(kind.len() + 4);
    let mut previous_lower = false;
    for c in kind.chars() {
        if c.is_ascii_uppercase() {
            if previous_lower {
                snake.push('_');
            }
            snake.push(c.to_ascii_lowercase());
            previous_lower = false;
        } else if c.is_ascii_alphanumeric() {
            snake.push(c);
            previous_lower = true;
        } else {
            if !snake.ends_with('_') && !snake.is_empty() {
                snake.push('_');
            }
            previous_lower = false;
        }
    }
    snake.trim_end_matches('_').to_string()
}

/// The object of an SES notification holding the event's details (and its timestamp).
fn ses_details_key(kind: &str) -> String {
    match kind {
        "Rendering Failure" => "failure".to_string(),
        _ => {
            let mut chars = kind.chars();
            chars.next().map(|first| first.to_ascii_lowercase().to_string() + chars.as_str()).unwrap_or_default()
        }
    }
}

fn s3_notification(document: &Value) -> Option<Notification> {
    if let Some(records) = document.get("Records") {
        let records = records.as_array()?;
        let all_s3 = !records.is_empty()
            && records.iter().all(|record| record.get("eventSource").and_then(Value::as_str) == Some("aws:s3"));
        if !all_s3 {
            return None;
        }
        return Some(Notification {
            message_type: AWS_S3_EVENT_TYPE.to_string(),
            source: "aws:s3",
            time: parse_time(records[0].get("eventTime")),
        });
    }
    if document.get("Event").and_then(Value::as_str) == Some("s3:TestEvent")
        && document.get("Service").and_then(Value::as_str) == Some("Amazon S3")
    {
        return Some(Notification {
            message_type: AWS_S3_EVENT_TYPE.to_string(),
            source: "aws:s3",
            time: parse_time(document.get("Time")),
        });
    }
    None
}

/// Notifications Auto Scaling sends to SNS and SQS: lifecycle hooks (and their test notification)
/// and the notifications of an Auto Scaling group.
fn autoscaling_notification(document: &Value) -> Option<Notification> {
    if document.get("Service").and_then(Value::as_str) != Some("AWS Auto Scaling") {
        return None;
    }
    let event = match document.get("LifecycleTransition").and_then(Value::as_str) {
        Some("autoscaling:EC2_INSTANCE_LAUNCHING") => "ec2_instance_launch_lifecycle_action".to_string(),
        Some("autoscaling:EC2_INSTANCE_TERMINATING") => "ec2_instance_terminate_lifecycle_action".to_string(),
        Some(transition) => transition.trim_start_matches("autoscaling:").to_ascii_lowercase(),
        None => document.get("Event")?.as_str()?.trim_start_matches("autoscaling:").to_ascii_lowercase(),
    };
    if event.is_empty() {
        return None;
    }
    Some(Notification {
        message_type: format!("aws.autoscaling.{event}"),
        source: "aws.autoscaling",
        time: parse_time(document.get("Time")),
    })
}

/// A CloudWatch alarm's notification to an SNS topic.
fn cloudwatch_alarm(document: &Value) -> Option<Notification> {
    document.get("AlarmName")?.as_str()?;
    document.get("NewStateValue")?.as_str()?;
    Some(Notification {
        message_type: AWS_CLOUDWATCH_ALARM_TYPE.to_string(),
        source: "aws.cloudwatch",
        time: parse_time(document.get("StateChangeTime")),
    })
}

/// The record of an asynchronous Lambda invocation that a destination (SQS or SNS) received, or
/// of a batch an event source mapping's on-failure destination received.
fn lambda_invocation_record(document: &Value) -> Option<Notification> {
    let context = document.get("requestContext")?;
    context.get("functionArn")?.as_str()?;
    let condition = context.get("condition")?.as_str()?;
    Some(Notification {
        message_type: lambda_invocation_type(condition == "Success").to_string(),
        source: "aws.lambda",
        time: parse_time(document.get("timestamp")),
    })
}

/// The message type of Lambda invocation records.
pub(crate) fn lambda_invocation_type(success: bool) -> &'static str {
    if success { "aws.lambda.invocation_success" } else { "aws.lambda.invocation_failure" }
}

fn ses_notification(document: &Value) -> Option<Notification> {
    let mail = document.get("mail").filter(|mail| mail.is_object())?;
    let kind = document
        .get("eventType")
        .or_else(|| document.get("notificationType"))
        .and_then(Value::as_str)
        .filter(|kind| !kind.is_empty())?;
    let message_type = snake_case(kind);
    if message_type.is_empty() {
        return None;
    }
    let time = parse_time(document.get(ses_details_key(kind)).and_then(|details| details.get("timestamp")))
        .or_else(|| parse_time(mail.get("timestamp")));
    Some(Notification { message_type: format!("{AWS_SES_TYPE_PREFIX}{message_type}"), source: "aws:ses", time })
}

impl Envelope for AwsNotifications {
    fn name(&self) -> &str {
        "aws"
    }

    /// The payload as it is, without attributes: how AWS services send their notifications.
    fn wrap(&self, payload: &Payload) -> Result<WireMessage> {
        // A JSON document, whatever the content type says (a `str` body is text/plain).
        if serde_json::from_slice::<serde::de::IgnoredAny>(&payload.data).is_err() {
            return Err(Error::Envelope(format!(
                "AWS notifications are JSON documents, not {}: publish the notification's document",
                payload.content_type.as_deref().unwrap_or("binary data")
            )));
        }
        Ok(WireMessage { body: payload.data.clone(), attributes: MessageAttributes::new() })
    }

    fn unwrap(&self, wire: &WireMessage) -> Result<Option<Payload>> {
        // Messages of fanferro producers carry their own type, even when their body looks like a
        // notification (a forwarded one, say).
        if wire.attributes.contains_key(reserved::MESSAGE_ID) || wire.attributes.contains_key(reserved::MESSAGE_TYPE) {
            return Ok(None);
        }
        // An event a Lambda function failed to process, as it was sent to the function: its body
        // is whatever the function received (even an S3 notification), its type says it failed.
        if LAMBDA_DEAD_LETTER_ATTRIBUTES.iter().all(|name| wire.attributes.contains_key(*name)) {
            let json = serde_json::from_slice::<serde::de::IgnoredAny>(&wire.body).is_ok();
            return Ok(Some(Payload {
                data: Bytes::clone(&wire.body),
                content_type: json.then(|| JSON_CONTENT_TYPE.to_string()),
                message_type: Some(AWS_LAMBDA_DEAD_LETTER_TYPE.to_string()),
                id: None,
                time: None,
                source: Some("aws.lambda".to_string()),
                subject: None,
                data_schema: None,
                headers: Headers::new(),
            }));
        }
        let body = wire.body.trim_ascii_start();
        if !body.starts_with(b"{") {
            return Ok(None);
        }
        let find = |needle: &[u8]| memchr::memmem::find(body, needle).is_some();
        if find(b"\"detail-type\"")
            && let Some(payload) = eventbridge::unwrap(&wire.body)
        {
            return Ok(Some(payload));
        }
        let s3 = (find(b"\"Records\"") && find(b"\"aws:s3\"")) || find(b"\"s3:TestEvent\"");
        let ses = find(b"\"mail\"") && (find(b"\"notificationType\"") || find(b"\"eventType\""));
        let autoscaling = find(b"\"AWS Auto Scaling\"");
        let alarm = find(b"\"NewStateValue\"") && find(b"\"AlarmName\"");
        let lambda = find(b"\"requestContext\"") && find(b"\"functionArn\"");
        if !s3 && !ses && !autoscaling && !alarm && !lambda {
            return Ok(None);
        }
        let Ok(document) = serde_json::from_slice::<Value>(&wire.body) else {
            return Ok(None);
        };
        let notification = if s3 { s3_notification(&document) } else { None }
            .or_else(|| if ses { ses_notification(&document) } else { None })
            .or_else(|| if autoscaling { autoscaling_notification(&document) } else { None })
            .or_else(|| if alarm { cloudwatch_alarm(&document) } else { None })
            .or_else(|| if lambda { lambda_invocation_record(&document) } else { None });
        Ok(notification.map(|notification| Payload {
            data: Bytes::clone(&wire.body),
            content_type: Some(JSON_CONTENT_TYPE.to_string()),
            message_type: Some(notification.message_type),
            id: None,
            time: notification.time,
            source: Some(notification.source.to_string()),
            subject: None,
            data_schema: None,
            headers: Headers::new(),
        }))
    }
}

#[cfg(test)]
mod tests;
