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
//! SES types are `aws.ses.` and the snake-cased event type: `aws.ses.complaint`,
//! `aws.ses.delivery`, `aws.ses.delivery_delay`, `aws.ses.rendering_failure`, `aws.ses.received`...
//! The body is the payload as it is (JSON); the event time is the message time.

use bytes::Bytes;
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::Envelope;
use crate::error::{Error, Result};
use crate::types::{Headers, MessageAttributes, Payload, WireMessage, reserved};

/// Message type of S3 event notifications (and S3's test event).
pub const AWS_S3_EVENT_TYPE: &str = "aws.s3.event";

/// Prefix of the message types of SES notifications (`aws.ses.bounce`, ...).
pub const AWS_SES_TYPE_PREFIX: &str = "aws.ses.";

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
    let text = value?.as_str()?;
    OffsetDateTime::parse(text, &Rfc3339).ok().or_else(|| {
        // SES writes some timestamps as `2018-10-08T14:05:45 +0000`.
        let (stamp, offset) = text.rsplit_once(' ')?;
        let offset =
            offset.strip_prefix('+').map(|o| ('+', o)).or_else(|| offset.strip_prefix('-').map(|o| ('-', o)))?;
        let (hours, minutes) = (offset.1.get(..2)?, offset.1.get(2..)?);
        OffsetDateTime::parse(&format!("{stamp}{}{hours}:{minutes}", offset.0), &Rfc3339).ok()
    })
}

/// `DeliveryDelay` -> `delivery_delay`, `Rendering Failure` -> `rendering_failure`.
fn snake_case(kind: &str) -> String {
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
        let body = wire.body.trim_ascii_start();
        if !body.starts_with(b"{") {
            return Ok(None);
        }
        let find = |needle: &[u8]| memchr::memmem::find(body, needle).is_some();
        let s3 = (find(b"\"Records\"") && find(b"\"aws:s3\"")) || find(b"\"s3:TestEvent\"");
        let ses = find(b"\"mail\"") && (find(b"\"notificationType\"") || find(b"\"eventType\""));
        if !s3 && !ses {
            return Ok(None);
        }
        let Ok(document) = serde_json::from_slice::<Value>(&wire.body) else {
            return Ok(None);
        };
        let notification = if s3 { s3_notification(&document) } else { None }
            .or_else(|| if ses { ses_notification(&document) } else { None });
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
