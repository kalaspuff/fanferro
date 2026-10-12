//! Unwrapping SNS notifications delivered to SQS without raw message delivery.
//!
//! When an SQS queue subscribes to a topic with `RawMessageDelivery=false` (the SNS
//! default), each SQS message body is a JSON document:
//!
//! ```json
//! {"Type":"Notification","MessageId":"...","TopicArn":"...","Message":"<original body>",
//!  "MessageAttributes":{"name":{"Type":"String","Value":"..."}}, ...}
//! ```
//!
//! fanferro unwraps it so handlers see the original body and attributes either way.
//! Subscription confirmation messages (cross-account subscriptions) are recognised as
//! control messages and never reach handlers.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use serde::Deserialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::error::{Error, Result};
use crate::types::{AttributeData, AttributeValue, MessageAttributes};

/// Whether to unwrap SNS notification bodies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnsUnwrap {
    /// Detect SNS notifications and unwrap them (default).
    #[default]
    Auto,
    /// Never unwrap (bodies are passed through as-is).
    Never,
}

/// The kind of SNS message found in the body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnsMessageKind {
    /// A regular notification.
    Notification,
    /// A subscription confirmation request (cross-account subscriptions).
    SubscriptionConfirmation,
    /// An unsubscribe confirmation.
    UnsubscribeConfirmation,
}

/// Metadata of an unwrapped SNS message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnsInfo {
    /// Kind of SNS message.
    pub kind: SnsMessageKind,
    /// SNS message id.
    pub message_id: String,
    /// Topic the message was published to.
    pub topic_arn: String,
    /// Subject, when published with one.
    pub subject: Option<String>,
    /// Publish timestamp.
    pub timestamp: Option<OffsetDateTime>,
    /// FIFO sequence number.
    pub sequence_number: Option<String>,
    /// Confirmation token (confirmation messages only).
    pub token: Option<String>,
}

/// The result of unwrapping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unwrapped {
    /// The original message body.
    pub message: String,
    /// The original message attributes.
    pub attributes: MessageAttributes,
    /// SNS metadata.
    pub info: SnsInfo,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct RawNotification {
    #[serde(rename = "Type")]
    kind: String,
    message_id: String,
    topic_arn: String,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    subject: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default)]
    sequence_number: Option<String>,
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    message_attributes: Option<std::collections::BTreeMap<String, RawAttribute>>,
}

#[derive(Deserialize)]
struct RawAttribute {
    #[serde(rename = "Type")]
    data_type: String,
    #[serde(rename = "Value")]
    value: serde_json::Value,
}

/// Unwraps `body` if it is an SNS notification (or confirmation) document.
///
/// Returns `Ok(None)` for anything else. SQS message attributes are ignored because SNS
/// does not set any when it wraps messages.
pub fn unwrap(body: &str, mode: SnsUnwrap) -> Result<Option<Unwrapped>> {
    if mode == SnsUnwrap::Never {
        return Ok(None);
    }
    let trimmed = body.trim_start();
    if !trimmed.starts_with('{') || !body.contains("\"TopicArn\"") || !body.contains("\"Type\"") {
        return Ok(None);
    }
    let Ok(raw) = serde_json::from_str::<RawNotification>(body) else {
        return Ok(None);
    };
    let kind = match raw.kind.as_str() {
        "Notification" => SnsMessageKind::Notification,
        "SubscriptionConfirmation" => SnsMessageKind::SubscriptionConfirmation,
        "UnsubscribeConfirmation" => SnsMessageKind::UnsubscribeConfirmation,
        _ => return Ok(None),
    };
    if !raw.topic_arn.starts_with("arn:") {
        return Ok(None);
    }
    let mut attributes = MessageAttributes::new();
    for (name, attribute) in raw.message_attributes.unwrap_or_default() {
        let text = match attribute.value {
            serde_json::Value::String(text) => text,
            other => other.to_string(),
        };
        let data = if attribute.data_type.starts_with("Binary") {
            AttributeData::Binary(Bytes::from(BASE64.decode(text.trim()).map_err(|err| {
                Error::Envelope(format!("invalid binary attribute {name:?} in SNS notification: {err}"))
            })?))
        } else {
            AttributeData::Text(text)
        };
        let value = AttributeValue::with_type(attribute.data_type, data)?;
        attributes.insert(name, value);
    }
    Ok(Some(Unwrapped {
        message: raw.message.unwrap_or_default(),
        attributes,
        info: SnsInfo {
            kind,
            message_id: raw.message_id,
            topic_arn: raw.topic_arn,
            subject: raw.subject,
            timestamp: raw.timestamp.and_then(|t| OffsetDateTime::parse(&t, &Rfc3339).ok()),
            sequence_number: raw.sequence_number,
            token: raw.token,
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOTIFICATION: &str = r#"{
      "Type" : "Notification",
      "MessageId" : "22b80b92-fdea-4c2c-8f9d-bdfb0c7bf324",
      "TopicArn" : "arn:aws:sns:us-west-2:123456789012:MyTopic",
      "Subject" : "My First Message",
      "Message" : "{\"specversion\":\"1.0\"}",
      "Timestamp" : "2012-05-02T00:54:06.655Z",
      "SignatureVersion" : "1",
      "Signature" : "EXAMPLEw6JRN",
      "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-f3ecfb7224c7233fe7bb5f59f96de52f.pem",
      "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe",
      "MessageAttributes" : {
        "content-type" : {"Type":"String","Value":"application/cloudevents+json"},
        "count" : {"Type":"Number","Value":"5"},
        "blob" : {"Type":"Binary","Value":"AQID"}
      }
    }"#;

    #[test]
    fn unwraps_notifications() {
        let unwrapped = unwrap(NOTIFICATION, SnsUnwrap::Auto).unwrap().unwrap();
        assert_eq!(unwrapped.message, r#"{"specversion":"1.0"}"#);
        assert_eq!(unwrapped.info.kind, SnsMessageKind::Notification);
        assert_eq!(unwrapped.info.subject.as_deref(), Some("My First Message"));
        assert_eq!(unwrapped.info.topic_arn, "arn:aws:sns:us-west-2:123456789012:MyTopic");
        assert!(unwrapped.info.timestamp.is_some());
        assert_eq!(unwrapped.attributes.get("count"), Some(&AttributeValue::number("5")));
        assert_eq!(unwrapped.attributes.get("blob"), Some(&AttributeValue::binary(vec![1u8, 2, 3])));
    }

    #[test]
    fn ignores_other_bodies() {
        assert!(unwrap("hello", SnsUnwrap::Auto).unwrap().is_none());
        assert!(unwrap(r#"{"Type":"Notification"}"#, SnsUnwrap::Auto).unwrap().is_none());
        assert!(unwrap(r#"{"Type":"Other","TopicArn":"arn:x","MessageId":"1"}"#, SnsUnwrap::Auto).unwrap().is_none());
        assert!(unwrap(NOTIFICATION, SnsUnwrap::Never).unwrap().is_none());
    }

    #[test]
    fn recognises_confirmations() {
        let body = r#"{"Type":"SubscriptionConfirmation","MessageId":"1","Token":"tok","TopicArn":"arn:aws:sns:us-east-1:1:t","Message":"confirm","SubscribeURL":"https://x"}"#;
        let unwrapped = unwrap(body, SnsUnwrap::Auto).unwrap().unwrap();
        assert_eq!(unwrapped.info.kind, SnsMessageKind::SubscriptionConfirmation);
        assert_eq!(unwrapped.info.token.as_deref(), Some("tok"));
    }

    #[test]
    fn invalid_binary_attribute_errors() {
        let body = r#"{"Type":"Notification","MessageId":"1","TopicArn":"arn:aws:sns:us-east-1:1:t","Message":"m","MessageAttributes":{"b":{"Type":"Binary","Value":"***"}}}"#;
        assert!(unwrap(body, SnsUnwrap::Auto).is_err());
    }
}
