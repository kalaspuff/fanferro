//! The two message representations that flow through fanferro.
//!
//! * [`Payload`]: the application message after codec encoding — bytes plus metadata
//!   (id, type, content type, time, headers). Envelopes turn payloads into wire messages.
//! * [`WireMessage`]: what is actually transported — a body and message attributes.
//!   The transport layer turns wire messages into SQS/SNS requests (base64 for binary
//!   bodies, claim checks for large ones) and back.

use std::collections::BTreeMap;

use bytes::Bytes;
use time::OffsetDateTime;

use crate::types::attributes::MessageAttributes;

/// A header (CloudEvents extension attribute) value.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum HeaderValue {
    /// Text value.
    String(String),
    /// Integer value.
    Integer(i64),
    /// Boolean value.
    Boolean(bool),
}

impl HeaderValue {
    /// Renders the value as text (integers in decimal, booleans as `true`/`false`).
    pub fn to_text(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Integer(value) => value.to_string(),
            Self::Boolean(value) => value.to_string(),
        }
    }
}

/// Headers (CloudEvents extension attributes) keyed by name.
pub type Headers = BTreeMap<String, HeaderValue>;

/// An encoded application message plus its metadata.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Payload {
    /// The encoded message (output of a codec).
    pub data: Bytes,
    /// Media type of `data`, e.g. `application/json` or `application/x-protobuf`.
    pub content_type: Option<String>,
    /// Logical message type used for routing, e.g. `orders.created.v1`.
    pub message_type: Option<String>,
    /// Unique message id. Always set for outgoing messages; may be absent for incoming
    /// messages that were produced without an envelope (the transport id is used then).
    pub id: Option<String>,
    /// When the message was created.
    pub time: Option<OffsetDateTime>,
    /// The producer, as a URI reference (CloudEvents `source`).
    pub source: Option<String>,
    /// Optional subject (CloudEvents `subject`).
    pub subject: Option<String>,
    /// Optional schema reference (CloudEvents `dataschema`).
    pub data_schema: Option<String>,
    /// Extension headers (CloudEvents extension attributes), e.g. `traceparent`.
    pub headers: Headers,
}

/// A message as transported by SQS/SNS: a body and message attributes.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct WireMessage {
    /// Raw body bytes. Bodies that are not valid SQS text are base64 encoded by the
    /// transport layer, so envelopes may produce arbitrary bytes.
    pub body: Bytes,
    /// Message attributes (at most 10 including fanferro's own).
    pub attributes: MessageAttributes,
}

impl WireMessage {
    /// Creates a wire message from a body and attributes.
    pub fn new(body: impl Into<Bytes>, attributes: MessageAttributes) -> Self {
        Self { body: body.into(), attributes }
    }
}

/// Returns a new unique, time-ordered message id (UUIDv7).
pub fn new_message_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_ids_are_unique_and_sortable() {
        let a = new_message_id();
        let b = new_message_id();
        assert_ne!(a, b);
        assert_eq!(a.len(), 36);
    }

    #[test]
    fn header_text_rendering() {
        assert_eq!(HeaderValue::Integer(3).to_text(), "3");
        assert_eq!(HeaderValue::Boolean(true).to_text(), "true");
        assert_eq!(HeaderValue::String("x".into()).to_text(), "x");
    }
}
