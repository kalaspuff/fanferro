//! Envelopes: how a [`Payload`] (encoded message + metadata) is laid out on the wire.
//!
//! An envelope turns a payload into a [`WireMessage`] (body + attributes) when publishing
//! and recognises/parses its own format when consuming. Built-in envelopes:
//!
//! | name                   | body                                  | detection                                   |
//! |------------------------|---------------------------------------|---------------------------------------------|
//! | `cloudevents`          | CloudEvents 1.0 structured JSON       | `content-type` attribute or JSON sniffing    |
//! | `cloudevents-protobuf` | CloudEvents 1.0 protobuf (binary)     | `content-type` attribute                     |
//! | `aws`                  | S3 and SES notification documents     | the document's fields                        |
//! | `raw`                  | payload bytes as-is                   | always matches (use last)                    |
//! | `tomodachi-json`       | tomodachi's `JsonBase` envelope       | `metadata.protocol_version` in the body      |
//! | `tomodachi-protobuf`   | tomodachi's `ProtobufBase` (base64)   | `metadata.protocol_version` in the body      |
//!
//! Consumers try their configured envelopes in order; the first that returns
//! `Ok(Some(payload))` wins. Custom envelopes implemented in Python follow the same
//! protocol (see `python/fanferro/envelopes/__init__.py`).

mod aws;
mod cloudevents_json;
mod cloudevents_proto;
mod raw;
mod tomodachi;

use std::fmt::Debug;
use std::sync::Arc;

pub use aws::{AWS_S3_EVENT_TYPE, AWS_SES_TYPE_PREFIX, AwsNotifications};
pub use cloudevents_json::{CLOUDEVENTS_JSON_CONTENT_TYPE, CloudEventsJson};
pub use cloudevents_proto::{CLOUDEVENTS_PROTOBUF_CONTENT_TYPE, CloudEventsProtobuf};
pub use raw::{IMPLICIT_CONTENT_TYPE, RawEnvelope};
pub use tomodachi::{
    TOMODACHI_COMPRESS_THRESHOLD, TOMODACHI_JSON_PROTOCOL_VERSION, TOMODACHI_PROTOBUF_PROTOCOL_VERSION,
    TomodachiDestination, TomodachiJson, TomodachiProtobuf, decode_tomodachi_topic,
};

use crate::error::{Error, Result};
use crate::types::attributes::validate_attribute_name;
use crate::types::{AttributeValue, HeaderValue, Headers, MessageAttributes, Payload, WireMessage, reserved};

/// Message type used when a payload has none (CloudEvents requires a type).
pub const DEFAULT_MESSAGE_TYPE: &str = "message";

/// Default CloudEvents `source` when neither the payload nor the envelope sets one.
pub const DEFAULT_SOURCE: &str = "fanferro";

/// Wraps payloads into wire messages and recognises/unwraps its own wire format.
pub trait Envelope: Send + Sync + Debug {
    /// Stable identifier of the envelope format, e.g. `"cloudevents"`.
    fn name(&self) -> &str;

    /// Produces the wire representation of `payload`.
    fn wrap(&self, payload: &Payload) -> Result<WireMessage>;

    /// Parses `wire` if it is in this envelope's format.
    ///
    /// Returns `Ok(None)` when the message is not in this format (so the next envelope can
    /// try), and `Err` when it is in this format but malformed.
    fn unwrap(&self, wire: &WireMessage) -> Result<Option<Payload>>;
}

/// An ordered list of envelopes used to decode incoming messages.
#[derive(Clone, Debug)]
pub struct EnvelopeSet {
    envelopes: Vec<Arc<dyn Envelope>>,
}

impl EnvelopeSet {
    /// Creates a set from envelopes in priority order.
    pub fn new(envelopes: Vec<Arc<dyn Envelope>>) -> Self {
        Self { envelopes }
    }

    /// The default consumer set: CloudEvents JSON, CloudEvents protobuf, AWS notifications, raw.
    pub fn default_set() -> Self {
        Self::new(vec![
            Arc::new(CloudEventsJson::default()),
            Arc::new(CloudEventsProtobuf::default()),
            Arc::new(AwsNotifications),
            Arc::new(RawEnvelope::default()),
        ])
    }

    /// Unwraps `wire` with the first matching envelope, returning the payload and the
    /// name of the envelope that matched.
    pub fn unwrap(&self, wire: &WireMessage) -> Result<(Payload, String)> {
        match self.unwrap_indexed(wire)? {
            Some((payload, index)) => Ok((payload, self.envelopes[index].name().to_string())),
            None => Err(Error::Envelope(format!(
                "no configured envelope ({}) recognised the message",
                self.envelopes.iter().map(|e| e.name()).collect::<Vec<_>>().join(", ")
            ))),
        }
    }

    /// Unwraps `wire` with the first matching envelope, returning the payload and the position of
    /// the envelope that matched (`None` when no envelope recognises the message).
    pub fn unwrap_indexed(&self, wire: &WireMessage) -> Result<Option<(Payload, usize)>> {
        for (index, envelope) in self.envelopes.iter().enumerate() {
            if let Some(payload) = envelope.unwrap(wire)? {
                return Ok(Some((payload, index)));
            }
        }
        Ok(None)
    }

    /// The envelopes, in priority order.
    pub fn envelopes(&self) -> &[Arc<dyn Envelope>] {
        &self.envelopes
    }
}

/// Returns `true` for JSON media types (`application/json`, `*/*+json`, `text/json`).
pub fn is_json_content_type(content_type: &str) -> bool {
    let essence = media_type_essence(content_type);
    essence == "application/json" || essence == "text/json" || essence.ends_with("+json")
}

/// Returns `true` for textual media types (`text/*`, JSON, XML, YAML).
pub fn is_text_content_type(content_type: &str) -> bool {
    let essence = media_type_essence(content_type);
    essence.starts_with("text/")
        || is_json_content_type(content_type)
        || essence == "application/xml"
        || essence.ends_with("+xml")
        || essence == "application/yaml"
        || essence == "application/x-yaml"
}

/// Lowercased media type without parameters (`"Application/JSON; charset=utf-8"` -> `"application/json"`).
pub fn media_type_essence(content_type: &str) -> String {
    content_type.split(';').next().unwrap_or_default().trim().to_ascii_lowercase()
}

/// `text` as bytes: a slice of `source` when `text` lies in it (no copy), a copy otherwise.
pub(crate) fn share(source: &bytes::Bytes, text: &str) -> bytes::Bytes {
    let start = (text.as_ptr() as usize).wrapping_sub(source.as_ptr() as usize);
    if start <= source.len() && text.len() <= source.len() - start {
        source.slice(start..start + text.len())
    } else {
        bytes::Bytes::copy_from_slice(text.as_bytes())
    }
}

/// Reads a text attribute.
pub(crate) fn text_attribute<'a>(attributes: &'a MessageAttributes, name: &str) -> Option<&'a str> {
    attributes.get(name).and_then(AttributeValue::as_text)
}

/// Inserts the optional `content-type` and type attributes shared by the built-ins.
pub(crate) fn insert_envelope_attributes(
    attributes: &mut MessageAttributes,
    content_type: Option<(&str, &str)>,
    type_attribute: Option<&str>,
    message_type: Option<&str>,
) {
    if let Some((name, value)) = content_type {
        attributes.insert(name.to_string(), AttributeValue::string(value));
    }
    if let (Some(name), Some(message_type)) = (type_attribute, message_type)
        && !message_type.is_empty()
    {
        attributes.insert(name.to_string(), AttributeValue::string(message_type));
    }
}

/// Message attributes carrying payload headers, for envelopes without room for them in the body.
pub(crate) fn header_attributes(headers: &Headers) -> Result<MessageAttributes> {
    let mut attributes = MessageAttributes::new();
    for (name, value) in headers {
        validate_attribute_name(name)?;
        let attribute = match value {
            HeaderValue::Integer(number) => AttributeValue::number(number.to_string()),
            other => AttributeValue::string(other.to_text()),
        };
        attributes.insert(name.clone(), attribute);
    }
    Ok(attributes)
}

/// Default name of the promoted message type attribute.
pub fn default_type_attribute() -> Option<String> {
    Some(reserved::MESSAGE_TYPE.to_string())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn content_type_helpers() {
        assert!(is_json_content_type("application/json"));
        assert!(is_json_content_type("Application/JSON; charset=utf-8"));
        assert!(is_json_content_type("application/cloudevents+json"));
        assert!(!is_json_content_type("application/x-protobuf"));
        assert!(is_text_content_type("text/plain"));
        assert!(is_text_content_type("application/json"));
        assert!(!is_text_content_type("application/octet-stream"));
    }

    #[test]
    fn default_set_detects_each_format() {
        let set = EnvelopeSet::default_set();
        let payload = Payload {
            data: Bytes::from_static(br#"{"a":1}"#),
            content_type: Some("application/json".into()),
            message_type: Some("t".into()),
            id: Some("1".into()),
            ..Payload::default()
        };
        let wire = CloudEventsJson::default().wrap(&payload).unwrap();
        assert_eq!(set.unwrap(&wire).unwrap().1, "cloudevents");
        let wire = CloudEventsProtobuf::default().wrap(&payload).unwrap();
        assert_eq!(set.unwrap(&wire).unwrap().1, "cloudevents-protobuf");
        let wire = RawEnvelope::default().wrap(&payload).unwrap();
        assert_eq!(set.unwrap(&wire).unwrap().1, "raw");
    }

    #[test]
    fn empty_set_reports_error() {
        let set = EnvelopeSet::new(vec![]);
        assert!(set.unwrap(&WireMessage::default()).is_err());
        let set = EnvelopeSet::new(vec![Arc::new(CloudEventsJson::default())]);
        assert!(set.unwrap_indexed(&WireMessage::default()).unwrap().is_none());
    }

    #[test]
    fn indexed_unwrap_reports_the_matching_position() {
        let set = EnvelopeSet::default_set();
        let wire =
            RawEnvelope::default().wrap(&Payload { data: Bytes::from_static(b"x"), ..Payload::default() }).unwrap();
        assert_eq!(set.unwrap_indexed(&wire).unwrap().unwrap().1, 3);
        assert_eq!(set.envelopes()[3].name(), "raw");
    }
}
