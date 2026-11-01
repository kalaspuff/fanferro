//! The raw envelope: the payload bytes are the body, metadata travels in attributes.
//!
//! fanferro's default for publishing: plain bodies (e.g. JSON documents) that any producer or
//! consumer understands, with the message id and type in two attributes. JSON payloads are sent
//! without a content-type attribute (it is implied, see [`RawEnvelope::implicit_content_type`]).
//! Because it recognises any message it must be the last envelope in a consumer's list.

use super::{Envelope, default_type_attribute, header_attributes, insert_envelope_attributes, text_attribute};
use crate::error::Result;
use crate::types::{AttributeValue, Headers, Payload, WireMessage, reserved};

/// Content type of payloads sent without a content-type attribute by default.
pub const IMPLICIT_CONTENT_TYPE: &str = "application/json";

/// The raw (no envelope) format.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RawEnvelope {
    /// Attribute carrying the payload content type (`None` to omit).
    pub content_type_attribute: Option<String>,
    /// Attribute carrying the message type (`None` to omit).
    pub type_attribute: Option<String>,
    /// Attribute carrying the message id (`None` to omit; the transport id is used instead).
    pub id_attribute: Option<String>,
    /// Content type that is not written as an attribute (default `application/json`; `None`
    /// writes every content type). Unwrapping assumes it for messages without a content-type
    /// attribute that carry this envelope's id or type attribute (messages from a fanferro
    /// producer); foreign messages keep an unknown content type.
    pub implicit_content_type: Option<String>,
}

impl Default for RawEnvelope {
    fn default() -> Self {
        Self {
            content_type_attribute: Some(reserved::CONTENT_TYPE.into()),
            type_attribute: default_type_attribute(),
            id_attribute: Some(reserved::MESSAGE_ID.into()),
            implicit_content_type: Some(IMPLICIT_CONTENT_TYPE.into()),
        }
    }
}

impl RawEnvelope {
    /// Whether `wire` carries this envelope's id or type attribute.
    fn has_metadata(&self, wire: &WireMessage) -> bool {
        [&self.id_attribute, &self.type_attribute]
            .into_iter()
            .any(|name| name.as_deref().is_some_and(|name| wire.attributes.contains_key(name)))
    }
}

impl Envelope for RawEnvelope {
    fn name(&self) -> &str {
        "raw"
    }

    fn wrap(&self, payload: &Payload) -> Result<WireMessage> {
        // Headers have nowhere else to go without an envelope: send them as attributes.
        let mut attributes = header_attributes(&payload.headers)?;
        let content_type = match (&self.content_type_attribute, &payload.content_type) {
            (Some(_), Some(value)) if self.implicit_content_type.as_deref() == Some(value.as_str()) => None,
            (Some(name), Some(value)) => Some((name.as_str(), value.as_str())),
            _ => None,
        };
        insert_envelope_attributes(
            &mut attributes,
            content_type,
            self.type_attribute.as_deref(),
            payload.message_type.as_deref(),
        );
        if let (Some(name), Some(id)) = (&self.id_attribute, &payload.id) {
            attributes.insert(name.clone(), AttributeValue::string(id.clone()));
        }
        Ok(WireMessage { body: payload.data.clone(), attributes })
    }

    fn unwrap(&self, wire: &WireMessage) -> Result<Option<Payload>> {
        let read = |name: &Option<String>| {
            name.as_deref().and_then(|n| text_attribute(&wire.attributes, n)).map(str::to_string)
        };
        let content_type = match read(&self.content_type_attribute) {
            None if self.content_type_attribute.is_some() && self.has_metadata(wire) => {
                self.implicit_content_type.clone()
            }
            content_type => content_type,
        };
        Ok(Some(Payload {
            data: wire.body.clone(),
            content_type,
            message_type: read(&self.type_attribute),
            id: read(&self.id_attribute),
            time: None,
            source: None,
            subject: None,
            data_schema: None,
            headers: Headers::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::types::{HeaderValue, MessageAttributes};

    #[test]
    fn roundtrip_with_metadata_attributes() {
        let envelope = RawEnvelope::default();
        let mut headers = Headers::new();
        headers.insert("tenant".into(), HeaderValue::String("acme".into()));
        headers.insert("version".into(), HeaderValue::Integer(2));
        let payload = Payload {
            data: Bytes::from_static(b"{\"a\":1}"),
            content_type: Some("application/json".into()),
            message_type: Some("orders.created".into()),
            id: Some("m-1".into()),
            headers,
            ..Payload::default()
        };
        let wire = envelope.wrap(&payload).unwrap();
        assert_eq!(wire.body, payload.data);
        assert_eq!(wire.attributes.get("tenant"), Some(&AttributeValue::string("acme")));
        assert_eq!(wire.attributes.get("version"), Some(&AttributeValue::number("2")));
        let decoded = envelope.unwrap(&wire).unwrap().unwrap();
        assert_eq!(decoded.content_type.as_deref(), Some("application/json"));
        assert_eq!(decoded.message_type.as_deref(), Some("orders.created"));
        assert_eq!(decoded.id.as_deref(), Some("m-1"));
    }

    #[test]
    fn accepts_foreign_messages() {
        let wire = WireMessage { body: Bytes::from_static(b"hello"), attributes: MessageAttributes::new() };
        let decoded = RawEnvelope::default().unwrap(&wire).unwrap().unwrap();
        assert_eq!(&decoded.data[..], b"hello");
        assert!(decoded.id.is_none());
        assert!(decoded.message_type.is_none());
    }

    #[test]
    fn json_content_type_is_implied() {
        let envelope = RawEnvelope::default();
        let json = Payload {
            data: Bytes::from_static(b"[1]"),
            content_type: Some("application/json".into()),
            id: Some("m-1".into()),
            ..Payload::default()
        };
        let wire = envelope.wrap(&json).unwrap();
        assert!(!wire.attributes.contains_key("content-type"));
        assert_eq!(envelope.unwrap(&wire).unwrap().unwrap().content_type.as_deref(), Some("application/json"));
        // Other content types (including JSON variants) are written as they are.
        for content_type in ["text/plain; charset=utf-8", "application/json; charset=utf-8", "application/x-protobuf"] {
            let payload = Payload { content_type: Some(content_type.into()), ..json.clone() };
            let wire = envelope.wrap(&payload).unwrap();
            assert_eq!(wire.attributes.get("content-type"), Some(&AttributeValue::string(content_type)));
            assert_eq!(envelope.unwrap(&wire).unwrap().unwrap().content_type.as_deref(), Some(content_type));
        }
        // Foreign messages (no fanferro metadata) keep an unknown content type.
        let foreign = WireMessage { body: Bytes::from_static(b"[1]"), attributes: MessageAttributes::new() };
        assert_eq!(envelope.unwrap(&foreign).unwrap().unwrap().content_type, None);
        // Without an implicit content type every content type is written (fanferro 0.1).
        let explicit = RawEnvelope { implicit_content_type: None, ..RawEnvelope::default() };
        let wire = explicit.wrap(&json).unwrap();
        assert_eq!(wire.attributes.get("content-type"), Some(&AttributeValue::string("application/json")));
        let lean = envelope.wrap(&json).unwrap();
        assert_eq!(explicit.unwrap(&lean).unwrap().unwrap().content_type, None);
    }

    #[test]
    fn attributes_can_be_disabled() {
        let envelope = RawEnvelope {
            content_type_attribute: None,
            type_attribute: None,
            id_attribute: None,
            implicit_content_type: None,
        };
        let payload = Payload {
            data: Bytes::from_static(b"x"),
            content_type: Some("text/plain".into()),
            message_type: Some("t".into()),
            id: Some("1".into()),
            ..Payload::default()
        };
        assert!(envelope.wrap(&payload).unwrap().attributes.is_empty());
    }
}
