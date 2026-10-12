//! The raw envelope: the payload bytes are the body, metadata travels in attributes.
//!
//! Use it to interoperate with producers/consumers that do not use fanferro (plain JSON
//! bodies, AWS console messages, other frameworks). Because it recognises any message it
//! must be the last envelope in a consumer's list.

use super::{Envelope, default_type_attribute, insert_envelope_attributes, text_attribute};
use crate::error::Result;
use crate::types::attributes::validate_attribute_name;
use crate::types::{AttributeValue, HeaderValue, Headers, MessageAttributes, Payload, WireMessage, reserved};

/// The raw (no envelope) format.
#[derive(Clone, Debug)]
pub struct RawEnvelope {
    /// Attribute carrying the payload content type (`None` to omit).
    pub content_type_attribute: Option<String>,
    /// Attribute carrying the message type (`None` to omit).
    pub type_attribute: Option<String>,
    /// Attribute carrying the message id (`None` to omit; the transport id is used instead).
    pub id_attribute: Option<String>,
}

impl Default for RawEnvelope {
    fn default() -> Self {
        Self {
            content_type_attribute: Some(reserved::CONTENT_TYPE.into()),
            type_attribute: default_type_attribute(),
            id_attribute: Some(reserved::MESSAGE_ID.into()),
        }
    }
}

impl Envelope for RawEnvelope {
    fn name(&self) -> &str {
        "raw"
    }

    fn wrap(&self, payload: &Payload) -> Result<WireMessage> {
        let mut attributes = MessageAttributes::new();
        // Headers have nowhere else to go without an envelope: send them as attributes.
        for (name, value) in &payload.headers {
            validate_attribute_name(name)?;
            let attribute = match value {
                HeaderValue::Integer(number) => AttributeValue::number(number.to_string()),
                other => AttributeValue::string(other.to_text()),
            };
            attributes.insert(name.clone(), attribute);
        }
        let content_type = match (&self.content_type_attribute, &payload.content_type) {
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
        Ok(Some(Payload {
            data: wire.body.clone(),
            content_type: read(&self.content_type_attribute),
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
    fn attributes_can_be_disabled() {
        let envelope = RawEnvelope { content_type_attribute: None, type_attribute: None, id_attribute: None };
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
