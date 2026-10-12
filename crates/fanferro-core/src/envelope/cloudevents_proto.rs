//! CloudEvents 1.0 protobuf event format (binary envelope).
//!
//! Spec: <https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/formats/protobuf-format.md>.
//! The message definitions below mirror `io.cloudevents.v1.CloudEvent` from the spec's
//! `cloudevents.proto`, so events are readable by any CloudEvents protobuf SDK. Because the
//! body is binary, the transport layer base64-encodes it; the `content-type` attribute
//! (`application/cloudevents+protobuf`) identifies the format on the receiving side.

use std::collections::HashMap;

use bytes::Bytes;
use prost::Message as _;
use time::OffsetDateTime;

use super::{
    DEFAULT_MESSAGE_TYPE, DEFAULT_SOURCE, Envelope, cloudevents_json::validate_extension_name, default_type_attribute,
    insert_envelope_attributes, is_text_content_type, media_type_essence, text_attribute,
};
use crate::error::{Error, Result};
use crate::types::{HeaderValue, Headers, MessageAttributes, Payload, WireMessage, new_message_id, reserved};

/// Media type of a protobuf-encoded CloudEvent.
pub const CLOUDEVENTS_PROTOBUF_CONTENT_TYPE: &str = "application/cloudevents+protobuf";

/// `io.cloudevents.v1.CloudEvent`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct CloudEventProto {
    /// Required: event id.
    #[prost(string, tag = "1")]
    pub id: String,
    /// Required: URI-reference of the producer.
    #[prost(string, tag = "2")]
    pub source: String,
    /// Required: spec version (`1.0`).
    #[prost(string, tag = "3")]
    pub spec_version: String,
    /// Required: event type.
    #[prost(string, tag = "4")]
    pub r#type: String,
    /// Optional and extension attributes.
    #[prost(map = "string, message", tag = "5")]
    pub attributes: HashMap<String, CloudEventAttributeValue>,
    /// Event data.
    #[prost(oneof = "Data", tags = "6, 7, 8")]
    pub data: Option<Data>,
}

/// `CloudEvent.data` oneof (variant names mirror the CloudEvents proto definition).
#[derive(Clone, PartialEq, prost::Oneof)]
#[allow(clippy::enum_variant_names)]
pub enum Data {
    /// Binary data.
    #[prost(bytes, tag = "6")]
    BinaryData(Vec<u8>),
    /// Text data.
    #[prost(string, tag = "7")]
    TextData(String),
    /// Protobuf message data.
    #[prost(message, tag = "8")]
    ProtoData(prost_types::Any),
}

/// `io.cloudevents.v1.CloudEvent.CloudEventAttributeValue`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct CloudEventAttributeValue {
    /// The attribute value.
    #[prost(oneof = "Attr", tags = "1, 2, 3, 4, 5, 6, 7")]
    pub attr: Option<Attr>,
}

/// `CloudEventAttributeValue.attr` oneof (variant names mirror the CloudEvents proto definition).
#[derive(Clone, PartialEq, prost::Oneof)]
#[allow(clippy::enum_variant_names)]
pub enum Attr {
    /// Boolean.
    #[prost(bool, tag = "1")]
    CeBoolean(bool),
    /// 32-bit integer.
    #[prost(int32, tag = "2")]
    CeInteger(i32),
    /// String.
    #[prost(string, tag = "3")]
    CeString(String),
    /// Bytes.
    #[prost(bytes, tag = "4")]
    CeBytes(Vec<u8>),
    /// Absolute URI.
    #[prost(string, tag = "5")]
    CeUri(String),
    /// URI reference.
    #[prost(string, tag = "6")]
    CeUriRef(String),
    /// Timestamp.
    #[prost(message, tag = "7")]
    CeTimestamp(prost_types::Timestamp),
}

/// The CloudEvents protobuf (binary) envelope.
#[derive(Clone, Debug)]
pub struct CloudEventsProtobuf {
    /// Promote the message type to this message attribute (for SNS filter policies).
    pub type_attribute: Option<String>,
    /// `source` used when the payload has none.
    pub default_source: String,
}

impl Default for CloudEventsProtobuf {
    fn default() -> Self {
        Self { type_attribute: default_type_attribute(), default_source: DEFAULT_SOURCE.into() }
    }
}

fn string_attr(value: impl Into<String>) -> CloudEventAttributeValue {
    CloudEventAttributeValue { attr: Some(Attr::CeString(value.into())) }
}

fn to_timestamp(time: OffsetDateTime) -> prost_types::Timestamp {
    prost_types::Timestamp { seconds: time.unix_timestamp(), nanos: i32::try_from(time.nanosecond()).unwrap_or(0) }
}

fn from_timestamp(ts: &prost_types::Timestamp) -> Option<OffsetDateTime> {
    let nanos = i128::from(ts.seconds) * 1_000_000_000 + i128::from(ts.nanos);
    OffsetDateTime::from_unix_timestamp_nanos(nanos).ok()
}

fn attr_text(value: &CloudEventAttributeValue) -> Option<String> {
    match value.attr.as_ref()? {
        Attr::CeString(text) | Attr::CeUri(text) | Attr::CeUriRef(text) => Some(text.clone()),
        Attr::CeBoolean(flag) => Some(flag.to_string()),
        Attr::CeInteger(number) => Some(number.to_string()),
        Attr::CeBytes(_) | Attr::CeTimestamp(_) => None,
    }
}

impl Envelope for CloudEventsProtobuf {
    fn name(&self) -> &str {
        "cloudevents-protobuf"
    }

    fn wrap(&self, payload: &Payload) -> Result<WireMessage> {
        let message_type =
            payload.message_type.clone().filter(|t| !t.is_empty()).unwrap_or_else(|| DEFAULT_MESSAGE_TYPE.into());
        let mut attributes = HashMap::new();
        if let Some(content_type) = &payload.content_type {
            attributes.insert("datacontenttype".to_string(), string_attr(content_type));
        }
        if let Some(schema) = &payload.data_schema {
            attributes
                .insert("dataschema".to_string(), CloudEventAttributeValue { attr: Some(Attr::CeUri(schema.clone())) });
        }
        if let Some(subject) = &payload.subject {
            attributes.insert("subject".to_string(), string_attr(subject));
        }
        let time = payload.time.unwrap_or_else(OffsetDateTime::now_utc);
        attributes
            .insert("time".to_string(), CloudEventAttributeValue { attr: Some(Attr::CeTimestamp(to_timestamp(time))) });
        for (name, value) in &payload.headers {
            validate_extension_name(name)?;
            let attr = match value {
                HeaderValue::String(text) => Attr::CeString(text.clone()),
                HeaderValue::Boolean(flag) => Attr::CeBoolean(*flag),
                HeaderValue::Integer(number) => match i32::try_from(*number) {
                    Ok(small) => Attr::CeInteger(small),
                    Err(_) => Attr::CeString(number.to_string()),
                },
            };
            attributes.insert(name.clone(), CloudEventAttributeValue { attr: Some(attr) });
        }
        let data = if payload.data.is_empty() {
            None
        } else {
            match (payload.content_type.as_deref(), std::str::from_utf8(&payload.data)) {
                (Some(ct), Ok(text)) if is_text_content_type(ct) => Some(Data::TextData(text.to_string())),
                _ => Some(Data::BinaryData(payload.data.to_vec())),
            }
        };
        let event = CloudEventProto {
            id: payload.id.clone().filter(|id| !id.is_empty()).unwrap_or_else(new_message_id),
            source: payload.source.clone().filter(|s| !s.is_empty()).unwrap_or_else(|| self.default_source.clone()),
            spec_version: "1.0".into(),
            r#type: message_type.clone(),
            attributes,
            data,
        };
        let mut wire_attributes = MessageAttributes::new();
        insert_envelope_attributes(
            &mut wire_attributes,
            Some((reserved::CONTENT_TYPE, CLOUDEVENTS_PROTOBUF_CONTENT_TYPE)),
            self.type_attribute.as_deref(),
            Some(&message_type),
        );
        Ok(WireMessage { body: Bytes::from(event.encode_to_vec()), attributes: wire_attributes })
    }

    fn unwrap(&self, wire: &WireMessage) -> Result<Option<Payload>> {
        match text_attribute(&wire.attributes, reserved::CONTENT_TYPE) {
            Some(ct) if media_type_essence(ct) == CLOUDEVENTS_PROTOBUF_CONTENT_TYPE => {}
            _ => return Ok(None),
        }
        let event = CloudEventProto::decode(wire.body.as_ref())
            .map_err(|err| Error::Envelope(format!("invalid CloudEvents protobuf: {err}")))?;
        if !event.spec_version.starts_with("1.") {
            return Err(Error::Envelope(format!("unsupported CloudEvents specversion {:?}", event.spec_version)));
        }
        for (name, value) in [("id", &event.id), ("source", &event.source), ("type", &event.r#type)] {
            if value.is_empty() {
                return Err(Error::Envelope(format!("CloudEvents attribute {name:?} is required")));
            }
        }
        let mut content_type = None;
        let mut data_schema = None;
        let mut subject = None;
        let mut time = None;
        let mut headers = Headers::new();
        for (name, value) in &event.attributes {
            match name.as_str() {
                "datacontenttype" => content_type = attr_text(value),
                "dataschema" => data_schema = attr_text(value),
                "subject" => subject = attr_text(value),
                "time" => {
                    time = match &value.attr {
                        Some(Attr::CeTimestamp(ts)) => from_timestamp(ts),
                        _ => None,
                    }
                }
                _ => {
                    let header = match &value.attr {
                        Some(Attr::CeBoolean(flag)) => HeaderValue::Boolean(*flag),
                        Some(Attr::CeInteger(number)) => HeaderValue::Integer(i64::from(*number)),
                        Some(Attr::CeTimestamp(ts)) => match from_timestamp(ts)
                            .and_then(|t| t.format(&time::format_description::well_known::Rfc3339).ok())
                        {
                            Some(text) => HeaderValue::String(text),
                            None => continue,
                        },
                        Some(Attr::CeBytes(bytes)) => {
                            use base64::Engine as _;
                            HeaderValue::String(base64::engine::general_purpose::STANDARD.encode(bytes))
                        }
                        Some(Attr::CeString(text) | Attr::CeUri(text) | Attr::CeUriRef(text)) => {
                            HeaderValue::String(text.clone())
                        }
                        None => continue,
                    };
                    headers.insert(name.clone(), header);
                }
            }
        }
        let mut message_type = Some(event.r#type);
        let data = match event.data {
            Some(Data::BinaryData(bytes)) => Bytes::from(bytes),
            Some(Data::TextData(text)) => Bytes::from(text.into_bytes()),
            Some(Data::ProtoData(any)) => {
                if content_type.is_none() {
                    content_type = Some("application/x-protobuf".into());
                }
                if message_type.as_deref().is_none_or(str::is_empty) {
                    message_type = any.type_url.rsplit('/').next().map(str::to_string);
                }
                Bytes::from(any.value)
            }
            None => Bytes::new(),
        };
        Ok(Some(Payload {
            data,
            content_type,
            message_type,
            id: Some(event.id),
            time,
            source: Some(event.source),
            subject,
            data_schema,
            headers,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_binary_payload() {
        let envelope = CloudEventsProtobuf::default();
        let mut headers = Headers::new();
        headers.insert("traceparent".into(), HeaderValue::String("00-a-b-01".into()));
        headers.insert("version".into(), HeaderValue::Integer(3));
        headers.insert("huge".into(), HeaderValue::Integer(i64::MAX));
        headers.insert("flag".into(), HeaderValue::Boolean(false));
        let payload = Payload {
            data: Bytes::from_static(&[8, 150, 1, 0, 255]),
            content_type: Some("application/x-protobuf".into()),
            message_type: Some("acme.orders.v1.OrderCreated".into()),
            id: Some("id-1".into()),
            time: Some(OffsetDateTime::from_unix_timestamp_nanos(1_700_000_000_123_456_789).unwrap()),
            source: Some("urn:svc".into()),
            subject: Some("order/1".into()),
            data_schema: Some("https://schemas/x".into()),
            headers: headers.clone(),
        };
        let wire = envelope.wrap(&payload).unwrap();
        assert_eq!(
            wire.attributes.get("content-type").and_then(|v| v.as_text()),
            Some(CLOUDEVENTS_PROTOBUF_CONTENT_TYPE)
        );
        let decoded = envelope.unwrap(&wire).unwrap().unwrap();
        assert_eq!(decoded.data, payload.data);
        assert_eq!(decoded.id, payload.id);
        assert_eq!(decoded.message_type, payload.message_type);
        assert_eq!(decoded.content_type, payload.content_type);
        assert_eq!(decoded.time, payload.time);
        assert_eq!(decoded.subject, payload.subject);
        assert_eq!(decoded.data_schema, payload.data_schema);
        let mut expected = headers;
        expected.insert("huge".into(), HeaderValue::String(i64::MAX.to_string()));
        assert_eq!(decoded.headers, expected);
    }

    #[test]
    fn text_payloads_use_text_data() {
        let envelope = CloudEventsProtobuf::default();
        let payload = Payload {
            data: Bytes::from_static(br#"{"a":1}"#),
            content_type: Some("application/json".into()),
            message_type: Some("t".into()),
            ..Payload::default()
        };
        let wire = envelope.wrap(&payload).unwrap();
        let event = CloudEventProto::decode(wire.body.as_ref()).unwrap();
        assert!(matches!(event.data, Some(Data::TextData(_))));
        assert_eq!(envelope.unwrap(&wire).unwrap().unwrap().data, payload.data);
    }

    #[test]
    fn requires_content_type_marker() {
        let envelope = CloudEventsProtobuf::default();
        let wire = WireMessage { body: Bytes::from_static(b"\x0a\x01x"), attributes: MessageAttributes::new() };
        assert!(envelope.unwrap(&wire).unwrap().is_none());
    }

    #[test]
    fn proto_data_any_is_supported() {
        let event = CloudEventProto {
            id: "1".into(),
            source: "s".into(),
            spec_version: "1.0".into(),
            r#type: "acme.Thing".into(),
            attributes: HashMap::new(),
            data: Some(Data::ProtoData(prost_types::Any {
                type_url: "type.googleapis.com/acme.Thing".into(),
                value: vec![1, 2, 3],
            })),
        };
        let mut attributes = MessageAttributes::new();
        attributes
            .insert("content-type".into(), crate::types::AttributeValue::string(CLOUDEVENTS_PROTOBUF_CONTENT_TYPE));
        let wire = WireMessage { body: Bytes::from(event.encode_to_vec()), attributes };
        let decoded = CloudEventsProtobuf::default().unwrap(&wire).unwrap().unwrap();
        assert_eq!(&decoded.data[..], &[1, 2, 3]);
        assert_eq!(decoded.content_type.as_deref(), Some("application/x-protobuf"));
    }

    #[test]
    fn garbage_with_marker_errors() {
        let mut attributes = MessageAttributes::new();
        attributes
            .insert("content-type".into(), crate::types::AttributeValue::string(CLOUDEVENTS_PROTOBUF_CONTENT_TYPE));
        let wire = WireMessage { body: Bytes::from_static(&[0xff, 0xff, 0xff]), attributes };
        assert!(CloudEventsProtobuf::default().unwrap(&wire).is_err());
    }
}
