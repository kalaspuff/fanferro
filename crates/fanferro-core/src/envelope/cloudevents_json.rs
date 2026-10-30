//! CloudEvents 1.0 structured mode, JSON event format.
//!
//! Spec: <https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/formats/json-format.md>.
//! JSON payloads are embedded verbatim in `data` (no re-serialization, so number precision
//! and key order are preserved), textual payloads as a JSON string, and anything else as
//! `data_base64`. Payload headers become CloudEvents extension attributes, which is how
//! the distributed tracing extension (`traceparent`/`tracestate`) travels in the envelope.

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use serde::Serialize;
use serde_json::value::RawValue;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use super::{
    DEFAULT_MESSAGE_TYPE, DEFAULT_SOURCE, Envelope, default_type_attribute, insert_envelope_attributes,
    is_json_content_type, is_text_content_type, media_type_essence, text_attribute,
};
use crate::error::{Error, Result};
use crate::types::{HeaderValue, Headers, MessageAttributes, Payload, WireMessage, new_message_id, reserved};

/// Media type of a structured-mode JSON CloudEvent.
pub const CLOUDEVENTS_JSON_CONTENT_TYPE: &str = "application/cloudevents+json";

const CORE_ATTRIBUTES: &[&str] =
    &["specversion", "id", "source", "type", "datacontenttype", "dataschema", "subject", "time", "data", "data_base64"];

/// The CloudEvents structured JSON envelope (fanferro's default).
#[derive(Clone, Debug)]
pub struct CloudEventsJson {
    /// Write `content-type: application/cloudevents+json` as a message attribute.
    /// Consumers still detect events without it by sniffing the JSON body.
    pub content_type_attribute: bool,
    /// Promote the message type to this message attribute (for SNS filter policies).
    pub type_attribute: Option<String>,
    /// `source` used when the payload has none.
    pub default_source: String,
}

impl Default for CloudEventsJson {
    fn default() -> Self {
        Self {
            content_type_attribute: true,
            type_attribute: default_type_attribute(),
            default_source: DEFAULT_SOURCE.into(),
        }
    }
}

#[derive(Serialize)]
struct OutgoingEvent<'a> {
    specversion: &'static str,
    id: &'a str,
    source: &'a str,
    #[serde(rename = "type")]
    message_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    datacontenttype: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dataschema: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    subject: Option<&'a str>,
    time: String,
    #[serde(flatten)]
    extensions: BTreeMap<&'a str, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<DataField<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data_base64: Option<String>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum DataField<'a> {
    Raw(&'a RawValue),
    Text(&'a str),
}

/// Validates a CloudEvents extension attribute name (lowercase ASCII letters and digits).
pub fn validate_extension_name(name: &str) -> Result<()> {
    if name.is_empty() || !name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()) {
        return Err(Error::Envelope(format!(
            "CloudEvents extension (header) name {name:?} must consist of lowercase letters a-z and digits 0-9"
        )));
    }
    if CORE_ATTRIBUTES.contains(&name) {
        return Err(Error::Envelope(format!("header name {name:?} collides with a CloudEvents core attribute")));
    }
    Ok(())
}

fn header_to_json(value: &HeaderValue) -> serde_json::Value {
    match value {
        HeaderValue::String(text) => serde_json::Value::String(text.clone()),
        HeaderValue::Integer(number) => serde_json::Value::from(*number),
        HeaderValue::Boolean(flag) => serde_json::Value::Bool(*flag),
    }
}

impl Envelope for CloudEventsJson {
    fn name(&self) -> &str {
        "cloudevents"
    }

    fn wrap(&self, payload: &Payload) -> Result<WireMessage> {
        let generated_id;
        let id = match payload.id.as_deref() {
            Some(id) if !id.is_empty() => id,
            _ => {
                generated_id = new_message_id();
                &generated_id
            }
        };
        let message_type = payload.message_type.as_deref().filter(|t| !t.is_empty()).unwrap_or(DEFAULT_MESSAGE_TYPE);
        let source = payload.source.as_deref().filter(|s| !s.is_empty()).unwrap_or(&self.default_source);
        let time = payload
            .time
            .unwrap_or_else(OffsetDateTime::now_utc)
            .format(&Rfc3339)
            .map_err(|err| Error::Envelope(format!("cannot format event time: {err}")))?;

        let mut extensions = BTreeMap::new();
        for (name, value) in &payload.headers {
            validate_extension_name(name)?;
            extensions.insert(name.as_str(), header_to_json(value));
        }

        let content_type = payload.content_type.as_deref();
        let text = std::str::from_utf8(&payload.data).ok();
        let raw_json = if payload.data.is_empty() {
            None
        } else {
            match (content_type, text) {
                (None, Some(text)) => serde_json::from_str::<&RawValue>(text).ok(),
                (Some(ct), Some(text)) if is_json_content_type(ct) => serde_json::from_str::<&RawValue>(text).ok(),
                _ => None,
            }
        };
        let (data, data_base64) = if payload.data.is_empty() {
            (None, None)
        } else if let Some(raw) = raw_json {
            (Some(DataField::Raw(raw)), None)
        } else if let (Some(ct), Some(text)) = (content_type, text)
            && is_text_content_type(ct)
            && !is_json_content_type(ct)
        {
            (Some(DataField::Text(text)), None)
        } else {
            (None, Some(BASE64.encode(&payload.data)))
        };

        let event = OutgoingEvent {
            specversion: "1.0",
            id,
            source,
            message_type,
            datacontenttype: content_type,
            dataschema: payload.data_schema.as_deref(),
            subject: payload.subject.as_deref(),
            time,
            extensions,
            data,
            data_base64,
        };
        let body =
            serde_json::to_vec(&event).map_err(|err| Error::Envelope(format!("cannot serialize CloudEvent: {err}")))?;

        let mut attributes = MessageAttributes::new();
        insert_envelope_attributes(
            &mut attributes,
            self.content_type_attribute.then_some((reserved::CONTENT_TYPE, CLOUDEVENTS_JSON_CONTENT_TYPE)),
            self.type_attribute.as_deref(),
            Some(message_type),
        );
        Ok(WireMessage { body: Bytes::from(body), attributes })
    }

    fn unwrap(&self, wire: &WireMessage) -> Result<Option<Payload>> {
        match text_attribute(&wire.attributes, reserved::CONTENT_TYPE) {
            Some(ct) if media_type_essence(ct) == CLOUDEVENTS_JSON_CONTENT_TYPE => parse_event(&wire.body).map(Some),
            Some(_) => Ok(None),
            // The raw envelope's id attribute (never written with a CloudEvent): a raw message,
            // even when its JSON body is a CloudEvent itself (a relayed event).
            None if wire.attributes.contains_key(reserved::MESSAGE_ID) => Ok(None),
            None => {
                // Without the attribute, only a JSON object with a `specversion` key can be a
                // CloudEvent: other bodies (raw JSON, the default envelope) are not parsed here.
                let trimmed = wire.body.trim_ascii_start();
                if !trimmed.starts_with(b"{") || memchr::memmem::find(trimmed, b"\"specversion\"").is_none() {
                    return Ok(None);
                }
                let Ok(map) = serde_json::from_slice::<BTreeMap<String, &RawValue>>(&wire.body) else {
                    return Ok(None);
                };
                if !map.contains_key("specversion") {
                    return Ok(None);
                }
                parse_map(map).map(Some)
            }
        }
    }
}

fn parse_event(body: &[u8]) -> Result<Payload> {
    let map: BTreeMap<String, &RawValue> =
        serde_json::from_slice(body).map_err(|err| Error::Envelope(format!("invalid CloudEvents JSON: {err}")))?;
    parse_map(map)
}

fn string_field(map: &BTreeMap<String, &RawValue>, name: &str) -> Result<Option<String>> {
    match map.get(name) {
        None => Ok(None),
        Some(raw) if raw.get() == "null" => Ok(None),
        Some(raw) => serde_json::from_str::<String>(raw.get())
            .map(Some)
            .map_err(|_| Error::Envelope(format!("CloudEvents attribute {name:?} must be a string"))),
    }
}

fn required_field(map: &BTreeMap<String, &RawValue>, name: &str) -> Result<String> {
    match string_field(map, name)? {
        Some(value) if !value.is_empty() => Ok(value),
        _ => Err(Error::Envelope(format!("CloudEvents attribute {name:?} is required"))),
    }
}

fn parse_map(map: BTreeMap<String, &RawValue>) -> Result<Payload> {
    let specversion = required_field(&map, "specversion")?;
    if !specversion.starts_with("1.") {
        return Err(Error::Envelope(format!("unsupported CloudEvents specversion {specversion:?}")));
    }
    let id = required_field(&map, "id")?;
    let source = required_field(&map, "source")?;
    let message_type = required_field(&map, "type")?;
    let mut content_type = string_field(&map, "datacontenttype")?;
    let data_schema = string_field(&map, "dataschema")?;
    let subject = string_field(&map, "subject")?;
    let time = string_field(&map, "time")?.and_then(|text| OffsetDateTime::parse(&text, &Rfc3339).ok());

    let data_base64 = string_field(&map, "data_base64")?;
    let data_raw = map.get("data").copied().filter(|raw| raw.get() != "null");
    let data = match (data_raw, data_base64) {
        (Some(_), Some(_)) => {
            return Err(Error::Envelope("CloudEvent must not contain both data and data_base64".into()));
        }
        (None, Some(encoded)) => Bytes::from(
            BASE64
                .decode(encoded.trim())
                .map_err(|err| Error::Envelope(format!("invalid data_base64 in CloudEvent: {err}")))?,
        ),
        (Some(raw), None) => {
            let json_typed = content_type.as_deref().is_none_or(is_json_content_type);
            if json_typed {
                if content_type.is_none() {
                    content_type = Some("application/json".into());
                }
                Bytes::copy_from_slice(raw.get().as_bytes())
            } else if let Ok(text) = serde_json::from_str::<String>(raw.get()) {
                Bytes::from(text.into_bytes())
            } else {
                Bytes::copy_from_slice(raw.get().as_bytes())
            }
        }
        (None, None) => Bytes::new(),
    };

    let mut headers = Headers::new();
    for (name, raw) in &map {
        if CORE_ATTRIBUTES.contains(&name.as_str()) {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(raw.get())
            .map_err(|err| Error::Envelope(format!("invalid extension {name:?}: {err}")))?;
        let header = match value {
            serde_json::Value::Null => continue,
            serde_json::Value::String(text) => HeaderValue::String(text),
            serde_json::Value::Bool(flag) => HeaderValue::Boolean(flag),
            serde_json::Value::Number(number) => match number.as_i64() {
                Some(integer) => HeaderValue::Integer(integer),
                None => HeaderValue::String(number.to_string()),
            },
            other => HeaderValue::String(other.to_string()),
        };
        headers.insert(name.clone(), header);
    }

    Ok(Payload {
        data,
        content_type,
        message_type: Some(message_type),
        id: Some(id),
        time,
        source: Some(source),
        subject,
        data_schema,
        headers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::AttributeValue;

    fn payload(data: &'static [u8], content_type: Option<&str>) -> Payload {
        Payload {
            data: Bytes::from_static(data),
            content_type: content_type.map(str::to_string),
            message_type: Some("orders.created.v1".into()),
            id: Some("abc".into()),
            source: Some("urn:service:orders".into()),
            time: Some(OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()),
            ..Payload::default()
        }
    }

    #[test]
    fn json_payload_is_embedded_verbatim() {
        let data = br#"{"big":123456789012345678901234567890,"z":1,"a":[1.50]}"#;
        let envelope = CloudEventsJson::default();
        let wire = envelope.wrap(&payload(data, Some("application/json"))).unwrap();
        let body = std::str::from_utf8(&wire.body).unwrap();
        assert!(body.contains(r#""data":{"big":123456789012345678901234567890,"z":1,"a":[1.50]}"#), "{body}");
        assert!(body.starts_with(
            r#"{"specversion":"1.0","id":"abc","source":"urn:service:orders","type":"orders.created.v1""#
        ));
        assert_eq!(wire.attributes.get("content-type"), Some(&AttributeValue::string(CLOUDEVENTS_JSON_CONTENT_TYPE)));
        assert_eq!(wire.attributes.get("fanferro.type"), Some(&AttributeValue::string("orders.created.v1")));

        let decoded = envelope.unwrap(&wire).unwrap().unwrap();
        assert_eq!(&decoded.data[..], &data[..]);
        assert_eq!(decoded.id.as_deref(), Some("abc"));
        assert_eq!(decoded.message_type.as_deref(), Some("orders.created.v1"));
        assert_eq!(decoded.content_type.as_deref(), Some("application/json"));
        assert_eq!(decoded.time, Some(OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()));
    }

    #[test]
    fn text_and_binary_payloads_roundtrip() {
        let envelope = CloudEventsJson::default();
        for (data, ct) in [
            (&b"hello \"world\""[..], Some("text/plain")),
            (&[0u8, 159, 146, 150][..], Some("application/x-protobuf")),
            (&b"not json"[..], None),
            (&b"{broken"[..], Some("application/json")),
        ] {
            let mut p = payload(b"", ct);
            p.data = Bytes::copy_from_slice(data);
            let wire = envelope.wrap(&p).unwrap();
            let decoded = envelope.unwrap(&wire).unwrap().unwrap();
            assert_eq!(&decoded.data[..], data, "content type {ct:?}");
        }
    }

    #[test]
    fn headers_become_extensions() {
        let envelope = CloudEventsJson::default();
        let mut p = payload(b"{}", Some("application/json"));
        p.headers.insert("traceparent".into(), HeaderValue::String("00-abc-def-01".into()));
        p.headers.insert("version".into(), HeaderValue::Integer(2));
        p.headers.insert("replay".into(), HeaderValue::Boolean(true));
        let wire = envelope.wrap(&p).unwrap();
        let decoded = envelope.unwrap(&wire).unwrap().unwrap();
        assert_eq!(decoded.headers, p.headers);
    }

    #[test]
    fn invalid_extension_names_are_rejected() {
        let envelope = CloudEventsJson::default();
        let mut p = payload(b"{}", Some("application/json"));
        p.headers.insert("Bad-Name".into(), HeaderValue::String("x".into()));
        assert!(envelope.wrap(&p).is_err());
        let mut p = payload(b"{}", Some("application/json"));
        p.headers.insert("source".into(), HeaderValue::String("x".into()));
        assert!(envelope.wrap(&p).is_err());
    }

    #[test]
    fn sniffs_events_without_content_type_attribute() {
        let envelope = CloudEventsJson::default();
        let body = br#"{"specversion":"1.0","id":"1","source":"s","type":"t","data":{"x":1}}"#;
        let wire = WireMessage { body: Bytes::from_static(body), attributes: MessageAttributes::new() };
        let decoded = envelope.unwrap(&wire).unwrap().unwrap();
        assert_eq!(&decoded.data[..], br#"{"x":1}"#);
        assert_eq!(decoded.content_type.as_deref(), Some("application/json"));
        // Any key order and whitespace (other CloudEvents producers).
        let body = br#" { "data" : {"x":1}, "type":"t", "id":"1", "source":"s", "specversion" : "1.0" }"#;
        let wire = WireMessage { body: Bytes::from_static(body), attributes: MessageAttributes::new() };
        assert_eq!(envelope.unwrap(&wire).unwrap().unwrap().message_type.as_deref(), Some("t"));
    }

    #[test]
    fn foreign_messages_are_not_claimed() {
        let envelope = CloudEventsJson::default();
        for body in [&b"plain text"[..], br#"{"order":1}"#, b"[1,2]", b"{bad json", br#"{"doc":{"specversion":"1.0"}}"#]
        {
            let wire = WireMessage { body: Bytes::copy_from_slice(body), attributes: MessageAttributes::new() };
            assert!(envelope.unwrap(&wire).unwrap().is_none());
        }
        let mut attributes = MessageAttributes::new();
        attributes.insert("content-type".into(), AttributeValue::string("application/json"));
        let wire = WireMessage { body: Bytes::from_static(br#"{"specversion":"1.0"}"#), attributes };
        assert!(envelope.unwrap(&wire).unwrap().is_none());
    }

    #[test]
    fn raw_messages_that_look_like_events_are_not_claimed() {
        // A raw message whose JSON body is (or looks like) a CloudEvent, such as a relayed event:
        // the raw envelope's id attribute says what it is, CloudEvents never write it.
        let raw = super::super::raw::RawEnvelope::default();
        let body = br#"{"specversion":"1.0","id":"evt-1","source":"upstream","type":"relayed","data":{"x":1}}"#;
        let payload = Payload {
            data: Bytes::from_static(body),
            content_type: Some("application/json".into()),
            message_type: Some("relay".into()),
            id: Some("m-1".into()),
            ..Payload::default()
        };
        let wire = raw.wrap(&payload).unwrap();
        assert!(CloudEventsJson::default().unwrap(&wire).unwrap().is_none());
        let unwrapped = raw.unwrap(&wire).unwrap().unwrap();
        assert_eq!((unwrapped.id.as_deref(), &unwrapped.data[..]), (Some("m-1"), &body[..]));
    }

    #[test]
    fn malformed_declared_events_error() {
        let envelope = CloudEventsJson::default();
        let mut attributes = MessageAttributes::new();
        attributes.insert("content-type".into(), AttributeValue::string(CLOUDEVENTS_JSON_CONTENT_TYPE));
        let wire = WireMessage { body: Bytes::from_static(br#"{"specversion":"1.0","id":"1"}"#), attributes };
        assert!(envelope.unwrap(&wire).is_err());
        let wire = WireMessage {
            body: Bytes::from_static(br#"{"specversion":"2.0","id":"1","source":"s","type":"t"}"#),
            attributes: MessageAttributes::new(),
        };
        assert!(envelope.unwrap(&wire).is_err());
    }

    #[test]
    fn missing_id_and_type_get_defaults() {
        let envelope = CloudEventsJson { type_attribute: None, content_type_attribute: false, ..Default::default() };
        let wire = envelope.wrap(&Payload { data: Bytes::from_static(b"1"), ..Payload::default() }).unwrap();
        assert!(wire.attributes.is_empty());
        let decoded = envelope.unwrap(&wire).unwrap().unwrap();
        assert_eq!(decoded.message_type.as_deref(), Some(DEFAULT_MESSAGE_TYPE));
        assert_eq!(decoded.source.as_deref(), Some(DEFAULT_SOURCE));
        assert_eq!(decoded.id.unwrap().len(), 36);
    }

    #[test]
    fn empty_data_is_omitted() {
        let envelope = CloudEventsJson::default();
        let wire = envelope.wrap(&payload(b"", None)).unwrap();
        let body = std::str::from_utf8(&wire.body).unwrap();
        assert!(!body.contains("\"data"));
        let decoded = envelope.unwrap(&wire).unwrap().unwrap();
        assert!(decoded.data.is_empty());
    }
}
