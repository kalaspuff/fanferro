//! A protobuf envelope with a metadata message, for tomodachi services that use one.
//!
//! tomodachi lets services bring envelopes of their own. A common layout is a protobuf message
//! holding the data and a metadata message that says how the data is encoded, base64-encoded as
//! the body:
//!
//! ```proto
//! message Metadata {
//!     string uuid = 1;            // the message id
//!     string envelope = 2;        // names the envelope and its version: "<name>--1.0.0"
//!     string topic = 3;
//!     string origin = 4;          // the producing service
//!     string timestamp = 5;       // "2026-11-01T12:00:00.123456Z"
//!     string encoding = 6;        // proto, json, string or raw; "gzip_" in front when compressed
//!     string external_path = 7;
//!     string correlation_id = 8;
//!     string data_definition = 9; // protobuf data: its .proto file
//!     string data_class = 10;     // protobuf data: its message name
//! }
//!
//! message Message {
//!     bytes data = 1;
//!     Metadata metadata = 2;
//! }
//! ```
//!
//! Nothing but `metadata.envelope` tells such a message apart, so the envelope's name is a
//! setting: [`MetadataProtobuf`] reads messages whose `metadata.envelope` begins with it and
//! writes it with its version.

use std::borrow::Cow;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use prost::Message as _;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;

use super::{
    BINARY_CONTENT_TYPE, IncomingQueueMessage, PROTOBUF_CONTENT_TYPE, TOMODACHI_COMPRESS_THRESHOLD,
    TomodachiDestination, body_for, metadata_topic, unsupported_encoding, zlib_compress, zlib_decompress,
};
use crate::envelope::{
    Envelope, default_type_attribute, header_attributes, insert_envelope_attributes, is_json_content_type,
    media_type_essence, text_attribute,
};
use crate::error::{Error, Result};
use crate::types::{HeaderValue, Headers, MessageAttributes, Payload, WireMessage, new_message_id};

/// The header holding `metadata.correlation_id`.
pub const CORRELATION_ID_HEADER: &str = "correlationid";

/// The header holding `metadata.data_class` (the protobuf message name of the data).
pub const DATA_CLASS_HEADER: &str = "dataclass";

/// Content type of `json` data.
const JSON_CONTENT_TYPE: &str = "application/json";

/// Content type of `string` data (fanferro's text codec writes and reads it).
const TEXT_CONTENT_TYPE: &str = "text/plain; charset=utf-8";

/// Media types of serialized protobuf messages.
const PROTOBUF_MEDIA_TYPES: [&str; 3] =
    ["application/x-protobuf", "application/protobuf", "application/vnd.google.protobuf"];

/// Namespace of correlation ids derived from message ids (`generate_missing_correlation_id`).
const CORRELATION_ID_NAMESPACE: uuid::Uuid = uuid::Uuid::from_u128(0x6f1d_3c52_8a0e_4b7d_9c41_2e5f_a8b3_d017);

/// The body's protobuf message.
#[derive(Clone, PartialEq, prost::Message)]
struct MessageProto {
    #[prost(bytes = "bytes", tag = "1")]
    data: Bytes,
    #[prost(message, optional, tag = "2")]
    metadata: Option<MetadataProto>,
}

/// The metadata message.
#[derive(Clone, PartialEq, prost::Message)]
struct MetadataProto {
    #[prost(string, tag = "1")]
    uuid: String,
    #[prost(string, tag = "2")]
    envelope: String,
    #[prost(string, tag = "3")]
    topic: String,
    #[prost(string, tag = "4")]
    origin: String,
    #[prost(string, tag = "5")]
    timestamp: String,
    #[prost(string, tag = "6")]
    encoding: String,
    #[prost(string, tag = "7")]
    external_path: String,
    #[prost(string, tag = "8")]
    correlation_id: String,
    #[prost(string, tag = "9")]
    data_definition: String,
    #[prost(string, tag = "10")]
    data_class: String,
}

/// A protobuf envelope of data and a metadata message, named by `metadata.envelope` (see the
/// module documentation).
///
/// The data's encoding follows the payload's content type: `proto` (protobuf), `json`, `string`
/// (text) or `raw` (binary), compressed with zlib above the threshold (`gzip_` in front).
/// `metadata.correlation_id` is the `correlationid` header, `metadata.data_class` the `dataclass`
/// header and `metadata.data_definition` the payload's data schema; other headers travel as
/// message attributes.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MetadataProtobuf {
    /// The envelope's name: incoming messages are recognised by a `metadata.envelope` that begins
    /// with it.
    pub envelope: String,
    /// Written after the name and `--` in `metadata.envelope` (`None`: the name alone).
    pub version: Option<String>,
    /// `metadata.origin` of published messages (`None`: the payload's source).
    pub service_name: Option<String>,
    /// Promote the message type to this message attribute (for SNS filter policies).
    pub type_attribute: Option<String>,
    /// Compress data of more than this many bytes (`None`: never compress).
    pub compress_threshold: Option<usize>,
    /// Give incoming messages without a `metadata.correlation_id` one derived from their uuid.
    pub generate_missing_correlation_id: bool,
}

impl MetadataProtobuf {
    /// An envelope named `envelope`, with the default settings.
    pub fn new(envelope: impl Into<String>) -> Self {
        Self {
            envelope: envelope.into(),
            version: None,
            service_name: None,
            type_attribute: default_type_attribute(),
            compress_threshold: Some(TOMODACHI_COMPRESS_THRESHOLD),
            generate_missing_correlation_id: true,
        }
    }

    /// Checks the settings: the name must be given, and the version must not be empty.
    pub fn validate(&self) -> Result<()> {
        if self.envelope.trim().is_empty() {
            return Err(Error::Config(
                "the metadata protobuf envelope needs the name its messages carry in metadata.envelope".into(),
            ));
        }
        if self.version.as_deref().is_some_and(|version| version.trim().is_empty()) {
            return Err(Error::Config(
                "the version of a metadata protobuf envelope must not be empty (None writes the name alone)".into(),
            ));
        }
        Ok(())
    }

    /// `metadata.envelope` of published messages: the name, and `--` and the version if set.
    pub fn marker(&self) -> Cow<'_, str> {
        match &self.version {
            Some(version) => Cow::Owned(format!("{}--{version}", self.envelope)),
            None => Cow::Borrowed(&self.envelope),
        }
    }

    /// Wraps `payload` for `destination` (writing the topic, or tomodachi's queue message document).
    pub fn wrap_for(&self, payload: &Payload, destination: TomodachiDestination<'_>) -> Result<WireMessage> {
        let mut encoding = Cow::Borrowed(data_encoding(payload.content_type.as_deref())?);
        let data = match self.compress_threshold {
            Some(threshold) if payload.data.len() > threshold => {
                encoding = Cow::Owned(format!("gzip_{encoding}"));
                Bytes::from(zlib_compress(&payload.data))
            }
            _ => payload.data.clone(),
        };
        let uuid = match payload.id.as_deref() {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => new_message_id(),
        };
        let correlation_id = match payload.headers.get(CORRELATION_ID_HEADER).map(HeaderValue::to_text) {
            Some(id) if !id.is_empty() => id,
            _ => uuid::Uuid::new_v4().to_string(),
        };
        let time = payload.time.unwrap_or_else(OffsetDateTime::now_utc).to_offset(time::UtcOffset::UTC);
        let timestamp = time
            .format(format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:6]Z"))
            .map_err(|err| Error::Envelope(format!("cannot format the message timestamp: {err}")))?;
        let message = MessageProto {
            data,
            metadata: Some(MetadataProto {
                uuid,
                envelope: self.marker().into_owned(),
                topic: metadata_topic(destination).into_owned(),
                origin: self.service_name.as_deref().or(payload.source.as_deref()).unwrap_or_default().to_string(),
                timestamp,
                encoding: encoding.into_owned(),
                external_path: String::new(),
                correlation_id,
                data_definition: payload.data_schema.clone().unwrap_or_default(),
                data_class: payload.headers.get(DATA_CLASS_HEADER).map(HeaderValue::to_text).unwrap_or_default(),
            }),
        };
        let envelope = BASE64.encode(message.encode_to_vec());
        Ok(WireMessage { body: body_for(envelope, destination)?, attributes: self.attributes(payload)? })
    }

    /// The attributes of an outgoing message: headers the metadata has no field for, and the
    /// promoted message type.
    fn attributes(&self, payload: &Payload) -> Result<MessageAttributes> {
        let mut attributes = if payload.headers.keys().any(|name| is_metadata_header(name)) {
            let headers: Headers = payload
                .headers
                .iter()
                .filter(|(name, _)| !is_metadata_header(name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            header_attributes(&headers)?
        } else {
            header_attributes(&payload.headers)?
        };
        insert_envelope_attributes(
            &mut attributes,
            None,
            self.type_attribute.as_deref(),
            payload.message_type.as_deref(),
        );
        Ok(attributes)
    }

    fn parse(&self, text: &[u8], wire: &WireMessage) -> Result<Option<Payload>> {
        let text = text.trim_ascii();
        // Padded base64 starting with the key of field 1 or 2 (`C`, `E`), checked before decoding.
        if text.is_empty() || !text.len().is_multiple_of(4) || !matches!(text[0], b'C' | b'E') {
            return Ok(None);
        }
        let Ok(decoded) = BASE64.decode(text) else {
            return Ok(None);
        };
        let Ok(message) = MessageProto::decode(Bytes::from(decoded)) else {
            return Ok(None);
        };
        let Some(metadata) = message.metadata.filter(|m| m.envelope.starts_with(self.envelope.as_str())) else {
            return Ok(None);
        };
        let format = "metadata protobuf";
        let mut encoding = metadata.encoding.as_str();
        let mut data = message.data;
        if encoding.starts_with("oob_") {
            return Err(Error::Envelope(format!(
                "the data of {format} message {:?} is stored out of band ({encoding}), which fanferro does not read",
                metadata.uuid
            )));
        }
        if let Some(rest) = encoding.strip_prefix("gzip_") {
            data = zlib_decompress(&data, format)?;
            encoding = rest;
        }
        let content_type = match encoding {
            "proto" => PROTOBUF_CONTENT_TYPE,
            "json" => JSON_CONTENT_TYPE,
            "string" => TEXT_CONTENT_TYPE,
            "raw" => BINARY_CONTENT_TYPE,
            _ => return Err(unsupported_encoding(format, Some(metadata.encoding.as_str()).filter(|e| !e.is_empty()))),
        };
        let correlation_id = if !metadata.correlation_id.is_empty() {
            Some(metadata.correlation_id)
        } else if self.generate_missing_correlation_id {
            Some(derived_correlation_id(&metadata.uuid))
        } else {
            None
        };
        let mut headers = Headers::new();
        if let Some(correlation_id) = correlation_id {
            headers.insert(CORRELATION_ID_HEADER.to_string(), HeaderValue::String(correlation_id));
        }
        if !metadata.data_class.is_empty() {
            headers.insert(DATA_CLASS_HEADER.to_string(), HeaderValue::String(metadata.data_class));
        }
        let message_type = self
            .type_attribute
            .as_deref()
            .and_then(|name| text_attribute(&wire.attributes, name))
            .filter(|t| !t.is_empty())
            .map(str::to_string)
            .or_else(|| Some(metadata.topic).filter(|t| !t.is_empty()));
        Ok(Some(Payload {
            data,
            content_type: Some(content_type.to_string()),
            message_type,
            id: Some(metadata.uuid).filter(|id| !id.is_empty()),
            time: OffsetDateTime::parse(&metadata.timestamp, &Rfc3339).ok(),
            source: Some(metadata.origin).filter(|s| !s.is_empty()),
            subject: None,
            data_schema: Some(metadata.data_definition).filter(|d| !d.is_empty()),
            headers,
        }))
    }
}

/// Whether the metadata has a field for header `name` (it is not written as an attribute).
fn is_metadata_header(name: &str) -> bool {
    name == CORRELATION_ID_HEADER || name == DATA_CLASS_HEADER
}

/// The `encoding` of data of `content_type`.
fn data_encoding(content_type: Option<&str>) -> Result<&'static str> {
    let Some(content_type) = content_type else {
        return Ok("raw");
    };
    let essence = media_type_essence(content_type);
    if PROTOBUF_MEDIA_TYPES.contains(&essence.as_str()) {
        Ok("proto")
    } else if is_json_content_type(content_type) {
        Ok("json")
    } else if essence.starts_with("text/") {
        Ok("string")
    } else if essence == BINARY_CONTENT_TYPE {
        Ok("raw")
    } else {
        Err(Error::Envelope(format!(
            "the metadata protobuf envelope carries protobuf messages, JSON, text and binary data, not {content_type}: \
             encode the message with a protobuf, JSON, text or bytes codec"
        )))
    }
}

/// The correlation id of a message without one: derived from its uuid, so that every delivery of
/// the message (and every copy SNS fans out) gets the same id. Random when the uuid is missing too.
fn derived_correlation_id(uuid: &str) -> String {
    if uuid.is_empty() {
        return uuid::Uuid::new_v4().to_string();
    }
    uuid::Uuid::new_v5(&CORRELATION_ID_NAMESPACE, uuid.as_bytes()).to_string()
}

impl Envelope for MetadataProtobuf {
    fn name(&self) -> &str {
        "metadata-protobuf"
    }

    /// Wraps `payload` as published to a topic, with an empty `metadata.topic`; destinations
    /// use [`MetadataProtobuf::wrap_for`].
    fn wrap(&self, payload: &Payload) -> Result<WireMessage> {
        self.wrap_for(payload, TomodachiDestination::Topic(""))
    }

    fn unwrap(&self, wire: &WireMessage) -> Result<Option<Payload>> {
        let trimmed = wire.body.trim_ascii_start();
        if trimmed.starts_with(b"{") {
            // tomodachi's queue message document around the base64 envelope.
            if memchr::memmem::find(trimmed, b"\"Type\"").is_none() {
                return Ok(None);
            }
            let Ok(document) = serde_json::from_slice::<IncomingQueueMessage<'_>>(&wire.body) else {
                return Ok(None);
            };
            return match document.into_message() {
                Some(inner) => self.parse(inner.as_bytes(), wire),
                None => Ok(None),
            };
        }
        self.parse(&wire.body, wire)
    }
}

#[cfg(test)]
mod tests;
