//! The envelopes of [tomodachi](https://github.com/kalaspuff/tomodachi) services.
//!
//! tomodachi's `JsonBase` and `ProtobufBase` envelopes (`tomodachi.envelope`) lay a message out
//! as a document with the producing service, metadata and the data:
//!
//! ```json
//! {"service": {"name": "orders", "uuid": "<service uuid>"},
//!  "metadata": {"message_uuid": "<service uuid>.<uuid4>", "protocol_version": "tomodachi-json-base--1.0.0",
//!               "compatible_protocol_versions": ["json_base-wip"], "timestamp": 1730000000.123,
//!               "topic": "order-created", "data_encoding": "raw"},
//!  "data": {...}}
//! ```
//!
//! The protobuf envelope is the `SNSSQSMessage` message of tomodachi's `sns_sqs_message.proto`
//! (same fields; `data` holds the serialized protobuf message), base64-encoded as the body.
//! Data of 60,000 bytes or more is zlib-compressed by tomodachi (`base64_gzip_json`,
//! `gzip_proto`); both envelopes read and write that.
//!
//! tomodachi publishes to SNS and reads the body of SNS notifications (subscriptions without raw
//! message delivery). When it sends to an SQS queue it wraps the envelope in a document shaped
//! like such a notification, `{"Type": "Message", "Message": "<envelope>", ...}`, which these
//! envelopes unwrap, and write for queues ([`TomodachiDestination::Queue`]).
//!
//! The tomodachi topic is the fanferro message type: incoming messages take their type from
//! `metadata.topic` (unless they carry the type attribute), and a message published to a topic
//! names that topic, decoded from tomodachi's topic name encoding, in `metadata.topic`.

use std::borrow::Cow;
use std::sync::OnceLock;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use prost::Message as _;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use time::OffsetDateTime;
use time::macros::format_description;

use super::{
    Envelope, default_type_attribute, header_attributes, insert_envelope_attributes, is_json_content_type,
    is_text_content_type, share, text_attribute,
};
use crate::error::{Error, Result};
use crate::types::{Headers, MessageAttributes, Payload, WireMessage, new_message_id};

/// `metadata.protocol_version` of tomodachi's JSON envelope (`tomodachi.envelope.JsonBase`).
pub const TOMODACHI_JSON_PROTOCOL_VERSION: &str = "tomodachi-json-base--1.0.0";

/// `metadata.protocol_version` of tomodachi's protobuf envelope (`tomodachi.envelope.ProtobufBase`).
pub const TOMODACHI_PROTOBUF_PROTOCOL_VERSION: &str = "tomodachi-protobuf-base--1.0.0";

/// Size from which tomodachi compresses the data of a message, in bytes.
pub const TOMODACHI_COMPRESS_THRESHOLD: usize = 60_000;

/// Content type of the data of protobuf envelopes (fanferro's protobuf codec reads it).
const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";

/// Content type of protobuf envelope data that tomodachi marks as not being protobuf (`raw`).
const BINARY_CONTENT_TYPE: &str = "application/octet-stream";

/// Largest data a compressed message may expand to (guards against decompression bombs).
const MAX_DECOMPRESSED_SIZE: usize = 64 * 1024 * 1024;

/// zlib level of `zlib.compress()` in Python, which tomodachi uses.
const COMPRESSION_LEVEL: u8 = 6;

/// Where a message wrapped by a tomodachi envelope is sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TomodachiDestination<'a> {
    /// An SNS topic, by its name without the client's topic prefix (`metadata.topic` names it).
    Topic(&'a str),
    /// An SQS queue: the envelope is wrapped in tomodachi's SNS-like queue message document.
    Queue,
}

/// tomodachi's JSON envelope (`tomodachi.envelope.JsonBase`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TomodachiJson {
    /// `service.name` of published messages (`None`: the payload's source).
    pub service_name: Option<String>,
    /// `service.uuid` of published messages and the prefix of their `message_uuid` (`None`: a
    /// UUID generated once per process, like a tomodachi service's).
    pub service_uuid: Option<String>,
    /// Promote the message type to this message attribute (for SNS filter policies).
    pub type_attribute: Option<String>,
    /// Compress data of this many bytes or more like tomodachi (`None`: never compress).
    pub compress_threshold: Option<usize>,
}

impl Default for TomodachiJson {
    fn default() -> Self {
        Self {
            service_name: None,
            service_uuid: None,
            type_attribute: default_type_attribute(),
            compress_threshold: Some(TOMODACHI_COMPRESS_THRESHOLD),
        }
    }
}

/// tomodachi's protobuf envelope (`tomodachi.envelope.ProtobufBase`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TomodachiProtobuf {
    /// `service.name` of published messages (`None`: the payload's source).
    pub service_name: Option<String>,
    /// `service.uuid` of published messages and the prefix of their `message_uuid` (`None`: a
    /// UUID generated once per process, like a tomodachi service's).
    pub service_uuid: Option<String>,
    /// Promote the message type to this message attribute (for SNS filter policies).
    pub type_attribute: Option<String>,
    /// Compress data of more than this many bytes like tomodachi (`None`: never compress).
    pub compress_threshold: Option<usize>,
}

impl Default for TomodachiProtobuf {
    fn default() -> Self {
        Self {
            service_name: None,
            service_uuid: None,
            type_attribute: default_type_attribute(),
            compress_threshold: Some(TOMODACHI_COMPRESS_THRESHOLD),
        }
    }
}

/// The service uuid of envelopes without one: generated once per process.
fn process_service_uuid() -> &'static str {
    static SERVICE_UUID: OnceLock<String> = OnceLock::new();
    SERVICE_UUID.get_or_init(|| uuid::Uuid::new_v4().to_string())
}

/// Decodes tomodachi's encoding of topic names (`order___2e_created` -> `order.created`).
///
/// tomodachi replaces each character SNS does not allow in topic names by `___`, the hex digits
/// of its UTF-8 bytes and `_`. Sequences that do not decode stay as they are.
pub fn decode_tomodachi_topic(name: &str) -> Cow<'_, str> {
    if !name.contains("___") {
        return Cow::Borrowed(name);
    }
    let mut decoded = String::with_capacity(name.len());
    let mut rest = name;
    while let Some(start) = rest.find("___") {
        decoded.push_str(&rest[..start]);
        let after = &rest[start + 3..];
        // Like tomodachi's regular expression, the shortest run of 2, 4, 6 or 8 lowercase hex
        // digits that is followed by `_` and decodes.
        let replaced = [2, 4, 6, 8].into_iter().find_map(|len| {
            let digits = after.get(..len)?;
            if after.as_bytes().get(len) != Some(&b'_')
                || !digits.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
            {
                return None;
            }
            let bytes = hex::decode(digits).ok()?;
            Some((String::from_utf8(bytes).ok()?, len))
        });
        match replaced {
            Some((text, len)) => {
                decoded.push_str(&text);
                rest = &after[len + 1..];
            }
            None => {
                // Like the regular expression's scan: try again one character further.
                decoded.push('_');
                rest = &rest[start + 1..];
            }
        }
    }
    decoded.push_str(rest);
    Cow::Owned(decoded)
}

/// `metadata.topic` of a message sent to `destination`.
fn metadata_topic(destination: TomodachiDestination<'_>) -> Cow<'_, str> {
    match destination {
        // tomodachi adds the suffix of FIFO topics itself: its `topic` is the name without it.
        TomodachiDestination::Topic(name) => decode_tomodachi_topic(name.strip_suffix(".fifo").unwrap_or(name)),
        TomodachiDestination::Queue => Cow::Borrowed(""),
    }
}

/// The producer fields of an outgoing message.
struct Origin<'a> {
    service_name: &'a str,
    service_uuid: &'a str,
    message_uuid: String,
    timestamp: f64,
}

impl<'a> Origin<'a> {
    fn new(service_name: &'a Option<String>, service_uuid: &'a Option<String>, payload: &'a Payload) -> Self {
        let service_name = service_name.as_deref().or(payload.source.as_deref()).unwrap_or_default();
        let service_uuid = service_uuid.as_deref().unwrap_or_else(|| process_service_uuid());
        let message_uuid = match payload.id.as_deref() {
            Some(id) if !id.is_empty() => format!("{service_uuid}.{id}"),
            _ => format!("{service_uuid}.{}", new_message_id()),
        };
        let time = payload.time.unwrap_or_else(OffsetDateTime::now_utc);
        // Seconds as a float, like Python's time.time().
        #[allow(clippy::cast_precision_loss)]
        let timestamp = time.unix_timestamp_nanos() as f64 / 1e9;
        Self { service_name, service_uuid, message_uuid, timestamp }
    }
}

/// The attributes of an outgoing message: its headers and the promoted message type.
fn outgoing_attributes(payload: &Payload, type_attribute: Option<&str>) -> Result<MessageAttributes> {
    let mut attributes = header_attributes(&payload.headers)?;
    insert_envelope_attributes(&mut attributes, None, type_attribute, payload.message_type.as_deref());
    Ok(attributes)
}

/// tomodachi's document for messages sent straight to an SQS queue, shaped like an SNS
/// notification so that its consumer reads both the same way.
#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct OutgoingQueueMessage<'a> {
    #[serde(rename = "Type")]
    kind: &'static str,
    version: &'static str,
    message: &'a str,
    timestamp: String,
    /// tomodachi copies the attributes here; it reads those of the SQS message when it has any,
    /// which it does whenever this envelope has written any.
    message_attributes: serde_json::Map<String, serde_json::Value>,
}

/// The body for `destination`: the envelope itself, or the queue message document holding it.
fn body_for(envelope: String, destination: TomodachiDestination<'_>) -> Result<Bytes> {
    match destination {
        TomodachiDestination::Topic(_) => Ok(Bytes::from(envelope)),
        TomodachiDestination::Queue => {
            let timestamp = OffsetDateTime::now_utc()
                .format(format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"))
                .map_err(|err| Error::Envelope(format!("cannot format the message timestamp: {err}")))?;
            let document = OutgoingQueueMessage {
                kind: "Message",
                version: "0",
                message: &envelope,
                timestamp,
                message_attributes: serde_json::Map::new(),
            };
            serde_json::to_vec(&document)
                .map(Bytes::from)
                .map_err(|err| Error::Envelope(format!("cannot serialize the tomodachi queue message: {err}")))
        }
    }
}

/// `{"Type": "Message", "Message": ...}`: the fields of tomodachi's queue message document.
#[derive(Deserialize)]
struct IncomingQueueMessage<'a> {
    #[serde(rename = "Type", borrow, default)]
    kind: Option<Cow<'a, str>>,
    #[serde(rename = "Message", borrow, default)]
    message: Option<Cow<'a, str>>,
}

impl<'a> IncomingQueueMessage<'a> {
    fn into_message(self) -> Option<Cow<'a, str>> {
        if self.kind.as_deref() == Some("Message") { self.message } else { None }
    }
}

fn zlib_compress(data: &[u8]) -> Vec<u8> {
    miniz_oxide::deflate::compress_to_vec_zlib(data, COMPRESSION_LEVEL)
}

fn zlib_decompress(data: &[u8], format: &str) -> Result<Bytes> {
    miniz_oxide::inflate::decompress_to_vec_zlib_with_limit(data, MAX_DECOMPRESSED_SIZE).map(Bytes::from).map_err(
        |err| match err.status {
            miniz_oxide::inflate::TINFLStatus::HasMoreOutput => Error::Envelope(format!(
                "compressed data of a {format} message expands to more than {MAX_DECOMPRESSED_SIZE} bytes"
            )),
            status => Error::Envelope(format!("invalid compressed data in a {format} message ({status:?})")),
        },
    )
}

/// The fields of an incoming message besides its data.
struct Incoming<'a> {
    service_name: Option<&'a str>,
    service_uuid: Option<&'a str>,
    message_uuid: Option<&'a str>,
    timestamp: Option<f64>,
    topic: Option<&'a str>,
}

impl Incoming<'_> {
    fn into_payload(
        self,
        data: Bytes,
        content_type: &str,
        type_attribute: Option<&str>,
        attributes: &MessageAttributes,
    ) -> Payload {
        let message_type = type_attribute
            .and_then(|name| text_attribute(attributes, name))
            .or(self.topic)
            .filter(|t| !t.is_empty())
            .map(str::to_string);
        // The message uuid without its service uuid prefix: the id fanferro published it with.
        let id = self.message_uuid.filter(|id| !id.is_empty()).map(|message_uuid| {
            message_uuid
                .strip_prefix(self.service_uuid.unwrap_or_default())
                .and_then(|rest| rest.strip_prefix('.'))
                .filter(|rest| !rest.is_empty())
                .unwrap_or(message_uuid)
                .to_string()
        });
        let time = self
            .timestamp
            .filter(|t| t.is_finite() && *t > 0.0)
            .and_then(|t| OffsetDateTime::from_unix_timestamp_nanos((t * 1e9).round() as i128).ok());
        Payload {
            data,
            content_type: Some(content_type.to_string()),
            message_type,
            id,
            time,
            source: self.service_name.filter(|s| !s.is_empty()).map(str::to_string),
            subject: None,
            data_schema: None,
            headers: Headers::new(),
        }
    }
}

fn unsupported_encoding(format: &str, encoding: Option<&str>) -> Error {
    match encoding {
        Some(encoding) => Error::Envelope(format!("unsupported data_encoding {encoding:?} in a {format} message")),
        None => Error::Envelope(format!("{format} message without a data_encoding")),
    }
}

// --- JSON ------------------------------------------------------------------------------------

#[derive(Serialize)]
struct OutgoingJson<'a> {
    service: OutgoingService<'a>,
    metadata: OutgoingJsonMetadata<'a>,
    data: OutgoingJsonData<'a>,
}

#[derive(Serialize)]
struct OutgoingService<'a> {
    name: &'a str,
    uuid: &'a str,
}

#[derive(Serialize)]
struct OutgoingJsonMetadata<'a> {
    message_uuid: &'a str,
    protocol_version: &'static str,
    compatible_protocol_versions: [&'static str; 1],
    timestamp: f64,
    topic: &'a str,
    data_encoding: &'static str,
}

#[derive(Serialize)]
#[serde(untagged)]
enum OutgoingJsonData<'a> {
    Raw(&'a RawValue),
    Text(String),
}

/// A JSON body that may be tomodachi's JSON envelope or its queue message document.
#[derive(Deserialize)]
struct IncomingJson<'a> {
    #[serde(borrow, default)]
    service: Option<IncomingService<'a>>,
    #[serde(borrow, default)]
    metadata: Option<IncomingJsonMetadata<'a>>,
    #[serde(borrow, default)]
    data: Option<&'a RawValue>,
    #[serde(rename = "Type", borrow, default)]
    kind: Option<Cow<'a, str>>,
    #[serde(rename = "Message", borrow, default)]
    message: Option<Cow<'a, str>>,
}

#[derive(Deserialize, Default)]
struct IncomingService<'a> {
    #[serde(borrow, default)]
    name: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    uuid: Option<Cow<'a, str>>,
}

#[derive(Deserialize)]
struct IncomingJsonMetadata<'a> {
    #[serde(borrow, default)]
    message_uuid: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    protocol_version: Option<Cow<'a, str>>,
    #[serde(default)]
    timestamp: Option<f64>,
    #[serde(borrow, default)]
    topic: Option<Cow<'a, str>>,
    #[serde(borrow, default)]
    data_encoding: Option<Cow<'a, str>>,
}

/// Whether `protocol_version` is one of tomodachi's JSON envelope.
fn is_json_protocol(protocol_version: &str) -> bool {
    protocol_version.starts_with("tomodachi-json-base") || protocol_version.starts_with("json_base")
}

impl TomodachiJson {
    /// Wraps `payload` for `destination` (writing the topic, or tomodachi's queue message document).
    pub fn wrap_for(&self, payload: &Payload, destination: TomodachiDestination<'_>) -> Result<WireMessage> {
        let content_type = payload.content_type.as_deref();
        let text = std::str::from_utf8(&payload.data).ok();
        let json = match (content_type, text) {
            (None, Some(text)) => serde_json::from_str::<&RawValue>(text).ok(),
            (Some(ct), Some(text)) if is_json_content_type(ct) => Some(
                serde_json::from_str::<&RawValue>(text)
                    .map_err(|err| Error::Envelope(format!("the payload is not valid JSON ({ct}): {err}")))?,
            ),
            _ => None,
        };
        let data = match (json, content_type, text) {
            (Some(raw), _, _) => OutgoingJsonData::Raw(raw),
            (None, None, Some(text)) => OutgoingJsonData::Text(text.to_string()),
            (None, Some(ct), Some(text)) if is_text_content_type(ct) => OutgoingJsonData::Text(text.to_string()),
            _ => {
                return Err(Error::Envelope(format!(
                    "tomodachi's JSON envelope carries JSON and text, not {}: publish JSON data or use the \
                     tomodachi protobuf envelope for protobuf messages",
                    content_type.unwrap_or("binary data")
                )));
            }
        };
        let (data, data_encoding) = match self.compress_threshold {
            Some(threshold) => {
                let encoded = match &data {
                    OutgoingJsonData::Raw(raw) => Cow::Borrowed(raw.get()),
                    OutgoingJsonData::Text(text) => Cow::Owned(
                        serde_json::to_string(text)
                            .map_err(|err| Error::Envelope(format!("cannot serialize the payload: {err}")))?,
                    ),
                };
                if encoded.len() >= threshold {
                    let compressed = BASE64.encode(zlib_compress(encoded.as_bytes()));
                    (OutgoingJsonData::Text(compressed), "base64_gzip_json")
                } else {
                    (data, "raw")
                }
            }
            None => (data, "raw"),
        };
        let origin = Origin::new(&self.service_name, &self.service_uuid, payload);
        let topic = metadata_topic(destination);
        let message = OutgoingJson {
            service: OutgoingService { name: origin.service_name, uuid: origin.service_uuid },
            metadata: OutgoingJsonMetadata {
                message_uuid: &origin.message_uuid,
                protocol_version: TOMODACHI_JSON_PROTOCOL_VERSION,
                compatible_protocol_versions: ["json_base-wip"],
                timestamp: origin.timestamp,
                topic: &topic,
                data_encoding,
            },
            data,
        };
        let envelope = serde_json::to_string(&message)
            .map_err(|err| Error::Envelope(format!("cannot serialize the tomodachi JSON message: {err}")))?;
        Ok(WireMessage {
            body: body_for(envelope, destination)?,
            attributes: outgoing_attributes(payload, self.type_attribute.as_deref())?,
        })
    }

    /// Parses a tomodachi JSON envelope found in `text`, a slice of `body` or a copy of part of it.
    fn parse(&self, body: &Bytes, text: &str, wire: &WireMessage, nested: bool) -> Result<Option<Payload>> {
        let Ok(document) = serde_json::from_str::<IncomingJson<'_>>(text) else {
            return Ok(None);
        };
        let Some(metadata) = document.metadata.filter(|m| m.protocol_version.as_deref().is_some_and(is_json_protocol))
        else {
            let queue_message = IncomingQueueMessage { kind: document.kind, message: document.message };
            return match queue_message.into_message() {
                Some(inner) if !nested => match inner {
                    Cow::Borrowed(inner) => self.parse(body, inner, wire, true),
                    Cow::Owned(inner) => self.parse(&Bytes::new(), &inner, wire, true),
                },
                _ => Ok(None),
            };
        };
        let raw = document.data.map_or("null", RawValue::get);
        let data = match metadata.data_encoding.as_deref() {
            Some("raw") => share(body, raw),
            Some("base64_gzip_json") => {
                let encoded: Cow<'_, str> = serde_json::from_str(raw).map_err(|_| {
                    Error::Envelope("the data of a base64_gzip_json tomodachi message is not a string".into())
                })?;
                let compressed = BASE64.decode(encoded.trim()).map_err(|err| {
                    Error::Envelope(format!("invalid base64 data in a base64_gzip_json tomodachi message: {err}"))
                })?;
                zlib_decompress(&compressed, "tomodachi JSON")?
            }
            other => return Err(unsupported_encoding("tomodachi JSON", other)),
        };
        let service = document.service.unwrap_or_default();
        let incoming = Incoming {
            service_name: service.name.as_deref(),
            service_uuid: service.uuid.as_deref(),
            message_uuid: metadata.message_uuid.as_deref(),
            timestamp: metadata.timestamp,
            topic: metadata.topic.as_deref(),
        };
        Ok(Some(incoming.into_payload(data, "application/json", self.type_attribute.as_deref(), &wire.attributes)))
    }
}

impl Envelope for TomodachiJson {
    fn name(&self) -> &str {
        "tomodachi-json"
    }

    /// Wraps `payload` as published to a topic, with an empty `metadata.topic`; destinations
    /// use [`TomodachiJson::wrap_for`].
    fn wrap(&self, payload: &Payload) -> Result<WireMessage> {
        self.wrap_for(payload, TomodachiDestination::Topic(""))
    }

    fn unwrap(&self, wire: &WireMessage) -> Result<Option<Payload>> {
        // Only JSON objects naming the envelope's metadata (or tomodachi's queue message
        // document) are parsed: other bodies are left to the next envelope untouched.
        let trimmed = wire.body.trim_ascii_start();
        if !trimmed.starts_with(b"{")
            || (memchr::memmem::find(trimmed, b"\"metadata\"").is_none()
                && memchr::memmem::find(trimmed, b"\"Type\"").is_none())
        {
            return Ok(None);
        }
        let Ok(text) = std::str::from_utf8(&wire.body) else {
            return Ok(None);
        };
        self.parse(&wire.body, text, wire, false)
    }
}

// --- protobuf ----------------------------------------------------------------------------------

/// `proto.SNSSQSMessage` of tomodachi's `sns_sqs_message.proto`.
#[derive(Clone, PartialEq, prost::Message)]
struct SnsSqsMessage {
    #[prost(message, optional, tag = "1")]
    service: Option<ServiceProto>,
    #[prost(message, optional, tag = "2")]
    metadata: Option<MetadataProto>,
    #[prost(bytes = "bytes", tag = "3")]
    data: Bytes,
}

/// `proto.Service`.
#[derive(Clone, PartialEq, prost::Message)]
struct ServiceProto {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    uuid: String,
}

/// `proto.Metadata`.
#[derive(Clone, PartialEq, prost::Message)]
struct MetadataProto {
    #[prost(string, tag = "1")]
    message_uuid: String,
    #[prost(string, tag = "2")]
    protocol_version: String,
    #[prost(double, tag = "3")]
    timestamp: f64,
    #[prost(string, tag = "4")]
    topic: String,
    #[prost(string, tag = "5")]
    data_encoding: String,
}

/// Whether `protocol_version` is one of tomodachi's protobuf envelope.
fn is_protobuf_protocol(protocol_version: &str) -> bool {
    protocol_version.starts_with("tomodachi-protobuf-base") || protocol_version.starts_with("protobuf_base")
}

/// Whether `text` may be a base64-encoded `SNSSQSMessage`: padded base64 starting with the key of
/// its first field (`C`, `E` or `G` encode the keys of fields 1-3), checked before decoding.
fn may_be_protobuf_message(text: &[u8]) -> bool {
    !text.is_empty() && text.len().is_multiple_of(4) && matches!(text[0], b'C' | b'E' | b'G')
}

impl TomodachiProtobuf {
    /// Wraps `payload` for `destination` (writing the topic, or tomodachi's queue message document).
    pub fn wrap_for(&self, payload: &Payload, destination: TomodachiDestination<'_>) -> Result<WireMessage> {
        if let Some(ct) = payload.content_type.as_deref()
            && is_text_content_type(ct)
        {
            return Err(Error::Envelope(format!(
                "tomodachi's protobuf envelope carries protobuf messages, not {ct}: publish a protobuf message \
                 (with the protobuf codec's binary format) or use the tomodachi JSON envelope"
            )));
        }
        let (data, data_encoding) = match self.compress_threshold {
            Some(threshold) if payload.data.len() > threshold => {
                (Bytes::from(zlib_compress(&payload.data)), "gzip_proto")
            }
            _ => (payload.data.clone(), "proto"),
        };
        let origin = Origin::new(&self.service_name, &self.service_uuid, payload);
        let message = SnsSqsMessage {
            service: Some(ServiceProto {
                name: origin.service_name.to_string(),
                uuid: origin.service_uuid.to_string(),
            }),
            metadata: Some(MetadataProto {
                message_uuid: origin.message_uuid,
                protocol_version: TOMODACHI_PROTOBUF_PROTOCOL_VERSION.to_string(),
                timestamp: origin.timestamp,
                topic: metadata_topic(destination).into_owned(),
                data_encoding: data_encoding.to_string(),
            }),
            data,
        };
        let envelope = BASE64.encode(message.encode_to_vec());
        Ok(WireMessage {
            body: body_for(envelope, destination)?,
            attributes: outgoing_attributes(payload, self.type_attribute.as_deref())?,
        })
    }

    fn parse(&self, text: &[u8], wire: &WireMessage) -> Result<Option<Payload>> {
        let text = text.trim_ascii();
        if !may_be_protobuf_message(text) {
            return Ok(None);
        }
        let Ok(decoded) = BASE64.decode(text) else {
            return Ok(None);
        };
        let Ok(message) = SnsSqsMessage::decode(Bytes::from(decoded)) else {
            return Ok(None);
        };
        let Some(metadata) = message.metadata.filter(|m| is_protobuf_protocol(&m.protocol_version)) else {
            return Ok(None);
        };
        let format = "tomodachi protobuf";
        let base64 = |data: &[u8]| {
            BASE64
                .decode(data.trim_ascii())
                .map_err(|err| Error::Envelope(format!("invalid base64 data in a {format} message: {err}")))
        };
        let (data, content_type) = match metadata.data_encoding.as_str() {
            "proto" => (message.data, PROTOBUF_CONTENT_TYPE),
            "gzip_proto" => (zlib_decompress(&message.data, format)?, PROTOBUF_CONTENT_TYPE),
            // Written by old tomodachi versions.
            "base64" => (Bytes::from(base64(&message.data)?), PROTOBUF_CONTENT_TYPE),
            "base64_gzip_proto" => (zlib_decompress(&base64(&message.data)?, format)?, PROTOBUF_CONTENT_TYPE),
            "raw" => (message.data, BINARY_CONTENT_TYPE),
            other => return Err(unsupported_encoding(format, Some(other).filter(|e| !e.is_empty()))),
        };
        let service = message.service.unwrap_or_default();
        let incoming = Incoming {
            service_name: Some(&service.name),
            service_uuid: Some(&service.uuid),
            message_uuid: Some(&metadata.message_uuid),
            timestamp: Some(metadata.timestamp),
            topic: Some(&metadata.topic),
        };
        Ok(Some(incoming.into_payload(data, content_type, self.type_attribute.as_deref(), &wire.attributes)))
    }
}

impl Envelope for TomodachiProtobuf {
    fn name(&self) -> &str {
        "tomodachi-protobuf"
    }

    /// Wraps `payload` as published to a topic, with an empty `metadata.topic`; destinations
    /// use [`TomodachiProtobuf::wrap_for`].
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
