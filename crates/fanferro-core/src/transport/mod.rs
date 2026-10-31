//! The transport layer: wire messages <-> SQS/SNS request bodies.
//!
//! Outgoing ([`Transport::encode`]):
//! 1. bodies that are not valid SQS text are base64 encoded (`fanferro.encoding=base64`);
//! 2. messages larger than the offload threshold or the destination limit (or all, with
//!    `always`) are stored in S3 and replaced by a pointer ([`claim_check`]);
//! 3. messages still over the destination limit fail fast with [`Error::MessageTooLarge`];
//! 4. attributes are validated (names, values, at most 10; for offloaded messages before the
//!    upload, with one left for the pointer).
//!
//! Incoming ([`Transport::decode`], after [`sns_notification::unwrap`]): pointers are
//! resolved, base64 bodies decoded, and transport attributes removed again.

pub mod claim_check;
pub mod sns_notification;

use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;

use crate::backend::Backend;
use crate::error::{Error, Result};
use crate::types::attributes::{MAX_ATTRIBUTES, attributes_encoded_len, validate_attributes};
use crate::types::text::as_sqs_safe_text;
use crate::types::{AttributeValue, MessageAttributes, WireMessage, reserved};
pub use claim_check::{FetchSettings, OffloadSettings, PayloadPointer, PointerFormat};

/// Maximum SQS message size (body plus attributes).
pub const SQS_MAX_MESSAGE_SIZE: usize = 1_048_576;
/// Default maximum SNS message size (topics may raise it with `MaximumMessageSize`).
pub const SNS_DEFAULT_MAX_MESSAGE_SIZE: usize = 262_144;

/// A message ready to be sent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutgoingBody {
    /// SQS/SNS-safe body text.
    pub body: String,
    /// Attributes including transport attributes.
    pub attributes: MessageAttributes,
    /// Set when the payload was offloaded.
    pub offloaded: Option<PayloadPointer>,
}

impl OutgoingBody {
    /// Encoded size in bytes (body plus attributes) as counted by SQS/SNS.
    pub fn encoded_len(&self) -> usize {
        self.body.len() + attributes_encoded_len(&self.attributes)
    }
}

/// A received message body after transport decoding.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedBody {
    /// The wire message (body bytes and attributes without transport attributes).
    pub wire: WireMessage,
    /// The pointer the body was fetched from, if it was offloaded.
    pub pointer: Option<PayloadPointer>,
}

/// Encodes and decodes message bodies for transport.
#[derive(Clone, Debug)]
pub struct Transport {
    backend: Backend,
    offload: Option<Arc<OffloadSettings>>,
    fetch: Arc<FetchSettings>,
}

impl Transport {
    /// Creates a transport; validates the offload settings.
    pub fn new(backend: Backend, offload: Option<OffloadSettings>, fetch: FetchSettings) -> Result<Self> {
        if let Some(settings) = &offload {
            settings.validate()?;
        }
        Ok(Self { backend, offload: offload.map(Arc::new), fetch: Arc::new(fetch) })
    }

    /// The offload settings, if offloading is enabled.
    pub fn offload_settings(&self) -> Option<&OffloadSettings> {
        self.offload.as_deref()
    }

    /// The fetch settings.
    pub fn fetch_settings(&self) -> &FetchSettings {
        &self.fetch
    }

    /// Returns a copy with different offload settings (per-destination overrides).
    pub fn with_offload(&self, offload: Option<OffloadSettings>) -> Result<Self> {
        Self::new(self.backend.clone(), offload, (*self.fetch).clone())
    }

    /// Encodes `wire` for a destination whose size limit is `limit` bytes.
    pub async fn encode(&self, wire: &WireMessage, message_id: &str, limit: usize) -> Result<OutgoingBody> {
        let mut attributes = wire.attributes.clone();
        for name in [reserved::ENCODING, reserved::CLAIM_CHECK, reserved::EXTENDED_PAYLOAD_SIZE] {
            if attributes.contains_key(name) {
                return Err(Error::validation(format!("attribute {name:?} is reserved for the transport layer")));
            }
        }
        let text = as_sqs_safe_text(&wire.body);
        let inline_body_len = match text {
            Some(text) => text.len(),
            None => base64_len(wire.body.len()),
        };
        if text.is_none() {
            attributes.insert(reserved::ENCODING.into(), AttributeValue::string("base64"));
        }
        let size = inline_body_len + attributes_encoded_len(&attributes);

        // Offloading also takes messages the destination could not take inline, however high the
        // threshold is (an SNS topic's 256 KiB, a queue's lower MaximumMessageSize).
        if let Some(settings) = self.offload.as_deref()
            && (settings.always || size > settings.threshold.min(limit))
        {
            attributes.remove(reserved::ENCODING);
            // Before the upload, so that nothing is stored for a message that cannot be sent; the
            // pointer adds one attribute (counted first, so that the error gives the right limit).
            if attributes.len() >= MAX_ATTRIBUTES {
                return Err(Error::validation(format!(
                    "a message whose payload is offloaded can carry at most {} attributes (the pointer to the payload \
                     needs one), got {} ({})",
                    MAX_ATTRIBUTES - 1,
                    attributes.len(),
                    attributes.keys().cloned().collect::<Vec<_>>().join(", ")
                )));
            }
            validate_attributes(&attributes)?;
            let attributes_len = attributes_encoded_len(&attributes);
            if attributes_len > limit {
                return Err(Error::MessageTooLarge { size: attributes_len, limit });
            }
            let pointer = claim_check::offload(&self.backend, settings, message_id, wire.body.clone()).await?;
            match pointer.format {
                PointerFormat::Fanferro => {
                    attributes.insert(reserved::CLAIM_CHECK.into(), AttributeValue::string("v1"));
                }
                PointerFormat::AwsExtended => {
                    attributes.insert(
                        reserved::EXTENDED_PAYLOAD_SIZE.into(),
                        AttributeValue::number(wire.body.len().to_string()),
                    );
                }
            }
            let outgoing = OutgoingBody { body: pointer.to_body()?, attributes, offloaded: Some(pointer) };
            validate_attributes(&outgoing.attributes)?;
            let size = outgoing.encoded_len();
            if size > limit {
                return Err(Error::MessageTooLarge { size, limit });
            }
            return Ok(outgoing);
        }
        if size > limit {
            return Err(Error::MessageTooLarge { size, limit });
        }
        let body = match text {
            Some(text) => text.to_string(),
            None => BASE64.encode(&wire.body),
        };
        validate_attributes(&attributes)?;
        Ok(OutgoingBody { body, attributes, offloaded: None })
    }

    /// Decodes a received body (already unwrapped from an SNS notification, if any).
    pub async fn decode(&self, body: &str, mut attributes: MessageAttributes) -> Result<DecodedBody> {
        let pointer = if attributes.contains_key(reserved::CLAIM_CHECK) {
            attributes.remove(reserved::CLAIM_CHECK);
            Some(PayloadPointer::parse_fanferro(body)?)
        } else if let Some(size) = attributes
            .get(reserved::EXTENDED_PAYLOAD_SIZE)
            .or_else(|| attributes.get(reserved::LEGACY_EXTENDED_PAYLOAD_SIZE))
        {
            let size = size.as_text().and_then(|text| text.parse::<u64>().ok());
            attributes.remove(reserved::EXTENDED_PAYLOAD_SIZE);
            attributes.remove(reserved::LEGACY_EXTENDED_PAYLOAD_SIZE);
            Some(PayloadPointer::parse_aws(body, size)?)
        } else {
            None
        };
        if let Some(pointer) = pointer {
            let data = claim_check::fetch(&self.backend, &self.fetch, &pointer).await?;
            attributes.remove(reserved::ENCODING);
            return Ok(DecodedBody { wire: WireMessage { body: data, attributes }, pointer: Some(pointer) });
        }
        let encoding = attributes.remove(reserved::ENCODING);
        let body = match encoding.as_ref().and_then(AttributeValue::as_text) {
            Some("base64") => Bytes::from(
                BASE64
                    .decode(body.trim())
                    .map_err(|err| Error::Envelope(format!("invalid base64 message body: {err}")))?,
            ),
            Some(other) => return Err(Error::Envelope(format!("unsupported body encoding {other:?}"))),
            None => Bytes::copy_from_slice(body.as_bytes()),
        };
        Ok(DecodedBody { wire: WireMessage { body, attributes }, pointer: None })
    }

    /// Deletes an offloaded payload object (only possible for S3 pointers).
    pub async fn delete_payload(&self, pointer: &PayloadPointer) -> Result<()> {
        match (&pointer.bucket, &pointer.key) {
            (Some(bucket), Some(key)) => self.backend.objects.delete_object(bucket, key).await,
            _ => Ok(()),
        }
    }
}

fn base64_len(len: usize) -> usize {
    len.div_ceil(3) * 4
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    use crate::backend::memory::MemoryBroker;
    use crate::transport::claim_check::{EncryptionSettings, ReferenceMode};

    fn transport(offload: Option<OffloadSettings>) -> (Transport, Backend) {
        let backend = MemoryBroker::new("us-east-1", "000000000000").backend();
        (Transport::new(backend.clone(), offload, FetchSettings::default()).unwrap(), backend)
    }

    fn offload(threshold: usize, format: PointerFormat) -> OffloadSettings {
        OffloadSettings {
            bucket: "payloads".into(),
            key_prefix: "x/".into(),
            threshold,
            always: false,
            reference: ReferenceMode::S3,
            presign_ttl: 60.0,
            format,
            encryption: EncryptionSettings::BucketDefault,
            storage_class: None,
        }
    }

    #[tokio::test]
    async fn text_bodies_pass_through() {
        let (transport, _) = transport(None);
        let wire = WireMessage::new(Bytes::from_static(b"hello"), MessageAttributes::new());
        let out = transport.encode(&wire, "id", 1000).await.unwrap();
        assert_eq!(out.body, "hello");
        assert!(out.attributes.is_empty());
        let decoded = transport.decode(&out.body, out.attributes).await.unwrap();
        assert_eq!(decoded.wire, wire);
    }

    #[tokio::test]
    async fn binary_bodies_are_base64_encoded() {
        let (transport, _) = transport(None);
        let wire = WireMessage::new(Bytes::from_static(&[0, 1, 2, 255]), MessageAttributes::new());
        let out = transport.encode(&wire, "id", 1000).await.unwrap();
        assert_eq!(out.body, "AAEC/w==");
        assert_eq!(out.attributes.get(reserved::ENCODING), Some(&AttributeValue::string("base64")));
        let decoded = transport.decode(&out.body, out.attributes).await.unwrap();
        assert_eq!(decoded.wire, wire);
    }

    #[tokio::test]
    async fn too_large_without_offloading_fails_fast() {
        let (transport, _) = transport(None);
        let wire = WireMessage::new(Bytes::from(vec![b'a'; 100]), MessageAttributes::new());
        let err = transport.encode(&wire, "id", 50).await.unwrap_err();
        assert!(matches!(err, Error::MessageTooLarge { size: 100, limit: 50 }));
    }

    #[tokio::test]
    async fn large_bodies_are_offloaded_and_resolved() {
        for format in [PointerFormat::Fanferro, PointerFormat::AwsExtended] {
            let (transport, backend) = transport(Some(offload(10, format)));
            backend.objects.create_bucket("payloads").await.unwrap();
            let mut attributes = MessageAttributes::new();
            attributes.insert("tenant".into(), AttributeValue::string("acme"));
            let wire = WireMessage::new(Bytes::from(vec![0u8; 64]), attributes);
            let out = transport.encode(&wire, "msg-1", 1000).await.unwrap();
            assert!(out.offloaded.is_some());
            assert!(!out.attributes.contains_key(reserved::ENCODING));
            let decoded = transport.decode(&out.body, out.attributes).await.unwrap();
            assert_eq!(decoded.wire, wire, "{format:?}");
            let key = decoded.pointer.unwrap().key.unwrap();
            assert_eq!(key, format!("x/msg-1/{}", hex::encode(Sha256::digest(&wire.body))), "{format:?}");
        }
    }

    #[tokio::test]
    async fn bodies_over_the_destination_limit_are_offloaded_below_the_threshold() {
        // A queue with a smaller MaximumMessageSize, or an SNS topic (256 KiB) when the threshold
        // was raised for SQS (1 MiB): offloading is enabled, so the message must not be refused.
        let (transport, backend) = transport(Some(offload(1000, PointerFormat::Fanferro)));
        backend.objects.create_bucket("payloads").await.unwrap();
        let wire = WireMessage::new(Bytes::from(vec![b'a'; 600]), MessageAttributes::new());
        let out = transport.encode(&wire, "over-limit", 500).await.unwrap();
        assert!(out.offloaded.is_some());
        assert!(out.encoded_len() <= 500);
        assert_eq!(transport.decode(&out.body, out.attributes).await.unwrap().wire, wire);
        let inline = transport.encode(&wire, "under-limit", 800).await.unwrap();
        assert!(inline.offloaded.is_none(), "under both the limit and the threshold");
    }

    #[tokio::test]
    async fn a_message_published_again_with_another_body_keeps_the_first_payload() {
        // A publish retried with the same message id but other bytes (CloudEvents stamp the time):
        // replacing the first object broke the copy that points to it (its SHA-256 no longer fit).
        let (transport, backend) = transport(Some(offload(10, PointerFormat::Fanferro)));
        backend.objects.create_bucket("payloads").await.unwrap();
        let first = WireMessage::new(Bytes::from(vec![b'a'; 64]), MessageAttributes::new());
        let second = WireMessage::new(Bytes::from(vec![b'b'; 64]), MessageAttributes::new());
        let first_out = transport.encode(&first, "same-id", 1000).await.unwrap();
        let second_out = transport.encode(&second, "same-id", 1000).await.unwrap();
        assert_eq!(transport.decode(&first_out.body, first_out.attributes).await.unwrap().wire, first);
        assert_eq!(transport.decode(&second_out.body, second_out.attributes).await.unwrap().wire, second);
        // The same bytes again share the object (retries do not pile up objects).
        let again = transport.encode(&first, "same-id", 1000).await.unwrap();
        assert_eq!(again.offloaded.unwrap().key, first_out.offloaded.unwrap().key);
    }

    #[tokio::test]
    async fn attributes_are_checked_before_a_payload_is_uploaded() {
        // No bucket: an upload would fail with NoSuchBucket. A message that cannot be sent must
        // fail before anything is stored (it left an orphaned object).
        let (transport, _) = transport(Some(offload(10, PointerFormat::Fanferro)));
        let body = Bytes::from(vec![b'a'; 64]);
        for count in [10, 11] {
            let attributes: MessageAttributes =
                (0..count).map(|i| (format!("a{i}"), AttributeValue::string("v"))).collect();
            let error = transport.encode(&WireMessage::new(body.clone(), attributes), "id", 100_000).await.unwrap_err();
            assert!(matches!(error, Error::Validation(_)), "{error:?}");
            assert!(error.to_string().contains("at most 9 attributes"), "{count}: {error}");
        }
        let mut reserved_name = MessageAttributes::new();
        reserved_name.insert("AWS.trace".into(), AttributeValue::string("v"));
        let error = transport.encode(&WireMessage::new(body.clone(), reserved_name), "id", 100_000).await.unwrap_err();
        assert!(matches!(error, Error::Validation(_)), "{error:?}");
        let mut huge = MessageAttributes::new();
        huge.insert("note".into(), AttributeValue::string("x".repeat(2000)));
        let error = transport.encode(&WireMessage::new(body, huge), "id", 1000).await.unwrap_err();
        assert!(matches!(error, Error::MessageTooLarge { .. }), "{error:?}");
    }

    #[tokio::test]
    async fn small_bodies_stay_inline_unless_always() {
        let (transport, backend) = transport(Some(offload(1000, PointerFormat::Fanferro)));
        backend.objects.create_bucket("payloads").await.unwrap();
        let wire = WireMessage::new(Bytes::from_static(b"tiny"), MessageAttributes::new());
        assert!(transport.encode(&wire, "a", 5000).await.unwrap().offloaded.is_none());
        let always = transport
            .with_offload(Some(OffloadSettings { always: true, ..offload(1000, PointerFormat::Fanferro) }))
            .unwrap();
        let out = always.encode(&wire, "b", 5000).await.unwrap();
        assert!(out.offloaded.is_some());
        assert_eq!(always.decode(&out.body, out.attributes).await.unwrap().wire, wire);
    }

    #[tokio::test]
    async fn reserved_attributes_are_rejected() {
        let (transport, _) = transport(None);
        let mut attributes = MessageAttributes::new();
        attributes.insert(reserved::ENCODING.into(), AttributeValue::string("base64"));
        let wire = WireMessage::new(Bytes::from_static(b"x"), attributes);
        assert!(transport.encode(&wire, "id", 100).await.is_err());
    }

    #[tokio::test]
    async fn invalid_encodings_are_reported() {
        let (transport, _) = transport(None);
        let mut attributes = MessageAttributes::new();
        attributes.insert(reserved::ENCODING.into(), AttributeValue::string("rot13"));
        assert!(transport.decode("x", attributes).await.is_err());
        let mut attributes = MessageAttributes::new();
        attributes.insert(reserved::ENCODING.into(), AttributeValue::string("base64"));
        assert!(transport.decode("!!!", attributes).await.is_err());
    }
}
