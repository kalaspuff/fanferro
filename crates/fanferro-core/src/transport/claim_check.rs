//! Claim checks: storing large payloads in S3 and sending a pointer instead.
//!
//! Two pointer formats are supported:
//!
//! * `fanferro` (default): body `{"fanferro":"claim-check/v1","bucket":..,"key":..,
//!   "url":..,"size":..,"sha256":..}` plus the `fanferro.claim-check` attribute. Can carry a
//!   presigned URL so receivers need no S3 permissions.
//! * `aws_extended`: the format of the AWS SQS/SNS extended client libraries (Java,
//!   Python): body `["software.amazon.payloadoffloading.PayloadS3Pointer",
//!   {"s3BucketName":..,"s3Key":..}]` plus the `ExtendedPayloadSize` attribute. Use it to
//!   interoperate with services built on those libraries. Receivers accept both formats.
//!
//! Fetching by URL is restricted to an allow-list of hosts (AWS endpoints and the
//! configured custom endpoint by default) to prevent server-side request forgery through
//! crafted messages, and downloads are capped at [`FetchSettings::max_bytes`].

use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::backend::{Backend, BackendKind, ObjectEncryption, PutOptions};
use crate::error::{Error, Result};
use crate::util::duration_from_secs;

/// Pointer format marker (fanferro format).
pub const FANFERRO_POINTER_MARKER: &str = "claim-check/v1";
/// Class name used by the AWS extended client libraries.
pub const AWS_POINTER_CLASS: &str = "software.amazon.payloadoffloading.PayloadS3Pointer";

/// What the pointer contains.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceMode {
    /// Bucket and key (receivers need `s3:GetObject`).
    #[default]
    S3,
    /// A presigned HTTPS URL only (receivers need no AWS permissions).
    Url,
    /// Both; receivers use their preferred method and fall back to the other.
    Both,
}

/// Pointer serialization format.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PointerFormat {
    /// fanferro's JSON pointer.
    #[default]
    Fanferro,
    /// AWS extended client library pointer (S3 only).
    AwsExtended,
}

/// Server-side encryption settings for stored payloads.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum EncryptionSettings {
    /// Use the bucket's default encryption.
    #[default]
    BucketDefault,
    /// SSE-S3.
    S3,
    /// SSE-KMS.
    Kms {
        /// Key id/ARN/alias (AWS managed `aws/s3` key when omitted).
        #[serde(default)]
        key_id: Option<String>,
        /// Enable S3 bucket keys.
        #[serde(default)]
        bucket_key: bool,
    },
}

fn default_prefix() -> String {
    "fanferro/".into()
}

fn default_threshold() -> usize {
    256 * 1024
}

fn default_presign_ttl() -> f64 {
    24.0 * 3600.0
}

/// When and how outgoing payloads are offloaded.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OffloadSettings {
    /// Bucket for payload objects.
    pub bucket: String,
    /// Key prefix; the object key is `{key_prefix}{message_id}`.
    #[serde(default = "default_prefix")]
    pub key_prefix: String,
    /// Offload when the encoded message (body + attributes) exceeds this many bytes.
    #[serde(default = "default_threshold")]
    pub threshold: usize,
    /// Offload every message regardless of size.
    #[serde(default)]
    pub always: bool,
    /// What the pointer contains.
    #[serde(default)]
    pub reference: ReferenceMode,
    /// Lifetime of presigned URLs in seconds (24 hours by default, at most 7 days). URLs signed
    /// with temporary credentials stop working when those expire.
    #[serde(default = "default_presign_ttl")]
    pub presign_ttl: f64,
    /// Pointer format.
    #[serde(default)]
    pub format: PointerFormat,
    /// Server-side encryption.
    #[serde(default)]
    pub encryption: EncryptionSettings,
    /// S3 storage class.
    #[serde(default)]
    pub storage_class: Option<String>,
}

impl OffloadSettings {
    /// Validates the settings.
    pub fn validate(&self) -> Result<()> {
        if self.bucket.is_empty() {
            return Err(Error::config("large payload offloading requires a bucket"));
        }
        if self.threshold == 0 {
            return Err(Error::config("large payload threshold must be positive"));
        }
        if self.format == PointerFormat::AwsExtended && self.reference != ReferenceMode::S3 {
            return Err(Error::config("the aws_extended pointer format only supports reference='s3'"));
        }
        let ttl = duration_from_secs(self.presign_ttl, "presign_ttl")?;
        if ttl.is_zero() || ttl > Duration::from_secs(7 * 24 * 3600) {
            return Err(Error::config("presign_ttl must be between 1 second and 7 days"));
        }
        Ok(())
    }

    fn put_options(&self) -> PutOptions {
        PutOptions {
            content_type: Some("application/octet-stream".into()),
            encryption: match &self.encryption {
                EncryptionSettings::BucketDefault => ObjectEncryption::BucketDefault,
                EncryptionSettings::S3 => ObjectEncryption::S3Managed,
                EncryptionSettings::Kms { key_id, bucket_key } => {
                    ObjectEncryption::Kms { key_id: key_id.clone(), bucket_key: *bucket_key }
                }
            },
            storage_class: self.storage_class.clone(),
            metadata: Default::default(),
        }
    }
}

/// Which retrieval method receivers try first.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FetchPreference {
    /// `GetObject` first, presigned URL as fallback.
    #[default]
    S3,
    /// Presigned URL first, `GetObject` as fallback.
    Url,
}

/// How incoming pointers are resolved.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FetchSettings {
    /// Maximum payload size to download.
    pub max_bytes: usize,
    /// Hosts presigned URLs may point to (`*.example.com` wildcards allowed). Empty means
    /// AWS hosts plus the configured custom endpoint.
    pub allowed_url_hosts: Vec<String>,
    /// Allow plain `http://` URLs (always allowed for the custom endpoint host).
    pub allow_http: bool,
    /// Preferred retrieval method.
    pub prefer: FetchPreference,
    /// Verify size and SHA-256 when the pointer records them.
    pub verify_checksum: bool,
}

impl Default for FetchSettings {
    fn default() -> Self {
        Self {
            max_bytes: 64 * 1024 * 1024,
            allowed_url_hosts: Vec::new(),
            allow_http: false,
            prefer: FetchPreference::S3,
            verify_checksum: true,
        }
    }
}

/// A reference to an offloaded payload.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PayloadPointer {
    /// Bucket, when the pointer includes S3 coordinates.
    pub bucket: Option<String>,
    /// Key, when the pointer includes S3 coordinates.
    pub key: Option<String>,
    /// Presigned URL, when included.
    pub url: Option<String>,
    /// Payload size in bytes.
    pub size: Option<u64>,
    /// Hex SHA-256 of the payload.
    pub sha256: Option<String>,
    /// The format the pointer was read from / written in.
    pub format: PointerFormat,
}

#[derive(Serialize, Deserialize)]
struct FanferroPointerJson {
    fanferro: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sha256: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AwsPointerJson {
    s3_bucket_name: String,
    s3_key: String,
}

impl PayloadPointer {
    /// Serializes the pointer as a message body.
    pub fn to_body(&self) -> Result<String> {
        match self.format {
            PointerFormat::Fanferro => serde_json::to_string(&FanferroPointerJson {
                fanferro: FANFERRO_POINTER_MARKER.into(),
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                url: self.url.clone(),
                size: self.size,
                sha256: self.sha256.clone(),
            }),
            PointerFormat::AwsExtended => {
                let (Some(bucket), Some(key)) = (&self.bucket, &self.key) else {
                    return Err(Error::config("aws_extended pointers require a bucket and key"));
                };
                serde_json::to_string(&(
                    AWS_POINTER_CLASS,
                    AwsPointerJson { s3_bucket_name: bucket.clone(), s3_key: key.clone() },
                ))
            }
        }
        .map_err(|err| Error::Payload(format!("cannot serialize payload pointer: {err}")))
    }

    /// Parses a fanferro-format pointer body.
    pub fn parse_fanferro(body: &str) -> Result<Self> {
        let json: FanferroPointerJson =
            serde_json::from_str(body).map_err(|err| Error::Payload(format!("invalid claim check pointer: {err}")))?;
        if json.fanferro != FANFERRO_POINTER_MARKER {
            return Err(Error::Payload(format!("unsupported claim check pointer version {:?}", json.fanferro)));
        }
        let pointer = Self {
            bucket: json.bucket,
            key: json.key,
            url: json.url,
            size: json.size,
            sha256: json.sha256,
            format: PointerFormat::Fanferro,
        };
        if pointer.url.is_none() && (pointer.bucket.is_none() || pointer.key.is_none()) {
            return Err(Error::Payload("claim check pointer has neither bucket/key nor url".into()));
        }
        Ok(pointer)
    }

    /// Parses an AWS extended client pointer body.
    pub fn parse_aws(body: &str, size: Option<u64>) -> Result<Self> {
        let (class, json): (String, AwsPointerJson) = serde_json::from_str(body)
            .map_err(|err| Error::Payload(format!("invalid extended client payload pointer: {err}")))?;
        if class != AWS_POINTER_CLASS {
            return Err(Error::Payload(format!("unsupported extended client pointer class {class:?}")));
        }
        Ok(Self {
            bucket: Some(json.s3_bucket_name),
            key: Some(json.s3_key),
            url: None,
            size,
            sha256: None,
            format: PointerFormat::AwsExtended,
        })
    }
}

/// Stores `data` according to `settings` and returns the pointer.
pub async fn offload(
    backend: &Backend,
    settings: &OffloadSettings,
    message_id: &str,
    data: Bytes,
) -> Result<PayloadPointer> {
    let key = format!("{}{}", settings.key_prefix, message_id);
    let sha256 = hex::encode(Sha256::digest(&data));
    let size = data.len() as u64;
    backend.objects.put_object(&settings.bucket, &key, data, &settings.put_options()).await?;
    let url = match settings.reference {
        ReferenceMode::S3 => None,
        ReferenceMode::Url | ReferenceMode::Both => Some(
            backend
                .objects
                .presign_get(&settings.bucket, &key, duration_from_secs(settings.presign_ttl, "presign_ttl")?)
                .await?,
        ),
    };
    let (bucket, key) = match settings.reference {
        ReferenceMode::Url => (None, None),
        ReferenceMode::S3 | ReferenceMode::Both => (Some(settings.bucket.clone()), Some(key)),
    };
    let sha256 = (settings.format == PointerFormat::Fanferro).then_some(sha256);
    Ok(PayloadPointer { bucket, key, url, size: Some(size), sha256, format: settings.format })
}

/// Downloads the payload a pointer refers to, verifying size and checksum.
pub async fn fetch(backend: &Backend, settings: &FetchSettings, pointer: &PayloadPointer) -> Result<Bytes> {
    let has_s3 = pointer.bucket.is_some() && pointer.key.is_some();
    let order = match settings.prefer {
        FetchPreference::S3 => [FetchPreference::S3, FetchPreference::Url],
        FetchPreference::Url => [FetchPreference::Url, FetchPreference::S3],
    };
    let mut last_error = None;
    for method in order {
        let result = match method {
            FetchPreference::S3 if has_s3 => {
                let (Some(bucket), Some(key)) = (&pointer.bucket, &pointer.key) else { continue };
                backend.objects.get_object(bucket, key, settings.max_bytes).await
            }
            FetchPreference::Url => match &pointer.url {
                Some(url) => match check_url_allowed(url, settings, backend) {
                    Ok(()) => backend.http.get(url, settings.max_bytes).await,
                    Err(err) => Err(err),
                },
                None => continue,
            },
            FetchPreference::S3 => continue,
        };
        match result {
            Ok(data) => return verify(pointer, data, settings.verify_checksum),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error.unwrap_or_else(|| Error::Payload("claim check pointer has neither bucket/key nor url".into())))
}

/// The error for a presigned payload URL that expired before the message was consumed.
pub(crate) fn expired_url_error(expired_at: OffsetDateTime) -> Error {
    let expired_at = expired_at.format(&Rfc3339).unwrap_or_else(|_| expired_at.to_string());
    Error::Payload(format!(
        "payload URL expired at {expired_at}: the message waited longer than the presign_ttl it was published \
         with (raise it, or publish with reference=\"both\" so that consumers that can read the bucket fetch it \
         from there)"
    ))
}

fn verify(pointer: &PayloadPointer, data: Bytes, verify_checksum: bool) -> Result<Bytes> {
    if let Some(size) = pointer.size
        && size != data.len() as u64
    {
        return Err(Error::PayloadIntegrity(format!("expected {size} bytes, fetched {}", data.len())));
    }
    if verify_checksum && let Some(expected) = &pointer.sha256 {
        let actual = hex::encode(Sha256::digest(&data));
        if !actual.eq_ignore_ascii_case(expected) {
            return Err(Error::PayloadIntegrity("SHA-256 of the fetched payload does not match the pointer".into()));
        }
    }
    Ok(data)
}

fn host_matches(pattern: &str, host: &str) -> bool {
    let pattern = pattern.trim().to_ascii_lowercase();
    match pattern.strip_prefix("*.") {
        Some(suffix) => host.ends_with(&format!(".{suffix}")),
        None => host == pattern,
    }
}

/// Rejects URLs that do not point to an allowed host (SSRF protection).
pub fn check_url_allowed(url: &str, settings: &FetchSettings, backend: &Backend) -> Result<()> {
    if url.starts_with("memory://") {
        return if backend.kind == BackendKind::Memory {
            Ok(())
        } else {
            Err(Error::Payload("memory:// payload URLs can only be resolved by the in-memory backend".into()))
        };
    }
    let parsed = reqwest::Url::parse(url).map_err(|_| Error::Payload("invalid payload URL".into()))?;
    let host = parsed
        .host_str()
        .map(str::to_ascii_lowercase)
        .ok_or_else(|| Error::Payload("payload URL has no host".into()))?;
    let endpoint_host = backend
        .endpoint_url
        .as_deref()
        .and_then(|endpoint| reqwest::Url::parse(endpoint).ok())
        .and_then(|endpoint| endpoint.host_str().map(str::to_ascii_lowercase));
    let is_endpoint_host = endpoint_host.as_deref() == Some(host.as_str());
    match parsed.scheme() {
        "https" => {}
        "http" if settings.allow_http || is_endpoint_host => {}
        scheme => {
            return Err(Error::Payload(format!(
                "refusing to fetch a payload over {scheme}:// (only https is allowed)"
            )));
        }
    }
    let allowed = is_endpoint_host
        || if settings.allowed_url_hosts.is_empty() {
            host.ends_with(".amazonaws.com") || host.ends_with(".amazonaws.com.cn")
        } else {
            settings.allowed_url_hosts.iter().any(|pattern| host_matches(pattern, &host))
        };
    if !allowed {
        return Err(Error::Payload(format!(
            "refusing to fetch a payload from host {host:?}: not in the allowed host list (configure allowed_url_hosts)"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::memory::MemoryBroker;

    fn settings(bucket: &str) -> OffloadSettings {
        OffloadSettings {
            bucket: bucket.into(),
            key_prefix: "p/".into(),
            threshold: 10,
            always: false,
            reference: ReferenceMode::Both,
            presign_ttl: 60.0,
            format: PointerFormat::Fanferro,
            encryption: EncryptionSettings::BucketDefault,
            storage_class: None,
        }
    }

    #[test]
    fn pointer_formats_roundtrip() {
        let pointer = PayloadPointer {
            bucket: Some("b".into()),
            key: Some("k".into()),
            url: Some("https://b.s3.amazonaws.com/k".into()),
            size: Some(3),
            sha256: Some("ab".into()),
            format: PointerFormat::Fanferro,
        };
        let body = pointer.to_body().unwrap();
        assert!(body.starts_with(r#"{"fanferro":"claim-check/v1""#));
        assert_eq!(PayloadPointer::parse_fanferro(&body).unwrap(), pointer);

        let aws = PayloadPointer { url: None, sha256: None, format: PointerFormat::AwsExtended, ..pointer };
        let body = aws.to_body().unwrap();
        assert_eq!(body, r#"["software.amazon.payloadoffloading.PayloadS3Pointer",{"s3BucketName":"b","s3Key":"k"}]"#);
        assert_eq!(PayloadPointer::parse_aws(&body, Some(3)).unwrap(), aws);
        assert!(PayloadPointer::parse_fanferro(r#"{"fanferro":"claim-check/v9","url":"x"}"#).is_err());
        assert!(PayloadPointer::parse_fanferro(r#"{"fanferro":"claim-check/v1"}"#).is_err());
    }

    #[test]
    fn settings_validation() {
        assert!(settings("b").validate().is_ok());
        assert!(OffloadSettings { bucket: String::new(), ..settings("b") }.validate().is_err());
        assert!(OffloadSettings { format: PointerFormat::AwsExtended, ..settings("b") }.validate().is_err());
        assert!(OffloadSettings { presign_ttl: 8.0 * 24.0 * 3600.0, ..settings("b") }.validate().is_err());
    }

    #[tokio::test]
    async fn offload_and_fetch_with_integrity_checks() {
        let broker = MemoryBroker::new("us-east-1", "000000000000");
        let backend = broker.backend();
        backend.objects.create_bucket("b").await.unwrap();
        let pointer = offload(&backend, &settings("b"), "m1", Bytes::from_static(b"payload")).await.unwrap();
        assert_eq!(pointer.key.as_deref(), Some("p/m1"));
        assert!(pointer.url.as_deref().unwrap().starts_with("memory://"));
        let fetched = fetch(&backend, &FetchSettings::default(), &pointer).await.unwrap();
        assert_eq!(&fetched[..], b"payload");
        let by_url = FetchSettings { prefer: FetchPreference::Url, ..FetchSettings::default() };
        assert_eq!(&fetch(&backend, &by_url, &pointer).await.unwrap()[..], b"payload");

        let tampered = PayloadPointer { sha256: Some("00".into()), ..pointer.clone() };
        assert!(matches!(fetch(&backend, &FetchSettings::default(), &tampered).await, Err(Error::PayloadIntegrity(_))));
        let wrong_size = PayloadPointer { size: Some(1), ..pointer };
        assert!(matches!(
            fetch(&backend, &FetchSettings::default(), &wrong_size).await,
            Err(Error::PayloadIntegrity(_))
        ));
    }

    #[test]
    fn url_allow_list() {
        let broker = MemoryBroker::new("us-east-1", "000000000000");
        let mut backend = broker.backend();
        backend.kind = BackendKind::Aws;
        backend.endpoint_url = Some("http://localhost:4566".into());
        let defaults = FetchSettings::default();
        assert!(
            check_url_allowed("https://bucket.s3.eu-west-1.amazonaws.com/k?X-Amz-Signature=1", &defaults, &backend)
                .is_ok()
        );
        assert!(check_url_allowed("http://localhost:4566/bucket/k", &defaults, &backend).is_ok());
        assert!(check_url_allowed("http://169.254.169.254/latest/meta-data", &defaults, &backend).is_err());
        assert!(check_url_allowed("https://evil.example.com/x", &defaults, &backend).is_err());
        assert!(check_url_allowed("http://bucket.s3.amazonaws.com/k", &defaults, &backend).is_err());
        assert!(check_url_allowed("file:///etc/passwd", &defaults, &backend).is_err());
        assert!(check_url_allowed("memory://s3/b/k", &defaults, &backend).is_err());
        let custom = FetchSettings { allowed_url_hosts: vec!["*.example.com".into()], ..FetchSettings::default() };
        assert!(check_url_allowed("https://cdn.example.com/x", &custom, &backend).is_ok());
        assert!(check_url_allowed("https://bucket.s3.amazonaws.com/k", &custom, &backend).is_err());
    }
}
