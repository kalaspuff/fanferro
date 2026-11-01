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
//!   interoperate with services built on those libraries. Receivers accept both formats, and
//!   the legacy variant of the extended clients (`SQSLargePayloadSize` and a
//!   `com.amazon.sqs.javamessaging.MessageS3Pointer` body) too.
//!
//! Fetching by URL is restricted to an allow-list of hosts (S3 endpoints and the configured
//! custom endpoint by default) and to pointers that record the payload's SHA-256, to prevent
//! server-side request forgery through crafted messages; reading by bucket and key can be
//! restricted to [`FetchSettings::allowed_buckets`]. Downloads are capped at
//! [`FetchSettings::max_bytes`].

use std::time::Duration;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::backend::{Backend, BackendKind, ObjectEncryption, PutOptions};
use crate::error::{Error, Result};
use crate::types::arn;
use crate::util::duration_from_secs;

/// Pointer format marker (fanferro format).
pub const FANFERRO_POINTER_MARKER: &str = "claim-check/v1";
/// Class name used by the AWS extended client libraries.
pub const AWS_POINTER_CLASS: &str = "software.amazon.payloadoffloading.PayloadS3Pointer";
/// Class name of the pointers that the extended clients write with the legacy
/// `SQSLargePayloadSize` attribute (the Java library before 2.0, the Python ones with
/// `use_legacy_attribute=True`). Read only.
pub const LEGACY_AWS_POINTER_CLASS: &str = "com.amazon.sqs.javamessaging.MessageS3Pointer";

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
    /// Hosts presigned URLs may point to (`*.example.com` wildcards allowed, any port). Empty
    /// means the endpoints of S3 (see [`is_s3_endpoint`]). URLs on the configured custom endpoint
    /// (same scheme, host and port) are allowed either way.
    pub allowed_url_hosts: Vec<String>,
    /// Allow plain `http://` URLs (always allowed for an `http://` custom endpoint).
    pub allow_http: bool,
    /// Buckets that S3 pointers may name (exact names), to read their payloads and to delete them
    /// ([`Transport::delete_payload`](super::Transport::delete_payload)). Empty means any bucket,
    /// as far as the credentials allow.
    pub allowed_buckets: Vec<String>,
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
            allowed_buckets: Vec::new(),
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
        if class != AWS_POINTER_CLASS && class != LEGACY_AWS_POINTER_CLASS {
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
    let sha256 = sha256_hex(&data).await?;
    // The body's hash in the key: a message published again with the same id but other bytes
    // (CloudEvents stamp the time) must not replace the object an earlier copy points to, while
    // retries of the same bytes keep writing the same object.
    let key = format!("{}{}/{}", settings.key_prefix, message_id, sha256);
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
    let mut missing_object = None;
    for method in order {
        let result = match method {
            FetchPreference::S3 if has_s3 => {
                let (Some(bucket), Some(key)) = (&pointer.bucket, &pointer.key) else { continue };
                match check_bucket_allowed(bucket, settings, "read") {
                    Ok(()) => backend.objects.get_object(bucket, key, settings.max_bytes).await,
                    Err(err) => Err(err),
                }
            }
            FetchPreference::Url => match &pointer.url {
                Some(url) => {
                    match check_url_allowed(url, settings, backend).and_then(|()| check_url_pointer(pointer)) {
                        Ok(()) => backend.http.get(url, settings.max_bytes).await,
                        Err(err) => Err(err),
                    }
                }
                None => continue,
            },
            FetchPreference::S3 => continue,
        };
        match result {
            Ok(data) => return verify(pointer, data, settings.verify_checksum).await,
            Err(err) if method == FetchPreference::S3 && is_missing_object(&err) => missing_object = Some(err),
            Err(err) => last_error = Some(err),
        }
    }
    // A pointer's URL refers to the same object: when S3 says it no longer exists, that is the
    // error to report (consumers act on it), not the URL's answer.
    Err(missing_object
        .or(last_error)
        .unwrap_or_else(|| Error::Payload("claim check pointer has neither bucket/key nor url".into())))
}

/// Whether S3 answered that the payload object does not exist (deleted, or expired by a lifecycle
/// rule).
fn is_missing_object(error: &Error) -> bool {
    error.aws().is_some_and(|aws| aws.code.as_deref() == Some("NoSuchKey"))
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

/// Payloads at least this large are hashed on Tokio's blocking thread pool: hashing 1 MiB takes
/// about 0.5-2.5 ms, which would hold up the heartbeats, settlements and other tasks of the 2-4
/// worker threads.
pub const BLOCKING_HASH_THRESHOLD: usize = 1024 * 1024;

/// The hex SHA-256 of `data`, computed on the blocking thread pool for payloads of at least
/// [`BLOCKING_HASH_THRESHOLD`] bytes.
pub async fn sha256_hex(data: &Bytes) -> Result<String> {
    if data.len() < BLOCKING_HASH_THRESHOLD {
        return Ok(hex::encode(Sha256::digest(data)));
    }
    let data = data.clone();
    tokio::task::spawn_blocking(move || hex::encode(Sha256::digest(&data)))
        .await
        .map_err(|err| Error::Payload(format!("hashing the payload failed: {err}")))
}

async fn verify(pointer: &PayloadPointer, data: Bytes, verify_checksum: bool) -> Result<Bytes> {
    if let Some(size) = pointer.size
        && size != data.len() as u64
    {
        return Err(Error::PayloadIntegrity(format!("expected {size} bytes, fetched {}", data.len())));
    }
    if verify_checksum && let Some(expected) = &pointer.sha256 {
        let actual = sha256_hex(&data).await?;
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

/// Rejects S3 pointers to a bucket outside [`FetchSettings::allowed_buckets`], when it is set;
/// `operation` (`read`, `delete`) is what the error says was refused.
pub fn check_bucket_allowed(bucket: &str, settings: &FetchSettings, operation: &str) -> Result<()> {
    if settings.allowed_buckets.is_empty() || settings.allowed_buckets.iter().any(|allowed| allowed == bucket) {
        Ok(())
    } else {
        Err(Error::Payload(format!(
            "refusing to {operation} a payload from bucket {bucket:?}: not in the allowed bucket list (configure \
             allowed_buckets)"
        )))
    }
}

/// Rejects fetching by URL a pointer that does not record the payload's SHA-256.
///
/// fanferro writes one in every pointer with a URL (pointers of the extended clients carry no URL), so
/// only a crafted pointer lacks it: without it, whatever the URL's host answers would become the
/// message body.
fn check_url_pointer(pointer: &PayloadPointer) -> Result<()> {
    match &pointer.sha256 {
        Some(_) => Ok(()),
        None => Err(Error::Payload(
            "refusing to fetch a payload by URL for a pointer without a sha256 (fanferro records one in every \
             pointer)"
                .into(),
        )),
    }
}

/// Rejects URLs that do not point to an allowed host (SSRF protection).
///
/// Allowed are URLs on the custom endpoint (same scheme, host and port), and those on a host of
/// [`FetchSettings::allowed_url_hosts`] or, when that is empty, on an S3 endpoint
/// ([`is_s3_endpoint`]) of the commercial and China partitions and of the client's own. Other
/// hosts under AWS's domains, such as load balancers and EC2 instances, often resolve to private
/// addresses.
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
    let on_endpoint = backend
        .endpoint_url
        .as_deref()
        .and_then(|endpoint| reqwest::Url::parse(endpoint).ok())
        .is_some_and(|endpoint| endpoint.origin() == parsed.origin());
    match parsed.scheme() {
        "https" => {}
        "http" if settings.allow_http || on_endpoint => {}
        "http" => {
            let port = parsed.port().map(|port| format!(":{port}")).unwrap_or_default();
            return Err(Error::Payload(format!(
                "refusing to fetch a payload over http:// from {host}{port}: only https is allowed, and http on \
                 the endpoint_url's own host and port (configure allow_http)"
            )));
        }
        scheme => {
            return Err(Error::Payload(format!(
                "refusing to fetch a payload over {scheme}:// (only https is allowed)"
            )));
        }
    }
    if on_endpoint {
        return Ok(());
    }
    if settings.allowed_url_hosts.is_empty() {
        let partition_suffix = arn::dns_suffix(arn::partition_for_region(&backend.region));
        if !is_s3_endpoint(&host, partition_suffix) {
            return Err(Error::Payload(format!(
                "refusing to fetch a payload from host {host:?}: not an S3 endpoint{} (configure \
                 allowed_url_hosts)",
                if backend.endpoint_url.is_some() { " nor the endpoint_url" } else { "" }
            )));
        }
    } else if !settings.allowed_url_hosts.iter().any(|pattern| host_matches(pattern, &host)) {
        return Err(Error::Payload(format!(
            "refusing to fetch a payload from host {host:?}: not in the allowed host list (configure \
             allowed_url_hosts)"
        )));
    }
    Ok(())
}

/// Whether `host` (lowercase) is the name of an S3 endpoint under `amazonaws.com`,
/// `amazonaws.com.cn` or `partition_suffix` (the DNS suffix of the client's partition).
///
/// Path-style and virtual-hosted-style names are accepted (a bucket's or access point's labels
/// before S3's own) in their regional (`s3.<region>`), legacy (`s3-<region>`, `s3-external-1`,
/// `s3.amazonaws.com`), dual-stack, FIPS, Transfer Acceleration, access point, Multi-Region Access
/// Point and S3 Express (directory bucket) forms, each with the S3 labels right before the region
/// or the suffix: names that merely start with `s3` (a load balancer can be called so) are not. S3
/// website, Object Lambda, Outposts and VPC endpoint (`vpce`) names are not accepted either.
pub fn is_s3_endpoint(host: &str, partition_suffix: &str) -> bool {
    ["amazonaws.com", "amazonaws.com.cn", partition_suffix].into_iter().any(|suffix| {
        host.strip_suffix(suffix).and_then(|rest| rest.strip_suffix('.')).is_some_and(|rest| {
            let labels: Vec<&str> = rest.split('.').collect();
            !labels.contains(&"") && is_s3_labels(&labels)
        })
    })
}

/// The labels of an S3 endpoint name before the DNS suffix.
fn is_s3_labels(labels: &[&str]) -> bool {
    match labels {
        [before @ .., region] if is_region(region) => {
            matches!(
                before,
                [.., "s3" | "s3-fips" | "s3-accesspoint" | "s3-accesspoint-fips"]
                    | [.., "s3" | "s3-fips" | "s3-accesspoint" | "s3-accesspoint-fips", "dualstack"]
            ) || before.last().is_some_and(|label| is_s3_express_label(label))
        }
        [.., "s3" | "s3-accelerate" | "s3-external-1"] | [.., "s3-accelerate", "dualstack"] => true,
        [.., "accesspoint", "s3-global"] => true,
        [.., legacy] => {
            legacy.strip_prefix("s3-").is_some_and(|region| is_region(region.strip_prefix("fips-").unwrap_or(region)))
        }
        [] => false,
    }
}

/// `s3express-<zone id>` (`s3express-usw2-az1`), `s3express-fips-<zone id>` and
/// `s3express-control`: S3 Express One Zone endpoints.
fn is_s3_express_label(label: &str) -> bool {
    label.strip_prefix("s3express-").is_some_and(|rest| {
        !rest.is_empty() && rest.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    })
}

/// Whether `label` has the shape of an AWS region name: `us-east-1`, `us-gov-west-1`,
/// `eusc-de-east-1`.
fn is_region(label: &str) -> bool {
    let parts: Vec<&str> = label.split('-').collect();
    let [area, names @ .., number] = parts.as_slice() else { return false };
    let letters = |part: &&str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_lowercase());
    (2..=4).contains(&area.len())
        && letters(area)
        && !names.is_empty()
        && names.iter().all(letters)
        && !number.is_empty()
        && number.bytes().all(|byte| byte.is_ascii_digit())
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
        // The legacy class (written with SQSLargePayloadSize) was refused as unsupported.
        let legacy = r#"["com.amazon.sqs.javamessaging.MessageS3Pointer",{"s3BucketName":"b","s3Key":"k"}]"#;
        assert_eq!(PayloadPointer::parse_aws(legacy, Some(3)).unwrap(), aws);
        assert!(
            PayloadPointer::parse_aws(r#"["com.example.Pointer",{"s3BucketName":"b","s3Key":"k"}]"#, None).is_err()
        );
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
        let sha256 = hex::encode(Sha256::digest(b"payload"));
        assert_eq!(pointer.key, Some(format!("p/m1/{sha256}")));
        assert_eq!(pointer.sha256, Some(sha256));
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

    #[tokio::test]
    async fn large_payloads_are_hashed_and_verified_off_the_worker_threads() {
        let broker = MemoryBroker::new("us-east-1", "000000000000");
        let backend = broker.backend();
        backend.objects.create_bucket("b").await.unwrap();
        let data = Bytes::from((0..BLOCKING_HASH_THRESHOLD + 7).map(|i| (i % 251) as u8).collect::<Vec<u8>>());
        let sha256 = hex::encode(Sha256::digest(&data));
        assert_eq!(sha256_hex(&data).await.unwrap(), sha256);
        let pointer = offload(&backend, &settings("b"), "big", data.clone()).await.unwrap();
        assert_eq!(pointer.sha256.as_deref(), Some(sha256.as_str()));
        assert_eq!(fetch(&backend, &FetchSettings::default(), &pointer).await.unwrap(), data);
        let tampered = PayloadPointer { sha256: Some(hex::encode(Sha256::digest(b"other"))), ..pointer };
        assert!(matches!(fetch(&backend, &FetchSettings::default(), &tampered).await, Err(Error::PayloadIntegrity(_))));
    }

    #[tokio::test]
    async fn a_deleted_object_is_reported_as_missing_whichever_method_is_tried_last() {
        let broker = MemoryBroker::new("us-east-1", "000000000000");
        let backend = broker.backend();
        backend.objects.create_bucket("b").await.unwrap();
        let pointer = offload(&backend, &settings("b"), "m1", Bytes::from_static(b"payload")).await.unwrap();
        assert!(pointer.url.is_some());
        backend.objects.delete_object("b", pointer.key.as_deref().unwrap()).await.unwrap();
        for prefer in [FetchPreference::S3, FetchPreference::Url] {
            let error =
                fetch(&backend, &FetchSettings { prefer, ..FetchSettings::default() }, &pointer).await.unwrap_err();
            assert!(is_missing_object(&error), "{prefer:?}: {error}");
        }
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
        // Presigned URLs of the client's own partition, whichever it is.
        let sovereign_url = "https://bucket.s3.eusc-de-east-1.amazonaws.eu/k?X-Amz-Signature=1";
        assert!(check_url_allowed(sovereign_url, &defaults, &backend).is_err(), "another partition");
        let mut sovereign = MemoryBroker::new("eusc-de-east-1", "000000000000").backend();
        sovereign.kind = BackendKind::Aws;
        assert!(check_url_allowed(sovereign_url, &defaults, &sovereign).is_ok());
        let mut iso = MemoryBroker::new("us-iso-east-1", "000000000000").backend();
        iso.kind = BackendKind::Aws;
        assert!(check_url_allowed("https://bucket.s3.us-iso-east-1.c2s.ic.gov/k", &defaults, &iso).is_ok());
    }

    #[test]
    fn only_s3_endpoints_are_allowed_by_default() {
        let mut backend = MemoryBroker::new("us-east-1", "000000000000").backend();
        backend.kind = BackendKind::Aws;
        let defaults = FetchSettings::default();
        for host in [
            "s3.amazonaws.com",
            "bucket.s3.amazonaws.com",
            "my.dotted.bucket.s3.amazonaws.com",
            "s3.eu-west-1.amazonaws.com",
            "bucket.s3.eu-west-1.amazonaws.com",
            "bucket.s3-eu-west-1.amazonaws.com",
            "bucket.s3-external-1.amazonaws.com",
            "bucket.s3.dualstack.eu-west-1.amazonaws.com",
            "bucket.s3-fips.us-east-1.amazonaws.com",
            "bucket.s3-fips.dualstack.us-east-1.amazonaws.com",
            "bucket.s3-fips-us-gov-west-1.amazonaws.com",
            "bucket.s3.us-gov-west-1.amazonaws.com",
            "bucket.s3-accelerate.amazonaws.com",
            "bucket.s3-accelerate.dualstack.amazonaws.com",
            "points-123456789012.s3-accesspoint.eu-west-1.amazonaws.com",
            "points-123456789012.s3-accesspoint.dualstack.eu-west-1.amazonaws.com",
            "points-123456789012.s3-accesspoint-fips.us-east-1.amazonaws.com",
            "mfzwi23gnjvgw.mrap.accesspoint.s3-global.amazonaws.com",
            "bucket--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com",
            "bucket.s3.cn-north-1.amazonaws.com.cn",
        ] {
            let url = format!("https://{host}/key?X-Amz-Signature=1");
            assert!(check_url_allowed(&url, &defaults, &backend).is_ok(), "{host} is an S3 endpoint");
        }
        for host in [
            // Load balancers (internal ones resolve to private addresses), also when named s3-...
            "internal-orders-1234567890.eu-west-1.elb.amazonaws.com",
            "s3-orders-1234567890.eu-west-1.elb.amazonaws.com",
            "dualstack.s3-orders-1234567890.eu-west-1.elb.amazonaws.com",
            "s3-orders-0123456789abcdef.elb.eu-west-1.amazonaws.com",
            // EC2 public DNS names resolve to private addresses inside the VPC.
            "ec2-203-0-113-25.compute-1.amazonaws.com",
            "ec2-203-0-113-25.eu-west-1.compute.amazonaws.com",
            // Other services' endpoints.
            "abcdef1234.execute-api.eu-west-1.amazonaws.com",
            "sqs.eu-west-1.amazonaws.com",
            "sts.amazonaws.com",
            "mydb.abcdefghijkl.eu-west-1.rds.amazonaws.com",
            // VPC endpoints (interface endpoints resolve to private addresses), S3's too.
            "vpce-0123456789abcdef0-abcdefgh.execute-api.eu-west-1.vpce.amazonaws.com",
            "bucket.vpce-0123456789abcdef0-abcdefgh.s3.eu-west-1.vpce.amazonaws.com",
            // S3 hosts that do not serve presigned GetObject requests of a bucket.
            "bucket.s3-website-eu-west-1.amazonaws.com",
            "bucket.s3-website.eu-west-1.amazonaws.com",
            "points-123456789012.s3-object-lambda.eu-west-1.amazonaws.com",
            "points-123456789012.op-0123456789abcdef0.s3-outposts.eu-west-1.amazonaws.com",
            // Look-alikes.
            "s3.eu-west-1.amazonaws.com.example.com",
            "bucket.s3.eu-west-1.evilamazonaws.com",
            "s3.amazonaws.com.",
            "amazonaws.com",
            "s3..amazonaws.com",
            "eu-west-1.amazonaws.com",
            "bucket.s3.compute-1.amazonaws.com",
        ] {
            let url = format!("https://{host}/key?X-Amz-Signature=1");
            let error = check_url_allowed(&url, &defaults, &backend).unwrap_err();
            assert!(error.to_string().contains("not an S3 endpoint"), "{host}: {error}");
        }
        // Other hosts need allowed_url_hosts, which replaces the S3 endpoints.
        let custom = FetchSettings { allowed_url_hosts: vec!["*.elb.amazonaws.com".into()], ..defaults };
        assert!(check_url_allowed("https://lb-1.eu-west-1.elb.amazonaws.com/k", &custom, &backend).is_ok());
        assert!(check_url_allowed("https://bucket.s3.amazonaws.com/k", &custom, &backend).is_err());
    }

    #[test]
    fn the_endpoint_url_is_allowed_on_its_scheme_host_and_port_only() {
        let mut backend = MemoryBroker::new("us-east-1", "000000000000").backend();
        backend.kind = BackendKind::Aws;
        backend.endpoint_url = Some("http://localhost:4566".into());
        let defaults = FetchSettings::default();
        assert!(check_url_allowed("http://localhost:4566/bucket/k", &defaults, &backend).is_ok());
        assert!(check_url_allowed("http://LOCALHOST:4566/bucket/k", &defaults, &backend).is_ok());
        for url in
            ["http://localhost:6379/", "http://localhost/", "https://localhost:4566/bucket/k", "http://127.0.0.1:4566/"]
        {
            let error = check_url_allowed(url, &defaults, &backend).unwrap_err();
            assert!(error.to_string().contains("refusing to fetch"), "{url}: {error}");
        }
        // allow_http does not open other ports of the endpoint's host.
        let error = check_url_allowed("http://localhost:6379/", &defaults, &backend).unwrap_err();
        assert!(error.to_string().contains("over http:// from localhost:6379"), "{error}");
        let http = FetchSettings { allow_http: true, ..FetchSettings::default() };
        assert!(check_url_allowed("http://localhost:6379/", &http, &backend).is_err());
        // A default port is the same port, written or not.
        backend.endpoint_url = Some("https://s3.example.internal".into());
        assert!(check_url_allowed("https://s3.example.internal:443/bucket/k", &defaults, &backend).is_ok());
        assert!(check_url_allowed("https://s3.example.internal:8443/bucket/k", &defaults, &backend).is_err());
        assert!(check_url_allowed("http://s3.example.internal/bucket/k", &defaults, &backend).is_err());
        // And with allowed_url_hosts, the endpoint stays allowed.
        let custom = FetchSettings { allowed_url_hosts: vec!["cdn.example.com".into()], ..FetchSettings::default() };
        assert!(check_url_allowed("https://s3.example.internal/bucket/k", &custom, &backend).is_ok());
    }

    #[tokio::test]
    async fn pointers_without_a_sha256_are_not_fetched_by_url() {
        let broker = MemoryBroker::new("us-east-1", "000000000000");
        let backend = broker.backend();
        backend.objects.create_bucket("b").await.unwrap();
        let pointer = offload(&backend, &settings("b"), "m1", Bytes::from_static(b"payload")).await.unwrap();
        let by_url = FetchSettings { prefer: FetchPreference::Url, ..FetchSettings::default() };
        let url_only = PayloadPointer { bucket: None, key: None, ..pointer.clone() };
        assert_eq!(&fetch(&backend, &by_url, &url_only).await.unwrap()[..], b"payload");

        let unchecked = PayloadPointer { sha256: None, ..url_only };
        for settings in [by_url.clone(), FetchSettings::default(), FetchSettings { verify_checksum: false, ..by_url }] {
            let error = fetch(&backend, &settings, &unchecked).await.unwrap_err();
            assert!(error.to_string().contains("without a sha256"), "{error}");
        }
        // Bucket and key are still read without one (the extended clients' pointers have none).
        let s3_only = PayloadPointer { sha256: None, url: None, ..pointer.clone() };
        assert_eq!(&fetch(&backend, &FetchSettings::default(), &s3_only).await.unwrap()[..], b"payload");
        let both = PayloadPointer { sha256: None, ..pointer };
        assert_eq!(&fetch(&backend, &FetchSettings::default(), &both).await.unwrap()[..], b"payload");
    }

    #[tokio::test]
    async fn allowed_buckets_restrict_s3_pointers() {
        let broker = MemoryBroker::new("us-east-1", "000000000000");
        let backend = broker.backend();
        backend.objects.create_bucket("b").await.unwrap();
        let pointer = offload(&backend, &settings("b"), "m1", Bytes::from_static(b"payload")).await.unwrap();
        let s3_only = PayloadPointer { url: None, ..pointer.clone() };

        let allowed = FetchSettings { allowed_buckets: vec!["other".into(), "b".into()], ..FetchSettings::default() };
        assert_eq!(&fetch(&backend, &allowed, &s3_only).await.unwrap()[..], b"payload");
        let others = FetchSettings { allowed_buckets: vec!["other".into()], ..FetchSettings::default() };
        let error = fetch(&backend, &others, &s3_only).await.unwrap_err();
        assert!(error.to_string().contains("refusing to read a payload from bucket \"b\""), "{error}");
        // The URL is checked on its own: a pointer with both falls back to it.
        assert_eq!(&fetch(&backend, &others, &pointer).await.unwrap()[..], b"payload");
    }
}
