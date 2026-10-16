//! AWS SDK for Rust implementation of the backend traits.
//!
//! Works against AWS and AWS-compatible endpoints (MiniStack and similar emulators) by
//! setting [`AwsConfig::endpoint_url`]. Retries for individual API calls are delegated to
//! the SDK (standard or adaptive retry mode with jittered exponential backoff); fanferro
//! adds its own retries on top only where the SDK cannot (partial batch failures, the
//! receive loop, settlement).

mod errors;
mod s3;
mod sns;
mod sqs;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_credential_types::Credentials;
use aws_types::app_name::AppName;
use aws_types::region::Region;
use bytes::{Bytes, BytesMut};
use serde::Deserialize;
use time::macros::format_description;
use time::{OffsetDateTime, PrimitiveDateTime};
use tokio::sync::OnceCell;

pub(crate) use errors::classify;
use errors::map_sdk_error;
pub use s3::AwsObjectStore;
pub use sns::AwsSns;
pub use sqs::AwsSqs;

use super::{Backend, BackendKind, HttpFetch, IdentityApi};
use crate::error::{Error, Result};
use crate::util::duration_from_secs;

/// Static credentials (useful for emulators and tests).
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticCredentials {
    /// Access key id.
    pub access_key_id: String,
    /// Secret access key.
    pub secret_access_key: String,
    /// Optional session token.
    #[serde(default)]
    pub session_token: Option<String>,
}

impl std::fmt::Debug for StaticCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticCredentials").field("access_key_id", &self.access_key_id).finish_non_exhaustive()
    }
}

/// SDK retry mode.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RetryMode {
    /// Standard mode: jittered exponential backoff with a retry quota.
    #[default]
    Standard,
    /// Adaptive mode: standard plus client-side rate limiting when throttled.
    Adaptive,
}

/// Retry settings for individual AWS API calls.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetrySettings {
    /// Maximum attempts per call (including the first). `1` disables retries.
    pub max_attempts: u32,
    /// Retry mode.
    pub mode: RetryMode,
    /// Initial backoff in seconds.
    pub initial_backoff: f64,
    /// Maximum backoff in seconds.
    pub max_backoff: f64,
}

impl Default for RetrySettings {
    fn default() -> Self {
        Self { max_attempts: 5, mode: RetryMode::Standard, initial_backoff: 0.2, max_backoff: 20.0 }
    }
}

/// Timeouts for AWS API calls (seconds). `None` keeps the SDK default.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TimeoutSettings {
    /// TCP/TLS connect timeout.
    pub connect: Option<f64>,
    /// Time to wait for response data.
    pub read: Option<f64>,
    /// Total time for a call including retries.
    pub operation: Option<f64>,
    /// Time for a single attempt.
    pub attempt: Option<f64>,
}

impl Default for TimeoutSettings {
    fn default() -> Self {
        Self { connect: Some(3.1), read: None, operation: None, attempt: None }
    }
}

/// How to connect to AWS (or an AWS-compatible emulator).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AwsConfig {
    /// Region; falls back to the standard provider chain (env, profile, IMDS).
    pub region: Option<String>,
    /// Custom endpoint, e.g. `http://localhost:4566` for MiniStack.
    pub endpoint_url: Option<String>,
    /// Named profile from `~/.aws/config`.
    pub profile: Option<String>,
    /// Static credentials; falls back to the standard provider chain.
    pub credentials: Option<StaticCredentials>,
    /// Account id; when unset it is looked up with STS `GetCallerIdentity` when needed.
    pub account_id: Option<String>,
    /// Application name added to the user agent.
    pub app_name: Option<String>,
    /// Per-call retry settings.
    pub retry: RetrySettings,
    /// Per-call timeouts.
    pub timeouts: TimeoutSettings,
    /// Use path-style S3 addressing (defaults to `true` when `endpoint_url` is set).
    pub s3_force_path_style: Option<bool>,
}

/// Builds a [`Backend`] from an [`AwsConfig`].
pub async fn connect(config: &AwsConfig) -> Result<Backend> {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(region) = &config.region {
        loader = loader.region(Region::new(region.clone()));
    }
    if let Some(profile) = &config.profile {
        loader = loader.profile_name(profile);
    }
    if let Some(endpoint) = &config.endpoint_url {
        loader = loader.endpoint_url(endpoint);
    }
    if let Some(credentials) = &config.credentials {
        loader = loader.credentials_provider(Credentials::new(
            credentials.access_key_id.clone(),
            credentials.secret_access_key.clone(),
            credentials.session_token.clone(),
            None,
            "fanferro-static",
        ));
    }
    let app_name = config.app_name.clone().unwrap_or_else(|| "fanferro".to_string());
    loader = loader.app_name(
        AppName::new(app_name.clone()).map_err(|err| Error::config(format!("invalid app_name {app_name:?}: {err}")))?,
    );
    loader = loader.retry_config(retry_config(&config.retry)?);
    loader = loader.timeout_config(timeout_config(&config.timeouts)?);
    let sdk_config = loader.load().await;
    let region = sdk_config
        .region()
        .map(ToString::to_string)
        .ok_or_else(|| Error::config("no AWS region configured; pass region=... or set AWS_REGION"))?;

    let sqs_client = aws_sdk_sqs::Client::new(&sdk_config);
    let sns_client = aws_sdk_sns::Client::new(&sdk_config);
    let force_path_style = config.s3_force_path_style.unwrap_or(config.endpoint_url.is_some());
    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config).force_path_style(force_path_style).build();
    let s3_client = aws_sdk_s3::Client::from_conf(s3_config);
    let sts_client = aws_sdk_sts::Client::new(&sdk_config);

    let identity = AwsIdentity { client: sts_client, account_id: OnceCell::new() };
    if let Some(account_id) = &config.account_id {
        identity.account_id.set(account_id.clone()).ok();
    }
    Ok(Backend {
        kind: BackendKind::Aws,
        region,
        endpoint_url: config.endpoint_url.clone(),
        sqs: Arc::new(AwsSqs::new(sqs_client)),
        sns: Arc::new(AwsSns::new(sns_client)),
        objects: Arc::new(AwsObjectStore::new(s3_client)),
        http: Arc::new(ReqwestFetch::default()),
        identity: Arc::new(identity),
    })
}

fn retry_config(settings: &RetrySettings) -> Result<RetryConfig> {
    if settings.max_attempts == 0 {
        return Err(Error::config("retry.max_attempts must be at least 1"));
    }
    let base = match settings.mode {
        RetryMode::Standard => RetryConfig::standard(),
        RetryMode::Adaptive => RetryConfig::adaptive(),
    };
    Ok(base
        .with_max_attempts(settings.max_attempts)
        .with_initial_backoff(duration_from_secs(settings.initial_backoff, "retry.initial_backoff")?)
        .with_max_backoff(duration_from_secs(settings.max_backoff, "retry.max_backoff")?))
}

fn timeout_config(settings: &TimeoutSettings) -> Result<TimeoutConfig> {
    let mut builder = TimeoutConfig::builder();
    if let Some(secs) = settings.connect {
        builder = builder.connect_timeout(duration_from_secs(secs, "timeouts.connect")?);
    }
    if let Some(secs) = settings.read {
        builder = builder.read_timeout(duration_from_secs(secs, "timeouts.read")?);
    }
    if let Some(secs) = settings.operation {
        builder = builder.operation_timeout(duration_from_secs(secs, "timeouts.operation")?);
    }
    if let Some(secs) = settings.attempt {
        builder = builder.operation_attempt_timeout(duration_from_secs(secs, "timeouts.attempt")?);
    }
    Ok(builder.build())
}

/// STS-backed identity with a cached account id.
#[derive(Debug)]
struct AwsIdentity {
    client: aws_sdk_sts::Client,
    account_id: OnceCell<String>,
}

#[async_trait]
impl IdentityApi for AwsIdentity {
    async fn account_id(&self) -> Result<String> {
        self.account_id
            .get_or_try_init(|| async {
                let output = self
                    .client
                    .get_caller_identity()
                    .send()
                    .await
                    .map_err(|err| map_sdk_error("sts", "GetCallerIdentity", err))?;
                output.account().map(str::to_string).ok_or_else(|| Error::config("STS returned no account id"))
            })
            .await
            .cloned()
    }
}

/// `reqwest`-based fetcher for presigned URLs. Its client is built on first use: building it loads
/// the system's root certificates (milliseconds), which most applications never need.
#[derive(Debug, Default)]
struct ReqwestFetch {
    client: OnceCell<reqwest::Client>,
}

impl ReqwestFetch {
    async fn client(&self) -> Result<&reqwest::Client> {
        self.client
            .get_or_try_init(|| async {
                reqwest::Client::builder()
                    .connect_timeout(Duration::from_secs(10))
                    .timeout(Duration::from_secs(300))
                    .user_agent(concat!("fanferro/", env!("CARGO_PKG_VERSION")))
                    .build()
                    .map_err(|err| Error::config(format!("cannot build HTTP client: {err}")))
            })
            .await
    }
}

#[async_trait]
impl HttpFetch for ReqwestFetch {
    async fn get(&self, url: &str, max_bytes: usize) -> Result<Bytes> {
        let mut response =
            self.client().await?.get(url).send().await.map_err(|err| Error::Http(redact_url_error(&err)))?;
        let status = response.status();
        if !status.is_success() {
            return Err(if status.as_u16() == 404 || status.as_u16() == 403 {
                inaccessible_url_error(url, status, OffsetDateTime::now_utc())
            } else {
                Error::Http(format!("GET returned HTTP {status}"))
            });
        }
        if let Some(length) = response.content_length()
            && length > max_bytes as u64
        {
            return Err(Error::Payload(format!("payload of {length} bytes exceeds the limit of {max_bytes} bytes")));
        }
        let mut buffer = BytesMut::new();
        while let Some(chunk) = response.chunk().await.map_err(|err| Error::Http(redact_url_error(&err)))? {
            if buffer.len() + chunk.len() > max_bytes {
                return Err(Error::Payload(format!("payload exceeds the limit of {max_bytes} bytes")));
            }
            buffer.extend_from_slice(&chunk);
        }
        Ok(buffer.freeze())
    }
}

/// Why a presigned URL was refused: it expired (by its `X-Amz-Date` and `X-Amz-Expires`), or it
/// is not accessible for another reason. The URL itself is left out: it carries credentials.
fn inaccessible_url_error(url: &str, status: reqwest::StatusCode, now: OffsetDateTime) -> Error {
    let parsed = reqwest::Url::parse(url).ok();
    let parameter =
        |name: &str| parsed.as_ref()?.query_pairs().find(|(key, _)| key == name).map(|(_, value)| value.into_owned());
    let signed_at = parameter("X-Amz-Date").and_then(|date| {
        PrimitiveDateTime::parse(&date, format_description!("[year][month][day]T[hour][minute][second]Z")).ok()
    });
    let lifetime = parameter("X-Amz-Expires").and_then(|seconds| seconds.parse::<i64>().ok());
    if let (Some(signed_at), Some(lifetime)) = (signed_at, lifetime) {
        let expired_at = signed_at.assume_utc() + time::Duration::seconds(lifetime);
        if expired_at <= now {
            return crate::transport::claim_check::expired_url_error(expired_at);
        }
    }
    let reason = if parameter("X-Amz-Security-Token").is_some() {
        "it was signed with temporary credentials, which may have expired"
    } else {
        "it may have expired"
    };
    Error::Payload(format!("payload URL is not accessible (HTTP {status}); {reason}"))
}

/// Formats a reqwest error without the URL (presigned URLs contain credentials).
fn redact_url_error(err: &reqwest::Error) -> String {
    let mut text = err.to_string();
    if let Some(url) = err.url() {
        text = text.replace(url.as_str(), "<redacted url>");
    }
    if err.is_timeout() {
        text.push_str(" (timeout)");
    }
    text
}

#[cfg(test)]
mod tests {
    use time::macros::datetime;

    use super::*;

    #[test]
    fn expired_presigned_urls_say_when_they_expired() {
        let url = "https://b.s3.amazonaws.com/k?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20261016T120000Z\
                   &X-Amz-Expires=3600&X-Amz-Signature=abc";
        let forbidden = reqwest::StatusCode::FORBIDDEN;
        let error = inaccessible_url_error(url, forbidden, datetime!(2026-10-16 13:00 UTC)).to_string();
        assert!(error.contains("expired at 2026-10-16T13:00:00Z"), "{error}");
        assert!(!error.contains("Signature"), "the URL is left out: {error}");
        let error = inaccessible_url_error(url, forbidden, datetime!(2026-10-16 12:59 UTC)).to_string();
        assert!(error.contains("not accessible (HTTP 403 Forbidden); it may have expired"), "{error}");
        let session = format!("{url}&X-Amz-Security-Token=token");
        let error = inaccessible_url_error(&session, forbidden, datetime!(2026-10-16 12:30 UTC)).to_string();
        assert!(error.contains("temporary credentials"), "{error}");
        let error = inaccessible_url_error("not a url", reqwest::StatusCode::NOT_FOUND, OffsetDateTime::now_utc());
        assert!(error.to_string().contains("HTTP 404"), "{error}");
    }
}
