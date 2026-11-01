//! S3 adapter used for large payloads.

use std::time::Duration;

use async_trait::async_trait;
use aws_runtime::auth::PayloadSigningOverride;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration, ServerSideEncryption, StorageClass};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;
use aws_smithy_types::error::display::DisplayErrorContext;
use bytes::Bytes;

use super::errors::map_sdk_error;
use super::{PayloadBuffer, upload_timeouts};
use crate::backend::{ObjectEncryption, ObjectStoreApi, PutOptions};
use crate::error::{AwsError, AwsErrorKind, Error, Result};

/// S3 backed by the AWS SDK.
#[derive(Clone, Debug)]
pub struct AwsObjectStore {
    client: aws_sdk_s3::Client,
}

impl AwsObjectStore {
    /// Wraps an SDK client.
    pub fn new(client: aws_sdk_s3::Client) -> Self {
        Self { client }
    }
}

impl AwsObjectStore {
    /// Whether a bucket whose `HeadBucket` S3 answered with 403 exists. The answer to a `HEAD` has
    /// no body, so it is the same for a bucket of another account and for credentials S3 does not
    /// know (docs/parity P1); the error of a `GET` names its reason.
    async fn denied_bucket_exists(&self, bucket: &str) -> Result<bool> {
        let Err(err) = self.client.get_bucket_location().bucket(bucket).send().await else { return Ok(true) };
        let error = map_sdk_error("s3", "GetBucketLocation", err);
        let code = error.aws().and_then(|aws| aws.code.as_deref());
        if matches!(
            code,
            Some(
                "InvalidAccessKeyId"
                    | "SignatureDoesNotMatch"
                    | "ExpiredToken"
                    | "InvalidToken"
                    | "TokenRefreshRequired"
            )
        ) {
            return Err(error);
        }
        // Access denied (another account's bucket, or no s3:GetBucketLocation): it exists.
        Ok(!error.is_not_found())
    }
}

/// Signs a request with the payload hash the caller computed (`x-amz-content-sha256`), instead of
/// the one the SDK computes by reading the whole body.
#[derive(Debug)]
struct PrecomputedPayloadHash(String);

impl Intercept for PrecomputedPayloadHash {
    fn name(&self) -> &'static str {
        "PrecomputedPayloadHash"
    }

    fn modify_before_signing(
        &self,
        _context: &mut BeforeTransmitInterceptorContextMut<'_>,
        _components: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> std::result::Result<(), BoxError> {
        cfg.interceptor_state().store_put(PayloadSigningOverride::Precomputed(self.0.clone()));
        Ok(())
    }
}

/// A hex SHA-256 as S3's `x-amz-checksum-sha256` takes it (base64 of the digest).
fn base64_digest(hex_digest: &str) -> Option<String> {
    use base64::Engine as _;
    let digest = hex::decode(hex_digest).ok().filter(|digest| digest.len() == 32)?;
    Some(base64::engine::general_purpose::STANDARD.encode(digest))
}

#[async_trait]
impl ObjectStoreApi for AwsObjectStore {
    async fn put_object(&self, bucket: &str, key: &str, data: Bytes, options: &PutOptions) -> Result<()> {
        let timeouts = upload_timeouts(self.client.config().timeout_config(), data.len());
        let mut call = self
            .client
            .put_object()
            .bucket(bucket)
            .key(key)
            .content_length(i64::try_from(data.len()).unwrap_or(i64::MAX))
            .body(ByteStream::from(data))
            .set_content_type(options.content_type.clone())
            .set_storage_class(options.storage_class.as_deref().map(StorageClass::from))
            .set_metadata((!options.metadata.is_empty()).then(|| options.metadata.clone().into_iter().collect()))
            .set_checksum_sha256(options.sha256.as_deref().and_then(base64_digest));
        call = match &options.encryption {
            ObjectEncryption::BucketDefault => call,
            ObjectEncryption::S3Managed => call.server_side_encryption(ServerSideEncryption::Aes256),
            ObjectEncryption::Kms { key_id, bucket_key } => call
                .server_side_encryption(ServerSideEncryption::AwsKms)
                .set_ssekms_key_id(key_id.clone())
                .bucket_key_enabled(*bucket_key),
        };
        let mut call = call.customize();
        if let Some(timeouts) = timeouts {
            call = call.config_override(aws_sdk_s3::config::Builder::default().timeout_config(timeouts));
        }
        // The SHA-256 the claim check computed signs the request: the SDK would hash the payload
        // again for SigV4 (and a checksum given by the caller spares it the CRC32 of its own).
        if let Some(sha256) = &options.sha256 {
            call = call.interceptor(PrecomputedPayloadHash(sha256.clone()));
        }
        call.send().await.map_err(|err| map_sdk_error("s3", "PutObject", err))?;
        Ok(())
    }

    async fn get_object(&self, bucket: &str, key: &str, max_bytes: usize) -> Result<Bytes> {
        let output = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| map_sdk_error("s3", "GetObject", err))?;
        let length = output.content_length().and_then(|length| u64::try_from(length).ok());
        let mut buffer = PayloadBuffer::new(length, max_bytes)?;
        let mut body = output.body;
        while let Some(chunk) = body.try_next().await.map_err(|err| {
            // The connection broke off (or the SDK cut off a stalled transfer): like a request
            // that failed, retrying may succeed.
            Error::from(AwsError::new(
                "s3",
                "GetObject",
                AwsErrorKind::Network,
                format!("failed to read the payload object: {}", DisplayErrorContext(&err)),
            ))
        })? {
            buffer.push(&chunk)?;
        }
        Ok(buffer.finish())
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| map_sdk_error("s3", "DeleteObject", err))?;
        Ok(())
    }

    async fn presign_get(&self, bucket: &str, key: &str, ttl: Duration) -> Result<String> {
        let config = PresigningConfig::expires_in(ttl)
            .map_err(|err| Error::config(format!("invalid presigned URL lifetime {ttl:?}: {err}")))?;
        let request = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .presigned(config)
            .await
            .map_err(|err| map_sdk_error("s3", "PresignGetObject", err))?;
        Ok(request.uri().to_string())
    }

    async fn bucket_exists(&self, bucket: &str) -> Result<bool> {
        match self.client.head_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(true),
            Err(err) => {
                let error = map_sdk_error("s3", "HeadBucket", err);
                match error.aws().map(|aws| (aws.kind, aws.status)) {
                    Some((AwsErrorKind::NotFound, _)) => Ok(false),
                    // S3 answered 403: the bucket exists but belongs to someone else / we lack
                    // ListBucket, or S3 does not know the credentials. Credentials that could not
                    // be loaded are access denied without an answer: nothing was checked.
                    Some((AwsErrorKind::AccessDenied, Some(_))) => self.denied_bucket_exists(bucket).await,
                    _ => Err(error),
                }
            }
        }
    }

    async fn create_bucket(&self, bucket: &str) -> Result<()> {
        let region = self.client.config().region().map(ToString::to_string).unwrap_or_default();
        let mut call = self.client.create_bucket().bucket(bucket);
        if !region.is_empty() && region != "us-east-1" {
            call = call.create_bucket_configuration(
                CreateBucketConfiguration::builder()
                    .location_constraint(BucketLocationConstraint::from(region.as_str()))
                    .build(),
            );
        }
        match call.send().await {
            Ok(_) => Ok(()),
            Err(err) => {
                let error = map_sdk_error("s3", "CreateBucket", err);
                // Creating a bucket we already own is not an error for provisioning purposes.
                if error.aws().and_then(|aws| aws.code.as_deref()) == Some("BucketAlreadyOwnedByYou") {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }
}
