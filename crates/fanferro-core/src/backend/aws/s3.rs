//! S3 adapter used for large payloads.

use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration, ServerSideEncryption, StorageClass};
use aws_smithy_types::error::display::DisplayErrorContext;
use bytes::{Bytes, BytesMut};

use super::errors::map_sdk_error;
use super::upload_timeouts;
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
            .set_metadata((!options.metadata.is_empty()).then(|| options.metadata.clone().into_iter().collect()));
        call = match &options.encryption {
            ObjectEncryption::BucketDefault => call,
            ObjectEncryption::S3Managed => call.server_side_encryption(ServerSideEncryption::Aes256),
            ObjectEncryption::Kms { key_id, bucket_key } => call
                .server_side_encryption(ServerSideEncryption::AwsKms)
                .set_ssekms_key_id(key_id.clone())
                .bucket_key_enabled(*bucket_key),
        };
        match timeouts {
            Some(timeouts) => {
                let timeouts = aws_sdk_s3::config::Builder::default().timeout_config(timeouts);
                call.customize().config_override(timeouts).send().await
            }
            None => call.send().await,
        }
        .map_err(|err| map_sdk_error("s3", "PutObject", err))?;
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
        if let Some(length) = output.content_length()
            && u64::try_from(length).unwrap_or(0) > max_bytes as u64
        {
            return Err(Error::Payload(format!("payload of {length} bytes exceeds the limit of {max_bytes} bytes")));
        }
        let mut body = output.body;
        let mut buffer = BytesMut::new();
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
            if buffer.len() + chunk.len() > max_bytes {
                return Err(Error::Payload(format!("payload exceeds the limit of {max_bytes} bytes")));
            }
            buffer.extend_from_slice(&chunk);
        }
        Ok(buffer.freeze())
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
                match error.aws().map(|aws| aws.kind) {
                    Some(AwsErrorKind::NotFound) => Ok(false),
                    // The bucket exists but belongs to someone else / we lack ListBucket.
                    Some(AwsErrorKind::AccessDenied) => Ok(true),
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
