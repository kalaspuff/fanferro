//! Mapping AWS SDK errors to [`AwsError`] with a retry-relevant classification.

use aws_credential_types::provider::error::CredentialsError;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::Response as HttpResponse;
use aws_smithy_types::error::display::DisplayErrorContext;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use aws_types::request_id::RequestId;

use crate::error::{AwsError, AwsErrorKind, Error};

/// Converts an SDK error into a classified [`Error::Aws`].
pub(crate) fn map_sdk_error<E>(service: &'static str, operation: &'static str, err: SdkError<E, HttpResponse>) -> Error
where
    E: ProvideErrorMetadata + std::error::Error + Send + Sync + 'static,
{
    let request_id = err.request_id().map(str::to_string);
    let status = err.raw_response().map(|response| response.status().as_u16());
    let (kind, code, message) = match &err {
        SdkError::ConstructionFailure(_) => {
            (AwsErrorKind::InvalidRequest, None, Some(DisplayErrorContext(&err).to_string()))
        }
        SdkError::TimeoutError(_) => (AwsErrorKind::Timeout, None, Some(DisplayErrorContext(&err).to_string())),
        SdkError::DispatchFailure(failure) => {
            let kind = if failure.is_timeout() {
                AwsErrorKind::Timeout
            } else {
                credentials_failure(&err).unwrap_or(AwsErrorKind::Network)
            };
            (kind, None, Some(DisplayErrorContext(&err).to_string()))
        }
        SdkError::ResponseError(_) => (AwsErrorKind::Transient, None, Some(DisplayErrorContext(&err).to_string())),
        SdkError::ServiceError(context) => {
            let service_error = context.err();
            let code = service_error.code().map(str::to_string);
            let message = service_error.message().map(str::to_string);
            (classify(code.as_deref(), message.as_deref(), status), code, message)
        }
        _ => (AwsErrorKind::Service, None, Some(DisplayErrorContext(&err).to_string())),
    };
    Error::from(AwsError { service, operation, kind, code, message, request_id, status })
}

/// How a call failed because no credentials could be loaded. The SDK resolves them before it sends
/// the request and reports a failure as a dispatch failure, like a connection error; without
/// credentials, or with an invalid configuration, retrying cannot help.
fn credentials_failure(error: &(dyn std::error::Error + 'static)) -> Option<AwsErrorKind> {
    let mut source = Some(error);
    while let Some(error) = source {
        if let Some(credentials) = error.downcast_ref::<CredentialsError>() {
            return match credentials {
                CredentialsError::CredentialsNotLoaded(_) | CredentialsError::InvalidConfiguration(_) => {
                    Some(AwsErrorKind::AccessDenied)
                }
                CredentialsError::ProviderTimedOut(_) => Some(AwsErrorKind::Timeout),
                // The provider could not reach its source (IMDS, STS, SSO), or failed unexpectedly.
                _ => None,
            };
        }
        source = error.source();
    }
    None
}

/// Classifies a service error code (and message/status as fallbacks).
pub(crate) fn classify(code: Option<&str>, message: Option<&str>, status: Option<u16>) -> AwsErrorKind {
    let full = code.unwrap_or_default();
    // SQS query-protocol codes look like "AWS.SimpleQueueService.NonExistentQueue".
    let short = full.rsplit('.').next().unwrap_or(full);
    let message_lower = message.unwrap_or_default().to_ascii_lowercase();
    match short {
        "QueueDoesNotExist"
        | "NonExistentQueue"
        | "NotFound"
        | "NotFoundException"
        | "ResourceNotFoundException"
        | "NoSuchKey"
        | "NoSuchBucket"
        | "NoSuchEntity" => AwsErrorKind::NotFound,
        "ReceiptHandleIsInvalid" | "InvalidReceiptHandle" | "MessageNotInflight" => AwsErrorKind::ReceiptHandleInvalid,
        "AccessDenied"
        | "AccessDeniedException"
        | "AuthorizationError"
        | "InvalidClientTokenId"
        | "UnrecognizedClientException"
        | "SignatureDoesNotMatch"
        | "ExpiredToken"
        | "ExpiredTokenException"
        | "InvalidSecurity"
        | "MissingAuthenticationToken"
        | "IncompleteSignature"
        | "OptInRequired"
        | "AuthFailure" => AwsErrorKind::AccessDenied,
        c if (c.starts_with("Kms") || c.starts_with("KMS")) && c.contains("Throttl") => AwsErrorKind::Throttling,
        c if c.starts_with("Kms") || c.starts_with("KMS") => AwsErrorKind::Kms,
        "Throttling"
        | "ThrottlingException"
        | "ThrottledException"
        | "Throttled"
        | "RequestThrottled"
        | "RequestThrottledException"
        | "TooManyRequestsException"
        | "RequestLimitExceeded"
        | "SlowDown"
        | "PriorRequestNotComplete"
        | "BandwidthLimitExceeded"
        | "OverLimit" => AwsErrorKind::Throttling,
        "RequestTimeout" | "RequestTimeoutException" => AwsErrorKind::Timeout,
        "InternalError"
        | "InternalFailure"
        | "ServiceUnavailable"
        | "InternalErrorException"
        | "ServiceUnavailableException"
        | "InternalServerError" => AwsErrorKind::Transient,
        "InvalidParameterValue" | "InvalidParameter" | "InvalidParameterException"
            if message_lower.contains("receipt handle") || message_lower.contains("receipthandle") =>
        {
            AwsErrorKind::ReceiptHandleInvalid
        }
        "InvalidParameter"
        | "InvalidParameterValue"
        | "InvalidParameterException"
        | "InvalidAttributeName"
        | "InvalidAttributeValue"
        | "InvalidMessageContents"
        | "ValidationError"
        | "ValidationException"
        | "MissingParameter"
        | "InvalidBatchEntryId"
        | "TooManyEntriesInBatchRequest"
        | "BatchRequestTooLong"
        | "BatchEntryIdsNotDistinct"
        | "EmptyBatchRequest"
        | "UnsupportedOperation"
        | "QueueDeletedRecently"
        | "QueueAlreadyExists"
        | "QueueNameExists"
        | "InvalidAddress"
        | "PurgeQueueInProgress"
        | "InvalidAction"
        | "MalformedQueryString" => AwsErrorKind::InvalidRequest,
        _ => match status {
            Some(404) => AwsErrorKind::NotFound,
            Some(403 | 401) => AwsErrorKind::AccessDenied,
            Some(429) => AwsErrorKind::Throttling,
            Some(500..=599) => AwsErrorKind::Transient,
            Some(400..=499) => AwsErrorKind::InvalidRequest,
            _ => AwsErrorKind::Service,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use aws_config::BehaviorVersion;
    use aws_config::retry::RetryConfig;
    use aws_credential_types::provider::{ProvideCredentials, future};
    use aws_types::region::Region;

    use super::*;

    /// A credentials provider that fails like the default chain without credentials.
    #[derive(Debug)]
    struct FailingCredentials(fn() -> CredentialsError);

    impl ProvideCredentials for FailingCredentials {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            future::ProvideCredentials::ready(Err((self.0)()))
        }
    }

    async fn call_with(credentials: FailingCredentials) -> Error {
        let config = aws_sdk_sqs::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url("http://127.0.0.1:9") // never reached: the request is not signed
            .credentials_provider(credentials)
            .retry_config(RetryConfig::disabled())
            .build();
        let error = aws_sdk_sqs::Client::from_conf(config).get_queue_url().queue_name("q").send().await.unwrap_err();
        map_sdk_error("sqs", "GetQueueUrl", error)
    }

    #[tokio::test]
    async fn missing_credentials_are_access_denied() {
        let error = call_with(FailingCredentials(|| CredentialsError::not_loaded("no credentials"))).await;
        assert_eq!(error.aws().map(|aws| aws.kind), Some(AwsErrorKind::AccessDenied), "{error:?}");
        assert!(!error.is_retryable());
        let error = call_with(FailingCredentials(|| CredentialsError::invalid_configuration("bad profile"))).await;
        assert_eq!(error.aws().map(|aws| aws.kind), Some(AwsErrorKind::AccessDenied), "{error:?}");
        let error =
            call_with(FailingCredentials(|| CredentialsError::provider_timed_out(Duration::from_secs(5)))).await;
        assert_eq!(error.aws().map(|aws| aws.kind), Some(AwsErrorKind::Timeout), "{error:?}");
        assert!(error.is_retryable());
        // A provider that could not reach its source (IMDS, STS) may succeed when retried.
        let error = call_with(FailingCredentials(|| CredentialsError::provider_error("connection refused"))).await;
        assert_eq!(error.aws().map(|aws| aws.kind), Some(AwsErrorKind::Network), "{error:?}");
    }

    #[tokio::test]
    async fn a_bucket_is_not_found_without_credentials() {
        use crate::backend::ObjectStoreApi;

        // S3 answers 403 for a bucket of another account: it exists. Without credentials nothing
        // was asked, so provisioning must not report the bucket as found.
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url("http://127.0.0.1:9")
            .force_path_style(true)
            .credentials_provider(FailingCredentials(|| CredentialsError::not_loaded("no credentials")))
            .retry_config(RetryConfig::disabled())
            .build();
        let store = super::super::AwsObjectStore::new(aws_sdk_s3::Client::from_conf(config));
        let error = store.bucket_exists("payloads").await.unwrap_err();
        assert_eq!(error.aws().map(|aws| aws.kind), Some(AwsErrorKind::AccessDenied), "{error:?}");
    }

    #[test]
    fn classifies_common_codes() {
        assert_eq!(classify(Some("AWS.SimpleQueueService.NonExistentQueue"), None, Some(400)), AwsErrorKind::NotFound);
        assert_eq!(classify(Some("QueueDoesNotExist"), None, None), AwsErrorKind::NotFound);
        assert_eq!(classify(Some("NotFound"), None, Some(404)), AwsErrorKind::NotFound);
        assert_eq!(classify(Some("ThrottlingException"), None, Some(400)), AwsErrorKind::Throttling);
        assert_eq!(classify(Some("KMSThrottlingException"), None, None), AwsErrorKind::Throttling);
        assert_eq!(classify(Some("KmsDisabled"), None, None), AwsErrorKind::Kms);
        assert_eq!(classify(Some("AuthorizationError"), None, Some(403)), AwsErrorKind::AccessDenied);
        assert_eq!(classify(Some("ReceiptHandleIsInvalid"), None, None), AwsErrorKind::ReceiptHandleInvalid);
        assert_eq!(
            classify(
                Some("InvalidParameterValue"),
                Some("Value x for parameter ReceiptHandle is invalid. Reason: The receipt handle has expired."),
                Some(400)
            ),
            AwsErrorKind::ReceiptHandleInvalid
        );
        assert_eq!(classify(Some("InvalidParameterValue"), Some("bad"), Some(400)), AwsErrorKind::InvalidRequest);
        assert_eq!(classify(Some("Weird"), None, Some(503)), AwsErrorKind::Transient);
        assert_eq!(classify(None, None, Some(429)), AwsErrorKind::Throttling);
        assert_eq!(classify(None, None, None), AwsErrorKind::Service);
    }
}
