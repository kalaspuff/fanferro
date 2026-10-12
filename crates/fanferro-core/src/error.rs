//! Error model for the core.
//!
//! Every fallible operation in the core returns [`Result<T>`]. Errors carry enough
//! structure for callers (and the Python bindings) to decide what to do next:
//! [`Error::is_retryable`] tells whether retrying the same operation may succeed, and
//! [`AwsError`] keeps the service error code, request id and HTTP status so they can be
//! surfaced in logs, events and Python exceptions.

use std::fmt;

/// Convenience alias used throughout the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The kind of AWS resource an error or event refers to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    /// An SQS queue.
    Queue,
    /// An SNS topic.
    Topic,
    /// An SNS subscription.
    Subscription,
    /// An S3 bucket used for large payloads.
    Bucket,
}

impl ResourceKind {
    /// Stable lowercase name, used in events and error messages.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::Topic => "topic",
            Self::Subscription => "subscription",
            Self::Bucket => "bucket",
        }
    }
}

impl fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Classification of an AWS (or emulator) failure.
///
/// The classification drives retry decisions and the Python exception type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AwsErrorKind {
    /// Request rate exceeded; retry with backoff.
    Throttling,
    /// Server side or transient failure (5xx, incomplete response); retry.
    Transient,
    /// The request timed out; retry.
    Timeout,
    /// The request could not be dispatched (DNS, connect, TLS, IO); retry.
    Network,
    /// The addressed resource (queue, topic, subscription, object) does not exist.
    NotFound,
    /// Credentials are missing or lack permissions.
    AccessDenied,
    /// The request was rejected as invalid (bad parameter, attribute, size...).
    InvalidRequest,
    /// A KMS key problem (disabled, not found, access denied, throttled).
    Kms,
    /// The receipt handle is no longer valid (message was deleted or became visible again).
    ReceiptHandleInvalid,
    /// Any other service error.
    Service,
}

impl AwsErrorKind {
    /// Stable snake_case name, used in events and Python exceptions.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Throttling => "throttling",
            Self::Transient => "transient",
            Self::Timeout => "timeout",
            Self::Network => "network",
            Self::NotFound => "not_found",
            Self::AccessDenied => "access_denied",
            Self::InvalidRequest => "invalid_request",
            Self::Kms => "kms",
            Self::ReceiptHandleInvalid => "receipt_handle_invalid",
            Self::Service => "service",
        }
    }

    /// Whether an operation that failed with this kind may succeed when retried unchanged.
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::Throttling | Self::Transient | Self::Timeout | Self::Network)
    }
}

/// A failed call to an AWS API (or an emulator speaking the same protocol).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AwsError {
    /// Service name, e.g. `"sqs"`, `"sns"`, `"s3"`, `"sts"`.
    pub service: &'static str,
    /// Operation name, e.g. `"SendMessage"`.
    pub operation: &'static str,
    /// Classification used for retry decisions.
    pub kind: AwsErrorKind,
    /// Service error code, e.g. `"QueueDoesNotExist"`.
    pub code: Option<String>,
    /// Human readable message from the service or the transport.
    pub message: Option<String>,
    /// AWS request id when available.
    pub request_id: Option<String>,
    /// HTTP status code when a response was received.
    pub status: Option<u16>,
}

impl AwsError {
    /// Creates an error with the given classification and message.
    pub fn new(service: &'static str, operation: &'static str, kind: AwsErrorKind, message: impl Into<String>) -> Self {
        Self { service, operation, kind, code: None, message: Some(message.into()), request_id: None, status: None }
    }

    /// Sets the service error code.
    #[must_use]
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    /// Sets the HTTP status.
    #[must_use]
    pub fn with_status(mut self, status: u16) -> Self {
        self.status = Some(status);
        self
    }
}

impl fmt::Display for AwsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{} failed ({})", self.service, self.operation, self.kind.as_str())?;
        if let Some(code) = &self.code {
            write!(f, " [{code}]")?;
        }
        if let Some(message) = &self.message {
            write!(f, ": {message}")?;
        }
        if let Some(request_id) = &self.request_id {
            write!(f, " (request id {request_id})")?;
        }
        Ok(())
    }
}

impl std::error::Error for AwsError {}

/// All errors produced by the core.
#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Invalid configuration supplied by the caller.
    #[error("invalid configuration: {0}")]
    Config(String),
    /// A value failed validation before any network call was made.
    #[error("validation failed: {0}")]
    Validation(String),
    /// The encoded message exceeds the destination limit and offloading is not enabled.
    #[error(
        "message too large: {size} bytes exceeds the limit of {limit} bytes; enable large payload offloading (claim check) to send bigger messages"
    )]
    MessageTooLarge {
        /// Encoded size in bytes (body plus attributes).
        size: usize,
        /// The applicable limit.
        limit: usize,
    },
    /// A resource could not be found during lookup.
    #[error("{resource} not found: {name}")]
    NotFound {
        /// Kind of resource.
        resource: ResourceKind,
        /// Name, URL or ARN that was looked up.
        name: String,
    },
    /// An AWS API call failed.
    #[error(transparent)]
    Aws(Box<AwsError>),
    /// Large payload (claim check) storage or retrieval failed.
    #[error("payload error: {0}")]
    Payload(String),
    /// A fetched payload did not match its recorded size or checksum.
    #[error("payload integrity check failed: {0}")]
    PayloadIntegrity(String),
    /// An envelope could not be produced or parsed.
    #[error("envelope error: {0}")]
    Envelope(String),
    /// The message lease is no longer valid (already settled, lost or expired).
    #[error("lease error: {0}")]
    Lease(String),
    /// The operation was rejected because the component is shutting down or closed.
    #[error("shut down: {0}")]
    Shutdown(String),
    /// An HTTP request (for presigned URLs) failed.
    #[error("http error: {0}")]
    Http(String),
    /// A local timeout elapsed.
    #[error("timed out: {0}")]
    Timeout(String),
    /// A checksum (MD5) returned by the service did not match the local value.
    #[error("checksum mismatch: {0}")]
    Checksum(String),
}

impl Error {
    /// Whether retrying the same operation unchanged may succeed.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Aws(err) => err.kind.is_retryable(),
            Self::Http(_) | Self::Timeout(_) | Self::Checksum(_) => true,
            _ => false,
        }
    }

    /// Returns the AWS error details when this error came from an AWS call.
    pub fn aws(&self) -> Option<&AwsError> {
        match self {
            Self::Aws(err) => Some(err),
            _ => None,
        }
    }

    /// Whether this error means the addressed resource does not exist.
    pub fn is_not_found(&self) -> bool {
        match self {
            Self::NotFound { .. } => true,
            Self::Aws(err) => err.kind == AwsErrorKind::NotFound,
            _ => false,
        }
    }

    /// Stable snake_case name of the error category (used in events).
    pub fn category(&self) -> &'static str {
        match self {
            Self::Config(_) => "config",
            Self::Validation(_) => "validation",
            Self::MessageTooLarge { .. } => "message_too_large",
            Self::NotFound { .. } => "not_found",
            Self::Aws(err) => err.kind.as_str(),
            Self::Payload(_) => "payload",
            Self::PayloadIntegrity(_) => "payload_integrity",
            Self::Envelope(_) => "envelope",
            Self::Lease(_) => "lease",
            Self::Shutdown(_) => "shutdown",
            Self::Http(_) => "http",
            Self::Timeout(_) => "timeout",
            Self::Checksum(_) => "checksum",
        }
    }

    /// Shorthand for a validation error.
    pub fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    /// Shorthand for a configuration error.
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }
}

impl From<AwsError> for Error {
    fn from(err: AwsError) -> Self {
        Self::Aws(Box::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_classification() {
        let throttled = Error::from(AwsError::new("sqs", "SendMessage", AwsErrorKind::Throttling, "slow down"));
        assert!(throttled.is_retryable());
        let denied = Error::from(AwsError::new("sqs", "SendMessage", AwsErrorKind::AccessDenied, "no"));
        assert!(!denied.is_retryable());
        assert!(!Error::validation("bad").is_retryable());
        assert!(Error::Http("reset".into()).is_retryable());
    }

    #[test]
    fn not_found_detection() {
        let err = Error::from(AwsError::new("sqs", "GetQueueUrl", AwsErrorKind::NotFound, "missing"));
        assert!(err.is_not_found());
        assert!(Error::NotFound { resource: ResourceKind::Queue, name: "q".into() }.is_not_found());
        assert!(!Error::validation("x").is_not_found());
    }

    #[test]
    fn display_includes_code_and_request_id() {
        let mut err = AwsError::new("sns", "Publish", AwsErrorKind::InvalidRequest, "bad attribute")
            .with_code("InvalidParameter");
        err.request_id = Some("req-1".into());
        let text = err.to_string();
        assert!(text.contains("sns:Publish"));
        assert!(text.contains("[InvalidParameter]"));
        assert!(text.contains("req-1"));
    }
}
