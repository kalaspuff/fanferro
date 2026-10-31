//! Backends ("ports"): the minimal SQS, SNS, S3, HTTP and identity operations the rest of
//! the core needs, expressed with fanferro's own request/response types.
//!
//! Two implementations exist:
//!
//! * [`aws`]: the AWS SDK for Rust (also used for AWS-compatible emulators such as MiniStack).
//! * [`memory`]: an in-process broker with SQS/SNS semantics for fast, deterministic tests.
//!
//! Everything above this module (transport, provisioning, publisher, consumer engine) only
//! talks to these traits. Adding an operation means: add it to the trait, implement it in
//! both backends, and cover it with tests in both (see `AGENTS.md`).

pub mod aws;
pub mod memory;
#[cfg(test)]
pub(crate) mod testing;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use time::OffsetDateTime;

use crate::error::Result;
use crate::types::MessageAttributes;

/// String-to-string map used for queue/topic/subscription attributes and tags.
pub type StringMap = BTreeMap<String, String>;

/// SQS system attributes fanferro reads from received messages.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SystemAttributes {
    /// How many times the message has been received (including this receive).
    pub approximate_receive_count: u32,
    /// When the message was sent.
    pub sent_timestamp: Option<OffsetDateTime>,
    /// When the message was first received.
    pub approximate_first_receive_timestamp: Option<OffsetDateTime>,
    /// Sender principal id.
    pub sender_id: Option<String>,
    /// FIFO message group id.
    pub message_group_id: Option<String>,
    /// FIFO deduplication id.
    pub message_deduplication_id: Option<String>,
    /// FIFO sequence number.
    pub sequence_number: Option<String>,
    /// X-Ray trace header (`AWSTraceHeader`).
    pub aws_trace_header: Option<String>,
    /// Source queue ARN when the message was moved to a dead-letter queue by SQS.
    pub dead_letter_queue_source_arn: Option<String>,
}

/// A message as returned by `ReceiveMessage`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceivedMessage {
    /// SQS message id.
    pub message_id: String,
    /// Receipt handle for this receive.
    pub receipt_handle: String,
    /// The raw body.
    pub body: String,
    /// MD5 of the body as computed by SQS.
    pub md5_of_body: Option<String>,
    /// Message attributes.
    pub attributes: MessageAttributes,
    /// MD5 of the message attributes as computed by SQS.
    pub md5_of_message_attributes: Option<String>,
    /// System attributes.
    pub system: SystemAttributes,
}

/// Parameters for `ReceiveMessage`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiveRequest {
    /// Queue URL.
    pub queue_url: String,
    /// 1..=10.
    pub max_messages: u8,
    /// Long polling wait time (0..=20 seconds).
    pub wait_time: Duration,
    /// Visibility timeout to apply to received messages (queue default if `None`).
    pub visibility_timeout: Option<Duration>,
    /// FIFO receive request attempt id (makes retried receives idempotent).
    pub attempt_id: Option<String>,
}

/// Parameters for `SendMessage` / one `SendMessageBatch` entry.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SendRequest {
    /// Body (must be SQS-safe text).
    pub body: String,
    /// Message attributes.
    pub attributes: MessageAttributes,
    /// Per-message delay (standard queues only).
    pub delay: Option<Duration>,
    /// FIFO message group id.
    pub group_id: Option<String>,
    /// FIFO deduplication id.
    pub deduplication_id: Option<String>,
    /// X-Ray trace header sent as the `AWSTraceHeader` system attribute.
    pub trace_header: Option<String>,
}

/// Result of a successful send.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SendOutput {
    /// SQS message id.
    pub message_id: String,
    /// FIFO sequence number.
    pub sequence_number: Option<String>,
    /// MD5 of the body as computed by SQS.
    pub md5_of_body: Option<String>,
    /// MD5 of the attributes as computed by SQS.
    pub md5_of_message_attributes: Option<String>,
}

/// A failed batch entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchEntryError {
    /// Error code.
    pub code: String,
    /// Error message.
    pub message: Option<String>,
    /// `true` when the caller is at fault (retrying unchanged will not help).
    pub sender_fault: bool,
}

impl BatchEntryError {
    /// Whether retrying this entry may succeed.
    pub fn is_retryable(&self) -> bool {
        !self.sender_fault
    }
}

/// Per-entry results of a batch call, in request order.
pub type BatchResult<T> = Vec<std::result::Result<T, BatchEntryError>>;

/// One `ChangeMessageVisibilityBatch` entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VisibilityChange {
    /// Receipt handle.
    pub receipt_handle: String,
    /// New visibility timeout (from now).
    pub timeout: Duration,
}

/// SQS operations used by fanferro.
#[async_trait]
pub trait SqsApi: Send + Sync + Debug {
    /// `SendMessage`.
    async fn send_message(&self, queue_url: &str, request: SendRequest) -> Result<SendOutput>;
    /// `SendMessageBatch` (1..=10 entries).
    async fn send_message_batch(&self, queue_url: &str, entries: Vec<SendRequest>) -> Result<BatchResult<SendOutput>>;
    /// `ReceiveMessage` requesting all message and system attributes.
    async fn receive_messages(&self, request: ReceiveRequest) -> Result<Vec<ReceivedMessage>>;
    /// `DeleteMessageBatch` (1..=10 receipt handles).
    async fn delete_message_batch(&self, queue_url: &str, receipt_handles: Vec<String>) -> Result<BatchResult<()>>;
    /// `ChangeMessageVisibilityBatch` (1..=10 entries).
    async fn change_visibility_batch(&self, queue_url: &str, entries: Vec<VisibilityChange>)
    -> Result<BatchResult<()>>;
    /// `GetQueueUrl`.
    async fn get_queue_url(&self, name: &str, owner_account_id: Option<&str>) -> Result<String>;
    /// `GetQueueAttributes` with `All`.
    async fn get_queue_attributes(&self, queue_url: &str) -> Result<StringMap>;
    /// `CreateQueue`; returns the queue URL.
    async fn create_queue(&self, name: &str, attributes: &StringMap, tags: &StringMap) -> Result<String>;
    /// `SetQueueAttributes`.
    async fn set_queue_attributes(&self, queue_url: &str, attributes: &StringMap) -> Result<()>;
    /// `ListQueueTags`.
    async fn list_queue_tags(&self, queue_url: &str) -> Result<StringMap>;
    /// `TagQueue`.
    async fn tag_queue(&self, queue_url: &str, tags: &StringMap) -> Result<()>;
    /// `DeleteQueue`.
    async fn delete_queue(&self, queue_url: &str) -> Result<()>;
    /// `PurgeQueue`.
    async fn purge_queue(&self, queue_url: &str) -> Result<()>;
}

/// Parameters for `Publish` / one `PublishBatch` entry.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PublishRequest {
    /// Message (must be SNS-safe text).
    pub message: String,
    /// Message attributes.
    pub attributes: MessageAttributes,
    /// Optional subject (used by email subscriptions; carried in notifications).
    pub subject: Option<String>,
    /// FIFO message group id.
    pub group_id: Option<String>,
    /// FIFO deduplication id.
    pub deduplication_id: Option<String>,
    /// X-Ray trace header (sent as the `X-Amzn-Trace-Id` HTTP header).
    pub trace_header: Option<String>,
}

/// The trace header of a `PublishBatch` call: SNS takes one per request, not one per entry, so
/// it is sent when every entry has the same one (the core splits batches by trace header when the
/// client's `split_batches_by_trace_header` is set).
pub(crate) fn shared_trace_header(entries: &[PublishRequest]) -> Option<&str> {
    let first = entries.first()?.trace_header.as_deref()?;
    entries.iter().all(|entry| entry.trace_header.as_deref() == Some(first)).then_some(first)
}

/// Result of a successful publish.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PublishOutput {
    /// SNS message id.
    pub message_id: String,
    /// FIFO sequence number.
    pub sequence_number: Option<String>,
}

/// A subscription as listed by `ListSubscriptionsByTopic`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionInfo {
    /// Subscription ARN (or `PendingConfirmation`).
    pub subscription_arn: String,
    /// Protocol, e.g. `sqs`.
    pub protocol: String,
    /// Endpoint, e.g. a queue ARN.
    pub endpoint: String,
    /// Topic ARN.
    pub topic_arn: String,
}

/// SNS operations used by fanferro.
#[async_trait]
pub trait SnsApi: Send + Sync + Debug {
    /// `Publish`.
    async fn publish(&self, topic_arn: &str, request: PublishRequest) -> Result<PublishOutput>;
    /// `PublishBatch` (1..=10 entries).
    async fn publish_batch(&self, topic_arn: &str, entries: Vec<PublishRequest>) -> Result<BatchResult<PublishOutput>>;
    /// `CreateTopic`; returns the topic ARN.
    async fn create_topic(&self, name: &str, attributes: &StringMap, tags: &StringMap) -> Result<String>;
    /// `GetTopicAttributes`.
    async fn get_topic_attributes(&self, topic_arn: &str) -> Result<StringMap>;
    /// `SetTopicAttributes` (one attribute per call, as the API requires).
    async fn set_topic_attribute(&self, topic_arn: &str, name: &str, value: &str) -> Result<()>;
    /// `ListTagsForResource`.
    async fn list_topic_tags(&self, topic_arn: &str) -> Result<StringMap>;
    /// `TagResource`.
    async fn tag_topic(&self, topic_arn: &str, tags: &StringMap) -> Result<()>;
    /// `DeleteTopic`.
    async fn delete_topic(&self, topic_arn: &str) -> Result<()>;
    /// `Subscribe` (returns the subscription ARN).
    async fn subscribe(
        &self,
        topic_arn: &str,
        protocol: &str,
        endpoint: &str,
        attributes: &StringMap,
    ) -> Result<String>;
    /// `ListSubscriptionsByTopic` (all pages).
    async fn list_subscriptions_by_topic(&self, topic_arn: &str) -> Result<Vec<SubscriptionInfo>>;
    /// `GetSubscriptionAttributes`.
    async fn get_subscription_attributes(&self, subscription_arn: &str) -> Result<StringMap>;
    /// `SetSubscriptionAttributes` (one attribute per call).
    async fn set_subscription_attribute(&self, subscription_arn: &str, name: &str, value: &str) -> Result<()>;
    /// `Unsubscribe`.
    async fn unsubscribe(&self, subscription_arn: &str) -> Result<()>;
    /// `ConfirmSubscription`; returns the subscription ARN.
    async fn confirm_subscription(&self, topic_arn: &str, token: &str) -> Result<String>;
}

/// Server-side encryption for stored payload objects.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ObjectEncryption {
    /// Bucket default.
    #[default]
    BucketDefault,
    /// SSE-S3 (`AES256`).
    S3Managed,
    /// SSE-KMS with an optional key id (bucket/AWS-managed key when `None`).
    Kms {
        /// KMS key id, ARN or alias.
        key_id: Option<String>,
        /// Use an S3 bucket key to reduce KMS calls.
        bucket_key: bool,
    },
}

/// Options for storing an object.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PutOptions {
    /// `Content-Type` of the object.
    pub content_type: Option<String>,
    /// Server-side encryption.
    pub encryption: ObjectEncryption,
    /// Storage class (e.g. `STANDARD`, `INTELLIGENT_TIERING`).
    pub storage_class: Option<String>,
    /// Object metadata.
    pub metadata: StringMap,
}

/// Object storage (S3) operations used for large payloads.
#[async_trait]
pub trait ObjectStoreApi: Send + Sync + Debug {
    /// `PutObject`.
    async fn put_object(&self, bucket: &str, key: &str, data: Bytes, options: &PutOptions) -> Result<()>;
    /// `GetObject`, failing if the object is larger than `max_bytes`.
    async fn get_object(&self, bucket: &str, key: &str, max_bytes: usize) -> Result<Bytes>;
    /// `DeleteObject`.
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()>;
    /// A presigned `GET` URL valid for `ttl`.
    async fn presign_get(&self, bucket: &str, key: &str, ttl: Duration) -> Result<String>;
    /// Whether the bucket exists and is accessible.
    async fn bucket_exists(&self, bucket: &str) -> Result<bool>;
    /// `CreateBucket`.
    async fn create_bucket(&self, bucket: &str) -> Result<()>;
}

/// Plain HTTP(S) `GET`, used to fetch payloads by presigned URL.
#[async_trait]
pub trait HttpFetch: Send + Sync + Debug {
    /// Downloads `url`, failing if the body exceeds `max_bytes`.
    async fn get(&self, url: &str, max_bytes: usize) -> Result<Bytes>;
}

/// Caller identity (used to build topic ARNs from names).
#[async_trait]
pub trait IdentityApi: Send + Sync + Debug {
    /// The account id of the configured credentials.
    async fn account_id(&self) -> Result<String>;
}

/// Which implementation backs a [`Backend`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    /// AWS or an AWS-compatible endpoint.
    Aws,
    /// The in-memory broker.
    Memory,
}

/// A complete set of service clients.
#[derive(Clone, Debug)]
pub struct Backend {
    /// Which implementation this is.
    pub kind: BackendKind,
    /// Region the clients are bound to.
    pub region: String,
    /// Custom endpoint (emulators), if any.
    pub endpoint_url: Option<String>,
    /// Whether a `ReceiveMessage` long poll that the client aborts goes on until its wait time
    /// ends and takes messages that arrive meanwhile (SQS does; the in-memory broker when asked
    /// to). The consumer engine then keeps the messages it releases while stopping invisible
    /// until such polls are over.
    pub orphaned_polls: bool,
    /// SQS.
    pub sqs: Arc<dyn SqsApi>,
    /// SNS.
    pub sns: Arc<dyn SnsApi>,
    /// S3.
    pub objects: Arc<dyn ObjectStoreApi>,
    /// HTTP fetcher for presigned URLs.
    pub http: Arc<dyn HttpFetch>,
    /// Caller identity.
    pub identity: Arc<dyn IdentityApi>,
}
