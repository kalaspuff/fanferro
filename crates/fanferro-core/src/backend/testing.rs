//! Test helpers for backends (compiled for tests only).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use bytes::Bytes;

use super::{
    BatchResult, ObjectStoreApi, PublishOutput, PublishRequest, PutOptions, ReceiveRequest, ReceivedMessage,
    SendOutput, SendRequest, SnsApi, SqsApi, StringMap, SubscriptionInfo, VisibilityChange,
};
use crate::error::{AwsError, AwsErrorKind, Result};

/// Calls in progress, and the most there were at once.
#[derive(Debug, Default)]
pub(crate) struct Overlap {
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl Overlap {
    /// The most calls that were in progress at once (reset to zero).
    pub(crate) fn take_peak(&self) -> usize {
        self.peak.swap(0, Ordering::SeqCst)
    }

    async fn around<T>(&self, delay: Duration, call: impl Future<Output = T>) -> T {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(delay).await;
        let result = call.await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        result
    }
}

/// S3 whose uploads take `delay` each (and that records their overlap).
#[derive(Debug)]
pub(crate) struct SlowUploads {
    inner: Arc<dyn ObjectStoreApi>,
    delay: Duration,
    /// `PutObject` calls.
    pub(crate) puts: Overlap,
}

impl SlowUploads {
    pub(crate) fn new(inner: Arc<dyn ObjectStoreApi>, delay: Duration) -> Self {
        Self { inner, delay, puts: Overlap::default() }
    }
}

#[async_trait]
impl ObjectStoreApi for SlowUploads {
    async fn put_object(&self, bucket: &str, key: &str, data: Bytes, options: &PutOptions) -> Result<()> {
        self.puts.around(self.delay, self.inner.put_object(bucket, key, data, options)).await
    }
    async fn get_object(&self, bucket: &str, key: &str, max_bytes: usize) -> Result<Bytes> {
        self.inner.get_object(bucket, key, max_bytes).await
    }
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.inner.delete_object(bucket, key).await
    }
    async fn presign_get(&self, bucket: &str, key: &str, ttl: Duration) -> Result<String> {
        self.inner.presign_get(bucket, key, ttl).await
    }
    async fn bucket_exists(&self, bucket: &str) -> Result<bool> {
        self.inner.bucket_exists(bucket).await
    }
    async fn create_bucket(&self, bucket: &str) -> Result<()> {
        self.inner.create_bucket(bucket).await
    }
}

/// S3 whose downloads take `delay` each (a large payload over a slow link).
#[derive(Debug)]
pub(crate) struct SlowDownloads {
    inner: Arc<dyn ObjectStoreApi>,
    delay: Duration,
    /// `GetObject` calls.
    pub(crate) gets: Overlap,
}

impl SlowDownloads {
    pub(crate) fn new(inner: Arc<dyn ObjectStoreApi>, delay: Duration) -> Self {
        Self { inner, delay, gets: Overlap::default() }
    }
}

#[async_trait]
impl ObjectStoreApi for SlowDownloads {
    async fn put_object(&self, bucket: &str, key: &str, data: Bytes, options: &PutOptions) -> Result<()> {
        self.inner.put_object(bucket, key, data, options).await
    }
    async fn get_object(&self, bucket: &str, key: &str, max_bytes: usize) -> Result<Bytes> {
        self.gets.around(self.delay, self.inner.get_object(bucket, key, max_bytes)).await
    }
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        self.inner.delete_object(bucket, key).await
    }
    async fn presign_get(&self, bucket: &str, key: &str, ttl: Duration) -> Result<String> {
        self.inner.presign_get(bucket, key, ttl).await
    }
    async fn bucket_exists(&self, bucket: &str) -> Result<bool> {
        self.inner.bucket_exists(bucket).await
    }
    async fn create_bucket(&self, bucket: &str) -> Result<()> {
        self.inner.create_bucket(bucket).await
    }
}

/// SQS whose first `stuck` visibility changes never complete (a connection that stopped
/// answering), or complete only after a delay (a slow connection: the change still applies).
#[derive(Debug)]
pub(crate) struct StuckVisibility {
    inner: Arc<dyn SqsApi>,
    stuck: parking_lot::Mutex<usize>,
    delay: Option<Duration>,
    /// Stuck or slow calls that started.
    pub(crate) started: std::sync::atomic::AtomicUsize,
}

impl StuckVisibility {
    pub(crate) fn new(inner: Arc<dyn SqsApi>, stuck: usize) -> Self {
        Self { inner, stuck: parking_lot::Mutex::new(stuck), delay: None, started: Default::default() }
    }

    /// The first `slow` visibility changes take `delay` before they reach `inner`.
    pub(crate) fn slow(inner: Arc<dyn SqsApi>, slow: usize, delay: Duration) -> Self {
        Self { delay: Some(delay), ..Self::new(inner, slow) }
    }
}

#[async_trait]
impl SqsApi for StuckVisibility {
    async fn send_message(&self, queue_url: &str, request: SendRequest) -> Result<SendOutput> {
        self.inner.send_message(queue_url, request).await
    }
    async fn send_message_batch(&self, queue_url: &str, entries: Vec<SendRequest>) -> Result<BatchResult<SendOutput>> {
        self.inner.send_message_batch(queue_url, entries).await
    }
    async fn receive_messages(&self, request: ReceiveRequest) -> Result<Vec<ReceivedMessage>> {
        self.inner.receive_messages(request).await
    }
    async fn delete_message_batch(&self, queue_url: &str, receipt_handles: Vec<String>) -> Result<BatchResult<()>> {
        self.inner.delete_message_batch(queue_url, receipt_handles).await
    }
    async fn change_visibility_batch(
        &self,
        queue_url: &str,
        entries: Vec<VisibilityChange>,
    ) -> Result<BatchResult<()>> {
        let stuck = {
            let mut stuck = self.stuck.lock();
            *stuck > 0 && {
                *stuck -= 1;
                true
            }
        };
        if stuck {
            self.started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            match self.delay {
                Some(delay) => tokio::time::sleep(delay).await,
                None => std::future::pending::<()>().await,
            }
        }
        self.inner.change_visibility_batch(queue_url, entries).await
    }
    async fn get_queue_url(&self, name: &str, owner_account_id: Option<&str>) -> Result<String> {
        self.inner.get_queue_url(name, owner_account_id).await
    }
    async fn get_queue_attributes(&self, queue_url: &str) -> Result<StringMap> {
        self.inner.get_queue_attributes(queue_url).await
    }
    async fn create_queue(&self, name: &str, attributes: &StringMap, tags: &StringMap) -> Result<String> {
        self.inner.create_queue(name, attributes, tags).await
    }
    async fn set_queue_attributes(&self, queue_url: &str, attributes: &StringMap) -> Result<()> {
        self.inner.set_queue_attributes(queue_url, attributes).await
    }
    async fn list_queue_tags(&self, queue_url: &str) -> Result<StringMap> {
        self.inner.list_queue_tags(queue_url).await
    }
    async fn tag_queue(&self, queue_url: &str, tags: &StringMap) -> Result<()> {
        self.inner.tag_queue(queue_url, tags).await
    }
    async fn delete_queue(&self, queue_url: &str) -> Result<()> {
        self.inner.delete_queue(queue_url).await
    }
    async fn purge_queue(&self, queue_url: &str) -> Result<()> {
        self.inner.purge_queue(queue_url).await
    }
}

/// SQS whose receives take `delay` (a network round trip) before they reach `inner`, and that
/// records their overlap.
#[derive(Debug)]
pub(crate) struct SlowReceives {
    inner: Arc<dyn SqsApi>,
    delay: Duration,
    /// `ReceiveMessage` calls.
    pub(crate) receives: Overlap,
}

impl SlowReceives {
    pub(crate) fn new(inner: Arc<dyn SqsApi>, delay: Duration) -> Self {
        Self { inner, delay, receives: Overlap::default() }
    }
}

impl Overlap {
    /// Calls in progress now.
    pub(crate) fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl SqsApi for SlowReceives {
    async fn send_message(&self, queue_url: &str, request: SendRequest) -> Result<SendOutput> {
        self.inner.send_message(queue_url, request).await
    }
    async fn send_message_batch(&self, queue_url: &str, entries: Vec<SendRequest>) -> Result<BatchResult<SendOutput>> {
        self.inner.send_message_batch(queue_url, entries).await
    }
    async fn receive_messages(&self, request: ReceiveRequest) -> Result<Vec<ReceivedMessage>> {
        self.receives.around(self.delay, self.inner.receive_messages(request)).await
    }
    async fn delete_message_batch(&self, queue_url: &str, receipt_handles: Vec<String>) -> Result<BatchResult<()>> {
        self.inner.delete_message_batch(queue_url, receipt_handles).await
    }
    async fn change_visibility_batch(
        &self,
        queue_url: &str,
        entries: Vec<VisibilityChange>,
    ) -> Result<BatchResult<()>> {
        self.inner.change_visibility_batch(queue_url, entries).await
    }
    async fn get_queue_url(&self, name: &str, owner_account_id: Option<&str>) -> Result<String> {
        self.inner.get_queue_url(name, owner_account_id).await
    }
    async fn get_queue_attributes(&self, queue_url: &str) -> Result<StringMap> {
        self.inner.get_queue_attributes(queue_url).await
    }
    async fn create_queue(&self, name: &str, attributes: &StringMap, tags: &StringMap) -> Result<String> {
        self.inner.create_queue(name, attributes, tags).await
    }
    async fn set_queue_attributes(&self, queue_url: &str, attributes: &StringMap) -> Result<()> {
        self.inner.set_queue_attributes(queue_url, attributes).await
    }
    async fn list_queue_tags(&self, queue_url: &str) -> Result<StringMap> {
        self.inner.list_queue_tags(queue_url).await
    }
    async fn tag_queue(&self, queue_url: &str, tags: &StringMap) -> Result<()> {
        self.inner.tag_queue(queue_url, tags).await
    }
    async fn delete_queue(&self, queue_url: &str) -> Result<()> {
        self.inner.delete_queue(queue_url).await
    }
    async fn purge_queue(&self, queue_url: &str) -> Result<()> {
        self.inner.purge_queue(queue_url).await
    }
}

/// SQS whose first `failures` batch sends fail with a retryable (throttling) error.
#[derive(Debug)]
pub(crate) struct ThrottledBatches {
    inner: Arc<dyn SqsApi>,
    failures: parking_lot::Mutex<usize>,
}

impl ThrottledBatches {
    pub(crate) fn new(inner: Arc<dyn SqsApi>, failures: usize) -> Self {
        Self { inner, failures: parking_lot::Mutex::new(failures) }
    }

    fn take_failure(&self) -> bool {
        let mut failures = self.failures.lock();
        *failures > 0 && {
            *failures -= 1;
            true
        }
    }
}

#[async_trait]
impl SqsApi for ThrottledBatches {
    async fn send_message(&self, queue_url: &str, request: SendRequest) -> Result<SendOutput> {
        self.inner.send_message(queue_url, request).await
    }
    async fn send_message_batch(&self, queue_url: &str, entries: Vec<SendRequest>) -> Result<BatchResult<SendOutput>> {
        if self.take_failure() {
            return Err(AwsError::new("sqs", "SendMessageBatch", AwsErrorKind::Throttling, "Rate exceeded").into());
        }
        self.inner.send_message_batch(queue_url, entries).await
    }
    async fn receive_messages(&self, request: ReceiveRequest) -> Result<Vec<ReceivedMessage>> {
        self.inner.receive_messages(request).await
    }
    async fn delete_message_batch(&self, queue_url: &str, receipt_handles: Vec<String>) -> Result<BatchResult<()>> {
        self.inner.delete_message_batch(queue_url, receipt_handles).await
    }
    async fn change_visibility_batch(
        &self,
        queue_url: &str,
        entries: Vec<VisibilityChange>,
    ) -> Result<BatchResult<()>> {
        self.inner.change_visibility_batch(queue_url, entries).await
    }
    async fn get_queue_url(&self, name: &str, owner_account_id: Option<&str>) -> Result<String> {
        self.inner.get_queue_url(name, owner_account_id).await
    }
    async fn get_queue_attributes(&self, queue_url: &str) -> Result<StringMap> {
        self.inner.get_queue_attributes(queue_url).await
    }
    async fn create_queue(&self, name: &str, attributes: &StringMap, tags: &StringMap) -> Result<String> {
        self.inner.create_queue(name, attributes, tags).await
    }
    async fn set_queue_attributes(&self, queue_url: &str, attributes: &StringMap) -> Result<()> {
        self.inner.set_queue_attributes(queue_url, attributes).await
    }
    async fn list_queue_tags(&self, queue_url: &str) -> Result<StringMap> {
        self.inner.list_queue_tags(queue_url).await
    }
    async fn tag_queue(&self, queue_url: &str, tags: &StringMap) -> Result<()> {
        self.inner.tag_queue(queue_url, tags).await
    }
    async fn delete_queue(&self, queue_url: &str) -> Result<()> {
        self.inner.delete_queue(queue_url).await
    }
    async fn purge_queue(&self, queue_url: &str) -> Result<()> {
        self.inner.purge_queue(queue_url).await
    }
}

/// SQS whose batch sends and batch deletes take `delay` each (and that records their overlap).
#[derive(Debug)]
pub(crate) struct SlowBatches {
    inner: Arc<dyn SqsApi>,
    delay: Duration,
    /// `SendMessageBatch` calls.
    pub(crate) sends: Overlap,
    /// `DeleteMessageBatch` calls.
    pub(crate) deletes: Overlap,
}

impl SlowBatches {
    pub(crate) fn new(inner: Arc<dyn SqsApi>, delay: Duration) -> Self {
        Self { inner, delay, sends: Overlap::default(), deletes: Overlap::default() }
    }
}

#[async_trait]
impl SqsApi for SlowBatches {
    async fn send_message(&self, queue_url: &str, request: SendRequest) -> Result<SendOutput> {
        self.inner.send_message(queue_url, request).await
    }
    async fn send_message_batch(&self, queue_url: &str, entries: Vec<SendRequest>) -> Result<BatchResult<SendOutput>> {
        self.sends.around(self.delay, self.inner.send_message_batch(queue_url, entries)).await
    }
    async fn receive_messages(&self, request: ReceiveRequest) -> Result<Vec<ReceivedMessage>> {
        self.inner.receive_messages(request).await
    }
    async fn delete_message_batch(&self, queue_url: &str, receipt_handles: Vec<String>) -> Result<BatchResult<()>> {
        self.deletes.around(self.delay, self.inner.delete_message_batch(queue_url, receipt_handles)).await
    }
    async fn change_visibility_batch(
        &self,
        queue_url: &str,
        entries: Vec<VisibilityChange>,
    ) -> Result<BatchResult<()>> {
        self.inner.change_visibility_batch(queue_url, entries).await
    }
    async fn get_queue_url(&self, name: &str, owner_account_id: Option<&str>) -> Result<String> {
        self.inner.get_queue_url(name, owner_account_id).await
    }
    async fn get_queue_attributes(&self, queue_url: &str) -> Result<StringMap> {
        self.inner.get_queue_attributes(queue_url).await
    }
    async fn create_queue(&self, name: &str, attributes: &StringMap, tags: &StringMap) -> Result<String> {
        self.inner.create_queue(name, attributes, tags).await
    }
    async fn set_queue_attributes(&self, queue_url: &str, attributes: &StringMap) -> Result<()> {
        self.inner.set_queue_attributes(queue_url, attributes).await
    }
    async fn list_queue_tags(&self, queue_url: &str) -> Result<StringMap> {
        self.inner.list_queue_tags(queue_url).await
    }
    async fn tag_queue(&self, queue_url: &str, tags: &StringMap) -> Result<()> {
        self.inner.tag_queue(queue_url, tags).await
    }
    async fn delete_queue(&self, queue_url: &str) -> Result<()> {
        self.inner.delete_queue(queue_url).await
    }
    async fn purge_queue(&self, queue_url: &str) -> Result<()> {
        self.inner.purge_queue(queue_url).await
    }
}

/// SNS whose first `failures` batch publishes fail with a retryable (throttling) error, and that
/// records the trace header of every `PublishBatch` call that reached `inner`.
#[derive(Debug)]
pub(crate) struct ThrottledPublishBatches {
    inner: Arc<dyn SnsApi>,
    failures: parking_lot::Mutex<usize>,
    /// The trace headers of the entries of each call that reached `inner`.
    pub(crate) calls: parking_lot::Mutex<Vec<Vec<Option<String>>>>,
}

impl ThrottledPublishBatches {
    pub(crate) fn new(inner: Arc<dyn SnsApi>, failures: usize) -> Self {
        Self { inner, failures: parking_lot::Mutex::new(failures), calls: parking_lot::Mutex::default() }
    }
}

#[async_trait]
impl SnsApi for ThrottledPublishBatches {
    async fn publish(&self, topic_arn: &str, request: PublishRequest) -> Result<PublishOutput> {
        self.inner.publish(topic_arn, request).await
    }
    async fn publish_batch(&self, topic_arn: &str, entries: Vec<PublishRequest>) -> Result<BatchResult<PublishOutput>> {
        let fail = {
            let mut failures = self.failures.lock();
            *failures > 0 && {
                *failures -= 1;
                true
            }
        };
        if fail {
            return Err(AwsError::new("sns", "PublishBatch", AwsErrorKind::Throttling, "Rate exceeded").into());
        }
        self.calls.lock().push(entries.iter().map(|entry| entry.trace_header.clone()).collect());
        self.inner.publish_batch(topic_arn, entries).await
    }
    async fn create_topic(&self, name: &str, attributes: &StringMap, tags: &StringMap) -> Result<String> {
        self.inner.create_topic(name, attributes, tags).await
    }
    async fn get_topic_attributes(&self, topic_arn: &str) -> Result<StringMap> {
        self.inner.get_topic_attributes(topic_arn).await
    }
    async fn set_topic_attribute(&self, topic_arn: &str, name: &str, value: &str) -> Result<()> {
        self.inner.set_topic_attribute(topic_arn, name, value).await
    }
    async fn list_topic_tags(&self, topic_arn: &str) -> Result<StringMap> {
        self.inner.list_topic_tags(topic_arn).await
    }
    async fn tag_topic(&self, topic_arn: &str, tags: &StringMap) -> Result<()> {
        self.inner.tag_topic(topic_arn, tags).await
    }
    async fn delete_topic(&self, topic_arn: &str) -> Result<()> {
        self.inner.delete_topic(topic_arn).await
    }
    async fn subscribe(
        &self,
        topic_arn: &str,
        protocol: &str,
        endpoint: &str,
        attributes: &StringMap,
    ) -> Result<String> {
        self.inner.subscribe(topic_arn, protocol, endpoint, attributes).await
    }
    async fn list_subscriptions_by_topic(&self, topic_arn: &str) -> Result<Vec<SubscriptionInfo>> {
        self.inner.list_subscriptions_by_topic(topic_arn).await
    }
    async fn get_subscription_attributes(&self, subscription_arn: &str) -> Result<StringMap> {
        self.inner.get_subscription_attributes(subscription_arn).await
    }
    async fn set_subscription_attribute(&self, subscription_arn: &str, name: &str, value: &str) -> Result<()> {
        self.inner.set_subscription_attribute(subscription_arn, name, value).await
    }
    async fn unsubscribe(&self, subscription_arn: &str) -> Result<()> {
        self.inner.unsubscribe(subscription_arn).await
    }
    async fn confirm_subscription(&self, topic_arn: &str, token: &str) -> Result<String> {
        self.inner.confirm_subscription(topic_arn, token).await
    }
}
