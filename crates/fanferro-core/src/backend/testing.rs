//! Test helpers for backends (compiled for tests only).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use bytes::Bytes;

use super::{
    BatchResult, ObjectStoreApi, PutOptions, ReceiveRequest, ReceivedMessage, SendOutput, SendRequest, SqsApi,
    StringMap, VisibilityChange,
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

/// SQS whose first `stuck` visibility changes never complete (a connection that stopped answering).
#[derive(Debug)]
pub(crate) struct StuckVisibility {
    inner: Arc<dyn SqsApi>,
    stuck: parking_lot::Mutex<usize>,
}

impl StuckVisibility {
    pub(crate) fn new(inner: Arc<dyn SqsApi>, stuck: usize) -> Self {
        Self { inner, stuck: parking_lot::Mutex::new(stuck) }
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
            std::future::pending::<()>().await;
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
