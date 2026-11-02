//! Tests of the settle flush: how failing deletes are retried and reported.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;

use super::*;
use crate::backend::memory::MemoryBroker;
use crate::backend::testing::{DeleteFailure, FailingDeletes};
use crate::backend::{ReceiveRequest, SendRequest, StringMap};
use crate::client::ClientConfig;
use crate::error::AwsErrorKind;

struct Fixture {
    broker: MemoryBroker,
    deletes: Arc<FailingDeletes>,
    context: SettleContext,
}

impl Fixture {
    /// A queue whose deletes fail as listed (one failure per call), settled without linger.
    async fn new(failures: impl IntoIterator<Item = DeleteFailure>) -> Self {
        let broker = MemoryBroker::new("us-east-1", "000000000000");
        let mut backend = broker.backend();
        let deletes = Arc::new(FailingDeletes::new(backend.sqs.clone(), failures));
        backend.sqs = deletes.clone();
        let client = Client::with_backend(backend, ClientConfig::default()).unwrap();
        let queue_url = client.backend().sqs.create_queue("work", &StringMap::new(), &StringMap::new()).await.unwrap();
        let context = SettleContext {
            client,
            queue_url,
            metrics: Arc::new(ConsumerMetrics::default()),
            linger: Duration::ZERO,
            retry: RetrySettings { max_attempts: 3, ..RetrySettings::default() },
            backoff: Backoff::new(Duration::from_millis(100), Duration::from_secs(1)),
        };
        Self { broker, deletes, context }
    }

    /// Sends and receives `count` messages; returns their receipt handles.
    async fn received(&self, count: usize) -> Vec<String> {
        let sqs = &self.context.client.backend().sqs;
        for index in 0..count {
            let request = SendRequest { body: format!("m{index}"), ..SendRequest::default() };
            sqs.send_message(&self.context.queue_url, request).await.unwrap();
        }
        let received = sqs
            .receive_messages(ReceiveRequest {
                queue_url: self.context.queue_url.clone(),
                max_messages: u8::try_from(count).unwrap(),
                wait_time: Duration::ZERO,
                visibility_timeout: Some(Duration::from_secs(60)),
                attempt_id: None,
            })
            .await
            .unwrap();
        assert_eq!(received.len(), count);
        received.into_iter().map(|message| message.receipt_handle).collect()
    }

    /// Flushes deletes of `receipt_handles`; returns each one's outcome.
    async fn delete(&self, receipt_handles: Vec<String>) -> Vec<Result<()>> {
        let (commands, replies): (Vec<_>, Vec<_>) = receipt_handles
            .into_iter()
            .map(|receipt_handle| {
                let (sender, receiver) = oneshot::channel();
                (SettleCommand::new(receipt_handle, SettleKind::Delete, Some(Reply::Channel(sender))), receiver)
            })
            .unzip();
        flush(&self.context, commands).await;
        let mut results = Vec::new();
        for reply in replies {
            results.push(reply.await.expect("every command gets a reply"));
        }
        results
    }

    fn stored(&self) -> usize {
        self.broker.queue_messages(&self.context.queue_url).unwrap().len()
    }

    fn calls(&self) -> Vec<usize> {
        self.deletes.calls.lock().clone()
    }
}

fn kind(result: &Result<()>) -> AwsErrorKind {
    result.as_ref().unwrap_err().aws().expect("an AWS error").kind
}

#[tokio::test(start_paused = true)]
async fn a_failed_delete_call_is_retried() {
    let fx = Fixture::new([DeleteFailure::Throttled]).await;
    let handles = fx.received(3).await;
    let results = fx.delete(handles).await;
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(fx.calls(), [3, 3]);
    assert_eq!(fx.stored(), 0);
    assert_eq!(fx.context.metrics.delete_requests.get(), 2);
}

#[tokio::test(start_paused = true)]
async fn failed_entries_are_retried_when_retrying_may_help() {
    let fx = Fixture::new([DeleteFailure::Internal, DeleteFailure::Internal]).await;
    let handles = fx.received(2).await;
    let results = fx.delete(handles).await;
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(fx.calls(), [2, 2, 2]);
    assert_eq!(fx.stored(), 0);
}

#[tokio::test(start_paused = true)]
async fn retries_end_after_the_last_attempt() {
    for failure in [DeleteFailure::Throttled, DeleteFailure::Internal] {
        let fx = Fixture::new([failure; 3]).await;
        let handles = fx.received(2).await;
        let results = fx.delete(handles).await;
        let expected =
            if failure == DeleteFailure::Throttled { AwsErrorKind::Throttling } else { AwsErrorKind::Transient };
        assert!(results.iter().all(|result| kind(result) == expected), "{results:?}");
        assert_eq!(fx.calls(), [2, 2, 2], "{failure:?}");
        assert_eq!(fx.stored(), 2);
    }
}

#[tokio::test(start_paused = true)]
async fn failures_retrying_cannot_fix_are_reported_at_once() {
    let fx = Fixture::new([DeleteFailure::InvalidReceipt]).await;
    let handles = fx.received(2).await;
    let results = fx.delete(handles).await;
    assert!(results.iter().all(|result| kind(result) == AwsErrorKind::ReceiptHandleInvalid), "{results:?}");
    assert_eq!(fx.calls(), [2]);

    let fx = Fixture::new([DeleteFailure::Denied]).await;
    let handles = fx.received(2).await;
    let results = fx.delete(handles).await;
    assert!(results.iter().all(|result| kind(result) == AwsErrorKind::AccessDenied), "{results:?}");
    assert_eq!(fx.calls(), [2]);
    assert_eq!(fx.stored(), 2);
}
