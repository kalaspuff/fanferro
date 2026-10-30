//! Receive loops.

use std::sync::Arc;

use tokio::sync::OwnedSemaphorePermit;

use crate::backend::{ReceiveRequest, ReceivedMessage, VisibilityChange};
use crate::events::{Event, EventKind};
use crate::retry::sleep_or_cancel;

use super::EngineInner;

/// One receive loop. Runs until the engine's receive token is cancelled.
pub(crate) async fn poll_loop(inner: Arc<EngineInner>) {
    let mut consecutive_failures: u32 = 0;
    loop {
        if inner.receive_token.is_cancelled() || !inner.wait_unpaused().await {
            break;
        }
        let Some(mut permits) = acquire_permits(&inner).await else {
            break;
        };
        if let Some(limiter) = &inner.rate_limiter {
            let wanted = u32::try_from(permits.len()).unwrap_or(10);
            match limiter.acquire_up_to(wanted, &inner.receive_token).await {
                Some(granted) => permits.truncate(granted as usize),
                None => break,
            }
        }
        let requested = permits.len();
        let request = ReceiveRequest {
            queue_url: inner.settings.queue_url.clone(),
            max_messages: u8::try_from(requested).unwrap_or(10),
            wait_time: inner.settings.wait_time,
            visibility_timeout: Some(inner.settings.visibility_timeout),
            attempt_id: inner.settings.fifo.then(|| uuid::Uuid::new_v4().to_string()),
        };
        let Some(receiving) = inner.begin_receive() else {
            if let Some(limiter) = &inner.rate_limiter {
                limiter.refund(u32::try_from(requested).unwrap_or(0));
            }
            break;
        };
        inner.metrics.receive_requests.inc();
        let result = tokio::select! {
            () = inner.abort_token.cancelled() => {
                if let Some(limiter) = &inner.rate_limiter {
                    limiter.refund(u32::try_from(requested).unwrap_or(0));
                }
                break;
            }
            result = inner.client.backend().sqs.receive_messages(request) => result,
        };
        drop(receiving);
        if inner.receive_token.is_cancelled() {
            // Receiving stopped while this poll was in flight: hand its messages straight back.
            if let Ok(messages) = result
                && !messages.is_empty()
            {
                release_now(&inner, messages).await;
            }
            break;
        }
        match result {
            Ok(mut messages) => {
                consecutive_failures = 0;
                if let Some(limiter) = &inner.rate_limiter {
                    limiter.refund(u32::try_from(requested.saturating_sub(messages.len())).unwrap_or(0));
                }
                if messages.is_empty() {
                    inner.metrics.empty_receives.inc();
                    inner.client.events().emit(EventKind::ReceiveEmpty, || {
                        Event::new(EventKind::ReceiveEmpty, "no messages received")
                            .field("queue", &inner.settings.queue_url)
                    });
                    continue;
                }
                if messages.len() > requested {
                    // Never hold more than we have permits for: hand extras straight back.
                    release_now(&inner, messages.split_off(requested)).await;
                }
                inner.client.events().emit(EventKind::ReceiveSucceeded, || {
                    Event::new(EventKind::ReceiveSucceeded, "messages received")
                        .field("queue", &inner.settings.queue_url)
                        .field("count", messages.len())
                });
                permits.truncate(messages.len());
                inner.accept(messages, permits);
            }
            Err(error) => {
                drop(permits);
                if let Some(limiter) = &inner.rate_limiter {
                    limiter.refund(u32::try_from(requested).unwrap_or(0));
                }
                inner.metrics.receive_errors.inc();
                let delay = inner.settings.receive_backoff.delay(consecutive_failures);
                consecutive_failures = consecutive_failures.saturating_add(1);
                inner.client.events().emit(EventKind::ReceiveFailed, || {
                    Event::new(EventKind::ReceiveFailed, "receive failed; backing off")
                        .field("queue", &inner.settings.queue_url)
                        .field("consecutive_failures", consecutive_failures)
                        .field("retry_in_seconds", delay.as_secs_f64())
                        .error(&error)
                });
                if !sleep_or_cancel(delay, &inner.receive_token).await {
                    break;
                }
            }
        }
    }
}

/// In-flight permits for the next receive, one per requested message: waits for a free slot,
/// then up to `receive_batch_wait` until `min_receive_batch` slots are free (so receives stay
/// full instead of asking for whatever single slot just became free), then takes every other
/// free slot up to `batch_size`. Returns `None` once receiving stops.
async fn acquire_permits(inner: &EngineInner) -> Option<Vec<OwnedSemaphorePermit>> {
    let settings = &inner.settings;
    let batch_size = usize::from(settings.batch_size);
    let first = tokio::select! {
        () = inner.receive_token.cancelled() => return None,
        permit = inner.permits.clone().acquire_owned() => permit.ok()?,
    };
    let mut permits = Vec::with_capacity(batch_size);
    permits.push(first);
    if settings.min_receive_batch > 1 {
        let rest = u32::try_from(settings.min_receive_batch - 1).unwrap_or(u32::MAX);
        tokio::select! {
            () = inner.receive_token.cancelled() => return None,
            more = inner.permits.clone().acquire_many_owned(rest) => {
                if let Ok(mut more) = more {
                    while let Some(permit) = more.split(1) {
                        permits.push(permit);
                    }
                }
            }
            // Slow handlers must not hold back the slots that are free (head-of-line blocking).
            () = tokio::time::sleep(settings.receive_batch_wait) => {}
        }
    }
    while permits.len() < batch_size {
        match inner.permits.clone().try_acquire_owned() {
            Ok(permit) => permits.push(permit),
            Err(_) => break,
        }
    }
    Some(permits)
}

/// Makes messages visible again right away (in batches of ten), or once an aborted long poll of
/// another poller is over.
async fn release_now(inner: &EngineInner, messages: Vec<ReceivedMessage>) {
    let timeout = inner.visibility_floor(std::time::Duration::ZERO);
    let mut entries: Vec<VisibilityChange> =
        messages.into_iter().map(|m| VisibilityChange { receipt_handle: m.receipt_handle, timeout }).collect();
    while !entries.is_empty() {
        let rest = entries.split_off(entries.len().min(crate::publish::MAX_BATCH_ENTRIES));
        let count = entries.len();
        if inner.client.backend().sqs.change_visibility_batch(&inner.settings.queue_url, entries).await.is_ok() {
            inner.metrics.messages_released.add(count as u64);
        }
        entries = rest;
    }
}
