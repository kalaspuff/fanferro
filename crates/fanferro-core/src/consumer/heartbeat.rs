//! Visibility heartbeats: extend leases before they expire, in batches of ten.

use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use tokio::time::Instant;

use super::EngineInner;
use super::config::MAX_VISIBILITY;
use super::lease::{LeaseStatus, is_lost};
use crate::backend::VisibilityChange;
use crate::events::{Event, EventKind};
use crate::publish::MAX_BATCH_ENTRIES;
use crate::retry::sleep_or_cancel;

const RETRY_SOON: Duration = Duration::from_secs(1);
const SAFETY_MARGIN: Duration = Duration::from_secs(5);

struct Due {
    id: u64,
    receipt_handle: String,
    message_id: String,
    timeout: Duration,
}

/// Runs until the engine shuts down.
pub(crate) async fn heartbeat_loop(inner: Arc<EngineInner>) {
    loop {
        if !sleep_or_cancel(inner.settings.heartbeat_tick, &inner.shutdown_token).await {
            break;
        }
        let now = Instant::now();
        let mut due = Vec::new();
        let mut expired = Vec::new();
        {
            let mut leases = inner.leases.lock();
            for (id, lease) in leases.iter_mut() {
                if !matches!(lease.status, LeaseStatus::Buffered | LeaseStatus::Active)
                    || lease.extending
                    || lease.next_heartbeat > now
                {
                    continue;
                }
                let hard_cap =
                    (lease.received_at + MAX_VISIBILITY).saturating_duration_since(now).saturating_sub(SAFETY_MARGIN);
                let remaining = lease.max_until.saturating_duration_since(now).min(hard_cap);
                if remaining.is_zero() {
                    lease.status = LeaseStatus::Expired;
                    expired.push(lease.message_id.clone());
                    continue;
                }
                lease.extending = true;
                due.push(Due {
                    id: *id,
                    receipt_handle: lease.receipt_handle.clone(),
                    message_id: lease.message_id.clone(),
                    timeout: inner.settings.extend_by.min(remaining),
                });
            }
        }
        for message_id in expired {
            inner.metrics.leases_expired.inc();
            inner.client.events().emit(EventKind::LeaseExpired, || {
                Event::new(
                    EventKind::LeaseExpired,
                    "maximum lease duration reached; the message will become visible again",
                )
                .field("queue", &inner.settings.queue_url)
                .field("sqs_message_id", message_id)
            });
        }
        if due.is_empty() {
            continue;
        }
        let mut chunks = Vec::new();
        while !due.is_empty() {
            let rest = due.split_off(due.len().min(MAX_BATCH_ENTRIES));
            chunks.push(std::mem::replace(&mut due, rest));
        }
        join_all(chunks.into_iter().map(|chunk| extend_chunk(&inner, chunk))).await;
    }
}

async fn extend_chunk(inner: &Arc<EngineInner>, chunk: Vec<Due>) {
    let started = Instant::now();
    let entries = chunk
        .iter()
        .map(|due| VisibilityChange { receipt_handle: due.receipt_handle.clone(), timeout: due.timeout })
        .collect();
    let result = inner.client.backend().sqs.change_visibility_batch(&inner.settings.queue_url, entries).await;
    let mut leases = inner.leases.lock();
    match result {
        Ok(results) => {
            for (due, outcome) in chunk.into_iter().zip(results) {
                let Some(lease) = leases.get_mut(&due.id) else { continue };
                lease.extending = false;
                match outcome {
                    Ok(()) => {
                        lease.visible_until = started + due.timeout;
                        lease.next_heartbeat = started + inner.settings.heartbeat_interval.min(due.timeout / 2);
                        inner.metrics.lease_extensions.inc();
                        inner.client.events().emit(EventKind::LeaseExtended, || {
                            Event::new(EventKind::LeaseExtended, "visibility extended")
                                .field("queue", &inner.settings.queue_url)
                                .field("sqs_message_id", &due.message_id)
                                .field("visibility_seconds", due.timeout.as_secs_f64())
                        });
                    }
                    Err(entry) => {
                        let error = crate::error::Error::from(
                            crate::error::AwsError::new(
                                "sqs",
                                "ChangeMessageVisibilityBatch",
                                crate::backend::aws::classify(Some(&entry.code), entry.message.as_deref(), None),
                                entry.message.clone().unwrap_or_default(),
                            )
                            .with_code(entry.code.clone()),
                        );
                        if is_lost(&error) && matches!(lease.status, LeaseStatus::Buffered | LeaseStatus::Active) {
                            lease.status = LeaseStatus::Lost;
                            inner.metrics.leases_lost.inc();
                            inner.client.events().emit(EventKind::LeaseLost, || {
                                Event::new(
                                    EventKind::LeaseLost,
                                    "receipt handle is no longer valid; the message may be processed twice",
                                )
                                .field("queue", &inner.settings.queue_url)
                                .field("sqs_message_id", &due.message_id)
                                .error(&error)
                            });
                        } else {
                            lease.next_heartbeat = Instant::now() + RETRY_SOON;
                            inner.metrics.lease_extension_failures.inc();
                            inner.client.events().emit(EventKind::LeaseExtensionFailed, || {
                                Event::new(EventKind::LeaseExtensionFailed, "failed to extend visibility; retrying")
                                    .field("queue", &inner.settings.queue_url)
                                    .field("sqs_message_id", &due.message_id)
                                    .error(&error)
                            });
                        }
                    }
                }
            }
        }
        Err(error) => {
            for due in chunk {
                if let Some(lease) = leases.get_mut(&due.id) {
                    lease.extending = false;
                    lease.next_heartbeat = Instant::now() + RETRY_SOON;
                }
                inner.metrics.lease_extension_failures.inc();
            }
            inner.client.events().emit(EventKind::LeaseExtensionFailed, || {
                Event::new(EventKind::LeaseExtensionFailed, "failed to extend visibility; retrying")
                    .field("queue", &inner.settings.queue_url)
                    .error(&error)
            });
        }
    }
}
