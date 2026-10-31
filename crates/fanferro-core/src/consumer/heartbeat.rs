//! Visibility heartbeats: extend leases before they expire, in batches of ten.

use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::EngineInner;
use super::config::MAX_VISIBILITY;
use super::lease::{LeaseStatus, is_lost};
use crate::backend::{ReceivedMessage, VisibilityChange};
use crate::events::{Event, EventKind};
use crate::publish::MAX_BATCH_ENTRIES;
use crate::retry::sleep_or_cancel;

const RETRY_SOON: Duration = Duration::from_secs(1);
const SAFETY_MARGIN: Duration = Duration::from_secs(5);

/// How long an extension call may take before its leases are retried (at the next tick): the loop
/// waits for its calls, and the SDK has no read timeout by default, so one stuck connection would
/// otherwise stall every lease's heartbeat until their visibility runs out.
pub(super) fn call_timeout(heartbeat_interval: Duration) -> Duration {
    (heartbeat_interval / 2).clamp(Duration::from_millis(500), Duration::from_secs(10))
}

struct Due {
    id: u64,
    message: Arc<ReceivedMessage>,
    timeout: Duration,
}

/// Runs until the engine shuts down.
pub(crate) async fn heartbeat_loop(inner: Arc<EngineInner>) {
    loop {
        if !sleep_or_cancel(inner.settings.heartbeat_tick, &inner.shutdown_token).await {
            break;
        }
        let now = Instant::now();
        // Batch calls of up to ten leases, each with the token its leases keep while it is in flight.
        let mut chunks: Vec<(CancellationToken, Vec<Due>)> = Vec::new();
        let mut expired = Vec::new();
        {
            let mut leases = inner.leases.lock();
            for (id, lease) in leases.iter_mut() {
                if !matches!(lease.status, LeaseStatus::Buffered | LeaseStatus::Active)
                    || lease.extending.is_some()
                    || lease.next_heartbeat > now
                {
                    continue;
                }
                let hard_cap =
                    (lease.received_at + MAX_VISIBILITY).saturating_duration_since(now).saturating_sub(SAFETY_MARGIN);
                let remaining = lease.max_until.saturating_duration_since(now).min(hard_cap);
                if remaining.is_zero() {
                    lease.status = LeaseStatus::Expired;
                    expired.push(lease.message.clone());
                    continue;
                }
                if chunks.last().is_none_or(|(_, chunk)| chunk.len() == MAX_BATCH_ENTRIES) {
                    chunks.push((CancellationToken::new(), Vec::with_capacity(MAX_BATCH_ENTRIES)));
                }
                let Some((call, chunk)) = chunks.last_mut() else { continue };
                lease.extending = Some(call.clone());
                chunk.push(Due {
                    id: *id,
                    message: lease.message.clone(),
                    timeout: inner.settings.extend_by.min(remaining),
                });
            }
        }
        for message in expired {
            inner.metrics.leases_expired.inc();
            inner.client.events().emit(EventKind::LeaseExpired, || {
                Event::new(
                    EventKind::LeaseExpired,
                    "maximum lease duration reached; the message will become visible again",
                )
                .field("queue", &inner.settings.queue_url)
                .field("sqs_message_id", &message.message_id)
            });
        }
        join_all(chunks.into_iter().map(|(call, chunk)| extend_chunk(&inner, call, chunk))).await;
    }
}

/// Extends a chunk of leases; cancels `call` when the call ended (also when it timed out, or when
/// this future is dropped), after the leases were updated.
async fn extend_chunk(inner: &Arc<EngineInner>, call: CancellationToken, chunk: Vec<Due>) {
    let _ended = call.drop_guard();
    let started = Instant::now();
    let entries = chunk
        .iter()
        .map(|due| VisibilityChange { receipt_handle: due.message.receipt_handle.clone(), timeout: due.timeout })
        .collect();
    let limit = call_timeout(inner.settings.heartbeat_interval);
    let call = inner.client.backend().sqs.change_visibility_batch(&inner.settings.queue_url, entries);
    let result = tokio::time::timeout(limit, call).await.unwrap_or_else(|_| {
        Err(crate::error::Error::Timeout(format!(
            "ChangeMessageVisibilityBatch did not complete within {:.1} s",
            limit.as_secs_f64()
        )))
    });
    let mut leases = inner.leases.lock();
    match result {
        Ok(results) => {
            for (due, outcome) in chunk.into_iter().zip(results) {
                let Some(lease) = leases.get_mut(&due.id) else { continue };
                lease.extending = None;
                match outcome {
                    Ok(()) => {
                        lease.visible_until = started + due.timeout;
                        lease.next_heartbeat = started + inner.settings.heartbeat_interval.min(due.timeout / 2);
                        inner.metrics.lease_extensions.inc();
                        inner.client.events().emit(EventKind::LeaseExtended, || {
                            Event::new(EventKind::LeaseExtended, "visibility extended")
                                .field("queue", &inner.settings.queue_url)
                                .field("sqs_message_id", &due.message.message_id)
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
                                .field("sqs_message_id", &due.message.message_id)
                                .error(&error)
                            });
                        } else {
                            lease.next_heartbeat = Instant::now() + RETRY_SOON;
                            inner.metrics.lease_extension_failures.inc();
                            inner.client.events().emit(EventKind::LeaseExtensionFailed, || {
                                Event::new(EventKind::LeaseExtensionFailed, "failed to extend visibility; retrying")
                                    .field("queue", &inner.settings.queue_url)
                                    .field("sqs_message_id", &due.message.message_id)
                                    .error(&error)
                            });
                        }
                    }
                }
            }
        }
        Err(error) => {
            // A call that timed out got no answer at all: try again at the next tick.
            let retry_in = if matches!(error, crate::error::Error::Timeout(_)) { Duration::ZERO } else { RETRY_SOON };
            for due in chunk {
                if let Some(lease) = leases.get_mut(&due.id) {
                    lease.extending = None;
                    lease.next_heartbeat = Instant::now() + retry_in;
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
