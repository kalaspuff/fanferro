//! Visibility heartbeats: extend leases before they expire, in batches of ten.
//!
//! Each tick starts a round of extension calls for the leases that are due; the loop keeps the
//! rounds in flight next to its tick, so one slow call does not hold back the leases that come
//! due meanwhile (a lease with an extension in flight is skipped until the call ended). A lease
//! whose extension failed is retried after a delay that grows with its consecutive failures when
//! retrying soon cannot help (throttling, access denied, a deleted queue), and each round reports
//! its failures in one `lease.extension_failed` event.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use futures::future::join_all;
use futures::stream::FuturesUnordered;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::EngineInner;
use super::config::MAX_VISIBILITY;
use super::lease::{LeaseStatus, is_lost};
use crate::backend::{ReceivedMessage, VisibilityChange};
use crate::error::{AwsErrorKind, Error};
use crate::events::{Event, EventKind};
use crate::publish::MAX_BATCH_ENTRIES;

/// The first retry of a failed extension (and of every retryable one but throttling).
const RETRY_SOON: Duration = Duration::from_secs(1);
/// The most consecutive failures that still double the retry delay (2^10 s is beyond any cap).
const MAX_BACKOFF_DOUBLINGS: u32 = 10;
const SAFETY_MARGIN: Duration = Duration::from_secs(5);

/// How long an extension call may take before its leases are retried (at the next tick): the SDK
/// has no read timeout by default, so one stuck connection would otherwise keep its leases from
/// being extended until their visibility runs out.
pub(super) fn call_timeout(heartbeat_interval: Duration) -> Duration {
    (heartbeat_interval / 2).clamp(Duration::from_millis(500), Duration::from_secs(10))
}

struct Due {
    id: u64,
    message: Arc<ReceivedMessage>,
    timeout: Duration,
}

/// When to retry the extension of a lease that failed `failures` times in a row with `error`.
///
/// A call that timed out got no answer at all and is retried at the next tick; other retryable
/// failures (a 5xx, a reset connection) after a second. Throttling and failures that retrying the
/// same call cannot fix (access denied, a deleted queue, a KMS problem) back off exponentially
/// from a second up to the heartbeat interval, so that they do not flood SQS and the logs until
/// the messages' visibility runs out, while a fixed policy is still picked up within an interval.
pub(super) fn retry_delay(error: &Error, failures: u32, heartbeat_interval: Duration) -> Duration {
    if matches!(error, Error::Timeout(_)) {
        return Duration::ZERO;
    }
    let throttled = error.aws().is_some_and(|aws| aws.kind == AwsErrorKind::Throttling);
    if error.is_retryable() && !throttled {
        return RETRY_SOON;
    }
    let doublings = failures.saturating_sub(1).min(MAX_BACKOFF_DOUBLINGS);
    (RETRY_SOON * 2u32.pow(doublings)).min(heartbeat_interval.max(RETRY_SOON))
}

/// What a round of extension calls (one tick) reports when its calls ended.
#[derive(Default)]
struct RoundReport {
    /// Leases whose extension failed (not counting lost ones).
    failed: usize,
    /// The message id of the failed lease, when only one failed.
    failed_message: Option<String>,
    /// The first failure.
    error: Option<Error>,
    /// The longest retry delay given to a failed lease.
    retry_in: Duration,
    /// A failure said the consumer may not change the visibility of the queue's messages.
    access_denied: Option<Error>,
}

impl RoundReport {
    fn failed(&mut self, count: usize, message_id: Option<&str>, error: &Error, retry_in: Duration) {
        self.failed_message = if self.failed == 0 && count == 1 { message_id.map(str::to_string) } else { None };
        self.failed += count;
        self.retry_in = self.retry_in.max(retry_in);
        if self.error.is_none() {
            self.error = Some(error.clone());
        }
        if self.access_denied.is_none() && error.aws().is_some_and(|aws| aws.kind == AwsErrorKind::AccessDenied) {
            self.access_denied = Some(error.clone());
        }
    }

    fn merge(&mut self, other: Self) {
        if let Some(error) = &other.error {
            let message = if other.failed == 1 { other.failed_message.as_deref() } else { None };
            self.failed(other.failed, message, error, other.retry_in);
        }
        if self.access_denied.is_none() {
            self.access_denied = other.access_denied;
        }
    }
}

/// Runs until the engine shuts down, then waits for the extension calls in flight.
pub(crate) async fn heartbeat_loop(inner: Arc<EngineInner>) {
    let mut rounds = FuturesUnordered::new();
    let mut warned_access_denied = false;
    let mut next_tick = Instant::now() + inner.settings.heartbeat_tick;
    loop {
        tokio::select! {
            () = inner.shutdown_token.cancelled() => break,
            Some(report) = rounds.next(), if !rounds.is_empty() => {
                report_round(&inner, report, &mut warned_access_denied);
                continue;
            }
            () = tokio::time::sleep_until(next_tick) => {}
        }
        next_tick = Instant::now() + inner.settings.heartbeat_tick;
        if let Some(round) = start_round(&inner) {
            rounds.push(round);
        }
    }
    while let Some(report) = rounds.next().await {
        report_round(&inner, report, &mut warned_access_denied);
    }
}

/// Marks the leases that are due as being extended and returns the round of calls that extends
/// them (none when no lease is due); expires the leases that reached their maximum duration.
fn start_round(inner: &Arc<EngineInner>) -> Option<impl Future<Output = RoundReport> + use<>> {
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
            Event::new(EventKind::LeaseExpired, "maximum lease duration reached; the message will become visible again")
                .field("queue", &inner.settings.queue_url)
                .field("sqs_message_id", &message.message_id)
        });
    }
    if chunks.is_empty() {
        return None;
    }
    let inner = inner.clone();
    Some(async move {
        let reports = join_all(chunks.into_iter().map(|(call, chunk)| extend_chunk(&inner, call, chunk))).await;
        let mut round = RoundReport::default();
        for report in reports {
            round.merge(report);
        }
        round
    })
}

/// Emits one `lease.extension_failed` event for the failures of a round, and a `config.warning`
/// the first time the queue refuses the consumer's visibility changes.
fn report_round(inner: &EngineInner, report: RoundReport, warned_access_denied: &mut bool) {
    if let Some(error) = &report.error {
        inner.client.events().emit(EventKind::LeaseExtensionFailed, || {
            let message = if report.failed == 1 {
                "failed to extend the visibility of a message; retrying".to_string()
            } else {
                format!("failed to extend the visibility of {} messages; retrying", report.failed)
            };
            Event::new(EventKind::LeaseExtensionFailed, message)
                .field("queue", &inner.settings.queue_url)
                .field("messages", report.failed)
                .field_opt("sqs_message_id", report.failed_message.as_deref())
                .field("retry_seconds", report.retry_in.as_secs_f64())
                .error(error)
        });
    }
    if let Some(error) = &report.access_denied
        && !*warned_access_denied
    {
        *warned_access_denied = true;
        inner.client.events().emit(EventKind::ConfigWarning, || {
            Event::new(
                EventKind::ConfigWarning,
                "heartbeats cannot extend the visibility of messages: access denied; grant the consumer \
                 sqs:ChangeMessageVisibility on the queue, or messages become visible again while they are processed",
            )
            .field("queue", &inner.settings.queue_url)
            .field("permission", "sqs:ChangeMessageVisibility")
            .error(error)
        });
    }
}

/// Extends a chunk of leases; cancels `call` when the call ended (also when it timed out, or when
/// this future is dropped), after the leases were updated.
async fn extend_chunk(inner: &Arc<EngineInner>, call: CancellationToken, chunk: Vec<Due>) -> RoundReport {
    let _ended = call.drop_guard();
    let started = Instant::now();
    let entries = chunk
        .iter()
        .map(|due| VisibilityChange { receipt_handle: due.message.receipt_handle.clone(), timeout: due.timeout })
        .collect();
    let limit = call_timeout(inner.settings.heartbeat_interval);
    let call = inner.client.backend().sqs.change_visibility_batch(&inner.settings.queue_url, entries);
    let result = tokio::time::timeout(limit, call).await.unwrap_or_else(|_| {
        Err(Error::Timeout(format!(
            "ChangeMessageVisibilityBatch did not complete within {:.1} s",
            limit.as_secs_f64()
        )))
    });
    let interval = inner.settings.heartbeat_interval;
    let mut report = RoundReport::default();
    let mut leases = inner.leases.lock();
    match result {
        Ok(results) => {
            for (due, outcome) in chunk.into_iter().zip(results) {
                let Some(lease) = leases.get_mut(&due.id) else { continue };
                lease.extending = None;
                match outcome {
                    Ok(()) => {
                        lease.extension_failures = 0;
                        lease.visible_until = started + due.timeout;
                        lease.next_heartbeat = started + interval.min(due.timeout / 2);
                        inner.metrics.lease_extensions.inc();
                        inner.client.events().emit(EventKind::LeaseExtended, || {
                            Event::new(EventKind::LeaseExtended, "visibility extended")
                                .field("queue", &inner.settings.queue_url)
                                .field("sqs_message_id", &due.message.message_id)
                                .field("visibility_seconds", due.timeout.as_secs_f64())
                        });
                    }
                    Err(entry) => {
                        let error = Error::from(
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
                            lease.extension_failures = lease.extension_failures.saturating_add(1);
                            let retry_in = retry_delay(&error, lease.extension_failures, interval);
                            lease.next_heartbeat = Instant::now() + retry_in;
                            inner.metrics.lease_extension_failures.inc();
                            report.failed(1, Some(&due.message.message_id), &error, retry_in);
                        }
                    }
                }
            }
        }
        Err(error) => {
            let mut longest = Duration::ZERO;
            for due in &chunk {
                if let Some(lease) = leases.get_mut(&due.id) {
                    lease.extending = None;
                    lease.extension_failures = lease.extension_failures.saturating_add(1);
                    let retry_in = retry_delay(&error, lease.extension_failures, interval);
                    longest = longest.max(retry_in);
                    lease.next_heartbeat = Instant::now() + retry_in;
                }
                inner.metrics.lease_extension_failures.inc();
            }
            let message_id = (chunk.len() == 1).then(|| chunk[0].message.message_id.as_str());
            report.failed(chunk.len(), message_id, &error, longest);
        }
    }
    report
}
