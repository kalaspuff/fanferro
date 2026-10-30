//! Leases: exclusive, heartbeat-protected ownership of a received message.
//!
//! A lease exists from the moment a message is received until it is settled (acked,
//! retried, released, requeued or dead-lettered). Until its settlement starts, the heartbeat
//! keeps the message invisible to other consumers and the lease holds one in-flight permit,
//! which is what bounds how many unsettled messages a consumer holds at once. A settlement
//! exchanges that permit for a settling permit (bounding the settlement backlog), so the
//! poller can refill the slot during the settlement's round trip. Dropping an unsettled lease
//! releases the message (makes it visible again immediately) as a safety net, unless the lease
//! was left ([`Lease::leave`]) or the engine stops without releasing unsettled messages
//! ([`ConsumerEngine::leave_unsettled`](super::ConsumerEngine::leave_unsettled)).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::{OwnedSemaphorePermit, oneshot};
use tokio::time::Instant;

use super::EngineInner;
use super::config::MAX_VISIBILITY;
use super::settle::{Reply, SettleCommand, SettleKind};
use crate::backend::{ReceivedMessage, SendRequest};
use crate::error::{AwsErrorKind, Error, Result};
use crate::events::{Event, EventKind};
use crate::types::attributes::MAX_ATTRIBUTES;
use crate::types::{AttributeValue, reserved};

/// Lifecycle state of a lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseStatus {
    /// Received and waiting for a handler.
    Buffered,
    /// Being processed.
    Active,
    /// A settlement is in progress.
    Settling,
    /// The receipt handle is no longer valid (the message may be redelivered).
    Lost,
    /// The maximum lease duration was reached; heartbeats stopped.
    Expired,
    /// Settled (no longer tracked).
    Settled,
}

impl LeaseStatus {
    /// Lowercase name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::Active => "active",
            Self::Settling => "settling",
            Self::Lost => "lost",
            Self::Expired => "expired",
            Self::Settled => "settled",
        }
    }
}

/// Bookkeeping for one lease (owned by the engine's lease table).
#[derive(Debug)]
pub(crate) struct LeaseEntry {
    /// The message (shared with its [`Lease`]: its receipt handle and id are not copied).
    pub message: Arc<ReceivedMessage>,
    pub received_at: Instant,
    pub visible_until: Instant,
    pub next_heartbeat: Instant,
    pub max_until: Instant,
    pub status: LeaseStatus,
    pub extending: bool,
}

/// How a message was settled (used for events and metrics).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Acked,
    Retried,
    Released,
    Requeued,
    DeadLettered,
}

/// A handle to a leased message.
pub struct Lease {
    pub(crate) id: u64,
    pub(crate) inner: Arc<EngineInner>,
    pub(crate) message: Arc<ReceivedMessage>,
    /// In-flight slot: held from receipt until the settlement starts.
    permit: Mutex<Option<OwnedSemaphorePermit>>,
    /// Settling slot: held while the settlement is in progress.
    settling: Mutex<Option<OwnedSemaphorePermit>>,
    settled: AtomicBool,
    /// The next settlement is awaited by a handler (see [`Lease::mark_awaited`]).
    awaited: AtomicBool,
    /// How many settlements are awaited, when everything that could settle waits (0: unknown;
    /// see [`Lease::mark_awaited`]).
    waiting_handlers: AtomicUsize,
}

impl std::fmt::Debug for Lease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lease").field("id", &self.id).field("message_id", &self.message.message_id).finish()
    }
}

const MAX_DETAIL_LEN: usize = 1024;

fn truncate(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_string();
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

impl Lease {
    pub(crate) fn new(
        id: u64,
        inner: Arc<EngineInner>,
        message: Arc<ReceivedMessage>,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            id,
            inner,
            message,
            permit: Mutex::new(Some(permit)),
            settling: Mutex::new(None),
            settled: AtomicBool::new(false),
            awaited: AtomicBool::new(false),
            waiting_handlers: AtomicUsize::new(0),
        }
    }

    /// The raw received message.
    pub fn message(&self) -> &ReceivedMessage {
        &self.message
    }

    /// Whether the lease has been settled (or settlement has started).
    pub fn is_settled(&self) -> bool {
        self.settled.load(Ordering::SeqCst)
    }

    /// Current status.
    pub fn status(&self) -> LeaseStatus {
        self.inner.leases.lock().get(&self.id).map_or(LeaseStatus::Settled, |entry| entry.status)
    }

    /// Seconds until the message becomes visible again (0 when settled or unknown).
    pub fn remaining_visibility(&self) -> Duration {
        self.inner
            .leases
            .lock()
            .get(&self.id)
            .map_or(Duration::ZERO, |entry| entry.visible_until.saturating_duration_since(Instant::now()))
    }

    /// Stops protecting the message without settling it: heartbeats stop, its in-flight slot is
    /// freed, and it becomes visible again when its visibility timeout runs out (instead of right
    /// away, as when an unsettled lease is dropped). Until then it can still be settled. Does
    /// nothing once a settlement started.
    pub fn leave(&self) {
        if self.is_settled() {
            return;
        }
        if self.inner.leases.lock().remove(&self.id).is_some() {
            self.inner.metrics.in_flight.add(-1);
        }
        self.permit.lock().take();
    }

    /// Marks the message as being processed (stops counting it as buffered).
    pub fn mark_active(&self) {
        if let Some(entry) = self.inner.leases.lock().get_mut(&self.id)
            && entry.status == LeaseStatus::Buffered
        {
            entry.status = LeaseStatus::Active;
        }
    }

    /// Starts a settlement: waits for a settling slot (heartbeats keep protecting the message
    /// meanwhile), then hands the in-flight slot back so that receiving continues while the
    /// settlement completes. The lease stays tracked until [`Lease::finish`].
    async fn begin(&self) -> Result<()> {
        self.claim()?;
        self.start_settling().await;
        Ok(())
    }

    /// Marks the lease as being settled; fails if a settlement was requested already.
    pub(crate) fn claim(&self) -> Result<()> {
        if self.settled.swap(true, Ordering::SeqCst) {
            return Err(Error::Lease(format!("message {} was already settled", self.message.message_id)));
        }
        Ok(())
    }

    pub(crate) async fn start_settling(&self) {
        // The semaphore is never closed, so this only fails if the engine is gone.
        let permit = self.inner.settling.clone().acquire_owned().await.ok();
        self.enter_settling(permit);
    }

    /// Takes the settling slot, then hands the in-flight slot back.
    fn enter_settling(&self, permit: Option<OwnedSemaphorePermit>) {
        if let Some(permit) = permit {
            *self.settling.lock() = Some(permit);
            self.inner.metrics.settling.add(1);
        }
        if let Some(entry) = self.inner.leases.lock().get_mut(&self.id) {
            entry.status = LeaseStatus::Settling;
        }
        self.permit.lock().take();
    }

    /// Returns the settling slot (if one is held).
    fn end_settling(&self) {
        if self.settling.lock().take().is_some() {
            self.inner.metrics.settling.add(-1);
        }
    }

    fn finish(&self, outcome: Outcome, result: &Result<()>) {
        if self.inner.leases.lock().remove(&self.id).is_some() {
            self.inner.metrics.in_flight.add(-1);
        }
        self.permit.lock().take();
        self.end_settling();
        let metrics = &self.inner.metrics;
        let queue = self.inner.settings.queue_url.as_str();
        let message_id = self.message.message_id.as_str();
        match (outcome, result) {
            (Outcome::Acked, Ok(())) => {
                metrics.messages_acked.inc();
                self.inner.client.events().emit(EventKind::MessageAcked, || {
                    Event::new(EventKind::MessageAcked, "message acked")
                        .field("queue", queue)
                        .field("sqs_message_id", message_id)
                });
            }
            (Outcome::Acked, Err(error)) => {
                metrics.ack_failures.inc();
                self.inner.client.events().emit(EventKind::MessageAckFailed, || {
                    Event::new(EventKind::MessageAckFailed, "failed to delete message; it may be redelivered")
                        .field("queue", queue)
                        .field("sqs_message_id", message_id)
                        .error(error)
                });
            }
            (Outcome::Retried, Ok(())) => metrics.messages_retried.inc(),
            (Outcome::Released, Ok(())) => metrics.messages_released.inc(),
            (Outcome::Requeued, Ok(())) => metrics.messages_requeued.inc(),
            (Outcome::DeadLettered, Ok(())) => metrics.messages_dead_lettered.inc(),
            (Outcome::DeadLettered, Err(_)) => metrics.dead_letter_failures.inc(),
            _ => {}
        }
    }

    async fn settle(&self, kind: SettleKind) -> Result<()> {
        let (reply, receiver) = oneshot::channel();
        self.queue(kind, Reply::Channel(reply));
        receiver.await.map_err(|_| Error::Shutdown("the consumer closed before settling".into()))?
    }

    /// Hands a settlement command to the settle loop, which sends the outcome to `reply` (right
    /// away if the consumer is closed).
    pub(crate) fn queue(&self, kind: SettleKind, reply: Reply) {
        let Some(queue) = self.inner.settle_queue() else {
            reply.send(Err(Error::Shutdown("the consumer is closed".into())));
            return;
        };
        let mut command = SettleCommand::new(self.message.receipt_handle.clone(), kind, Some(reply));
        command.awaited = self.awaited.swap(false, Ordering::SeqCst);
        command.waiting_handlers = Some(self.waiting_handlers.swap(0, Ordering::SeqCst)).filter(|&n| n > 0);
        if let Err(command) = queue.push(command)
            && let Some(reply) = command.reply
        {
            reply.send(Err(Error::Shutdown("the consumer is closed".into())));
        }
    }

    /// Notes that a handler (or a FIFO message group) waits for this lease's next settlement or
    /// explicit extension, so that the batch it joins is not held back for settlements that
    /// cannot come.
    ///
    /// `waiting_handlers`: when every handler that could request another settlement waits for
    /// one, how many settlements are awaited. The settle loop sends the batch as soon as it holds
    /// that many awaited settlements; while fewer are pending, some of them were sent already,
    /// and their completion will soon make room for handlers that settle more.
    pub fn mark_awaited(&self, waiting_handlers: Option<usize>) {
        self.awaited.store(true, Ordering::SeqCst);
        self.waiting_handlers.store(waiting_handlers.unwrap_or(0), Ordering::SeqCst);
    }

    fn cap_visibility(&self, timeout: Duration) -> Duration {
        let received_at = self.inner.leases.lock().get(&self.id).map(|entry| entry.received_at);
        let elapsed = received_at.map_or(Duration::ZERO, |at| at.elapsed());
        timeout.min(MAX_VISIBILITY.saturating_sub(elapsed + Duration::from_secs(5)))
    }

    /// Deletes the message (successful processing).
    pub async fn ack(&self) -> Result<()> {
        self.claim()?;
        self.ack_claimed().await
    }

    /// [`Lease::ack`] after [`Lease::claim`] succeeded.
    pub(crate) async fn ack_claimed(&self) -> Result<()> {
        self.start_settling().await;
        let result = self.settle(SettleKind::Delete).await;
        self.finish(Outcome::Acked, &result);
        result
    }

    /// The fast path of an ack after [`Lease::claim`]: when a settling slot is free (the usual
    /// case), takes it and queues the delete with `reply` right away, so acks requested one after
    /// another reach the settle loop in that order. Hands `reply` back when no slot is free.
    pub(crate) fn try_queue_ack(&self, reply: Reply) -> std::result::Result<(), Reply> {
        let Ok(permit) = self.inner.settling.clone().try_acquire_owned() else {
            return Err(reply);
        };
        self.enter_settling(Some(permit));
        self.queue(SettleKind::Delete, reply);
        Ok(())
    }

    /// Records the outcome of an ack whose delete was queued with [`Lease::queue`].
    pub(crate) fn finish_ack(&self, result: &Result<()>) {
        self.finish(Outcome::Acked, result);
    }

    /// Makes the message visible again after `delay` (retry with backoff).
    pub async fn nack(&self, delay: Duration) -> Result<()> {
        self.begin().await?;
        let delay = self.cap_visibility(self.inner.visibility_floor(delay));
        let result = self.settle(SettleKind::Visibility(delay)).await;
        self.finish(Outcome::Retried, &result);
        if result.is_ok() {
            self.inner.client.events().emit(EventKind::MessageRetryScheduled, || {
                Event::new(
                    EventKind::MessageRetryScheduled,
                    format!("message will be retried in {:.1}s", delay.as_secs_f64()),
                )
                .field("queue", &self.inner.settings.queue_url)
                .field("sqs_message_id", &self.message.message_id)
                .field("delay_seconds", delay.as_secs_f64())
                .field("receive_count", self.message.system.approximate_receive_count)
            });
        }
        result
    }

    /// Makes the message visible again immediately (no processing happened).
    pub async fn release(&self) -> Result<()> {
        self.begin().await?;
        let result = self.settle(SettleKind::Visibility(self.inner.visibility_floor(Duration::ZERO))).await;
        self.finish(Outcome::Released, &result);
        if result.is_ok() {
            self.inner.client.events().emit(EventKind::MessageReleased, || {
                Event::new(EventKind::MessageReleased, "message released")
                    .field("queue", &self.inner.settings.queue_url)
                    .field("sqs_message_id", &self.message.message_id)
            });
        }
        result
    }

    /// Sets the visibility timeout to `timeout` from now without settling.
    pub async fn extend(&self, timeout: Duration) -> Result<()> {
        if self.is_settled() {
            return Err(Error::Lease(format!("message {} was already settled", self.message.message_id)));
        }
        let timeout = self.cap_visibility(timeout);
        let started = Instant::now();
        let result = self.settle(SettleKind::Visibility(timeout)).await;
        if result.is_ok()
            && let Some(entry) = self.inner.leases.lock().get_mut(&self.id)
        {
            entry.visible_until = started + timeout;
            entry.next_heartbeat = started + self.inner.settings.heartbeat_interval.min(timeout / 2);
        }
        result
    }

    /// Forwards the message to the dead-letter queue, then deletes it.
    ///
    /// The original body and attributes are preserved (so the message can be redriven),
    /// and a `fanferro.dead-letter` attribute records why it was dead-lettered.
    pub async fn dead_letter(&self, reason: &str, details: &BTreeMap<String, String>) -> Result<()> {
        let Some(dlq_url) = self.inner.settings.dead_letter_queue_url.clone() else {
            return Err(Error::config(format!(
                "cannot dead-letter message {}: no dead-letter queue is configured for {}",
                self.message.message_id, self.inner.settings.queue_url
            )));
        };
        self.begin().await?;
        let result = self.forward_to_dead_letter_queue(&dlq_url, reason, details).await;
        let result = match result {
            Ok(()) => self.settle(SettleKind::Delete).await,
            Err(error) => Err(error),
        };
        self.finish(Outcome::DeadLettered, &result);
        let message_id = self.message.message_id.as_str();
        let queue = self.inner.settings.queue_url.as_str();
        match &result {
            Ok(()) => self.inner.client.events().emit(EventKind::MessageDeadLettered, || {
                Event::new(EventKind::MessageDeadLettered, format!("message dead-lettered: {reason}"))
                    .field("queue", queue)
                    .field("dead_letter_queue", dlq_url)
                    .field("sqs_message_id", message_id)
                    .field("reason", reason)
            }),
            Err(error) => self.inner.client.events().emit(EventKind::MessageDeadLetterFailed, || {
                Event::new(EventKind::MessageDeadLetterFailed, "failed to forward message to the dead-letter queue")
                    .field("queue", queue)
                    .field("dead_letter_queue", dlq_url)
                    .field("sqs_message_id", message_id)
                    .field("reason", reason)
                    .error(error)
            }),
        }
        result
    }

    async fn forward_to_dead_letter_queue(
        &self,
        dlq_url: &str,
        reason: &str,
        details: &BTreeMap<String, String>,
    ) -> Result<()> {
        let message = &self.message;
        let mut info = serde_json::Map::new();
        info.insert("reason".into(), truncate(reason, MAX_DETAIL_LEN).into());
        info.insert(
            "source".into(),
            self.inner.settings.queue_arn.clone().unwrap_or_else(|| self.inner.settings.queue_url.clone()).into(),
        );
        info.insert("sqs_message_id".into(), message.message_id.clone().into());
        info.insert("receive_count".into(), message.system.approximate_receive_count.into());
        info.insert("time".into(), OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_default().into());
        for (key, value) in details {
            info.insert(key.clone(), truncate(value, MAX_DETAIL_LEN).into());
        }
        let mut attributes = message.attributes.clone();
        if attributes.len() < MAX_ATTRIBUTES || attributes.contains_key(reserved::DEAD_LETTER) {
            attributes.insert(
                reserved::DEAD_LETTER.into(),
                AttributeValue::string(serde_json::Value::Object(info).to_string()),
            );
        }
        let fifo = self.inner.settings.dead_letter_fifo;
        let request = SendRequest {
            body: message.body.clone(),
            attributes,
            delay: None,
            group_id: if fifo {
                Some(message.system.message_group_id.clone().unwrap_or_else(|| "fanferro-dead-letter".into()))
            } else {
                None
            },
            deduplication_id: fifo.then(|| message.message_id.clone()),
            trace_header: message.system.aws_trace_header.clone(),
        };
        self.inner.client.backend().sqs.send_message(dlq_url, request).await.map(|_| ())
    }

    /// Sends a copy of the message back to the queue with an incremented attempt counter
    /// (and optional delay on standard queues), then deletes the original.
    pub async fn requeue(&self, delay: Option<Duration>, attempt: u32) -> Result<()> {
        self.begin().await?;
        let message = &self.message;
        let mut attributes = message.attributes.clone();
        let result = if attributes.len() >= MAX_ATTRIBUTES && !attributes.contains_key(reserved::ATTEMPT) {
            Err(Error::validation(
                "cannot requeue: the message already has 10 attributes (no room for fanferro.attempt)",
            ))
        } else {
            attributes.insert(reserved::ATTEMPT.into(), AttributeValue::number(attempt.to_string()));
            let fifo = self.inner.settings.fifo;
            let request = SendRequest {
                body: message.body.clone(),
                attributes,
                delay: if fifo { None } else { delay.map(|d| d.min(crate::publish::MAX_DELAY)) },
                group_id: message.system.message_group_id.clone(),
                deduplication_id: fifo.then(|| format!("{}-r{attempt}", message.message_id)),
                trace_header: message.system.aws_trace_header.clone(),
            };
            match self.inner.client.backend().sqs.send_message(&self.inner.settings.queue_url, request).await {
                Ok(_) => self.settle(SettleKind::Delete).await,
                Err(error) => Err(error),
            }
        };
        self.finish(Outcome::Requeued, &result);
        if result.is_ok() {
            self.inner.client.events().emit(EventKind::MessageRequeued, || {
                Event::new(EventKind::MessageRequeued, "message requeued")
                    .field("queue", &self.inner.settings.queue_url)
                    .field("sqs_message_id", &message.message_id)
                    .field("attempt", attempt)
                    .field("delay_seconds", delay.unwrap_or_default().as_secs_f64())
            });
        }
        result
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.end_settling();
        let was_settled = self.settled.swap(true, Ordering::SeqCst);
        // `finish` and `close` remove settled entries; anything still tracked is ours to clean up.
        let tracked = self.inner.leases.lock().remove(&self.id).is_some();
        if !tracked {
            return;
        }
        self.inner.metrics.in_flight.add(-1);
        if was_settled {
            // A settlement was interrupted (its future was dropped); nothing left to do.
            return;
        }
        if self.inner.leave_dropped.load(Ordering::SeqCst) {
            // Stopping without releasing unfinished messages: left to time out.
            return;
        }
        // Dropped without settlement: stop heartbeating and make the message visible again.
        if let Some(queue) = self.inner.settle_queue() {
            let _ = queue.push(SettleCommand::new(
                self.message.receipt_handle.clone(),
                SettleKind::Visibility(self.inner.visibility_floor(Duration::ZERO)),
                None,
            ));
            self.inner.metrics.messages_released.inc();
        }
    }
}

/// Whether an error means the receipt handle can no longer be used.
pub(crate) fn is_lost(error: &Error) -> bool {
    error.aws().is_some_and(|aws| aws.kind == AwsErrorKind::ReceiptHandleInvalid)
}
