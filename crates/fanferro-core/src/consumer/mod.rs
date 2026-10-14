//! The consumer engine: receiving, leasing, heartbeats, settlement and shutdown.
//!
//! ```text
//!   pollers (N) ──ReceiveMessage──▶ lease table ──decode──▶ delivery channel ──▶ next_batch()
//!       ▲  in-flight permits + rate limiter        │                                │
//!       │                                          ▼                                ▼
//!   in-flight permit returned       heartbeat (ChangeMessageVisibilityBatch)   Lease::ack/nack/...
//!   when settlement starts                                                         │ settling permit
//!                                                     settle loop (Delete/ChangeVisibility batches)
//! ```
//!
//! * At most `max_in_flight` unsettled messages are leased at any time (buffered + processing):
//!   pollers acquire one in-flight permit per requested message before calling `ReceiveMessage`.
//! * A settlement first takes a settling permit (at most `max_settling` settlements run at once),
//!   then hands the message's in-flight permit back: the next receive does not wait for the
//!   delete round trip. A consumer therefore holds at most `max_in_flight + max_settling`
//!   messages.
//! * Every leased message is protected by heartbeats until its settlement starts (also while it
//!   waits for a settling permit); it stays in the lease table until the settlement completed.
//! * Shutdown is two-phase: [`ConsumerEngine::stop_receiving`] stops the pollers (the
//!   caller drains in-flight work), then [`ConsumerEngine::close`] releases or abandons
//!   what is left, flushes pending settlements and stops the heartbeat.

mod config;
mod heartbeat;
mod lease;
mod poller;
mod settle;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use time::OffsetDateTime;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub use config::{ConsumerConfig, HeartbeatConfig, MAX_VISIBILITY, RateLimitConfig, SettleConfig};
pub use lease::{Lease, LeaseStatus};

use crate::backend::ReceivedMessage;
use crate::client::Client;
use crate::error::{Error, Result};
use crate::events::{Event, EventKind};
use crate::metrics::ConsumerMetrics;
use crate::retry::RateLimiter;
use crate::transport::DecodedBody;
use crate::transport::sns_notification::{self, SnsInfo, SnsMessageKind};
use crate::types::attributes::{md5_of_attributes, md5_of_body};
use crate::types::reserved;
use config::Settings;
use lease::LeaseEntry;
use settle::{SettleCommand, SettleKind};

/// A received message handed to the application.
#[derive(Debug)]
pub struct Delivery {
    /// The lease (settle the message through it).
    pub lease: Lease,
    /// SNS metadata when the message arrived as an SNS notification.
    pub sns: Option<SnsInfo>,
    /// The transport-decoded message, or why decoding failed.
    pub decoded: std::result::Result<DecodedBody, Error>,
    /// When the message was received.
    pub received_at: OffsetDateTime,
}

impl Delivery {
    /// The raw message as received from SQS.
    pub fn message(&self) -> &ReceivedMessage {
        self.lease.message()
    }
}

/// Lifecycle state of the engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EngineState {
    /// Created, not started.
    Created,
    /// Receiving.
    Running,
    /// Receiving stopped; deliveries can still be settled.
    Stopping,
    /// Closed.
    Stopped,
}

impl EngineState {
    /// Lowercase name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
        }
    }
}

pub(crate) struct EngineInner {
    pub(crate) client: Client,
    pub(crate) settings: Settings,
    pub(crate) permits: Arc<Semaphore>,
    pub(crate) settling: Arc<Semaphore>,
    pub(crate) leases: Mutex<HashMap<u64, LeaseEntry>>,
    pub(crate) metrics: Arc<ConsumerMetrics>,
    next_lease_id: AtomicU64,
    deliveries_tx: mpsc::Sender<Delivery>,
    deliveries_rx: tokio::sync::Mutex<mpsc::Receiver<Delivery>>,
    settle_tx: Mutex<Option<mpsc::UnboundedSender<SettleCommand>>>,
    receive_token: CancellationToken,
    abort_token: CancellationToken,
    shutdown_token: CancellationToken,
    paused: watch::Sender<bool>,
    state: Mutex<EngineState>,
    pollers: Mutex<Vec<JoinHandle<()>>>,
    background: Mutex<Vec<JoinHandle<()>>>,
    decoders: TaskTracker,
    rate_limiter: Option<RateLimiter>,
    runtime: tokio::runtime::Handle,
    /// Until when an aborted long poll may still be served by the queue (see
    /// [`EngineInner::visibility_floor`]).
    orphaned_receives_until: Mutex<Option<Instant>>,
}

impl EngineInner {
    pub(crate) fn settle_sender(&self) -> Option<mpsc::UnboundedSender<SettleCommand>> {
        self.settle_tx.lock().clone()
    }

    fn queue(&self) -> &str {
        &self.settings.queue_url
    }

    /// Raises a visibility timeout so the message cannot become visible while a long poll that
    /// was aborted during shutdown may still be served by the queue.
    ///
    /// Dropping a `ReceiveMessage` future closes the connection, but SQS (and emulators) keep the
    /// long poll open on the server until its wait time ends and may hand it messages that then
    /// stay invisible for a whole visibility timeout. Messages released during shutdown therefore
    /// become visible only after such orphaned polls are over.
    pub(crate) fn visibility_floor(&self, timeout: Duration) -> Duration {
        match *self.orphaned_receives_until.lock() {
            Some(until) => {
                let remaining = until.saturating_duration_since(Instant::now());
                if remaining.is_zero() { timeout } else { timeout.max(Duration::from_secs(remaining.as_secs() + 1)) }
            }
            None => timeout,
        }
    }

    /// Registers leases for received messages and decodes them in order.
    pub(crate) fn accept(self: &Arc<Self>, messages: Vec<ReceivedMessage>, permits: Vec<OwnedSemaphorePermit>) {
        let now = Instant::now();
        let mut leased = Vec::with_capacity(messages.len());
        {
            let mut leases = self.leases.lock();
            for (message, permit) in messages.into_iter().zip(permits) {
                let id = self.next_lease_id.fetch_add(1, Ordering::Relaxed);
                leases.insert(
                    id,
                    LeaseEntry {
                        receipt_handle: message.receipt_handle.clone(),
                        message_id: message.message_id.clone(),
                        received_at: now,
                        visible_until: now + self.settings.visibility_timeout,
                        next_heartbeat: now + self.settings.heartbeat_interval,
                        max_until: now + self.settings.max_lease,
                        status: LeaseStatus::Buffered,
                        extending: false,
                    },
                );
                leased.push(Lease::new(id, self.clone(), Arc::new(message), permit));
            }
        }
        self.metrics.messages_received.add(leased.len() as u64);
        self.metrics.in_flight.add(i64::try_from(leased.len()).unwrap_or(i64::MAX));
        for lease in &leased {
            let message = lease.message();
            self.client.events().emit(EventKind::MessageReceived, || {
                Event::new(EventKind::MessageReceived, "message received")
                    .field("queue", self.queue())
                    .field("sqs_message_id", &message.message_id)
                    .field("receive_count", message.system.approximate_receive_count)
                    .field_opt("group_id", message.system.message_group_id.clone())
            });
        }
        let inner = self.clone();
        self.decoders.spawn(async move {
            for lease in leased {
                if let Some(delivery) = inner.decode(lease).await {
                    inner.metrics.buffered.add(1);
                    if let Err(error) = inner.deliveries_tx.send(delivery).await {
                        // The engine is gone; dropping the delivery releases the message.
                        inner.metrics.buffered.add(-1);
                        drop(error);
                    }
                }
            }
        });
    }

    async fn decode(self: &Arc<Self>, lease: Lease) -> Option<Delivery> {
        let received_at = OffsetDateTime::now_utc();
        let message = lease.message().clone();
        if self.settings.verify_md5 {
            let body_ok =
                message.md5_of_body.as_ref().is_none_or(|md5| md5.eq_ignore_ascii_case(&md5_of_body(&message.body)));
            let attributes_ok = message.attributes.is_empty()
                || message
                    .md5_of_message_attributes
                    .as_ref()
                    .is_none_or(|md5| md5.eq_ignore_ascii_case(&md5_of_attributes(&message.attributes)));
            if !body_ok || !attributes_ok {
                self.metrics.decode_failures.inc();
                return Some(Delivery {
                    lease,
                    sns: None,
                    decoded: Err(Error::Checksum("MD5 of the received message does not match".into())),
                    received_at,
                });
            }
        }
        let (body, mut attributes, sns) = match sns_notification::unwrap(&message.body, self.settings.sns_unwrap) {
            Ok(Some(unwrapped)) => {
                if unwrapped.info.kind != SnsMessageKind::Notification {
                    self.handle_sns_control(lease, unwrapped.info).await;
                    return None;
                }
                (unwrapped.message, unwrapped.attributes, Some(unwrapped.info))
            }
            Ok(None) => (message.body.clone(), message.attributes.clone(), None),
            Err(error) => {
                self.metrics.decode_failures.inc();
                return Some(Delivery { lease, sns: None, decoded: Err(error), received_at });
            }
        };
        if sns.is_some() {
            // Attributes added at the SQS level by fanferro (retries, dead-lettering) survive
            // re-enqueueing of SNS-wrapped messages.
            for name in [reserved::ATTEMPT, reserved::DEAD_LETTER] {
                if let Some(value) = message.attributes.get(name) {
                    attributes.entry(name.to_string()).or_insert_with(|| value.clone());
                }
            }
        }
        let decoded = self.client.transport().decode(&body, attributes).await;
        match &decoded {
            Ok(DecodedBody { pointer: Some(pointer), wire }) => {
                self.metrics.payloads_fetched.inc();
                self.client.events().emit(EventKind::PayloadFetched, || {
                    Event::new(EventKind::PayloadFetched, "offloaded payload fetched")
                        .field("queue", self.queue())
                        .field("sqs_message_id", &message.message_id)
                        .field("size", wire.body.len())
                        .field_opt("bucket", pointer.bucket.clone())
                        .field_opt("key", pointer.key.clone())
                });
            }
            Ok(_) => {}
            Err(_) => self.metrics.decode_failures.inc(),
        }
        Some(Delivery { lease, sns, decoded, received_at })
    }

    async fn handle_sns_control(&self, lease: Lease, info: SnsInfo) {
        let confirm = self.settings.auto_confirm_subscriptions && info.kind == SnsMessageKind::SubscriptionConfirmation;
        if confirm && let Some(token) = &info.token {
            match self.client.backend().sns.confirm_subscription(&info.topic_arn, token).await {
                Ok(subscription_arn) => self.client.events().emit(EventKind::SubscriptionConfirmed, || {
                    Event::new(EventKind::SubscriptionConfirmed, "SNS subscription confirmed")
                        .field("queue", self.queue())
                        .field("topic_arn", &info.topic_arn)
                        .field("subscription_arn", subscription_arn)
                }),
                Err(error) => {
                    self.client.events().emit(EventKind::ConfigWarning, || {
                        Event::new(EventKind::ConfigWarning, "failed to confirm SNS subscription; it will be retried")
                            .field("queue", self.queue())
                            .field("topic_arn", &info.topic_arn)
                            .error(&error)
                    });
                    let _ = lease.nack(Duration::from_secs(30)).await;
                    return;
                }
            }
        } else {
            self.client.events().emit(EventKind::ConfigWarning, || {
                Event::new(
                    EventKind::ConfigWarning,
                    "dropped an SNS subscription control message; enable auto_confirm_subscriptions or confirm it manually",
                )
                .field("queue", self.queue())
                .field("topic_arn", &info.topic_arn)
                .field("sns_type", format!("{:?}", info.kind))
            });
        }
        let _ = lease.ack().await;
    }

    pub(crate) async fn wait_unpaused(&self) -> bool {
        let mut receiver = self.paused.subscribe();
        loop {
            if !*receiver.borrow_and_update() {
                return true;
            }
            tokio::select! {
                () = self.receive_token.cancelled() => return false,
                changed = receiver.changed() => if changed.is_err() { return false },
            }
        }
    }
}

/// Statistics of a running engine.
#[derive(Clone, Debug)]
pub struct EngineStats {
    /// Lifecycle state.
    pub state: EngineState,
    /// Whether receiving is paused.
    pub paused: bool,
    /// Leases currently tracked.
    pub leases: usize,
    /// Free in-flight slots.
    pub available_permits: usize,
    /// Counter and gauge snapshot.
    pub metrics: BTreeMap<&'static str, i64>,
}

/// Receives messages from one queue and hands them out as [`Delivery`] values.
#[derive(Clone)]
pub struct ConsumerEngine {
    inner: Arc<EngineInner>,
}

impl std::fmt::Debug for ConsumerEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerEngine")
            .field("queue", &self.inner.settings.queue_url)
            .field("state", &self.state())
            .finish()
    }
}

impl ConsumerEngine {
    /// Creates an engine; must be called within a Tokio runtime.
    pub fn new(client: &Client, config: ConsumerConfig) -> Result<Self> {
        let settings = config.validate()?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::config("ConsumerEngine::new must be called within a Tokio runtime"))?;
        let (deliveries_tx, deliveries_rx) = mpsc::channel(settings.max_in_flight);
        let rate_limiter = settings.rate_limit.map(|(per_second, burst)| RateLimiter::new(per_second, burst));
        Ok(Self {
            inner: Arc::new(EngineInner {
                client: client.clone(),
                permits: Arc::new(Semaphore::new(settings.max_in_flight)),
                settling: Arc::new(Semaphore::new(settings.max_settling)),
                leases: Mutex::new(HashMap::new()),
                metrics: Arc::new(ConsumerMetrics::default()),
                next_lease_id: AtomicU64::new(1),
                deliveries_tx,
                deliveries_rx: tokio::sync::Mutex::new(deliveries_rx),
                settle_tx: Mutex::new(None),
                receive_token: CancellationToken::new(),
                abort_token: CancellationToken::new(),
                shutdown_token: CancellationToken::new(),
                paused: watch::Sender::new(false),
                state: Mutex::new(EngineState::Created),
                pollers: Mutex::new(Vec::new()),
                background: Mutex::new(Vec::new()),
                decoders: TaskTracker::new(),
                rate_limiter,
                runtime,
                orphaned_receives_until: Mutex::new(None),
                settings,
            }),
        })
    }

    /// The queue URL.
    pub fn queue_url(&self) -> &str {
        &self.inner.settings.queue_url
    }

    /// Current lifecycle state.
    pub fn state(&self) -> EngineState {
        *self.inner.state.lock()
    }

    /// Starts receiving.
    pub fn start(&self) -> Result<()> {
        {
            let mut state = self.inner.state.lock();
            if *state != EngineState::Created {
                return Err(Error::config(format!("cannot start a consumer engine in state {}", state.as_str())));
            }
            *state = EngineState::Running;
        }
        let inner = &self.inner;
        let (settle_tx, settle_rx) = mpsc::unbounded_channel();
        *inner.settle_tx.lock() = Some(settle_tx);
        let settle_task = inner.runtime.spawn(settle::settle_loop(
            inner.client.clone(),
            inner.settings.queue_url.clone(),
            settle_rx,
            inner.settings.settle_linger,
            inner.settings.settle_retry.clone(),
            inner.settings.settle_backoff,
        ));
        let mut background = vec![settle_task];
        if inner.settings.heartbeat_enabled {
            background.push(inner.runtime.spawn(heartbeat::heartbeat_loop(inner.clone())));
        }
        *inner.background.lock() = background;
        let pollers =
            (0..inner.settings.pollers).map(|_| inner.runtime.spawn(poller::poll_loop(inner.clone()))).collect();
        *inner.pollers.lock() = pollers;
        inner.client.events().emit(EventKind::ConsumerStarted, || {
            Event::new(EventKind::ConsumerStarted, "consumer started")
                .field("queue", inner.queue())
                .field("max_in_flight", inner.settings.max_in_flight)
                .field("pollers", inner.settings.pollers)
        });
        Ok(())
    }

    /// Waits up to `timeout` for deliveries and returns at most `max` of them. Returns an
    /// empty list on timeout or once receiving has stopped and the buffer is empty.
    ///
    /// Buffered deliveries are always returned first, also after receiving stopped; messages
    /// that are still being decoded when receiving stops are left for [`Self::drain_buffered`]
    /// (after [`Self::stop_receiving`] returned).
    pub async fn next_batch(&self, max: usize, timeout: Duration) -> Vec<Delivery> {
        let mut receiver = self.inner.deliveries_rx.lock().await;
        let mut deliveries = Vec::new();
        if max == 0 {
            return deliveries;
        }
        tokio::select! {
            biased;
            _ = receiver.recv_many(&mut deliveries, max) => {}
            () = self.inner.receive_token.cancelled() => {}
            () = self.inner.shutdown_token.cancelled() => {}
            () = tokio::time::sleep(timeout) => {}
        }
        self.inner.metrics.buffered.add(-i64::try_from(deliveries.len()).unwrap_or(0));
        deliveries
    }

    /// Returns buffered deliveries without waiting.
    pub fn drain_buffered(&self) -> Vec<Delivery> {
        let Ok(mut receiver) = self.inner.deliveries_rx.try_lock() else { return Vec::new() };
        let mut deliveries = Vec::new();
        while let Ok(delivery) = receiver.try_recv() {
            deliveries.push(delivery);
        }
        self.inner.metrics.buffered.add(-i64::try_from(deliveries.len()).unwrap_or(0));
        deliveries
    }

    /// Pauses receiving (in-flight messages keep their leases).
    pub fn pause(&self) {
        if !self.inner.paused.send_replace(true) {
            self.inner.client.events().emit(EventKind::ConsumerPaused, || {
                Event::new(EventKind::ConsumerPaused, "consumer paused").field("queue", self.inner.queue())
            });
        }
    }

    /// Resumes receiving.
    pub fn resume(&self) {
        if self.inner.paused.send_replace(false) {
            self.inner.client.events().emit(EventKind::ConsumerResumed, || {
                Event::new(EventKind::ConsumerResumed, "consumer resumed").field("queue", self.inner.queue())
            });
        }
    }

    /// Whether receiving is paused.
    pub fn is_paused(&self) -> bool {
        *self.inner.paused.borrow()
    }

    /// Stops receiving and waits until every received message has been decoded into the
    /// delivery buffer.
    ///
    /// By default a long poll that is in flight is allowed to finish (at most `wait_time`) and
    /// whatever it returns is released immediately. Aborting it instead (`abort = true`) stops
    /// faster, but the queue keeps serving the abandoned poll on its side and may hand it
    /// messages that then stay invisible until their visibility timeout expires; messages this
    /// engine releases afterwards are therefore kept invisible until such polls are over.
    pub async fn stop_receiving(&self, abort: bool) {
        {
            let mut state = self.inner.state.lock();
            if *state == EngineState::Running || *state == EngineState::Created {
                *state = EngineState::Stopping;
            }
        }
        if abort
            && !self.inner.abort_token.is_cancelled()
            && self.inner.client.backend().kind == crate::backend::BackendKind::Aws
            && !self.inner.pollers.lock().is_empty()
        {
            *self.inner.orphaned_receives_until.lock() = Some(Instant::now() + self.inner.settings.wait_time);
        }
        self.inner.receive_token.cancel();
        if abort {
            self.inner.abort_token.cancel();
        }
        let pollers: Vec<_> = self.inner.pollers.lock().drain(..).collect();
        for poller in pollers {
            let _ = poller.await;
        }
        self.inner.decoders.close();
        self.inner.decoders.wait().await;
    }

    /// Closes the engine: releases (or abandons) unsettled messages, flushes pending
    /// settlements and stops the heartbeat. Call after [`ConsumerEngine::stop_receiving`]
    /// and after in-flight handlers finished or were cancelled.
    pub async fn close(&self, release_unsettled: bool) {
        self.stop_receiving(false).await;
        for delivery in self.drain_buffered() {
            if release_unsettled {
                let _ = delivery.lease.release().await;
            }
        }
        let remaining: Vec<(u64, LeaseEntry)> = self.inner.leases.lock().drain().collect();
        self.inner.metrics.in_flight.add(-i64::try_from(remaining.len()).unwrap_or(0));
        if release_unsettled && let Some(sender) = self.inner.settle_sender() {
            let mut replies = Vec::new();
            for (_, entry) in remaining {
                if matches!(entry.status, LeaseStatus::Buffered | LeaseStatus::Active) {
                    let (reply, receiver) = tokio::sync::oneshot::channel();
                    if sender
                        .send(SettleCommand {
                            receipt_handle: entry.receipt_handle,
                            kind: SettleKind::Visibility(self.inner.visibility_floor(Duration::ZERO)),
                            reply: Some(reply),
                        })
                        .is_ok()
                    {
                        self.inner.metrics.messages_released.inc();
                        replies.push(receiver);
                    }
                }
            }
            for receiver in replies {
                let _ = receiver.await;
            }
        }
        self.inner.settle_tx.lock().take();
        self.inner.shutdown_token.cancel();
        let background: Vec<_> = self.inner.background.lock().drain(..).collect();
        for task in background {
            let _ = task.await;
        }
        let was = std::mem::replace(&mut *self.inner.state.lock(), EngineState::Stopped);
        if was != EngineState::Stopped {
            self.inner.client.events().emit(EventKind::ConsumerStopped, || {
                Event::new(EventKind::ConsumerStopped, "consumer stopped").field("queue", self.inner.queue())
            });
        }
    }

    /// A snapshot of the engine's statistics.
    pub fn stats(&self) -> EngineStats {
        EngineStats {
            state: self.state(),
            paused: self.is_paused(),
            leases: self.inner.leases.lock().len(),
            available_permits: self.inner.permits.available_permits(),
            metrics: self.inner.metrics.snapshot(),
        }
    }

    /// The engine's metrics.
    pub fn metrics(&self) -> &Arc<ConsumerMetrics> {
        &self.inner.metrics
    }
}

#[cfg(test)]
mod tests;
