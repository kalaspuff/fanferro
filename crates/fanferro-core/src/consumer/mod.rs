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
//! * `pollers` receive loops always run; while receives come back full and a batch of in-flight
//!   permits is free, extra loops start (up to `max_pollers`), each ending at its first receive
//!   that is not full, so the receives in flight follow the backlog and the free handler slots.
//!   Each extra loop may add `extra_poller_slots` in-flight permits while it runs (the slots its
//!   round trips hold), which are taken back once they are free after it ended.
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
pub mod records;
mod settle;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
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
use crate::envelope::EnvelopeSet;
use crate::error::{Error, Result};
use crate::events::{Event, EventKind};
use crate::metrics::ConsumerMetrics;
use crate::retry::RateLimiter;
use crate::transport::DecodedBody;
use crate::transport::sns_notification::{self, SnsInfo, SnsMessageKind};
use crate::types::attributes::{md5_of_attributes, md5_of_body};
use crate::types::{Payload, reserved};
use config::Settings;
use lease::LeaseEntry;
use settle::{Reply, SettleCommand, SettleKind, SettleQueue};

/// A received message handed to the application.
#[derive(Debug)]
pub struct Delivery {
    /// The lease (settle the message through it).
    pub lease: Lease,
    /// SNS metadata when the message arrived as an SNS notification.
    pub sns: Option<SnsInfo>,
    /// The transport-decoded message, or why decoding failed.
    pub decoded: std::result::Result<DecodedBody, Error>,
    /// What the engine's envelopes made of the decoded message (see
    /// [`ConsumerEngine::with_envelopes`]).
    pub unwrapped: Unwrapped,
    /// When the message was received.
    pub received_at: OffsetDateTime,
}

/// The outcome of unwrapping a delivery with the engine's envelopes.
#[derive(Debug)]
pub enum Unwrapped {
    /// The engine has no envelopes, or transport decoding failed.
    NotAttempted,
    /// The envelope at position `envelope` recognised the message.
    Payload {
        /// The payload.
        payload: Payload,
        /// Position of the envelope in the engine's set.
        envelope: usize,
    },
    /// No envelope recognised the message.
    Unrecognised {
        /// The names of the envelopes that were tried, comma-separated.
        tried: String,
    },
    /// The envelope that recognised the message could not parse it.
    Failed(Error),
}

impl Delivery {
    /// The raw message as received from SQS.
    pub fn message(&self) -> &ReceivedMessage {
        self.lease.message()
    }

    /// Acks the message without waiting for the delete. The settlement runs on the engine's
    /// runtime and is tracked ([`ConsumerEngine::close`] waits for it); its outcome is reported
    /// through metrics (`messages_acked`, `ack_failures`) and events (`message.acked`,
    /// `message.ack_failed`).
    ///
    /// # Errors
    ///
    /// [`Error::Lease`] if the message was settled (or its settlement started) already.
    pub fn ack_in_background(self: &Arc<Self>) -> Result<()> {
        self.lease.claim()?;
        // Failures are reported by the lease (metrics and events).
        self.continue_ack(|_| {});
        Ok(())
    }

    /// Acks the message and hands the outcome to `done`. The delete is queued before this
    /// returns (when a settling slot is free), so acks requested one after another reach the
    /// settle loop in that order; `done` runs once the delete completed.
    pub fn spawn_ack<Done>(self: &Arc<Self>, done: Done)
    where
        Done: FnOnce(Result<()>) + Send + 'static,
    {
        match self.lease.claim() {
            Ok(()) => self.continue_ack(done),
            Err(error) => done(Err(error)),
        }
    }

    /// An ack after its lease was claimed. Usually the delete is queued right away and finished by
    /// the settle loop (no task per ack); without a free settling slot, a tracked task waits for
    /// one.
    fn continue_ack<Done>(self: &Arc<Self>, done: Done)
    where
        Done: FnOnce(Result<()>) + Send + 'static,
    {
        let delivery = self.clone();
        let finish = Reply::Callback(Box::new(move |result: Result<()>| {
            delivery.lease.finish_ack(&result);
            done(result);
        }));
        if let Err(finish) = self.lease.try_queue_ack(finish) {
            let delivery = self.clone();
            let inner = &self.lease.inner;
            inner.settlements.spawn_on(
                async move {
                    delivery.lease.start_settling().await;
                    delivery.lease.queue(SettleKind::Delete, finish);
                },
                &inner.runtime,
            );
        }
    }

    /// Runs `settle` (an ack, retry, release... of this delivery) on the engine's runtime and
    /// hands its outcome to `done`. Like background acks, the settlement is tracked:
    /// [`ConsumerEngine::close`] waits for it, and it completes even if nobody waits for `done`.
    pub fn spawn_settlement<Settle, Fut, Done>(self: &Arc<Self>, settle: Settle, done: Done)
    where
        Settle: FnOnce(Arc<Self>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
        Done: FnOnce(Result<()>) + Send + 'static,
    {
        let delivery = self.clone();
        let inner = &self.lease.inner;
        inner.settlements.spawn_on(async move { done(settle(delivery).await) }, &inner.runtime);
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
    /// Decoded deliveries, one entry per received batch (dispatched together).
    deliveries_tx: mpsc::Sender<Vec<Delivery>>,
    deliveries_rx: tokio::sync::Mutex<DeliveryBuffer>,
    /// Pending settlements (set when the engine starts).
    settle: OnceLock<Arc<SettleQueue>>,
    receive_token: CancellationToken,
    abort_token: CancellationToken,
    shutdown_token: CancellationToken,
    paused: watch::Sender<bool>,
    state: Mutex<EngineState>,
    pollers: Mutex<Pollers>,
    /// Receive calls in flight (see [`EngineInner::begin_receive`]).
    receives: Mutex<ReceivesInFlight>,
    /// Leases dropped without a settlement are left to time out instead of released (see
    /// [`ConsumerEngine::leave_unsettled`]).
    pub(crate) leave_dropped: AtomicBool,
    background: Mutex<Vec<JoinHandle<()>>>,
    decoders: TaskTracker,
    /// Settlements running in the background (see [`Delivery::ack_in_background`]).
    settlements: TaskTracker,
    rate_limiter: Option<RateLimiter>,
    runtime: tokio::runtime::Handle,
    /// Until when an aborted long poll may still be served by the queue (see
    /// [`EngineInner::visibility_floor`]).
    orphaned_receives_until: Mutex<Option<Instant>>,
    /// Envelopes applied while decoding (see [`ConsumerEngine::with_envelopes`]).
    envelopes: Option<EnvelopeSet>,
    /// Receipt handles of messages settled with a delete, which an engine with
    /// `external` messages records instead of sending (see [`ConsumerEngine::take_deleted`]).
    pub(crate) deleted: Mutex<Vec<String>>,
    /// How receiving goes (see [`ConsumerEngine::receive_health`]).
    health: Mutex<HealthState>,
}

/// How the receive loops are doing (see [`ConsumerEngine::receive_health`]).
#[derive(Clone, Debug)]
pub struct ReceiveHealth {
    /// When a receive last succeeded (an empty one included).
    pub last_receive_at: Option<OffsetDateTime>,
    /// How long the engine has been receiving without a success: the time since the last
    /// successful receive (or the start), leaving out the time its receive loops waited for free
    /// in-flight slots, the rate limiter or a resume, when it did not want messages. Zero for an
    /// engine that does not receive itself (`external`).
    pub receive_gap: Duration,
    /// Receives that failed since the last one that succeeded.
    pub consecutive_failures: u32,
    /// The latest failed receive (also after later receives succeeded).
    pub last_error: Option<ReceiveFailure>,
}

/// A failed receive (see [`ReceiveHealth::last_error`]).
#[derive(Clone, Debug)]
pub struct ReceiveFailure {
    /// Why the receive failed.
    pub error: Error,
    /// When it failed.
    pub at: OffsetDateTime,
}

/// What [`ReceiveHealth`] is made of.
#[derive(Debug)]
struct HealthState {
    last_success: Option<OffsetDateTime>,
    /// Where the receive gap starts (moved forward by the time spent waiting, see
    /// [`EngineInner::not_receiving`]).
    gap_since: Instant,
    /// Receive loops waiting for capacity, and since when at least one did.
    waiting: usize,
    waiting_since: Instant,
    consecutive_failures: u32,
    last_error: Option<ReceiveFailure>,
}

impl HealthState {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            last_success: None,
            gap_since: now,
            waiting: 0,
            waiting_since: now,
            consecutive_failures: 0,
            last_error: None,
        }
    }

    fn gap(&self, now: Instant) -> Duration {
        let until = if self.waiting > 0 { self.waiting_since } else { now };
        until.saturating_duration_since(self.gap_since)
    }
}

/// A receive loop waiting for capacity (see [`EngineInner::not_receiving`]); ends the wait when
/// dropped.
pub(crate) struct NotReceiving<'a>(&'a EngineInner);

impl Drop for NotReceiving<'_> {
    fn drop(&mut self) {
        let mut health = self.0.health.lock();
        health.waiting -= 1;
        if health.waiting == 0 {
            // The time every waiting loop spent does not count: the engine did not want messages.
            let now = Instant::now();
            let from = health.waiting_since.max(health.gap_since);
            health.gap_since += now.saturating_duration_since(from);
        }
    }
}

/// The receive loops: `pollers` that always run, and extra ones while the queue keeps receives
/// full (see [`EngineInner::add_poller`]).
#[derive(Debug, Default)]
struct Pollers {
    handles: Vec<JoinHandle<()>>,
    /// Loops running.
    active: usize,
    /// In-flight permits that ended extra loops added and that are still to be taken back
    /// (they were in use when the loop ended).
    surplus: usize,
}

/// A running receive loop; unregisters it when dropped.
pub(crate) struct PollerSlot {
    pub(crate) inner: Arc<EngineInner>,
    /// In-flight permits the loop added (see `extra_poller_slots`).
    added: usize,
}

impl Drop for PollerSlot {
    fn drop(&mut self) {
        let mut pollers = self.inner.pollers.lock();
        pollers.active -= 1;
        pollers.surplus += self.added;
        self.inner.take_back_surplus(&mut pollers);
        self.inner.metrics.pollers.add(-1);
    }
}

/// The `ReceiveMessage` calls in flight.
#[derive(Debug, Default)]
struct ReceivesInFlight {
    count: usize,
    /// When the latest call was sent.
    latest_start: Option<Instant>,
}

/// A registered receive call (see [`EngineInner::begin_receive`]); unregisters it when dropped.
pub(crate) struct ReceiveInFlight<'a>(&'a EngineInner);

impl Drop for ReceiveInFlight<'_> {
    fn drop(&mut self) {
        self.0.receives.lock().count -= 1;
    }
}

/// The receiving end of the delivery channel plus deliveries a smaller `next_batch` left over.
struct DeliveryBuffer {
    receiver: mpsc::Receiver<Vec<Delivery>>,
    leftover: VecDeque<Delivery>,
}

impl DeliveryBuffer {
    /// Moves up to `max` deliveries (leftovers first) into `deliveries`.
    fn take(&mut self, max: usize, deliveries: &mut Vec<Delivery>) {
        while self.leftover.len() < max
            && let Ok(batch) = self.receiver.try_recv()
        {
            self.leftover.extend(batch);
        }
        let count = max.min(self.leftover.len());
        deliveries.extend(self.leftover.drain(..count));
    }
}

impl EngineInner {
    /// The settle queue, once the engine started.
    pub(crate) fn settle_queue(&self) -> Option<&SettleQueue> {
        self.settle.get().map(Arc::as_ref)
    }

    fn queue(&self) -> &str {
        &self.settings.queue_url
    }

    /// How long a visibility settlement waits for a heartbeat extension in flight: the extension
    /// call gives up after [`heartbeat::call_timeout`].
    pub(crate) fn extension_wait(&self) -> Duration {
        heartbeat::call_timeout(self.settings.heartbeat_interval) + Duration::from_secs(1)
    }

    /// Waits until a heartbeat extension of lease `id` that is in flight ended, so that a
    /// visibility settlement (a retry, a release) reaches SQS after it: sent before, it would be
    /// overridden by the extension, and the message would stay hidden for another `extend_by`.
    pub(crate) async fn after_extension(&self, id: u64) {
        let extension = self.leases.lock().get(&id).and_then(|entry| entry.extending.clone());
        if let Some(extension) = extension {
            let _ = tokio::time::timeout(self.extension_wait(), extension.cancelled()).await;
        }
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

    /// Registers a `ReceiveMessage` call about to be sent, for as long as the returned guard lives;
    /// `None` once receiving stopped (the call must not be sent then: a stop that aborts the
    /// calls in flight guards against those it saw, see [`EngineInner::visibility_floor`]).
    pub(crate) fn begin_receive(&self) -> Option<ReceiveInFlight<'_>> {
        let mut receives = self.receives.lock();
        if self.receive_token.is_cancelled() {
            return None;
        }
        receives.count += 1;
        receives.latest_start = Some(Instant::now());
        Some(ReceiveInFlight(self))
    }

    /// Starts another receive loop after a full receive, if a full batch of in-flight slots is
    /// free and fewer than `max_pollers` loops run. It stops at its first receive that does not
    /// come back full.
    pub(crate) fn add_poller(self: &Arc<Self>) {
        let mut pollers = self.pollers.lock();
        // Under the lock: `stop_receiving` cancels before it takes the loops to wait for.
        if self.receive_token.is_cancelled() || pollers.active >= self.settings.max_pollers || self.is_paused() {
            return;
        }
        self.take_back_surplus(&mut pollers);
        if self.permits.available_permits() < usize::from(self.settings.batch_size) {
            return;
        }
        pollers.handles.retain(|handle| !handle.is_finished());
        let handle = self.spawn_poller(&mut pollers, true);
        pollers.handles.push(handle);
    }

    /// Spawns a receive loop (`extra`: one that ends at its first receive that is not full, and
    /// adds `extra_poller_slots` in-flight permits while it runs).
    fn spawn_poller(self: &Arc<Self>, pollers: &mut Pollers, extra: bool) -> JoinHandle<()> {
        pollers.active += 1;
        self.metrics.pollers.add(1);
        let added = if extra { self.settings.extra_poller_slots } else { 0 };
        // Permits an ended loop left behind are reused before new ones are added.
        let reused = added.min(pollers.surplus);
        pollers.surplus -= reused;
        self.permits.add_permits(added - reused);
        let slot = PollerSlot { inner: self.clone(), added };
        self.runtime.spawn(poller::poll_loop(slot, extra))
    }

    /// Takes back the free permits ended extra loops added (see [`Pollers::surplus`]).
    fn take_back_surplus(&self, pollers: &mut Pollers) {
        if pollers.surplus > 0 {
            pollers.surplus -= self.permits.forget_permits(pollers.surplus);
        }
    }

    /// Takes back the free permits ended extra loops added, before a receive loop takes permits.
    pub(crate) fn trim_permits(&self) {
        let mut pollers = self.pollers.lock();
        self.take_back_surplus(&mut pollers);
    }

    /// Registers leases for received messages and decodes them in order.
    pub(crate) fn accept(self: &Arc<Self>, messages: Vec<ReceivedMessage>, permits: Vec<OwnedSemaphorePermit>) {
        let now = Instant::now();
        let mut leased = Vec::with_capacity(messages.len());
        {
            let mut leases = self.leases.lock();
            for (message, permit) in messages.into_iter().zip(permits) {
                let id = self.next_lease_id.fetch_add(1, Ordering::Relaxed);
                let message = Arc::new(message);
                leases.insert(id, self.lease_entry(&message, now));
                leased.push(Lease::new(id, self.clone(), message, Some(permit)));
            }
        }
        self.received(&leased);
        let inner = self.clone();
        self.decoders.spawn(async move {
            // Decoded concurrently (claim-check downloads overlap), handed over in order and
            // together, so the dispatcher sees the whole receive batch at once.
            let decode = futures::future::join_all(leased.into_iter().map(|lease| inner.decode(lease)));
            // A stop that aborts receiving does not wait for downloads (which may take minutes,
            // past a container's grace period, #188): dropping the decodes drops their leases,
            // which releases the messages (or leaves them, after `leave_unsettled`).
            let abort = inner.abort_token.clone();
            let decoded = tokio::select! {
                biased;
                decoded = decode => decoded,
                () = abort.cancelled() => return,
            };
            let deliveries: Vec<Delivery> = decoded.into_iter().flatten().collect();
            if deliveries.is_empty() {
                return;
            }
            let count = i64::try_from(deliveries.len()).unwrap_or(i64::MAX);
            inner.metrics.buffered.add(count);
            if let Err(error) = inner.deliveries_tx.send(deliveries).await {
                // The engine is gone; dropping the deliveries releases the messages.
                inner.metrics.buffered.add(-count);
                drop(error);
            }
        });
    }

    fn lease_entry(&self, message: &Arc<ReceivedMessage>, now: Instant) -> LeaseEntry {
        LeaseEntry {
            message: message.clone(),
            received_at: now,
            visible_until: now + self.settings.visibility_timeout,
            next_heartbeat: now + self.settings.heartbeat_interval,
            max_until: now + self.settings.max_lease,
            status: LeaseStatus::Buffered,
            extending: None,
        }
    }

    /// Counts newly leased messages and reports them (`message.received`).
    fn received(&self, leased: &[Lease]) {
        self.metrics.messages_received.add(leased.len() as u64);
        self.metrics.in_flight.add(i64::try_from(leased.len()).unwrap_or(i64::MAX));
        for lease in leased {
            let message = lease.message();
            self.client.events().emit(EventKind::MessageReceived, || {
                Event::new(EventKind::MessageReceived, "message received")
                    .field("queue", self.queue())
                    .field("sqs_message_id", &message.message_id)
                    .field("receive_count", message.system.approximate_receive_count)
                    .field_opt("group_id", message.system.message_group_id.clone())
            });
        }
    }

    async fn decode(self: &Arc<Self>, lease: Lease) -> Option<Delivery> {
        let received_at = OffsetDateTime::now_utc();
        let message = lease.message.clone();
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
                    unwrapped: Unwrapped::NotAttempted,
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
                (Cow::Owned(unwrapped.message), unwrapped.attributes, Some(unwrapped.info))
            }
            Ok(None) => (Cow::Borrowed(message.body.as_str()), message.attributes.clone(), None),
            Err(error) => {
                self.metrics.decode_failures.inc();
                return Some(Delivery {
                    lease,
                    sns: None,
                    decoded: Err(error),
                    unwrapped: Unwrapped::NotAttempted,
                    received_at,
                });
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
        let unwrapped = match (&self.envelopes, &decoded) {
            (Some(envelopes), Ok(decoded)) => match envelopes.unwrap_indexed(&decoded.wire) {
                Ok(Some((payload, envelope))) => Unwrapped::Payload { payload, envelope },
                Ok(None) => Unwrapped::Unrecognised {
                    tried: envelopes.envelopes().iter().map(|e| e.name()).collect::<Vec<_>>().join(", "),
                },
                Err(error) => Unwrapped::Failed(error),
            },
            _ => Unwrapped::NotAttempted,
        };
        Some(Delivery { lease, sns, decoded, unwrapped, received_at })
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
            let message = match info.kind {
                SnsMessageKind::UnsubscribeConfirmation => {
                    "dropped an SNS unsubscribe confirmation: the queue's subscription to the topic was removed"
                }
                _ if confirm => "dropped an SNS subscription confirmation without a token; confirm it manually",
                _ => {
                    "dropped an SNS subscription confirmation; enable auto_confirm_subscriptions or confirm it manually"
                }
            };
            self.client.events().emit(EventKind::ConfigWarning, || {
                Event::new(EventKind::ConfigWarning, message)
                    .field("queue", self.queue())
                    .field("topic_arn", &info.topic_arn)
                    .field("sns_type", format!("{:?}", info.kind))
            });
        }
        let _ = lease.ack().await;
    }

    pub(crate) fn is_paused(&self) -> bool {
        *self.paused.borrow()
    }

    /// Marks a receive loop as waiting for capacity (free in-flight slots, the rate limiter, a
    /// resume) until the returned guard is dropped: that time does not count in the receive gap.
    pub(crate) fn not_receiving(&self) -> NotReceiving<'_> {
        let mut health = self.health.lock();
        if health.waiting == 0 {
            health.waiting_since = Instant::now();
        }
        health.waiting += 1;
        NotReceiving(self)
    }

    /// Records a receive that succeeded (also an empty one).
    pub(crate) fn receive_succeeded(&self) {
        let mut health = self.health.lock();
        health.last_success = Some(OffsetDateTime::now_utc());
        health.gap_since = Instant::now();
        health.consecutive_failures = 0;
    }

    /// Records a receive that failed.
    pub(crate) fn receive_failed(&self, error: &Error) {
        let mut health = self.health.lock();
        health.consecutive_failures = health.consecutive_failures.saturating_add(1);
        health.last_error = Some(ReceiveFailure { error: error.clone(), at: OffsetDateTime::now_utc() });
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
        Self::with_envelopes(client, config, None)
    }

    /// Creates an engine that also unwraps the envelopes of received messages while decoding them
    /// (see [`Delivery::unwrapped`]), off the thread that handles the deliveries.
    pub fn with_envelopes(client: &Client, config: ConsumerConfig, envelopes: Option<EnvelopeSet>) -> Result<Self> {
        let settings = config.validate()?;
        if config.min_receive_batch.is_some() && config.requested_min_receive_batch() > settings.max_in_flight {
            client.events().emit(EventKind::ConfigWarning, || {
                Event::new(
                    EventKind::ConfigWarning,
                    format!(
                        "min_receive_batch ({}) exceeds the in-flight limit ({}); receives ask for at most {} messages",
                        config.requested_min_receive_batch(),
                        settings.max_in_flight,
                        settings.min_receive_batch
                    ),
                )
                .field("queue", settings.queue_url.as_str())
                .field("min_receive_batch", settings.min_receive_batch)
                .field("max_in_flight", settings.max_in_flight)
            });
        }
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::config("ConsumerEngine::new must be called within a Tokio runtime"))?;
        // Every delivery holds an in-flight permit, so the channel never holds more batches.
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
                deliveries_rx: tokio::sync::Mutex::new(DeliveryBuffer {
                    receiver: deliveries_rx,
                    leftover: VecDeque::new(),
                }),
                settle: OnceLock::new(),
                receive_token: CancellationToken::new(),
                abort_token: CancellationToken::new(),
                shutdown_token: CancellationToken::new(),
                paused: watch::Sender::new(false),
                state: Mutex::new(EngineState::Created),
                pollers: Mutex::new(Pollers::default()),
                receives: Mutex::new(ReceivesInFlight::default()),
                // Messages received by someone else are left to them (see `ConsumerConfig::external`).
                leave_dropped: AtomicBool::new(settings.external),
                background: Mutex::new(Vec::new()),
                decoders: TaskTracker::new(),
                settlements: TaskTracker::new(),
                rate_limiter,
                runtime,
                orphaned_receives_until: Mutex::new(None),
                envelopes,
                deleted: Mutex::new(Vec::new()),
                health: Mutex::new(HealthState::new()),
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
        let queue = inner.settle.get_or_init(Arc::default).clone();
        let settle_task = inner.runtime.spawn(settle::settle_loop(
            settle::SettleContext {
                client: inner.client.clone(),
                queue_url: inner.settings.queue_url.clone(),
                metrics: inner.metrics.clone(),
                linger: inner.settings.settle_linger,
                retry: inner.settings.settle_retry.clone(),
                backoff: inner.settings.settle_backoff,
            },
            queue,
        ));
        // The receive gap starts now.
        inner.health.lock().gap_since = Instant::now();
        let mut background = vec![settle_task];
        if inner.settings.heartbeat_enabled {
            background.push(inner.runtime.spawn(heartbeat::heartbeat_loop(inner.clone())));
        }
        *inner.background.lock() = background;
        if !inner.settings.external {
            let mut pollers = inner.pollers.lock();
            for _ in 0..inner.settings.pollers {
                let handle = inner.spawn_poller(&mut pollers, false);
                pollers.handles.push(handle);
            }
        }
        inner.client.events().emit(EventKind::ConsumerStarted, || {
            Event::new(EventKind::ConsumerStarted, "consumer started")
                .field("queue", inner.queue())
                .field("max_in_flight", inner.settings.max_in_flight)
                .field("min_receive_batch", inner.settings.min_receive_batch)
                .field("pollers", inner.settings.pollers)
                .field("max_pollers", inner.settings.max_pollers)
        });
        Ok(())
    }

    /// Leases and decodes messages that someone else received (an AWS Lambda event source
    /// mapping), like received messages, and returns their deliveries in order (SNS subscription
    /// confirmations are handled and left out, as when receiving). Settling them records deletes
    /// instead of sending them ([`Self::take_deleted`]); visibility changes, dead-letter and
    /// requeue copies are sent. Their in-flight slots are not limited: whoever received the
    /// messages bounds how many arrive at once.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] unless the engine was configured with `external` messages and is
    /// running.
    pub async fn accept_records(&self, messages: Vec<ReceivedMessage>) -> Result<Vec<Delivery>> {
        let inner = &self.inner;
        if !inner.settings.external {
            return Err(Error::config("accept_records needs a consumer engine configured with external = true"));
        }
        let state = self.state();
        if state != EngineState::Running {
            return Err(Error::config(format!("cannot accept records in consumer engine state {}", state.as_str())));
        }
        let now = Instant::now();
        let leased: Vec<Lease> = {
            let mut leases = inner.leases.lock();
            messages
                .into_iter()
                .map(|message| {
                    let id = inner.next_lease_id.fetch_add(1, Ordering::Relaxed);
                    let message = Arc::new(message);
                    leases.insert(id, inner.lease_entry(&message, now));
                    Lease::new(id, inner.clone(), message, None)
                })
                .collect()
        };
        inner.received(&leased);
        let decoded = futures::future::join_all(leased.into_iter().map(|lease| inner.decode(lease))).await;
        Ok(decoded.into_iter().flatten().collect())
    }

    /// Takes the receipt handles of the messages settled with a delete (acked, dead-lettered,
    /// requeued) since the last call, in an engine with `external` messages: whoever received
    /// them deletes them (see [`Self::accept_records`]).
    pub fn take_deleted(&self) -> Vec<String> {
        std::mem::take(&mut *self.inner.deleted.lock())
    }

    /// Waits until the settlements running in the background (acks that were not awaited...)
    /// completed.
    pub async fn wait_settlements(&self) {
        let settlements = &self.inner.settlements;
        settlements.close();
        settlements.wait().await;
        settlements.reopen();
    }

    /// Waits up to `timeout` for deliveries and returns at most `max` of them. Returns an
    /// empty list on timeout or once receiving has stopped and the buffer is empty.
    ///
    /// Buffered deliveries are always returned first, also after receiving stopped; messages
    /// that are still being decoded when receiving stops are left for [`Self::drain_buffered`]
    /// (after [`Self::stop_receiving`] returned).
    pub async fn next_batch(&self, max: usize, timeout: Duration) -> Vec<Delivery> {
        let mut buffer = self.inner.deliveries_rx.lock().await;
        let mut deliveries = Vec::new();
        if max == 0 {
            return deliveries;
        }
        if buffer.leftover.is_empty() {
            let mut batches = Vec::new();
            tokio::select! {
                biased;
                _ = buffer.receiver.recv_many(&mut batches, max) => {}
                () = self.inner.receive_token.cancelled() => {}
                () = self.inner.shutdown_token.cancelled() => {}
                () = tokio::time::sleep(timeout) => {}
            }
            buffer.leftover.extend(batches.into_iter().flatten());
        }
        buffer.take(max, &mut deliveries);
        self.inner.metrics.buffered.add(-i64::try_from(deliveries.len()).unwrap_or(0));
        deliveries
    }

    /// Returns buffered deliveries without waiting.
    pub fn drain_buffered(&self) -> Vec<Delivery> {
        let Ok(mut buffer) = self.inner.deliveries_rx.try_lock() else { return Vec::new() };
        let mut deliveries = Vec::new();
        buffer.take(usize::MAX, &mut deliveries);
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
        self.inner.is_paused()
    }

    /// Stops receiving and waits until every received message has been decoded into the
    /// delivery buffer (with `abort`, messages still being decoded, such as payloads being
    /// downloaded, are released instead).
    ///
    /// By default a long poll that is in flight is allowed to finish (at most `wait_time`) and
    /// whatever it returns is released immediately. Aborting it instead (`abort = true`) stops
    /// faster, but the queue keeps serving the abandoned poll on its side and may hand it
    /// messages that then stay invisible until their visibility timeout expires; messages this
    /// engine releases afterwards are therefore kept invisible until such polls are over. A call
    /// with `abort` also cuts short a call without it that is still waiting for its poll.
    pub async fn stop_receiving(&self, abort: bool) {
        {
            let mut state = self.inner.state.lock();
            if *state == EngineState::Running || *state == EngineState::Created {
                *state = EngineState::Stopping;
            }
        }
        // Cancelled before the calls in flight are looked at: pollers register a call only while
        // receiving goes on (see `begin_receive`), so none is sent unseen.
        self.inner.receive_token.cancel();
        if abort && !self.inner.abort_token.is_cancelled() && self.inner.client.backend().orphaned_polls {
            let receives = self.inner.receives.lock();
            if receives.count > 0
                && let Some(started) = receives.latest_start
            {
                // The latest call is served the longest: up to its wait time after it was sent.
                *self.inner.orphaned_receives_until.lock() = Some(started + self.inner.settings.wait_time);
            }
        }
        if abort {
            self.inner.abort_token.cancel();
        }
        let pollers = std::mem::take(&mut self.inner.pollers.lock().handles);
        for poller in pollers {
            let _ = poller.await;
        }
        self.inner.decoders.close();
        self.inner.decoders.wait().await;
    }

    /// From now on, deliveries dropped without a settlement are left to time out instead of
    /// released (a stop that does not release unfinished messages: handlers that are cancelled,
    /// messages still waiting for one).
    pub fn leave_unsettled(&self) {
        self.inner.leave_dropped.store(true, Ordering::SeqCst);
    }

    /// Closes the engine: releases (or abandons) unsettled messages, flushes pending
    /// settlements and stops the heartbeat. Call after [`ConsumerEngine::stop_receiving`]
    /// and after in-flight handlers finished or were cancelled.
    pub async fn close(&self, release_unsettled: bool) {
        if !release_unsettled {
            self.leave_unsettled();
        }
        self.stop_receiving(false).await;
        // Background acks complete first: they need the settle loop, which stops below.
        self.inner.settlements.close();
        self.inner.settlements.wait().await;
        let buffered = self.drain_buffered();
        if release_unsettled {
            // Together: one batch call per ten messages instead of one round trip each.
            futures::future::join_all(buffered.iter().map(|delivery| delivery.lease.release())).await;
        }
        drop(buffered);
        let remaining: Vec<(u64, LeaseEntry)> = self.inner.leases.lock().drain().collect();
        self.inner.metrics.in_flight.add(-i64::try_from(remaining.len()).unwrap_or(0));
        if release_unsettled && let Some(queue) = self.inner.settle_queue() {
            // After the extensions in flight, which would hide the messages again.
            let wait = self.inner.extension_wait();
            let extensions = remaining.iter().filter_map(|(_, entry)| entry.extending.clone());
            futures::future::join_all(extensions.map(|extension| async move {
                let _ = tokio::time::timeout(wait, extension.cancelled_owned()).await;
            }))
            .await;
            let mut replies = Vec::new();
            for (_, entry) in remaining {
                if matches!(entry.status, LeaseStatus::Buffered | LeaseStatus::Active) {
                    let (reply, receiver) = tokio::sync::oneshot::channel();
                    let mut command = SettleCommand::new(
                        entry.message.receipt_handle.clone(),
                        SettleKind::Visibility(self.inner.visibility_floor(Duration::ZERO)),
                        Some(Reply::Channel(reply)),
                    );
                    command.urgent = true;
                    if queue.push(command).is_ok() {
                        self.inner.metrics.messages_released.inc();
                        replies.push(receiver);
                    }
                }
            }
            for receiver in replies {
                let _ = receiver.await;
            }
        }
        if let Some(queue) = self.inner.settle_queue() {
            queue.close();
        }
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

    /// How receiving goes: when a receive last succeeded, for how long none has, the failures
    /// since and the latest one. Readiness and liveness probes build on it.
    pub fn receive_health(&self) -> ReceiveHealth {
        let health = self.inner.health.lock();
        let receive_gap = if self.inner.settings.external { Duration::ZERO } else { health.gap(Instant::now()) };
        ReceiveHealth {
            last_receive_at: health.last_success,
            receive_gap,
            consecutive_failures: health.consecutive_failures,
            last_error: health.last_error.clone(),
        }
    }

    /// The engine's metrics.
    pub fn metrics(&self) -> &Arc<ConsumerMetrics> {
        &self.inner.metrics
    }
}

#[cfg(test)]
mod tests;
