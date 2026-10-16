//! Batched settlement: deletes and visibility changes grouped into batch calls.
//!
//! Every ten deletes (or ten visibility changes) are sent at once, also while earlier calls are
//! still in flight. Fewer wait for more, at most `linger` after the oldest of them was queued; they
//! are sent earlier when nothing else can join them (every handler waits for one of these
//! settlements, see [`SettleCommand::waiting_handlers`]) or when a settlement is urgent
//! (shutdown).
//!
//! Leases queue settlements in a [`SettleQueue`] shared with the settle loop. Queueing wakes the
//! loop only when it can then send something sooner than planned: for the first pending settlement
//! (which starts the linger), a full batch, or an urgent or awaited settlement. Most settlements
//! (background acks, many at a time) therefore cost no task wake-up of their own.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use tokio::sync::{Notify, oneshot};
use tokio::time::Instant;

use crate::backend::{BatchEntryError, VisibilityChange};
use crate::client::Client;
use crate::error::{AwsError, Error, Result};
use crate::metrics::ConsumerMetrics;
use crate::publish::MAX_BATCH_ENTRIES;
use crate::retry::{Backoff, RetrySettings};

/// What to do with a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SettleKind {
    /// `DeleteMessage`.
    Delete,
    /// `ChangeMessageVisibility` to the given timeout.
    Visibility(Duration),
}

/// Where the outcome of a settlement goes.
pub(crate) enum Reply {
    /// A task awaits it.
    Channel(oneshot::Sender<Result<()>>),
    /// Runs in the settle loop right after the batch call (keep it short: no I/O, no waiting).
    Callback(Box<dyn FnOnce(Result<()>) + Send>),
}

impl Reply {
    pub(crate) fn send(self, result: Result<()>) {
        match self {
            Self::Channel(sender) => {
                let _ = sender.send(result);
            }
            Self::Callback(callback) => callback(result),
        }
    }
}

pub(crate) struct SettleCommand {
    pub receipt_handle: String,
    pub kind: SettleKind,
    pub reply: Option<Reply>,
    /// Send the open batch now instead of waiting to fill it.
    pub urgent: bool,
    /// A handler waits for this settlement (an explicit `await message.ack()`...).
    pub awaited: bool,
    /// Set when every handler that could request another settlement waits for one: how many
    /// settlements are awaited (by handlers, or by FIFO message groups whose next message
    /// waits). Once the batch holds that many awaited settlements, nothing else can join it;
    /// while fewer are pending, some of them were sent already, and their completion will soon
    /// make room for handlers that settle more.
    pub waiting_handlers: Option<usize>,
    /// When the command was queued (the linger counts from the oldest pending command).
    pub queued_at: Instant,
}

impl SettleCommand {
    pub(crate) fn new(receipt_handle: String, kind: SettleKind, reply: Option<Reply>) -> Self {
        Self {
            receipt_handle,
            kind,
            reply,
            urgent: false,
            awaited: false,
            waiting_handlers: None,
            queued_at: Instant::now(),
        }
    }
}

/// Settlements waiting to be sent, shared by the leases that queue them and the settle loop.
#[derive(Default)]
pub(crate) struct SettleQueue {
    pending: Mutex<Pending>,
    wake: Notify,
}

impl SettleQueue {
    /// Queues a settlement (hands it back when the consumer is closed).
    pub(crate) fn push(&self, command: SettleCommand) -> std::result::Result<(), SettleCommand> {
        let wake = {
            let mut pending = self.pending.lock();
            if pending.closed {
                return Err(command);
            }
            let first = pending.is_empty();
            let hurry = command.urgent || command.awaited || command.waiting_handlers.is_some();
            let full = pending.push(command);
            first || hurry || full
        };
        if wake {
            self.wake.notify_one();
        }
        Ok(())
    }

    /// Stops accepting settlements: the loop sends what is pending, waits for its calls and returns.
    pub(crate) fn close(&self) {
        self.pending.lock().closed = true;
        self.wake.notify_one();
    }
}

/// Settlements waiting to be sent (each kind becomes its own batch calls).
#[derive(Default)]
struct Pending {
    deletes: Vec<SettleCommand>,
    changes: Vec<SettleCommand>,
    /// Send the partial batches now (an urgent settlement arrived).
    send_now: bool,
    /// See [`SettleCommand::waiting_handlers`] (the latest value since partial batches were sent).
    waiting_handlers: Option<usize>,
    closed: bool,
}

impl Pending {
    /// Adds a settlement; returns whether it completed a batch.
    fn push(&mut self, command: SettleCommand) -> bool {
        self.send_now |= command.urgent;
        if command.waiting_handlers.is_some() {
            self.waiting_handlers = command.waiting_handlers;
        }
        let commands = if command.kind == SettleKind::Delete { &mut self.deletes } else { &mut self.changes };
        commands.push(command);
        commands.len() % MAX_BATCH_ENTRIES == 0
    }

    fn is_empty(&self) -> bool {
        self.deletes.is_empty() && self.changes.is_empty()
    }

    /// When the oldest pending settlement was queued.
    fn oldest(&self) -> Option<Instant> {
        self.deletes.iter().chain(&self.changes).map(|command| command.queued_at).min()
    }

    /// Whether waiting longer cannot make the partial batches fuller (or a caller cannot wait).
    fn complete(&self) -> bool {
        let awaited = || self.deletes.iter().chain(&self.changes).filter(|command| command.awaited).count();
        self.send_now || self.waiting_handlers.is_some_and(|waiting| awaited() >= waiting)
    }

    /// Full batches, and the partial ones too when `partial` is set.
    fn take(&mut self, partial: bool) -> Vec<Vec<SettleCommand>> {
        let mut batches = Vec::new();
        for commands in [&mut self.deletes, &mut self.changes] {
            while commands.len() >= MAX_BATCH_ENTRIES || (partial && !commands.is_empty()) {
                let rest = commands.split_off(commands.len().min(MAX_BATCH_ENTRIES));
                batches.push(std::mem::replace(commands, rest));
            }
        }
        if partial {
            self.send_now = false;
            self.waiting_handlers = None;
        }
        batches
    }
}

fn entry_error(operation: &'static str, entry: &BatchEntryError) -> Error {
    let mut kind = crate::backend::aws::classify(Some(&entry.code), entry.message.as_deref(), None);
    if !entry.sender_fault && !kind.is_retryable() {
        kind = crate::error::AwsErrorKind::Transient;
    }
    let mut error = AwsError::new("sqs", operation, kind, entry.message.clone().unwrap_or_default());
    error.code = Some(entry.code.clone());
    Error::from(error)
}

fn reply(command: SettleCommand, result: Result<()>) {
    if let Some(reply) = command.reply {
        reply.send(result);
    }
}

/// Everything the settle loop needs besides its commands.
pub(crate) struct SettleContext {
    pub client: Client,
    pub queue_url: String,
    pub metrics: Arc<ConsumerMetrics>,
    pub linger: Duration,
    pub retry: RetrySettings,
    pub backoff: Backoff,
}

/// Sends what `queue` collects until it is closed, then sends what is left, waits for the calls in
/// flight and returns.
pub(crate) async fn settle_loop(context: SettleContext, queue: Arc<SettleQueue>) {
    let mut in_flight = FuturesUnordered::new();
    loop {
        let (batches, deadline, done) = {
            let mut pending = queue.pending.lock();
            let lingered = pending.oldest().is_some_and(|oldest| oldest + context.linger <= Instant::now());
            let partial = pending.closed || lingered || pending.complete();
            let batches = pending.take(partial);
            let deadline = pending.oldest().map(|oldest| oldest + context.linger);
            (batches, deadline, pending.closed && pending.is_empty())
        };
        for batch in batches {
            in_flight.push(flush(&context, batch));
        }
        if done {
            while in_flight.next().await.is_some() {}
            return;
        }
        tokio::select! {
            () = queue.wake.notified() => {}
            Some(()) = in_flight.next(), if !in_flight.is_empty() => {}
            () = sleep_until_some(deadline), if deadline.is_some() => {}
        }
    }
}

async fn sleep_until_some(deadline: Option<Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    }
}

/// Sends one batch (all deletes or all visibility changes), retrying retryable failures.
async fn flush(context: &SettleContext, mut pending: Vec<SettleCommand>) {
    let SettleContext { client, queue_url, metrics, retry, backoff, .. } = context;
    let max_attempts = retry.max_attempts.max(1);
    let mut attempt = 0;
    while !pending.is_empty() {
        let is_delete = pending[0].kind == SettleKind::Delete;
        let operation = if is_delete { "DeleteMessageBatch" } else { "ChangeMessageVisibilityBatch" };
        if is_delete {
            metrics.delete_requests.inc();
        } else {
            metrics.visibility_requests.inc();
        }
        let call = if is_delete {
            client
                .backend()
                .sqs
                .delete_message_batch(queue_url, pending.iter().map(|c| c.receipt_handle.clone()).collect())
                .await
        } else {
            let entries = pending
                .iter()
                .map(|c| VisibilityChange {
                    receipt_handle: c.receipt_handle.clone(),
                    timeout: match c.kind {
                        SettleKind::Visibility(timeout) => timeout,
                        SettleKind::Delete => Duration::ZERO,
                    },
                })
                .collect();
            client.backend().sqs.change_visibility_batch(queue_url, entries).await
        };
        let last_attempt = attempt + 1 >= max_attempts;
        match call {
            Ok(results) => {
                let mut retry_later = Vec::new();
                for (command, result) in pending.into_iter().zip(results) {
                    match result {
                        Ok(()) => reply(command, Ok(())),
                        Err(entry) => {
                            let error = entry_error(operation, &entry);
                            if error.is_retryable() && !last_attempt {
                                retry_later.push(command);
                            } else {
                                reply(command, Err(error));
                            }
                        }
                    }
                }
                pending = retry_later;
            }
            Err(error) if error.is_retryable() && !last_attempt => {}
            Err(error) => {
                for command in pending {
                    reply(command, Err(error.clone()));
                }
                return;
            }
        }
        if pending.is_empty() {
            return;
        }
        tokio::time::sleep(backoff.delay(attempt)).await;
        attempt += 1;
    }
}
