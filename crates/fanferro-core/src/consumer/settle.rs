//! Batched settlement: deletes and visibility changes grouped into batch calls.
//!
//! The first pending settlement opens a batch that stays open for at most `linger`. It is sent
//! as soon as one call's worth (ten deletes or ten visibility changes) is pending, or when a
//! settlement is marked urgent (its caller is blocked on it and nothing else is expected
//! soon), or when the linger expires. Settlements already queued at that point go along.

use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use tokio::sync::{mpsc, oneshot};
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

pub(crate) struct SettleCommand {
    pub receipt_handle: String,
    pub kind: SettleKind,
    pub reply: Option<oneshot::Sender<Result<()>>>,
    /// Send the open batch now instead of waiting to fill it.
    pub urgent: bool,
    /// When the command was queued (the linger counts from the oldest pending command).
    pub queued_at: Instant,
}

impl SettleCommand {
    pub(crate) fn new(receipt_handle: String, kind: SettleKind, reply: Option<oneshot::Sender<Result<()>>>) -> Self {
        Self { receipt_handle, kind, reply, urgent: false, queued_at: Instant::now() }
    }
}

/// Settlements collected for the next round of batch calls.
struct Pending {
    commands: Vec<SettleCommand>,
    deletes: usize,
    urgent: bool,
}

impl Pending {
    fn new(first: SettleCommand) -> Self {
        let mut pending = Self { commands: Vec::new(), deletes: 0, urgent: false };
        pending.push(first);
        pending
    }

    fn push(&mut self, command: SettleCommand) {
        self.deletes += usize::from(command.kind == SettleKind::Delete);
        self.urgent |= command.urgent;
        self.commands.push(command);
    }

    /// Whether waiting longer cannot make the next call fuller (or a caller cannot wait).
    fn ready(&self) -> bool {
        self.urgent || self.deletes >= MAX_BATCH_ENTRIES || self.commands.len() - self.deletes >= MAX_BATCH_ENTRIES
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
        let _ = reply.send(result);
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

/// Runs until every sender is dropped, then flushes what is left and returns.
pub(crate) async fn settle_loop(context: SettleContext, mut receiver: mpsc::UnboundedReceiver<SettleCommand>) {
    while let Some(first) = receiver.recv().await {
        let deadline = first.queued_at + context.linger;
        let mut pending = Pending::new(first);
        while !pending.ready() {
            // Already queued settlements join without waiting for the deadline.
            let command = match receiver.try_recv() {
                Ok(command) => Some(command),
                Err(mpsc::error::TryRecvError::Disconnected) => None,
                Err(mpsc::error::TryRecvError::Empty) => tokio::select! {
                    command = receiver.recv() => command,
                    () = tokio::time::sleep_until(deadline) => None,
                },
            };
            match command {
                Some(command) => pending.push(command),
                None => break,
            }
        }
        while pending.commands.len() < MAX_BATCH_ENTRIES * 10 {
            match receiver.try_recv() {
                Ok(command) => pending.push(command),
                Err(_) => break,
            }
        }
        let (deletes, changes): (Vec<_>, Vec<_>) =
            pending.commands.into_iter().partition(|c| c.kind == SettleKind::Delete);
        let mut flushes = Vec::new();
        let mut deletes = deletes;
        while !deletes.is_empty() {
            let rest = deletes.split_off(deletes.len().min(MAX_BATCH_ENTRIES));
            flushes.push(flush(&context, std::mem::replace(&mut deletes, rest)));
        }
        let mut changes = changes;
        while !changes.is_empty() {
            let rest = changes.split_off(changes.len().min(MAX_BATCH_ENTRIES));
            flushes.push(flush(&context, std::mem::replace(&mut changes, rest)));
        }
        join_all(flushes).await;
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
