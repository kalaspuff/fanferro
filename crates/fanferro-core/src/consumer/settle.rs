//! Batched settlement: deletes and visibility changes grouped into batch calls.

use std::time::Duration;

use futures::future::join_all;
use tokio::sync::{mpsc, oneshot};

use crate::backend::{BatchEntryError, VisibilityChange};
use crate::client::Client;
use crate::error::{AwsError, Error, Result};
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

/// Runs until every sender is dropped, then flushes what is left and returns.
pub(crate) async fn settle_loop(
    client: Client,
    queue_url: String,
    mut receiver: mpsc::UnboundedReceiver<SettleCommand>,
    linger: Duration,
    retry: RetrySettings,
    backoff: Backoff,
) {
    while let Some(first) = receiver.recv().await {
        let mut batch = vec![first];
        if !linger.is_zero() {
            let deadline = tokio::time::Instant::now() + linger;
            while batch.len() < MAX_BATCH_ENTRIES * 2 {
                tokio::select! {
                    command = receiver.recv() => match command {
                        Some(command) => batch.push(command),
                        None => break,
                    },
                    () = tokio::time::sleep_until(deadline) => break,
                }
            }
        }
        while batch.len() < MAX_BATCH_ENTRIES * 10 {
            match receiver.try_recv() {
                Ok(command) => batch.push(command),
                Err(_) => break,
            }
        }
        let (deletes, changes): (Vec<_>, Vec<_>) = batch.into_iter().partition(|c| c.kind == SettleKind::Delete);
        let mut flushes = Vec::new();
        let mut deletes = deletes;
        while !deletes.is_empty() {
            let rest = deletes.split_off(deletes.len().min(MAX_BATCH_ENTRIES));
            flushes.push(flush(&client, &queue_url, std::mem::replace(&mut deletes, rest), &retry, backoff));
        }
        let mut changes = changes;
        while !changes.is_empty() {
            let rest = changes.split_off(changes.len().min(MAX_BATCH_ENTRIES));
            flushes.push(flush(&client, &queue_url, std::mem::replace(&mut changes, rest), &retry, backoff));
        }
        join_all(flushes).await;
    }
}

/// Sends one batch (all deletes or all visibility changes), retrying retryable failures.
async fn flush(
    client: &Client,
    queue_url: &str,
    mut pending: Vec<SettleCommand>,
    retry: &RetrySettings,
    backoff: Backoff,
) {
    let max_attempts = retry.max_attempts.max(1);
    let mut attempt = 0;
    while !pending.is_empty() {
        let is_delete = pending[0].kind == SettleKind::Delete;
        let operation = if is_delete { "DeleteMessageBatch" } else { "ChangeMessageVisibilityBatch" };
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
