//! Inspecting and redriving dead-letter queues.
//!
//! [`Client::peek`] receives messages of a queue, holds them hidden until it has collected the
//! requested number and then makes them visible again, so it returns distinct messages without
//! taking them away from the queue for longer than the call.
//!
//! [`Client::redrive`] moves messages of a dead-letter queue back to the queue they came from
//! (the `source` of fanferro's `fanferro.dead-letter` attribute, or the
//! `DeadLetterQueueSourceArn` SQS sets when its redrive policy moved the message) or to another
//! queue. Each message is sent with its body and attributes unchanged except for
//! `fanferro.dead-letter`, and by default `fanferro.attempt`, which `RetryPolicy(strategy="requeue")`
//! counts attempts with: a redriven message starts with a fresh retry budget. It is deleted from
//! the dead-letter queue only once the send succeeded. Messages that cannot be moved (no known
//! source, a source that does not exist, a failed send) stay in the dead-letter queue and are
//! listed in the report.
//!
//! Both work with the operations every backend has (receive, send, change visibility, delete).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use tokio::time::Instant;

use crate::backend::{ReceiveRequest, ReceivedMessage, SendRequest, VisibilityChange};
use crate::client::Client;
use crate::error::{Error, Result};
use crate::publish::entry_error;
use crate::types::arn::Arn;
use crate::types::reserved;

/// The most messages one receive, send or delete batch carries.
const BATCH: usize = 10;

/// Options of [`Client::peek`].
#[derive(Clone, Debug)]
pub struct PeekOptions {
    /// How many messages to return at most.
    pub limit: usize,
    /// How long the messages stay hidden while the peek collects them (they are made visible
    /// again when it returns; if it is interrupted, when this runs out).
    pub visibility_timeout: Duration,
    /// Long polling wait time of each receive (SQS samples a subset of its servers without it).
    pub wait_time: Duration,
}

impl Default for PeekOptions {
    fn default() -> Self {
        Self { limit: BATCH, visibility_timeout: Duration::from_secs(30), wait_time: Duration::from_secs(1) }
    }
}

/// Options of [`Client::redrive`].
#[derive(Clone, Debug)]
pub struct RedriveOptions {
    /// The queue URL to move every message to; `None` moves each message to its source queue.
    pub to: Option<String>,
    /// How many messages to move at most (`None`: until the queue is empty).
    pub limit: Option<usize>,
    /// The most messages to move per second (`None`: as fast as the calls go).
    pub max_per_second: Option<f64>,
    /// Remove `fanferro.attempt`, so that the moved message starts with a fresh retry budget.
    pub reset_attempts: bool,
    /// How long received messages stay hidden while they are moved; the messages that are not
    /// moved are made visible again at the end.
    pub visibility_timeout: Duration,
    /// Long polling wait time of each receive.
    pub wait_time: Duration,
}

impl Default for RedriveOptions {
    fn default() -> Self {
        Self {
            to: None,
            limit: None,
            max_per_second: None,
            reset_attempts: true,
            visibility_timeout: Duration::from_secs(60),
            wait_time: Duration::from_secs(1),
        }
    }
}

impl RedriveOptions {
    fn validate(&self) -> Result<()> {
        if self.limit == Some(0) {
            return Err(Error::validation("limit must be at least 1 (or None for every message)"));
        }
        if let Some(rate) = self.max_per_second
            && !(rate.is_finite() && rate > 0.0)
        {
            return Err(Error::validation(format!("max_per_second must be a positive number, not {rate}")));
        }
        validate_receive(self.visibility_timeout, self.wait_time)
    }
}

fn validate_receive(visibility_timeout: Duration, wait_time: Duration) -> Result<()> {
    if visibility_timeout.is_zero() || visibility_timeout > Duration::from_secs(12 * 3600) {
        return Err(Error::validation("visibility_timeout must be between 1 second and 12 hours"));
    }
    if wait_time > Duration::from_secs(20) {
        return Err(Error::validation("wait_time must be at most 20 seconds"));
    }
    Ok(())
}

/// A message [`Client::redrive`] left in the dead-letter queue, and why.
#[derive(Clone, Debug)]
pub struct NotMoved {
    /// SQS message id in the dead-letter queue.
    pub sqs_message_id: String,
    /// Where it was to be moved, when that was known.
    pub destination: Option<String>,
    /// Why it was not moved.
    pub error: Error,
}

/// What [`Client::redrive`] did.
#[derive(Clone, Debug, Default)]
pub struct RedriveReport {
    /// How many messages were moved.
    pub moved: usize,
    /// How many messages were moved to each queue URL.
    pub destinations: BTreeMap<String, usize>,
    /// The messages that stay in the dead-letter queue.
    pub not_moved: Vec<NotMoved>,
    /// Messages that were sent to their destination but could not be deleted from the
    /// dead-letter queue: they are in both queues now (and counted in `moved`).
    pub not_deleted: Vec<NotMoved>,
}

/// The queue a dead-lettered message came from: the `source` fanferro recorded, or the queue
/// SQS's redrive policy moved it from.
pub fn dead_letter_source(message: &ReceivedMessage) -> Option<String> {
    let recorded = message
        .attributes
        .get(reserved::DEAD_LETTER)
        .and_then(|value| value.as_text())
        .and_then(|text| serde_json::from_str::<serde_json::Value>(text).ok())
        .and_then(|info| info.get("source").and_then(|source| source.as_str()).map(str::to_owned))
        .filter(|source| !source.is_empty());
    recorded.or_else(|| message.system.dead_letter_queue_source_arn.clone())
}

impl Client {
    /// Returns up to `options.limit` messages of a queue without settling them.
    ///
    /// The messages are received (which counts as a receive in their `ApproximateReceiveCount`,
    /// and so towards the `maxReceiveCount` of a redrive policy the queue may have), held hidden
    /// until enough are collected or a receive returns none, then made visible again.
    pub async fn peek(&self, queue_url: &str, options: &PeekOptions) -> Result<Vec<ReceivedMessage>> {
        validate_receive(options.visibility_timeout, options.wait_time)?;
        let sqs = &self.backend().sqs;
        let mut messages: Vec<ReceivedMessage> = Vec::new();
        let mut received = Ok(());
        while messages.len() < options.limit {
            let request = ReceiveRequest {
                queue_url: queue_url.to_owned(),
                max_messages: (options.limit - messages.len()).min(BATCH) as u8,
                wait_time: options.wait_time,
                visibility_timeout: Some(options.visibility_timeout),
                attempt_id: None,
            };
            match sqs.receive_messages(request).await {
                Ok(batch) if batch.is_empty() => break,
                Ok(batch) => messages.extend(batch),
                Err(error) => {
                    received = Err(error);
                    break;
                }
            }
        }
        release(self, queue_url, messages.iter().map(|message| message.receipt_handle.clone()).collect()).await;
        received.map(|()| messages)
    }

    /// Moves the messages of a dead-letter queue to their source queue or to `options.to`.
    ///
    /// Runs until the queue has no visible messages left, `options.limit` messages were moved,
    /// or a receive fails (the error is returned; messages moved until then stay moved).
    pub async fn redrive(&self, queue_url: &str, options: &RedriveOptions) -> Result<RedriveReport> {
        options.validate()?;
        if options.to.as_deref() == Some(queue_url) {
            return Err(Error::validation("cannot redrive a queue to itself"));
        }
        let mut run = Redrive::new(self, queue_url, options);
        let result = run.run().await;
        let held = run.held.drain().map(|(_, handle)| handle).collect();
        release(self, queue_url, held).await;
        result.map(|()| run.report)
    }
}

/// Makes messages visible again (best effort: those not released reappear when their visibility
/// timeout runs out).
async fn release(client: &Client, queue_url: &str, receipt_handles: Vec<String>) {
    for chunk in receipt_handles.chunks(BATCH) {
        let entries =
            chunk.iter().map(|handle| VisibilityChange { receipt_handle: handle.clone(), timeout: Duration::ZERO });
        let _ = client.backend().sqs.change_visibility_batch(queue_url, entries.collect()).await;
    }
}

struct Redrive<'a> {
    client: &'a Client,
    queue_url: &'a str,
    options: &'a RedriveOptions,
    report: RedriveReport,
    /// Messages left in the queue during this run: their latest receipt handle by message id.
    held: HashMap<String, String>,
    /// Queue URLs of sources, by source (or why the source cannot be used).
    urls: HashMap<String, std::result::Result<String, Error>>,
    /// FIFO message groups (by destination) with a message that was not moved: later messages of
    /// the group are not moved either, to keep the group's order.
    blocked_groups: HashSet<(String, String)>,
    started: Instant,
    sent: usize,
}

impl<'a> Redrive<'a> {
    fn new(client: &'a Client, queue_url: &'a str, options: &'a RedriveOptions) -> Self {
        Self {
            client,
            queue_url,
            options,
            report: RedriveReport::default(),
            held: HashMap::new(),
            urls: HashMap::new(),
            blocked_groups: HashSet::new(),
            started: Instant::now(),
            sent: 0,
        }
    }

    async fn run(&mut self) -> Result<()> {
        loop {
            let mut wanted = BATCH;
            if let Some(limit) = self.options.limit {
                if self.report.moved >= limit {
                    return Ok(());
                }
                wanted = wanted.min(limit - self.report.moved);
            }
            if let Some(rate) = self.options.max_per_second {
                wanted = wanted.min(rate.ceil() as usize).max(1);
            }
            let request = ReceiveRequest {
                queue_url: self.queue_url.to_owned(),
                max_messages: wanted as u8,
                wait_time: self.options.wait_time,
                visibility_timeout: Some(self.options.visibility_timeout),
                attempt_id: None,
            };
            let batch = self.client.backend().sqs.receive_messages(request).await?;
            let mut fresh = Vec::with_capacity(batch.len());
            for message in batch {
                // A message left in the queue earlier came back: hold it with its new handle.
                match self.held.get_mut(&message.message_id) {
                    Some(handle) => *handle = message.receipt_handle,
                    None => fresh.push(message),
                }
            }
            if fresh.is_empty() {
                // Nothing visible, or only what this run already left in the queue.
                return Ok(());
            }
            self.move_batch(fresh).await;
        }
    }

    async fn destination(&mut self, message: &ReceivedMessage) -> std::result::Result<String, Error> {
        if let Some(to) = &self.options.to {
            return Ok(to.clone());
        }
        let Some(source) = dead_letter_source(message) else {
            return Err(Error::validation(
                "the message records no source queue (no fanferro.dead-letter attribute or DeadLetterQueueSourceArn); redrive it to a queue of your choice",
            ));
        };
        if let Some(url) = self.urls.get(&source) {
            return url.clone();
        }
        let url = self.source_url(&source).await;
        self.urls.insert(source, url.clone());
        url
    }

    async fn source_url(&self, source: &str) -> std::result::Result<String, Error> {
        if source.contains("://") {
            return Ok(source.to_owned());
        }
        let arn = Arn::parse(source)?;
        if arn.service != "sqs" {
            return Err(Error::validation(format!("the source {source:?} is not an SQS queue")));
        }
        self.client.backend().sqs.get_queue_url(arn.resource_name(), Some(&arn.account)).await
    }

    async fn move_batch(&mut self, messages: Vec<ReceivedMessage>) {
        // Per destination, the messages to send, in the order they were received.
        let mut sends: BTreeMap<String, Vec<ReceivedMessage>> = BTreeMap::new();
        for message in messages {
            match self.destination(&message).await {
                Ok(url) if url == self.queue_url => {
                    self.not_moved(message, Some(url), Error::validation("the message's source is this queue"));
                }
                Ok(url) => sends.entry(url).or_default().push(message),
                Err(error) => self.not_moved(message, None, error),
            }
        }
        for (url, messages) in sends {
            self.send(url, messages).await;
        }
    }

    fn not_moved(&mut self, message: ReceivedMessage, destination: Option<String>, error: Error) {
        self.held.insert(message.message_id.clone(), message.receipt_handle);
        self.report.not_moved.push(NotMoved { sqs_message_id: message.message_id, destination, error });
    }

    fn request(&self, message: &ReceivedMessage, fifo: bool) -> SendRequest {
        let mut attributes = message.attributes.clone();
        attributes.remove(reserved::DEAD_LETTER);
        if self.options.reset_attempts {
            attributes.remove(reserved::ATTEMPT);
        }
        SendRequest {
            body: message.body.clone(),
            attributes,
            delay: None,
            group_id: fifo
                .then(|| message.system.message_group_id.clone().unwrap_or_else(|| "fanferro-redrive".into())),
            // The dead-letter copy's own id: a redrive that is run again after a failed delete
            // within SQS's 5-minute deduplication interval does not send it twice.
            deduplication_id: fifo.then(|| message.message_id.clone()),
            trace_header: message.system.aws_trace_header.clone(),
        }
    }

    async fn send(&mut self, url: String, messages: Vec<ReceivedMessage>) {
        let fifo = url.ends_with(".fifo");
        let mut pending = messages.into_iter().peekable();
        while pending.peek().is_some() {
            let mut chunk = Vec::with_capacity(BATCH);
            while chunk.len() < BATCH
                && let Some(message) = pending.next()
            {
                let group = (url.clone(), message.system.message_group_id.clone().unwrap_or_default());
                if fifo && self.blocked_groups.contains(&group) {
                    let error = Error::validation("an earlier message of its FIFO message group was not moved");
                    self.not_moved(message, Some(url.clone()), error);
                } else {
                    chunk.push(message);
                }
            }
            if chunk.is_empty() {
                continue;
            }
            self.throttle(chunk.len()).await;
            let entries = chunk.iter().map(|message| self.request(message, fifo)).collect();
            let results = match self.client.backend().sqs.send_message_batch(&url, entries).await {
                Ok(results) => results
                    .into_iter()
                    .map(|result| result.map(|_| ()).map_err(|e| entry_error("sqs", "SendMessageBatch", &e)))
                    .collect(),
                Err(error) => vec![Err(error); chunk.len()],
            };
            let mut sent = Vec::with_capacity(chunk.len());
            for (message, result) in chunk.into_iter().zip(results) {
                match result {
                    Ok(()) => sent.push(message),
                    Err(error) => {
                        if fifo {
                            let group = message.system.message_group_id.clone().unwrap_or_default();
                            self.blocked_groups.insert((url.clone(), group));
                        }
                        self.not_moved(message, Some(url.clone()), error);
                    }
                }
            }
            self.delete(&url, sent).await;
        }
    }

    async fn delete(&mut self, url: &str, sent: Vec<ReceivedMessage>) {
        if sent.is_empty() {
            return;
        }
        self.report.moved += sent.len();
        *self.report.destinations.entry(url.to_owned()).or_default() += sent.len();
        let handles = sent.iter().map(|message| message.receipt_handle.clone()).collect();
        let results = match self.client.backend().sqs.delete_message_batch(self.queue_url, handles).await {
            Ok(results) => results
                .into_iter()
                .map(|result| result.map_err(|e| entry_error("sqs", "DeleteMessageBatch", &e)))
                .collect(),
            Err(error) => vec![Err(error); sent.len()],
        };
        for (message, result) in sent.into_iter().zip(results) {
            if let Err(error) = result {
                self.report.not_deleted.push(NotMoved {
                    sqs_message_id: message.message_id,
                    destination: Some(url.to_owned()),
                    error,
                });
            }
        }
    }

    /// Waits until sending `count` more messages keeps to `max_per_second`.
    async fn throttle(&mut self, count: usize) {
        if let Some(rate) = self.options.max_per_second {
            let due = self.started + Duration::from_secs_f64(self.sent as f64 / rate);
            tokio::time::sleep_until(due).await;
        }
        self.sent += count;
    }
}

#[cfg(test)]
mod tests;
