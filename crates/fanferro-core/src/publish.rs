//! Sending to queues and publishing to topics.
//!
//! Idempotency: every outgoing message carries a client-generated message id that is
//! stable across retries. FIFO destinations use it as the deduplication id unless one is
//! given or content-based deduplication is enabled, so retried sends are deduplicated by
//! SQS/SNS. Standard destinations rely on consumers deduplicating by message id.

use std::time::Duration;

use futures::StreamExt as _;
use serde::Deserialize;

use crate::backend::aws::classify;
use crate::backend::{BatchEntryError, PublishOutput, PublishRequest, SendOutput, SendRequest};
use crate::client::Client;
use crate::error::{AwsError, Error, Result};
use crate::events::{Event, EventKind};
use crate::retry::Backoff;
use crate::transport::{OutgoingBody, PayloadPointer, SNS_DEFAULT_MAX_MESSAGE_SIZE, SQS_MAX_MESSAGE_SIZE, Transport};
use crate::types::WireMessage;
use crate::types::attributes::{md5_of_attributes, md5_of_body};

/// Maximum entries per batch call.
pub const MAX_BATCH_ENTRIES: usize = 10;
/// Batch calls of one `publish_batch` in flight at once (standard destinations; FIFO destinations
/// send their batches one after another, and retry a batch's failed entries before the next batch, so
/// that message groups keep their order).
pub const MAX_CONCURRENT_BATCH_CALLS: usize = 8;
/// Messages of one `publish_batch` prepared at once: each may upload its payload to S3, and a large
/// batch must not open a connection per message.
pub const MAX_CONCURRENT_UPLOADS: usize = 16;
/// Maximum per-message delay for SQS standard queues.
pub const MAX_DELAY: Duration = Duration::from_secs(900);

/// Where a message goes.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Destination {
    /// An SQS queue.
    Queue {
        /// Queue URL.
        url: String,
        /// FIFO queue.
        #[serde(default)]
        fifo: bool,
        /// Content-based deduplication enabled on the queue.
        #[serde(default)]
        content_based_deduplication: bool,
        /// Maximum message size (defaults to 1 MiB).
        #[serde(default)]
        max_message_size: Option<usize>,
    },
    /// An SNS topic.
    Topic {
        /// Topic ARN.
        arn: String,
        /// FIFO topic.
        #[serde(default)]
        fifo: bool,
        /// Content-based deduplication enabled on the topic.
        #[serde(default)]
        content_based_deduplication: bool,
        /// Maximum message size (defaults to 256 KiB).
        #[serde(default)]
        max_message_size: Option<usize>,
    },
}

impl Destination {
    /// Queue or topic identifier (URL or ARN).
    pub fn address(&self) -> &str {
        match self {
            Self::Queue { url, .. } => url,
            Self::Topic { arn, .. } => arn,
        }
    }

    /// Whether the destination is FIFO.
    pub fn is_fifo(&self) -> bool {
        match self {
            Self::Queue { fifo, .. } | Self::Topic { fifo, .. } => *fifo,
        }
    }

    fn content_based_deduplication(&self) -> bool {
        match self {
            Self::Queue { content_based_deduplication, .. } | Self::Topic { content_based_deduplication, .. } => {
                *content_based_deduplication
            }
        }
    }

    /// Maximum encoded message size.
    pub fn max_message_size(&self) -> usize {
        match self {
            Self::Queue { max_message_size, .. } => max_message_size.unwrap_or(SQS_MAX_MESSAGE_SIZE),
            Self::Topic { max_message_size, .. } => max_message_size.unwrap_or(SNS_DEFAULT_MAX_MESSAGE_SIZE),
        }
    }

    fn service(&self) -> &'static str {
        match self {
            Self::Queue { .. } => "sqs",
            Self::Topic { .. } => "sns",
        }
    }
}

/// A message to send/publish.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OutgoingMessage {
    /// Body and attributes (output of an envelope).
    pub wire: WireMessage,
    /// fanferro message id (also used as claim-check key and default FIFO dedup id).
    pub message_id: String,
    /// FIFO message group id (also allowed on standard queues for fair queueing).
    pub group_id: Option<String>,
    /// FIFO deduplication id.
    pub deduplication_id: Option<String>,
    /// Delivery delay (standard queues only, at most 15 minutes).
    pub delay: Option<Duration>,
    /// SNS subject (topics only).
    pub subject: Option<String>,
    /// X-Ray trace header.
    pub trace_header: Option<String>,
}

/// The result of a successful send/publish.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    /// fanferro message id.
    pub message_id: String,
    /// SQS/SNS message id.
    pub transport_message_id: String,
    /// FIFO sequence number.
    pub sequence_number: Option<String>,
    /// Pointer to the offloaded payload, if it was offloaded.
    pub offloaded: Option<PayloadPointer>,
    /// Encoded size in bytes.
    pub size: usize,
}

/// One batch entry's raw outcome: `(transport message id, sequence number, SQS send output)`.
type EntryOutcome = std::result::Result<(String, Option<String>, Option<SendOutput>), BatchEntryError>;

/// How failed batch entries are retried.
struct BatchRetry {
    backoff: Backoff,
    max_attempts: u32,
}

struct Prepared {
    body: OutgoingBody,
    group_id: Option<String>,
    deduplication_id: Option<String>,
    delay: Option<Duration>,
    subject: Option<String>,
    trace_header: Option<String>,
    message_id: String,
}

fn validate(destination: &Destination, message: &OutgoingMessage) -> Result<()> {
    if message.message_id.is_empty() {
        return Err(Error::validation("message_id must not be empty"));
    }
    if let Some(header) = &message.trace_header
        && (header.is_empty() || !header.bytes().all(|byte| matches!(byte, b' '..=b'~')))
    {
        return Err(Error::validation(format!(
            "trace_header must be an AWS X-Ray trace header (printable ASCII), got {header:?}"
        )));
    }
    match destination {
        Destination::Queue { fifo, .. } => {
            if message.subject.is_some() {
                return Err(Error::validation("subject is only supported when publishing to SNS topics"));
            }
            if let Some(delay) = message.delay {
                if *fifo {
                    return Err(Error::validation("per-message delay is not supported by FIFO queues"));
                }
                if delay > MAX_DELAY {
                    return Err(Error::validation("delay must be at most 15 minutes"));
                }
            }
        }
        Destination::Topic { .. } => {
            if message.delay.is_some() {
                return Err(Error::validation("delay is only supported when sending to SQS queues"));
            }
        }
    }
    if destination.is_fifo() {
        if message.group_id.as_deref().is_none_or(str::is_empty) {
            return Err(Error::validation(format!(
                "FIFO destination {} requires a group_id (message group id)",
                destination.address()
            )));
        }
        match &message.deduplication_id {
            Some(id) if !is_fifo_id(id) => {
                return Err(Error::validation(format!("deduplication_id must be {FIFO_ID_RULE}, got {id:?}")));
            }
            None if !destination.content_based_deduplication() && !is_fifo_id(&message.message_id) => {
                return Err(Error::validation(format!(
                    "the message id is the FIFO deduplication id unless deduplication_id is set: it must be {FIFO_ID_RULE}, got {:?}",
                    message.message_id
                )));
            }
            _ => {}
        }
    } else if message.deduplication_id.is_some() {
        return Err(Error::validation("deduplication_id is only supported by FIFO queues and topics"));
    }
    if let Some(group_id) = &message.group_id
        && !is_fifo_id(group_id)
    {
        return Err(Error::validation(format!("group_id must be {FIFO_ID_RULE}, got {group_id:?}")));
    }
    Ok(())
}

/// What SQS and SNS accept as a message group id and a deduplication id (docs/parity M8).
const FIFO_ID_RULE: &str = "1-128 ASCII letters, digits and punctuation characters (no spaces)";

/// Whether SQS and SNS accept `id` as a message group or deduplication id.
pub(crate) fn is_fifo_id(id: &str) -> bool {
    (1..=128).contains(&id.len()) && id.bytes().all(|byte| byte.is_ascii_graphic())
}

pub(crate) fn entry_error(service: &'static str, operation: &'static str, error: &BatchEntryError) -> Error {
    let mut kind = classify(Some(&error.code), error.message.as_deref(), None);
    if !error.sender_fault && !kind.is_retryable() {
        kind = crate::error::AwsErrorKind::Transient;
    }
    let mut aws = AwsError::new(service, operation, kind, error.message.clone().unwrap_or_default());
    aws.code = Some(error.code.clone());
    Error::from(aws)
}

impl Client {
    async fn prepare(
        &self,
        transport: &Transport,
        destination: &Destination,
        message: OutgoingMessage,
    ) -> Result<Prepared> {
        validate(destination, &message)?;
        let body = transport.encode(&message.wire, &message.message_id, destination.max_message_size()).await?;
        if let Some(pointer) = &body.offloaded {
            self.inner.metrics.payloads_offloaded.inc();
            self.inner.metrics.bytes_offloaded.add(message.wire.body.len() as u64);
            self.inner.events.emit(EventKind::PayloadOffloaded, || {
                Event::new(EventKind::PayloadOffloaded, "payload offloaded")
                    .field("message_id", &message.message_id)
                    .field("destination", destination.address())
                    .field("size", message.wire.body.len())
                    .field_opt("bucket", pointer.bucket.clone())
                    .field_opt("key", pointer.key.clone())
                    .field("presigned_url", pointer.url.is_some())
            });
        }
        let deduplication_id = match (&message.deduplication_id, destination.is_fifo()) {
            (Some(id), _) => Some(id.clone()),
            (None, true) if !destination.content_based_deduplication() => Some(message.message_id.clone()),
            _ => None,
        };
        Ok(Prepared {
            body,
            group_id: message.group_id,
            deduplication_id,
            delay: message.delay,
            subject: message.subject,
            trace_header: message.trace_header,
            message_id: message.message_id,
        })
    }

    fn verify_send(&self, destination: &Destination, prepared: &Prepared, output: &SendOutput) -> Result<()> {
        // FIFO queues answer a deduplicated send with the digests of the originally stored
        // message, which legitimately differ when the retried body changed (e.g. a new
        // envelope timestamp), so digests are only comparable for standard queues.
        if !self.inner.config.verify_md5 || destination.is_fifo() {
            return Ok(());
        }
        if let Some(md5) = &output.md5_of_body
            && !md5.eq_ignore_ascii_case(&md5_of_body(&prepared.body.body))
        {
            return Err(Error::Checksum("MD5 of the message body returned by SQS does not match".into()));
        }
        if let Some(md5) = &output.md5_of_message_attributes
            && !prepared.body.attributes.is_empty()
            && !md5.eq_ignore_ascii_case(&md5_of_attributes(&prepared.body.attributes))
        {
            return Err(Error::Checksum("MD5 of the message attributes returned by SQS does not match".into()));
        }
        Ok(())
    }

    fn send_request(prepared: &Prepared) -> SendRequest {
        SendRequest {
            body: prepared.body.body.clone(),
            attributes: prepared.body.attributes.clone(),
            delay: prepared.delay,
            group_id: prepared.group_id.clone(),
            deduplication_id: prepared.deduplication_id.clone(),
            trace_header: prepared.trace_header.clone(),
        }
    }

    fn publish_request(prepared: &Prepared) -> PublishRequest {
        PublishRequest {
            message: prepared.body.body.clone(),
            attributes: prepared.body.attributes.clone(),
            subject: prepared.subject.clone(),
            group_id: prepared.group_id.clone(),
            deduplication_id: prepared.deduplication_id.clone(),
            trace_header: prepared.trace_header.clone(),
        }
    }

    fn receipt(prepared: &Prepared, transport_message_id: String, sequence_number: Option<String>) -> Receipt {
        Receipt {
            message_id: prepared.message_id.clone(),
            transport_message_id,
            sequence_number,
            offloaded: prepared.body.offloaded.clone(),
            size: prepared.body.encoded_len(),
        }
    }

    /// Sends `prepared` once, moving its body and attributes into the request (no copies): for
    /// sends that are never retried.
    async fn send_owned(&self, destination: &Destination, prepared: Prepared) -> Result<Receipt> {
        let Prepared { body, group_id, deduplication_id, delay, subject, trace_header, message_id } = prepared;
        let size = body.encoded_len();
        let (transport_message_id, sequence_number) = match destination {
            Destination::Queue { url, .. } => {
                let request = SendRequest {
                    body: body.body,
                    attributes: body.attributes,
                    delay,
                    group_id,
                    deduplication_id,
                    trace_header,
                };
                let output = self.inner.backend.sqs.send_message(url, request).await?;
                (output.message_id, output.sequence_number)
            }
            Destination::Topic { arn, .. } => {
                let request = PublishRequest {
                    message: body.body,
                    attributes: body.attributes,
                    subject,
                    group_id,
                    deduplication_id,
                    trace_header,
                };
                let output = self.inner.backend.sns.publish(arn, request).await?;
                (output.message_id, output.sequence_number)
            }
        };
        Ok(Receipt { message_id, transport_message_id, sequence_number, offloaded: body.offloaded, size })
    }

    async fn send_once(&self, destination: &Destination, prepared: &Prepared) -> Result<Receipt> {
        match destination {
            Destination::Queue { url, .. } => {
                let output = self.inner.backend.sqs.send_message(url, Self::send_request(prepared)).await?;
                self.verify_send(destination, prepared, &output)?;
                Ok(Self::receipt(prepared, output.message_id, output.sequence_number))
            }
            Destination::Topic { arn, .. } => {
                let output: PublishOutput =
                    self.inner.backend.sns.publish(arn, Self::publish_request(prepared)).await?;
                Ok(Self::receipt(prepared, output.message_id, output.sequence_number))
            }
        }
    }

    /// Sends one message, using the client-wide transport settings.
    pub async fn publish(&self, destination: &Destination, message: OutgoingMessage) -> Result<Receipt> {
        self.publish_with(&self.inner.transport, destination, message).await
    }

    /// Sends one message using a specific transport (per-destination offload settings).
    pub async fn publish_with(
        &self,
        transport: &Transport,
        destination: &Destination,
        message: OutgoingMessage,
    ) -> Result<Receipt> {
        let result = async {
            let prepared = self.prepare(transport, destination, message).await?;
            // Only a checksum mismatch is retried here (individual calls are retried by the SDK),
            // and only standard queues are checked: other sends need no copy of the message.
            let checked = self.inner.config.verify_md5 && matches!(destination, Destination::Queue { fifo: false, .. });
            if !checked {
                return self.send_owned(destination, prepared).await;
            }
            let backoff = self.inner.config.publish_retry.backoff()?;
            let max_attempts = self.inner.config.publish_retry.max_attempts.max(1);
            let mut attempt = 0;
            loop {
                match self.send_once(destination, &prepared).await {
                    Err(Error::Checksum(reason)) if attempt + 1 < max_attempts => {
                        self.inner.metrics.publish_retries.inc();
                        self.inner.events.emit(EventKind::PublishRetried, || {
                            Event::new(EventKind::PublishRetried, "retrying publish after checksum mismatch")
                                .field("message_id", &prepared.message_id)
                                .field("destination", destination.address())
                                .field("reason", reason.clone())
                                .field("attempt", u64::from(attempt + 1))
                        });
                        tokio::time::sleep(backoff.delay(attempt)).await;
                        attempt += 1;
                    }
                    other => break other,
                }
            }
        }
        .await;
        match &result {
            Ok(receipt) => {
                self.inner.metrics.messages_published.inc();
                self.inner.metrics.bytes_published.add(receipt.size as u64);
            }
            Err(_) => self.inner.metrics.publish_failures.inc(),
        }
        result
    }

    /// Sends many messages with batch calls (10 per call, split by size; up to
    /// [`MAX_CONCURRENT_BATCH_CALLS`] at once unless the destination is FIFO), retrying failed
    /// entries that are retryable (on FIFO destinations before the next batch call, so that no
    /// message overtakes an earlier one). Results are returned in input order.
    pub async fn publish_batch(
        &self,
        destination: &Destination,
        messages: Vec<OutgoingMessage>,
    ) -> Result<Vec<Result<Receipt>>> {
        self.publish_batch_with(&self.inner.transport, destination, messages).await
    }

    /// Batch variant of [`Client::publish_with`].
    pub async fn publish_batch_with(
        &self,
        transport: &Transport,
        destination: &Destination,
        messages: Vec<OutgoingMessage>,
    ) -> Result<Vec<Result<Receipt>>> {
        let count = messages.len();
        let prepared: Vec<Result<Prepared>> =
            futures::stream::iter(messages.into_iter().map(|message| self.prepare(transport, destination, message)))
                .buffered(MAX_CONCURRENT_UPLOADS)
                .collect()
                .await;
        let mut results: Vec<Option<Result<Receipt>>> = (0..count).map(|_| None).collect();
        let mut pending: Vec<(usize, Prepared)> = Vec::new();
        for (index, entry) in prepared.into_iter().enumerate() {
            match entry {
                Ok(prepared) => pending.push((index, prepared)),
                Err(err) => results[index] = Some(Err(err)),
            }
        }
        let retry = BatchRetry {
            backoff: self.inner.config.publish_retry.backoff()?,
            max_attempts: self.inner.config.publish_retry.max_attempts.max(1),
        };
        let chunks = self.chunk(destination, pending);
        if destination.is_fifo() {
            // One batch call at a time, and the retries of a batch before the next batch: entries
            // sent later must not overtake earlier ones of their message group.
            for chunk in chunks {
                self.send_with_retries(destination, vec![chunk], 1, &retry, &mut results).await;
            }
        } else {
            self.send_with_retries(destination, chunks, MAX_CONCURRENT_BATCH_CALLS, &retry, &mut results).await;
        }
        let results: Vec<Result<Receipt>> = results
            .into_iter()
            .map(|slot| slot.unwrap_or_else(|| Err(Error::validation("batch entry was not processed"))))
            .collect();
        for result in &results {
            match result {
                Ok(receipt) => {
                    self.inner.metrics.messages_published.inc();
                    self.inner.metrics.bytes_published.add(receipt.size as u64);
                }
                Err(_) => self.inner.metrics.publish_failures.inc(),
            }
        }
        Ok(results)
    }

    /// Splits entries into batch calls: by size, and by trace header for topics when the client's
    /// `split_batches_by_trace_header` is set (the first calls and those of retries alike).
    fn chunk(&self, destination: &Destination, entries: Vec<(usize, Prepared)>) -> Vec<Vec<(usize, Prepared)>> {
        if self.inner.config.split_batches_by_trace_header && destination.service() == "sns" {
            chunk_by_trace_header(entries, destination.is_fifo(), destination.max_message_size())
        } else {
            chunk_by_size(entries, destination.max_message_size())
        }
    }

    /// Sends `chunks` (up to `concurrency` batch calls at once), then retries the entries that
    /// failed with retryable errors, all of them together, until they succeed or run out of attempts.
    async fn send_with_retries(
        &self,
        destination: &Destination,
        mut chunks: Vec<Vec<(usize, Prepared)>>,
        concurrency: usize,
        retry: &BatchRetry,
        results: &mut [Option<Result<Receipt>>],
    ) {
        let mut attempt = 0;
        loop {
            let mut failed: Vec<(usize, Prepared, Error)> = Vec::new();
            let mut calls = futures::stream::iter(chunks.into_iter().map(|chunk| self.send_chunk(destination, chunk)))
                .buffered(concurrency);
            while let Some(sent) = calls.next().await {
                for (index, prepared, outcome) in sent {
                    match outcome {
                        Ok(receipt) => results[index] = Some(Ok(receipt)),
                        Err(err) if err.is_retryable() && attempt + 1 < retry.max_attempts => {
                            failed.push((index, prepared, err));
                        }
                        Err(err) => results[index] = Some(Err(err)),
                    }
                }
            }
            drop(calls);
            if failed.is_empty() {
                return;
            }
            self.inner.metrics.publish_retries.add(failed.len() as u64);
            self.inner.events.emit(EventKind::PublishRetried, || {
                Event::new(EventKind::PublishRetried, "retrying failed batch entries")
                    .field("destination", destination.address())
                    .field("entries", failed.len())
                    .field("attempt", u64::from(attempt + 1))
                    .error(&failed[0].2)
            });
            tokio::time::sleep(retry.backoff.delay(attempt)).await;
            attempt += 1;
            let pending = failed.into_iter().map(|(index, prepared, _)| (index, prepared)).collect();
            chunks = self.chunk(destination, pending);
        }
    }

    async fn send_chunk(
        &self,
        destination: &Destination,
        chunk: Vec<(usize, Prepared)>,
    ) -> Vec<(usize, Prepared, Result<Receipt>)> {
        let service = destination.service();
        let call: Result<Vec<EntryOutcome>> = match destination {
            Destination::Queue { url, .. } => {
                let requests = chunk.iter().map(|(_, prepared)| Self::send_request(prepared)).collect();
                self.inner.backend.sqs.send_message_batch(url, requests).await.map(|results| {
                    results
                        .into_iter()
                        .map(|r| r.map(|o| (o.message_id.clone(), o.sequence_number.clone(), Some(o))))
                        .collect()
                })
            }
            Destination::Topic { arn, .. } => {
                let requests = chunk.iter().map(|(_, prepared)| Self::publish_request(prepared)).collect();
                self.inner.backend.sns.publish_batch(arn, requests).await.map(|results| {
                    results.into_iter().map(|r| r.map(|o| (o.message_id, o.sequence_number, None))).collect()
                })
            }
        };
        let operation = if service == "sqs" { "SendMessageBatch" } else { "PublishBatch" };
        match call {
            Err(err) => chunk.into_iter().map(|(index, prepared)| (index, prepared, Err(err.clone()))).collect(),
            Ok(outputs) => chunk
                .into_iter()
                .zip(outputs)
                .map(|((index, prepared), output)| {
                    let result = match output {
                        Ok((message_id, sequence_number, send_output)) => match send_output {
                            Some(send_output) => self
                                .verify_send(destination, &prepared, &send_output)
                                .map(|()| Self::receipt(&prepared, message_id, sequence_number)),
                            None => Ok(Self::receipt(&prepared, message_id, sequence_number)),
                        },
                        Err(entry) => Err(entry_error(service, operation, &entry)),
                    };
                    (index, prepared, result)
                })
                .collect(),
        }
    }
}

fn chunk_by_size(entries: Vec<(usize, Prepared)>, max_total: usize) -> Vec<Vec<(usize, Prepared)>> {
    let mut chunks = Vec::new();
    let mut current: Vec<(usize, Prepared)> = Vec::new();
    let mut current_size = 0;
    for entry in entries {
        let size = entry.1.body.encoded_len();
        if !current.is_empty() && (current.len() == MAX_BATCH_ENTRIES || current_size + size > max_total) {
            chunks.push(std::mem::take(&mut current));
            current_size = 0;
        }
        current_size += size;
        current.push(entry);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

/// Batch calls whose entries have the same trace header, each [`chunk_by_size`]: runs of
/// consecutive entries when `ordered` (FIFO: the chunks are sent in order), otherwise one group per
/// header, in the order of first appearance.
fn chunk_by_trace_header(
    entries: Vec<(usize, Prepared)>,
    ordered: bool,
    max_total: usize,
) -> Vec<Vec<(usize, Prepared)>> {
    let mut groups: Vec<Vec<(usize, Prepared)>> = Vec::new();
    for entry in entries {
        let header = entry.1.trace_header.as_deref();
        let group = if ordered {
            groups.last_mut().filter(|group| group[0].1.trace_header.as_deref() == header)
        } else {
            groups.iter_mut().find(|group| group[0].1.trace_header.as_deref() == header)
        };
        match group {
            Some(group) => group.push(entry),
            None => groups.push(vec![entry]),
        }
    }
    groups.into_iter().flat_map(|group| chunk_by_size(group, max_total)).collect()
}

#[cfg(test)]
mod tests;
