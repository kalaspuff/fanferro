//! In-memory backend: an in-process broker with SQS, SNS and S3 semantics.
//!
//! It exists for fast, deterministic tests (fanferro's own and its users'), so it models
//! the behaviour that matters to message processing rather than every API detail:
//!
//! * SQS: visibility timeouts, receipt handles, receive counts, long polling, per-message
//!   and queue delays (in whole seconds, rounded up as the AWS adapter rounds them), redrive to a
//!   dead-letter queue (`RedrivePolicy`), FIFO ordering per message group, FIFO deduplication
//!   (explicit and content based, per queue or per message group as `DeduplicationScope` says),
//!   batch operations, MD5 digests.
//! * SNS: topics (standard and FIFO), SQS subscriptions with raw and notification (JSON)
//!   delivery, filter policies (see [`filter_policy`]), FIFO deduplication.
//! * S3: buckets and objects, presigned URLs (`memory://` scheme, resolvable only by the
//!   in-memory HTTP fetcher).
//!
//! IAM policies and KMS are accepted as attributes but not enforced. Time uses
//! `tokio::time::Instant`, so tests can pause and advance the clock.

pub mod filter_policy;
mod store;

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::Notify;
use tokio::time::Instant;

use super::{
    Backend, BackendKind, BatchEntryError, BatchResult, HttpFetch, IdentityApi, ObjectStoreApi, PublishOutput,
    PublishRequest, PutOptions, ReceiveRequest, ReceivedMessage, SendOutput, SendRequest, SnsApi, SqsApi, StringMap,
    SubscriptionInfo, SystemAttributes, VisibilityChange,
};
use crate::error::{AwsError, AwsErrorKind, Error, Result};
use crate::types::attributes::{attributes_encoded_len, md5_of_attributes, md5_of_body, validate_attributes};
use crate::types::text::is_sqs_safe_text;
use crate::types::{AttributeData, MessageAttributes, arn};
use crate::util::whole_seconds;
use filter_policy::{FilterPolicy, FilterScope};
use store::{Dedup, Messages, StoredMessage};

const DEFAULT_MAX_MESSAGE_SIZE: usize = 1_048_576;
const DEFAULT_TOPIC_MAX_MESSAGE_SIZE: usize = 262_144;

fn aws_error(
    service: &'static str,
    operation: &'static str,
    kind: AwsErrorKind,
    code: &str,
    message: impl Into<String>,
) -> Error {
    let status = match kind {
        AwsErrorKind::NotFound => 404,
        AwsErrorKind::AccessDenied => 403,
        _ => 400,
    };
    Error::from(AwsError::new(service, operation, kind, message).with_code(code).with_status(status))
}

fn queue_not_found(operation: &'static str) -> Error {
    aws_error(
        "sqs",
        operation,
        AwsErrorKind::NotFound,
        "AWS.SimpleQueueService.NonExistentQueue",
        "The specified queue does not exist.",
    )
}

fn invalid(service: &'static str, operation: &'static str, message: impl Into<String>) -> Error {
    aws_error(service, operation, AwsErrorKind::InvalidRequest, "InvalidParameterValue", message)
}

#[derive(Debug)]
struct Queue {
    name: String,
    url: String,
    arn: String,
    fifo: bool,
    attributes: StringMap,
    tags: StringMap,
    messages: Messages,
    /// FIFO deduplication key (see [`Queue::dedup_key`]) -> message id and sequence number.
    dedup: Dedup<(String, Option<String>)>,
    notify: Arc<Notify>,
}

impl Queue {
    /// What a FIFO message is deduplicated by: its deduplication id, within its message group when
    /// the queue's `DeduplicationScope` is `messageGroup` (the same id in two groups is two messages).
    fn dedup_key(&self, group_id: Option<&str>, deduplication_id: &str) -> String {
        match group_id {
            Some(group) if self.attributes.get("DeduplicationScope").map(String::as_str) == Some("messageGroup") => {
                // Neither id may contain a NUL character, so the key cannot be ambiguous.
                format!("{group}\0{deduplication_id}")
            }
            _ => deduplication_id.to_string(),
        }
    }

    fn attribute_secs(&self, name: &str, default: u64) -> u64 {
        self.attributes.get(name).and_then(|value| value.parse().ok()).unwrap_or(default)
    }

    fn max_message_size(&self) -> usize {
        self.attributes.get("MaximumMessageSize").and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_MAX_MESSAGE_SIZE)
    }
}

#[derive(Debug)]
struct Topic {
    name: String,
    arn: String,
    fifo: bool,
    attributes: StringMap,
    tags: StringMap,
    /// FIFO deduplication id -> message id.
    dedup: Dedup<String>,
}

#[derive(Debug, Clone)]
struct Subscription {
    arn: String,
    topic_arn: String,
    protocol: String,
    endpoint: String,
    attributes: StringMap,
    /// The `FilterPolicy` attribute, validated for its `FilterPolicyScope`.
    filter: Option<Arc<FilterPolicy>>,
}

#[derive(Debug, Clone)]
struct StoredObject {
    data: Bytes,
}

#[derive(Debug, Default)]
struct State {
    queues: BTreeMap<String, Queue>,
    topics: BTreeMap<String, Topic>,
    subscriptions: BTreeMap<String, Subscription>,
    buckets: BTreeMap<String, BTreeMap<String, StoredObject>>,
    retired_receipts: HashMap<String, String>,
    sequence: u64,
}

impl State {
    fn next_sequence(&mut self) -> String {
        self.sequence += 1;
        format!("{:020}", self.sequence)
    }

    fn queue_url_by_arn(&self, queue_arn: &str) -> Option<String> {
        self.queues.values().find(|queue| queue.arn == queue_arn).map(|queue| queue.url.clone())
    }
}

struct Inner {
    state: Mutex<State>,
    region: String,
    account: String,
}

impl fmt::Debug for Inner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryBroker")
            .field("region", &self.region)
            .field("account", &self.account)
            .finish_non_exhaustive()
    }
}

/// A snapshot of a stored message (for test assertions).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageSnapshot {
    /// SQS message id.
    pub message_id: String,
    /// Body.
    pub body: String,
    /// Attributes.
    pub attributes: MessageAttributes,
    /// Receive count so far.
    pub receive_count: u32,
    /// Whether the message is currently in flight (invisible after a receive).
    pub in_flight: bool,
    /// FIFO group id.
    pub group_id: Option<String>,
}

/// The in-memory broker. Cheap to clone; clones share state.
#[derive(Clone, Debug)]
pub struct MemoryBroker {
    inner: Arc<Inner>,
}

impl MemoryBroker {
    /// Creates an empty broker for `region` and `account`.
    pub fn new(region: impl Into<String>, account: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State::default()),
                region: region.into(),
                account: account.into(),
            }),
        }
    }

    /// A [`Backend`] whose services all use this broker.
    pub fn backend(&self) -> Backend {
        Backend {
            kind: BackendKind::Memory,
            region: self.inner.region.clone(),
            endpoint_url: None,
            sqs: Arc::new(MemorySqs(self.inner.clone())),
            sns: Arc::new(MemorySns(self.inner.clone())),
            objects: Arc::new(MemoryObjects(self.inner.clone())),
            http: Arc::new(MemoryHttp(self.inner.clone())),
            identity: Arc::new(MemoryIdentity(self.inner.account.clone())),
        }
    }

    /// Messages currently stored in a queue (visible, delayed and in flight), in order.
    pub fn queue_messages(&self, queue_url: &str) -> Result<Vec<MessageSnapshot>> {
        let mut state = self.inner.state.lock();
        let queue = state.queues.get_mut(queue_url).ok_or_else(|| queue_not_found("Snapshot"))?;
        let now = Instant::now();
        queue.messages.promote(now);
        Ok(queue
            .messages
            .iter()
            .map(|m| MessageSnapshot {
                message_id: m.message_id.clone(),
                body: m.body.clone(),
                attributes: m.attributes.clone(),
                receive_count: m.receive_count,
                in_flight: m.in_flight && m.available_at > now,
                group_id: m.group_id.clone(),
            })
            .collect())
    }

    /// URLs of all queues.
    pub fn queue_urls(&self) -> Vec<String> {
        self.inner.state.lock().queues.keys().cloned().collect()
    }

    /// Removes every queue, topic, subscription and bucket.
    pub fn reset(&self) {
        *self.inner.state.lock() = State::default();
    }
}

#[derive(Debug)]
struct MemorySqs(Arc<Inner>);

#[derive(Debug)]
struct MemorySns(Arc<Inner>);

#[derive(Debug)]
struct MemoryObjects(Arc<Inner>);

#[derive(Debug)]
struct MemoryHttp(Arc<Inner>);

#[derive(Debug)]
struct MemoryIdentity(String);

fn content_dedup_id(body: &str) -> String {
    hex::encode(Sha256::digest(body.as_bytes()))
}

struct Enqueue {
    body: String,
    attributes: MessageAttributes,
    delay: Option<Duration>,
    group_id: Option<String>,
    deduplication_id: Option<String>,
    trace_header: Option<String>,
    dead_letter_source: Option<String>,
    message_id: Option<String>,
    sent: Option<OffsetDateTime>,
}

fn enqueue(state: &mut State, queue_url: &str, operation: &'static str, request: Enqueue) -> Result<SendOutput> {
    let sequence = state.next_sequence();
    let queue = state.queues.get_mut(queue_url).ok_or_else(|| queue_not_found(operation))?;
    if request.body.is_empty() {
        return Err(aws_error(
            "sqs",
            operation,
            AwsErrorKind::InvalidRequest,
            "MissingParameter",
            "The request must contain the parameter MessageBody.",
        ));
    }
    if !is_sqs_safe_text(&request.body) {
        return Err(aws_error(
            "sqs",
            operation,
            AwsErrorKind::InvalidRequest,
            "InvalidMessageContents",
            "Invalid characters found in the message body.",
        ));
    }
    validate_attributes(&request.attributes).map_err(|err| invalid("sqs", operation, err.to_string()))?;
    let size = request.body.len() + attributes_encoded_len(&request.attributes);
    if size > queue.max_message_size() {
        return Err(invalid(
            "sqs",
            operation,
            format!("Message must be shorter than {} bytes.", queue.max_message_size()),
        ));
    }
    let now = Instant::now();
    let mut deduplication_id = request.deduplication_id;
    if queue.fifo {
        if request.group_id.is_none() {
            return Err(aws_error(
                "sqs",
                operation,
                AwsErrorKind::InvalidRequest,
                "MissingParameter",
                "The request must contain the parameter MessageGroupId.",
            ));
        }
        if request.delay.is_some_and(|d| !d.is_zero()) {
            return Err(invalid(
                "sqs",
                operation,
                "Value for parameter DelaySeconds is invalid. Reason: The request include parameter that is not valid for this queue type.",
            ));
        }
        if deduplication_id.is_none() {
            if queue.attributes.get("ContentBasedDeduplication").map(String::as_str) == Some("true") {
                deduplication_id = Some(content_dedup_id(&request.body));
            } else {
                return Err(invalid(
                    "sqs",
                    operation,
                    "The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly",
                ));
            }
        }
        if let Some(id) = &deduplication_id
            && let Some((message_id, sequence_number)) =
                queue.dedup.get(&queue.dedup_key(request.group_id.as_deref(), id), now)
        {
            return Ok(SendOutput {
                message_id: message_id.clone(),
                sequence_number: sequence_number.clone(),
                md5_of_body: Some(md5_of_body(&request.body)),
                md5_of_message_attributes: (!request.attributes.is_empty())
                    .then(|| md5_of_attributes(&request.attributes)),
            });
        }
    }
    let delay =
        request.delay.map_or_else(|| Duration::from_secs(queue.attribute_secs("DelaySeconds", 0)), whole_seconds);
    let message_id = request.message_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let sequence_number = queue.fifo.then_some(sequence);
    if let Some(id) = &deduplication_id
        && queue.fifo
    {
        let key = queue.dedup_key(request.group_id.as_deref(), id);
        queue.dedup.insert(key, now, (message_id.clone(), sequence_number.clone()));
    }
    let md5_body = md5_of_body(&request.body);
    let md5_attributes = (!request.attributes.is_empty()).then(|| md5_of_attributes(&request.attributes));
    queue.messages.push(
        StoredMessage {
            message_id: message_id.clone(),
            body: request.body,
            attributes: request.attributes,
            md5_of_body: md5_body.clone(),
            md5_of_attributes: md5_attributes.clone(),
            sent: request.sent.unwrap_or_else(OffsetDateTime::now_utc),
            available_at: now + delay,
            receive_count: 0,
            first_receive: None,
            receipt_handle: None,
            in_flight: false,
            group_id: request.group_id,
            deduplication_id,
            sequence_number: sequence_number.clone(),
            trace_header: request.trace_header,
            dead_letter_source: request.dead_letter_source,
        },
        now,
    );
    queue.notify.notify_waiters();
    Ok(SendOutput {
        message_id,
        sequence_number,
        md5_of_body: Some(md5_body),
        md5_of_message_attributes: md5_attributes,
    })
}

/// Moves messages that exhausted `maxReceiveCount` to the dead-letter queue.
fn apply_redrive(state: &mut State, queue_url: &str) {
    let Some(queue) = state.queues.get_mut(queue_url) else { return };
    let Some(policy) =
        queue.attributes.get("RedrivePolicy").and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
    else {
        return;
    };
    let max_receive_count = match &policy["maxReceiveCount"] {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    };
    let (Some(max_receive_count), Some(target_arn)) = (max_receive_count, policy["deadLetterTargetArn"].as_str())
    else {
        return;
    };
    let source_arn = queue.arn.clone();
    // Only messages received before can have reached the count (unless it is zero).
    let exhausted = |(sequence, message): (u64, &StoredMessage)| {
        (u64::from(message.receive_count) >= max_receive_count).then_some(sequence)
    };
    let sequences: Vec<u64> = if max_receive_count == 0 {
        queue.messages.visible().filter_map(exhausted).collect()
    } else {
        queue.messages.redelivered().filter_map(exhausted).collect()
    };
    let moved: Vec<StoredMessage> =
        sequences.into_iter().filter_map(|sequence| queue.messages.remove(sequence)).collect();
    if moved.is_empty() {
        return;
    }
    let Some(target_url) = state.queue_url_by_arn(target_arn) else { return };
    for message in moved {
        let _ = enqueue(
            state,
            &target_url,
            "Redrive",
            Enqueue {
                body: message.body,
                attributes: message.attributes,
                delay: None,
                group_id: message.group_id,
                deduplication_id: message.deduplication_id.map(|id| format!("{id}-redrive")),
                trace_header: message.trace_header,
                dead_letter_source: Some(source_arn.clone()),
                message_id: Some(message.message_id),
                sent: Some(message.sent),
            },
        );
    }
}

fn try_receive(state: &mut State, request: &ReceiveRequest, now: Instant) -> Result<Vec<ReceivedMessage>> {
    state.queues.get_mut(&request.queue_url).ok_or_else(|| queue_not_found("ReceiveMessage"))?.messages.promote(now);
    apply_redrive(state, &request.queue_url);
    let queue = state.queues.get_mut(&request.queue_url).ok_or_else(|| queue_not_found("ReceiveMessage"))?;
    let visibility = request
        .visibility_timeout
        .map_or_else(|| Duration::from_secs(queue.attribute_secs("VisibilityTimeout", 30)), whole_seconds);
    let max = usize::from(request.max_messages.clamp(1, 10));
    // FIFO: a group with a message in flight is blocked (messages received now block it afterwards).
    let fifo = queue.fifo;
    let sequences: Vec<u64> = queue
        .messages
        .visible()
        .filter(|(_, message)| {
            !(fifo && message.group_id.as_deref().is_some_and(|g| queue.messages.group_in_flight(g)))
        })
        .map(|(sequence, _)| sequence)
        .take(max)
        .collect();
    let mut received = Vec::with_capacity(sequences.len());
    let mut retired = Vec::new();
    for sequence in sequences {
        let Some((message, previous)) = queue.messages.receive(sequence, now + visibility) else { continue };
        if let Some(previous) = previous {
            retired.push((previous, message.message_id.clone()));
        }
        received.push(ReceivedMessage {
            message_id: message.message_id.clone(),
            receipt_handle: message.receipt_handle.clone().unwrap_or_default(),
            body: message.body.clone(),
            md5_of_body: Some(message.md5_of_body.clone()),
            attributes: message.attributes.clone(),
            md5_of_message_attributes: message.md5_of_attributes.clone(),
            system: SystemAttributes {
                approximate_receive_count: message.receive_count,
                sent_timestamp: Some(message.sent),
                approximate_first_receive_timestamp: message.first_receive,
                sender_id: Some("AIDAMEMORYBACKEND".into()),
                message_group_id: message.group_id.clone(),
                message_deduplication_id: message.deduplication_id.clone(),
                sequence_number: message.sequence_number.clone(),
                aws_trace_header: message.trace_header.clone(),
                dead_letter_queue_source_arn: message.dead_letter_source.clone(),
            },
        });
    }
    for (handle, message_id) in retired {
        state.retired_receipts.insert(handle, message_id);
    }
    Ok(received)
}

fn receipt_error(code: &str, message: &str) -> BatchEntryError {
    BatchEntryError { code: code.into(), message: Some(message.into()), sender_fault: true }
}

#[async_trait]
impl SqsApi for MemorySqs {
    async fn send_message(&self, queue_url: &str, request: SendRequest) -> Result<SendOutput> {
        let mut state = self.0.state.lock();
        enqueue(
            &mut state,
            queue_url,
            "SendMessage",
            Enqueue {
                body: request.body,
                attributes: request.attributes,
                delay: request.delay,
                group_id: request.group_id,
                deduplication_id: request.deduplication_id,
                trace_header: request.trace_header,
                dead_letter_source: None,
                message_id: None,
                sent: None,
            },
        )
    }

    async fn send_message_batch(&self, queue_url: &str, entries: Vec<SendRequest>) -> Result<BatchResult<SendOutput>> {
        if entries.is_empty() || entries.len() > 10 {
            return Err(invalid("sqs", "SendMessageBatch", "a batch must contain 1-10 entries"));
        }
        let mut state = self.0.state.lock();
        if !state.queues.contains_key(queue_url) {
            return Err(queue_not_found("SendMessageBatch"));
        }
        Ok(entries
            .into_iter()
            .map(|request| {
                enqueue(
                    &mut state,
                    queue_url,
                    "SendMessageBatch",
                    Enqueue {
                        body: request.body,
                        attributes: request.attributes,
                        delay: request.delay,
                        group_id: request.group_id,
                        deduplication_id: request.deduplication_id,
                        trace_header: request.trace_header,
                        dead_letter_source: None,
                        message_id: None,
                        sent: None,
                    },
                )
                .map_err(|err| {
                    let aws = err.aws().cloned();
                    BatchEntryError {
                        code: aws.as_ref().and_then(|a| a.code.clone()).unwrap_or_else(|| "InternalError".into()),
                        message: Some(err.to_string()),
                        sender_fault: aws.is_some_and(|a| !a.kind.is_retryable()),
                    }
                })
            })
            .collect())
    }

    async fn receive_messages(&self, request: ReceiveRequest) -> Result<Vec<ReceivedMessage>> {
        let deadline = Instant::now() + request.wait_time.min(Duration::from_secs(20));
        loop {
            let notify = {
                let state = self.0.state.lock();
                state.queues.get(&request.queue_url).ok_or_else(|| queue_not_found("ReceiveMessage"))?.notify.clone()
            };
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let now = Instant::now();
            let next_change = {
                let mut state = self.0.state.lock();
                let messages = try_receive(&mut state, &request, now)?;
                if !messages.is_empty() {
                    return Ok(messages);
                }
                state.queues.get(&request.queue_url).and_then(|queue| queue.messages.next_change())
            };
            if now >= deadline {
                return Ok(Vec::new());
            }
            let wake_at = next_change.map_or(deadline, |at| at.min(deadline));
            tokio::select! {
                () = &mut notified => {}
                () = tokio::time::sleep_until(wake_at) => {}
            }
        }
    }

    async fn delete_message_batch(&self, queue_url: &str, receipt_handles: Vec<String>) -> Result<BatchResult<()>> {
        let mut state = self.0.state.lock();
        let State { queues, retired_receipts, .. } = &mut *state;
        let queue = queues.get_mut(queue_url).ok_or_else(|| queue_not_found("DeleteMessageBatch"))?;
        // Deleting can unblock a FIFO message group: wake long polls like SQS would.
        queue.notify.notify_waiters();
        Ok(receipt_handles
            .into_iter()
            .map(|handle| {
                if let Some(sequence) = queue.messages.by_receipt(&handle) {
                    queue.messages.remove(sequence);
                    Ok(())
                } else if let Some(message_id) = retired_receipts.get(&handle) {
                    if queue.messages.contains_id(message_id) {
                        Err(receipt_error("InvalidParameterValue", "The receipt handle has expired."))
                    } else {
                        Ok(())
                    }
                } else if let Some((message_id, _)) = handle.split_once('#')
                    && !queue.messages.contains_id(message_id)
                {
                    // Already deleted with its current handle: deleting again succeeds.
                    Ok(())
                } else {
                    Err(receipt_error("ReceiptHandleIsInvalid", "The input receipt handle is invalid."))
                }
            })
            .collect())
    }

    async fn change_visibility_batch(
        &self,
        queue_url: &str,
        entries: Vec<VisibilityChange>,
    ) -> Result<BatchResult<()>> {
        let mut state = self.0.state.lock();
        let queue = state.queues.get_mut(queue_url).ok_or_else(|| queue_not_found("ChangeMessageVisibilityBatch"))?;
        let now = Instant::now();
        // Messages whose visibility timeout expired are no longer in flight.
        queue.messages.promote(now);
        let mut changed = false;
        let results = entries
            .into_iter()
            .map(|entry| {
                if whole_seconds(entry.timeout) > Duration::from_secs(43_200) {
                    return Err(receipt_error(
                        "InvalidParameterValue",
                        "VisibilityTimeout must be at most 43200 seconds.",
                    ));
                }
                let Some(sequence) = queue.messages.by_receipt(&entry.receipt_handle) else {
                    return Err(receipt_error("ReceiptHandleIsInvalid", "The input receipt handle is invalid."));
                };
                if !queue.messages.get(sequence).is_some_and(|message| message.in_flight) {
                    return Err(receipt_error("MessageNotInflight", "The message referred to is not in flight."));
                }
                let timeout = whole_seconds(entry.timeout);
                queue.messages.change_visibility(sequence, now, timeout);
                changed |= timeout.is_zero();
                Ok(())
            })
            .collect();
        if changed {
            queue.notify.notify_waiters();
        }
        Ok(results)
    }

    async fn get_queue_url(&self, name: &str, owner_account_id: Option<&str>) -> Result<String> {
        let state = self.0.state.lock();
        state
            .queues
            .values()
            .find(|queue| queue.name == name && owner_account_id.is_none_or(|owner| owner == self.0.account))
            .map(|queue| queue.url.clone())
            .ok_or_else(|| queue_not_found("GetQueueUrl"))
    }

    async fn get_queue_attributes(&self, queue_url: &str) -> Result<StringMap> {
        let mut state = self.0.state.lock();
        let queue = state.queues.get_mut(queue_url).ok_or_else(|| queue_not_found("GetQueueAttributes"))?;
        queue.messages.promote(Instant::now());
        let mut attributes = queue.attributes.clone();
        let (visible, in_flight, delayed) = queue.messages.counts();
        attributes.insert("ApproximateNumberOfMessages".into(), visible.to_string());
        attributes.insert("ApproximateNumberOfMessagesNotVisible".into(), in_flight.to_string());
        attributes.insert("ApproximateNumberOfMessagesDelayed".into(), delayed.to_string());
        Ok(attributes)
    }

    async fn create_queue(&self, name: &str, attributes: &StringMap, tags: &StringMap) -> Result<String> {
        let valid_name = !name.is_empty()
            && name.len() <= 80
            && name.trim_end_matches(".fifo").chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        if !valid_name {
            return Err(invalid("sqs", "CreateQueue", format!("Invalid queue name {name:?}")));
        }
        let fifo_attribute = attributes.get("FifoQueue").map(String::as_str) == Some("true");
        if fifo_attribute != name.ends_with(".fifo") {
            return Err(invalid("sqs", "CreateQueue", "FIFO queue names must end with .fifo and set FifoQueue=true"));
        }
        if let Some(policy) = attributes.get("RedrivePolicy")
            && serde_json::from_str::<serde_json::Value>(policy).is_err()
        {
            return Err(invalid("sqs", "CreateQueue", "Invalid value for the parameter RedrivePolicy."));
        }
        let mut state = self.0.state.lock();
        if let Some(existing) = state.queues.values().find(|queue| queue.name == name) {
            let conflicting = attributes.iter().any(|(key, value)| existing.attributes.get(key) != Some(value));
            if conflicting {
                return Err(aws_error(
                    "sqs",
                    "CreateQueue",
                    AwsErrorKind::InvalidRequest,
                    "QueueAlreadyExists",
                    "A queue already exists with the same name and a different value for attribute(s)",
                ));
            }
            return Ok(existing.url.clone());
        }
        let url = format!("memory://{}/{}/{}", self.0.region, self.0.account, name);
        let queue_arn = arn::queue_arn(&self.0.region, &self.0.account, name);
        let now = OffsetDateTime::now_utc().unix_timestamp().to_string();
        let mut all = StringMap::from([
            ("VisibilityTimeout".to_string(), "30".to_string()),
            ("MessageRetentionPeriod".to_string(), "345600".to_string()),
            ("DelaySeconds".to_string(), "0".to_string()),
            ("MaximumMessageSize".to_string(), DEFAULT_MAX_MESSAGE_SIZE.to_string()),
            ("ReceiveMessageWaitTimeSeconds".to_string(), "0".to_string()),
            ("SqsManagedSseEnabled".to_string(), "true".to_string()),
            ("QueueArn".to_string(), queue_arn.clone()),
            ("CreatedTimestamp".to_string(), now.clone()),
            ("LastModifiedTimestamp".to_string(), now),
        ]);
        if fifo_attribute {
            all.insert("ContentBasedDeduplication".into(), "false".into());
            all.insert("DeduplicationScope".into(), "queue".into());
            all.insert("FifoThroughputLimit".into(), "perQueue".into());
        }
        all.extend(attributes.clone());
        state.queues.insert(
            url.clone(),
            Queue {
                name: name.to_string(),
                url: url.clone(),
                arn: queue_arn,
                fifo: fifo_attribute,
                attributes: all,
                tags: tags.clone(),
                messages: Messages::default(),
                dedup: Dedup::default(),
                notify: Arc::new(Notify::new()),
            },
        );
        Ok(url)
    }

    async fn set_queue_attributes(&self, queue_url: &str, attributes: &StringMap) -> Result<()> {
        if let Some(policy) = attributes.get("RedrivePolicy")
            && !policy.is_empty()
            && serde_json::from_str::<serde_json::Value>(policy).is_err()
        {
            return Err(invalid("sqs", "SetQueueAttributes", "Invalid value for the parameter RedrivePolicy."));
        }
        if attributes.contains_key("FifoQueue") {
            return Err(invalid("sqs", "SetQueueAttributes", "FifoQueue cannot be changed"));
        }
        let mut state = self.0.state.lock();
        let queue = state.queues.get_mut(queue_url).ok_or_else(|| queue_not_found("SetQueueAttributes"))?;
        for (key, value) in attributes {
            if value.is_empty() {
                queue.attributes.remove(key);
            } else {
                queue.attributes.insert(key.clone(), value.clone());
            }
        }
        queue.attributes.insert("LastModifiedTimestamp".into(), OffsetDateTime::now_utc().unix_timestamp().to_string());
        queue.notify.notify_waiters();
        Ok(())
    }

    async fn list_queue_tags(&self, queue_url: &str) -> Result<StringMap> {
        let state = self.0.state.lock();
        Ok(state.queues.get(queue_url).ok_or_else(|| queue_not_found("ListQueueTags"))?.tags.clone())
    }

    async fn tag_queue(&self, queue_url: &str, tags: &StringMap) -> Result<()> {
        let mut state = self.0.state.lock();
        state.queues.get_mut(queue_url).ok_or_else(|| queue_not_found("TagQueue"))?.tags.extend(tags.clone());
        Ok(())
    }

    async fn delete_queue(&self, queue_url: &str) -> Result<()> {
        let mut state = self.0.state.lock();
        let queue = state.queues.remove(queue_url).ok_or_else(|| queue_not_found("DeleteQueue"))?;
        queue.notify.notify_waiters();
        Ok(())
    }

    async fn purge_queue(&self, queue_url: &str) -> Result<()> {
        let mut state = self.0.state.lock();
        state.queues.get_mut(queue_url).ok_or_else(|| queue_not_found("PurgeQueue"))?.messages.clear();
        Ok(())
    }
}

fn topic_not_found(operation: &'static str) -> Error {
    aws_error("sns", operation, AwsErrorKind::NotFound, "NotFound", "Topic does not exist")
}

fn subscription_not_found(operation: &'static str) -> Error {
    aws_error("sns", operation, AwsErrorKind::NotFound, "NotFound", "Subscription does not exist")
}

fn format_timestamp(time: OffsetDateTime) -> String {
    time.replace_nanosecond(time.millisecond() as u32 * 1_000_000)
        .ok()
        .and_then(|t| t.format(&Rfc3339).ok())
        .unwrap_or_default()
}

#[allow(clippy::too_many_arguments)]
fn notification_body(
    region: &str,
    topic_arn: &str,
    subscription_arn: &str,
    message_id: &str,
    sequence_number: Option<&str>,
    request: &PublishRequest,
) -> String {
    let mut attributes = serde_json::Map::new();
    for (name, value) in &request.attributes {
        let rendered = match value.data() {
            AttributeData::Text(text) => text.clone(),
            AttributeData::Binary(bytes) => BASE64.encode(bytes),
        };
        attributes.insert(name.clone(), serde_json::json!({"Type": value.data_type(), "Value": rendered}));
    }
    let mut document = serde_json::json!({
        "Type": "Notification",
        "MessageId": message_id,
        "TopicArn": topic_arn,
        "Message": request.message,
        "Timestamp": format_timestamp(OffsetDateTime::now_utc()),
        "SignatureVersion": "1",
        "Signature": "bWVtb3J5LWJhY2tlbmQ=",
        "SigningCertURL": format!("https://sns.{region}.amazonaws.com/SimpleNotificationService-memory.pem"),
        "UnsubscribeURL": format!("https://sns.{region}.amazonaws.com/?Action=Unsubscribe&SubscriptionArn={subscription_arn}"),
    });
    if let Some(subject) = &request.subject {
        document["Subject"] = serde_json::Value::String(subject.clone());
    }
    if let Some(sequence) = sequence_number {
        document["SequenceNumber"] = serde_json::Value::String(sequence.to_string());
    }
    if !attributes.is_empty() {
        document["MessageAttributes"] = serde_json::Value::Object(attributes);
    }
    document.to_string()
}

fn publish_locked(inner: &Inner, state: &mut State, topic_arn: &str, request: PublishRequest) -> Result<PublishOutput> {
    let sequence = state.next_sequence();
    let topic = state.topics.get_mut(topic_arn).ok_or_else(|| topic_not_found("Publish"))?;
    if request.message.is_empty() {
        return Err(aws_error(
            "sns",
            "Publish",
            AwsErrorKind::InvalidRequest,
            "InvalidParameter",
            "Invalid parameter: Empty message",
        ));
    }
    if !is_sqs_safe_text(&request.message) {
        return Err(aws_error(
            "sns",
            "Publish",
            AwsErrorKind::InvalidRequest,
            "InvalidParameter",
            "Invalid parameter: Message contains invalid characters",
        ));
    }
    validate_attributes(&request.attributes).map_err(|err| invalid("sns", "Publish", err.to_string()))?;
    let limit = topic
        .attributes
        .get("MaximumMessageSize")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_TOPIC_MAX_MESSAGE_SIZE);
    let size = request.message.len() + attributes_encoded_len(&request.attributes);
    if size > limit {
        return Err(aws_error(
            "sns",
            "Publish",
            AwsErrorKind::InvalidRequest,
            "InvalidParameter",
            format!("Invalid parameter: Message too long ({size} > {limit})"),
        ));
    }
    let now = Instant::now();
    let mut deduplication_id = request.deduplication_id.clone();
    let sequence_number = topic.fifo.then_some(sequence);
    if topic.fifo {
        if request.group_id.is_none() {
            return Err(aws_error(
                "sns",
                "Publish",
                AwsErrorKind::InvalidRequest,
                "InvalidParameter",
                "Invalid parameter: The MessageGroupId parameter is required for FIFO topics",
            ));
        }
        if deduplication_id.is_none() {
            if topic.attributes.get("ContentBasedDeduplication").map(String::as_str) == Some("true") {
                deduplication_id = Some(content_dedup_id(&request.message));
            } else {
                return Err(aws_error(
                    "sns",
                    "Publish",
                    AwsErrorKind::InvalidRequest,
                    "InvalidParameter",
                    "Invalid parameter: The topic should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly",
                ));
            }
        }
        if let Some(message_id) = deduplication_id.as_ref().and_then(|id| topic.dedup.get(id, now)) {
            return Ok(PublishOutput { message_id: message_id.clone(), sequence_number });
        }
    }
    let message_id = uuid::Uuid::new_v4().to_string();
    if let (true, Some(id)) = (topic.fifo, &deduplication_id) {
        topic.dedup.insert(id.clone(), now, message_id.clone());
    }
    let subscriptions: Vec<Subscription> =
        state.subscriptions.values().filter(|s| s.topic_arn == topic_arn && s.protocol == "sqs").cloned().collect();
    for subscription in subscriptions {
        if subscription.filter.as_ref().is_some_and(|filter| !filter.matches(&request.attributes, &request.message)) {
            continue;
        }
        let Some(queue_url) = state.queue_url_by_arn(&subscription.endpoint) else { continue };
        let raw = subscription.attributes.get("RawMessageDelivery").map(String::as_str) == Some("true");
        let (body, attributes) = if raw {
            (request.message.clone(), request.attributes.clone())
        } else {
            (
                notification_body(
                    &inner.region,
                    topic_arn,
                    &subscription.arn,
                    &message_id,
                    sequence_number.as_deref(),
                    &request,
                ),
                MessageAttributes::new(),
            )
        };
        let fifo_queue = state.queues.get(&queue_url).is_some_and(|queue| queue.fifo);
        let _ = enqueue(
            state,
            &queue_url,
            "SnsDelivery",
            Enqueue {
                body,
                attributes,
                delay: None,
                group_id: if fifo_queue { request.group_id.clone().or_else(|| Some("sns".into())) } else { None },
                deduplication_id: if fifo_queue {
                    Some(deduplication_id.clone().unwrap_or_else(|| message_id.clone()))
                } else {
                    None
                },
                trace_header: request.trace_header.clone(),
                dead_letter_source: None,
                message_id: None,
                sent: None,
            },
        );
    }
    Ok(PublishOutput { message_id, sequence_number })
}

/// Validates a subscription's filter policy like SNS (for its scope) and compiles it.
fn filter_of(
    policy: Option<&String>,
    scope: Option<&String>,
    operation: &'static str,
) -> Result<Option<Arc<FilterPolicy>>> {
    let Some(policy) = policy.filter(|policy| !policy.is_empty()) else { return Ok(None) };
    match FilterPolicy::parse(policy, FilterScope::from_attribute(scope.map(String::as_str))) {
        Ok(filter) => Ok(Some(Arc::new(filter))),
        Err(reason) => Err(aws_error(
            "sns",
            operation,
            AwsErrorKind::InvalidRequest,
            "InvalidParameter",
            format!("Invalid parameter: FilterPolicy: {reason}"),
        )),
    }
}

fn validate_subscription_attribute(name: &str, value: &str) -> Result<()> {
    match name {
        "RawMessageDelivery" if value != "true" && value != "false" => {
            Err(invalid("sns", "SetSubscriptionAttributes", "RawMessageDelivery must be true or false"))
        }
        "RedrivePolicy" | "DeliveryPolicy"
            if !value.is_empty() && serde_json::from_str::<serde_json::Value>(value).is_err() =>
        {
            Err(invalid("sns", "SetSubscriptionAttributes", format!("Invalid parameter: {name} is not valid JSON")))
        }
        "FilterPolicyScope" if value != "MessageAttributes" && value != "MessageBody" => Err(invalid(
            "sns",
            "SetSubscriptionAttributes",
            "FilterPolicyScope must be MessageAttributes or MessageBody",
        )),
        _ => Ok(()),
    }
}

#[async_trait]
impl SnsApi for MemorySns {
    async fn publish(&self, topic_arn: &str, request: PublishRequest) -> Result<PublishOutput> {
        let mut state = self.0.state.lock();
        publish_locked(&self.0, &mut state, topic_arn, request)
    }

    async fn publish_batch(&self, topic_arn: &str, entries: Vec<PublishRequest>) -> Result<BatchResult<PublishOutput>> {
        if entries.is_empty() || entries.len() > 10 {
            return Err(invalid("sns", "PublishBatch", "a batch must contain 1-10 entries"));
        }
        let mut state = self.0.state.lock();
        if !state.topics.contains_key(topic_arn) {
            return Err(topic_not_found("PublishBatch"));
        }
        Ok(entries
            .into_iter()
            .map(|entry| {
                publish_locked(&self.0, &mut state, topic_arn, entry).map_err(|err| {
                    let aws = err.aws().cloned();
                    BatchEntryError {
                        code: aws.as_ref().and_then(|a| a.code.clone()).unwrap_or_else(|| "InternalError".into()),
                        message: Some(err.to_string()),
                        sender_fault: aws.is_some_and(|a| !a.kind.is_retryable()),
                    }
                })
            })
            .collect())
    }

    async fn create_topic(&self, name: &str, attributes: &StringMap, tags: &StringMap) -> Result<String> {
        let valid_name = !name.is_empty()
            && name.len() <= 256
            && name.trim_end_matches(".fifo").chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        if !valid_name {
            return Err(invalid("sns", "CreateTopic", format!("Invalid parameter: Topic Name {name:?}")));
        }
        let fifo = attributes.get("FifoTopic").map(String::as_str) == Some("true");
        if fifo != name.ends_with(".fifo") {
            return Err(invalid(
                "sns",
                "CreateTopic",
                "Invalid parameter: FIFO topic names must end with .fifo and set FifoTopic=true",
            ));
        }
        let topic_arn = arn::topic_arn(&self.0.region, &self.0.account, name);
        let mut state = self.0.state.lock();
        if let Some(existing) = state.topics.get(&topic_arn) {
            if attributes.iter().any(|(key, value)| existing.attributes.get(key) != Some(value)) {
                return Err(invalid(
                    "sns",
                    "CreateTopic",
                    "Invalid parameter: Attributes Reason: Topic already exists with different attributes",
                ));
            }
            return Ok(topic_arn);
        }
        let mut all = StringMap::from([
            ("TopicArn".to_string(), topic_arn.clone()),
            ("Owner".to_string(), self.0.account.clone()),
            ("DisplayName".to_string(), String::new()),
        ]);
        if fifo {
            all.insert("ContentBasedDeduplication".into(), "false".into());
        }
        all.extend(attributes.clone());
        state.topics.insert(
            topic_arn.clone(),
            Topic {
                name: name.to_string(),
                arn: topic_arn.clone(),
                fifo,
                attributes: all,
                tags: tags.clone(),
                dedup: Dedup::default(),
            },
        );
        Ok(topic_arn)
    }

    async fn get_topic_attributes(&self, topic_arn: &str) -> Result<StringMap> {
        let state = self.0.state.lock();
        let topic = state.topics.get(topic_arn).ok_or_else(|| topic_not_found("GetTopicAttributes"))?;
        let mut attributes = topic.attributes.clone();
        let count = state.subscriptions.values().filter(|s| s.topic_arn == topic.arn).count();
        attributes.insert("SubscriptionsConfirmed".into(), count.to_string());
        attributes.insert("SubscriptionsPending".into(), "0".into());
        attributes.insert("SubscriptionsDeleted".into(), "0".into());
        let _ = &topic.name;
        Ok(attributes)
    }

    async fn set_topic_attribute(&self, topic_arn: &str, name: &str, value: &str) -> Result<()> {
        if name == "FifoTopic" {
            return Err(invalid("sns", "SetTopicAttributes", "Invalid parameter: FifoTopic cannot be changed"));
        }
        let mut state = self.0.state.lock();
        let topic = state.topics.get_mut(topic_arn).ok_or_else(|| topic_not_found("SetTopicAttributes"))?;
        topic.attributes.insert(name.to_string(), value.to_string());
        Ok(())
    }

    async fn list_topic_tags(&self, topic_arn: &str) -> Result<StringMap> {
        let state = self.0.state.lock();
        Ok(state.topics.get(topic_arn).ok_or_else(|| topic_not_found("ListTagsForResource"))?.tags.clone())
    }

    async fn tag_topic(&self, topic_arn: &str, tags: &StringMap) -> Result<()> {
        let mut state = self.0.state.lock();
        state.topics.get_mut(topic_arn).ok_or_else(|| topic_not_found("TagResource"))?.tags.extend(tags.clone());
        Ok(())
    }

    async fn delete_topic(&self, topic_arn: &str) -> Result<()> {
        let mut state = self.0.state.lock();
        state.topics.remove(topic_arn);
        state.subscriptions.retain(|_, sub| sub.topic_arn != topic_arn);
        Ok(())
    }

    async fn subscribe(
        &self,
        topic_arn: &str,
        protocol: &str,
        endpoint: &str,
        attributes: &StringMap,
    ) -> Result<String> {
        for (name, value) in attributes {
            validate_subscription_attribute(name, value)?;
        }
        let filter = filter_of(attributes.get("FilterPolicy"), attributes.get("FilterPolicyScope"), "Subscribe")?;
        let mut state = self.0.state.lock();
        if !state.topics.contains_key(topic_arn) {
            return Err(topic_not_found("Subscribe"));
        }
        if let Some(existing) = state
            .subscriptions
            .values()
            .find(|s| s.topic_arn == topic_arn && s.protocol == protocol && s.endpoint == endpoint)
        {
            if attributes.iter().any(|(key, value)| existing.attributes.get(key) != Some(value)) {
                return Err(invalid(
                    "sns",
                    "Subscribe",
                    "Invalid parameter: Attributes Reason: Subscription already exists with different attributes",
                ));
            }
            return Ok(existing.arn.clone());
        }
        let subscription_arn = format!("{topic_arn}:{}", uuid::Uuid::new_v4());
        let mut all = StringMap::from([("RawMessageDelivery".to_string(), "false".to_string())]);
        all.extend(attributes.clone());
        state.subscriptions.insert(
            subscription_arn.clone(),
            Subscription {
                arn: subscription_arn.clone(),
                topic_arn: topic_arn.to_string(),
                protocol: protocol.to_string(),
                endpoint: endpoint.to_string(),
                attributes: all,
                filter,
            },
        );
        Ok(subscription_arn)
    }

    async fn list_subscriptions_by_topic(&self, topic_arn: &str) -> Result<Vec<SubscriptionInfo>> {
        let state = self.0.state.lock();
        if !state.topics.contains_key(topic_arn) {
            return Err(topic_not_found("ListSubscriptionsByTopic"));
        }
        Ok(state
            .subscriptions
            .values()
            .filter(|s| s.topic_arn == topic_arn)
            .map(|s| SubscriptionInfo {
                subscription_arn: s.arn.clone(),
                protocol: s.protocol.clone(),
                endpoint: s.endpoint.clone(),
                topic_arn: s.topic_arn.clone(),
            })
            .collect())
    }

    async fn get_subscription_attributes(&self, subscription_arn: &str) -> Result<StringMap> {
        let state = self.0.state.lock();
        let subscription = state
            .subscriptions
            .get(subscription_arn)
            .ok_or_else(|| subscription_not_found("GetSubscriptionAttributes"))?;
        let mut attributes = subscription.attributes.clone();
        attributes.insert("SubscriptionArn".into(), subscription.arn.clone());
        attributes.insert("TopicArn".into(), subscription.topic_arn.clone());
        attributes.insert("Protocol".into(), subscription.protocol.clone());
        attributes.insert("Endpoint".into(), subscription.endpoint.clone());
        attributes.insert("Owner".into(), self.0.account.clone());
        attributes.insert("PendingConfirmation".into(), "false".into());
        attributes.insert("ConfirmationWasAuthenticated".into(), "true".into());
        Ok(attributes)
    }

    async fn set_subscription_attribute(&self, subscription_arn: &str, name: &str, value: &str) -> Result<()> {
        validate_subscription_attribute(name, value)?;
        let mut state = self.0.state.lock();
        let subscription = state
            .subscriptions
            .get_mut(subscription_arn)
            .ok_or_else(|| subscription_not_found("SetSubscriptionAttributes"))?;
        let mut attributes = subscription.attributes.clone();
        if value.is_empty() {
            attributes.remove(name);
        } else {
            attributes.insert(name.to_string(), value.to_string());
        }
        if name == "FilterPolicy" || name == "FilterPolicyScope" {
            subscription.filter = filter_of(
                attributes.get("FilterPolicy"),
                attributes.get("FilterPolicyScope"),
                "SetSubscriptionAttributes",
            )?;
        }
        subscription.attributes = attributes;
        Ok(())
    }

    async fn unsubscribe(&self, subscription_arn: &str) -> Result<()> {
        self.0.state.lock().subscriptions.remove(subscription_arn);
        Ok(())
    }

    async fn confirm_subscription(&self, topic_arn: &str, _token: &str) -> Result<String> {
        let state = self.0.state.lock();
        state
            .subscriptions
            .values()
            .find(|s| s.topic_arn == topic_arn)
            .map(|s| s.arn.clone())
            .ok_or_else(|| subscription_not_found("ConfirmSubscription"))
    }
}

fn no_such_bucket(operation: &'static str) -> Error {
    aws_error("s3", operation, AwsErrorKind::NotFound, "NoSuchBucket", "The specified bucket does not exist")
}

fn encode_key(key: &str) -> String {
    key.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => (b as char).to_string(),
            other => format!("%{other:02X}"),
        })
        .collect()
}

fn decode_key(encoded: &str) -> Option<String> {
    let bytes = encoded.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = encoded.get(index + 1..index + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out).ok()
}

#[async_trait]
impl ObjectStoreApi for MemoryObjects {
    async fn put_object(&self, bucket: &str, key: &str, data: Bytes, _options: &PutOptions) -> Result<()> {
        let mut state = self.0.state.lock();
        let objects = state.buckets.get_mut(bucket).ok_or_else(|| no_such_bucket("PutObject"))?;
        objects.insert(key.to_string(), StoredObject { data });
        Ok(())
    }

    async fn get_object(&self, bucket: &str, key: &str, max_bytes: usize) -> Result<Bytes> {
        let state = self.0.state.lock();
        let objects = state.buckets.get(bucket).ok_or_else(|| no_such_bucket("GetObject"))?;
        let object = objects.get(key).ok_or_else(|| {
            aws_error("s3", "GetObject", AwsErrorKind::NotFound, "NoSuchKey", "The specified key does not exist.")
        })?;
        if object.data.len() > max_bytes {
            return Err(Error::Payload(format!(
                "payload of {} bytes exceeds the limit of {max_bytes} bytes",
                object.data.len()
            )));
        }
        Ok(object.data.clone())
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        let mut state = self.0.state.lock();
        if let Some(objects) = state.buckets.get_mut(bucket) {
            objects.remove(key);
        }
        Ok(())
    }

    async fn presign_get(&self, bucket: &str, key: &str, ttl: Duration) -> Result<String> {
        let expires = OffsetDateTime::now_utc().unix_timestamp() + i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX / 2);
        Ok(format!("memory://s3/{bucket}/{}?expires={expires}", encode_key(key)))
    }

    async fn bucket_exists(&self, bucket: &str) -> Result<bool> {
        Ok(self.0.state.lock().buckets.contains_key(bucket))
    }

    async fn create_bucket(&self, bucket: &str) -> Result<()> {
        self.0.state.lock().buckets.entry(bucket.to_string()).or_default();
        Ok(())
    }
}

#[async_trait]
impl HttpFetch for MemoryHttp {
    async fn get(&self, url: &str, max_bytes: usize) -> Result<Bytes> {
        let Some(rest) = url.strip_prefix("memory://s3/") else {
            return Err(Error::Http("the in-memory backend can only fetch memory:// URLs".into()));
        };
        let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
        let expires: Option<i64> =
            query.split('&').find_map(|pair| pair.strip_prefix("expires=")).and_then(|v| v.parse().ok());
        if let Some(at) = expires.filter(|at| *at < OffsetDateTime::now_utc().unix_timestamp()) {
            let expired_at = OffsetDateTime::from_unix_timestamp(at).unwrap_or(OffsetDateTime::UNIX_EPOCH);
            return Err(crate::transport::claim_check::expired_url_error(expired_at));
        }
        let (bucket, key) = path.split_once('/').ok_or_else(|| Error::Http("malformed memory URL".into()))?;
        let key = decode_key(key).ok_or_else(|| Error::Http("malformed memory URL".into()))?;
        MemoryObjects(self.0.clone()).get_object(bucket, &key, max_bytes).await.map_err(|err| {
            if err.is_not_found() {
                Error::Payload("payload URL is not accessible (HTTP 404); it may have expired".into())
            } else {
                err
            }
        })
    }
}

#[async_trait]
impl IdentityApi for MemoryIdentity {
    async fn account_id(&self) -> Result<String> {
        Ok(self.0.clone())
    }
}

#[cfg(test)]
mod tests;
