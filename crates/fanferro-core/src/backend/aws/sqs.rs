//! SQS adapter.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_sqs::types::{
    ChangeMessageVisibilityBatchRequestEntry, DeleteMessageBatchRequestEntry, MessageAttributeValue,
    MessageSystemAttributeName, MessageSystemAttributeNameForSends, MessageSystemAttributeValue, QueueAttributeName,
    SendMessageBatchRequestEntry,
};
use aws_smithy_types::Blob;
use aws_smithy_types::timeout::TimeoutConfig;
use bytes::Bytes;
use time::OffsetDateTime;

use super::errors::map_sdk_error;
use super::upload_timeouts;
use crate::backend::{
    BatchEntryError, BatchResult, ReceiveRequest, ReceivedMessage, SendOutput, SendRequest, SqsApi, StringMap,
    SystemAttributes, VisibilityChange,
};
use crate::error::{Error, Result};
use crate::types::attributes::attributes_encoded_len;
use crate::types::{AttributeData, AttributeValue, MessageAttributes};
use crate::util::whole_seconds;

/// SQS backed by the AWS SDK.
#[derive(Clone, Debug)]
pub struct AwsSqs {
    client: aws_sdk_sqs::Client,
}

impl AwsSqs {
    /// Wraps an SDK client.
    pub fn new(client: aws_sdk_sqs::Client) -> Self {
        Self { client }
    }
}

fn duration_secs_i32(duration: Duration) -> i32 {
    i32::try_from(duration.as_secs()).unwrap_or(i32::MAX)
}

/// A visibility timeout or delay in whole seconds, rounded up (never shorter than requested).
fn timeout_secs_i32(duration: Duration) -> i32 {
    duration_secs_i32(whole_seconds(duration))
}

/// Converts an attribute for a request (moving its strings, without copying them).
fn to_sdk_attribute(value: AttributeValue) -> Result<MessageAttributeValue> {
    let (data_type, data) = value.into_parts();
    let builder = MessageAttributeValue::builder().data_type(data_type);
    let builder = match data {
        AttributeData::Text(text) => builder.string_value(text),
        AttributeData::Binary(bytes) => builder.binary_value(Blob::new(Vec::from(bytes))),
    };
    builder.build().map_err(|err| Error::validation(format!("invalid message attribute: {err}")))
}

fn to_sdk_attributes(attributes: MessageAttributes) -> Result<Option<HashMap<String, MessageAttributeValue>>> {
    if attributes.is_empty() {
        return Ok(None);
    }
    attributes.into_iter().map(|(name, value)| Ok((name, to_sdk_attribute(value)?))).collect::<Result<_>>().map(Some)
}

/// Converts a received attribute (moving its strings, without copying them).
fn from_sdk_attribute(value: MessageAttributeValue) -> Option<AttributeValue> {
    let data = match (value.string_value, value.binary_value) {
        (Some(text), _) => AttributeData::Text(text),
        (None, Some(binary)) => AttributeData::Binary(Bytes::from(binary.into_inner())),
        (None, None) => return None,
    };
    AttributeValue::with_type(value.data_type, data).ok()
}

fn trace_header_attribute(header: &str) -> Result<MessageSystemAttributeValue> {
    MessageSystemAttributeValue::builder()
        .data_type("String")
        .string_value(header)
        .build()
        .map_err(|err| Error::validation(format!("invalid trace header: {err}")))
}

fn millis_to_time(value: Option<&String>) -> Option<OffsetDateTime> {
    let millis: i128 = value?.parse().ok()?;
    OffsetDateTime::from_unix_timestamp_nanos(millis * 1_000_000).ok()
}

/// Converts a received message, moving its strings (bodies of up to 1 MiB) instead of copying them.
fn to_received(message: aws_sdk_sqs::types::Message) -> Option<ReceivedMessage> {
    let mut system = message.attributes.unwrap_or_default();
    let mut take = |name: MessageSystemAttributeName| system.remove(&name);
    let attributes = message
        .message_attributes
        .map(|attrs| attrs.into_iter().filter_map(|(name, value)| Some((name, from_sdk_attribute(value)?))).collect())
        .unwrap_or_default();
    Some(ReceivedMessage {
        message_id: message.message_id?,
        receipt_handle: message.receipt_handle?,
        body: message.body.unwrap_or_default(),
        md5_of_body: message.md5_of_body,
        attributes,
        md5_of_message_attributes: message.md5_of_message_attributes,
        system: SystemAttributes {
            approximate_receive_count: take(MessageSystemAttributeName::ApproximateReceiveCount)
                .and_then(|v| v.parse().ok())
                .unwrap_or(1),
            sent_timestamp: millis_to_time(take(MessageSystemAttributeName::SentTimestamp).as_ref()),
            approximate_first_receive_timestamp: millis_to_time(
                take(MessageSystemAttributeName::ApproximateFirstReceiveTimestamp).as_ref(),
            ),
            sender_id: take(MessageSystemAttributeName::SenderId),
            message_group_id: take(MessageSystemAttributeName::MessageGroupId),
            message_deduplication_id: take(MessageSystemAttributeName::MessageDeduplicationId),
            sequence_number: take(MessageSystemAttributeName::SequenceNumber),
            aws_trace_header: take(MessageSystemAttributeName::AwsTraceHeader),
            dead_letter_queue_source_arn: take(MessageSystemAttributeName::DeadLetterQueueSourceArn),
        },
    })
}

fn batch_error(entry: &aws_sdk_sqs::types::BatchResultErrorEntry) -> BatchEntryError {
    BatchEntryError {
        code: entry.code().to_string(),
        message: entry.message().map(str::to_string),
        sender_fault: entry.sender_fault(),
    }
}

/// Builds per-entry results from successful ids and failures (ids are entry indexes).
fn collect_batch<T>(
    count: usize,
    successes: impl IntoIterator<Item = (String, T)>,
    failures: impl IntoIterator<Item = (String, BatchEntryError)>,
) -> BatchResult<T> {
    let mut results: Vec<Option<std::result::Result<T, BatchEntryError>>> = (0..count).map(|_| None).collect();
    for (id, value) in successes {
        if let Some(slot) = id.parse::<usize>().ok().and_then(|index| results.get_mut(index)) {
            *slot = Some(Ok(value));
        }
    }
    for (id, error) in failures {
        if let Some(slot) = id.parse::<usize>().ok().and_then(|index| results.get_mut(index)) {
            *slot = Some(Err(error));
        }
    }
    results
        .into_iter()
        .map(|slot| {
            slot.unwrap_or_else(|| {
                Err(BatchEntryError {
                    code: "MissingBatchResult".into(),
                    message: Some("the service returned no result for this entry".into()),
                    sender_fault: false,
                })
            })
        })
        .collect()
}

fn to_string_map(attributes: Option<&HashMap<QueueAttributeName, String>>) -> StringMap {
    attributes.map(|map| map.iter().map(|(k, v)| (k.as_str().to_string(), v.clone())).collect()).unwrap_or_default()
}

fn to_queue_attributes(attributes: &StringMap) -> Option<HashMap<QueueAttributeName, String>> {
    if attributes.is_empty() {
        return None;
    }
    Some(attributes.iter().map(|(k, v)| (QueueAttributeName::from(k.as_str()), v.clone())).collect())
}

#[async_trait]
impl SqsApi for AwsSqs {
    async fn send_message(&self, queue_url: &str, request: SendRequest) -> Result<SendOutput> {
        let size = request.body.len() + attributes_encoded_len(&request.attributes);
        let mut call = self
            .client
            .send_message()
            .queue_url(queue_url)
            .message_body(request.body)
            .set_message_attributes(to_sdk_attributes(request.attributes)?)
            .set_delay_seconds(request.delay.map(timeout_secs_i32))
            .set_message_group_id(request.group_id)
            .set_message_deduplication_id(request.deduplication_id);
        if let Some(header) = &request.trace_header {
            call = call.message_system_attributes(
                MessageSystemAttributeNameForSends::AwsTraceHeader,
                trace_header_attribute(header)?,
            );
        }
        let output = match upload_timeouts(self.client.config().timeout_config(), size) {
            Some(timeouts) => {
                let timeouts = aws_sdk_sqs::config::Builder::default().timeout_config(timeouts);
                call.customize().config_override(timeouts).send().await
            }
            None => call.send().await,
        }
        .map_err(|err| map_sdk_error("sqs", "SendMessage", err))?;
        Ok(SendOutput {
            message_id: output.message_id().unwrap_or_default().to_string(),
            sequence_number: output.sequence_number().map(str::to_string),
            md5_of_body: output.md5_of_message_body().map(str::to_string),
            md5_of_message_attributes: output.md5_of_message_attributes().map(str::to_string),
        })
    }

    async fn send_message_batch(&self, queue_url: &str, entries: Vec<SendRequest>) -> Result<BatchResult<SendOutput>> {
        let count = entries.len();
        let size = entries.iter().map(|entry| entry.body.len() + attributes_encoded_len(&entry.attributes)).sum();
        let mut sdk_entries = Vec::with_capacity(count);
        for (index, entry) in entries.into_iter().enumerate() {
            let mut builder = SendMessageBatchRequestEntry::builder()
                .id(index.to_string())
                .message_body(entry.body)
                .set_message_attributes(to_sdk_attributes(entry.attributes)?)
                .set_delay_seconds(entry.delay.map(timeout_secs_i32))
                .set_message_group_id(entry.group_id)
                .set_message_deduplication_id(entry.deduplication_id);
            if let Some(header) = &entry.trace_header {
                builder = builder.message_system_attributes(
                    MessageSystemAttributeNameForSends::AwsTraceHeader,
                    trace_header_attribute(header)?,
                );
            }
            sdk_entries.push(builder.build().map_err(|err| Error::validation(format!("invalid batch entry: {err}")))?);
        }
        let call = self.client.send_message_batch().queue_url(queue_url).set_entries(Some(sdk_entries));
        let output = match upload_timeouts(self.client.config().timeout_config(), size) {
            Some(timeouts) => {
                let timeouts = aws_sdk_sqs::config::Builder::default().timeout_config(timeouts);
                call.customize().config_override(timeouts).send().await
            }
            None => call.send().await,
        }
        .map_err(|err| map_sdk_error("sqs", "SendMessageBatch", err))?;
        Ok(collect_batch(
            count,
            output.successful().iter().map(|entry| {
                (
                    entry.id().to_string(),
                    SendOutput {
                        message_id: entry.message_id().to_string(),
                        sequence_number: entry.sequence_number().map(str::to_string),
                        md5_of_body: Some(entry.md5_of_message_body().to_string()),
                        md5_of_message_attributes: entry.md5_of_message_attributes().map(str::to_string),
                    },
                )
            }),
            output.failed().iter().map(|entry| (entry.id().to_string(), batch_error(entry))),
        ))
    }

    async fn receive_messages(&self, request: ReceiveRequest) -> Result<Vec<ReceivedMessage>> {
        let wait = request.wait_time.min(Duration::from_secs(20));
        // Long polls must not be cut short by user-configured read, attempt or operation timeouts
        // (unset fields of an override inherit the client's): a poll cut short stays open on the
        // server, where it may still take messages that then stay invisible.
        let budget = wait + Duration::from_secs(15);
        let configured = self.client.config().timeout_config();
        let at_least_budget = |timeout: Option<Duration>| timeout.map_or(budget, |timeout| timeout.max(budget));
        let mut timeouts = TimeoutConfig::builder()
            .read_timeout(at_least_budget(configured.and_then(TimeoutConfig::read_timeout)))
            .operation_attempt_timeout(at_least_budget(configured.and_then(TimeoutConfig::operation_attempt_timeout)));
        if let Some(operation) = configured.and_then(TimeoutConfig::operation_timeout) {
            timeouts = timeouts.operation_timeout(operation.max(budget));
        }
        let override_config = aws_sdk_sqs::config::Builder::default().timeout_config(timeouts.build());
        let output = self
            .client
            .receive_message()
            .queue_url(&request.queue_url)
            .max_number_of_messages(i32::from(request.max_messages.clamp(1, 10)))
            .wait_time_seconds(duration_secs_i32(wait))
            .set_visibility_timeout(request.visibility_timeout.map(timeout_secs_i32))
            .set_receive_request_attempt_id(request.attempt_id)
            .message_system_attribute_names(MessageSystemAttributeName::All)
            .message_attribute_names("All")
            .customize()
            .config_override(override_config)
            .send()
            .await
            .map_err(|err| map_sdk_error("sqs", "ReceiveMessage", err))?;
        Ok(output.messages.unwrap_or_default().into_iter().filter_map(to_received).collect())
    }

    async fn delete_message_batch(&self, queue_url: &str, receipt_handles: Vec<String>) -> Result<BatchResult<()>> {
        let count = receipt_handles.len();
        let entries = receipt_handles
            .into_iter()
            .enumerate()
            .map(|(index, handle)| {
                DeleteMessageBatchRequestEntry::builder()
                    .id(index.to_string())
                    .receipt_handle(handle)
                    .build()
                    .map_err(|err| Error::validation(format!("invalid delete entry: {err}")))
            })
            .collect::<Result<Vec<_>>>()?;
        let output = self
            .client
            .delete_message_batch()
            .queue_url(queue_url)
            .set_entries(Some(entries))
            .send()
            .await
            .map_err(|err| map_sdk_error("sqs", "DeleteMessageBatch", err))?;
        Ok(collect_batch(
            count,
            output.successful().iter().map(|entry| (entry.id().to_string(), ())),
            output.failed().iter().map(|entry| (entry.id().to_string(), batch_error(entry))),
        ))
    }

    async fn change_visibility_batch(
        &self,
        queue_url: &str,
        entries: Vec<VisibilityChange>,
    ) -> Result<BatchResult<()>> {
        let count = entries.len();
        let sdk_entries = entries
            .into_iter()
            .enumerate()
            .map(|(index, entry)| {
                ChangeMessageVisibilityBatchRequestEntry::builder()
                    .id(index.to_string())
                    .receipt_handle(entry.receipt_handle)
                    .visibility_timeout(timeout_secs_i32(entry.timeout))
                    .build()
                    .map_err(|err| Error::validation(format!("invalid visibility entry: {err}")))
            })
            .collect::<Result<Vec<_>>>()?;
        let output = self
            .client
            .change_message_visibility_batch()
            .queue_url(queue_url)
            .set_entries(Some(sdk_entries))
            .send()
            .await
            .map_err(|err| map_sdk_error("sqs", "ChangeMessageVisibilityBatch", err))?;
        Ok(collect_batch(
            count,
            output.successful().iter().map(|entry| (entry.id().to_string(), ())),
            output.failed().iter().map(|entry| (entry.id().to_string(), batch_error(entry))),
        ))
    }

    async fn get_queue_url(&self, name: &str, owner_account_id: Option<&str>) -> Result<String> {
        let output = self
            .client
            .get_queue_url()
            .queue_name(name)
            .set_queue_owner_aws_account_id(owner_account_id.map(str::to_string))
            .send()
            .await
            .map_err(|err| map_sdk_error("sqs", "GetQueueUrl", err))?;
        output.queue_url().map(str::to_string).ok_or_else(|| Error::validation("GetQueueUrl returned no URL"))
    }

    async fn get_queue_attributes(&self, queue_url: &str) -> Result<StringMap> {
        let output = self
            .client
            .get_queue_attributes()
            .queue_url(queue_url)
            .attribute_names(QueueAttributeName::All)
            .send()
            .await
            .map_err(|err| map_sdk_error("sqs", "GetQueueAttributes", err))?;
        Ok(to_string_map(output.attributes()))
    }

    async fn create_queue(&self, name: &str, attributes: &StringMap, tags: &StringMap) -> Result<String> {
        let output = self
            .client
            .create_queue()
            .queue_name(name)
            .set_attributes(to_queue_attributes(attributes))
            .set_tags((!tags.is_empty()).then(|| tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect()))
            .send()
            .await
            .map_err(|err| map_sdk_error("sqs", "CreateQueue", err))?;
        output.queue_url().map(str::to_string).ok_or_else(|| Error::validation("CreateQueue returned no URL"))
    }

    async fn set_queue_attributes(&self, queue_url: &str, attributes: &StringMap) -> Result<()> {
        if attributes.is_empty() {
            return Ok(());
        }
        self.client
            .set_queue_attributes()
            .queue_url(queue_url)
            .set_attributes(to_queue_attributes(attributes))
            .send()
            .await
            .map_err(|err| map_sdk_error("sqs", "SetQueueAttributes", err))?;
        Ok(())
    }

    async fn list_queue_tags(&self, queue_url: &str) -> Result<StringMap> {
        let output = self
            .client
            .list_queue_tags()
            .queue_url(queue_url)
            .send()
            .await
            .map_err(|err| map_sdk_error("sqs", "ListQueueTags", err))?;
        Ok(output.tags().map(|tags| tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect()).unwrap_or_default())
    }

    async fn tag_queue(&self, queue_url: &str, tags: &StringMap) -> Result<()> {
        if tags.is_empty() {
            return Ok(());
        }
        self.client
            .tag_queue()
            .queue_url(queue_url)
            .set_tags(Some(tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect()))
            .send()
            .await
            .map_err(|err| map_sdk_error("sqs", "TagQueue", err))?;
        Ok(())
    }

    async fn delete_queue(&self, queue_url: &str) -> Result<()> {
        self.client
            .delete_queue()
            .queue_url(queue_url)
            .send()
            .await
            .map_err(|err| map_sdk_error("sqs", "DeleteQueue", err))?;
        Ok(())
    }

    async fn purge_queue(&self, queue_url: &str) -> Result<()> {
        self.client
            .purge_queue()
            .queue_url(queue_url)
            .send()
            .await
            .map_err(|err| map_sdk_error("sqs", "PurgeQueue", err))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_batch_aligns_results_by_id() {
        let results = collect_batch(
            3,
            vec![("2".to_string(), "c"), ("0".to_string(), "a")],
            vec![("1".to_string(), BatchEntryError { code: "X".into(), message: None, sender_fault: true })],
        );
        assert_eq!(results[0], Ok("a"));
        assert!(results[1].is_err());
        assert_eq!(results[2], Ok("c"));
    }

    #[test]
    fn missing_results_are_retryable_errors() {
        let results: BatchResult<()> = collect_batch(2, vec![("0".to_string(), ())], vec![]);
        let error = results[1].clone().unwrap_err();
        assert!(error.is_retryable());
    }

    #[test]
    fn timestamps_are_parsed_from_millis() {
        let parsed = millis_to_time(Some(&"1700000000123".to_string())).unwrap();
        assert_eq!(parsed.unix_timestamp(), 1_700_000_000);
        assert_eq!(parsed.millisecond(), 123);
        assert!(millis_to_time(Some(&"nope".to_string())).is_none());
    }
}
