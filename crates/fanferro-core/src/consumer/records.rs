//! SQS messages as an AWS Lambda event source mapping hands them to a function.
//!
//! Lambda receives the messages itself and invokes the function with them as `Records` of an
//! event; each record carries what `ReceiveMessage` returned, in Lambda's own JSON layout
//! (camel-case keys, system attributes as strings, binary attribute values in base64). This
//! module converts such records into [`ReceivedMessage`] values, so that the consumer engine can
//! decode and settle them like messages it received itself (see
//! [`ConsumerEngine::accept_records`](super::ConsumerEngine::accept_records)), and back (for tests
//! that build events from messages they received).

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use std::time::Duration;

use crate::backend::{ReceiveRequest, ReceivedMessage, SystemAttributes};
use crate::client::Client;
use crate::error::{Error, Result};
use crate::types::AttributeValue;
use crate::types::attributes::{AttributeData, MessageAttributes};

/// One record of an SQS event (`event["Records"][i]`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsEventRecord {
    /// SQS message id (the `itemIdentifier` of a batch item failure).
    pub message_id: String,
    /// Receipt handle of the receive Lambda made.
    pub receipt_handle: String,
    /// The body.
    #[serde(default)]
    pub body: String,
    /// System attributes (`ApproximateReceiveCount`, `SentTimestamp`, `MessageGroupId`...).
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
    /// Message attributes.
    #[serde(default)]
    pub message_attributes: BTreeMap<String, SqsEventAttribute>,
    /// MD5 of the body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub md5_of_body: Option<String>,
    /// MD5 of the message attributes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub md5_of_message_attributes: Option<String>,
    /// `aws:sqs`.
    #[serde(default)]
    pub event_source: String,
    /// ARN of the queue the message came from.
    #[serde(default, rename = "eventSourceARN")]
    pub event_source_arn: String,
    /// Region of the queue.
    #[serde(default)]
    pub aws_region: String,
}

/// A message attribute of an [`SqsEventRecord`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsEventAttribute {
    /// `String`, `Number` or `Binary`, optionally with a custom label (`Number.int`).
    pub data_type: String,
    /// Value of `String` and `Number` attributes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub string_value: Option<String>,
    /// Value of `Binary` attributes, in base64.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binary_value: Option<String>,
    /// Unused by SQS (always empty).
    #[serde(default)]
    pub string_list_values: Vec<String>,
    /// Unused by SQS (always empty).
    #[serde(default)]
    pub binary_list_values: Vec<String>,
}

fn millis_to_time(value: Option<&String>) -> Option<OffsetDateTime> {
    let millis: i128 = value?.parse().ok()?;
    OffsetDateTime::from_unix_timestamp_nanos(millis * 1_000_000).ok()
}

fn time_to_millis(time: OffsetDateTime) -> String {
    (time.unix_timestamp_nanos() / 1_000_000).to_string()
}

impl SqsEventRecord {
    /// The queue name, the last part of [`Self::event_source_arn`].
    pub fn queue_name(&self) -> Option<&str> {
        queue_arn_parts(&self.event_source_arn).map(|parts| parts.name)
    }

    /// Converts the record into the message `ReceiveMessage` returned.
    ///
    /// # Errors
    ///
    /// [`Error::Validation`] when the record has no message id or receipt handle, or one of its
    /// attributes has no value of its type (or binary data that is not base64).
    pub fn into_received(self) -> Result<ReceivedMessage> {
        if self.message_id.is_empty() {
            return Err(Error::validation("an SQS event record has no messageId"));
        }
        if self.receipt_handle.is_empty() {
            return Err(Error::validation(format!("SQS event record {} has no receiptHandle", self.message_id)));
        }
        let mut attributes = MessageAttributes::new();
        for (name, attribute) in self.message_attributes {
            let data = match (attribute.string_value, attribute.binary_value) {
                (Some(text), _) if !attribute.data_type.starts_with("Binary") => AttributeData::Text(text),
                (_, Some(binary)) => {
                    AttributeData::Binary(Bytes::from(BASE64.decode(binary.as_bytes()).map_err(|error| {
                        Error::validation(format!(
                            "attribute {name:?} of SQS event record {} is not base64: {error}",
                            self.message_id
                        ))
                    })?))
                }
                (Some(text), None) => AttributeData::Text(text),
                (None, None) => {
                    return Err(Error::validation(format!(
                        "attribute {name:?} of SQS event record {} has no value",
                        self.message_id
                    )));
                }
            };
            attributes.insert(name, AttributeValue::with_type(attribute.data_type, data)?);
        }
        let mut system = self.attributes;
        let mut take = |name: &str| system.remove(name);
        Ok(ReceivedMessage {
            message_id: self.message_id,
            receipt_handle: self.receipt_handle,
            body: self.body,
            md5_of_body: self.md5_of_body,
            attributes,
            md5_of_message_attributes: self.md5_of_message_attributes,
            system: SystemAttributes {
                approximate_receive_count: take("ApproximateReceiveCount").and_then(|v| v.parse().ok()).unwrap_or(1),
                sent_timestamp: millis_to_time(take("SentTimestamp").as_ref()),
                approximate_first_receive_timestamp: millis_to_time(take("ApproximateFirstReceiveTimestamp").as_ref()),
                sender_id: take("SenderId"),
                message_group_id: take("MessageGroupId"),
                message_deduplication_id: take("MessageDeduplicationId"),
                sequence_number: take("SequenceNumber"),
                aws_trace_header: take("AWSTraceHeader"),
                dead_letter_queue_source_arn: take("DeadLetterQueueSourceArn"),
            },
        })
    }

    /// The record Lambda would hand a function for `message`, received from the queue `queue_arn`.
    pub fn from_received(message: &ReceivedMessage, queue_arn: &str) -> Self {
        let system = &message.system;
        let mut attributes = BTreeMap::new();
        attributes.insert("ApproximateReceiveCount".to_string(), system.approximate_receive_count.to_string());
        let mut put = |name: &str, value: Option<String>| {
            if let Some(value) = value {
                attributes.insert(name.to_string(), value);
            }
        };
        put("SentTimestamp", system.sent_timestamp.map(time_to_millis));
        put("ApproximateFirstReceiveTimestamp", system.approximate_first_receive_timestamp.map(time_to_millis));
        put("SenderId", system.sender_id.clone());
        put("MessageGroupId", system.message_group_id.clone());
        put("MessageDeduplicationId", system.message_deduplication_id.clone());
        put("SequenceNumber", system.sequence_number.clone());
        put("AWSTraceHeader", system.aws_trace_header.clone());
        put("DeadLetterQueueSourceArn", system.dead_letter_queue_source_arn.clone());
        let message_attributes = message
            .attributes
            .iter()
            .map(|(name, value)| {
                let (string_value, binary_value) = match value.data() {
                    AttributeData::Text(text) => (Some(text.clone()), None),
                    AttributeData::Binary(bytes) => (None, Some(BASE64.encode(bytes))),
                };
                let attribute = SqsEventAttribute {
                    data_type: value.data_type().to_string(),
                    string_value,
                    binary_value,
                    string_list_values: Vec::new(),
                    binary_list_values: Vec::new(),
                };
                (name.clone(), attribute)
            })
            .collect();
        Self {
            message_id: message.message_id.clone(),
            receipt_handle: message.receipt_handle.clone(),
            body: message.body.clone(),
            attributes,
            message_attributes,
            md5_of_body: message.md5_of_body.clone(),
            md5_of_message_attributes: message.md5_of_message_attributes.clone(),
            event_source: "aws:sqs".to_string(),
            event_source_arn: queue_arn.to_string(),
            aws_region: queue_arn_parts(queue_arn).map(|parts| parts.region.to_string()).unwrap_or_default(),
        }
    }
}

/// Receives up to `max_messages` (1-10) messages of a queue as the records Lambda would hand a
/// function: for tests of functions that consume SQS events, with real receipt handles.
///
/// # Errors
///
/// [`Error::Validation`] for `max_messages` outside 1-10 or a `wait_time` over 20 seconds; the
/// errors of `ReceiveMessage`.
pub async fn receive_records(
    client: &Client,
    queue_url: &str,
    queue_arn: &str,
    max_messages: u8,
    wait_time: Duration,
    visibility_timeout: Option<Duration>,
) -> Result<Vec<SqsEventRecord>> {
    if !(1..=10).contains(&max_messages) {
        return Err(Error::validation(format!("max_messages must be between 1 and 10, got {max_messages}")));
    }
    if wait_time > Duration::from_secs(20) {
        return Err(Error::validation("wait_time must be at most 20 seconds"));
    }
    let request = ReceiveRequest {
        queue_url: queue_url.to_string(),
        max_messages,
        wait_time,
        visibility_timeout,
        attempt_id: None,
    };
    let messages = client.backend().sqs.receive_messages(request).await?;
    Ok(messages.iter().map(|message| SqsEventRecord::from_received(message, queue_arn)).collect())
}

/// Deletes messages by receipt handle (ten per `DeleteMessageBatch` call), as an event source
/// mapping deletes the records a function processed: for tests.
///
/// # Errors
///
/// The first error of a call or of an entry.
pub async fn delete_records(client: &Client, queue_url: &str, receipt_handles: Vec<String>) -> Result<()> {
    for chunk in receipt_handles.chunks(crate::publish::MAX_BATCH_ENTRIES) {
        let results = client.backend().sqs.delete_message_batch(queue_url, chunk.to_vec()).await?;
        if let Some(Err(entry)) = results.into_iter().find(Result::is_err) {
            return Err(Error::validation(format!(
                "cannot delete a record: {} {}",
                entry.code,
                entry.message.unwrap_or_default()
            )));
        }
    }
    Ok(())
}

/// The parts of an SQS queue ARN (`arn:<partition>:sqs:<region>:<account>:<name>`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueArnParts<'a> {
    /// `aws`, `aws-cn`, `aws-us-gov`...
    pub partition: &'a str,
    /// Region.
    pub region: &'a str,
    /// Owner account id.
    pub account_id: &'a str,
    /// Queue name.
    pub name: &'a str,
}

/// Splits an SQS queue ARN; `None` when it is not one.
pub fn queue_arn_parts(arn: &str) -> Option<QueueArnParts<'_>> {
    let mut parts = arn.splitn(6, ':');
    let (Some("arn"), Some(partition), Some("sqs"), Some(region), Some(account_id), Some(name)) =
        (parts.next(), parts.next(), parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return None;
    };
    (!name.is_empty()).then_some(QueueArnParts { partition, region, account_id, name })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> serde_json::Value {
        serde_json::json!({
            "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
            "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a",
            "body": "{\"hello\":\"world\"}",
            "attributes": {
                "ApproximateReceiveCount": "3",
                "SentTimestamp": "1545082649183",
                "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                "ApproximateFirstReceiveTimestamp": "1545082649185",
                "MessageGroupId": "g1",
                "MessageDeduplicationId": "d1",
                "SequenceNumber": "18849496460467696128",
                "AWSTraceHeader": "Root=1-5759e988-bd862e3fe1be46a994272793;Sampled=1"
            },
            "messageAttributes": {
                "fanferro.type": {"stringValue": "OrderPlaced", "stringListValues": [], "binaryListValues": [], "dataType": "String"},
                "count": {"stringValue": "7", "stringListValues": [], "binaryListValues": [], "dataType": "Number.int"},
                "blob": {"binaryValue": "AAEC", "stringListValues": [], "binaryListValues": [], "dataType": "Binary"}
            },
            "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
            "md5OfMessageAttributes": null,
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue.fifo",
            "awsRegion": "us-east-2"
        })
    }

    #[test]
    fn records_convert_into_received_messages() {
        let parsed: SqsEventRecord = serde_json::from_value(record()).unwrap();
        assert_eq!(parsed.queue_name(), Some("my-queue.fifo"));
        let message = parsed.into_received().unwrap();
        assert_eq!(message.message_id, "059f36b4-87a3-44ab-83d2-661975830a7d");
        assert_eq!(message.body, "{\"hello\":\"world\"}");
        assert_eq!(message.system.approximate_receive_count, 3);
        assert_eq!(message.system.sent_timestamp.unwrap().unix_timestamp(), 1_545_082_649);
        assert_eq!(message.system.message_group_id.as_deref(), Some("g1"));
        assert_eq!(message.system.message_deduplication_id.as_deref(), Some("d1"));
        assert_eq!(message.system.sequence_number.as_deref(), Some("18849496460467696128"));
        assert!(message.system.aws_trace_header.as_deref().unwrap().starts_with("Root=1-"));
        assert_eq!(message.attributes["fanferro.type"], AttributeValue::string("OrderPlaced"));
        assert_eq!(message.attributes["count"].data_type(), "Number.int");
        assert_eq!(message.attributes["blob"], AttributeValue::binary(vec![0u8, 1, 2]));
        assert_eq!(message.md5_of_message_attributes, None);
    }

    #[test]
    fn records_round_trip() {
        let parsed: SqsEventRecord = serde_json::from_value(record()).unwrap();
        let arn = parsed.event_source_arn.clone();
        let message = parsed.into_received().unwrap();
        let again = SqsEventRecord::from_received(&message, &arn);
        assert_eq!(again.aws_region, "us-east-2");
        assert_eq!(again.event_source, "aws:sqs");
        assert_eq!(again.into_received().unwrap(), message);
    }

    #[test]
    fn invalid_records_are_rejected() {
        let parse = |value: serde_json::Value| serde_json::from_value::<SqsEventRecord>(value).unwrap();
        let mut value = record();
        value["receiptHandle"] = "".into();
        assert!(parse(value).into_received().unwrap_err().to_string().contains("receiptHandle"));
        let mut value = record();
        value["messageAttributes"]["blob"]["binaryValue"] = "not base64!".into();
        assert!(parse(value).into_received().unwrap_err().to_string().contains("base64"));
        let mut value = record();
        value["messageAttributes"]["blob"] = serde_json::json!({"dataType": "String"});
        assert!(parse(value).into_received().unwrap_err().to_string().contains("no value"));
    }

    #[test]
    fn queue_arns_split() {
        let parts = queue_arn_parts("arn:aws:sqs:eu-north-1:123456789012:orders").unwrap();
        assert_eq!(
            parts,
            QueueArnParts { partition: "aws", region: "eu-north-1", account_id: "123456789012", name: "orders" }
        );
        assert_eq!(queue_arn_parts("arn:aws:sns:eu-north-1:123456789012:orders"), None);
        assert_eq!(queue_arn_parts("arn:aws:sqs:eu-north-1:123456789012:"), None);
        assert_eq!(queue_arn_parts("orders"), None);
    }
}
