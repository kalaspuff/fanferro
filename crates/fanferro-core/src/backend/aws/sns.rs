//! SNS adapter.

use std::collections::HashMap;

use async_trait::async_trait;
use aws_sdk_sns::types::{MessageAttributeValue, PublishBatchRequestEntry, Tag};
use aws_smithy_types::Blob;

use super::errors::map_sdk_error;
use super::upload_timeouts;
use crate::backend::{
    BatchEntryError, BatchResult, PublishOutput, PublishRequest, SnsApi, StringMap, SubscriptionInfo,
};
use crate::error::{Error, Result};
use crate::types::attributes::attributes_encoded_len;
use crate::types::{AttributeData, AttributeValue, MessageAttributes};

/// SNS backed by the AWS SDK.
#[derive(Clone, Debug)]
pub struct AwsSns {
    client: aws_sdk_sns::Client,
}

impl AwsSns {
    /// Wraps an SDK client.
    pub fn new(client: aws_sdk_sns::Client) -> Self {
        Self { client }
    }
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

fn to_tags(tags: &StringMap) -> Result<Vec<Tag>> {
    tags.iter()
        .map(|(key, value)| {
            Tag::builder().key(key).value(value).build().map_err(|err| Error::validation(format!("invalid tag: {err}")))
        })
        .collect()
}

fn string_map(attributes: Option<&HashMap<String, String>>) -> StringMap {
    attributes.map(|map| map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()).unwrap_or_default()
}

#[async_trait]
impl SnsApi for AwsSns {
    async fn publish(&self, topic_arn: &str, request: PublishRequest) -> Result<PublishOutput> {
        let trace_header = request.trace_header.clone();
        let size = request.message.len() + attributes_encoded_len(&request.attributes);
        let timeouts = upload_timeouts(self.client.config().timeout_config(), size);
        let call = self
            .client
            .publish()
            .topic_arn(topic_arn)
            .message(request.message)
            .set_message_attributes(to_sdk_attributes(request.attributes)?)
            .set_subject(request.subject)
            .set_message_group_id(request.group_id)
            .set_message_deduplication_id(request.deduplication_id);
        let output = if trace_header.is_none() && timeouts.is_none() {
            call.send().await
        } else {
            let mut call = call.customize();
            if let Some(header) = trace_header {
                call = call.mutate_request(move |http_request| {
                    // Best effort: a value that is not a valid header is left out (no panic).
                    let _ = http_request.headers_mut().try_insert("x-amzn-trace-id", header.clone());
                });
            }
            if let Some(timeouts) = timeouts {
                call = call.config_override(aws_sdk_sns::config::Builder::default().timeout_config(timeouts));
            }
            call.send().await
        }
        .map_err(|err| map_sdk_error("sns", "Publish", err))?;
        Ok(PublishOutput {
            message_id: output.message_id().unwrap_or_default().to_string(),
            sequence_number: output.sequence_number().map(str::to_string),
        })
    }

    async fn publish_batch(&self, topic_arn: &str, entries: Vec<PublishRequest>) -> Result<BatchResult<PublishOutput>> {
        let count = entries.len();
        let size = entries.iter().map(|entry| entry.message.len() + attributes_encoded_len(&entry.attributes)).sum();
        let mut sdk_entries = Vec::with_capacity(count);
        for (index, entry) in entries.into_iter().enumerate() {
            sdk_entries.push(
                PublishBatchRequestEntry::builder()
                    .id(index.to_string())
                    .message(entry.message)
                    .set_message_attributes(to_sdk_attributes(entry.attributes)?)
                    .set_subject(entry.subject)
                    .set_message_group_id(entry.group_id)
                    .set_message_deduplication_id(entry.deduplication_id)
                    .build()
                    .map_err(|err| Error::validation(format!("invalid batch entry: {err}")))?,
            );
        }
        let call =
            self.client.publish_batch().topic_arn(topic_arn).set_publish_batch_request_entries(Some(sdk_entries));
        let output = match upload_timeouts(self.client.config().timeout_config(), size) {
            Some(timeouts) => {
                let timeouts = aws_sdk_sns::config::Builder::default().timeout_config(timeouts);
                call.customize().config_override(timeouts).send().await
            }
            None => call.send().await,
        }
        .map_err(|err| map_sdk_error("sns", "PublishBatch", err))?;
        let mut results: Vec<Option<std::result::Result<PublishOutput, BatchEntryError>>> =
            (0..count).map(|_| None).collect();
        for entry in output.successful() {
            if let Some(slot) = entry.id().and_then(|id| id.parse::<usize>().ok()).and_then(|i| results.get_mut(i)) {
                *slot = Some(Ok(PublishOutput {
                    message_id: entry.message_id().unwrap_or_default().to_string(),
                    sequence_number: entry.sequence_number().map(str::to_string),
                }));
            }
        }
        for entry in output.failed() {
            if let Some(slot) = entry.id().parse::<usize>().ok().and_then(|i| results.get_mut(i)) {
                *slot = Some(Err(BatchEntryError {
                    code: entry.code().to_string(),
                    message: entry.message().map(str::to_string),
                    sender_fault: entry.sender_fault(),
                }));
            }
        }
        Ok(results
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
            .collect())
    }

    async fn create_topic(&self, name: &str, attributes: &StringMap, tags: &StringMap) -> Result<String> {
        let output = self
            .client
            .create_topic()
            .name(name)
            .set_attributes(
                (!attributes.is_empty()).then(|| attributes.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            )
            .set_tags((!tags.is_empty()).then(|| to_tags(tags)).transpose()?)
            .send()
            .await
            .map_err(|err| map_sdk_error("sns", "CreateTopic", err))?;
        output.topic_arn().map(str::to_string).ok_or_else(|| Error::validation("CreateTopic returned no ARN"))
    }

    async fn get_topic_attributes(&self, topic_arn: &str) -> Result<StringMap> {
        let output = self
            .client
            .get_topic_attributes()
            .topic_arn(topic_arn)
            .send()
            .await
            .map_err(|err| map_sdk_error("sns", "GetTopicAttributes", err))?;
        Ok(string_map(output.attributes()))
    }

    async fn set_topic_attribute(&self, topic_arn: &str, name: &str, value: &str) -> Result<()> {
        self.client
            .set_topic_attributes()
            .topic_arn(topic_arn)
            .attribute_name(name)
            .attribute_value(value)
            .send()
            .await
            .map_err(|err| map_sdk_error("sns", "SetTopicAttributes", err))?;
        Ok(())
    }

    async fn list_topic_tags(&self, topic_arn: &str) -> Result<StringMap> {
        let output = self
            .client
            .list_tags_for_resource()
            .resource_arn(topic_arn)
            .send()
            .await
            .map_err(|err| map_sdk_error("sns", "ListTagsForResource", err))?;
        Ok(output.tags().iter().map(|tag| (tag.key().to_string(), tag.value().to_string())).collect())
    }

    async fn tag_topic(&self, topic_arn: &str, tags: &StringMap) -> Result<()> {
        if tags.is_empty() {
            return Ok(());
        }
        self.client
            .tag_resource()
            .resource_arn(topic_arn)
            .set_tags(Some(to_tags(tags)?))
            .send()
            .await
            .map_err(|err| map_sdk_error("sns", "TagResource", err))?;
        Ok(())
    }

    async fn delete_topic(&self, topic_arn: &str) -> Result<()> {
        self.client
            .delete_topic()
            .topic_arn(topic_arn)
            .send()
            .await
            .map_err(|err| map_sdk_error("sns", "DeleteTopic", err))?;
        Ok(())
    }

    async fn subscribe(
        &self,
        topic_arn: &str,
        protocol: &str,
        endpoint: &str,
        attributes: &StringMap,
    ) -> Result<String> {
        let output = self
            .client
            .subscribe()
            .topic_arn(topic_arn)
            .protocol(protocol)
            .endpoint(endpoint)
            .return_subscription_arn(true)
            .set_attributes(
                (!attributes.is_empty()).then(|| attributes.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            )
            .send()
            .await
            .map_err(|err| map_sdk_error("sns", "Subscribe", err))?;
        output.subscription_arn().map(str::to_string).ok_or_else(|| Error::validation("Subscribe returned no ARN"))
    }

    async fn list_subscriptions_by_topic(&self, topic_arn: &str) -> Result<Vec<SubscriptionInfo>> {
        let mut subscriptions = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            let output = self
                .client
                .list_subscriptions_by_topic()
                .topic_arn(topic_arn)
                .set_next_token(next_token.take())
                .send()
                .await
                .map_err(|err| map_sdk_error("sns", "ListSubscriptionsByTopic", err))?;
            subscriptions.extend(output.subscriptions().iter().map(|sub| SubscriptionInfo {
                subscription_arn: sub.subscription_arn().unwrap_or_default().to_string(),
                protocol: sub.protocol().unwrap_or_default().to_string(),
                endpoint: sub.endpoint().unwrap_or_default().to_string(),
                topic_arn: sub.topic_arn().unwrap_or(topic_arn).to_string(),
            }));
            match output.next_token() {
                Some(token) if !token.is_empty() => next_token = Some(token.to_string()),
                _ => break,
            }
        }
        Ok(subscriptions)
    }

    async fn get_subscription_attributes(&self, subscription_arn: &str) -> Result<StringMap> {
        let output = self
            .client
            .get_subscription_attributes()
            .subscription_arn(subscription_arn)
            .send()
            .await
            .map_err(|err| map_sdk_error("sns", "GetSubscriptionAttributes", err))?;
        Ok(string_map(output.attributes()))
    }

    async fn set_subscription_attribute(&self, subscription_arn: &str, name: &str, value: &str) -> Result<()> {
        self.client
            .set_subscription_attributes()
            .subscription_arn(subscription_arn)
            .attribute_name(name)
            .attribute_value(value)
            .send()
            .await
            .map_err(|err| map_sdk_error("sns", "SetSubscriptionAttributes", err))?;
        Ok(())
    }

    async fn unsubscribe(&self, subscription_arn: &str) -> Result<()> {
        self.client
            .unsubscribe()
            .subscription_arn(subscription_arn)
            .send()
            .await
            .map_err(|err| map_sdk_error("sns", "Unsubscribe", err))?;
        Ok(())
    }

    async fn confirm_subscription(&self, topic_arn: &str, token: &str) -> Result<String> {
        let output = self
            .client
            .confirm_subscription()
            .topic_arn(topic_arn)
            .token(token)
            .send()
            .await
            .map_err(|err| map_sdk_error("sns", "ConfirmSubscription", err))?;
        Ok(output.subscription_arn().unwrap_or_default().to_string())
    }
}
