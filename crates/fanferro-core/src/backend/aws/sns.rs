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

    /// One `PublishBatch` call, with `trace_header` as the request's `X-Amzn-Trace-Id` header.
    async fn publish_batch_call(
        &self,
        topic_arn: &str,
        trace_header: Option<String>,
        entries: Vec<PublishRequest>,
    ) -> Result<BatchResult<PublishOutput>> {
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
        let timeouts = upload_timeouts(self.client.config().timeout_config(), size);
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
        // PublishBatch has no trace header per entry, only the request's: entries with different
        // headers are sent in separate calls. On FIFO topics, runs of consecutive entries, one call
        // after the other (stopping at a failed call), so that no message overtakes an earlier one;
        // on standard topics, one call per header, all at once.
        let fifo = topic_arn.ends_with(".fifo");
        let count = entries.len();
        let mut groups: Vec<(Option<String>, Vec<usize>, Vec<PublishRequest>)> = Vec::new();
        for (index, entry) in entries.into_iter().enumerate() {
            let group = if fifo {
                groups.last_mut().filter(|(header, ..)| *header == entry.trace_header)
            } else {
                groups.iter_mut().find(|(header, ..)| *header == entry.trace_header)
            };
            match group {
                Some((_, indices, requests)) => {
                    indices.push(index);
                    requests.push(entry);
                }
                None => groups.push((entry.trace_header.clone(), vec![index], vec![entry])),
            }
        }
        if groups.len() <= 1 {
            let (header, _, requests) = groups.pop().unwrap_or_default();
            return self.publish_batch_call(topic_arn, header, requests).await;
        }
        let mut results: Vec<Option<std::result::Result<PublishOutput, BatchEntryError>>> =
            (0..count).map(|_| None).collect();
        let mut outcomes = Vec::with_capacity(groups.len());
        if fifo {
            let mut failed: Option<Error> = None;
            for (header, indices, requests) in groups {
                let outcome = match &failed {
                    // Not sent: retried together with the failed call's entries, in order.
                    Some(err) => Err(err.clone()),
                    None => self.publish_batch_call(topic_arn, header, requests).await,
                };
                if let Err(err) = &outcome {
                    failed.get_or_insert_with(|| err.clone());
                }
                outcomes.push((indices, outcome));
            }
        } else {
            let calls = groups.into_iter().map(|(header, indices, requests)| async move {
                (indices, self.publish_batch_call(topic_arn, header, requests).await)
            });
            outcomes = futures::future::join_all(calls).await;
        }
        // Nothing was published: the batch failed as a whole.
        if outcomes.iter().all(|(_, outcome)| outcome.is_err())
            && let Some((_, Err(err))) = outcomes.first()
        {
            return Err(err.clone());
        }
        for (indices, outcome) in outcomes {
            match outcome {
                Ok(entries) => {
                    for (index, entry) in indices.into_iter().zip(entries) {
                        results[index] = Some(entry);
                    }
                }
                Err(err) => {
                    let aws = err.aws().cloned();
                    let entry = BatchEntryError {
                        code: aws.as_ref().and_then(|a| a.code.clone()).unwrap_or_else(|| "InternalError".into()),
                        message: Some(err.to_string()),
                        sender_fault: !err.is_retryable(),
                    };
                    for index in indices {
                        results[index] = Some(Err(entry.clone()));
                    }
                }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::Mutex;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    use super::*;
    use crate::backend::Backend;
    use crate::backend::aws::{AwsConfig, RetrySettings, StaticCredentials, connect};

    /// A `PublishBatch` request as the server saw it: its trace header and its messages.
    type Call = (Option<String>, Vec<String>);

    /// Serves `PublishBatch`, recording each call, and fails the calls whose messages include
    /// `fail` with a retryable error.
    async fn server(fail: Option<&'static str>) -> (String, Arc<Mutex<Vec<Call>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls: Arc<Mutex<Vec<Call>>> = Arc::default();
        let recorded = calls.clone();
        tokio::spawn(async move {
            while let Ok((connection, _)) = listener.accept().await {
                let calls = recorded.clone();
                tokio::spawn(async move {
                    let mut connection = BufReader::new(connection);
                    loop {
                        let (mut trace_header, mut length) = (None, 0);
                        loop {
                            let mut line = String::new();
                            if connection.read_line(&mut line).await.unwrap_or(0) == 0 {
                                return;
                            }
                            let line = line.trim_end();
                            if line.is_empty() {
                                break;
                            }
                            if let Some((name, value)) = line.split_once(':') {
                                match name.to_ascii_lowercase().as_str() {
                                    "x-amzn-trace-id" => trace_header = Some(value.trim().to_string()),
                                    "content-length" => length = value.trim().parse().unwrap(),
                                    _ => {}
                                }
                            }
                        }
                        let mut body = vec![0; length];
                        connection.read_exact(&mut body).await.unwrap();
                        let body = String::from_utf8(body).unwrap();
                        let field = |name: &str| {
                            body.split('&')
                                .filter_map(|pair| pair.split_once('='))
                                .filter(|(key, _)| key.ends_with(name))
                                .map(|(_, value)| value.to_string())
                                .collect::<Vec<_>>()
                        };
                        let (ids, messages) = (field(".Id"), field(".Message"));
                        let failed = fail.is_some_and(|fail| messages.iter().any(|message| message == fail));
                        calls.lock().push((trace_header, messages));
                        let response = if failed {
                            let error = "<ErrorResponse><Error><Type>Receiver</Type><Code>InternalError</Code>\
                                <Message>try again</Message></Error><RequestId>r</RequestId></ErrorResponse>";
                            format!(
                                "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\n\r\n{error}",
                                error.len()
                            )
                        } else {
                            let members: String = ids
                                .iter()
                                .map(|id| format!("<member><Id>{id}</Id><MessageId>m-{id}</MessageId></member>"))
                                .collect();
                            let xml = format!(
                                "<PublishBatchResponse><PublishBatchResult><Successful>{members}</Successful><Failed/>\
                                 </PublishBatchResult><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata>\
                                 </PublishBatchResponse>"
                            );
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: text/xml\r\nContent-Length: {}\r\n\r\n{xml}",
                                xml.len()
                            )
                        };
                        connection.get_mut().write_all(response.as_bytes()).await.unwrap();
                    }
                });
            }
        });
        (format!("http://{address}"), calls)
    }

    async fn sns(endpoint: String) -> Backend {
        let config = AwsConfig {
            region: Some("us-east-1".into()),
            endpoint_url: Some(endpoint),
            credentials: Some(StaticCredentials {
                access_key_id: "test".into(),
                secret_access_key: "test".into(),
                session_token: None,
            }),
            retry: RetrySettings { max_attempts: 1, ..RetrySettings::default() },
            ..AwsConfig::default()
        };
        connect(&config).await.unwrap()
    }

    fn entry(message: &str, trace_header: Option<&str>) -> PublishRequest {
        PublishRequest {
            message: message.into(),
            group_id: Some("g".into()),
            deduplication_id: Some(message.into()),
            trace_header: trace_header.map(str::to_string),
            ..PublishRequest::default()
        }
    }

    const A: &str = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1";
    const B: &str = "Root=1-5759e988-bd862e3fe1be46a994272794;Parent=53995c3f42cd8ad9;Sampled=1";

    fn sorted(calls: &Mutex<Vec<Call>>) -> Vec<Call> {
        let mut calls = calls.lock().clone();
        calls.sort();
        calls
    }

    #[tokio::test]
    async fn batches_carry_the_trace_headers_of_their_entries() {
        // PublishBatch has no trace header per entry: the request header was left out, so batch
        // publishes to SNS lost the X-Ray trace context that single publishes carry.
        let (endpoint, calls) = server(None).await;
        let backend = sns(endpoint).await;
        let topic = "arn:aws:sns:us-east-1:000000000000:events";
        let results = backend.sns.publish_batch(topic, vec![entry("1", Some(A)), entry("2", Some(A))]).await.unwrap();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 2);
        assert_eq!(sorted(&calls), vec![(Some(A.to_string()), vec!["1".to_string(), "2".to_string()])]);

        // Different headers: one call per header (at once on a standard topic), results in order.
        calls.lock().clear();
        let entries = vec![entry("1", Some(A)), entry("2", Some(B)), entry("3", None), entry("4", Some(A))];
        let results = backend.sns.publish_batch(topic, entries).await.unwrap();
        let ids: Vec<String> = results.into_iter().map(|result| result.unwrap().message_id).collect();
        assert_eq!(ids, vec!["m-0", "m-0", "m-0", "m-1"], "ids are those of each call's entries");
        assert_eq!(
            sorted(&calls),
            vec![
                (None, vec!["3".to_string()]),
                (Some(A.to_string()), vec!["1".to_string(), "4".to_string()]),
                (Some(B.to_string()), vec!["2".to_string()]),
            ]
        );
    }

    #[tokio::test]
    async fn fifo_batches_keep_their_order_across_trace_headers() {
        let (endpoint, calls) = server(Some("3")).await;
        let backend = sns(endpoint).await;
        let topic = "arn:aws:sns:us-east-1:000000000000:events.fifo";
        let entries = vec![entry("1", Some(A)), entry("2", Some(A)), entry("3", Some(B)), entry("4", Some(A))];
        let results = backend.sns.publish_batch(topic, entries).await.unwrap();
        // Runs of consecutive entries, one call after the other; a failed call stops the rest,
        // which fail with its retryable error instead of overtaking it.
        assert_eq!(
            *calls.lock(),
            vec![
                (Some(A.to_string()), vec!["1".to_string(), "2".to_string()]),
                (Some(B.to_string()), vec!["3".to_string()]),
            ]
        );
        assert!(results[0].is_ok() && results[1].is_ok());
        for result in &results[2..] {
            let error = result.clone().unwrap_err();
            assert_eq!(error.code, "InternalError");
            assert!(!error.sender_fault, "retryable");
        }
        // Nothing published: the batch fails as a whole.
        calls.lock().clear();
        let error = backend.sns.publish_batch(topic, vec![entry("3", Some(B)), entry("5", Some(A))]).await.unwrap_err();
        assert!(error.is_retryable(), "{error:?}");
        assert_eq!(calls.lock().len(), 1);
    }
}
