//! Declarative provisioning of queues, topics and subscriptions.
//!
//! The flow for every resource is the same:
//!
//! 1. **lookup** (unless `policy.lookup` is false, in which case the URL/ARN is derived
//!    from the spec without any API call and the status is `Assumed`);
//! 2. **verify**: read attributes/tags and compute [`Drift`] against the spec;
//! 3. **update** drifted mutable attributes/tags if `policy.update` is set;
//! 4. **create** the resource when it is missing and `policy.create` is set.
//!
//! Failures never raise here: they are reported in the returned [`Report`] (status
//! `Missing`/`Failed`, drift list, error) so the Python layer can apply the configured
//! failure mode (raise, log, ignore or a custom fallback).

pub mod drift;
pub mod policy_doc;
mod queue;
mod subscription;
mod topic;

use serde::Deserialize;

pub use drift::Drift;
pub use queue::QueueInfo;
pub use subscription::SubscriptionInfo;
pub use topic::TopicInfo;

use std::time::Duration;

use crate::backend::StringMap;
use crate::client::Client;
use crate::error::{Error, ResourceKind, Result};
use crate::events::{Event, EventKind};
use crate::retry::Backoff;

fn default_true() -> bool {
    true
}

/// What provisioning is allowed to do.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ProvisionPolicy {
    /// Look the resource up. When `false`, the URL/ARN is derived from the spec and no
    /// API calls are made (for least-privilege deployments).
    #[serde(default = "default_true")]
    pub lookup: bool,
    /// Read attributes and report drift.
    #[serde(default = "default_true")]
    pub verify: bool,
    /// Create the resource if it does not exist.
    pub create: bool,
    /// Update drifted attributes and tags.
    pub update: bool,
}

impl Default for ProvisionPolicy {
    fn default() -> Self {
        Self { lookup: true, verify: true, create: false, update: false }
    }
}

/// Desired state of a queue.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueSpec {
    /// Queue name (required unless `url` is given).
    #[serde(default)]
    pub name: Option<String>,
    /// Queue URL (skips `GetQueueUrl`).
    #[serde(default)]
    pub url: Option<String>,
    /// Owner account for cross-account queues.
    #[serde(default)]
    pub owner_account_id: Option<String>,
    /// Desired attributes (AWS attribute names).
    #[serde(default)]
    pub attributes: StringMap,
    /// Desired tags.
    #[serde(default)]
    pub tags: StringMap,
    /// Dead-letter queue to provision first and wire up with a `RedrivePolicy`.
    #[serde(default)]
    pub dead_letter: Option<Box<DeadLetterSpec>>,
    /// What provisioning may do.
    #[serde(default)]
    pub policy: ProvisionPolicy,
}

/// Dead-letter queue configuration of a queue.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeadLetterSpec {
    /// The dead-letter queue.
    pub queue: QueueSpec,
    /// Receives before SQS moves a message to the dead-letter queue.
    pub max_receive_count: u32,
}

/// Desired state of a topic.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicSpec {
    /// Topic name (required unless `arn` is given).
    #[serde(default)]
    pub name: Option<String>,
    /// Topic ARN.
    #[serde(default)]
    pub arn: Option<String>,
    /// Desired attributes.
    #[serde(default)]
    pub attributes: StringMap,
    /// Desired tags.
    #[serde(default)]
    pub tags: StringMap,
    /// What provisioning may do.
    #[serde(default)]
    pub policy: ProvisionPolicy,
}

/// Desired state of an SNS-to-SQS subscription.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionSpec {
    /// Topic ARN.
    pub topic_arn: String,
    /// Queue ARN (the subscription endpoint).
    pub queue_arn: String,
    /// Queue URL (for policy management).
    pub queue_url: String,
    /// Desired subscription attributes (`RawMessageDelivery`, `FilterPolicy`,
    /// `FilterPolicyScope`, `RedrivePolicy`).
    #[serde(default)]
    pub attributes: StringMap,
    /// Ensure the queue policy lets the topic send messages.
    #[serde(default = "default_true")]
    pub manage_queue_policy: bool,
    /// URL of the subscription's dead-letter queue (its policy is managed too).
    #[serde(default)]
    pub dead_letter_queue_url: Option<String>,
    /// ARN of the subscription's dead-letter queue.
    #[serde(default)]
    pub dead_letter_queue_arn: Option<String>,
    /// What provisioning may do.
    #[serde(default)]
    pub policy: ProvisionPolicy,
}

/// Outcome of provisioning one resource.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProvisionStatus {
    /// Exists (drift, if any, is listed in the report).
    Found,
    /// Created.
    Created,
    /// Existed and was updated.
    Updated,
    /// Not looked up; derived from the spec.
    Assumed,
    /// Does not exist and was not created.
    Missing,
    /// An error occurred (see the report's error).
    Failed,
}

impl ProvisionStatus {
    /// Lowercase name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Found => "found",
            Self::Created => "created",
            Self::Updated => "updated",
            Self::Assumed => "assumed",
            Self::Missing => "missing",
            Self::Failed => "failed",
        }
    }
}

/// The result of provisioning a resource.
#[derive(Clone, Debug)]
pub struct Report<T> {
    /// Resource kind.
    pub kind: ResourceKind,
    /// Name, URL or ARN used to address the resource.
    pub name: String,
    /// Outcome.
    pub status: ProvisionStatus,
    /// The resolved resource (absent when missing or when lookup failed).
    pub resource: Option<T>,
    /// Attribute/tag differences that remain after provisioning.
    pub drift: Vec<Drift>,
    /// Human-readable list of changes made.
    pub actions: Vec<String>,
    /// The error, when the status is `Failed`.
    pub error: Option<Error>,
    /// Report of the dead-letter queue (queues only).
    pub dead_letter: Option<Box<Report<QueueInfo>>>,
}

impl<T> Report<T> {
    pub(crate) fn new(kind: ResourceKind, name: impl Into<String>) -> Self {
        Self {
            kind,
            name: name.into(),
            status: ProvisionStatus::Missing,
            resource: None,
            drift: Vec::new(),
            actions: Vec::new(),
            error: None,
            dead_letter: None,
        }
    }

    pub(crate) fn failed(mut self, error: Error) -> Self {
        self.status = ProvisionStatus::Failed;
        self.error = Some(error);
        self
    }
}

/// Whether a queue's `KmsMasterKeyId` is the AWS managed key of SQS, whose key policy does not
/// allow SNS: SNS cannot deliver to such a queue (docs/parity S8).
pub fn is_aws_managed_sqs_key(key_id: &str) -> bool {
    key_id == "alias/aws/sqs" || (key_id.starts_with("arn:") && key_id.ends_with(":alias/aws/sqs"))
}

/// Attempts at provisioning one resource, the first included, while its calls fail transiently.
pub(crate) const PROVISION_ATTEMPTS: u32 = 5;

/// Delays between the attempts: about 0.5, 1, 2 and 4 seconds (with jitter, at least half of each).
const PROVISION_BACKOFF: Backoff =
    Backoff { initial: Duration::from_millis(500), max: Duration::from_secs(4), multiplier: 2.0, jitter: true };

/// Whether provisioning that ended in this report may succeed when tried again: AWS throttled a
/// call or failed transiently, or a resource was not found that provisioning had just created or
/// resolved (AWS makes new queues and topics visible to every call only after a moment, and asks
/// to wait a second after `CreateQueue`; docs/parity R1). Not when SQS refuses to create a queue
/// within 60 seconds of deleting one of the same name: the retries would end long before that.
fn worth_retrying<T>(report: &Report<T>) -> bool {
    report.status == ProvisionStatus::Failed
        && report.error.as_ref().is_some_and(|error| {
            let deleted_recently = error
                .aws()
                .and_then(|aws| aws.code.as_deref())
                .is_some_and(|code| code.ends_with("QueueDeletedRecently"));
            (error.is_retryable() || error.is_not_found()) && !deleted_recently
        })
}

impl Client {
    /// Runs one provisioning attempt, and again (at most [`PROVISION_ATTEMPTS`] in all) while it
    /// fails transiently. Every step of provisioning is idempotent: a later attempt finds what an
    /// earlier one created, and its report keeps the earlier attempts' actions.
    pub(crate) async fn with_provisioning_retries<T, F, Fut>(&self, mut attempt: F) -> Result<Report<T>>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<Report<T>>>,
    {
        let mut earlier: Vec<String> = Vec::new();
        let mut number = 1;
        loop {
            let mut report = attempt().await?;
            if !earlier.is_empty() {
                let created = earlier.iter().any(|action| action.starts_with("created "));
                let later = std::mem::take(&mut report.actions);
                report.actions = std::mem::take(&mut earlier);
                for action in later {
                    if !report.actions.contains(&action) {
                        report.actions.push(action);
                    }
                }
                if created && matches!(report.status, ProvisionStatus::Found | ProvisionStatus::Updated) {
                    report.status = ProvisionStatus::Created;
                }
            }
            if number >= PROVISION_ATTEMPTS || !worth_retrying(&report) {
                return Ok(report);
            }
            let ceiling = PROVISION_BACKOFF.ceiling(number - 1);
            let delay = ceiling / 2 + PROVISION_BACKOFF.delay(number - 1) / 2;
            self.inner.events.emit(EventKind::ResourceRetried, || {
                let mut event = Event::new(
                    EventKind::ResourceRetried,
                    format!("provisioning {} {} failed; retrying", report.kind, report.name),
                )
                .field("resource", report.kind.as_str())
                .field("name", &report.name)
                .field("attempt", number)
                .field("delay", delay.as_secs_f64());
                if let Some(error) = &report.error {
                    event = event.error(error);
                }
                event
            });
            earlier = report.actions;
            tokio::time::sleep(delay).await;
            number += 1;
        }
    }

    pub(crate) fn emit_resource(
        &self,
        kind: EventKind,
        resource: ResourceKind,
        name: &str,
        detail: impl FnOnce(Event) -> Event,
    ) {
        self.inner.events.emit(kind, || {
            let message = match kind {
                EventKind::ResourceCreated => format!("{resource} created"),
                EventKind::ResourceUpdated => format!("{resource} updated"),
                EventKind::ResourceFound => format!("{resource} found"),
                EventKind::ResourceDeleted => format!("{resource} deleted"),
                EventKind::ResourcePolicyUpdated => "queue policy updated".to_string(),
                _ => format!("{resource} {}", kind.as_str()),
            };
            detail(Event::new(kind, message).field("resource", resource.as_str()).field("name", name))
        });
    }

    /// Deletes a queue (teardown, tests).
    pub async fn delete_queue(&self, queue_url: &str) -> Result<()> {
        self.inner.backend.sqs.delete_queue(queue_url).await?;
        self.emit_resource(EventKind::ResourceDeleted, ResourceKind::Queue, queue_url, |e| e);
        Ok(())
    }

    /// Deletes a topic (teardown, tests).
    pub async fn delete_topic(&self, topic_arn: &str) -> Result<()> {
        self.inner.backend.sns.delete_topic(topic_arn).await?;
        self.emit_resource(EventKind::ResourceDeleted, ResourceKind::Topic, topic_arn, |e| e);
        Ok(())
    }

    /// Removes a subscription (teardown, tests).
    pub async fn unsubscribe(&self, subscription_arn: &str) -> Result<()> {
        self.inner.backend.sns.unsubscribe(subscription_arn).await?;
        self.emit_resource(EventKind::ResourceDeleted, ResourceKind::Subscription, subscription_arn, |e| e);
        Ok(())
    }

    /// Deletes every message in a queue.
    pub async fn purge_queue(&self, queue_url: &str) -> Result<()> {
        self.inner.backend.sqs.purge_queue(queue_url).await
    }

    /// Reads all attributes of a queue.
    pub async fn queue_attributes(&self, queue_url: &str) -> Result<StringMap> {
        self.inner.backend.sqs.get_queue_attributes(queue_url).await
    }

    /// Reads all attributes of a topic.
    pub async fn topic_attributes(&self, topic_arn: &str) -> Result<StringMap> {
        self.inner.backend.sns.get_topic_attributes(topic_arn).await
    }

    /// Ensures the payload bucket exists (creates it when `create` is set).
    pub async fn ensure_bucket(&self, bucket: &str, create: bool) -> Result<Report<String>> {
        self.with_provisioning_retries(|| self.ensure_bucket_once(bucket, create)).await
    }

    async fn ensure_bucket_once(&self, bucket: &str, create: bool) -> Result<Report<String>> {
        let mut report = Report::new(ResourceKind::Bucket, bucket);
        match self.inner.backend.objects.bucket_exists(bucket).await {
            Ok(true) => {
                report.status = ProvisionStatus::Found;
                report.resource = Some(bucket.to_string());
            }
            Ok(false) if create => match self.inner.backend.objects.create_bucket(bucket).await {
                Ok(()) => {
                    report.status = ProvisionStatus::Created;
                    report.resource = Some(bucket.to_string());
                    report.actions.push("created bucket".into());
                    self.emit_resource(EventKind::ResourceCreated, ResourceKind::Bucket, bucket, |e| e);
                }
                Err(err) => return Ok(report.failed(err)),
            },
            Ok(false) => report.status = ProvisionStatus::Missing,
            Err(err) => return Ok(report.failed(err)),
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests;
