//! SNS-to-SQS subscription provisioning (including the queue access policy).

use super::drift::{Drift, compare};
use super::policy_doc;
use super::{ProvisionStatus, Report, SubscriptionSpec};
use crate::backend::StringMap;
use crate::client::Client;
use crate::error::{Error, ResourceKind, Result};
use crate::events::EventKind;

/// A resolved subscription.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionInfo {
    /// Subscription ARN (empty when assumed: it cannot be derived from the topic and queue).
    pub arn: String,
    /// Topic ARN.
    pub topic_arn: String,
    /// Queue ARN.
    pub queue_arn: String,
    /// Subscription attributes.
    pub attributes: StringMap,
}

impl Client {
    /// Provisions a subscription according to its spec and policy.
    pub async fn ensure_subscription(&self, spec: &SubscriptionSpec) -> Result<Report<SubscriptionInfo>> {
        let name = format!("{} -> {}", spec.topic_arn, spec.queue_arn);
        if spec.queue_arn.ends_with(".fifo") && !spec.topic_arn.ends_with(".fifo") {
            return Err(Error::validation(format!(
                "{name}: a FIFO queue cannot be subscribed to a standard topic (SNS delivers to FIFO queues from FIFO topics only)"
            )));
        }
        let mut report: Report<SubscriptionInfo> = Report::new(ResourceKind::Subscription, name.clone());
        if !spec.policy.lookup {
            // No API calls (not even for the queue policy): the subscription ARN stays unknown.
            report.status = ProvisionStatus::Assumed;
            report.resource = Some(SubscriptionInfo {
                arn: String::new(),
                topic_arn: spec.topic_arn.clone(),
                queue_arn: spec.queue_arn.clone(),
                attributes: spec.attributes.clone(),
            });
            return Ok(report);
        }
        let existing = match self.backend().sns.list_subscriptions_by_topic(&spec.topic_arn).await {
            Ok(subscriptions) => subscriptions.into_iter().find(|s| {
                s.protocol == "sqs" && s.endpoint == spec.queue_arn && s.subscription_arn.starts_with("arn:")
            }),
            Err(err) => return Ok(report.failed(err)),
        };

        let (subscription_arn, mut attributes) = match existing {
            Some(subscription) => {
                report.status = ProvisionStatus::Found;
                self.emit_resource(EventKind::ResourceFound, ResourceKind::Subscription, &name, |e| {
                    e.field("arn", &subscription.subscription_arn)
                });
                let attributes = if spec.policy.verify || spec.policy.update {
                    match self.backend().sns.get_subscription_attributes(&subscription.subscription_arn).await {
                        Ok(attributes) => attributes,
                        Err(err) => return Ok(report.failed(err)),
                    }
                } else {
                    spec.attributes.clone()
                };
                (subscription.subscription_arn, attributes)
            }
            None if spec.policy.create => {
                // Grant access before subscribing so no early message is rejected.
                if spec.manage_queue_policy
                    && let Err(err) = self
                        .ensure_topic_permission(&spec.queue_url, &spec.queue_arn, &spec.topic_arn, &mut report)
                        .await
                {
                    return Ok(report.failed(err));
                }
                match self.backend().sns.subscribe(&spec.topic_arn, "sqs", &spec.queue_arn, &spec.attributes).await {
                    Ok(subscription_arn) => {
                        report.status = ProvisionStatus::Created;
                        report.actions.push("created subscription".into());
                        self.emit_resource(EventKind::ResourceCreated, ResourceKind::Subscription, &name, |e| {
                            e.field("arn", &subscription_arn)
                        });
                        let attributes = self
                            .backend()
                            .sns
                            .get_subscription_attributes(&subscription_arn)
                            .await
                            .unwrap_or_else(|_| spec.attributes.clone());
                        (subscription_arn, attributes)
                    }
                    Err(err) => return Ok(report.failed(err)),
                }
            }
            None => {
                report.status = ProvisionStatus::Missing;
                return Ok(report);
            }
        };

        if report.status != ProvisionStatus::Created && (spec.policy.verify || spec.policy.update) {
            let mut drift = compare(&spec.attributes, &attributes);
            // SNS validates the filter policy against the current scope: a nested policy needs the
            // message body scope first, and the scope goes back to attributes after the policy.
            let body_scope = spec.attributes.get("FilterPolicyScope").map(String::as_str) == Some("MessageBody");
            drift.sort_by_key(|item| match item.attribute.as_str() {
                "FilterPolicyScope" if body_scope => 0,
                "FilterPolicyScope" => 2,
                _ => 1,
            });
            for item in drift {
                if spec.policy.update {
                    if let Err(err) = self
                        .backend()
                        .sns
                        .set_subscription_attribute(&subscription_arn, &item.attribute, &item.expected)
                        .await
                    {
                        return Ok(report.failed(err));
                    }
                    report.actions.push(format!("updated attribute {}", item.attribute));
                    attributes.insert(item.attribute.clone(), item.expected.clone());
                    report.status = ProvisionStatus::Updated;
                } else {
                    report.drift.push(item);
                }
            }
            if report.status == ProvisionStatus::Updated {
                self.emit_resource(EventKind::ResourceUpdated, ResourceKind::Subscription, &name, |e| {
                    e.field("arn", &subscription_arn)
                });
            }
        }

        if spec.manage_queue_policy {
            let may_write = spec.policy.create || spec.policy.update;
            let targets = std::iter::once((spec.queue_url.clone(), spec.queue_arn.clone()))
                .chain(spec.dead_letter_queue_url.clone().zip(spec.dead_letter_queue_arn.clone()));
            for (queue_url, queue_arn) in targets {
                if may_write {
                    if let Err(err) =
                        self.ensure_topic_permission(&queue_url, &queue_arn, &spec.topic_arn, &mut report).await
                    {
                        return Ok(report.failed(err));
                    }
                } else {
                    match self.queue_attributes(&queue_url).await {
                        Ok(queue_attributes) => {
                            let allowed = queue_attributes
                                .get("Policy")
                                .and_then(|policy| serde_json::from_str::<serde_json::Value>(policy).ok())
                                .is_some_and(|policy| policy_doc::allows_topic(&policy, &queue_arn, &spec.topic_arn));
                            if !allowed {
                                report.drift.push(Drift {
                                    attribute: format!("QueuePolicy({queue_arn})"),
                                    expected: format!("allow sqs:SendMessage from {}", spec.topic_arn),
                                    actual: queue_attributes.get("Policy").cloned(),
                                    mutable: true,
                                });
                            }
                        }
                        Err(err) => report.error = Some(err),
                    }
                }
            }
        }

        report.resource = Some(SubscriptionInfo {
            arn: subscription_arn,
            topic_arn: spec.topic_arn.clone(),
            queue_arn: spec.queue_arn.clone(),
            attributes,
        });
        Ok(report)
    }

    async fn ensure_topic_permission(
        &self,
        queue_url: &str,
        queue_arn: &str,
        topic_arn: &str,
        report: &mut Report<SubscriptionInfo>,
    ) -> Result<()> {
        let attributes = self.queue_attributes(queue_url).await?;
        if let Some(policy) =
            policy_doc::with_topic_permission(attributes.get("Policy").map(String::as_str), queue_arn, topic_arn)?
        {
            let update = StringMap::from([("Policy".to_string(), policy)]);
            self.backend().sqs.set_queue_attributes(queue_url, &update).await?;
            report.actions.push(format!("allowed {topic_arn} to send to {queue_arn}"));
            self.emit_resource(EventKind::ResourcePolicyUpdated, ResourceKind::Queue, queue_arn, |e| {
                e.field("topic_arn", topic_arn)
            });
        }
        Ok(())
    }
}
