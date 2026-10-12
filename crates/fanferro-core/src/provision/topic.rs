//! Topic provisioning.

use super::drift::{compare, compare_tags};
use super::{ProvisionStatus, Report, TopicSpec};
use crate::backend::StringMap;
use crate::client::Client;
use crate::error::{Error, ResourceKind, Result};
use crate::events::EventKind;
use crate::types::arn::{self, Arn};

/// A resolved topic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicInfo {
    /// Topic name.
    pub name: String,
    /// Topic ARN.
    pub arn: String,
    /// FIFO topic.
    pub fifo: bool,
    /// Attributes as read from SNS (or the desired attributes when assumed).
    pub attributes: StringMap,
}

impl TopicInfo {
    /// Whether content-based deduplication is enabled.
    pub fn content_based_deduplication(&self) -> bool {
        self.attributes.get("ContentBasedDeduplication").is_some_and(|v| v.eq_ignore_ascii_case("true"))
    }

    /// Maximum message size, when configured on the topic.
    pub fn max_message_size(&self) -> Option<usize> {
        self.attributes.get("MaximumMessageSize").and_then(|v| v.parse().ok())
    }
}

impl Client {
    /// Provisions a topic according to its spec and policy.
    pub async fn ensure_topic(&self, spec: &TopicSpec) -> Result<Report<TopicInfo>> {
        let name = match (&spec.name, &spec.arn) {
            (Some(name), _) => name.clone(),
            (None, Some(topic_arn)) => Arn::parse(topic_arn)?.resource_name().to_string(),
            (None, None) => return Err(Error::validation("a topic spec needs a name or an arn")),
        };
        let fifo = name.ends_with(".fifo");
        let mut report: Report<TopicInfo> = Report::new(ResourceKind::Topic, name.clone());
        let mut desired = spec.attributes.clone();
        if fifo {
            desired.insert("FifoTopic".into(), "true".into());
        } else if desired.get("FifoTopic").is_some_and(|v| v.eq_ignore_ascii_case("true")) {
            return Err(Error::validation(format!("FIFO topic names must end with .fifo (got {name:?})")));
        }
        let topic_arn = match &spec.arn {
            Some(topic_arn) => topic_arn.clone(),
            None => match self.account_id().await {
                Ok(account) => arn::topic_arn(self.region(), &account, &name),
                Err(err) => return Ok(report.failed(err)),
            },
        };

        if !spec.policy.lookup {
            report.status = ProvisionStatus::Assumed;
            report.resource = Some(TopicInfo { name, arn: topic_arn, fifo, attributes: desired });
            return Ok(report);
        }

        let mut current = match self.backend().sns.get_topic_attributes(&topic_arn).await {
            Ok(attributes) => {
                report.status = ProvisionStatus::Found;
                self.emit_resource(EventKind::ResourceFound, ResourceKind::Topic, &name, |e| {
                    e.field("arn", &topic_arn)
                });
                Some(attributes)
            }
            Err(err) if err.is_not_found() => None,
            Err(err) => return Ok(report.failed(err)),
        };

        let topic_arn = if current.is_none() {
            if !spec.policy.create {
                report.status = ProvisionStatus::Missing;
                return Ok(report);
            }
            let created_arn = match self.backend().sns.create_topic(&name, &desired, &spec.tags).await {
                Ok(created_arn) => created_arn,
                Err(err) => return Ok(report.failed(err)),
            };
            report.status = ProvisionStatus::Created;
            report.actions.push("created topic".into());
            self.emit_resource(EventKind::ResourceCreated, ResourceKind::Topic, &name, |e| {
                e.field("arn", &created_arn)
            });
            current = match self.backend().sns.get_topic_attributes(&created_arn).await {
                Ok(attributes) => Some(attributes),
                Err(err) => return Ok(report.failed(err)),
            };
            created_arn
        } else {
            topic_arn
        };
        let mut current = current.unwrap_or_default();

        if (spec.policy.verify || spec.policy.update) && report.status != ProvisionStatus::Created {
            let drift = compare(&desired, &current);
            let mut remaining = Vec::new();
            for item in drift {
                if spec.policy.update && item.mutable {
                    if let Err(err) =
                        self.backend().sns.set_topic_attribute(&topic_arn, &item.attribute, &item.expected).await
                    {
                        report.drift.push(item);
                        return Ok(report.failed(err));
                    }
                    report.actions.push(format!("updated attribute {}", item.attribute));
                    current.insert(item.attribute.clone(), item.expected.clone());
                    report.status = ProvisionStatus::Updated;
                } else {
                    remaining.push(item);
                }
            }
            if report.status == ProvisionStatus::Updated {
                self.emit_resource(EventKind::ResourceUpdated, ResourceKind::Topic, &name, |e| {
                    e.field("arn", &topic_arn)
                });
            }
            report.drift.extend(remaining);
            if !spec.tags.is_empty() {
                match self.backend().sns.list_topic_tags(&topic_arn).await {
                    Ok(tags) => {
                        let tag_drift = compare_tags(&spec.tags, &tags);
                        if !tag_drift.is_empty() && spec.policy.update {
                            if let Err(err) = self.backend().sns.tag_topic(&topic_arn, &spec.tags).await {
                                return Ok(report.failed(err));
                            }
                            report.actions.push("updated tags".into());
                            report.status = ProvisionStatus::Updated;
                        } else {
                            report.drift.extend(tag_drift);
                        }
                    }
                    Err(err) => report.error = Some(err),
                }
            }
        }

        report.resource = Some(TopicInfo { name, arn: topic_arn, fifo, attributes: current });
        Ok(report)
    }
}
