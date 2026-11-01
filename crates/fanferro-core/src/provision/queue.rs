//! Queue provisioning.

use futures::future::BoxFuture;

use super::drift::{Drift, compare, compare_tags};
use super::policy_doc;
use super::{ProvisionStatus, QueueSpec, Report};
use crate::backend::StringMap;
use crate::client::Client;
use crate::error::{Error, ResourceKind, Result};
use crate::events::EventKind;
use crate::types::arn;

/// A resolved queue.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueInfo {
    /// Queue name.
    pub name: String,
    /// Queue URL.
    pub url: String,
    /// Queue ARN.
    pub arn: String,
    /// FIFO queue.
    pub fifo: bool,
    /// Attributes as read from SQS (or the desired attributes when assumed).
    pub attributes: StringMap,
}

impl QueueInfo {
    /// Visibility timeout in seconds, when known.
    pub fn visibility_timeout(&self) -> Option<u64> {
        self.attributes.get("VisibilityTimeout").and_then(|v| v.parse().ok())
    }

    /// Whether content-based deduplication is enabled.
    pub fn content_based_deduplication(&self) -> bool {
        self.attributes.get("ContentBasedDeduplication").is_some_and(|v| v.eq_ignore_ascii_case("true"))
    }

    /// Maximum message size, when known.
    pub fn max_message_size(&self) -> Option<usize> {
        self.attributes.get("MaximumMessageSize").and_then(|v| v.parse().ok())
    }
}

fn queue_url_for(client: &Client, account: &str, name: &str) -> String {
    match &client.backend().endpoint_url {
        Some(endpoint) => format!("{}/{account}/{name}", endpoint.trim_end_matches('/')),
        None if client.backend().kind == crate::backend::BackendKind::Memory => {
            format!("memory://{}/{account}/{name}", client.region())
        }
        None => {
            let region = client.region();
            format!("https://sqs.{region}.{}/{account}/{name}", arn::dns_suffix(arn::partition_for_region(region)))
        }
    }
}

impl Client {
    /// Provisions a queue according to its spec and policy.
    pub fn ensure_queue<'a>(&'a self, spec: &'a QueueSpec) -> BoxFuture<'a, Result<Report<QueueInfo>>> {
        Box::pin(async move { self.with_provisioning_retries(|| self.ensure_queue_inner(spec)).await })
    }

    async fn ensure_queue_inner(&self, spec: &QueueSpec) -> Result<Report<QueueInfo>> {
        let name = match (&spec.name, &spec.url) {
            (Some(name), _) => name.clone(),
            (None, Some(url)) => arn::queue_name_from_url(url)?,
            (None, None) => return Err(Error::validation("a queue spec needs a name or a url")),
        };
        let fifo = name.ends_with(".fifo");
        let mut report: Report<QueueInfo> = Report::new(ResourceKind::Queue, name.clone());
        let mut desired = spec.attributes.clone();
        if fifo {
            desired.insert("FifoQueue".into(), "true".into());
        } else if desired.get("FifoQueue").is_some_and(|v| v.eq_ignore_ascii_case("true")) {
            return Err(Error::validation(format!("FIFO queue names must end with .fifo (got {name:?})")));
        }

        if let Some(dead_letter) = &spec.dead_letter {
            let dlq_report = self.ensure_queue(&dead_letter.queue).await?;
            match &dlq_report.resource {
                Some(dlq) => {
                    if dlq.fifo != fifo {
                        return Err(Error::validation(format!(
                            "the dead-letter queue of {name:?} must be {} like the queue itself",
                            if fifo { "FIFO" } else { "a standard queue" }
                        )));
                    }
                    desired.insert(
                        "RedrivePolicy".into(),
                        serde_json::json!({"deadLetterTargetArn": dlq.arn, "maxReceiveCount": dead_letter.max_receive_count})
                            .to_string(),
                    );
                }
                None => report.drift.push(Drift {
                    attribute: "RedrivePolicy".into(),
                    expected: "<dead-letter queue unavailable>".into(),
                    actual: None,
                    mutable: true,
                }),
            }
            report.dead_letter = Some(Box::new(dlq_report));
        }

        if !spec.policy.lookup {
            let account = match &spec.owner_account_id {
                Some(account) => account.clone(),
                None => match &spec.url {
                    Some(url) => arn::parse_queue_url(url)?.0,
                    None => match self.account_id().await {
                        Ok(account) => account,
                        Err(err) => return Ok(report.failed(err)),
                    },
                },
            };
            let url = spec.url.clone().unwrap_or_else(|| queue_url_for(self, &account, &name));
            report.status = ProvisionStatus::Assumed;
            report.resource = Some(QueueInfo {
                arn: arn::queue_arn(self.region(), &account, &name),
                name,
                url,
                fifo,
                attributes: desired,
            });
            return Ok(report);
        }

        let lookup = match &spec.url {
            Some(url) => Ok(url.clone()),
            None => self.backend().sqs.get_queue_url(&name, spec.owner_account_id.as_deref()).await,
        };
        let url = match lookup {
            Ok(url) => Some(url),
            Err(err) if err.is_not_found() => None,
            Err(err) => return Ok(report.failed(err)),
        };

        let url = match url {
            Some(url) => url,
            None if spec.policy.create => {
                match self.backend().sqs.create_queue(&name, &desired, &spec.tags).await {
                    Ok(url) => {
                        report.status = ProvisionStatus::Created;
                        report.actions.push("created queue".into());
                        self.emit_resource(EventKind::ResourceCreated, ResourceKind::Queue, &name, |e| {
                            e.field("url", &url)
                        });
                        url
                    }
                    Err(err)
                        if err
                            .aws()
                            .and_then(|a| a.code.as_deref())
                            .is_some_and(|c| c.ends_with("QueueAlreadyExists") || c.ends_with("QueueNameExists")) =>
                    {
                        // Created concurrently or exists with different attributes: verify below.
                        match self.backend().sqs.get_queue_url(&name, spec.owner_account_id.as_deref()).await {
                            Ok(url) => url,
                            Err(err) => return Ok(report.failed(err)),
                        }
                    }
                    Err(err) => return Ok(report.failed(err)),
                }
            }
            None => {
                report.status = ProvisionStatus::Missing;
                return Ok(report);
            }
        };

        let attributes = match self.backend().sqs.get_queue_attributes(&url).await {
            Ok(attributes) => attributes,
            Err(err) if err.is_not_found() && spec.url.is_some() && !spec.policy.create => {
                report.status = ProvisionStatus::Missing;
                return Ok(report);
            }
            Err(err) if !spec.policy.verify && report.status != ProvisionStatus::Created => {
                // Without verification we can still use the queue; derive what we know.
                let (account, _) = arn::parse_queue_url(&url)?;
                report.status = ProvisionStatus::Found;
                report.resource = Some(QueueInfo {
                    arn: arn::queue_arn(self.region(), &account, &name),
                    name,
                    url,
                    fifo,
                    attributes: desired,
                });
                report.error = Some(err);
                return Ok(report);
            }
            Err(err) => return Ok(report.failed(err)),
        };
        if report.status != ProvisionStatus::Created {
            report.status = ProvisionStatus::Found;
            self.emit_resource(EventKind::ResourceFound, ResourceKind::Queue, &name, |e| e.field("url", &url));
        }
        let mut current = attributes;

        if spec.policy.verify || spec.policy.update {
            // The permissions subscriptions added to the queue policy are part of the desired state.
            if let (Some(wanted), Some(actual)) = (desired.get("Policy"), current.get("Policy"))
                && let Some(merged) = policy_doc::with_managed_statements(wanted, actual)
            {
                desired.insert("Policy".into(), merged);
            }
            let mut drift = compare(&desired, &current);
            if spec.policy.update && report.status != ProvisionStatus::Created {
                let updates: StringMap =
                    drift.iter().filter(|d| d.mutable).map(|d| (d.attribute.clone(), d.expected.clone())).collect();
                if !updates.is_empty() {
                    if let Err(err) = self.backend().sqs.set_queue_attributes(&url, &updates).await {
                        report.drift.extend(drift);
                        return Ok(report.failed(err));
                    }
                    report.actions.push(format!(
                        "updated attributes: {}",
                        updates.keys().cloned().collect::<Vec<_>>().join(", ")
                    ));
                    report.status = ProvisionStatus::Updated;
                    self.emit_resource(EventKind::ResourceUpdated, ResourceKind::Queue, &name, |e| {
                        e.field("attributes", updates.keys().cloned().collect::<Vec<_>>().join(","))
                    });
                    current.extend(updates);
                    drift.retain(|d| !d.mutable);
                }
            }
            report.drift.extend(drift);
            if !spec.tags.is_empty() {
                match self.backend().sqs.list_queue_tags(&url).await {
                    Ok(tags) => {
                        let tag_drift = compare_tags(&spec.tags, &tags);
                        if !tag_drift.is_empty() && spec.policy.update {
                            if let Err(err) = self.backend().sqs.tag_queue(&url, &spec.tags).await {
                                return Ok(report.failed(err));
                            }
                            report.actions.push("updated tags".into());
                            if report.status == ProvisionStatus::Found {
                                report.status = ProvisionStatus::Updated;
                            }
                        } else {
                            report.drift.extend(tag_drift);
                        }
                    }
                    Err(err) => report.error = Some(err),
                }
            }
        }

        let queue_arn = match current.get("QueueArn") {
            Some(queue_arn) => queue_arn.clone(),
            None => arn::queue_arn(self.region(), &arn::parse_queue_url(&url)?.0, &name),
        };
        report.resource = Some(QueueInfo { name, url, arn: queue_arn, fifo, attributes: current });
        Ok(report)
    }
}
