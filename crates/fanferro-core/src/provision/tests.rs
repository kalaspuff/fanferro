use super::*;
use crate::backend::memory::MemoryBroker;
use crate::client::ClientConfig;

fn client() -> Client {
    Client::with_backend(MemoryBroker::new("us-east-1", "000000000000").backend(), ClientConfig::default()).unwrap()
}

fn create() -> ProvisionPolicy {
    ProvisionPolicy { create: true, ..ProvisionPolicy::default() }
}

fn ensure() -> ProvisionPolicy {
    ProvisionPolicy { create: true, update: true, ..ProvisionPolicy::default() }
}

fn attrs(pairs: &[(&str, &str)]) -> StringMap {
    pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
}

#[tokio::test]
async fn queue_lookup_create_and_drift() {
    let client = client();
    let mut spec = QueueSpec {
        name: Some("orders".into()),
        attributes: attrs(&[("VisibilityTimeout", "60")]),
        ..QueueSpec::default()
    };
    let report = client.ensure_queue(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Missing);
    assert!(report.resource.is_none());

    spec.policy = create();
    let report = client.ensure_queue(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Created);
    let queue = report.resource.unwrap();
    assert_eq!(queue.arn, "arn:aws:sqs:us-east-1:000000000000:orders");
    assert_eq!(queue.visibility_timeout(), Some(60));

    let report = client.ensure_queue(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Found);
    assert!(report.drift.is_empty());

    spec.attributes = attrs(&[("VisibilityTimeout", "90")]);
    let report = client.ensure_queue(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Found);
    assert_eq!(report.drift.len(), 1);
    assert_eq!(report.drift[0].actual.as_deref(), Some("60"));

    spec.policy = ensure();
    let report = client.ensure_queue(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Updated);
    assert!(report.drift.is_empty());
    assert_eq!(report.resource.unwrap().visibility_timeout(), Some(90));
}

#[tokio::test]
async fn queue_with_dead_letter_queue() {
    let client = client();
    let spec = QueueSpec {
        name: Some("work".into()),
        dead_letter: Some(Box::new(DeadLetterSpec {
            queue: QueueSpec { name: Some("work-dlq".into()), policy: create(), ..QueueSpec::default() },
            max_receive_count: 3,
        })),
        policy: create(),
        ..QueueSpec::default()
    };
    let report = client.ensure_queue(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Created);
    assert_eq!(report.dead_letter.as_ref().unwrap().status, ProvisionStatus::Created);
    let queue = report.resource.unwrap();
    let redrive: serde_json::Value = serde_json::from_str(&queue.attributes["RedrivePolicy"]).unwrap();
    assert_eq!(redrive["maxReceiveCount"], 3);
    assert_eq!(redrive["deadLetterTargetArn"], "arn:aws:sqs:us-east-1:000000000000:work-dlq");
    // Second run: no drift (maxReceiveCount number vs string normalized).
    let report = client.ensure_queue(&spec).await.unwrap();
    assert!(report.drift.is_empty(), "{:?}", report.drift);
}

#[tokio::test]
async fn missing_dead_letter_queue_is_reported_as_drift() {
    let client = client();
    let spec = QueueSpec {
        name: Some("work".into()),
        dead_letter: Some(Box::new(DeadLetterSpec {
            queue: QueueSpec { name: Some("absent-dlq".into()), ..QueueSpec::default() },
            max_receive_count: 3,
        })),
        policy: create(),
        ..QueueSpec::default()
    };
    let report = client.ensure_queue(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Created);
    assert_eq!(report.dead_letter.as_ref().unwrap().status, ProvisionStatus::Missing);
    assert_eq!(report.drift[0].attribute, "RedrivePolicy");
}

#[tokio::test]
async fn fifo_queues() {
    let client = client();
    let spec = QueueSpec { name: Some("events.fifo".into()), policy: create(), ..QueueSpec::default() };
    let queue = client.ensure_queue(&spec).await.unwrap().resource.unwrap();
    assert!(queue.fifo);
    assert_eq!(queue.attributes["FifoQueue"], "true");
    let bad = QueueSpec {
        name: Some("events".into()),
        attributes: attrs(&[("FifoQueue", "true")]),
        policy: create(),
        ..QueueSpec::default()
    };
    assert!(client.ensure_queue(&bad).await.is_err());
    let mismatched = QueueSpec {
        name: Some("events.fifo".into()),
        dead_letter: Some(Box::new(DeadLetterSpec {
            queue: QueueSpec { name: Some("standard-dlq".into()), policy: create(), ..QueueSpec::default() },
            max_receive_count: 2,
        })),
        policy: create(),
        ..QueueSpec::default()
    };
    assert!(client.ensure_queue(&mismatched).await.is_err());
}

#[tokio::test]
async fn assumed_queues_make_no_calls() {
    let client = client();
    let spec = QueueSpec {
        name: Some("ghost".into()),
        policy: ProvisionPolicy { lookup: false, ..ProvisionPolicy::default() },
        ..QueueSpec::default()
    };
    let report = client.ensure_queue(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Assumed);
    let queue = report.resource.unwrap();
    assert_eq!(queue.url, "memory://us-east-1/000000000000/ghost");
    assert_eq!(queue.arn, "arn:aws:sqs:us-east-1:000000000000:ghost");
}

#[tokio::test]
async fn assumed_subscriptions_make_no_calls() {
    // Neither the topic nor the queue exists: any call would fail.
    let client = client();
    let spec = SubscriptionSpec {
        topic_arn: "arn:aws:sns:us-east-1:000000000000:ghost".into(),
        queue_arn: "arn:aws:sqs:us-east-1:000000000000:ghost".into(),
        queue_url: "memory://us-east-1/000000000000/ghost".into(),
        attributes: attrs(&[("RawMessageDelivery", "true")]),
        manage_queue_policy: true,
        dead_letter_queue_url: None,
        dead_letter_queue_arn: None,
        policy: ProvisionPolicy { lookup: false, ..ProvisionPolicy::default() },
    };
    let report = client.ensure_subscription(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Assumed);
    assert!(report.error.is_none() && report.actions.is_empty() && report.drift.is_empty());
    let subscription = report.resource.unwrap();
    assert_eq!(subscription.topic_arn, spec.topic_arn);
    assert_eq!(subscription.attributes, spec.attributes);
}

#[tokio::test]
async fn fifo_queues_cannot_be_subscribed_to_standard_topics() {
    // SNS refuses the subscription; it failed only when provisioning reached AWS.
    let client = client();
    let spec = |topic: &str, queue: &str| SubscriptionSpec {
        topic_arn: format!("arn:aws:sns:us-east-1:000000000000:{topic}"),
        queue_arn: format!("arn:aws:sqs:us-east-1:000000000000:{queue}"),
        queue_url: format!("memory://us-east-1/000000000000/{queue}"),
        attributes: StringMap::new(),
        manage_queue_policy: true,
        dead_letter_queue_url: None,
        dead_letter_queue_arn: None,
        policy: ProvisionPolicy { lookup: false, ..ProvisionPolicy::default() },
    };
    let err = client.ensure_subscription(&spec("events", "worker.fifo")).await.unwrap_err();
    assert!(matches!(err, Error::Validation(_)), "{err:?}");
    assert!(err.to_string().contains("a FIFO queue cannot be subscribed to a standard topic"), "{err}");
    // FIFO topics deliver to FIFO and standard queues.
    for queue in ["worker.fifo", "worker"] {
        let report = client.ensure_subscription(&spec("events.fifo", queue)).await.unwrap();
        assert_eq!(report.status, ProvisionStatus::Assumed);
    }
}

#[tokio::test]
async fn assumed_queue_urls_use_the_partition_of_the_region() {
    for (region, url) in [
        ("eu-west-1", "https://sqs.eu-west-1.amazonaws.com/000000000000/ghost"),
        ("cn-north-1", "https://sqs.cn-north-1.amazonaws.com.cn/000000000000/ghost"),
        ("eusc-de-east-1", "https://sqs.eusc-de-east-1.amazonaws.eu/000000000000/ghost"),
    ] {
        let mut backend = MemoryBroker::new(region, "000000000000").backend();
        backend.kind = crate::backend::BackendKind::Aws;
        let client = Client::with_backend(backend, ClientConfig::default()).unwrap();
        let spec = QueueSpec {
            name: Some("ghost".into()),
            policy: ProvisionPolicy { lookup: false, ..ProvisionPolicy::default() },
            ..QueueSpec::default()
        };
        let queue = client.ensure_queue(&spec).await.unwrap().resource.unwrap();
        assert_eq!(queue.url, url);
        assert_eq!(queue.arn, crate::types::arn::queue_arn(region, "000000000000", "ghost"));
    }
}

#[tokio::test]
async fn queue_tags() {
    let client = client();
    let mut spec = QueueSpec {
        name: Some("tagged".into()),
        tags: attrs(&[("team", "payments")]),
        policy: create(),
        ..QueueSpec::default()
    };
    client.ensure_queue(&spec).await.unwrap();
    spec.tags = attrs(&[("team", "billing")]);
    let report = client.ensure_queue(&spec).await.unwrap();
    assert_eq!(report.drift[0].attribute, "tag:team");
    spec.policy = ensure();
    let report = client.ensure_queue(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Updated);
    assert!(report.drift.is_empty());
}

#[tokio::test]
async fn topics_lookup_create_update() {
    let client = client();
    let mut spec = TopicSpec {
        name: Some("events".into()),
        attributes: attrs(&[("DisplayName", "Events")]),
        ..TopicSpec::default()
    };
    assert_eq!(client.ensure_topic(&spec).await.unwrap().status, ProvisionStatus::Missing);
    spec.policy = create();
    let report = client.ensure_topic(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Created);
    assert_eq!(report.resource.as_ref().unwrap().arn, "arn:aws:sns:us-east-1:000000000000:events");
    spec.attributes = attrs(&[("DisplayName", "Renamed")]);
    let report = client.ensure_topic(&spec).await.unwrap();
    assert_eq!(report.drift.len(), 1);
    spec.policy = ensure();
    let report = client.ensure_topic(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Updated);
    assert_eq!(report.resource.unwrap().attributes["DisplayName"], "Renamed");

    let fifo = TopicSpec { name: Some("events.fifo".into()), policy: create(), ..TopicSpec::default() };
    assert!(client.ensure_topic(&fifo).await.unwrap().resource.unwrap().fifo);
    let by_arn = TopicSpec { arn: Some("arn:aws:sns:us-east-1:000000000000:events".into()), ..TopicSpec::default() };
    assert_eq!(client.ensure_topic(&by_arn).await.unwrap().status, ProvisionStatus::Found);
}

#[tokio::test]
async fn topics_of_other_accounts_and_regions_are_not_created_here() {
    let client = client();
    for arn in
        ["arn:aws:sns:us-east-1:222222222222:partner-orders", "arn:aws:sns:eu-west-1:000000000000:partner-orders"]
    {
        let spec = TopicSpec { arn: Some(arn.into()), policy: ensure(), ..TopicSpec::default() };
        let report = client.ensure_topic(&spec).await.unwrap();
        assert_eq!(report.status, ProvisionStatus::Failed, "{arn}");
        let error = report.error.unwrap().to_string();
        assert!(error.contains("cannot be created"), "{error}");
        assert!(report.resource.is_none());
    }
    let own = TopicSpec {
        arn: Some("arn:aws:sns:us-east-1:000000000000:partner-orders".into()),
        policy: ensure(),
        ..TopicSpec::default()
    };
    let report = client.ensure_topic(&own).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Created, "the client's own account and region");
    assert_eq!(report.resource.unwrap().arn, "arn:aws:sns:us-east-1:000000000000:partner-orders");
}

#[tokio::test]
async fn filter_policies_move_between_scopes() {
    let client = client();
    let topic = client
        .ensure_topic(&TopicSpec { name: Some("events".into()), policy: create(), ..TopicSpec::default() })
        .await
        .unwrap()
        .resource
        .unwrap();
    let queue = client
        .ensure_queue(&QueueSpec { name: Some("listener".into()), policy: create(), ..QueueSpec::default() })
        .await
        .unwrap()
        .resource
        .unwrap();
    let flat = (r#"{"type":["a"]}"#, "MessageAttributes");
    let nested = (r#"{"detail":{"type":["a"]}}"#, "MessageBody");
    let mut spec = SubscriptionSpec {
        topic_arn: topic.arn.clone(),
        queue_arn: queue.arn.clone(),
        queue_url: queue.url.clone(),
        attributes: StringMap::new(),
        manage_queue_policy: false,
        dead_letter_queue_url: None,
        dead_letter_queue_arn: None,
        policy: ensure(),
    };
    // A nested policy needs the message body scope: the scope changes first, and back last.
    for (index, (filter_policy, scope)) in [flat, nested, flat].into_iter().enumerate() {
        spec.attributes = attrs(&[("FilterPolicy", filter_policy), ("FilterPolicyScope", scope)]);
        let report = client.ensure_subscription(&spec).await.unwrap();
        assert!(report.error.is_none(), "step {index}: {:?}", report.error);
        let attributes = report.resource.unwrap().attributes;
        assert_eq!(
            (attributes["FilterPolicy"].as_str(), attributes["FilterPolicyScope"].as_str()),
            (filter_policy, scope)
        );
    }
}

#[tokio::test]
async fn subscriptions_and_queue_policy() {
    let client = client();
    let topic = client
        .ensure_topic(&TopicSpec { name: Some("events".into()), policy: create(), ..TopicSpec::default() })
        .await
        .unwrap()
        .resource
        .unwrap();
    let queue = client
        .ensure_queue(&QueueSpec { name: Some("listener".into()), policy: create(), ..QueueSpec::default() })
        .await
        .unwrap()
        .resource
        .unwrap();
    let mut spec = SubscriptionSpec {
        topic_arn: topic.arn.clone(),
        queue_arn: queue.arn.clone(),
        queue_url: queue.url.clone(),
        attributes: attrs(&[("RawMessageDelivery", "true")]),
        manage_queue_policy: true,
        dead_letter_queue_url: None,
        dead_letter_queue_arn: None,
        policy: ProvisionPolicy::default(),
    };
    let report = client.ensure_subscription(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Missing);

    spec.policy = create();
    let report = client.ensure_subscription(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Created);
    assert!(report.actions.iter().any(|a| a.starts_with("allowed")));
    let policy = client.queue_attributes(&queue.url).await.unwrap()["Policy"].clone();
    let policy: serde_json::Value = serde_json::from_str(&policy).unwrap();
    assert!(policy_doc::allows_topic(&policy, &queue.arn, &topic.arn));

    let report = client.ensure_subscription(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Found);
    assert!(report.drift.is_empty());

    spec.attributes = attrs(&[("RawMessageDelivery", "false"), ("FilterPolicy", r#"{"kind":["a"]}"#)]);
    spec.policy = ProvisionPolicy::default();
    let report = client.ensure_subscription(&spec).await.unwrap();
    assert_eq!(report.drift.len(), 2);
    spec.policy = ensure();
    let report = client.ensure_subscription(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Updated);
    assert_eq!(report.resource.unwrap().attributes["RawMessageDelivery"], "false");
}

#[tokio::test]
async fn queue_policy_updates_keep_subscription_permissions() {
    let client = client();
    let topic = client
        .ensure_topic(&TopicSpec { name: Some("events".into()), policy: ensure(), ..TopicSpec::default() })
        .await
        .unwrap()
        .resource
        .unwrap();
    let own = r#"{"Version":"2012-10-17","Statement":[{"Sid":"auditor","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::111111111111:root"},"Action":"sqs:GetQueueAttributes","Resource":"*"}]}"#;
    let queue_spec = QueueSpec {
        name: Some("listener".into()),
        attributes: attrs(&[("Policy", own)]),
        policy: ensure(),
        ..QueueSpec::default()
    };
    let queue = client.ensure_queue(&queue_spec).await.unwrap().resource.unwrap();
    let subscription = SubscriptionSpec {
        topic_arn: topic.arn.clone(),
        queue_arn: queue.arn.clone(),
        queue_url: queue.url.clone(),
        attributes: attrs(&[("RawMessageDelivery", "true")]),
        manage_queue_policy: true,
        dead_letter_queue_url: None,
        dead_letter_queue_arn: None,
        policy: ensure(),
    };
    client.ensure_subscription(&subscription).await.unwrap();

    // Provisioning again (the next deployment): the queue comes first and must keep the permission,
    // or SNS drops what is published until the subscription restores it.
    let report = client.ensure_queue(&queue_spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Found, "{:?}", report.actions);
    assert!(report.drift.is_empty());
    let policy: serde_json::Value =
        serde_json::from_str(&client.queue_attributes(&queue.url).await.unwrap()["Policy"]).unwrap();
    assert!(policy_doc::allows_topic(&policy, &queue.arn, &topic.arn));
    let read_only = QueueSpec { policy: ProvisionPolicy::default(), ..queue_spec.clone() };
    assert!(client.ensure_queue(&read_only).await.unwrap().drift.is_empty());

    // Changing the application's own statements updates them, still keeping the permission.
    let changed = own.replace("sqs:GetQueueAttributes", "sqs:*");
    let report = client
        .ensure_queue(&QueueSpec { attributes: attrs(&[("Policy", &changed)]), ..queue_spec.clone() })
        .await
        .unwrap();
    assert_eq!(report.status, ProvisionStatus::Updated);
    let policy: serde_json::Value =
        serde_json::from_str(&client.queue_attributes(&queue.url).await.unwrap()["Policy"]).unwrap();
    assert!(policy_doc::allows_topic(&policy, &queue.arn, &topic.arn));
    assert!(policy.to_string().contains("sqs:*"));
}

#[tokio::test]
async fn read_only_subscription_reports_missing_policy() {
    let client = client();
    let topic = client
        .ensure_topic(&TopicSpec { name: Some("t".into()), policy: create(), ..TopicSpec::default() })
        .await
        .unwrap()
        .resource
        .unwrap();
    let queue = client
        .ensure_queue(&QueueSpec { name: Some("q".into()), policy: create(), ..QueueSpec::default() })
        .await
        .unwrap()
        .resource
        .unwrap();
    client.backend().sns.subscribe(&topic.arn, "sqs", &queue.arn, &StringMap::new()).await.unwrap();
    let spec = SubscriptionSpec {
        topic_arn: topic.arn,
        queue_arn: queue.arn,
        queue_url: queue.url,
        attributes: StringMap::new(),
        manage_queue_policy: true,
        dead_letter_queue_url: None,
        dead_letter_queue_arn: None,
        policy: ProvisionPolicy::default(),
    };
    let report = client.ensure_subscription(&spec).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Found);
    assert!(report.drift[0].attribute.starts_with("QueuePolicy"));
}

#[tokio::test]
async fn buckets() {
    let client = client();
    assert_eq!(client.ensure_bucket("b", false).await.unwrap().status, ProvisionStatus::Missing);
    assert_eq!(client.ensure_bucket("b", true).await.unwrap().status, ProvisionStatus::Created);
    assert_eq!(client.ensure_bucket("b", false).await.unwrap().status, ProvisionStatus::Found);
}

#[tokio::test]
async fn subscribing_a_queue_encrypted_with_the_aws_managed_key_warns() {
    // docs/parity S8: SNS cannot use the AWS managed key of SQS, and the messages are lost silently.
    let client = client();
    client.events().set_enabled(&[EventKind::ConfigWarning]);
    let topic = client.backend().sns.create_topic("t", &StringMap::new(), &StringMap::new()).await.unwrap();
    for (queue, key) in [("plain", None), ("managed", Some("alias/aws/sqs")), ("custom", Some("alias/payments"))] {
        let attributes = key.map(|key| attrs(&[("KmsMasterKeyId", key)])).unwrap_or_default();
        let url = client.backend().sqs.create_queue(queue, &attributes, &StringMap::new()).await.unwrap();
        for policy in [ensure(), ProvisionPolicy::default()] {
            let spec = SubscriptionSpec {
                topic_arn: topic.clone(),
                queue_arn: format!("arn:aws:sqs:us-east-1:000000000000:{queue}"),
                queue_url: url.clone(),
                attributes: StringMap::new(),
                manage_queue_policy: policy.create,
                dead_letter_queue_url: None,
                dead_letter_queue_arn: None,
                policy,
            };
            client.ensure_subscription(&spec).await.unwrap();
            let warnings = client.events().drain(10);
            if key == Some("alias/aws/sqs") {
                assert_eq!(warnings.len(), 1, "{queue}");
                assert!(
                    warnings[0].message.contains("SNS cannot deliver to arn:aws:sqs:us-east-1:000000000000:managed")
                );
            } else {
                assert!(warnings.is_empty(), "{queue}: {warnings:?}");
            }
        }
    }
    assert!(is_aws_managed_sqs_key("arn:aws:kms:eu-north-1:123456789012:alias/aws/sqs"));
    assert!(!is_aws_managed_sqs_key("alias/aws/sqs-other"));
}
