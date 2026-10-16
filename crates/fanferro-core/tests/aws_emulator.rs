//! End-to-end tests of the AWS backend against an AWS-compatible emulator (MiniStack).
//!
//! Skipped unless `FANFERRO_TEST_ENDPOINT_URL` is set, e.g.
//! `FANFERRO_TEST_ENDPOINT_URL=http://localhost:4566 cargo test -p fanferro-core --test aws_emulator`.

use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use fanferro_core::backend::aws::{AwsConfig, StaticCredentials, TimeoutSettings};
use fanferro_core::backend::{ReceiveRequest, SendRequest, StringMap};
use fanferro_core::consumer::{ConsumerConfig, ConsumerEngine, Delivery};
use fanferro_core::provision::{
    DeadLetterSpec, ProvisionPolicy, ProvisionStatus, QueueSpec, SubscriptionSpec, TopicSpec,
};
use fanferro_core::publish::{Destination, OutgoingMessage};
use fanferro_core::transport::OffloadSettings;
use fanferro_core::transport::claim_check::{EncryptionSettings, PointerFormat, ReferenceMode};
use fanferro_core::types::{AttributeValue, MessageAttributes, WireMessage};
use fanferro_core::{Client, ClientConfig};

fn endpoint() -> Option<String> {
    std::env::var("FANFERRO_TEST_ENDPOINT_URL").ok().filter(|v| !v.is_empty())
}

fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", &uuid::Uuid::new_v4().simple().to_string()[..12])
}

fn aws_config() -> AwsConfig {
    AwsConfig {
        region: Some("us-east-1".into()),
        endpoint_url: endpoint(),
        credentials: Some(StaticCredentials {
            access_key_id: "test".into(),
            secret_access_key: "test".into(),
            session_token: None,
        }),
        ..AwsConfig::default()
    }
}

async fn client(config: ClientConfig) -> Client {
    Client::connect(&aws_config(), config).await.expect("connect")
}

fn create() -> ProvisionPolicy {
    ProvisionPolicy { create: true, update: true, ..ProvisionPolicy::default() }
}

async fn collect(engine: &ConsumerEngine, count: usize) -> Vec<Delivery> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut deliveries = Vec::new();
    while deliveries.len() < count && tokio::time::Instant::now() < deadline {
        deliveries.extend(engine.next_batch(count - deliveries.len(), Duration::from_millis(500)).await);
    }
    deliveries
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_roundtrip_with_heartbeats_and_dead_lettering() {
    if endpoint().is_none() {
        eprintln!("skipping: FANFERRO_TEST_ENDPOINT_URL is not set");
        return;
    }
    let client = client(ClientConfig::default()).await;
    let name = unique("e2e-q");
    let report = client
        .ensure_queue(&QueueSpec {
            name: Some(name.clone()),
            attributes: StringMap::from([("VisibilityTimeout".to_string(), "5".to_string())]),
            dead_letter: Some(Box::new(DeadLetterSpec {
                queue: QueueSpec { name: Some(format!("{name}-dlq")), policy: create(), ..QueueSpec::default() },
                max_receive_count: 5,
            })),
            policy: create(),
            ..QueueSpec::default()
        })
        .await
        .unwrap();
    assert_eq!(report.status, ProvisionStatus::Created, "{:?}", report.error);
    let queue = report.resource.unwrap();
    let dlq = report.dead_letter.unwrap().resource.unwrap();
    let again = client
        .ensure_queue(&QueueSpec {
            name: Some(name.clone()),
            attributes: StringMap::from([("VisibilityTimeout".to_string(), "5".to_string())]),
            policy: create(),
            ..QueueSpec::default()
        })
        .await
        .unwrap();
    assert_eq!(again.status, ProvisionStatus::Found);
    assert!(again.drift.is_empty(), "{:?}", again.drift);

    let destination = Destination::Queue {
        url: queue.url.clone(),
        fifo: false,
        content_based_deduplication: false,
        max_message_size: None,
    };
    let mut attributes = MessageAttributes::new();
    attributes.insert("tenant".into(), AttributeValue::string("acme"));
    attributes.insert("blob".into(), AttributeValue::binary(vec![0u8, 1, 2]));
    let receipt = client
        .publish(
            &destination,
            OutgoingMessage {
                wire: WireMessage::new(Bytes::from_static(&[0, 159, 146, 150]), attributes),
                message_id: "m-binary".into(),
                ..OutgoingMessage::default()
            },
        )
        .await
        .unwrap();
    assert!(!receipt.transport_message_id.is_empty());
    let batch: Vec<OutgoingMessage> = (0..12)
        .map(|i| OutgoingMessage {
            wire: WireMessage::new(Bytes::from(format!("batch {i}")), MessageAttributes::new()),
            message_id: format!("b{i}"),
            ..OutgoingMessage::default()
        })
        .collect();
    let results = client.publish_batch(&destination, batch).await.unwrap();
    assert!(results.iter().all(Result::is_ok), "{results:?}");

    let mut config = ConsumerConfig::new(queue.url.clone());
    config.queue_arn = Some(queue.arn.clone());
    config.visibility_timeout = 3.0;
    config.heartbeat.interval = Some(1.0);
    config.wait_time = 1.0;
    config.max_in_flight = 20;
    config.dead_letter_queue_url = Some(dlq.url.clone());
    let engine = ConsumerEngine::new(&client, config).unwrap();
    engine.start().unwrap();
    let deliveries = collect(&engine, 13).await;
    assert_eq!(deliveries.len(), 13);
    let binary = deliveries.iter().find(|d| d.decoded.as_ref().unwrap().wire.body.starts_with(&[0])).unwrap();
    let decoded = binary.decoded.as_ref().unwrap();
    assert_eq!(&decoded.wire.body[..], &[0, 159, 146, 150]);
    assert_eq!(decoded.wire.attributes.get("blob"), Some(&AttributeValue::binary(vec![0u8, 1, 2])));
    assert!(!decoded.wire.attributes.contains_key("fanferro.encoding"));

    // Hold everything past the visibility timeout: heartbeats must prevent redelivery.
    tokio::time::sleep(Duration::from_secs(7)).await;
    let attributes = client.queue_attributes(&queue.url).await.unwrap();
    assert_eq!(attributes["ApproximateNumberOfMessages"], "0", "{attributes:?}");
    assert!(engine.stats().metrics["lease_extensions"] > 0);

    let mut deliveries = deliveries.into_iter();
    let poison = deliveries.next().unwrap();
    poison.lease.dead_letter("test", &BTreeMap::new()).await.unwrap();
    for delivery in deliveries {
        delivery.lease.ack().await.unwrap();
    }
    engine.close(true).await;

    let dead = client
        .backend()
        .sqs
        .receive_messages(ReceiveRequest {
            queue_url: dlq.url.clone(),
            max_messages: 10,
            wait_time: Duration::from_secs(1),
            visibility_timeout: None,
            attempt_id: None,
        })
        .await
        .unwrap();
    assert_eq!(dead.len(), 1);
    assert!(dead[0].attributes.contains_key("fanferro.dead-letter"));
    let attributes = client.queue_attributes(&queue.url).await.unwrap();
    assert_eq!(attributes["ApproximateNumberOfMessages"], "0");
    assert_eq!(attributes["ApproximateNumberOfMessagesNotVisible"], "0");

    client.delete_queue(&queue.url).await.unwrap();
    client.delete_queue(&dlq.url).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn topic_fanout_with_claim_checks() {
    if endpoint().is_none() {
        eprintln!("skipping: FANFERRO_TEST_ENDPOINT_URL is not set");
        return;
    }
    let bucket = unique("payloads");
    let offload = OffloadSettings {
        bucket: bucket.clone(),
        key_prefix: "e2e/".into(),
        threshold: 1024,
        always: false,
        reference: ReferenceMode::Both,
        presign_ttl: 300.0,
        format: PointerFormat::Fanferro,
        encryption: EncryptionSettings::BucketDefault,
        storage_class: None,
    };
    let client = client(ClientConfig { offload: Some(offload), ..ClientConfig::default() }).await;
    assert_eq!(client.ensure_bucket(&bucket, true).await.unwrap().status, ProvisionStatus::Created);
    let topic = client
        .ensure_topic(&TopicSpec { name: Some(unique("e2e-topic")), policy: create(), ..TopicSpec::default() })
        .await
        .unwrap()
        .resource
        .unwrap();
    let raw_queue = client
        .ensure_queue(&QueueSpec { name: Some(unique("e2e-raw")), policy: create(), ..QueueSpec::default() })
        .await
        .unwrap()
        .resource
        .unwrap();
    let wrapped_queue = client
        .ensure_queue(&QueueSpec { name: Some(unique("e2e-wrapped")), policy: create(), ..QueueSpec::default() })
        .await
        .unwrap()
        .resource
        .unwrap();
    for (queue, raw) in [(&raw_queue, "true"), (&wrapped_queue, "false")] {
        let report = client
            .ensure_subscription(&SubscriptionSpec {
                topic_arn: topic.arn.clone(),
                queue_arn: queue.arn.clone(),
                queue_url: queue.url.clone(),
                attributes: StringMap::from([("RawMessageDelivery".to_string(), raw.to_string())]),
                manage_queue_policy: true,
                dead_letter_queue_url: None,
                dead_letter_queue_arn: None,
                policy: create(),
            })
            .await
            .unwrap();
        assert_eq!(report.status, ProvisionStatus::Created, "{:?}", report.error);
    }

    let destination = Destination::Topic {
        arn: topic.arn.clone(),
        fifo: false,
        content_based_deduplication: false,
        max_message_size: None,
    };
    let large = "z".repeat(10_000);
    let mut attributes = MessageAttributes::new();
    attributes.insert("kind".into(), AttributeValue::string("big"));
    let receipt = client
        .publish(
            &destination,
            OutgoingMessage {
                wire: WireMessage::new(Bytes::from(large.clone()), attributes),
                message_id: "large-1".into(),
                subject: Some("subject".into()),
                ..OutgoingMessage::default()
            },
        )
        .await
        .unwrap();
    let pointer = receipt.offloaded.expect("offloaded");
    assert!(pointer.url.as_deref().unwrap().contains("X-Amz-Signature"));

    for queue in [&raw_queue, &wrapped_queue] {
        let mut config = ConsumerConfig::new(queue.url.clone());
        config.wait_time = 1.0;
        let engine = ConsumerEngine::new(&client, config).unwrap();
        engine.start().unwrap();
        let delivery = collect(&engine, 1).await.remove(0);
        let decoded = delivery.decoded.as_ref().expect("decoded");
        assert_eq!(decoded.wire.body.len(), large.len());
        assert_eq!(decoded.wire.attributes.get("kind"), Some(&AttributeValue::string("big")));
        delivery.lease.ack().await.unwrap();
        engine.close(true).await;
    }

    // Presigned URL retrieval works without S3 permissions on the receiving side.
    let fetch = fanferro_core::transport::FetchSettings {
        prefer: fanferro_core::transport::claim_check::FetchPreference::Url,
        ..Default::default()
    };
    let data = fanferro_core::transport::claim_check::fetch(client.backend(), &fetch, &pointer).await.unwrap();
    assert_eq!(data.len(), large.len());

    client.delete_topic(&topic.arn).await.unwrap();
    client.delete_queue(&raw_queue.url).await.unwrap();
    client.delete_queue(&wrapped_queue.url).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn fifo_queue_and_topic() {
    if endpoint().is_none() {
        eprintln!("skipping: FANFERRO_TEST_ENDPOINT_URL is not set");
        return;
    }
    let client = client(ClientConfig::default()).await;
    let queue = client
        .ensure_queue(&QueueSpec {
            name: Some(format!("{}.fifo", unique("e2e-fifo"))),
            policy: create(),
            ..QueueSpec::default()
        })
        .await
        .unwrap()
        .resource
        .unwrap();
    assert!(queue.fifo);
    let destination = Destination::Queue {
        url: queue.url.clone(),
        fifo: true,
        content_based_deduplication: false,
        max_message_size: None,
    };
    for i in 0..3 {
        let message = OutgoingMessage {
            wire: WireMessage::new(Bytes::from(format!("ordered {i}")), MessageAttributes::new()),
            message_id: format!("f{i}"),
            group_id: Some("g1".into()),
            ..OutgoingMessage::default()
        };
        client.publish(&destination, message.clone()).await.unwrap();
        // Duplicate send (same message id → same dedup id) is dropped by SQS.
        client.publish(&destination, message).await.unwrap();
    }
    let received = client
        .backend()
        .sqs
        .receive_messages(ReceiveRequest {
            queue_url: queue.url.clone(),
            max_messages: 10,
            wait_time: Duration::from_secs(1),
            visibility_timeout: Some(Duration::from_secs(30)),
            attempt_id: Some("attempt-1".into()),
        })
        .await
        .unwrap();
    let bodies: Vec<&str> = received.iter().map(|m| m.body.as_str()).collect();
    assert_eq!(bodies, vec!["ordered 0", "ordered 1", "ordered 2"]);
    assert_eq!(received[0].system.message_group_id.as_deref(), Some("g1"));
    client.delete_queue(&queue.url).await.unwrap();

    let topic = client
        .ensure_topic(&TopicSpec {
            name: Some(format!("{}.fifo", unique("e2e-topic"))),
            attributes: StringMap::from([("ContentBasedDeduplication".to_string(), "true".to_string())]),
            policy: create(),
            ..TopicSpec::default()
        })
        .await
        .unwrap()
        .resource
        .unwrap();
    assert!(topic.fifo);
    let destination = Destination::Topic {
        arn: topic.arn.clone(),
        fifo: true,
        content_based_deduplication: true,
        max_message_size: None,
    };
    client
        .publish(
            &destination,
            OutgoingMessage {
                wire: WireMessage::new(Bytes::from_static(b"fifo event"), MessageAttributes::new()),
                message_id: "t1".into(),
                group_id: Some("g".into()),
                ..OutgoingMessage::default()
            },
        )
        .await
        .unwrap();
    client.delete_topic(&topic.arn).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn errors_are_classified() {
    if endpoint().is_none() {
        eprintln!("skipping: FANFERRO_TEST_ENDPOINT_URL is not set");
        return;
    }
    let client = client(ClientConfig::default()).await;
    let missing = client.backend().sqs.get_queue_url(&unique("missing"), None).await.unwrap_err();
    assert!(missing.is_not_found(), "{missing:?}");
    let report =
        client.ensure_topic(&TopicSpec { name: Some(unique("absent")), ..TopicSpec::default() }).await.unwrap();
    assert_eq!(report.status, ProvisionStatus::Missing, "{:?}", report.error);
    let err = client
        .backend()
        .sqs
        .send_message("http://localhost:1/000000000000/x", SendRequest { body: "x".into(), ..SendRequest::default() })
        .await
        .unwrap_err();
    assert!(err.is_retryable() || err.aws().is_some(), "{err:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn long_polls_outlast_a_configured_operation_timeout() {
    if endpoint().is_none() {
        eprintln!("skipping: FANFERRO_TEST_ENDPOINT_URL is not set");
        return;
    }
    let aws =
        AwsConfig { timeouts: TimeoutSettings { operation: Some(1.0), ..TimeoutSettings::default() }, ..aws_config() };
    let client = Client::connect(&aws, ClientConfig::default()).await.expect("connect");
    let queue = client
        .ensure_queue(&QueueSpec { name: Some(unique("e2e-timeout")), policy: create(), ..QueueSpec::default() })
        .await
        .unwrap()
        .resource
        .unwrap();
    let sender = client.clone();
    let url = queue.url.clone();
    let send = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        sender.backend().sqs.send_message(&url, SendRequest { body: "late".into(), ..SendRequest::default() }).await
    });
    let received = client
        .backend()
        .sqs
        .receive_messages(ReceiveRequest {
            queue_url: queue.url.clone(),
            max_messages: 1,
            wait_time: Duration::from_secs(4),
            visibility_timeout: None,
            attempt_id: None,
        })
        .await
        .expect("the long poll outlasts the 1 s operation timeout");
    send.await.unwrap().unwrap();
    assert_eq!(received.iter().map(|m| m.body.as_str()).collect::<Vec<_>>(), ["late"]);
    client.delete_queue(&queue.url).await.unwrap();
}
