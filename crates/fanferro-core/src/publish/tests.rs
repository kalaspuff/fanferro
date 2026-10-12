use bytes::Bytes;

use super::*;
use crate::backend::StringMap;
use crate::backend::memory::MemoryBroker;
use crate::client::ClientConfig;
use crate::transport::OffloadSettings;
use crate::transport::claim_check::{EncryptionSettings, PointerFormat, ReferenceMode};
use crate::types::{AttributeValue, MessageAttributes};

async fn setup(config: ClientConfig) -> (MemoryBroker, Client) {
    let broker = MemoryBroker::new("us-east-1", "000000000000");
    let client = Client::with_backend(broker.backend(), config).unwrap();
    (broker, client)
}

fn message(id: &str, body: &str) -> OutgoingMessage {
    OutgoingMessage {
        wire: WireMessage::new(Bytes::copy_from_slice(body.as_bytes()), MessageAttributes::new()),
        message_id: id.into(),
        ..OutgoingMessage::default()
    }
}

fn queue(url: &str) -> Destination {
    Destination::Queue { url: url.into(), fifo: false, content_based_deduplication: false, max_message_size: None }
}

#[tokio::test]
async fn sends_to_standard_queue() {
    let (broker, client) = setup(ClientConfig::default()).await;
    let url = client.backend().sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    let mut msg = message("m1", "hello");
    msg.wire.attributes.insert("tenant".into(), AttributeValue::string("acme"));
    let receipt = client.publish(&queue(&url), msg).await.unwrap();
    assert_eq!(receipt.message_id, "m1");
    assert!(!receipt.transport_message_id.is_empty());
    let stored = broker.queue_messages(&url).unwrap();
    assert_eq!(stored[0].body, "hello");
    assert_eq!(client.metrics().messages_published.get(), 1);
}

#[tokio::test]
async fn validates_destination_options() {
    let (_, client) = setup(ClientConfig::default()).await;
    let fifo =
        Destination::Queue { url: "u".into(), fifo: true, content_based_deduplication: false, max_message_size: None };
    let err = client.publish(&fifo, message("m", "x")).await.unwrap_err();
    assert!(err.to_string().contains("group_id"));
    let mut delayed = message("m", "x");
    delayed.delay = Some(Duration::from_secs(1));
    let mut grouped = delayed.clone();
    grouped.group_id = Some("g".into());
    assert!(client.publish(&fifo, grouped).await.is_err());
    delayed.delay = Some(Duration::from_secs(901));
    assert!(client.publish(&queue("u"), delayed).await.is_err());
    let mut dedup = message("m", "x");
    dedup.deduplication_id = Some("d".into());
    assert!(client.publish(&queue("u"), dedup).await.is_err());
    let topic =
        Destination::Topic { arn: "a".into(), fifo: false, content_based_deduplication: false, max_message_size: None };
    let mut topic_delay = message("m", "x");
    topic_delay.delay = Some(Duration::from_secs(1));
    assert!(client.publish(&topic, topic_delay).await.is_err());
    let mut subject = message("m", "x");
    subject.subject = Some("s".into());
    assert!(client.publish(&queue("u"), subject).await.is_err());
}

#[tokio::test]
async fn fifo_uses_message_id_as_dedup_id() {
    let (broker, client) = setup(ClientConfig::default()).await;
    let url = client
        .backend()
        .sqs
        .create_queue("q.fifo", &StringMap::from([("FifoQueue".to_string(), "true".to_string())]), &StringMap::new())
        .await
        .unwrap();
    let destination =
        Destination::Queue { url: url.clone(), fifo: true, content_based_deduplication: false, max_message_size: None };
    let mut first = message("same-id", "a");
    first.group_id = Some("g".into());
    let a = client.publish(&destination, first.clone()).await.unwrap();
    let b = client.publish(&destination, first).await.unwrap();
    assert_eq!(a.transport_message_id, b.transport_message_id);
    assert_eq!(broker.queue_messages(&url).unwrap().len(), 1);
}

#[tokio::test]
async fn batches_split_and_report_per_entry() {
    let (broker, client) = setup(ClientConfig::default()).await;
    let url = client.backend().sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    let mut messages: Vec<OutgoingMessage> = (0..23).map(|i| message(&format!("m{i}"), &format!("body {i}"))).collect();
    // An entry that fails validation does not affect the others.
    messages[5].wire.attributes.insert("bad name".into(), AttributeValue::string("x"));
    let results = client.publish_batch(&queue(&url), messages).await.unwrap();
    assert_eq!(results.len(), 23);
    assert!(results[5].is_err());
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 22);
    assert_eq!(results[22].as_ref().unwrap().message_id, "m22");
    assert_eq!(broker.queue_messages(&url).unwrap().len(), 22);
}

#[tokio::test]
async fn batch_against_missing_queue_fails_every_entry() {
    let (_, client) = setup(ClientConfig::default()).await;
    let results = client
        .publish_batch(&queue("memory://us-east-1/000000000000/missing"), vec![message("a", "x"), message("b", "y")])
        .await
        .unwrap();
    assert!(results.iter().all(|r| r.as_ref().unwrap_err().is_not_found()));
}

#[tokio::test]
async fn offloads_large_messages() {
    let offload = OffloadSettings {
        bucket: "payloads".into(),
        key_prefix: "p/".into(),
        threshold: 100,
        always: false,
        reference: ReferenceMode::Both,
        presign_ttl: 60.0,
        format: PointerFormat::Fanferro,
        encryption: EncryptionSettings::BucketDefault,
        storage_class: None,
    };
    let (broker, client) = setup(ClientConfig { offload: Some(offload), ..ClientConfig::default() }).await;
    client.backend().objects.create_bucket("payloads").await.unwrap();
    let url = client.backend().sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    let big = "x".repeat(5000);
    let receipt = client.publish(&queue(&url), message("big", &big)).await.unwrap();
    assert!(receipt.offloaded.is_some());
    let stored = broker.queue_messages(&url).unwrap();
    assert!(stored[0].body.starts_with(r#"{"fanferro":"claim-check/v1""#));
    assert!(stored[0].attributes.contains_key("fanferro.claim-check"));
    assert_eq!(client.metrics().payloads_offloaded.get(), 1);
}

#[tokio::test]
async fn too_large_messages_fail_fast() {
    let (_, client) = setup(ClientConfig::default()).await;
    let destination = Destination::Queue {
        url: "u".into(),
        fifo: false,
        content_based_deduplication: false,
        max_message_size: Some(10),
    };
    let err = client.publish(&destination, message("m", "this is longer than ten bytes")).await.unwrap_err();
    assert!(matches!(err, Error::MessageTooLarge { .. }));
}

#[tokio::test]
async fn publishes_to_topics_and_fans_out() {
    let (broker, client) = setup(ClientConfig::default()).await;
    let backend = client.backend().clone();
    let topic = backend.sns.create_topic("t", &StringMap::new(), &StringMap::new()).await.unwrap();
    let url = backend.sqs.create_queue("sub", &StringMap::new(), &StringMap::new()).await.unwrap();
    backend
        .sns
        .subscribe(
            &topic,
            "sqs",
            "arn:aws:sqs:us-east-1:000000000000:sub",
            &StringMap::from([("RawMessageDelivery".to_string(), "true".to_string())]),
        )
        .await
        .unwrap();
    let destination =
        Destination::Topic { arn: topic, fifo: false, content_based_deduplication: false, max_message_size: None };
    let mut msg = message("m1", "event");
    msg.subject = Some("subject".into());
    client.publish(&destination, msg).await.unwrap();
    let results = client.publish_batch(&destination, vec![message("m2", "e2"), message("m3", "e3")]).await.unwrap();
    assert!(results.iter().all(Result::is_ok));
    let bodies: Vec<String> = broker.queue_messages(&url).unwrap().into_iter().map(|m| m.body).collect();
    assert_eq!(bodies, vec!["event", "e2", "e3"]);
}

#[test]
fn chunking_respects_count_and_size() {
    let prepared = |size: usize| Prepared {
        body: OutgoingBody { body: "x".repeat(size), attributes: MessageAttributes::new(), offloaded: None },
        group_id: None,
        deduplication_id: None,
        delay: None,
        subject: None,
        trace_header: None,
        message_id: "m".into(),
    };
    let entries: Vec<(usize, Prepared)> = (0..25).map(|i| (i, prepared(10))).collect();
    let chunks = chunk_by_size(entries, 1_000_000);
    assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), vec![10, 10, 5]);
    let entries: Vec<(usize, Prepared)> = (0..4).map(|i| (i, prepared(400))).collect();
    let chunks = chunk_by_size(entries, 1000);
    assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), vec![2, 2]);
}
