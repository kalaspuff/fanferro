use bytes::Bytes;

use super::*;
use crate::backend::memory::MemoryBroker;
use crate::backend::testing::{SlowBatches, SlowUploads, ThrottledBatches, ThrottledPublishBatches};
use crate::backend::{ReceiveRequest, StringMap};
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
    for header in ["", "Root=1-5759e988-bd862e3fe1be46a994272793\nParent=1", "Root=\u{e9}"] {
        let mut traced = message("m", "x");
        traced.trace_header = Some(header.into());
        let err = client.publish(&topic, traced).await.unwrap_err();
        assert!(err.to_string().contains("trace_header"), "{err}");
    }
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

#[tokio::test]
async fn batch_publishes_to_topics_split_by_trace_header_when_configured() {
    // SNS takes one trace header per PublishBatch call: the in-memory broker kept each entry's.
    const A: &str = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1";
    const B: &str = "Root=1-5759e988-bd862e3fe1be46a994272794;Parent=53995c3f42cd8ad9;Sampled=1";
    let headers = [Some(A), Some(B), None, Some(A)];
    for (split, fifo) in [(false, false), (true, false), (true, true)] {
        let (_, client) = setup(ClientConfig { split_batches_by_trace_header: split, ..ClientConfig::default() }).await;
        let backend = client.backend().clone();
        let suffix = if fifo { ".fifo" } else { "" };
        let mut topic_attributes = StringMap::new();
        let mut queue_attributes = StringMap::new();
        if fifo {
            topic_attributes.insert("FifoTopic".into(), "true".into());
            queue_attributes.insert("FifoQueue".into(), "true".into());
        }
        let topic =
            backend.sns.create_topic(&format!("t{suffix}"), &topic_attributes, &StringMap::new()).await.unwrap();
        let url =
            backend.sqs.create_queue(&format!("sub{suffix}"), &queue_attributes, &StringMap::new()).await.unwrap();
        let raw = StringMap::from([("RawMessageDelivery".to_string(), "true".to_string())]);
        let queue_arn = format!("arn:aws:sqs:us-east-1:000000000000:sub{suffix}");
        backend.sns.subscribe(&topic, "sqs", &queue_arn, &raw).await.unwrap();
        let destination =
            Destination::Topic { arn: topic, fifo, content_based_deduplication: false, max_message_size: None };
        let batch = |headers: &[Option<&str>]| -> Vec<OutgoingMessage> {
            headers
                .iter()
                .enumerate()
                .map(|(i, header)| OutgoingMessage {
                    trace_header: header.map(str::to_string),
                    group_id: fifo.then(|| "g".to_string()),
                    ..message(&format!("m{i}"), &format!("e{i}"))
                })
                .collect()
        };
        let results = client.publish_batch(&destination, batch(&headers)).await.unwrap();
        assert!(results.iter().all(Result::is_ok));
        let request = ReceiveRequest {
            queue_url: url.clone(),
            max_messages: 10,
            wait_time: Duration::ZERO,
            visibility_timeout: Some(Duration::from_secs(30)),
            attempt_id: None,
        };
        let received = backend.sqs.receive_messages(request.clone()).await.unwrap();
        let mut seen: Vec<(String, Option<String>)> =
            received.into_iter().map(|m| (m.body, m.system.aws_trace_header)).collect();
        if fifo {
            let bodies: Vec<&str> = seen.iter().map(|(body, _)| body.as_str()).collect();
            assert_eq!(bodies, vec!["e0", "e1", "e2", "e3"], "in order");
        }
        seen.sort();
        let expected: Vec<(String, Option<String>)> = headers
            .iter()
            .enumerate()
            .map(|(i, header)| (format!("e{i}"), if split { header.map(str::to_string) } else { None }))
            .collect();
        assert_eq!(seen, expected, "split: {split}, fifo: {fifo}");
        // Entries that share their trace header keep it either way.
        if !split {
            client.publish_batch(&destination, batch(&[Some(A), Some(A)])).await.unwrap();
            let received = backend.sqs.receive_messages(request).await.unwrap();
            assert!(received.iter().all(|m| m.system.aws_trace_header.as_deref() == Some(A)));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn retried_batch_entries_stay_split_by_trace_header() {
    // Entries retried after a failed call were batched by size only: entries with different trace
    // headers went out together, in a call that sends none.
    const A: &str = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1";
    const B: &str = "Root=1-5759e988-bd862e3fe1be46a994272794;Parent=53995c3f42cd8ad9;Sampled=1";
    let broker = MemoryBroker::new("us-east-1", "000000000000");
    let mut backend = broker.backend();
    let topic = backend.sns.create_topic("t", &StringMap::new(), &StringMap::new()).await.unwrap();
    // The first calls of all three groups fail: their entries are retried together.
    let throttled = std::sync::Arc::new(ThrottledPublishBatches::new(backend.sns.clone(), 3));
    backend.sns = throttled.clone();
    let config = ClientConfig { split_batches_by_trace_header: true, ..ClientConfig::default() };
    let client = Client::with_backend(backend, config).unwrap();
    let destination =
        Destination::Topic { arn: topic, fifo: false, content_based_deduplication: false, max_message_size: None };
    let messages = [Some(A), Some(B), None, Some(A)]
        .iter()
        .enumerate()
        .map(|(i, header)| OutgoingMessage {
            trace_header: header.map(str::to_string),
            ..message(&format!("m{i}"), &format!("e{i}"))
        })
        .collect();
    let results = client.publish_batch(&destination, messages).await.unwrap();
    assert!(results.iter().all(Result::is_ok));
    let mut calls = throttled.calls.lock().clone();
    calls.sort();
    let header = |h: &str| Some(h.to_string());
    assert_eq!(calls, vec![vec![None], vec![header(A), header(A)], vec![header(B)]]);
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

#[tokio::test(start_paused = true)]
async fn batch_calls_overlap_except_for_fifo_destinations() {
    let broker = MemoryBroker::new("us-east-1", "000000000000");
    let mut backend = broker.backend();
    let slow = std::sync::Arc::new(SlowBatches::new(backend.sqs.clone(), Duration::from_millis(100)));
    backend.sqs = slow.clone();
    let client = Client::with_backend(backend, ClientConfig::default()).unwrap();

    // Ten batch calls, at most eight at once: two rounds instead of ten.
    let url = client.backend().sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    let messages = (0..95).map(|i| message(&format!("m{i}"), &format!("body {i}"))).collect();
    let started = tokio::time::Instant::now();
    let results = client.publish_batch(&queue(&url), messages).await.unwrap();
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(results[94].as_ref().unwrap().message_id, "m94");
    assert_eq!(slow.sends.take_peak(), MAX_CONCURRENT_BATCH_CALLS);
    assert_eq!(started.elapsed(), Duration::from_millis(200));
    assert_eq!(broker.queue_messages(&url).unwrap().len(), 95);

    // FIFO: one call after another, so that the group keeps its order.
    let fifo = client
        .backend()
        .sqs
        .create_queue("q.fifo", &StringMap::from([("FifoQueue".to_string(), "true".to_string())]), &StringMap::new())
        .await
        .unwrap();
    let destination = Destination::Queue {
        url: fifo.clone(),
        fifo: true,
        content_based_deduplication: false,
        max_message_size: None,
    };
    let messages = (0..25)
        .map(|i| OutgoingMessage { group_id: Some("g".into()), ..message(&format!("f{i}"), &format!("{i}")) })
        .collect();
    let started = tokio::time::Instant::now();
    let results = client.publish_batch(&destination, messages).await.unwrap();
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(slow.sends.take_peak(), 1);
    assert_eq!(started.elapsed(), Duration::from_millis(300));
    let bodies: Vec<String> = broker.queue_messages(&fifo).unwrap().into_iter().map(|m| m.body).collect();
    assert_eq!(bodies, (0..25).map(|i| i.to_string()).collect::<Vec<_>>());
}

#[tokio::test(start_paused = true)]
async fn fifo_batches_keep_their_order_when_a_batch_is_retried() {
    let broker = MemoryBroker::new("us-east-1", "000000000000");
    let mut backend = broker.backend();
    // The first batch call is throttled (after the SDK's own retries): its entries are retried.
    backend.sqs = std::sync::Arc::new(ThrottledBatches::new(backend.sqs.clone(), 1));
    let client = Client::with_backend(backend, ClientConfig::default()).unwrap();
    let url = client
        .backend()
        .sqs
        .create_queue("q.fifo", &StringMap::from([("FifoQueue".to_string(), "true".to_string())]), &StringMap::new())
        .await
        .unwrap();
    let destination =
        Destination::Queue { url: url.clone(), fifo: true, content_based_deduplication: false, max_message_size: None };
    let messages: Vec<OutgoingMessage> = (0..25)
        .map(|i| OutgoingMessage { group_id: Some("g".into()), ..message(&format!("m{i}"), &i.to_string()) })
        .collect();
    let results = client.publish_batch(&destination, messages).await.unwrap();
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(client.metrics().publish_retries.get(), 10);
    let bodies: Vec<String> = broker.queue_messages(&url).unwrap().into_iter().map(|m| m.body).collect();
    assert_eq!(bodies, (0..25).map(|i| i.to_string()).collect::<Vec<_>>(), "a later batch overtook a retried one");
}

#[tokio::test(start_paused = true)]
async fn standard_batches_retry_throttled_entries() {
    let broker = MemoryBroker::new("us-east-1", "000000000000");
    let mut backend = broker.backend();
    backend.sqs = std::sync::Arc::new(ThrottledBatches::new(backend.sqs.clone(), 2));
    let client = Client::with_backend(backend, ClientConfig::default()).unwrap();
    let url = client.backend().sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    let messages: Vec<OutgoingMessage> = (0..25).map(|i| message(&format!("m{i}"), &i.to_string())).collect();
    let results = client.publish_batch(&queue(&url), messages).await.unwrap();
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(broker.queue_messages(&url).unwrap().len(), 25);
}

#[tokio::test(start_paused = true)]
async fn batches_upload_a_bounded_number_of_payloads_at_once() {
    let broker = MemoryBroker::new("us-east-1", "000000000000");
    let mut backend = broker.backend();
    let uploads = std::sync::Arc::new(SlowUploads::new(backend.objects.clone(), Duration::from_millis(50)));
    backend.objects = uploads.clone();
    let offload = OffloadSettings {
        bucket: "payloads".into(),
        key_prefix: "p/".into(),
        threshold: 100,
        always: false,
        reference: ReferenceMode::S3,
        presign_ttl: 60.0,
        format: PointerFormat::Fanferro,
        encryption: EncryptionSettings::BucketDefault,
        storage_class: None,
    };
    let client =
        Client::with_backend(backend, ClientConfig { offload: Some(offload), ..ClientConfig::default() }).unwrap();
    client.backend().objects.create_bucket("payloads").await.unwrap();
    let url = client.backend().sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    let big = "x".repeat(500);
    let messages: Vec<OutgoingMessage> = (0..100).map(|i| message(&format!("m{i}"), &big)).collect();
    let results = client.publish_batch(&queue(&url), messages).await.unwrap();
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(client.metrics().payloads_offloaded.get(), 100);
    assert_eq!(uploads.puts.take_peak(), MAX_CONCURRENT_UPLOADS);
    assert_eq!(broker.queue_messages(&url).unwrap().len(), 100);
}

#[tokio::test]
async fn fifo_ids_are_validated_before_sending() {
    // docs/parity M8: SQS and SNS refuse group and deduplication ids that are not 1-128 ASCII
    // letters, digits and punctuation; fanferro sent them and failed at the service.
    let (_, client) = setup(ClientConfig::default()).await;
    let fifo_queue =
        Destination::Queue { url: "u".into(), fifo: true, content_based_deduplication: false, max_message_size: None };
    let fifo_topic =
        Destination::Topic { arn: "a".into(), fifo: true, content_based_deduplication: false, max_message_size: None };
    let content_based =
        Destination::Topic { arn: "a".into(), fifo: true, content_based_deduplication: true, max_message_size: None };
    for destination in [&fifo_queue, &fifo_topic] {
        for (group, deduplication, id, expected) in [
            (
                "a b",
                None,
                "m",
                "group_id must be 1-128 ASCII letters, digits and punctuation characters (no spaces), got \"a b\"",
            ),
            (&*"g".repeat(129), None, "m", "group_id must be"),
            ("gr\u{fc}n", None, "m", "group_id must be"),
            (
                "g",
                Some("d d"),
                "m",
                "deduplication_id must be 1-128 ASCII letters, digits and punctuation characters (no spaces), got \"d d\"",
            ),
            ("g", Some(""), "m", "deduplication_id must be"),
            (
                "g",
                None,
                "order 1",
                "the message id is the FIFO deduplication id unless deduplication_id is set: it must be 1-128 ASCII letters",
            ),
        ] {
            let mut outgoing = message(id, "x");
            outgoing.group_id = Some(group.into());
            outgoing.deduplication_id = deduplication.map(str::to_string);
            let err = client.publish(destination, outgoing).await.unwrap_err();
            assert!(matches!(err, Error::Validation(_)), "{err}");
            assert!(err.to_string().contains(expected), "{err}");
        }
    }
    // Content-based deduplication does not use the message id; a group id on a standard queue is
    // checked too (fair queueing).
    let mut outgoing = message("order 1", "x");
    outgoing.group_id = Some("g".into());
    let err = client.publish(&content_based, outgoing).await.unwrap_err();
    assert!(!err.to_string().contains("message id"), "{err}");
    let mut grouped = message("m", "x");
    grouped.group_id = Some("tenant a".into());
    let err = client.publish(&queue("u"), grouped).await.unwrap_err();
    assert!(err.to_string().contains("group_id must be"), "{err}");
}

#[tokio::test]
async fn fifo_ids_of_letters_digits_and_punctuation_are_sent() {
    let (broker, client) = setup(ClientConfig::default()).await;
    let fifo = StringMap::from([("FifoQueue".to_string(), "true".to_string())]);
    let url = client.backend().sqs.create_queue("q.fifo", &fifo, &StringMap::new()).await.unwrap();
    let destination =
        Destination::Queue { url: url.clone(), fifo: true, content_based_deduplication: false, max_message_size: None };
    let punctuation = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";
    let mut outgoing = message(&"m".repeat(128), "x");
    outgoing.group_id = Some(format!("Group-1{punctuation}"));
    client.publish(&destination, outgoing).await.unwrap();
    let stored = broker.queue_messages(&url).unwrap();
    assert_eq!(stored[0].group_id.as_deref(), Some(format!("Group-1{punctuation}").as_str()));
}
