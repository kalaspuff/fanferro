use std::time::Duration;

use super::*;
use crate::backend::StringMap;
use crate::backend::memory::MemoryBroker;
use crate::client::ClientConfig;
use crate::types::{AttributeValue, MessageAttributes};

fn client() -> (MemoryBroker, Client) {
    let broker = MemoryBroker::new("us-east-1", "000000000000");
    let client = Client::with_backend(broker.backend(), ClientConfig::default()).unwrap();
    (broker, client)
}

async fn queue(client: &Client, name: &str) -> (String, String) {
    let sqs = &client.backend().sqs;
    let attributes = if name.ends_with(".fifo") {
        StringMap::from([("FifoQueue".to_string(), "true".to_string())])
    } else {
        StringMap::new()
    };
    let url = sqs.create_queue(name, &attributes, &StringMap::new()).await.unwrap();
    let arn = sqs.get_queue_attributes(&url).await.unwrap()["QueueArn"].clone();
    (url, arn)
}

/// A message as fanferro's consumer dead-letters it from `source` (after `requeues` requeues).
fn dead_lettered(body: &str, source: Option<&str>, requeues: Option<u32>) -> SendRequest {
    let mut attributes = MessageAttributes::new();
    attributes.insert("tenant".into(), AttributeValue::string("acme"));
    if let Some(attempt) = requeues {
        attributes.insert(reserved::ATTEMPT.into(), AttributeValue::number(attempt.to_string()));
    }
    let mut info = serde_json::json!({"reason": "boom", "error_type": "ValueError"});
    if let Some(source) = source {
        info["source"] = source.into();
    }
    attributes.insert(reserved::DEAD_LETTER.into(), AttributeValue::string(info.to_string()));
    SendRequest { body: body.into(), attributes, ..SendRequest::default() }
}

async fn receive_all(client: &Client, url: &str) -> Vec<ReceivedMessage> {
    let options = PeekOptions { limit: 100, wait_time: Duration::ZERO, ..PeekOptions::default() };
    client.peek(url, &options).await.unwrap()
}

fn options() -> RedriveOptions {
    RedriveOptions { wait_time: Duration::ZERO, ..RedriveOptions::default() }
}

#[tokio::test(start_paused = true)]
async fn peek_returns_distinct_messages_and_leaves_them_visible() {
    let (broker, client) = client();
    let (url, _) = queue(&client, "dlq").await;
    for i in 0..25 {
        client.backend().sqs.send_message(&url, dead_lettered(&format!("m{i}"), None, None)).await.unwrap();
    }
    let peek = PeekOptions { limit: 15, wait_time: Duration::ZERO, ..PeekOptions::default() };
    let messages = client.peek(&url, &peek).await.unwrap();
    assert_eq!(messages.len(), 15);
    let ids: HashSet<_> = messages.iter().map(|message| message.message_id.clone()).collect();
    assert_eq!(ids.len(), 15);
    // Everything is visible again right away.
    assert_eq!(receive_all(&client, &url).await.len(), 25);
    assert_eq!(broker.queue_messages(&url).unwrap().len(), 25);

    let empty = queue(&client, "empty").await.0;
    assert!(client.peek(&empty, &peek).await.unwrap().is_empty());
    let invalid = PeekOptions { visibility_timeout: Duration::ZERO, ..peek };
    assert!(matches!(client.peek(&url, &invalid).await, Err(Error::Validation(_))));
}

#[tokio::test(start_paused = true)]
async fn redrive_moves_messages_to_their_recorded_source_without_fanferro_metadata() {
    let (broker, client) = client();
    let (dlq, _) = queue(&client, "dlq").await;
    let (orders, orders_arn) = queue(&client, "orders").await;
    let (billing, _) = queue(&client, "billing").await;
    let sqs = &client.backend().sqs;
    sqs.send_message(&dlq, dead_lettered("a", Some(&orders_arn), Some(4))).await.unwrap();
    sqs.send_message(&dlq, dead_lettered("b", Some(&orders_arn), None)).await.unwrap();
    // fanferro records the queue URL when it does not know the ARN.
    sqs.send_message(&dlq, dead_lettered("c", Some(&billing), None)).await.unwrap();

    let report = client.redrive(&dlq, &options()).await.unwrap();
    assert_eq!(report.moved, 3, "{report:?}");
    assert!(report.not_moved.is_empty() && report.not_deleted.is_empty());
    assert_eq!(report.destinations, BTreeMap::from([(orders.clone(), 2), (billing.clone(), 1)]));
    assert!(broker.queue_messages(&dlq).unwrap().is_empty());

    let mut moved = receive_all(&client, &orders).await;
    moved.sort_by(|a, b| a.body.cmp(&b.body));
    assert_eq!(moved.iter().map(|message| message.body.as_str()).collect::<Vec<_>>(), ["a", "b"]);
    for message in &moved {
        assert_eq!(message.attributes.keys().collect::<Vec<_>>(), ["tenant"]);
    }
    assert_eq!(receive_all(&client, &billing).await.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn redrive_can_keep_the_attempt_count() {
    let (_, client) = client();
    let (dlq, _) = queue(&client, "dlq").await;
    let (orders, orders_arn) = queue(&client, "orders").await;
    client.backend().sqs.send_message(&dlq, dead_lettered("a", Some(&orders_arn), Some(4))).await.unwrap();
    let report = client.redrive(&dlq, &RedriveOptions { reset_attempts: false, ..options() }).await.unwrap();
    assert_eq!(report.moved, 1);
    let moved = receive_all(&client, &orders).await;
    assert_eq!(moved[0].attributes[reserved::ATTEMPT].as_text(), Some("4"));
    assert!(!moved[0].attributes.contains_key(reserved::DEAD_LETTER));
}

#[tokio::test(start_paused = true)]
async fn redrive_follows_the_source_sqs_recorded() {
    let (_, client) = client();
    let (dlq, dlq_arn) = queue(&client, "dlq").await;
    let sqs = &client.backend().sqs;
    let policy = format!(r#"{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":"1"}}"#);
    let attributes = StringMap::from([("RedrivePolicy".to_string(), policy)]);
    let source = sqs.create_queue("source", &attributes, &StringMap::new()).await.unwrap();
    sqs.send_message(&source, SendRequest { body: "poison".into(), ..SendRequest::default() }).await.unwrap();
    let receive = |url: &str| ReceiveRequest {
        queue_url: url.into(),
        max_messages: 1,
        wait_time: Duration::ZERO,
        visibility_timeout: Some(Duration::from_secs(1)),
        attempt_id: None,
    };
    assert_eq!(sqs.receive_messages(receive(&source)).await.unwrap().len(), 1);
    tokio::time::advance(Duration::from_secs(2)).await;
    // The second receive moves it to the dead-letter queue.
    assert!(sqs.receive_messages(receive(&source)).await.unwrap().is_empty());
    assert_eq!(receive_all(&client, &dlq).await.len(), 1);

    let report = client.redrive(&dlq, &options()).await.unwrap();
    assert_eq!(report.destinations, BTreeMap::from([(source.clone(), 1)]));
    assert_eq!(receive_all(&client, &source).await[0].body, "poison");
}

#[tokio::test(start_paused = true)]
async fn messages_without_a_known_source_stay_until_redriven_to_a_queue() {
    let (broker, client) = client();
    let (dlq, _) = queue(&client, "dlq").await;
    let (target, _) = queue(&client, "target").await;
    let sqs = &client.backend().sqs;
    sqs.send_message(&dlq, dead_lettered("unknown", None, None)).await.unwrap();
    let missing = "arn:aws:sqs:us-east-1:000000000000:deleted";
    sqs.send_message(&dlq, dead_lettered("deleted", Some(missing), None)).await.unwrap();
    sqs.send_message(&dlq, dead_lettered("not-sqs", Some("arn:aws:sns:us-east-1:000000000000:t"), None)).await.unwrap();

    let report = client.redrive(&dlq, &options()).await.unwrap();
    assert_eq!(report.moved, 0);
    assert_eq!(report.not_moved.len(), 3);
    assert!(report.not_moved.iter().any(|left| left.error.is_not_found()));
    // They are visible again for the next run.
    assert_eq!(receive_all(&client, &dlq).await.len(), 3);

    let report = client.redrive(&dlq, &RedriveOptions { to: Some(target.clone()), ..options() }).await.unwrap();
    assert_eq!(report.moved, 3);
    assert!(broker.queue_messages(&dlq).unwrap().is_empty());
    assert_eq!(receive_all(&client, &target).await.len(), 3);

    let itself = RedriveOptions { to: Some(dlq.clone()), ..options() };
    assert!(matches!(client.redrive(&dlq, &itself).await, Err(Error::Validation(_))));
}

#[tokio::test(start_paused = true)]
async fn redrive_stops_at_the_limit_and_keeps_to_the_rate() {
    let (broker, client) = client();
    let (dlq, _) = queue(&client, "dlq").await;
    let (orders, orders_arn) = queue(&client, "orders").await;
    for i in 0..30 {
        client
            .backend()
            .sqs
            .send_message(&dlq, dead_lettered(&format!("m{i}"), Some(&orders_arn), None))
            .await
            .unwrap();
    }
    let report = client.redrive(&dlq, &RedriveOptions { limit: Some(12), ..options() }).await.unwrap();
    assert_eq!(report.moved, 12);
    assert_eq!(broker.queue_messages(&dlq).unwrap().len(), 18);

    let started = Instant::now();
    let rated = RedriveOptions { max_per_second: Some(4.0), ..options() };
    let report = client.redrive(&dlq, &rated).await.unwrap();
    assert_eq!(report.moved, 18);
    // 18 messages at 4 per second: the last batch of 4 starts after 16 / 4 = 4 seconds.
    assert!(started.elapsed() >= Duration::from_secs(4), "{:?}", started.elapsed());
    assert!(started.elapsed() < Duration::from_secs(5), "{:?}", started.elapsed());
    assert_eq!(receive_all(&client, &orders).await.len(), 30);

    assert!(matches!(
        client.redrive(&dlq, &RedriveOptions { max_per_second: Some(0.0), ..options() }).await,
        Err(Error::Validation(_))
    ));
    assert!(matches!(
        client.redrive(&dlq, &RedriveOptions { limit: Some(0), ..options() }).await,
        Err(Error::Validation(_))
    ));
}

#[tokio::test(start_paused = true)]
async fn redrive_to_a_fifo_queue_keeps_message_groups_in_order() {
    let (_, client) = client();
    let (dlq, _) = queue(&client, "dlq.fifo").await;
    let (orders, orders_arn) = queue(&client, "orders.fifo").await;
    let sqs = &client.backend().sqs;
    for (i, group) in ["a", "b", "a", "a", "b"].iter().enumerate() {
        let mut request = dead_lettered(&format!("{group}{i}"), Some(&orders_arn), Some(2));
        request.group_id = Some((*group).into());
        request.deduplication_id = Some(format!("d{i}"));
        sqs.send_message(&dlq, request).await.unwrap();
    }
    let report = client.redrive(&dlq, &options()).await.unwrap();
    assert_eq!(report.moved, 5, "{report:?}");
    let moved = receive_all(&client, &orders).await;
    let order = |group: &str| {
        moved
            .iter()
            .filter(|message| message.system.message_group_id.as_deref() == Some(group))
            .map(|message| message.body.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(order("a"), ["a0", "a2", "a3"]);
    assert_eq!(order("b"), ["b1", "b4"]);
}

#[test]
fn the_source_is_read_from_fanferro_metadata_first() {
    let mut message = ReceivedMessage {
        message_id: "m".into(),
        receipt_handle: "h".into(),
        body: String::new(),
        md5_of_body: None,
        attributes: MessageAttributes::new(),
        md5_of_message_attributes: None,
        system: Default::default(),
    };
    assert_eq!(dead_letter_source(&message), None);
    message.system.dead_letter_queue_source_arn = Some("arn:aws:sqs:eu-west-1:1:sqs".into());
    assert_eq!(dead_letter_source(&message).as_deref(), Some("arn:aws:sqs:eu-west-1:1:sqs"));
    message.attributes = dead_lettered("", Some("arn:aws:sqs:eu-west-1:1:fanferro"), None).attributes;
    assert_eq!(dead_letter_source(&message).as_deref(), Some("arn:aws:sqs:eu-west-1:1:fanferro"));
    message.attributes.insert(reserved::DEAD_LETTER.into(), AttributeValue::string("not json"));
    assert_eq!(dead_letter_source(&message).as_deref(), Some("arn:aws:sqs:eu-west-1:1:sqs"));
}
