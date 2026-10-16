use std::time::Duration;

use super::*;
use crate::types::AttributeValue;

fn broker() -> (MemoryBroker, Backend) {
    let broker = MemoryBroker::new("us-east-1", "000000000000");
    let backend = broker.backend();
    (broker, backend)
}

fn send(body: &str) -> SendRequest {
    SendRequest { body: body.into(), ..SendRequest::default() }
}

fn receive(url: &str, max: u8, visibility: Option<u64>) -> ReceiveRequest {
    ReceiveRequest {
        queue_url: url.into(),
        max_messages: max,
        wait_time: Duration::ZERO,
        visibility_timeout: visibility.map(Duration::from_secs),
        attempt_id: None,
    }
}

#[tokio::test(start_paused = true)]
async fn send_receive_delete_and_visibility() {
    let (_, backend) = broker();
    let url = backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    let mut attributes = MessageAttributes::new();
    attributes.insert("k".into(), AttributeValue::string("v"));
    let output = backend
        .sqs
        .send_message(&url, SendRequest { body: "hello".into(), attributes, ..SendRequest::default() })
        .await
        .unwrap();
    assert_eq!(output.md5_of_body.as_deref(), Some(md5_of_body("hello").as_str()));

    let messages = backend.sqs.receive_messages(receive(&url, 10, Some(5))).await.unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].system.approximate_receive_count, 1);
    assert_eq!(messages[0].attributes.get("k"), Some(&AttributeValue::string("v")));
    // Invisible until the timeout passes.
    assert!(backend.sqs.receive_messages(receive(&url, 10, Some(5))).await.unwrap().is_empty());
    tokio::time::advance(Duration::from_secs(6)).await;
    let again = backend.sqs.receive_messages(receive(&url, 10, Some(5))).await.unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].system.approximate_receive_count, 2);

    // The first receipt handle is stale now.
    let results = backend.sqs.delete_message_batch(&url, vec![messages[0].receipt_handle.clone()]).await.unwrap();
    assert!(results[0].is_err());
    let results = backend.sqs.delete_message_batch(&url, vec![again[0].receipt_handle.clone()]).await.unwrap();
    assert!(results[0].is_ok());
    // Deleting twice is idempotent.
    let results = backend.sqs.delete_message_batch(&url, vec![again[0].receipt_handle.clone()]).await.unwrap();
    assert!(results[0].is_ok());
    let results = backend.sqs.delete_message_batch(&url, vec!["garbage".into()]).await.unwrap();
    assert!(results[0].is_err());
}

#[tokio::test(start_paused = true)]
async fn change_visibility_extends_and_releases() {
    let (_, backend) = broker();
    let url = backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    backend.sqs.send_message(&url, send("a")).await.unwrap();
    let message = backend.sqs.receive_messages(receive(&url, 1, Some(5))).await.unwrap().remove(0);
    tokio::time::advance(Duration::from_secs(4)).await;
    let results = backend
        .sqs
        .change_visibility_batch(
            &url,
            vec![VisibilityChange { receipt_handle: message.receipt_handle.clone(), timeout: Duration::from_secs(10) }],
        )
        .await
        .unwrap();
    assert!(results[0].is_ok());
    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(backend.sqs.receive_messages(receive(&url, 1, Some(5))).await.unwrap().is_empty());
    let results = backend
        .sqs
        .change_visibility_batch(
            &url,
            vec![VisibilityChange { receipt_handle: message.receipt_handle.clone(), timeout: Duration::ZERO }],
        )
        .await
        .unwrap();
    assert!(results[0].is_ok());
    assert_eq!(backend.sqs.receive_messages(receive(&url, 1, Some(5))).await.unwrap().len(), 1);
    // Old handle is no longer in flight.
    let results = backend
        .sqs
        .change_visibility_batch(
            &url,
            vec![VisibilityChange { receipt_handle: message.receipt_handle, timeout: Duration::from_secs(1) }],
        )
        .await
        .unwrap();
    assert!(results[0].is_err());
}

#[tokio::test(start_paused = true)]
async fn redrive_moves_messages_to_dead_letter_queue() {
    let (broker, backend) = broker();
    let dlq_url = backend.sqs.create_queue("dlq", &StringMap::new(), &StringMap::new()).await.unwrap();
    let dlq_arn = backend.sqs.get_queue_attributes(&dlq_url).await.unwrap()["QueueArn"].clone();
    let policy = format!(r#"{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":"2"}}"#);
    let url = backend
        .sqs
        .create_queue("q", &StringMap::from([("RedrivePolicy".to_string(), policy)]), &StringMap::new())
        .await
        .unwrap();
    backend.sqs.send_message(&url, send("poison")).await.unwrap();
    for _ in 0..2 {
        assert_eq!(backend.sqs.receive_messages(receive(&url, 1, Some(1))).await.unwrap().len(), 1);
        tokio::time::advance(Duration::from_secs(2)).await;
    }
    assert!(backend.sqs.receive_messages(receive(&url, 1, Some(1))).await.unwrap().is_empty());
    let moved = backend.sqs.receive_messages(receive(&dlq_url, 1, None)).await.unwrap();
    assert_eq!(moved.len(), 1);
    assert_eq!(moved[0].body, "poison");
    assert!(moved[0].system.dead_letter_queue_source_arn.as_deref().unwrap().ends_with(":q"));
    assert_eq!(broker.queue_messages(&url).unwrap().len(), 0);
}

#[tokio::test(start_paused = true)]
async fn fifo_groups_and_deduplication() {
    let (_, backend) = broker();
    let attributes = StringMap::from([("FifoQueue".to_string(), "true".to_string())]);
    let url = backend.sqs.create_queue("q.fifo", &attributes, &StringMap::new()).await.unwrap();
    let fifo = |body: &str, group: &str, dedup: &str| SendRequest {
        body: body.into(),
        group_id: Some(group.into()),
        deduplication_id: Some(dedup.into()),
        ..SendRequest::default()
    };
    // Missing dedup id without content-based deduplication is rejected.
    assert!(
        backend
            .sqs
            .send_message(&url, SendRequest { body: "x".into(), group_id: Some("g".into()), ..SendRequest::default() })
            .await
            .is_err()
    );
    let first = backend.sqs.send_message(&url, fifo("a1", "a", "1")).await.unwrap();
    let duplicate = backend.sqs.send_message(&url, fifo("a1", "a", "1")).await.unwrap();
    assert_eq!(first.message_id, duplicate.message_id);
    backend.sqs.send_message(&url, fifo("a2", "a", "2")).await.unwrap();
    backend.sqs.send_message(&url, fifo("b1", "b", "3")).await.unwrap();

    let batch = backend.sqs.receive_messages(receive(&url, 1, Some(30))).await.unwrap();
    assert_eq!(batch[0].body, "a1");
    // Group "a" is blocked while a1 is in flight, so only b1 is available.
    let next = backend.sqs.receive_messages(receive(&url, 10, Some(30))).await.unwrap();
    assert_eq!(next.iter().map(|m| m.body.as_str()).collect::<Vec<_>>(), vec!["b1"]);
    backend.sqs.delete_message_batch(&url, vec![batch[0].receipt_handle.clone()]).await.unwrap();
    let after = backend.sqs.receive_messages(receive(&url, 10, Some(30))).await.unwrap();
    assert_eq!(after.iter().map(|m| m.body.as_str()).collect::<Vec<_>>(), vec!["a2"]);
    assert!(after[0].system.sequence_number.is_some());
}

#[tokio::test(start_paused = true)]
async fn long_poll_wakes_on_send_and_delays_apply() {
    let (_, backend) = broker();
    let url = backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    let sqs = backend.sqs.clone();
    let poll_url = url.clone();
    let waiter = tokio::spawn(async move {
        sqs.receive_messages(ReceiveRequest {
            queue_url: poll_url,
            max_messages: 10,
            wait_time: Duration::from_secs(20),
            visibility_timeout: None,
            attempt_id: None,
        })
        .await
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    backend.sqs.send_message(&url, SendRequest { delay: Some(Duration::from_secs(3)), ..send("late") }).await.unwrap();
    let started = Instant::now();
    let messages = waiter.await.unwrap().unwrap();
    assert_eq!(messages.len(), 1);
    assert!(started.elapsed() >= Duration::from_secs(3));
}

#[tokio::test]
async fn validation_errors() {
    let (_, backend) = broker();
    let url = backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    assert!(backend.sqs.send_message(&url, send("")).await.is_err());
    assert!(backend.sqs.send_message(&url, send("bad\u{1}")).await.is_err());
    let err = backend.sqs.send_message("memory://us-east-1/000000000000/missing", send("x")).await.unwrap_err();
    assert!(err.is_not_found());
    let err = backend.sqs.get_queue_url("missing", None).await.unwrap_err();
    assert!(err.is_not_found());
    // Same name, different attributes: conflict.
    let err = backend
        .sqs
        .create_queue("q", &StringMap::from([("VisibilityTimeout".to_string(), "99".to_string())]), &StringMap::new())
        .await
        .unwrap_err();
    assert_eq!(err.aws().unwrap().code.as_deref(), Some("QueueAlreadyExists"));
    assert!(backend.sqs.create_queue("bad.fifo", &StringMap::new(), &StringMap::new()).await.is_err());
}

#[tokio::test]
async fn sns_fanout_raw_and_notification_with_filters() {
    let (broker, backend) = broker();
    let topic = backend.sns.create_topic("orders", &StringMap::new(), &StringMap::new()).await.unwrap();
    let raw_url = backend.sqs.create_queue("raw", &StringMap::new(), &StringMap::new()).await.unwrap();
    let json_url = backend.sqs.create_queue("json", &StringMap::new(), &StringMap::new()).await.unwrap();
    let filtered_url = backend.sqs.create_queue("filtered", &StringMap::new(), &StringMap::new()).await.unwrap();
    let arn_of = |url: &str| format!("arn:aws:sqs:us-east-1:000000000000:{}", url.rsplit('/').next().unwrap());
    let raw_sub = backend
        .sns
        .subscribe(
            &topic,
            "sqs",
            &arn_of(&raw_url),
            &StringMap::from([("RawMessageDelivery".to_string(), "true".to_string())]),
        )
        .await
        .unwrap();
    // Subscribing again with the same attributes returns the same subscription.
    assert_eq!(
        backend
            .sns
            .subscribe(
                &topic,
                "sqs",
                &arn_of(&raw_url),
                &StringMap::from([("RawMessageDelivery".to_string(), "true".to_string())])
            )
            .await
            .unwrap(),
        raw_sub
    );
    backend.sns.subscribe(&topic, "sqs", &arn_of(&json_url), &StringMap::new()).await.unwrap();
    backend
        .sns
        .subscribe(
            &topic,
            "sqs",
            &arn_of(&filtered_url),
            &StringMap::from([("FilterPolicy".to_string(), r#"{"kind":["vip"]}"#.to_string())]),
        )
        .await
        .unwrap();
    let mut attributes = MessageAttributes::new();
    attributes.insert("kind".into(), AttributeValue::string("regular"));
    attributes.insert("blob".into(), AttributeValue::binary(vec![1u8, 2, 3]));
    backend
        .sns
        .publish(
            &topic,
            PublishRequest { message: "hi".into(), attributes, subject: Some("s".into()), ..PublishRequest::default() },
        )
        .await
        .unwrap();
    let raw = broker.queue_messages(&raw_url).unwrap();
    assert_eq!(raw[0].body, "hi");
    assert_eq!(raw[0].attributes.get("kind"), Some(&AttributeValue::string("regular")));
    let json = broker.queue_messages(&json_url).unwrap();
    let notification: serde_json::Value = serde_json::from_str(&json[0].body).unwrap();
    assert_eq!(notification["Type"], "Notification");
    assert_eq!(notification["Message"], "hi");
    assert_eq!(notification["Subject"], "s");
    assert_eq!(notification["MessageAttributes"]["blob"]["Value"], "AQID");
    assert!(json[0].attributes.is_empty());
    assert!(broker.queue_messages(&filtered_url).unwrap().is_empty());
}

#[tokio::test]
async fn sns_fifo_requires_group_and_dedups() {
    let (_, backend) = broker();
    let attributes = StringMap::from([
        ("FifoTopic".to_string(), "true".to_string()),
        ("ContentBasedDeduplication".to_string(), "true".to_string()),
    ]);
    let topic = backend.sns.create_topic("t.fifo", &attributes, &StringMap::new()).await.unwrap();
    assert!(
        backend.sns.publish(&topic, PublishRequest { message: "x".into(), ..PublishRequest::default() }).await.is_err()
    );
    let request = PublishRequest { message: "x".into(), group_id: Some("g".into()), ..PublishRequest::default() };
    let first = backend.sns.publish(&topic, request.clone()).await.unwrap();
    let second = backend.sns.publish(&topic, request).await.unwrap();
    assert_eq!(first.message_id, second.message_id);
    assert!(
        backend.sns.get_topic_attributes("arn:aws:sns:us-east-1:000000000000:nope").await.unwrap_err().is_not_found()
    );
}

#[tokio::test]
async fn objects_and_presigned_urls() {
    let (_, backend) = broker();
    assert!(!backend.objects.bucket_exists("b").await.unwrap());
    backend.objects.create_bucket("b").await.unwrap();
    backend.objects.put_object("b", "p/k 1", Bytes::from_static(b"data"), &PutOptions::default()).await.unwrap();
    assert_eq!(&backend.objects.get_object("b", "p/k 1", 10).await.unwrap()[..], b"data");
    assert!(backend.objects.get_object("b", "p/k 1", 2).await.is_err());
    let url = backend.objects.presign_get("b", "p/k 1", Duration::from_secs(60)).await.unwrap();
    assert_eq!(&backend.http.get(&url, 100).await.unwrap()[..], b"data");
    backend.objects.delete_object("b", "p/k 1").await.unwrap();
    assert!(backend.http.get(&url, 100).await.is_err());
    assert!(backend.http.get("https://example.com/x", 100).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn visibility_timeouts_and_delays_are_whole_seconds() {
    // SQS takes whole seconds; like the AWS adapter, the broker rounds fractions up.
    let (broker, backend) = broker();
    let url = backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    backend.sqs.send_message(&url, send("a")).await.unwrap();
    let request = ReceiveRequest { visibility_timeout: Some(Duration::from_millis(500)), ..receive(&url, 1, None) };
    let message = backend.sqs.receive_messages(request.clone()).await.unwrap().remove(0);
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(backend.sqs.receive_messages(request.clone()).await.unwrap().is_empty(), "0.5 s means 1 s");
    tokio::time::sleep(Duration::from_millis(400)).await;
    let message = {
        let again = backend.sqs.receive_messages(request.clone()).await.unwrap();
        assert_eq!(again.len(), 1);
        assert_ne!(again[0].receipt_handle, message.receipt_handle);
        again.into_iter().next().unwrap()
    };
    let change = VisibilityChange { receipt_handle: message.receipt_handle, timeout: Duration::from_millis(1200) };
    backend.sqs.change_visibility_batch(&url, vec![change]).await.unwrap().remove(0).unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(broker.queue_messages(&url).unwrap()[0].in_flight, "1.2 s means 2 s");
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!broker.queue_messages(&url).unwrap()[0].in_flight);

    let url = backend.sqs.create_queue("delayed", &StringMap::new(), &StringMap::new()).await.unwrap();
    backend.sqs.send_message(&url, SendRequest { delay: Some(Duration::from_millis(300)), ..send("b") }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(backend.sqs.receive_messages(receive(&url, 1, Some(5))).await.unwrap().is_empty(), "0.3 s means 1 s");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(backend.sqs.receive_messages(receive(&url, 1, Some(5))).await.unwrap().len(), 1);
}
