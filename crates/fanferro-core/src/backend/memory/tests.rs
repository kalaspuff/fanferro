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

    // A standard queue deletes the message with the first receipt handle too (docs/parity M2).
    let results = backend.sqs.delete_message_batch(&url, vec![messages[0].receipt_handle.clone()]).await.unwrap();
    assert!(results[0].is_ok());
    assert!(backend.sqs.receive_messages(receive(&url, 10, Some(5))).await.unwrap().is_empty());
    tokio::time::advance(Duration::from_secs(6)).await;
    assert!(backend.sqs.receive_messages(receive(&url, 10, Some(5))).await.unwrap().is_empty());
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
    let again = backend.sqs.receive_messages(receive(&url, 1, Some(5))).await.unwrap();
    assert_eq!(again.len(), 1);
    // A standard queue applies the handle of the earlier receive too (docs/parity M2).
    let results = backend
        .sqs
        .change_visibility_batch(
            &url,
            vec![VisibilityChange { receipt_handle: message.receipt_handle, timeout: Duration::ZERO }],
        )
        .await
        .unwrap();
    assert!(results[0].is_ok());
    assert_eq!(backend.sqs.receive_messages(receive(&url, 1, Some(5))).await.unwrap().len(), 1);
}

/// One visibility change, as the batch call's only entry.
async fn change_visibility(
    backend: &Backend,
    url: &str,
    handle: &str,
    seconds: u64,
) -> std::result::Result<(), BatchEntryError> {
    let change = VisibilityChange { receipt_handle: handle.into(), timeout: Duration::from_secs(seconds) };
    backend.sqs.change_visibility_batch(url, vec![change]).await.unwrap().remove(0)
}

async fn delete(backend: &Backend, url: &str, handle: &str) -> std::result::Result<(), BatchEntryError> {
    backend.sqs.delete_message_batch(url, vec![handle.into()]).await.unwrap().remove(0)
}

fn entry(result: std::result::Result<(), BatchEntryError>) -> (String, String) {
    let error = result.expect_err("the entry should fail");
    (error.code, error.message.unwrap_or_default())
}

#[tokio::test(start_paused = true)]
async fn fifo_queues_expire_the_handle_of_a_released_message() {
    // docs/parity M6: SQS answered a visibility change for a FIFO message that is visible again,
    // with its latest handle, with "The receipt handle has expired"; the broker said
    // MessageNotInflight.
    let (broker, backend) = broker();
    let url =
        backend.sqs.create_queue("q.fifo", &attributes(&[("FifoQueue", "true")]), &StringMap::new()).await.unwrap();
    let request = SendRequest { group_id: Some("g".into()), deduplication_id: Some("d".into()), ..send("a") };
    backend.sqs.send_message(&url, request).await.unwrap();
    let message = backend.sqs.receive_messages(receive(&url, 1, Some(30))).await.unwrap().remove(0);
    change_visibility(&backend, &url, &message.receipt_handle, 0).await.unwrap();
    assert_eq!(
        entry(change_visibility(&backend, &url, &message.receipt_handle, 10).await),
        ("ReceiptHandleIsInvalid".to_string(), "The receipt handle has expired".to_string())
    );
    assert!(!broker.queue_messages(&url).unwrap()[0].in_flight, "the message stays visible");
}

#[tokio::test(start_paused = true)]
async fn fifo_queues_refuse_receipt_handles_of_earlier_receives() {
    // docs/parity M3: the broker refused them with other codes and texts.
    let (broker, backend) = broker();
    let fifo = StringMap::from([("FifoQueue".to_string(), "true".to_string())]);
    let url = backend.sqs.create_queue("q.fifo", &fifo, &StringMap::new()).await.unwrap();
    let request = SendRequest { group_id: Some("g".into()), deduplication_id: Some("d".into()), ..send("a") };
    backend.sqs.send_message(&url, request).await.unwrap();
    let first = backend.sqs.receive_messages(receive(&url, 1, Some(2))).await.unwrap().remove(0);
    tokio::time::advance(Duration::from_secs(3)).await;
    let second = backend.sqs.receive_messages(receive(&url, 1, Some(30))).await.unwrap().remove(0);
    let expired = ("ReceiptHandleIsInvalid".to_string(), "The receipt handle has expired".to_string());
    assert_eq!(entry(delete(&backend, &url, &first.receipt_handle).await), expired);
    assert_eq!(entry(change_visibility(&backend, &url, &first.receipt_handle, 0).await), expired);
    assert!(broker.queue_messages(&url).unwrap()[0].in_flight, "the message stays received");
    delete(&backend, &url, &second.receipt_handle).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn receipt_handles_fail_like_sqs() {
    // docs/parity M4, M5, M7: deleted messages, handles that are none, and the visibility limits.
    let (_, backend) = broker();
    let url = backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    backend.sqs.send_message(&url, send("a")).await.unwrap();
    let message = backend.sqs.receive_messages(receive(&url, 1, Some(30))).await.unwrap().remove(0);
    let handle = message.receipt_handle.clone();
    assert_eq!(
        entry(change_visibility(&backend, &url, &handle, 43_201).await),
        (
            "InvalidParameterValue".to_string(),
            "Value 43201 for parameter VisibilityTimeout is invalid. Reason: VisibilityTimeout must be an integer between 0 and 43200.".to_string()
        )
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    // The visibility ends at most 12 hours after the receive.
    assert_eq!(
        entry(change_visibility(&backend, &url, &handle, 43_200).await),
        (
            "InvalidParameterValue".to_string(),
            "Value 43200 for parameter VisibilityTimeout is invalid. Reason: Total VisibilityTimeout for the message is beyond the limit [43200 seconds].".to_string()
        )
    );
    change_visibility(&backend, &url, &handle, 43_199).await.unwrap();
    assert_eq!(
        entry(delete(&backend, &url, "garbage").await),
        (
            "ReceiptHandleIsInvalid".to_string(),
            "The input receipt handle \"garbage\" is not a valid receipt handle.".to_string()
        )
    );
    assert_eq!(
        entry(change_visibility(&backend, &url, "garbage", 10).await),
        (
            "ReceiptHandleIsInvalid".to_string(),
            "The input receipt handle \"garbage\" is not a valid receipt handle.".to_string()
        )
    );
    delete(&backend, &url, &handle).await.unwrap();
    delete(&backend, &url, &handle).await.unwrap();
    assert_eq!(
        entry(change_visibility(&backend, &url, &handle, 10).await),
        (
            "InvalidParameterValue".to_string(),
            format!(
                "Value {handle} for parameter ReceiptHandle is invalid. Reason: Message does not exist or is not available for visibility timeout change."
            )
        )
    );
}

#[tokio::test(start_paused = true)]
async fn a_released_message_is_hidden_again_by_its_receipt_handle() {
    // docs/parity M6: a standard queue changes the visibility of a message that is visible again
    // (the broker answered MessageNotInflight).
    let (broker, backend) = broker();
    let url = backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    backend.sqs.send_message(&url, send("a")).await.unwrap();
    let message = backend.sqs.receive_messages(receive(&url, 1, Some(30))).await.unwrap().remove(0);
    change_visibility(&backend, &url, &message.receipt_handle, 0).await.unwrap();
    assert!(!broker.queue_messages(&url).unwrap()[0].in_flight);
    change_visibility(&backend, &url, &message.receipt_handle, 20).await.unwrap();
    assert!(broker.queue_messages(&url).unwrap()[0].in_flight);
    assert!(backend.sqs.receive_messages(receive(&url, 1, Some(30))).await.unwrap().is_empty());
    let attributes = backend.sqs.get_queue_attributes(&url).await.unwrap();
    assert_eq!(attributes["ApproximateNumberOfMessagesNotVisible"], "1");
    tokio::time::advance(Duration::from_secs(21)).await;
    assert_eq!(backend.sqs.receive_messages(receive(&url, 1, Some(30))).await.unwrap().len(), 1);
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
    // SQS counts receives across all queues: two in the source queue, one in the dead-letter queue.
    assert_eq!(moved[0].system.approximate_receive_count, 3);
    assert!(moved[0].system.approximate_first_receive_timestamp.is_some());
    assert_eq!(broker.queue_messages(&url).unwrap().len(), 0);
}

/// Receives a message of `url` `count` times (with a visibility timeout of 1 s).
async fn receive_times(backend: &Backend, url: &str, count: usize) {
    for _ in 0..count {
        assert_eq!(backend.sqs.receive_messages(receive(url, 10, Some(1))).await.unwrap().len(), 1);
        tokio::time::advance(Duration::from_secs(2)).await;
    }
}

fn redrive_policy(target_arn: &str, max_receive_count: &str) -> StringMap {
    StringMap::from([(
        "RedrivePolicy".to_string(),
        format!(r#"{{"deadLetterTargetArn":"{target_arn}","maxReceiveCount":{max_receive_count}}}"#),
    )])
}

#[tokio::test(start_paused = true)]
async fn redrive_policies_are_validated_like_sqs() {
    // Policies SQS refuses were accepted: a mistyped target made exhausted messages vanish, and a
    // maxReceiveCount of 0 moved every message on its first receive.
    let (_, backend) = broker();
    let fifo = StringMap::from([("FifoQueue".to_string(), "true".to_string())]);
    let dlq_url = backend.sqs.create_queue("dlq", &StringMap::new(), &StringMap::new()).await.unwrap();
    let dlq_arn = backend.sqs.get_queue_attributes(&dlq_url).await.unwrap()["QueueArn"].clone();
    let fifo_dlq_url = backend.sqs.create_queue("dlq.fifo", &fifo, &StringMap::new()).await.unwrap();
    let fifo_dlq_arn = backend.sqs.get_queue_attributes(&fifo_dlq_url).await.unwrap()["QueueArn"].clone();
    let missing = dlq_arn.replace(":dlq", ":dlx");
    for (name, policy, reason) in [
        ("q", redrive_policy(&missing, "3"), "Dead letter target does not exist."),
        ("q", redrive_policy(&dlq_arn, "0"), "Invalid value for maxReceiveCount: 0"),
        ("q", redrive_policy(&dlq_arn, r#""1001""#), "Invalid value for maxReceiveCount: \"1001\""),
        (
            "q",
            StringMap::from([("RedrivePolicy".to_string(), format!(r#"{{"deadLetterTargetArn":"{dlq_arn}"}}"#))]),
            "maxReceiveCount is missing",
        ),
        ("q", redrive_policy(&fifo_dlq_arn, "3"), "same type of queue as the source"),
        ("q.fifo", redrive_policy(&dlq_arn, "3"), "same type of queue as the source"),
    ] {
        let mut attributes = policy.clone();
        if name.ends_with(".fifo") {
            attributes.extend(fifo.clone());
        }
        let err = backend.sqs.create_queue(name, &attributes, &StringMap::new()).await.unwrap_err();
        assert_eq!(err.aws().and_then(|aws| aws.code.as_deref()), Some("InvalidParameterValue"), "{err}");
        assert!(err.to_string().contains(reason), "{err}");
        // Setting it on an existing queue is refused the same way.
        let url = backend.sqs.create_queue(name, &fifo_if(name), &StringMap::new()).await.unwrap();
        let err = backend.sqs.set_queue_attributes(&url, &policy).await.unwrap_err();
        assert!(err.to_string().contains(reason), "{err}");
        backend.sqs.delete_queue(&url).await.unwrap();
        // SQS refuses to create a queue of the same name for 60 seconds.
        tokio::time::advance(Duration::from_secs(60)).await;
    }
    for count in ["1", r#""1000""#] {
        backend.sqs.create_queue("valid", &redrive_policy(&dlq_arn, count), &StringMap::new()).await.unwrap();
        let url = backend.sqs.get_queue_url("valid", None).await.unwrap();
        backend.sqs.delete_queue(&url).await.unwrap();
        tokio::time::advance(Duration::from_secs(60)).await;
    }
}

fn fifo_if(name: &str) -> StringMap {
    if name.ends_with(".fifo") {
        StringMap::from([("FifoQueue".to_string(), "true".to_string())])
    } else {
        StringMap::new()
    }
}

#[tokio::test(start_paused = true)]
async fn redrive_keeps_messages_it_cannot_move() {
    // Exhausted messages were removed before the move, so a dead-letter queue deleted first (during
    // a teardown) or one refusing the message (a smaller MaximumMessageSize) lost them.
    let (broker, backend) = broker();
    let small = StringMap::from([("MaximumMessageSize".to_string(), "1024".to_string())]);
    let dlq_url = backend.sqs.create_queue("dlq", &small, &StringMap::new()).await.unwrap();
    let dlq_arn = backend.sqs.get_queue_attributes(&dlq_url).await.unwrap()["QueueArn"].clone();
    let url = backend.sqs.create_queue("q", &redrive_policy(&dlq_arn, "1"), &StringMap::new()).await.unwrap();
    let large = "x".repeat(2048);
    backend.sqs.send_message(&url, send(&large)).await.unwrap();
    receive_times(&backend, &url, 1).await;
    // The dead-letter queue refuses it: it stays in the source queue, and is received again.
    let again = backend.sqs.receive_messages(receive(&url, 10, Some(1))).await.unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].system.approximate_receive_count, 2);
    assert!(broker.queue_messages(&dlq_url).unwrap().is_empty());
    backend.sqs.delete_message_batch(&url, vec![again[0].receipt_handle.clone()]).await.unwrap();

    // The dead-letter queue is gone: the message stays in the source queue.
    backend.sqs.delete_queue(&dlq_url).await.unwrap();
    backend.sqs.send_message(&url, send("small")).await.unwrap();
    receive_times(&backend, &url, 1).await;
    let left = backend.sqs.receive_messages(receive(&url, 10, Some(1))).await.unwrap();
    assert_eq!(left.iter().map(|message| message.body.as_str()).collect::<Vec<_>>(), vec!["small"]);
}

#[tokio::test(start_paused = true)]
async fn redrive_to_a_shared_fifo_dead_letter_queue_does_not_deduplicate() {
    // Moves were sent with a deduplication id: the second of two copies with the same id, from
    // two FIFO queues sharing the dead-letter queue, was dropped as a duplicate.
    let (_, backend) = broker();
    let fifo = StringMap::from([("FifoQueue".to_string(), "true".to_string())]);
    let dlq_url = backend.sqs.create_queue("dlq.fifo", &fifo, &StringMap::new()).await.unwrap();
    let dlq_arn = backend.sqs.get_queue_attributes(&dlq_url).await.unwrap()["QueueArn"].clone();
    let mut attributes = redrive_policy(&dlq_arn, "1");
    attributes.extend(fifo);
    for name in ["a.fifo", "b.fifo"] {
        let url = backend.sqs.create_queue(name, &attributes, &StringMap::new()).await.unwrap();
        let request = SendRequest {
            body: name.into(),
            group_id: Some("g".into()),
            deduplication_id: Some("same".into()),
            ..SendRequest::default()
        };
        backend.sqs.send_message(&url, request).await.unwrap();
        receive_times(&backend, &url, 1).await;
        assert!(backend.sqs.receive_messages(receive(&url, 10, Some(1))).await.unwrap().is_empty());
    }
    let mut moved = Vec::new();
    for _ in 0..2 {
        let batch = backend.sqs.receive_messages(receive(&dlq_url, 10, Some(30))).await.unwrap();
        let handles = batch.iter().map(|message| message.receipt_handle.clone()).collect();
        moved.extend(batch.into_iter().map(|message| message.body));
        backend.sqs.delete_message_batch(&dlq_url, handles).await.unwrap();
    }
    assert_eq!(moved, vec!["a.fifo", "b.fifo"]);
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
async fn fifo_deduplication_scope_message_group() {
    // The deduplication id was the key whatever the scope: the same body sent to two groups with
    // content-based deduplication was delivered once, while SQS delivers it to both groups.
    let (_, backend) = broker();
    let fifo = |body: &str, group: &str| SendRequest {
        body: body.into(),
        group_id: Some(group.into()),
        ..SendRequest::default()
    };
    for (scope, delivered) in [("messageGroup", vec!["same", "same"]), ("queue", vec!["same"])] {
        let attributes = StringMap::from([
            ("FifoQueue".to_string(), "true".to_string()),
            ("ContentBasedDeduplication".to_string(), "true".to_string()),
            ("DeduplicationScope".to_string(), scope.to_string()),
        ]);
        let url = backend.sqs.create_queue(&format!("{scope}.fifo"), &attributes, &StringMap::new()).await.unwrap();
        let first = backend.sqs.send_message(&url, fifo("same", "a")).await.unwrap();
        let again = backend.sqs.send_message(&url, fifo("same", "a")).await.unwrap();
        assert_eq!(first.message_id, again.message_id, "{scope}: a duplicate within its group");
        let other = backend.sqs.send_message(&url, fifo("same", "b")).await.unwrap();
        assert_eq!(other.message_id != first.message_id, scope == "messageGroup", "{scope}");
        let received = backend.sqs.receive_messages(receive(&url, 10, Some(30))).await.unwrap();
        assert_eq!(received.iter().map(|m| m.body.as_str()).collect::<Vec<_>>(), delivered, "{scope}");
    }
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

#[tokio::test(start_paused = true)]
async fn long_poll_wakes_when_a_visibility_change_makes_a_message_visible_sooner() {
    // Only a timeout of zero woke waiting long polls: a retry after 2 s reached a 20 s long poll
    // when the poll ended, about 18 s late, while SQS returns the message once it is visible.
    let (_, backend) = broker();
    let url = backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    backend.sqs.send_message(&url, send("retried")).await.unwrap();
    let message = backend.sqs.receive_messages(receive(&url, 1, Some(30))).await.unwrap().remove(0);
    let sqs = backend.sqs.clone();
    let poll_url = url.clone();
    let waiter = tokio::spawn(async move {
        sqs.receive_messages(ReceiveRequest { wait_time: Duration::from_secs(20), ..receive(&poll_url, 10, None) })
            .await
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    let started = Instant::now();
    let results = backend
        .sqs
        .change_visibility_batch(
            &url,
            vec![VisibilityChange { receipt_handle: message.receipt_handle, timeout: Duration::from_secs(2) }],
        )
        .await
        .unwrap();
    assert!(results[0].is_ok());
    let messages = waiter.await.unwrap().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body, "retried");
    assert_eq!(started.elapsed(), Duration::from_secs(2));
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
async fn fifo_queues_cannot_be_subscribed_to_standard_topics() {
    // The broker accepted it and delivered with a made-up message group, while SNS refuses it.
    let (_, backend) = broker();
    let fifo = StringMap::from([("FifoQueue".to_string(), "true".to_string())]);
    let fifo_topic = StringMap::from([("FifoTopic".to_string(), "true".to_string())]);
    let topic = backend.sns.create_topic("events", &StringMap::new(), &StringMap::new()).await.unwrap();
    let topic_fifo = backend.sns.create_topic("events.fifo", &fifo_topic, &StringMap::new()).await.unwrap();
    let queue_url = backend.sqs.create_queue("worker.fifo", &fifo, &StringMap::new()).await.unwrap();
    let queue_arn = backend.sqs.get_queue_attributes(&queue_url).await.unwrap()["QueueArn"].clone();
    let standard_url = backend.sqs.create_queue("worker", &StringMap::new(), &StringMap::new()).await.unwrap();
    let standard_arn = backend.sqs.get_queue_attributes(&standard_url).await.unwrap()["QueueArn"].clone();
    let err = backend.sns.subscribe(&topic, "sqs", &queue_arn, &StringMap::new()).await.unwrap_err();
    assert_eq!(err.aws().unwrap().code.as_deref(), Some("InvalidParameter"));
    assert!(err.to_string().contains("FIFO SQS Queues can not be subscribed to standard SNS topics"), "{err}");
    assert!(backend.sns.list_subscriptions_by_topic(&topic).await.unwrap().is_empty());
    backend.sns.subscribe(&topic_fifo, "sqs", &queue_arn, &StringMap::new()).await.unwrap();
    backend.sns.subscribe(&topic_fifo, "sqs", &standard_arn, &StringMap::new()).await.unwrap();
    backend.sns.subscribe(&topic, "sqs", &standard_arn, &StringMap::new()).await.unwrap();
}

#[tokio::test]
async fn invalid_filter_policies_are_refused() {
    let (broker, backend) = broker();
    let topic = backend.sns.create_topic("events", &StringMap::new(), &StringMap::new()).await.unwrap();
    let url = backend.sqs.create_queue("events", &StringMap::new(), &StringMap::new()).await.unwrap();
    let queue_arn = "arn:aws:sqs:us-east-1:000000000000:events";
    let attributes = |pairs: &[(&str, &str)]| -> StringMap {
        pairs.iter().map(|(name, value)| ((*name).to_string(), (*value).to_string())).collect()
    };
    let error = backend
        .sns
        .subscribe(&topic, "sqs", queue_arn, &attributes(&[("FilterPolicy", r#"{"kind": [{"glob": "a*"}]}"#)]))
        .await
        .unwrap_err();
    // SNS's texts (docs/parity S5).
    assert_eq!(
        code_and_message(&error),
        (
            "InvalidParameter".to_string(),
            "Invalid parameter: Attributes Reason: FilterPolicy: Unrecognized match type glob".to_string()
        )
    );
    let nested =
        attributes(&[("FilterPolicy", r#"{"order": {"status": ["paid"]}}"#), ("FilterPolicyScope", "MessageBody")]);
    let subscription = backend.sns.subscribe(&topic, "sqs", queue_arn, &nested).await.unwrap();
    // The scope applies to the policy as it is: attribute policies cannot be nested.
    let error = backend
        .sns
        .set_subscription_attribute(&subscription, "FilterPolicyScope", "MessageAttributes")
        .await
        .unwrap_err();
    assert_eq!(
        code_and_message(&error).1,
        "Invalid parameter: Filter policy scope MessageAttributes does not support nested filter policy"
    );
    let request = |body: &str| PublishRequest { message: body.into(), ..PublishRequest::default() };
    backend.sns.publish(&topic, request(r#"{"order": {"status": "paid"}}"#)).await.unwrap();
    backend.sns.publish(&topic, request(r#"{"order": {"status": "open"}}"#)).await.unwrap();
    let delivered = broker.queue_messages(&url).unwrap();
    assert_eq!(delivered.len(), 1, "the subscription kept its scope and policy");
    backend.sns.set_subscription_attribute(&subscription, "FilterPolicy", "").await.unwrap();
    backend.sns.publish(&topic, request(r#"{"order": {"status": "open"}}"#)).await.unwrap();
    assert_eq!(broker.queue_messages(&url).unwrap().len(), 2, "without a policy everything is delivered");
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
    let expired = format!("{}?expires=1", url.split_once('?').unwrap().0);
    let error = backend.http.get(&expired, 100).await.unwrap_err().to_string();
    assert!(error.contains("expired at 1970-01-01T00:00:01Z"), "{error}");
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

/// The code and message of an AWS error.
fn code_and_message(error: &Error) -> (String, String) {
    let aws = error.aws().unwrap_or_else(|| panic!("not an AWS error: {error}"));
    (aws.code.clone().unwrap_or_default(), aws.message.clone().unwrap_or_default())
}

fn attributes(pairs: &[(&str, &str)]) -> StringMap {
    pairs.iter().map(|(name, value)| ((*name).to_string(), (*value).to_string())).collect()
}

#[tokio::test(start_paused = true)]
async fn queue_attributes_are_validated_like_sqs() {
    // docs/parity Q1-Q3: the broker accepted values and attributes SQS refuses.
    let (_, backend) = broker();
    let standard = backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    let fifo =
        backend.sqs.create_queue("f.fifo", &attributes(&[("FifoQueue", "true")]), &StringMap::new()).await.unwrap();
    for (name, value) in [
        ("VisibilityTimeout", "43201"),
        ("MessageRetentionPeriod", "59"),
        ("MaximumMessageSize", "1048577"),
        ("MaximumMessageSize", "1023"),
        ("DelaySeconds", "901"),
        ("ReceiveMessageWaitTimeSeconds", "21"),
        ("KmsDataKeyReusePeriodSeconds", "59"),
        ("VisibilityTimeout", "ten"),
        ("SqsManagedSseEnabled", "yes"),
    ] {
        let expected = ("InvalidAttributeValue".to_string(), format!("Invalid value for the parameter {name}."));
        let err = backend.sqs.create_queue("bad", &attributes(&[(name, value)]), &StringMap::new()).await.unwrap_err();
        assert_eq!(code_and_message(&err), expected);
        let err = backend.sqs.set_queue_attributes(&standard, &attributes(&[(name, value)])).await.unwrap_err();
        assert_eq!(code_and_message(&err), expected);
    }
    for (url, name) in [
        (&standard, "NoSuchAttribute"),
        (&standard, "ContentBasedDeduplication"),
        (&standard, "FifoThroughputLimit"),
        (&standard, "FifoQueue"),
        (&fifo, "FifoQueue"),
        (&standard, "QueueArn"),
    ] {
        let expected = ("InvalidAttributeName".to_string(), format!("Unknown Attribute {name}."));
        let err = backend.sqs.set_queue_attributes(url, &attributes(&[(name, "true")])).await.unwrap_err();
        assert_eq!(code_and_message(&err), expected);
        if name != "FifoQueue" {
            let err =
                backend.sqs.create_queue("bad", &attributes(&[(name, "true")]), &StringMap::new()).await.unwrap_err();
            assert_eq!(code_and_message(&err), expected);
        }
    }
    let per_group = attributes(&[("FifoQueue", "true"), ("FifoThroughputLimit", "perMessageGroupId")]);
    let err = backend.sqs.create_queue("g.fifo", &per_group, &StringMap::new()).await.unwrap_err();
    assert_eq!(
        code_and_message(&err),
        (
            "InvalidAttributeValue".to_string(),
            "Invalid value for the parameter FifoThroughputLimit. Reason: To set FifoThroughputLimit to perMessageGroupId, the DeduplicationScope must be messageGroup.".to_string()
        )
    );
    let err =
        backend.sqs.set_queue_attributes(&fifo, &attributes(&[("FifoThroughputLimit", "perMessageGroupId")])).await;
    assert!(err.is_err(), "the queue's DeduplicationScope is still queue");
    let high_throughput =
        attributes(&[("DeduplicationScope", "messageGroup"), ("FifoThroughputLimit", "perMessageGroupId")]);
    backend.sqs.set_queue_attributes(&fifo, &high_throughput).await.unwrap();
    // Nothing was changed by the refused calls.
    let current = backend.sqs.get_queue_attributes(&standard).await.unwrap();
    assert_eq!((current["VisibilityTimeout"].as_str(), current.get("FifoQueue")), ("30", None));
}

#[tokio::test(start_paused = true)]
async fn queue_names_and_existing_queues_are_reported_like_sqs() {
    // docs/parity Q5 and the attribute SQS names when a queue exists with other attributes.
    let (_, backend) = broker();
    let invalid_name = "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length";
    let fifo_name = "The name of a FIFO queue can only include alphanumeric characters, hyphens, or underscores, must end with .fifo suffix and be 1 to 80 in length.";
    let fifo = attributes(&[("FifoQueue", "true")]);
    for (name, attributes, message) in [
        ("bad name", StringMap::new(), invalid_name),
        (&"x".repeat(81), StringMap::new(), invalid_name),
        ("q.fifo", StringMap::new(), invalid_name),
        ("q", fifo.clone(), fifo_name),
        (".fifo", fifo.clone(), fifo_name),
    ] {
        let err = backend.sqs.create_queue(name, &attributes, &StringMap::new()).await.unwrap_err();
        assert_eq!(code_and_message(&err), ("InvalidParameterValue".to_string(), message.to_string()), "{name}");
    }
    backend.sqs.create_queue(&"x".repeat(80), &StringMap::new(), &StringMap::new()).await.unwrap();
    backend.sqs.create_queue("q", &attributes(&[("VisibilityTimeout", "30")]), &StringMap::new()).await.unwrap();
    let other = attributes(&[("DelaySeconds", "0"), ("VisibilityTimeout", "40")]);
    let err = backend.sqs.create_queue("q", &other, &StringMap::new()).await.unwrap_err();
    assert_eq!(
        code_and_message(&err),
        (
            "QueueAlreadyExists".to_string(),
            "A queue already exists with the same name and a different value for attribute VisibilityTimeout"
                .to_string()
        )
    );
}

#[tokio::test(start_paused = true)]
async fn queues_encrypted_with_kms_keys_are_not_encrypted_by_sqs() {
    // docs/parity Q6: SQS reports SqsManagedSseEnabled=false for a queue with a KMS key.
    let (_, backend) = broker();
    let kms = attributes(&[("KmsMasterKeyId", "alias/aws/sqs")]);
    let url = backend.sqs.create_queue("kms", &kms, &StringMap::new()).await.unwrap();
    assert_eq!(backend.sqs.get_queue_attributes(&url).await.unwrap()["SqsManagedSseEnabled"], "false");
    let url = backend.sqs.create_queue("later", &StringMap::new(), &StringMap::new()).await.unwrap();
    assert_eq!(backend.sqs.get_queue_attributes(&url).await.unwrap()["SqsManagedSseEnabled"], "true");
    backend.sqs.set_queue_attributes(&url, &kms).await.unwrap();
    assert_eq!(backend.sqs.get_queue_attributes(&url).await.unwrap()["SqsManagedSseEnabled"], "false");
}

#[tokio::test(start_paused = true)]
async fn queues_are_created_again_and_purged_again_only_after_60_seconds() {
    // docs/parity Q7, Q8: the broker created a deleted queue again, and purged a queue again, at once.
    let (_, backend) = broker();
    let url = backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    backend.sqs.purge_queue(&url).await.unwrap();
    tokio::time::advance(Duration::from_secs(59)).await;
    let err = backend.sqs.purge_queue(&url).await.unwrap_err();
    let aws = err.aws().unwrap();
    assert_eq!(
        (aws.code.as_deref(), aws.status, aws.message.as_deref(), aws.kind),
        (
            Some("AWS.SimpleQueueService.PurgeQueueInProgress"),
            Some(403),
            Some("Only one PurgeQueue operation on q is allowed every 60 seconds."),
            AwsErrorKind::Throttling
        )
    );
    assert!(err.is_retryable());
    tokio::time::advance(Duration::from_secs(1)).await;
    backend.sqs.purge_queue(&url).await.unwrap();

    backend.sqs.delete_queue(&url).await.unwrap();
    tokio::time::advance(Duration::from_secs(59)).await;
    let err = backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap_err();
    let aws = err.aws().unwrap();
    assert_eq!(
        (aws.code.as_deref(), aws.status, aws.message.as_deref(), aws.kind),
        (
            Some("AWS.SimpleQueueService.QueueDeletedRecently"),
            Some(400),
            Some("You must wait 60 seconds after deleting a queue before you can create another with the same name."),
            AwsErrorKind::Throttling
        )
    );
    // Other names are not affected; the name is free again after 60 seconds.
    backend.sqs.create_queue("other", &StringMap::new(), &StringMap::new()).await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
}

#[test]
fn numbers_are_stored_without_leading_and_trailing_zeros() {
    // docs/parity M1 (AWS returned 1.50, 3.0, -0.50, 0.000, .5 and 100 so; the rest follows the rule).
    for (sent, stored) in [
        ("1.50", Some("1.5")),
        ("3.0", Some("3")),
        ("12345678901234567890", Some("12345678901234567890")),
        ("100", Some("100")),
        ("007", Some("7")),
        ("-0.50", Some("-0.5")),
        ("0.000", Some("0")),
        ("-0", Some("0")),
        (".5", Some("0.5")),
        ("+5", Some("5")),
        ("1.0E2", Some("100")),
        ("1e-3", Some("0.001")),
        ("ten", None),
        ("1.2.3", None),
        ("", None),
        ("-", None),
    ] {
        assert_eq!(normalized_number(sent).as_deref(), stored, "{sent}");
    }
}

#[tokio::test(start_paused = true)]
async fn number_attributes_come_back_normalised_with_their_md5() {
    // docs/parity M1: SQS returns `1.50` as `1.5`; SendMessage reports the MD5 of the attributes as
    // sent, a receive the MD5 of the attributes as returned.
    let (_, backend) = broker();
    let url = backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    let mut sent = MessageAttributes::new();
    sent.insert("decimal".into(), AttributeValue::number("1.50"));
    sent.insert("custom".into(), AttributeValue::with_type("Number.int", AttributeData::Text("007".into())).unwrap());
    sent.insert("text".into(), AttributeValue::string("1.50"));
    let output = backend.sqs.send_message(&url, SendRequest { attributes: sent.clone(), ..send("a") }).await.unwrap();
    assert_eq!(output.md5_of_message_attributes.as_deref(), Some(md5_of_attributes(&sent).as_str()));
    let received = backend.sqs.receive_messages(receive(&url, 1, None)).await.unwrap().remove(0);
    let mut stored = sent.clone();
    stored.insert("decimal".into(), AttributeValue::number("1.5"));
    stored.insert("custom".into(), AttributeValue::with_type("Number.int", AttributeData::Text("7".into())).unwrap());
    assert_eq!(received.attributes, stored, "strings are kept as sent");
    assert_eq!(received.md5_of_message_attributes.as_deref(), Some(md5_of_attributes(&stored).as_str()));
}

#[tokio::test(start_paused = true)]
async fn sns_keeps_numbers_as_published_and_sqs_normalises_raw_deliveries() {
    // docs/parity M1, S9: the notification document keeps `3.0`; raw delivery stores it as `3`.
    let (_, backend) = broker();
    let topic = backend.sns.create_topic("t", &StringMap::new(), &StringMap::new()).await.unwrap();
    let raw = backend.sqs.create_queue("raw", &StringMap::new(), &StringMap::new()).await.unwrap();
    let wrapped = backend.sqs.create_queue("wrapped", &StringMap::new(), &StringMap::new()).await.unwrap();
    let raw_delivery = StringMap::from([("RawMessageDelivery".to_string(), "true".to_string())]);
    for (url, attributes) in [(&raw, &raw_delivery), (&wrapped, &StringMap::new())] {
        let arn = backend.sqs.get_queue_attributes(url).await.unwrap()["QueueArn"].clone();
        backend.sns.subscribe(&topic, "sqs", &arn, attributes).await.unwrap();
    }
    let mut attributes = MessageAttributes::new();
    attributes.insert("n".into(), AttributeValue::number("3.0"));
    backend
        .sns
        .publish(&topic, PublishRequest { message: "m".into(), attributes, ..PublishRequest::default() })
        .await
        .unwrap();
    let delivered = backend.sqs.receive_messages(receive(&raw, 1, None)).await.unwrap().remove(0);
    assert_eq!(delivered.attributes.get("n"), Some(&AttributeValue::number("3")));
    let notification = backend.sqs.receive_messages(receive(&wrapped, 1, None)).await.unwrap().remove(0);
    let document: serde_json::Value = serde_json::from_str(&notification.body).unwrap();
    assert_eq!(document["MessageAttributes"]["n"]["Value"], "3.0");
}

#[tokio::test(start_paused = true)]
async fn errors_carry_the_services_statuses_and_texts() {
    // docs/parity Q9, M9, P2: statuses, batch entry texts and other texts as AWS sends them.
    let (_, backend) = broker();
    let err = backend.sqs.get_queue_url("missing", None).await.unwrap_err();
    let aws = err.aws().unwrap();
    assert_eq!(
        (aws.code.as_deref(), aws.status, aws.message.as_deref()),
        (Some("AWS.SimpleQueueService.NonExistentQueue"), Some(400), Some("The specified queue does not exist."))
    );
    let url =
        backend.sqs.create_queue("q", &attributes(&[("MaximumMessageSize", "1024")]), &StringMap::new()).await.unwrap();
    let results =
        backend.sqs.send_message_batch(&url, vec![send("ok"), send(""), send(&"x".repeat(1025))]).await.unwrap();
    assert!(results[0].is_ok());
    let failures: Vec<(String, Option<String>, bool)> =
        results.into_iter().skip(1).map(|r| r.unwrap_err()).map(|e| (e.code, e.message, e.sender_fault)).collect();
    assert_eq!(
        failures,
        [
            (
                "EmptyValue".to_string(),
                Some("No Value Found for SendMessageBatchRequestEntry.2.MessageBody.".to_string()),
                true
            ),
            (
                "InvalidParameterValue".to_string(),
                Some(
                    "One or more parameters are invalid. Reason: Message must be shorter than 1024 bytes.".to_string()
                ),
                true
            ),
        ]
    );
    let err = backend.sqs.send_message(&url, send("bad \u{fffe} char")).await.unwrap_err();
    assert_eq!(
        code_and_message(&err),
        (
            "InvalidMessageContents".to_string(),
            "Invalid binary character '#xFFFE' was found in the message body, the set of allowed characters is #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF].".to_string()
        )
    );
    let topic = backend.sns.create_topic("t", &StringMap::new(), &StringMap::new()).await.unwrap();
    let entries = vec![
        PublishRequest { message: "ok".into(), ..PublishRequest::default() },
        PublishRequest { message: String::new(), ..PublishRequest::default() },
    ];
    let failure = backend.sns.publish_batch(&topic, entries).await.unwrap().remove(1).unwrap_err();
    assert_eq!(
        (failure.code.as_str(), failure.message.as_deref(), failure.sender_fault),
        ("InvalidParameter", Some("Invalid parameter: Empty message"), true)
    );
    let err = backend.http.get("memory://s3/missing-bucket/key", 1024).await.unwrap_err();
    assert_eq!(
        err.to_string(),
        "payload error: payload URL is not accessible (HTTP 404 Not Found); it may have expired"
    );
}

#[tokio::test(start_paused = true)]
async fn fifo_topics_refuse_invalid_ids_instead_of_dropping_the_deliveries() {
    // A FIFO topic accepted an id its FIFO subscribers refuse, and the delivery vanished.
    let (_, backend) = broker();
    let topic =
        backend.sns.create_topic("t.fifo", &attributes(&[("FifoTopic", "true")]), &StringMap::new()).await.unwrap();
    for (group, deduplication) in [("a b", "d"), ("g", &*"d".repeat(129))] {
        let request = PublishRequest {
            message: "m".into(),
            group_id: Some(group.into()),
            deduplication_id: Some(deduplication.into()),
            ..PublishRequest::default()
        };
        let err = backend.sns.publish(&topic, request).await.unwrap_err();
        let (code, message) = code_and_message(&err);
        assert_eq!(code, "InvalidParameter");
        assert!(
            message.ends_with("can only include alphanumeric and punctuation characters. 1 to 128 in length."),
            "{message}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn fifo_ids_are_validated_like_sqs() {
    // docs/parity M8: the broker accepted group and deduplication ids SQS refuses.
    let (_, backend) = broker();
    let url =
        backend.sqs.create_queue("q.fifo", &attributes(&[("FifoQueue", "true")]), &StringMap::new()).await.unwrap();
    for (group, deduplication, parameter, value) in
        [("a b", "d", "MessageGroupId", "a b"), ("g", &*"d".repeat(129), "MessageDeduplicationId", &*"d".repeat(129))]
    {
        let request =
            SendRequest { group_id: Some(group.into()), deduplication_id: Some(deduplication.into()), ..send("x") };
        let err = backend.sqs.send_message(&url, request).await.unwrap_err();
        assert_eq!(
            code_and_message(&err),
            (
                "InvalidParameterValue".to_string(),
                format!(
                    "Value {value} for parameter {parameter} is invalid. Reason: {parameter} can only include alphanumeric and punctuation characters. 1 to 128 in length."
                )
            )
        );
    }
}

#[tokio::test(start_paused = true)]
async fn topics_and_subscriptions_are_validated_and_reported_like_sns() {
    // docs/parity S1-S4: SNS's codes, texts and default attributes.
    let (_, backend) = broker();
    let invalid = |error: Error| code_and_message(&error);
    let parameter = |message: &str| ("InvalidParameter".to_string(), message.to_string());
    let fifo = attributes(&[("FifoTopic", "true")]);
    for (name, attributes) in [("bad name", StringMap::new()), ("t.fifo", StringMap::new()), ("t", fifo.clone())] {
        let err = backend.sns.create_topic(name, &attributes, &StringMap::new()).await.unwrap_err();
        assert_eq!(invalid(err), parameter("Invalid parameter: Topic Name"), "{name}");
    }
    let err = backend
        .sns
        .create_topic("t", &attributes(&[("ContentBasedDeduplication", "true")]), &StringMap::new())
        .await
        .unwrap_err();
    assert_eq!(
        invalid(err),
        parameter("Invalid parameter: Attributes Reason: Unknown attribute ContentBasedDeduplication")
    );
    let err =
        backend.sns.create_topic("t", &attributes(&[("SignatureVersion", "3")]), &StringMap::new()).await.unwrap_err();
    assert_eq!(
        invalid(err),
        parameter("Invalid parameter: Attributes Reason: SignatureVersion: Invalid value '[3]'. Must be 1 or 2.")
    );
    let topic = backend.sns.create_topic("t", &StringMap::new(), &StringMap::new()).await.unwrap();
    let err =
        backend.sns.create_topic("t", &attributes(&[("DisplayName", "Other")]), &StringMap::new()).await.unwrap_err();
    assert_eq!(
        invalid(err),
        parameter("Invalid parameter: Attributes Reason: Topic already exists with different attributes")
    );
    let err = backend.sns.set_topic_attribute(&topic, "SignatureVersion", "3").await.unwrap_err();
    assert_eq!(invalid(err), parameter("Invalid parameter: SignatureVersion: Invalid value '[3]'. Must be 1 or 2."));
    let err = backend.sns.set_topic_attribute(&topic, "FifoTopic", "true").await.unwrap_err();
    assert_eq!(invalid(err), parameter("Invalid parameter: AttributeName"));
    let reported = backend.sns.get_topic_attributes(&topic).await.unwrap();
    let policy: serde_json::Value = serde_json::from_str(&reported["Policy"]).unwrap();
    assert_eq!(policy["Statement"][0]["Resource"], topic.as_str());
    assert!(reported["EffectiveDeliveryPolicy"].contains("defaultHealthyRetryPolicy"));

    let fifo_queue =
        backend.sqs.create_queue("q.fifo", &attributes(&[("FifoQueue", "true")]), &StringMap::new()).await.unwrap();
    let fifo_arn = backend.sqs.get_queue_attributes(&fifo_queue).await.unwrap()["QueueArn"].clone();
    let err = backend.sns.subscribe(&topic, "sqs", &fifo_arn, &StringMap::new()).await.unwrap_err();
    assert_eq!(
        invalid(err),
        parameter(
            "Invalid parameter: Invalid parameter: Endpoint Reason: FIFO SQS Queues can not be subscribed to standard SNS topics"
        )
    );
    let queue = backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    let queue_arn = backend.sqs.get_queue_attributes(&queue).await.unwrap()["QueueArn"].clone();
    let filtered = attributes(&[("FilterPolicy", r#"{"kind": ["a"]}"#)]);
    let subscription = backend.sns.subscribe(&topic, "sqs", &queue_arn, &filtered).await.unwrap();
    let raw = attributes(&[("RawMessageDelivery", "true")]);
    let err = backend.sns.subscribe(&topic, "sqs", &queue_arn, &raw).await.unwrap_err();
    assert_eq!(
        invalid(err),
        parameter("Invalid parameter: Attributes Reason: Subscription already exists with different attributes")
    );
    let reported = backend.sns.get_subscription_attributes(&subscription).await.unwrap();
    assert_eq!(reported["FilterPolicyScope"], "MessageAttributes");
    assert_eq!(reported["SubscriptionPrincipal"], "arn:aws:iam::000000000000:root");

    // A topic ARN of another region (docs/parity S3).
    let elsewhere = topic.replace("us-east-1", "us-west-2");
    let request = PublishRequest { message: "m".into(), ..PublishRequest::default() };
    let err = backend.sns.publish(&elsewhere, request.clone()).await.unwrap_err();
    assert_eq!(invalid(err), parameter("Invalid parameter: TopicArn"));
    let err = backend.sns.publish_batch(&elsewhere, vec![request]).await.unwrap_err();
    assert_eq!(invalid(err), parameter("Invalid parameter: TopicArn"));
}

#[tokio::test(start_paused = true)]
async fn sns_does_not_deliver_to_queues_encrypted_with_the_aws_managed_key() {
    // docs/parity S8: SNS cannot use the AWS managed key of SQS; the broker delivered anyway.
    let (broker, backend) = broker();
    let topic = backend.sns.create_topic("t", &StringMap::new(), &StringMap::new()).await.unwrap();
    let mut urls = Vec::new();
    for (name, attributes) in [
        ("plain", StringMap::new()),
        ("managed", attributes(&[("KmsMasterKeyId", "alias/aws/sqs")])),
        ("custom", attributes(&[("KmsMasterKeyId", "alias/payments")])),
    ] {
        let url = backend.sqs.create_queue(name, &attributes, &StringMap::new()).await.unwrap();
        let arn = backend.sqs.get_queue_attributes(&url).await.unwrap()["QueueArn"].clone();
        backend.sns.subscribe(&topic, "sqs", &arn, &StringMap::new()).await.unwrap();
        urls.push(url);
    }
    backend.sns.publish(&topic, PublishRequest { message: "m".into(), ..PublishRequest::default() }).await.unwrap();
    let delivered: Vec<usize> = urls.iter().map(|url| broker.queue_messages(url).unwrap().len()).collect();
    assert_eq!(delivered, [1, 0, 1]);
}

#[tokio::test(start_paused = true)]
async fn active_tracing_gives_deliveries_a_parent_of_sns() {
    // docs/parity S7: with TracingConfig=Active, SNS passes the trace header on with its own parent.
    let (_, backend) = broker();
    let header = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1";
    for (tracing, same) in [("PassThrough", true), ("Active", false)] {
        let topic = backend
            .sns
            .create_topic(tracing, &attributes(&[("TracingConfig", tracing)]), &StringMap::new())
            .await
            .unwrap();
        let url = backend.sqs.create_queue(tracing, &StringMap::new(), &StringMap::new()).await.unwrap();
        let arn = backend.sqs.get_queue_attributes(&url).await.unwrap()["QueueArn"].clone();
        backend.sns.subscribe(&topic, "sqs", &arn, &StringMap::new()).await.unwrap();
        let request =
            PublishRequest { message: "m".into(), trace_header: Some(header.into()), ..PublishRequest::default() };
        backend.sns.publish(&topic, request).await.unwrap();
        let delivered = backend.sqs.receive_messages(receive(&url, 1, None)).await.unwrap().remove(0);
        let received = delivered.system.aws_trace_header.unwrap();
        assert_eq!(received == header, same, "{tracing}: {received}");
        let parts: Vec<&str> = received.split(';').collect();
        assert_eq!((parts[0], parts[2]), ("Root=1-5759e988-bd862e3fe1be46a994272793", "Sampled=1"));
        assert!(parts[1].starts_with("Parent=") && parts[1].len() == "Parent=".len() + 16, "{received}");
    }
}

/// Starts a 20 second long poll and aborts it after a second, as a consumer that stops does.
async fn abort_a_long_poll(backend: &Backend, url: &str) {
    let poll = ReceiveRequest { wait_time: Duration::from_secs(20), ..receive(url, 10, None) };
    let aborted = tokio::time::timeout(Duration::from_secs(1), backend.sqs.receive_messages(poll)).await;
    assert!(aborted.is_err(), "the poll was still waiting");
}

#[tokio::test(start_paused = true)]
async fn aborted_long_polls_take_messages_until_their_wait_time_ends() {
    // docs/parity M10: SQS goes on serving a long poll whose client went away; a message sent
    // meanwhile is received by nobody and comes back after its visibility timeout.
    let broker = MemoryBroker::with_options("us-east-1", "000000000000", MemoryOptions { orphaned_polls: true });
    let backend = broker.backend();
    assert!(backend.orphaned_polls);
    let url =
        backend.sqs.create_queue("q", &attributes(&[("VisibilityTimeout", "5")]), &StringMap::new()).await.unwrap();
    abort_a_long_poll(&backend, &url).await;
    backend.sqs.send_message(&url, send("taken")).await.unwrap();
    assert!(backend.sqs.receive_messages(receive(&url, 10, None)).await.unwrap().is_empty());
    let stored = broker.queue_messages(&url).unwrap();
    assert_eq!((stored[0].receive_count, stored[0].in_flight), (1, true));
    tokio::time::advance(Duration::from_secs(5)).await;
    let received = backend.sqs.receive_messages(receive(&url, 10, None)).await.unwrap();
    assert_eq!(received[0].system.approximate_receive_count, 2);
    // It took one batch; and once its wait time is over, nothing more.
    backend.sqs.delete_message_batch(&url, vec![received[0].receipt_handle.clone()]).await.unwrap();
    abort_a_long_poll(&backend, &url).await;
    tokio::time::advance(Duration::from_secs(19)).await;
    backend.sqs.send_message(&url, send("after")).await.unwrap();
    assert_eq!(backend.sqs.receive_messages(receive(&url, 10, None)).await.unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn aborted_long_polls_end_with_the_connection_by_default() {
    let (broker, backend) = broker();
    assert!(!backend.orphaned_polls && broker.options() == MemoryOptions::default());
    let url = backend.sqs.create_queue("q", &StringMap::new(), &StringMap::new()).await.unwrap();
    abort_a_long_poll(&backend, &url).await;
    backend.sqs.send_message(&url, send("free")).await.unwrap();
    assert_eq!(backend.sqs.receive_messages(receive(&url, 10, None)).await.unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn aborted_long_polls_take_messages_that_become_visible_while_nobody_calls() {
    // A message whose visibility timeout ended while an aborted poll waited went to the next live
    // poll when nothing called the broker before the aborted poll's wait time was over.
    let broker = MemoryBroker::with_options("us-east-1", "000000000000", MemoryOptions { orphaned_polls: true });
    let backend = broker.backend();
    let url =
        backend.sqs.create_queue("q", &attributes(&[("VisibilityTimeout", "30")]), &StringMap::new()).await.unwrap();
    backend.sqs.send_message(&url, send("returns")).await.unwrap();
    assert_eq!(backend.sqs.receive_messages(receive(&url, 10, Some(2))).await.unwrap().len(), 1);
    abort_a_long_poll(&backend, &url).await; // waits until second 20
    tokio::time::advance(Duration::from_secs(25)).await; // visible again at second 2, unobserved
    assert!(backend.sqs.receive_messages(receive(&url, 10, None)).await.unwrap().is_empty());
    let stored = broker.queue_messages(&url).unwrap();
    assert_eq!((stored[0].receive_count, stored[0].in_flight), (2, true), "the aborted poll took it");
    tokio::time::advance(Duration::from_secs(10)).await; // 2 + 30 seconds after the start
    let again = backend.sqs.receive_messages(receive(&url, 10, None)).await.unwrap();
    assert_eq!(again[0].system.approximate_receive_count, 3);
}

#[tokio::test(start_paused = true)]
async fn a_blocked_message_does_not_keep_an_aborted_poll_from_newer_ones() {
    // Group a has a message in flight and one waiting behind it: the aborted poll looked only at the
    // oldest stored message, could not take it, and missed a message of group b sent meanwhile.
    let broker = MemoryBroker::with_options("us-east-1", "000000000000", MemoryOptions { orphaned_polls: true });
    let backend = broker.backend();
    let url =
        backend.sqs.create_queue("q.fifo", &attributes(&[("FifoQueue", "true")]), &StringMap::new()).await.unwrap();
    let in_group = |group: &str, id: &str| SendRequest {
        group_id: Some(group.into()),
        deduplication_id: Some(id.into()),
        ..send(id)
    };
    backend.sqs.send_message(&url, in_group("a", "m1")).await.unwrap();
    backend.sqs.send_message(&url, in_group("a", "m2")).await.unwrap();
    assert_eq!(backend.sqs.receive_messages(receive(&url, 1, Some(60))).await.unwrap()[0].body, "m1");
    abort_a_long_poll(&backend, &url).await;
    tokio::time::advance(Duration::from_secs(2)).await;
    backend.sqs.send_message(&url, in_group("b", "m3")).await.unwrap();
    assert!(backend.sqs.receive_messages(receive(&url, 10, None)).await.unwrap().is_empty());
    let taken: Vec<(String, u32)> = broker
        .queue_messages(&url)
        .unwrap()
        .into_iter()
        .filter(|message| message.in_flight)
        .map(|message| (message.body, message.receive_count))
        .collect();
    assert_eq!(taken, [("m1".to_string(), 1), ("m3".to_string(), 1)], "the aborted poll took m3");
}
