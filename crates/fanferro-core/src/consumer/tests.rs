use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;

use super::*;
use crate::backend::memory::MemoryBroker;
use crate::backend::{SendRequest, StringMap};
use crate::client::ClientConfig;
use crate::events::EventKind;
use crate::publish::{Destination, OutgoingMessage};
use crate::transport::OffloadSettings;
use crate::transport::claim_check::{EncryptionSettings, PointerFormat, ReferenceMode};
use crate::types::{AttributeValue, MessageAttributes, WireMessage};

struct Fixture {
    broker: MemoryBroker,
    client: Client,
    url: String,
}

async fn fixture(config: ClientConfig) -> Fixture {
    let broker = MemoryBroker::new("us-east-1", "000000000000");
    let client = Client::with_backend(broker.backend(), config).unwrap();
    let url = client.backend().sqs.create_queue("work", &StringMap::new(), &StringMap::new()).await.unwrap();
    Fixture { broker, client, url }
}

impl Fixture {
    async fn send(&self, body: &str) {
        self.client
            .backend()
            .sqs
            .send_message(&self.url, SendRequest { body: body.into(), ..SendRequest::default() })
            .await
            .unwrap();
    }

    fn engine(&self, configure: impl FnOnce(&mut ConsumerConfig)) -> ConsumerEngine {
        let mut config = ConsumerConfig::new(self.url.clone());
        config.wait_time = 1.0;
        configure(&mut config);
        let engine = ConsumerEngine::new(&self.client, config).unwrap();
        engine.start().unwrap();
        engine
    }

    fn stored(&self) -> Vec<crate::backend::memory::MessageSnapshot> {
        self.broker.queue_messages(&self.url).unwrap()
    }
}

async fn collect(engine: &ConsumerEngine, count: usize) -> Vec<Delivery> {
    collect_within(engine, count, Duration::from_secs(10)).await
}

async fn collect_within(engine: &ConsumerEngine, count: usize, within: Duration) -> Vec<Delivery> {
    let deadline = tokio::time::Instant::now() + within;
    let mut deliveries = Vec::new();
    while deliveries.len() < count && tokio::time::Instant::now() < deadline {
        deliveries.extend(engine.next_batch(count - deliveries.len(), Duration::from_millis(200)).await);
    }
    deliveries
}

fn body(delivery: &Delivery) -> String {
    String::from_utf8(delivery.decoded.as_ref().unwrap().wire.body.to_vec()).unwrap()
}

#[tokio::test(start_paused = true)]
async fn receives_and_acks() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..3 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|_| {});
    let deliveries = collect(&engine, 3).await;
    assert_eq!(deliveries.len(), 3);
    let mut bodies: Vec<String> = deliveries.iter().map(body).collect();
    bodies.sort();
    assert_eq!(bodies, vec!["m0", "m1", "m2"]);
    for delivery in &deliveries {
        delivery.lease.mark_active();
        delivery.lease.ack().await.unwrap();
        assert!(delivery.lease.ack().await.is_err(), "double settlement must fail");
    }
    assert!(fx.stored().is_empty());
    let stats = engine.stats();
    assert_eq!(stats.metrics["messages_acked"], 3);
    assert_eq!(stats.metrics["in_flight"], 0);
    engine.close(true).await;
    assert_eq!(engine.state(), EngineState::Stopped);
}

#[tokio::test(start_paused = true)]
async fn max_in_flight_bounds_leases() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..5 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|c| c.max_in_flight = 2);
    let first = collect(&engine, 2).await;
    assert_eq!(first.len(), 2);
    assert!(engine.next_batch(10, Duration::from_millis(500)).await.is_empty());
    assert_eq!(fx.stored().iter().filter(|m| m.in_flight).count(), 2);
    for delivery in first {
        delivery.lease.ack().await.unwrap();
    }
    let next = collect(&engine, 2).await;
    assert_eq!(next.len(), 2);
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn settlement_frees_the_in_flight_slot_when_it_starts() {
    let fx = fixture(ClientConfig::default()).await;
    fx.send("a").await;
    fx.send("b").await;
    let engine = fx.engine(|c| {
        c.max_in_flight = 1;
        c.settle.linger = 1.0;
    });
    let first = collect(&engine, 1).await.remove(0);
    let acking = tokio::spawn(async move { first.lease.ack().await });
    // The delete lingers in the settle loop, but the slot is free as soon as the ack started.
    let second = collect_within(&engine, 1, Duration::from_millis(500)).await;
    assert_eq!(second.len(), 1);
    assert!(!acking.is_finished());
    let stats = engine.stats();
    assert_eq!(stats.metrics["settling"], 1);
    assert_eq!(stats.metrics["in_flight"], 2, "a settling message stays leased until the delete completed");
    assert_eq!(stats.leases, 2);
    acking.await.unwrap().unwrap();
    assert_eq!(engine.stats().metrics["settling"], 0);
    second[0].lease.ack().await.unwrap();
    assert_eq!(engine.stats().metrics["in_flight"], 0);
    engine.close(true).await;
    assert!(fx.stored().is_empty());
}

#[tokio::test(start_paused = true)]
async fn settling_backlog_is_bounded() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..3 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|c| {
        c.max_in_flight = 2;
        c.max_settling = Some(1);
        c.settle.linger = 1.0;
    });
    let mut deliveries = collect(&engine, 2).await;
    let second = deliveries.pop().unwrap();
    let first = deliveries.pop().unwrap();
    first.lease.mark_active();
    second.lease.mark_active();
    let first_ack = tokio::spawn(async move { first.lease.ack().await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let second = Arc::new(second);
    let second_ack = tokio::spawn({
        let second = second.clone();
        async move { second.lease.ack().await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The second ack waits for a settling slot and keeps its in-flight slot meanwhile (and its
    // status, so heartbeats still extend it); only the first message's slot was handed back
    // (and refilled with the third message).
    assert_eq!(second.lease.status(), LeaseStatus::Active);
    let stats = engine.stats();
    assert_eq!(stats.metrics["settling"], 1);
    assert_eq!(stats.metrics["in_flight"], 3);
    assert_eq!(stats.available_permits, 0);
    let third = collect(&engine, 1).await.remove(0);
    first_ack.await.unwrap().unwrap();
    second_ack.await.unwrap().unwrap();
    assert_eq!(second.lease.status(), LeaseStatus::Settled);
    third.lease.ack().await.unwrap();
    assert_eq!(engine.stats().metrics["settling"], 0);
    engine.close(true).await;
    assert!(fx.stored().is_empty());
}

#[tokio::test(start_paused = true)]
async fn heartbeats_keep_messages_invisible() {
    let fx = fixture(ClientConfig::default()).await;
    fx.send("slow").await;
    let engine = fx.engine(|c| {
        c.visibility_timeout = 3.0;
        c.heartbeat.interval = Some(1.0);
    });
    let delivery = collect(&engine, 1).await.remove(0);
    tokio::time::sleep(Duration::from_secs(20)).await;
    let stored = fx.stored();
    assert!(stored[0].in_flight);
    assert_eq!(stored[0].receive_count, 1);
    assert!(engine.stats().metrics["lease_extensions"] >= 10);
    assert!(delivery.lease.remaining_visibility() > Duration::ZERO);
    delivery.lease.ack().await.unwrap();
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn without_heartbeats_messages_reappear() {
    let fx = fixture(ClientConfig::default()).await;
    fx.send("slow").await;
    let engine = fx.engine(|c| {
        c.visibility_timeout = 3.0;
        c.heartbeat.enabled = false;
        c.max_in_flight = 2;
    });
    let first = collect(&engine, 1).await.remove(0);
    let second = collect(&engine, 1).await.remove(0);
    assert_eq!(second.message().system.approximate_receive_count, 2);
    // The first receipt handle is stale now.
    assert!(first.lease.ack().await.is_err());
    second.lease.ack().await.unwrap();
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn nack_release_and_extend() {
    let fx = fixture(ClientConfig::default()).await;
    fx.send("retry-me").await;
    let engine = fx.engine(|c| c.heartbeat.enabled = false);
    let delivery = collect(&engine, 1).await.remove(0);
    delivery.lease.nack(Duration::from_secs(5)).await.unwrap();
    let started = tokio::time::Instant::now();
    let again = collect(&engine, 1).await.remove(0);
    assert!(started.elapsed() >= Duration::from_secs(4));
    assert_eq!(again.message().system.approximate_receive_count, 2);
    again.lease.extend(Duration::from_secs(100)).await.unwrap();
    assert!(again.lease.remaining_visibility() > Duration::from_secs(90));
    again.lease.release().await.unwrap();
    let third = collect(&engine, 1).await.remove(0);
    assert_eq!(third.message().system.approximate_receive_count, 3);
    third.lease.ack().await.unwrap();
    assert_eq!(engine.stats().metrics["messages_retried"], 1);
    assert_eq!(engine.stats().metrics["messages_released"], 1);
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn dead_letter_and_requeue() {
    let fx = fixture(ClientConfig::default()).await;
    let dlq = fx.client.backend().sqs.create_queue("work-dlq", &StringMap::new(), &StringMap::new()).await.unwrap();
    let mut attributes = MessageAttributes::new();
    attributes.insert("tenant".into(), AttributeValue::string("acme"));
    fx.client
        .backend()
        .sqs
        .send_message(&fx.url, SendRequest { body: "poison".into(), attributes, ..SendRequest::default() })
        .await
        .unwrap();
    fx.send("flaky").await;
    let engine = fx.engine(|c| c.dead_letter_queue_url = Some(dlq.clone()));
    let deliveries = collect(&engine, 2).await;
    for delivery in deliveries {
        if body(&delivery) == "poison" {
            let details = BTreeMap::from([("error_type".to_string(), "ValueError".to_string())]);
            delivery.lease.dead_letter("handler failed", &details).await.unwrap();
        } else {
            delivery.lease.requeue(Some(Duration::from_secs(2)), 1).await.unwrap();
        }
    }
    let dead = fx.broker.queue_messages(&dlq).unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].body, "poison");
    assert_eq!(dead[0].attributes.get("tenant"), Some(&AttributeValue::string("acme")));
    let info: serde_json::Value =
        serde_json::from_str(dead[0].attributes["fanferro.dead-letter"].as_text().unwrap()).unwrap();
    assert_eq!(info["reason"], "handler failed");
    assert_eq!(info["error_type"], "ValueError");
    let requeued = collect(&engine, 1).await.remove(0);
    assert_eq!(body(&requeued), "flaky");
    assert_eq!(requeued.message().system.approximate_receive_count, 1);
    assert_eq!(
        requeued.decoded.as_ref().unwrap().wire.attributes.get("fanferro.attempt"),
        Some(&AttributeValue::number("1"))
    );
    requeued.lease.ack().await.unwrap();
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn dead_letter_without_queue_fails_cleanly() {
    let fx = fixture(ClientConfig::default()).await;
    fx.send("x").await;
    let engine = fx.engine(|_| {});
    let delivery = collect(&engine, 1).await.remove(0);
    assert!(delivery.lease.dead_letter("no dlq", &BTreeMap::new()).await.is_err());
    // The lease is still usable.
    delivery.lease.ack().await.unwrap();
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn dropped_leases_release_messages() {
    let fx = fixture(ClientConfig::default()).await;
    fx.send("dropped").await;
    let engine = fx.engine(|c| c.heartbeat.enabled = false);
    let delivery = collect(&engine, 1).await.remove(0);
    drop(delivery);
    let again = collect(&engine, 1).await.remove(0);
    assert_eq!(again.message().system.approximate_receive_count, 2);
    assert_eq!(engine.stats().metrics["in_flight"], 1);
    again.lease.ack().await.unwrap();
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn next_batch_returns_as_soon_as_receiving_stops() {
    let fx = fixture(ClientConfig::default()).await;
    for abort in [true, false] {
        let engine = fx.engine(|c| c.wait_time = 20.0);
        let waiting = tokio::spawn({
            let engine = engine.clone();
            async move {
                let started = tokio::time::Instant::now();
                let batch = engine.next_batch(10, Duration::from_secs(60)).await;
                (batch.len(), started.elapsed())
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Without `abort` the long poll in flight keeps `stop_receiving` busy for up to 20 s;
        // the dispatcher must not wait for it.
        let stopping = tokio::spawn({
            let engine = engine.clone();
            async move { engine.stop_receiving(abort).await }
        });
        let (count, elapsed) = waiting.await.unwrap();
        assert_eq!(count, 0);
        assert!(elapsed < Duration::from_millis(200), "abort={abort}: next_batch took {elapsed:?}");
        stopping.await.unwrap();
        engine.close(true).await;
    }
}

#[tokio::test(start_paused = true)]
async fn buffered_deliveries_are_returned_after_receiving_stops() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..3 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|_| {});
    while engine.stats().metrics["buffered"] < 3 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    engine.stop_receiving(true).await;
    let started = tokio::time::Instant::now();
    let deliveries = engine.next_batch(10, Duration::from_secs(5)).await;
    assert_eq!(deliveries.len(), 3);
    assert!(engine.next_batch(10, Duration::from_secs(5)).await.is_empty());
    assert!(started.elapsed() < Duration::from_millis(100));
    for delivery in deliveries {
        delivery.lease.ack().await.unwrap();
    }
    engine.close(true).await;
    assert!(fx.stored().is_empty());
}

#[tokio::test(start_paused = true)]
async fn close_releases_unsettled_messages() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..4 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|c| c.max_in_flight = 4);
    let deliveries = collect(&engine, 2).await;
    engine.stop_receiving(false).await;
    engine.close(true).await;
    assert!(fx.stored().iter().all(|m| !m.in_flight));
    for delivery in deliveries {
        assert!(matches!(delivery.lease.ack().await, Err(Error::Shutdown(_))));
    }
    assert_eq!(engine.stats().metrics["in_flight"], 0);
}

#[tokio::test(start_paused = true)]
async fn close_can_abandon_unsettled_messages() {
    let fx = fixture(ClientConfig::default()).await;
    fx.send("m").await;
    let engine = fx.engine(|c| c.visibility_timeout = 60.0);
    let _delivery = collect(&engine, 1).await;
    engine.close(false).await;
    assert!(fx.stored()[0].in_flight);
}

#[tokio::test(start_paused = true)]
async fn lost_leases_are_detected() {
    let fx = fixture(ClientConfig::default()).await;
    fx.send("contended").await;
    let engine = fx.engine(|c| {
        c.visibility_timeout = 3.0;
        c.heartbeat.interval = Some(1.0);
    });
    let delivery = collect(&engine, 1).await.remove(0);
    // Another consumer steals the message after its visibility is reset externally.
    fx.client
        .backend()
        .sqs
        .change_visibility_batch(
            &fx.url,
            vec![crate::backend::VisibilityChange {
                receipt_handle: delivery.message().receipt_handle.clone(),
                timeout: Duration::ZERO,
            }],
        )
        .await
        .unwrap();
    let stolen = fx
        .client
        .backend()
        .sqs
        .receive_messages(crate::backend::ReceiveRequest {
            queue_url: fx.url.clone(),
            max_messages: 1,
            wait_time: Duration::ZERO,
            visibility_timeout: Some(Duration::from_secs(60)),
            attempt_id: None,
        })
        .await
        .unwrap();
    assert_eq!(stolen.len(), 1);
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(delivery.lease.status(), LeaseStatus::Lost);
    assert_eq!(engine.stats().metrics["leases_lost"], 1);
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn max_lease_expires_heartbeats() {
    let fx = fixture(ClientConfig::default()).await;
    fx.send("long").await;
    let engine = fx.engine(|c| {
        c.visibility_timeout = 3.0;
        c.heartbeat.interval = Some(1.0);
        c.heartbeat.max_lease = Some(5.0);
        // The expired lease keeps its in-flight slot, so the message is not received again.
        c.max_in_flight = 1;
    });
    let delivery = collect(&engine, 1).await.remove(0);
    tokio::time::sleep(Duration::from_secs(12)).await;
    assert_eq!(delivery.lease.status(), LeaseStatus::Expired);
    assert_eq!(engine.stats().metrics["leases_expired"], 1);
    let stored = fx.stored();
    assert!(!stored[0].in_flight, "visibility must not be extended past max_lease");
    assert_eq!(stored[0].receive_count, 1);
    // Settling frees the slot as soon as it starts, and the message is visible again already:
    // the engine may receive it again before the delete lands (like any other consumer could),
    // in which case the old receipt handle is stale.
    let _ = delivery.lease.ack().await;
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn pause_and_resume() {
    let fx = fixture(ClientConfig::default()).await;
    let engine = fx.engine(|_| {});
    engine.pause();
    assert!(engine.is_paused());
    tokio::time::sleep(Duration::from_secs(2)).await;
    fx.send("later").await;
    assert!(engine.next_batch(1, Duration::from_secs(3)).await.is_empty());
    engine.resume();
    let delivery = collect(&engine, 1).await.remove(0);
    delivery.lease.ack().await.unwrap();
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn rate_limit_slows_receiving() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..6 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|c| {
        c.rate_limit = Some(RateLimitConfig { per_second: 2.0, burst: Some(2) });
        c.max_in_flight = 10;
    });
    let started = tokio::time::Instant::now();
    let deliveries = collect(&engine, 6).await;
    assert_eq!(deliveries.len(), 6);
    assert!(started.elapsed() >= Duration::from_millis(1900), "{:?}", started.elapsed());
    for delivery in deliveries {
        delivery.lease.ack().await.unwrap();
    }
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn unwraps_sns_notifications_and_resolves_claim_checks() {
    let offload = OffloadSettings {
        bucket: "payloads".into(),
        key_prefix: "p/".into(),
        threshold: 64,
        always: false,
        reference: ReferenceMode::S3,
        presign_ttl: 60.0,
        format: PointerFormat::Fanferro,
        encryption: EncryptionSettings::BucketDefault,
        storage_class: None,
    };
    let fx = fixture(ClientConfig { offload: Some(offload), ..ClientConfig::default() }).await;
    let backend = fx.client.backend().clone();
    backend.objects.create_bucket("payloads").await.unwrap();
    let topic = backend.sns.create_topic("events", &StringMap::new(), &StringMap::new()).await.unwrap();
    backend.sns.subscribe(&topic, "sqs", "arn:aws:sqs:us-east-1:000000000000:work", &StringMap::new()).await.unwrap();
    let mut attributes = MessageAttributes::new();
    attributes.insert("kind".into(), AttributeValue::string("big"));
    let large = "y".repeat(500);
    fx.client
        .publish(
            &Destination::Topic { arn: topic, fifo: false, content_based_deduplication: false, max_message_size: None },
            OutgoingMessage {
                wire: WireMessage::new(Bytes::from(large.clone()), attributes),
                message_id: "m1".into(),
                ..OutgoingMessage::default()
            },
        )
        .await
        .unwrap();
    let engine = fx.engine(|_| {});
    let delivery = collect(&engine, 1).await.remove(0);
    let decoded = delivery.decoded.as_ref().unwrap();
    assert_eq!(body(&delivery), large);
    assert!(decoded.pointer.is_some());
    assert_eq!(decoded.wire.attributes.get("kind"), Some(&AttributeValue::string("big")));
    assert!(!decoded.wire.attributes.contains_key("fanferro.claim-check"));
    assert!(delivery.sns.as_ref().unwrap().topic_arn.ends_with(":events"));
    delivery.lease.ack().await.unwrap();
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn subscription_confirmations_are_handled_internally() {
    let fx = fixture(ClientConfig::default()).await;
    fx.client.events().set_enabled(&[EventKind::ConfigWarning]);
    fx.send(r#"{"Type":"SubscriptionConfirmation","MessageId":"1","Token":"t","TopicArn":"arn:aws:sns:us-east-1:000000000000:x","Message":"confirm"}"#)
        .await;
    fx.send("regular").await;
    let engine = fx.engine(|_| {});
    let deliveries = collect(&engine, 1).await;
    assert_eq!(body(&deliveries[0]), "regular");
    deliveries[0].lease.ack().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(fx.stored().is_empty());
    let events = fx.client.events().drain(10);
    assert!(events.iter().any(|e| e.kind == EventKind::ConfigWarning));
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn decode_failures_are_delivered_with_errors() {
    let fx = fixture(ClientConfig::default()).await;
    let mut attributes = MessageAttributes::new();
    attributes.insert("fanferro.encoding".into(), AttributeValue::string("base64"));
    fx.client
        .backend()
        .sqs
        .send_message(&fx.url, SendRequest { body: "%%% not base64".into(), attributes, ..SendRequest::default() })
        .await
        .unwrap();
    let engine = fx.engine(|_| {});
    let delivery = collect(&engine, 1).await.remove(0);
    assert!(delivery.decoded.is_err());
    assert_eq!(engine.stats().metrics["decode_failures"], 1);
    delivery.lease.ack().await.unwrap();
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn receive_errors_back_off_and_recover() {
    let fx = fixture(ClientConfig::default()).await;
    fx.client.events().set_enabled(&[EventKind::ReceiveFailed]);
    let missing = "memory://us-east-1/000000000000/not-yet".to_string();
    let mut config = ConsumerConfig::new(missing.clone());
    config.wait_time = 1.0;
    let engine = ConsumerEngine::new(&fx.client, config).unwrap();
    engine.start().unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(engine.stats().metrics["receive_errors"] >= 2);
    fx.client.backend().sqs.create_queue("not-yet", &StringMap::new(), &StringMap::new()).await.unwrap();
    fx.client
        .backend()
        .sqs
        .send_message(&missing, SendRequest { body: "finally".into(), ..SendRequest::default() })
        .await
        .unwrap();
    let delivery = collect_within(&engine, 1, Duration::from_secs(120)).await.remove(0);
    assert_eq!(body(&delivery), "finally");
    delivery.lease.ack().await.unwrap();
    assert!(fx.client.events().drain(100).iter().any(|e| e.kind == EventKind::ReceiveFailed));
    engine.close(true).await;
}
