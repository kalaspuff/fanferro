use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;

use super::*;
use crate::backend::memory::MemoryBroker;
use crate::backend::testing::{SlowBatches, StuckVisibility};
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

fn receives(engine: &ConsumerEngine) -> i64 {
    engine.stats().metrics["receive_requests"]
}

#[tokio::test(start_paused = true)]
async fn receives_wait_for_a_full_batch() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..30 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|_| {});
    let mut first = collect(&engine, 10).await;
    assert_eq!(receives(&engine), 1);
    let rest = first.split_off(3);
    ack_all(first).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(receives(&engine), 1, "three free slots are not a full batch yet");
    ack_all(rest).await;
    let second = collect(&engine, 10).await;
    assert_eq!(second.len(), 10);
    assert_eq!(receives(&engine), 2, "one receive for the whole batch");
    ack_all(second).await;
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn receives_fall_back_to_the_free_slots_after_the_batch_wait() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..30 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|_| {});
    let mut slow = collect(&engine, 10).await;
    let done = slow.split_off(6);
    let freed_at = tokio::time::Instant::now();
    ack_all(done).await;
    // Six handlers are slow; the four free slots are used after `receive_batch_wait`.
    let next = engine.next_batch(10, Duration::from_secs(5)).await;
    assert_eq!(next.len(), 4);
    let waited = freed_at.elapsed();
    assert!(waited >= Duration::from_millis(100) && waited < Duration::from_millis(150), "{waited:?}");
    assert_eq!(receives(&engine), 2);
    ack_all(slow.into_iter().chain(next).collect()).await;
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn a_minimum_of_one_receives_into_any_free_slot() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..30 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|c| c.min_receive_batch = Some(1));
    let mut first = collect(&engine, 10).await;
    let freed_at = tokio::time::Instant::now();
    ack_all(first.split_off(9)).await;
    let next = engine.next_batch(10, Duration::from_secs(5)).await;
    assert_eq!(next.len(), 1);
    assert!(freed_at.elapsed() < Duration::from_millis(20));
    ack_all(first.into_iter().chain(next).collect()).await;
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn the_minimum_receive_batch_is_capped_by_the_in_flight_limit() {
    let fx = fixture(ClientConfig::default()).await;
    fx.client.events().set_enabled(&[EventKind::ConfigWarning]);
    for i in 0..10 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|c| c.max_in_flight = 3);
    assert!(fx.client.events().drain(10).is_empty(), "the default minimum is capped silently");
    let first = collect(&engine, 3).await;
    let freed_at = tokio::time::Instant::now();
    ack_all(first).await;
    let next = engine.next_batch(10, Duration::from_secs(5)).await;
    assert_eq!(next.len(), 3);
    assert!(freed_at.elapsed() < Duration::from_millis(20), "three slots are a full batch here");
    ack_all(next).await;
    engine.close(true).await;
    // An explicit minimum above the limit is capped too, with a warning.
    let engine = fx.engine(|c| {
        c.max_in_flight = 3;
        c.min_receive_batch = Some(8);
    });
    let warnings = fx.client.events().drain(10);
    assert!(warnings.iter().any(|e| e.kind == EventKind::ConfigWarning && e.message.contains("min_receive_batch")));
    engine.close(true).await;
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

/// Acks every delivery concurrently and returns how long the slowest ack took.
async fn ack_all(deliveries: Vec<Delivery>) -> Duration {
    let started = tokio::time::Instant::now();
    let acks = deliveries.into_iter().map(|delivery| async move { delivery.lease.ack().await.unwrap() });
    futures::future::join_all(acks).await;
    started.elapsed()
}

#[tokio::test(start_paused = true)]
async fn acks_within_the_linger_share_one_call() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..10 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|_| {});
    let deliveries = collect(&engine, 10).await;
    let elapsed = ack_all(deliveries).await;
    assert_eq!(engine.stats().metrics["delete_requests"], 1);
    assert!(elapsed < Duration::from_millis(5), "a full batch is sent without lingering ({elapsed:?})");
    engine.close(true).await;
    assert!(fx.stored().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_single_ack_is_sent_after_the_linger() {
    let fx = fixture(ClientConfig::default()).await;
    fx.send("alone").await;
    let engine = fx.engine(|_| {});
    let deliveries = collect(&engine, 1).await;
    let elapsed = ack_all(deliveries).await;
    assert_eq!(engine.stats().metrics["delete_requests"], 1);
    assert!(elapsed >= Duration::from_millis(5) && elapsed < Duration::from_millis(10), "{elapsed:?}");
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn a_burst_of_acks_is_sent_in_full_batches() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..25 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|c| c.max_in_flight = 25);
    let deliveries = collect(&engine, 25).await;
    ack_all(deliveries).await;
    assert_eq!(engine.stats().metrics["delete_requests"], 3);
    engine.close(true).await;
    assert!(fx.stored().is_empty());
}

#[tokio::test(start_paused = true)]
async fn full_batches_do_not_wait_for_calls_in_flight() {
    let broker = MemoryBroker::new("us-east-1", "000000000000");
    let mut backend = broker.backend();
    let slow = Arc::new(SlowBatches::new(backend.sqs.clone(), Duration::from_millis(100)));
    backend.sqs = slow.clone();
    let client = Client::with_backend(backend, ClientConfig::default()).unwrap();
    let url = client.backend().sqs.create_queue("work", &StringMap::new(), &StringMap::new()).await.unwrap();
    let fx = Fixture { broker, client, url };
    for i in 0..20 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|c| c.max_in_flight = 20);
    let mut first = collect(&engine, 20).await;
    let second = first.split_off(10);
    let first = tokio::spawn(ack_all(first));
    tokio::time::sleep(Duration::from_millis(10)).await;
    // The second batch is sent while the first call is still in flight.
    let elapsed = ack_all(second).await;
    assert!(elapsed < Duration::from_millis(150), "{elapsed:?}");
    first.await.unwrap();
    assert_eq!(slow.deletes.take_peak(), 2);
    assert_eq!(engine.stats().metrics["delete_requests"], 2);
    engine.close(true).await;
    assert!(fx.stored().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_partial_batch_waits_for_more_instead_of_going_with_a_full_one() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..20 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|c| {
        c.max_in_flight = 20;
        c.settle.linger = 1.0;
    });
    let mut deliveries = collect(&engine, 20).await;
    let rest = deliveries.split_off(13);
    let started = tokio::time::Instant::now();
    let first = tokio::spawn(ack_all(deliveries));
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(engine.stats().metrics["delete_requests"], 1, "ten went at once, three wait for more");
    ack_all(rest).await;
    first.await.unwrap();
    assert!(started.elapsed() < Duration::from_millis(100), "no batch waited for the linger");
    assert_eq!(engine.stats().metrics["delete_requests"], 2);
    engine.close(true).await;
    assert!(fx.stored().is_empty());
}

#[tokio::test(start_paused = true)]
async fn deletes_and_visibility_changes_use_separate_calls() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..12 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|c| {
        c.max_in_flight = 12;
        c.heartbeat.enabled = false;
    });
    let deliveries = collect(&engine, 12).await;
    let settlements = deliveries.iter().enumerate().map(|(index, delivery)| async move {
        if index % 2 == 0 { delivery.lease.ack().await } else { delivery.lease.nack(Duration::from_secs(60)).await }
    });
    for result in futures::future::join_all(settlements).await {
        result.unwrap();
    }
    let metrics = engine.stats().metrics;
    assert_eq!(metrics["delete_requests"], 1);
    assert_eq!(metrics["visibility_requests"], 1);
    assert_eq!(fx.stored().len(), 6);
    engine.close(false).await;
}

/// Acks `deliveries` the way waiting handlers do: each marked as awaited, the last one with the
/// number of handlers that wait. Returns how long the acks took.
async fn awaited_acks(deliveries: &[Delivery], waiting_handlers: usize) -> Duration {
    let started = tokio::time::Instant::now();
    let acks = deliveries.iter().enumerate().map(|(index, delivery)| {
        let last = index + 1 == deliveries.len();
        delivery.lease.mark_awaited(last.then_some(waiting_handlers));
        delivery.lease.ack()
    });
    for result in futures::future::join_all(acks).await {
        result.unwrap();
    }
    started.elapsed()
}

#[tokio::test(start_paused = true)]
async fn a_batch_every_handler_waits_for_is_sent_at_once() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..6 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|c| c.settle.linger = 1.0);
    let deliveries = collect(&engine, 6).await;
    // Three handlers, all waiting for one of these acks: nothing else can join the batch.
    let elapsed = awaited_acks(&deliveries[..3], 3).await;
    assert!(elapsed < Duration::from_millis(100), "{elapsed:?}");
    assert_eq!(engine.stats().metrics["delete_requests"], 1);
    // Four handlers wait, but one of them waits for a settlement that is not in this batch
    // (sent already): its slot will soon go to a handler that settles more, so the batch waits.
    let elapsed = awaited_acks(&deliveries[3..], 4).await;
    assert!(elapsed >= Duration::from_secs(1), "the batch lingers: {elapsed:?}");
    assert_eq!(engine.stats().metrics["delete_requests"], 2);
    engine.close(true).await;
}

#[tokio::test(start_paused = true)]
async fn background_acks_are_tracked_until_close() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..3 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|c| c.settle.linger = 1.0);
    let deliveries: Vec<Arc<Delivery>> = collect(&engine, 3).await.into_iter().map(Arc::new).collect();
    for delivery in &deliveries {
        delivery.ack_in_background().unwrap();
    }
    assert!(deliveries[0].ack_in_background().is_err(), "a second settlement is refused");
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(engine.stats().metrics["settling"], 3, "the deletes linger in the settle loop");
    // Closing waits for the background deletes instead of dropping them.
    engine.close(true).await;
    assert_eq!(engine.stats().metrics["messages_acked"], 3);
    assert!(fx.stored().is_empty());
}

#[tokio::test(start_paused = true)]
async fn background_acks_wait_for_a_settling_slot() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..3 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|c| {
        c.max_settling = Some(1);
        c.settle.linger = 0.5;
    });
    let deliveries: Vec<Arc<Delivery>> = collect(&engine, 3).await.into_iter().map(Arc::new).collect();
    let outcomes = Arc::new(parking_lot::Mutex::new(Vec::new()));
    for delivery in &deliveries {
        let outcomes = outcomes.clone();
        delivery.spawn_ack(move |result| outcomes.lock().push(result.is_ok()));
    }
    // One settling slot: the first delete is queued, the others wait (in tracked tasks).
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(engine.stats().metrics["settling"], 1);
    assert!(outcomes.lock().is_empty());
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(*outcomes.lock(), vec![true, true, true]);
    assert_eq!(engine.stats().metrics["delete_requests"], 3);
    engine.close(true).await;
    assert!(fx.stored().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_received_batch_is_handed_out_together() {
    let fx = fixture(ClientConfig::default()).await;
    for i in 0..7 {
        fx.send(&format!("m{i}")).await;
    }
    let engine = fx.engine(|_| {});
    // One receive returned all seven; they arrive in one next_batch call.
    let deliveries = engine.next_batch(10, Duration::from_secs(5)).await;
    assert_eq!(deliveries.len(), 7);
    ack_all(deliveries).await;
    // A smaller max keeps the rest of a batch for the next call.
    for i in 0..4 {
        fx.send(&format!("n{i}")).await;
    }
    let first = engine.next_batch(3, Duration::from_secs(5)).await;
    let second = engine.next_batch(3, Duration::from_millis(1)).await;
    assert_eq!((first.len(), second.len()), (3, 1));
    assert_eq!(engine.stats().metrics["buffered"], 0);
    ack_all(first.into_iter().chain(second).collect()).await;
    engine.close(true).await;
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
async fn a_stuck_extension_call_does_not_stall_heartbeats() {
    // The first ChangeMessageVisibilityBatch never answers (the SDK has no read timeout by default):
    // the heartbeat gives up on it and extends the lease again instead of waiting forever.
    let broker = MemoryBroker::new("us-east-1", "000000000000");
    let mut backend = broker.backend();
    backend.sqs = std::sync::Arc::new(StuckVisibility::new(backend.sqs.clone(), 1));
    let client = Client::with_backend(backend, ClientConfig::default()).unwrap();
    let url = client.backend().sqs.create_queue("work", &StringMap::new(), &StringMap::new()).await.unwrap();
    let fx = Fixture { broker, client, url };
    fx.send("slow").await;
    let engine = fx.engine(|c| {
        c.visibility_timeout = 3.0;
        c.heartbeat.interval = Some(1.0);
    });
    let delivery = collect(&engine, 1).await.remove(0);
    tokio::time::sleep(Duration::from_secs(20)).await;
    let stored = fx.stored();
    assert!(stored[0].in_flight);
    assert_eq!(stored[0].receive_count, 1, "the message became visible while its lease was held");
    let metrics = engine.stats().metrics;
    assert!(metrics["lease_extension_failures"] >= 1);
    assert!(metrics["lease_extensions"] >= 10);
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
async fn an_abort_during_a_graceful_stop_still_guards_against_the_orphaned_poll() {
    // A second stop request (a second signal) aborts the long poll a graceful stop waits for: the
    // queue keeps serving that poll, so messages released afterwards must stay invisible until it
    // is over, as when the stop aborts right away.
    let broker = MemoryBroker::new("us-east-1", "000000000000");
    let mut backend = broker.backend();
    backend.kind = crate::backend::BackendKind::Aws;
    let client = Client::with_backend(backend, ClientConfig::default()).unwrap();
    let url = client.backend().sqs.create_queue("work", &StringMap::new(), &StringMap::new()).await.unwrap();
    let fx = Fixture { broker, client, url };
    fx.send("held").await;
    let engine = fx.engine(|c| {
        c.wait_time = 20.0;
        c.min_receive_batch = Some(1);
    });
    let delivery = collect(&engine, 1).await.remove(0);
    tokio::time::sleep(Duration::from_millis(100)).await; // the next long poll is in flight
    let graceful = tokio::spawn({
        let engine = engine.clone();
        async move { engine.stop_receiving(false).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    engine.stop_receiving(true).await;
    graceful.await.unwrap();
    delivery.lease.release().await.unwrap();
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert!(fx.stored()[0].in_flight, "released into the window of the aborted poll");
    tokio::time::sleep(Duration::from_secs(15)).await;
    assert!(!fx.stored()[0].in_flight);
    engine.close(true).await;
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
async fn envelopes_are_unwrapped_while_decoding() {
    use crate::envelope::{CloudEventsJson, Envelope, EnvelopeSet, RawEnvelope};
    use crate::types::Payload;

    let fx = fixture(ClientConfig::default()).await;
    let payload = Payload {
        data: Bytes::from_static(br#"{"n":1}"#),
        content_type: Some("application/json".into()),
        message_type: Some("orders.created".into()),
        id: Some("m-1".into()),
        ..Payload::default()
    };
    let send = |wire: WireMessage| {
        let request = SendRequest {
            body: String::from_utf8(wire.body.to_vec()).unwrap(),
            attributes: wire.attributes,
            ..SendRequest::default()
        };
        fx.client.backend().sqs.send_message(&fx.url, request)
    };
    send(CloudEventsJson::default().wrap(&payload).unwrap()).await.unwrap();
    send(RawEnvelope::default().wrap(&payload).unwrap()).await.unwrap();
    let mut broken = CloudEventsJson::default().wrap(&payload).unwrap();
    broken.body = Bytes::from_static(br#"{"specversion":"1.0"}"#);
    send(broken).await.unwrap();

    let mut config = ConsumerConfig::new(fx.url.clone());
    config.wait_time = 1.0;
    let envelopes = EnvelopeSet::default_set();
    let engine = ConsumerEngine::with_envelopes(&fx.client, config, Some(envelopes)).unwrap();
    engine.start().unwrap();
    let deliveries = collect(&engine, 3).await;
    let mut seen = Vec::new();
    for delivery in &deliveries {
        match &delivery.unwrapped {
            Unwrapped::Payload { payload: unwrapped, envelope } => {
                assert_eq!(unwrapped.data, payload.data);
                assert_eq!(unwrapped.id, payload.id);
                assert_eq!(unwrapped.message_type, payload.message_type);
                seen.push(*envelope);
            }
            Unwrapped::Failed(error) => {
                assert!(matches!(error, Error::Envelope(_)), "{error}");
                seen.push(usize::MAX);
            }
            other => panic!("unexpected outcome {other:?}"),
        }
        delivery.lease.ack().await.unwrap();
    }
    seen.sort_unstable();
    assert_eq!(seen, [0, 2, usize::MAX], "CloudEvents, raw and a malformed CloudEvent");
    engine.close(true).await;

    // Without a matching envelope, and without envelopes at all.
    fx.send("plain").await;
    let mut config = ConsumerConfig::new(fx.url.clone());
    config.wait_time = 1.0;
    let only_cloudevents = EnvelopeSet::new(vec![Arc::new(CloudEventsJson::default())]);
    let engine = ConsumerEngine::with_envelopes(&fx.client, config, Some(only_cloudevents)).unwrap();
    engine.start().unwrap();
    let delivery = collect(&engine, 1).await.remove(0);
    assert!(matches!(&delivery.unwrapped, Unwrapped::Unrecognised { tried } if tried == "cloudevents"));
    delivery.lease.release().await.unwrap();
    engine.close(true).await;
    let engine = fx.engine(|_| {});
    let delivery = collect(&engine, 1).await.remove(0);
    assert!(matches!(delivery.unwrapped, Unwrapped::NotAttempted));
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
