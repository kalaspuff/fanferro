//! Lock-free counters exposed as on-demand snapshots.
//!
//! The Python layer reads snapshots to serve `client.metrics()` / `consumer.stats()` and
//! to feed OpenTelemetry observable instruments, so metrics can be pulled on demand or
//! exported continuously without the core depending on an OpenTelemetry SDK.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// A monotonically increasing counter.
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    /// Adds one.
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    /// Adds `n`.
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    /// Current value.
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A value that goes up and down.
#[derive(Debug, Default)]
pub struct Gauge(AtomicI64);

impl Gauge {
    /// Adds `n` (may be negative).
    pub fn add(&self, n: i64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    /// Current value.
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

macro_rules! metric_set {
    ($(#[$meta:meta])* $name:ident { $($(#[$fmeta:meta])* $field:ident : $kind:ident),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Default)]
        pub struct $name {
            $($(#[$fmeta])* pub $field: $kind),+
        }

        impl $name {
            /// Snapshot of all values keyed by metric name.
            pub fn snapshot(&self) -> BTreeMap<&'static str, i64> {
                let mut values = BTreeMap::new();
                $(values.insert(stringify!($field), metric_value(&self.$field));)+
                values
            }
        }
    };
}

trait MetricValue {
    fn value(&self) -> i64;
}

impl MetricValue for Counter {
    fn value(&self) -> i64 {
        i64::try_from(self.get()).unwrap_or(i64::MAX)
    }
}

impl MetricValue for Gauge {
    fn value(&self) -> i64 {
        self.get()
    }
}

fn metric_value(metric: &impl MetricValue) -> i64 {
    metric.value()
}

metric_set! {
    /// Client-wide counters (publishing and payloads).
    ClientMetrics {
        /// Messages sent to queues or published to topics.
        messages_published: Counter,
        /// Publish operations that failed.
        publish_failures: Counter,
        /// Encoded bytes published (body plus attributes).
        bytes_published: Counter,
        /// Batch entries retried after partial failures.
        publish_retries: Counter,
        /// Payloads offloaded to external storage.
        payloads_offloaded: Counter,
        /// Bytes offloaded to external storage.
        bytes_offloaded: Counter,
    }
}

metric_set! {
    /// Per-consumer counters and gauges.
    ConsumerMetrics {
        /// ReceiveMessage calls.
        receive_requests: Counter,
        /// ReceiveMessage calls that returned nothing.
        empty_receives: Counter,
        /// ReceiveMessage calls that failed.
        receive_errors: Counter,
        /// DeleteMessageBatch calls made to settle messages (acks, dead-letters, requeues).
        delete_requests: Counter,
        /// ChangeMessageVisibilityBatch calls made to settle messages (retries, releases,
        /// explicit extensions; heartbeats are counted as `lease_extensions`).
        visibility_requests: Counter,
        /// Messages received.
        messages_received: Counter,
        /// Messages deleted after processing.
        messages_acked: Counter,
        /// Deletes that failed.
        ack_failures: Counter,
        /// Messages whose visibility was changed for a retry.
        messages_retried: Counter,
        /// Messages re-enqueued as new messages.
        messages_requeued: Counter,
        /// Messages forwarded to the dead-letter queue.
        messages_dead_lettered: Counter,
        /// Dead-letter forwards that failed.
        dead_letter_failures: Counter,
        /// Messages released (made visible) without processing.
        messages_released: Counter,
        /// Messages that failed transport decoding.
        decode_failures: Counter,
        /// Offloaded payloads fetched.
        payloads_fetched: Counter,
        /// Successful visibility extensions.
        lease_extensions: Counter,
        /// Failed visibility extensions.
        lease_extension_failures: Counter,
        /// Leases whose receipt handle became invalid.
        leases_lost: Counter,
        /// Leases that hit the maximum lease duration.
        leases_expired: Counter,
        /// Messages leased and not yet settled (buffered, processing or settling).
        in_flight: Gauge,
        /// Messages whose settlement is in progress (their in-flight slot is free again).
        settling: Gauge,
        /// Messages decoded and waiting to be picked up by the dispatcher.
        buffered: Gauge,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_include_all_fields() {
        let metrics = ConsumerMetrics::default();
        metrics.messages_received.add(3);
        metrics.in_flight.add(2);
        metrics.in_flight.add(-1);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot["messages_received"], 3);
        assert_eq!(snapshot["in_flight"], 1);
        assert_eq!(snapshot["messages_acked"], 0);
        assert_eq!(ClientMetrics::default().snapshot().len(), 6);
    }
}
