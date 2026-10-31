//! Typed events emitted by the core.
//!
//! Every notable action (resource created, message received, lease extended, ...) can
//! emit an [`Event`]. Events are buffered in a bounded [`EventBus`] and consumed by the
//! Python layer, which routes them to logging and user hooks. Emission is filtered by
//! kind ([`EventBus::set_enabled`]) so disabled events cost a single atomic load.
//!
//! When adding a kind: add it to [`EventKind`], [`EventKind::ALL`], `as_str`,
//! `default_level`, the Python `EventKind` enum and `docs/reference/events.md`
//! (CI checks that all of these agree).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use time::OffsetDateTime;
use tokio::sync::{Mutex, Notify, mpsc};

use crate::error::Error;

/// Severity of an event (maps to Python logging levels).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Level {
    /// Detailed diagnostics.
    Debug,
    /// Normal operation milestones.
    Info,
    /// Something unexpected that was handled.
    Warning,
    /// A failure that needs attention.
    Error,
}

impl Level {
    /// Lowercase name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

macro_rules! event_kinds {
    ($($variant:ident => ($name:literal, $level:ident, $doc:literal)),+ $(,)?) => {
        /// Every kind of event the core (and the Python layer) can emit.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
        #[repr(u8)]
        pub enum EventKind {
            $(#[doc = $doc] $variant),+
        }

        impl EventKind {
            /// All kinds, in declaration order.
            pub const ALL: &'static [EventKind] = &[$(EventKind::$variant),+];

            /// Stable dotted name, e.g. `message.acked`.
            pub fn as_str(self) -> &'static str {
                match self { $(EventKind::$variant => $name),+ }
            }

            /// Default severity.
            pub fn default_level(self) -> Level {
                match self { $(EventKind::$variant => Level::$level),+ }
            }

            /// One-line description.
            pub fn description(self) -> &'static str {
                match self { $(EventKind::$variant => $doc),+ }
            }

            /// Parses a dotted name.
            pub fn parse(name: &str) -> Option<Self> {
                match name { $($name => Some(EventKind::$variant),)+ _ => None }
            }
        }
    };
}

event_kinds! {
    ResourceFound => ("resource.found", Debug, "A queue, topic or subscription was found during provisioning."),
    ResourceCreated => ("resource.created", Info, "A queue, topic, subscription or bucket was created."),
    ResourceUpdated => ("resource.updated", Info, "Resource attributes or tags were updated to match the spec."),
    ResourceDrift => ("resource.drift", Warning, "Resource attributes differ from the spec and were not updated."),
    ResourceMissing => ("resource.missing", Warning, "A resource does not exist and was not created."),
    ResourceFailed => ("resource.failed", Error, "Provisioning a resource failed."),
    ResourceFallback => ("resource.fallback", Warning, "A provisioning fallback handler supplied the resource."),
    ResourcePolicyUpdated => ("resource.policy_updated", Info, "A queue policy was updated to allow an SNS topic to send messages."),
    ResourceDeleted => ("resource.deleted", Info, "A resource was deleted (teardown)."),
    PublishSucceeded => ("publish.succeeded", Debug, "A message was published to a topic or sent to a queue."),
    PublishFailed => ("publish.failed", Error, "Publishing a message failed after retries."),
    PublishRetried => ("publish.retried", Warning, "Failed batch entries are being retried."),
    PayloadOffloaded => ("payload.offloaded", Debug, "A large payload was stored externally and replaced by a pointer."),
    PayloadFetched => ("payload.fetched", Debug, "An offloaded payload was downloaded."),
    PayloadDeleted => ("payload.deleted", Debug, "An offloaded payload object was deleted after processing."),
    ConsumerStarting => ("consumer.starting", Info, "A consumer is starting."),
    ConsumerStarted => ("consumer.started", Info, "A consumer started receiving messages."),
    ConsumerPaused => ("consumer.paused", Info, "A consumer paused receiving."),
    ConsumerResumed => ("consumer.resumed", Info, "A consumer resumed receiving."),
    ConsumerStopping => ("consumer.stopping", Info, "A consumer is shutting down."),
    ConsumerDraining => ("consumer.draining", Info, "A stopping consumer is waiting for in-flight messages."),
    ConsumerStopped => ("consumer.stopped", Info, "A consumer stopped."),
    ConsumerError => ("consumer.error", Error, "A consumer hit an unrecoverable error."),
    ReceiveSucceeded => ("receive.succeeded", Debug, "A receive call returned messages."),
    ReceiveEmpty => ("receive.empty", Debug, "A receive call returned no messages."),
    ReceiveFailed => ("receive.failed", Warning, "A receive call failed; the consumer backs off and retries."),
    MessageReceived => ("message.received", Debug, "A message was received and leased."),
    MessageDecodeFailed => ("message.decode_failed", Error, "A message could not be decoded (transport, envelope or codec)."),
    MessageUnroutable => ("message.unroutable", Warning, "No route matched a message."),
    MessageProcessed => ("message.processed", Debug, "A handler processed a message successfully."),
    MessageFailed => ("message.failed", Warning, "A handler raised an error for a message."),
    MessageDuplicate => ("message.duplicate", Info, "A message was skipped as an already processed duplicate."),
    MessageAcked => ("message.acked", Debug, "A message was deleted from the queue."),
    MessageAckFailed => ("message.ack_failed", Warning, "Deleting a message failed; it may be redelivered."),
    MessageRetryScheduled => ("message.retry_scheduled", Info, "A failed message will become visible again after a backoff delay."),
    MessageRequeued => ("message.requeued", Info, "A failed message was sent back to the queue as a new message."),
    MessageDeadLettered => ("message.dead_lettered", Warning, "A message was forwarded to the dead-letter queue."),
    MessageDeadLetterFailed => ("message.dead_letter_failed", Error, "Forwarding a message to the dead-letter queue failed."),
    MessageReleased => ("message.released", Debug, "A message was made visible again without processing."),
    MessageDropped => ("message.dropped", Warning, "A message was deleted without being processed."),
    LeaseExtended => ("lease.extended", Debug, "Message visibility was extended by the heartbeat."),
    LeaseExtensionFailed => ("lease.extension_failed", Warning, "Extending message visibility failed."),
    LeaseLost => ("lease.lost", Warning, "The receipt handle became invalid; the message may be processed twice."),
    LeaseExpired => ("lease.expired", Warning, "The maximum lease duration was reached; heartbeats stopped."),
    SubscriptionConfirmed => ("sns.subscription_confirmed", Info, "An SNS subscription confirmation was confirmed automatically."),
    HookFailed => ("hook.failed", Error, "An event hook or middleware raised an exception."),
    ConfigWarning => ("config.warning", Warning, "A configuration combination is likely a mistake."),
    EventsDropped => ("events.dropped", Warning, "Events were dropped because the event buffer was full, or async hooks were skipped because too many were running."),
}

/// A structured event field value.
#[derive(Clone, Debug, PartialEq)]
pub enum FieldValue {
    /// Text.
    Str(String),
    /// Signed integer.
    Int(i64),
    /// Floating point number.
    Float(f64),
    /// Boolean.
    Bool(bool),
}

impl From<&str> for FieldValue {
    fn from(value: &str) -> Self {
        Self::Str(value.to_string())
    }
}

impl From<String> for FieldValue {
    fn from(value: String) -> Self {
        Self::Str(value)
    }
}

impl From<&String> for FieldValue {
    fn from(value: &String) -> Self {
        Self::Str(value.clone())
    }
}

impl From<i64> for FieldValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<u32> for FieldValue {
    fn from(value: u32) -> Self {
        Self::Int(i64::from(value))
    }
}

impl From<usize> for FieldValue {
    fn from(value: usize) -> Self {
        Self::Int(i64::try_from(value).unwrap_or(i64::MAX))
    }
}

impl From<u64> for FieldValue {
    fn from(value: u64) -> Self {
        Self::Int(i64::try_from(value).unwrap_or(i64::MAX))
    }
}

impl From<f64> for FieldValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<bool> for FieldValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

/// Error details attached to an event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorInfo {
    /// Error category (see [`Error::category`]).
    pub category: String,
    /// Display text.
    pub message: String,
    /// Whether the failed operation is retryable.
    pub retryable: bool,
    /// AWS error code, if any.
    pub code: Option<String>,
    /// AWS request id, if any.
    pub request_id: Option<String>,
}

impl From<&Error> for ErrorInfo {
    fn from(error: &Error) -> Self {
        Self {
            category: error.category().to_string(),
            message: error.to_string(),
            retryable: error.is_retryable(),
            code: error.aws().and_then(|aws| aws.code.clone()),
            request_id: error.aws().and_then(|aws| aws.request_id.clone()),
        }
    }
}

/// An event.
#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    /// Kind.
    pub kind: EventKind,
    /// Severity (defaults to the kind's default level).
    pub level: Level,
    /// Human-readable summary.
    pub message: String,
    /// When it happened.
    pub time: OffsetDateTime,
    /// Structured fields (queue, topic, message_id, counts, durations...).
    pub fields: BTreeMap<String, FieldValue>,
    /// Error details for failure events.
    pub error: Option<ErrorInfo>,
}

impl Event {
    /// Creates an event of `kind` with its default level.
    pub fn new(kind: EventKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            level: kind.default_level(),
            message: message.into(),
            time: OffsetDateTime::now_utc(),
            fields: BTreeMap::new(),
            error: None,
        }
    }

    /// Adds a field.
    #[must_use]
    pub fn field(mut self, name: &str, value: impl Into<FieldValue>) -> Self {
        self.fields.insert(name.to_string(), value.into());
        self
    }

    /// Adds a field when `value` is `Some`.
    #[must_use]
    pub fn field_opt<V: Into<FieldValue>>(self, name: &str, value: Option<V>) -> Self {
        match value {
            Some(value) => self.field(name, value),
            None => self,
        }
    }

    /// Attaches error details.
    #[must_use]
    pub fn error(mut self, error: &Error) -> Self {
        self.error = Some(ErrorInfo::from(error));
        self
    }

    /// Overrides the level.
    #[must_use]
    pub fn level(mut self, level: Level) -> Self {
        self.level = level;
        self
    }
}

/// A bounded, filtered, multi-producer event buffer.
#[derive(Debug)]
pub struct EventBus {
    sender: mpsc::Sender<Event>,
    receiver: Mutex<mpsc::Receiver<Event>>,
    enabled: Vec<AtomicBool>,
    dropped: AtomicU64,
    wake: Notify,
}

impl EventBus {
    /// Creates a bus that buffers up to `capacity` events. All kinds start disabled.
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        Self {
            sender,
            receiver: Mutex::new(receiver),
            enabled: EventKind::ALL.iter().map(|_| AtomicBool::new(false)).collect(),
            dropped: AtomicU64::new(0),
            wake: Notify::new(),
        }
    }

    /// Whether events of `kind` are currently wanted.
    pub fn is_enabled(&self, kind: EventKind) -> bool {
        self.enabled.get(kind as usize).is_some_and(|flag| flag.load(Ordering::Relaxed))
    }

    /// Enables exactly the given kinds.
    pub fn set_enabled(&self, kinds: &[EventKind]) {
        for (index, flag) in self.enabled.iter().enumerate() {
            flag.store(kinds.iter().any(|kind| *kind as usize == index), Ordering::Relaxed);
        }
    }

    /// Emits an event built lazily by `build` if `kind` is enabled.
    pub fn emit(&self, kind: EventKind, build: impl FnOnce() -> Event) {
        if !self.is_enabled(kind) {
            return;
        }
        let event = build();
        if self.sender.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Number of events dropped because the buffer was full (resets the counter).
    pub fn take_dropped(&self) -> u64 {
        self.dropped.swap(0, Ordering::Relaxed)
    }

    /// Waits for at least one event (or `timeout`, or [`Self::wake`]) and returns up to `max`
    /// events.
    pub async fn next_batch(&self, max: usize, timeout: std::time::Duration) -> Vec<Event> {
        let mut receiver = self.receiver.lock().await;
        let mut events = Vec::new();
        tokio::select! {
            biased;
            _ = receiver.recv_many(&mut events, max.max(1)) => {}
            () = self.wake.notified() => {}
            () = tokio::time::sleep(timeout) => {}
        }
        events
    }

    /// Makes a pending [`Self::next_batch`] (or the next one) return right away, so that a
    /// consumer of events can stop without abandoning events a pending call already took.
    pub fn wake(&self) {
        self.wake.notify_one();
    }

    /// Returns up to `max` buffered events without waiting.
    pub fn drain(&self, max: usize) -> Vec<Event> {
        let Ok(mut receiver) = self.receiver.try_lock() else { return Vec::new() };
        let mut events = Vec::new();
        while events.len() < max {
            match receiver.try_recv() {
                Ok(event) => events.push(event),
                Err(_) => break,
            }
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_roundtrip_and_are_unique() {
        let mut names = std::collections::HashSet::new();
        for kind in EventKind::ALL {
            assert_eq!(EventKind::parse(kind.as_str()), Some(*kind));
            assert!(names.insert(kind.as_str()), "duplicate {}", kind.as_str());
            assert!(!kind.description().is_empty());
        }
        assert_eq!(EventKind::parse("nope"), None);
    }

    #[tokio::test]
    async fn filtering_and_dropping() {
        let bus = EventBus::new(2);
        bus.emit(EventKind::MessageAcked, || panic!("disabled events must not be built"));
        bus.set_enabled(&[EventKind::MessageAcked]);
        for _ in 0..3 {
            bus.emit(EventKind::MessageAcked, || Event::new(EventKind::MessageAcked, "acked").field("n", 1i64));
        }
        assert_eq!(bus.take_dropped(), 1);
        let events = bus.next_batch(10, std::time::Duration::from_millis(10)).await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].fields.get("n"), Some(&FieldValue::Int(1)));
        assert!(bus.next_batch(10, std::time::Duration::from_millis(1)).await.is_empty());
        bus.emit(EventKind::MessageAcked, || Event::new(EventKind::MessageAcked, "acked"));
        assert_eq!(bus.drain(10).len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn wake_ends_a_pending_wait() {
        let bus = std::sync::Arc::new(EventBus::new(8));
        let waiting = tokio::spawn({
            let bus = bus.clone();
            async move {
                let started = tokio::time::Instant::now();
                (bus.next_batch(10, std::time::Duration::from_secs(60)).await.len(), started.elapsed())
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        bus.wake();
        let (count, elapsed) = waiting.await.unwrap();
        assert_eq!(count, 0);
        assert!(elapsed < std::time::Duration::from_secs(1));
        // Buffered events still come first after a wake.
        bus.set_enabled(&[EventKind::MessageAcked]);
        bus.emit(EventKind::MessageAcked, || Event::new(EventKind::MessageAcked, "acked"));
        bus.wake();
        assert_eq!(bus.next_batch(10, std::time::Duration::from_secs(60)).await.len(), 1);
    }
}
