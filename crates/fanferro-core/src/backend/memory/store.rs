//! Indexed storage for the in-memory broker: a queue's messages and deduplication windows.
//!
//! Every per-message operation the broker performs (send, receive, delete, visibility change)
//! takes logarithmic time or less, however many messages a queue holds, so large test queues
//! stay fast.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::time::Duration;

use time::OffsetDateTime;
use tokio::time::Instant;

use crate::types::MessageAttributes;

/// How long a FIFO deduplication id is remembered.
pub(super) const DEDUP_WINDOW: Duration = Duration::from_secs(300);

/// A message stored in a queue.
#[derive(Debug)]
pub(super) struct StoredMessage {
    pub message_id: String,
    pub body: String,
    pub attributes: MessageAttributes,
    pub md5_of_body: String,
    pub md5_of_attributes: Option<String>,
    pub sent: OffsetDateTime,
    /// When the message becomes visible (delayed or in flight until then).
    pub available_at: Instant,
    pub receive_count: u32,
    pub first_receive: Option<OffsetDateTime>,
    pub receipt_handle: Option<String>,
    /// Received and not released since (until `available_at`).
    pub in_flight: bool,
    pub group_id: Option<String>,
    pub deduplication_id: Option<String>,
    pub sequence_number: Option<String>,
    pub trace_header: Option<String>,
    pub dead_letter_source: Option<String>,
}

/// The messages of a queue.
///
/// Each message is either visible (in `visible`, by send order: the order receives hand messages
/// out in) or hidden until its `available_at` (in `hidden`: delayed, or in flight). Hidden messages
/// whose time has come are made visible by [`Messages::promote`], which every operation calls
/// first, so that state never depends on how long ago the last operation ran.
#[derive(Debug, Default)]
pub(super) struct Messages {
    by_sequence: BTreeMap<u64, StoredMessage>,
    next_sequence: u64,
    visible: BTreeSet<u64>,
    /// Visible messages that were received before (the only ones a redrive policy can move).
    redelivered: BTreeSet<u64>,
    hidden: BTreeSet<(Instant, u64)>,
    receipts: HashMap<String, u64>,
    ids: HashMap<String, u64>,
    /// In-flight messages per message group.
    groups_in_flight: HashMap<String, usize>,
    in_flight: usize,
}

impl Messages {
    /// Makes hidden messages whose time has come visible (in-flight ones are released).
    pub fn promote(&mut self, now: Instant) {
        while let Some(&(at, sequence)) = self.hidden.first() {
            if at > now {
                break;
            }
            self.hidden.pop_first();
            if let Some(message) = self.by_sequence.get_mut(&sequence) {
                if message.in_flight {
                    message.in_flight = false;
                    release_group(&mut self.groups_in_flight, &mut self.in_flight, message.group_id.as_deref());
                }
                self.visible.insert(sequence);
                if message.receive_count > 0 {
                    self.redelivered.insert(sequence);
                }
            }
        }
    }

    /// Adds a message (hidden until its `available_at` if that is later than `now`).
    pub fn push(&mut self, message: StoredMessage, now: Instant) {
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.ids.insert(message.message_id.clone(), sequence);
        if message.available_at > now {
            self.hidden.insert((message.available_at, sequence));
        } else {
            self.visible.insert(sequence);
            // Moved by a redrive policy with its receive count.
            if message.receive_count > 0 {
                self.redelivered.insert(sequence);
            }
        }
        self.by_sequence.insert(sequence, message);
    }

    /// Removes a message.
    pub fn remove(&mut self, sequence: u64) -> Option<StoredMessage> {
        let message = self.by_sequence.remove(&sequence)?;
        self.visible.remove(&sequence);
        self.redelivered.remove(&sequence);
        self.hidden.remove(&(message.available_at, sequence));
        if let Some(handle) = &message.receipt_handle {
            self.receipts.remove(handle);
        }
        if self.ids.get(&message.message_id) == Some(&sequence) {
            self.ids.remove(&message.message_id);
        }
        if message.in_flight {
            release_group(&mut self.groups_in_flight, &mut self.in_flight, message.group_id.as_deref());
        }
        Some(message)
    }

    /// Removes every message.
    pub fn clear(&mut self) {
        let next_sequence = self.next_sequence;
        *self = Self { next_sequence, ..Self::default() };
    }

    /// Every message, in send order.
    pub fn iter(&self) -> impl Iterator<Item = &StoredMessage> {
        self.by_sequence.values()
    }

    /// Visible messages, in send order.
    pub fn visible(&self) -> impl Iterator<Item = (u64, &StoredMessage)> {
        self.visible.iter().filter_map(|sequence| self.by_sequence.get(sequence).map(|message| (*sequence, message)))
    }

    /// Visible messages that were received before, in send order.
    pub fn redelivered(&self) -> impl Iterator<Item = (u64, &StoredMessage)> {
        self.redelivered
            .iter()
            .filter_map(|sequence| self.by_sequence.get(sequence).map(|message| (*sequence, message)))
    }

    /// Whether a message of `group` is in flight.
    pub fn group_in_flight(&self, group: &str) -> bool {
        self.groups_in_flight.contains_key(group)
    }

    /// Receives a visible message: counts the receive and hides it, in flight until `until`, with
    /// a new receipt handle. Returns the message and the handle it replaced.
    pub fn receive(&mut self, sequence: u64, until: Instant) -> Option<(&StoredMessage, Option<String>)> {
        let message = self.by_sequence.get_mut(&sequence)?;
        self.visible.remove(&sequence);
        self.redelivered.remove(&sequence);
        message.receive_count += 1;
        message.first_receive.get_or_insert_with(OffsetDateTime::now_utc);
        let handle = format!("{}#{}", message.message_id, uuid::Uuid::new_v4());
        let previous = message.receipt_handle.replace(handle.clone());
        if let Some(previous) = &previous {
            self.receipts.remove(previous);
        }
        self.receipts.insert(handle, sequence);
        if !message.in_flight {
            message.in_flight = true;
            self.in_flight += 1;
            if let Some(group) = &message.group_id {
                *self.groups_in_flight.entry(group.clone()).or_default() += 1;
            }
        }
        message.available_at = until;
        self.hidden.insert((until, sequence));
        Some((message, previous))
    }

    /// The message with the current receipt handle `handle`.
    pub fn by_receipt(&self, handle: &str) -> Option<u64> {
        self.receipts.get(handle).copied()
    }

    /// A message by sequence.
    pub fn get(&self, sequence: u64) -> Option<&StoredMessage> {
        self.by_sequence.get(&sequence)
    }

    /// Whether a message with this message id is stored.
    pub fn contains_id(&self, message_id: &str) -> bool {
        self.ids.contains_key(message_id)
    }

    /// Changes the visibility of an in-flight message: visible again at `now + timeout` (released
    /// right away when `timeout` is zero).
    pub fn change_visibility(&mut self, sequence: u64, now: Instant, timeout: Duration) {
        let Some(message) = self.by_sequence.get_mut(&sequence) else { return };
        self.hidden.remove(&(message.available_at, sequence));
        message.available_at = now + timeout;
        if timeout.is_zero() {
            if message.in_flight {
                message.in_flight = false;
                release_group(&mut self.groups_in_flight, &mut self.in_flight, message.group_id.as_deref());
            }
            self.visible.insert(sequence);
            if message.receive_count > 0 {
                self.redelivered.insert(sequence);
            }
        } else {
            self.hidden.insert((message.available_at, sequence));
        }
    }

    /// When the next hidden message becomes visible.
    pub fn next_change(&self) -> Option<Instant> {
        self.hidden.first().map(|(at, _)| *at)
    }

    /// `(visible, in flight, delayed)` message counts.
    pub fn counts(&self) -> (usize, usize, usize) {
        (self.visible.len(), self.in_flight, self.hidden.len() - self.in_flight)
    }
}

fn release_group(groups: &mut HashMap<String, usize>, in_flight: &mut usize, group: Option<&str>) {
    *in_flight -= 1;
    if let Some(group) = group
        && let Some(count) = groups.get_mut(group)
    {
        *count -= 1;
        if *count == 0 {
            groups.remove(group);
        }
    }
}

/// Deduplication ids remembered for [`DEDUP_WINDOW`], forgotten in the order they were added.
#[derive(Debug)]
pub(super) struct Dedup<T> {
    entries: HashMap<String, (Instant, T)>,
    order: VecDeque<(Instant, String)>,
}

impl<T> Default for Dedup<T> {
    fn default() -> Self {
        Self { entries: HashMap::new(), order: VecDeque::new() }
    }
}

impl<T> Dedup<T> {
    /// What was remembered for `id` within the window before `now`.
    pub fn get(&mut self, id: &str, now: Instant) -> Option<&T> {
        self.forget_expired(now);
        self.entries.get(id).map(|(_, value)| value)
    }

    /// Remembers `id` from `now` on.
    pub fn insert(&mut self, id: String, now: Instant, value: T) {
        self.forget_expired(now);
        self.order.push_back((now, id.clone()));
        self.entries.insert(id, (now, value));
    }

    fn forget_expired(&mut self, now: Instant) {
        while let Some((at, _)) = self.order.front() {
            if now.duration_since(*at) < DEDUP_WINDOW {
                break;
            }
            if let Some((at, id)) = self.order.pop_front()
                && self.entries.get(&id).is_some_and(|(added, _)| *added == at)
            {
                self.entries.remove(&id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(id: &str, available_at: Instant, group: Option<&str>) -> StoredMessage {
        StoredMessage {
            message_id: id.into(),
            body: id.into(),
            attributes: MessageAttributes::new(),
            md5_of_body: String::new(),
            md5_of_attributes: None,
            sent: OffsetDateTime::UNIX_EPOCH,
            available_at,
            receive_count: 0,
            first_receive: None,
            receipt_handle: None,
            in_flight: false,
            group_id: group.map(str::to_string),
            deduplication_id: None,
            sequence_number: None,
            trace_header: None,
            dead_letter_source: None,
        }
    }

    fn visible_ids(messages: &Messages) -> Vec<String> {
        messages.visible().map(|(_, message)| message.message_id.clone()).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn messages_move_between_visible_and_hidden() {
        let now = Instant::now();
        let mut messages = Messages::default();
        messages.push(message("a", now, Some("g")), now);
        messages.push(message("b", now + Duration::from_secs(5), None), now);
        messages.push(message("c", now, None), now);
        assert_eq!(visible_ids(&messages), ["a", "c"]);
        assert_eq!(messages.counts(), (2, 0, 1));

        let a = messages.visible().next().unwrap().0;
        let (received, previous) = messages.receive(a, now + Duration::from_secs(10)).unwrap();
        assert_eq!((received.receive_count, previous), (1, None));
        let first = received.receipt_handle.clone().unwrap();
        assert!(first.starts_with("a#"));
        assert!(messages.group_in_flight("g"));
        assert_eq!(messages.by_receipt(&first), Some(a));
        assert_eq!(messages.counts(), (1, 1, 1));
        assert_eq!(messages.next_change(), Some(now + Duration::from_secs(5)));

        // The delayed message becomes visible in send order; the in-flight one comes back later.
        messages.promote(now + Duration::from_secs(6));
        assert_eq!(visible_ids(&messages), ["b", "c"]);
        messages.promote(now + Duration::from_secs(10));
        assert_eq!(visible_ids(&messages), ["a", "b", "c"]);
        assert!(!messages.group_in_flight("g"));
        assert_eq!(messages.redelivered().map(|(sequence, _)| sequence).collect::<Vec<_>>(), [a]);

        // A new receive replaces the handle; releasing makes the message visible at once.
        let later = now + Duration::from_secs(10);
        let (received, previous) = messages.receive(a, later + Duration::from_secs(30)).unwrap();
        assert_eq!((received.receive_count, previous.as_deref()), (2, Some(first.as_str())));
        let second = received.receipt_handle.clone().unwrap();
        assert_eq!(messages.by_receipt(&first), None);
        messages.change_visibility(a, later, Duration::ZERO);
        assert_eq!(visible_ids(&messages), ["a", "b", "c"]);
        assert_eq!(messages.counts(), (3, 0, 0));

        let removed = messages.remove(a).unwrap();
        assert_eq!(removed.message_id, "a");
        assert!(!messages.contains_id("a"));
        assert_eq!(messages.by_receipt(&second), None);
        assert_eq!(messages.iter().map(|m| m.message_id.as_str()).collect::<Vec<_>>(), ["b", "c"]);
        messages.clear();
        assert_eq!(messages.counts(), (0, 0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn dedup_forgets_ids_after_the_window() {
        let start = Instant::now();
        let mut dedup = Dedup::default();
        dedup.insert("a".into(), start, 1);
        dedup.insert("b".into(), start + Duration::from_secs(100), 2);
        assert_eq!(dedup.get("a", start + Duration::from_secs(299)), Some(&1));
        assert_eq!(dedup.get("a", start + DEDUP_WINDOW), None);
        assert_eq!(dedup.get("b", start + DEDUP_WINDOW), Some(&2));
        dedup.insert("a".into(), start + DEDUP_WINDOW, 3);
        assert_eq!(dedup.get("a", start + DEDUP_WINDOW + Duration::from_secs(1)), Some(&3));
        assert_eq!(dedup.get("b", start + Duration::from_secs(400)), None);
    }
}
