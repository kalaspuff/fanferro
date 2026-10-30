//! Consumer engine configuration.

use std::time::Duration;

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::retry::{Backoff, RetrySettings};
use crate::transport::sns_notification::SnsUnwrap;
use crate::util::{duration_from_secs, whole_seconds};

/// Maximum visibility timeout SQS allows.
pub const MAX_VISIBILITY: Duration = Duration::from_secs(12 * 3600);

fn d10() -> usize {
    10
}
fn d10u8() -> u8 {
    10
}
fn d20() -> f64 {
    20.0
}
fn d1() -> usize {
    1
}
fn d30() -> f64 {
    30.0
}
fn d_receive_batch_wait() -> f64 {
    0.1
}

/// Receive rate limit.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateLimitConfig {
    /// Messages per second.
    pub per_second: f64,
    /// Burst size (defaults to `max(1, per_second)`).
    #[serde(default)]
    pub burst: Option<u32>,
}

/// Visibility heartbeat settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HeartbeatConfig {
    /// Extend visibility while messages are buffered or processed.
    pub enabled: bool,
    /// Seconds between extensions (default: a third of the visibility timeout).
    pub interval: Option<f64>,
    /// Visibility timeout set by each extension (default: the visibility timeout).
    pub extend_by: Option<f64>,
    /// Stop extending after this many seconds since receipt (default and maximum: 12 hours).
    pub max_lease: Option<f64>,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self { enabled: true, interval: None, extend_by: None, max_lease: None }
    }
}

/// Settlement (ack/nack) batching.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SettleConfig {
    /// Seconds a batch waits for more settlements (at most 1). A batch is sent as soon as it
    /// holds ten deletes or ten visibility changes, or when no other settlement can join it;
    /// 0 sends whatever is queued immediately.
    pub linger: f64,
    /// Retries for failed delete/visibility calls.
    pub retry: RetrySettings,
}

/// Default settlement linger (seconds).
const DEFAULT_SETTLE_LINGER: f64 = 0.005;

impl Default for SettleConfig {
    fn default() -> Self {
        Self { linger: DEFAULT_SETTLE_LINGER, retry: RetrySettings::default() }
    }
}

/// Configuration of a [`ConsumerEngine`](super::ConsumerEngine).
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerConfig {
    /// Queue URL.
    pub queue_url: String,
    /// Queue ARN (used in dead-letter metadata).
    #[serde(default)]
    pub queue_arn: Option<String>,
    /// FIFO queue.
    #[serde(default)]
    pub fifo: bool,
    /// Maximum leased messages that are not being settled (processing plus prefetched).
    #[serde(default = "d10")]
    pub max_in_flight: usize,
    /// Maximum messages whose settlement (delete, visibility change, dead-letter, requeue) is in
    /// progress (default: four times `max_in_flight`, at least 20 so that full batches can form).
    /// A message hands its in-flight slot back when its settlement starts, so receiving
    /// continues during the settlement round trip; this bounds that backlog.
    #[serde(default)]
    pub max_settling: Option<usize>,
    /// Messages per receive call (1-10).
    #[serde(default = "d10u8")]
    pub batch_size: u8,
    /// Free in-flight slots a poller waits for before receiving (1 to `batch_size`; default:
    /// `batch_size` on standard queues, 1 on FIFO queues; at most `max_in_flight`). Waiting for
    /// a full batch keeps receives full when handlers are fast.
    #[serde(default)]
    pub min_receive_batch: Option<u8>,
    /// Seconds a poller waits, after the first slot is free, for `min_receive_batch` slots
    /// before it receives with fewer (0-20).
    #[serde(default = "d_receive_batch_wait")]
    pub receive_batch_wait: f64,
    /// Long polling wait time in seconds (0-20; fractions round up to whole seconds). With 0
    /// (short polling), an empty receive pauses a second before the next one.
    #[serde(default = "d20")]
    pub wait_time: f64,
    /// Concurrent receive loops.
    #[serde(default = "d1")]
    pub pollers: usize,
    /// Optional receive rate limit.
    #[serde(default)]
    pub rate_limit: Option<RateLimitConfig>,
    /// Visibility timeout applied on receive, in seconds.
    #[serde(default = "d30")]
    pub visibility_timeout: f64,
    /// Heartbeat settings.
    #[serde(default)]
    pub heartbeat: HeartbeatConfig,
    /// Settlement batching and retries.
    #[serde(default)]
    pub settle: SettleConfig,
    /// Dead-letter queue for explicit dead-lettering.
    #[serde(default)]
    pub dead_letter_queue_url: Option<String>,
    /// Whether the dead-letter queue is FIFO (defaults to the queue's type).
    #[serde(default)]
    pub dead_letter_fifo: Option<bool>,
    /// SNS notification unwrapping.
    #[serde(default)]
    pub sns_unwrap: SnsUnwrap,
    /// Confirm SNS subscription confirmation requests that arrive on the queue.
    #[serde(default)]
    pub auto_confirm_subscriptions: bool,
    /// Verify MD5 digests of received messages (opt-in).
    #[serde(default)]
    pub verify_md5: bool,
    /// Backoff after failed receive calls.
    #[serde(default = "default_receive_backoff")]
    pub receive_backoff: RetrySettings,
}

fn default_receive_backoff() -> RetrySettings {
    RetrySettings { max_attempts: u32::MAX, initial_backoff: 1.0, max_backoff: 30.0 }
}

/// Validated settings with durations.
#[derive(Clone, Debug)]
pub(crate) struct Settings {
    pub queue_url: String,
    pub queue_arn: Option<String>,
    pub fifo: bool,
    pub max_in_flight: usize,
    pub max_settling: usize,
    pub batch_size: u8,
    pub min_receive_batch: usize,
    pub receive_batch_wait: Duration,
    pub wait_time: Duration,
    pub pollers: usize,
    pub rate_limit: Option<(f64, u32)>,
    pub visibility_timeout: Duration,
    pub heartbeat_enabled: bool,
    pub heartbeat_interval: Duration,
    pub heartbeat_tick: Duration,
    pub extend_by: Duration,
    pub max_lease: Duration,
    pub settle_linger: Duration,
    pub settle_retry: RetrySettings,
    pub settle_backoff: Backoff,
    pub dead_letter_queue_url: Option<String>,
    pub dead_letter_fifo: bool,
    pub sns_unwrap: SnsUnwrap,
    pub auto_confirm_subscriptions: bool,
    pub verify_md5: bool,
    pub receive_backoff: Backoff,
}

impl ConsumerConfig {
    pub(crate) fn validate(&self) -> Result<Settings> {
        if self.queue_url.is_empty() {
            return Err(Error::config("consumer queue_url must not be empty"));
        }
        if self.max_in_flight == 0 {
            return Err(Error::config("max_in_flight must be at least 1"));
        }
        if self.max_settling == Some(0) {
            return Err(Error::config("max_settling must be at least 1"));
        }
        if !(1..=10).contains(&self.batch_size) {
            return Err(Error::config("batch_size must be between 1 and 10"));
        }
        if let Some(min) = self.min_receive_batch
            && !(1..=self.batch_size).contains(&min)
        {
            return Err(Error::config(format!(
                "min_receive_batch must be between 1 and batch_size ({}), got {min}",
                self.batch_size
            )));
        }
        let receive_batch_wait = duration_from_secs(self.receive_batch_wait, "receive_batch_wait")?;
        if receive_batch_wait > Duration::from_secs(20) {
            return Err(Error::config("receive_batch_wait must be at most 20 seconds"));
        }
        if !(1..=64).contains(&self.pollers) {
            return Err(Error::config("pollers must be between 1 and 64"));
        }
        // SQS waits whole seconds: a fraction rounds up (truncated, it would be a short poll).
        let wait_time = whole_seconds(duration_from_secs(self.wait_time, "wait_time")?);
        if wait_time > Duration::from_secs(20) {
            return Err(Error::config("wait_time must be at most 20 seconds"));
        }
        let visibility_timeout = duration_from_secs(self.visibility_timeout, "visibility_timeout")?;
        if visibility_timeout > MAX_VISIBILITY {
            return Err(Error::config("visibility_timeout must be at most 12 hours"));
        }
        let rate_limit = match &self.rate_limit {
            Some(limit) if !(limit.per_second.is_finite() && limit.per_second > 0.0) => {
                return Err(Error::config("rate_limit.per_second must be positive"));
            }
            Some(limit) => {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let default_burst = limit.per_second.ceil().max(1.0).min(f64::from(u32::MAX)) as u32;
                Some((limit.per_second, limit.burst.unwrap_or(default_burst).max(1)))
            }
            None => None,
        };
        let extend_by = match self.heartbeat.extend_by {
            Some(secs) => duration_from_secs(secs, "heartbeat.extend_by")?,
            None => visibility_timeout,
        };
        let heartbeat_enabled = self.heartbeat.enabled && !extend_by.is_zero();
        if self.heartbeat.enabled && extend_by.is_zero() {
            return Err(Error::config("heartbeats need a positive visibility timeout (or heartbeat.extend_by)"));
        }
        if extend_by > MAX_VISIBILITY {
            return Err(Error::config("heartbeat.extend_by must be at most 12 hours"));
        }
        let heartbeat_interval = match self.heartbeat.interval {
            Some(secs) => duration_from_secs(secs, "heartbeat.interval")?,
            None => (visibility_timeout.min(extend_by) / 3).max(Duration::from_millis(200)),
        };
        if heartbeat_enabled {
            if heartbeat_interval.is_zero() {
                return Err(Error::config("heartbeat.interval must be positive"));
            }
            if heartbeat_interval >= visibility_timeout.min(extend_by) {
                return Err(Error::config(
                    "heartbeat.interval must be shorter than the visibility timeout (and heartbeat.extend_by)",
                ));
            }
        }
        let max_lease = match self.heartbeat.max_lease {
            Some(secs) => duration_from_secs(secs, "heartbeat.max_lease")?.min(MAX_VISIBILITY),
            None => MAX_VISIBILITY,
        };
        let heartbeat_tick = (heartbeat_interval / 4).clamp(Duration::from_millis(50), Duration::from_secs(1));
        let settle_linger = duration_from_secs(self.settle.linger, "settle.linger")?;
        if settle_linger > Duration::from_secs(1) {
            return Err(Error::config("settle.linger must be at most 1 second"));
        }
        Ok(Settings {
            queue_url: self.queue_url.clone(),
            queue_arn: self.queue_arn.clone(),
            fifo: self.fifo,
            max_in_flight: self.max_in_flight,
            max_settling: self
                .max_settling
                .unwrap_or_else(|| self.max_in_flight.saturating_mul(4).max(2 * crate::publish::MAX_BATCH_ENTRIES)),
            batch_size: self.batch_size,
            min_receive_batch: self.requested_min_receive_batch().min(self.max_in_flight),
            receive_batch_wait,
            wait_time,
            pollers: self.pollers,
            rate_limit,
            visibility_timeout,
            heartbeat_enabled,
            heartbeat_interval,
            heartbeat_tick,
            extend_by,
            max_lease,
            settle_linger,
            settle_retry: self.settle.retry.clone(),
            settle_backoff: self.settle.retry.backoff()?,
            dead_letter_queue_url: self.dead_letter_queue_url.clone(),
            dead_letter_fifo: self.dead_letter_fifo.unwrap_or(self.fifo),
            sns_unwrap: self.sns_unwrap,
            auto_confirm_subscriptions: self.auto_confirm_subscriptions,
            verify_md5: self.verify_md5,
            receive_backoff: self.receive_backoff.backoff()?,
        })
    }

    /// The configured minimum receive batch, or its default (before the in-flight limit).
    pub(crate) fn requested_min_receive_batch(&self) -> usize {
        match self.min_receive_batch {
            Some(min) => usize::from(min),
            // Waiting to fill a batch would hold back FIFO message groups.
            None if self.fifo => 1,
            None => usize::from(self.batch_size),
        }
    }

    /// A configuration with defaults for `queue_url`.
    pub fn new(queue_url: impl Into<String>) -> Self {
        Self {
            queue_url: queue_url.into(),
            queue_arn: None,
            fifo: false,
            max_in_flight: d10(),
            max_settling: None,
            batch_size: d10u8(),
            min_receive_batch: None,
            receive_batch_wait: d_receive_batch_wait(),
            wait_time: d20(),
            pollers: d1(),
            rate_limit: None,
            visibility_timeout: d30(),
            heartbeat: HeartbeatConfig::default(),
            settle: SettleConfig::default(),
            dead_letter_queue_url: None,
            dead_letter_fifo: None,
            sns_unwrap: SnsUnwrap::Auto,
            auto_confirm_subscriptions: false,
            verify_md5: false,
            receive_backoff: default_receive_backoff(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validate() {
        let settings = ConsumerConfig::new("u").validate().unwrap();
        assert_eq!(settings.heartbeat_interval, Duration::from_secs(10));
        assert_eq!(settings.extend_by, Duration::from_secs(30));
        assert_eq!(settings.max_lease, MAX_VISIBILITY);
        assert!(settings.heartbeat_enabled);
        assert_eq!(settings.max_settling, 40);
        assert_eq!(settings.settle_linger, Duration::from_millis(5));
        let small = ConsumerConfig { max_in_flight: 1, ..ConsumerConfig::new("u") }.validate().unwrap();
        assert_eq!(small.max_settling, 20, "room for two full batches");
    }

    #[test]
    fn minimum_receive_batch_defaults_and_limits() {
        let base = ConsumerConfig::new("u");
        let settings = base.validate().unwrap();
        assert_eq!(settings.min_receive_batch, 10);
        assert_eq!(settings.receive_batch_wait, Duration::from_millis(100));
        assert_eq!(ConsumerConfig { fifo: true, ..base.clone() }.validate().unwrap().min_receive_batch, 1);
        assert_eq!(ConsumerConfig { batch_size: 4, ..base.clone() }.validate().unwrap().min_receive_batch, 4);
        // Never more than the in-flight limit allows.
        assert_eq!(ConsumerConfig { max_in_flight: 3, ..base.clone() }.validate().unwrap().min_receive_batch, 3);
        let explicit = ConsumerConfig { fifo: true, min_receive_batch: Some(5), ..base.clone() };
        assert_eq!(explicit.validate().unwrap().min_receive_batch, 5);
        assert!(ConsumerConfig { min_receive_batch: Some(0), ..base.clone() }.validate().is_err());
        assert!(ConsumerConfig { batch_size: 5, min_receive_batch: Some(6), ..base.clone() }.validate().is_err());
        assert!(ConsumerConfig { receive_batch_wait: 21.0, ..base.clone() }.validate().is_err());
        assert!(ConsumerConfig { receive_batch_wait: -1.0, ..base }.validate().is_err());
    }

    #[test]
    fn wait_times_are_whole_seconds() {
        let base = ConsumerConfig::new("u");
        let wait = |wait_time: f64| ConsumerConfig { wait_time, ..base.clone() }.validate().unwrap().wait_time;
        assert_eq!(wait(0.5), Duration::from_secs(1));
        assert_eq!(wait(19.2), Duration::from_secs(20));
        assert_eq!(wait(0.0), Duration::ZERO);
        assert_eq!(wait(20.0), Duration::from_secs(20));
    }

    #[test]
    fn invalid_configs_are_rejected() {
        let base = ConsumerConfig::new("u");
        assert!(ConsumerConfig { max_in_flight: 0, ..base.clone() }.validate().is_err());
        assert!(ConsumerConfig { max_settling: Some(0), ..base.clone() }.validate().is_err());
        assert!(ConsumerConfig { batch_size: 11, ..base.clone() }.validate().is_err());
        assert!(ConsumerConfig { wait_time: 21.0, ..base.clone() }.validate().is_err());
        assert!(ConsumerConfig { pollers: 0, ..base.clone() }.validate().is_err());
        assert!(
            ConsumerConfig {
                heartbeat: HeartbeatConfig { interval: Some(30.0), ..HeartbeatConfig::default() },
                ..base.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            ConsumerConfig { rate_limit: Some(RateLimitConfig { per_second: 0.0, burst: None }), ..base.clone() }
                .validate()
                .is_err()
        );
        assert!(
            ConsumerConfig { settle: SettleConfig { linger: 1.5, ..SettleConfig::default() }, ..base.clone() }
                .validate()
                .is_err()
        );
        assert!(ConsumerConfig { visibility_timeout: 0.0, ..base }.validate().is_err());
    }
}
