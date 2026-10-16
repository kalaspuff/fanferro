//! Backoff and rate limiting primitives used by fanferro's own retry loops.
//!
//! Individual AWS calls are retried by the SDK. fanferro retries at a higher level where
//! the SDK cannot: failed entries of batch calls, the receive loop, and settlement.

use std::time::Duration;

use parking_lot::Mutex;
use rand::RngExt as _;
use serde::Deserialize;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::util::duration_from_secs;

/// Exponential backoff with full jitter.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Backoff {
    /// Delay cap for the first retry.
    pub initial: Duration,
    /// Maximum delay.
    pub max: Duration,
    /// Growth factor per attempt.
    pub multiplier: f64,
    /// Randomize delays (full jitter) to avoid thundering herds.
    pub jitter: bool,
}

impl Backoff {
    /// Creates an exponential backoff with full jitter.
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self { initial, max, multiplier: 2.0, jitter: true }
    }

    /// The upper bound of the delay before retry number `attempt` (0-based).
    pub fn ceiling(&self, attempt: u32) -> Duration {
        let factor = self.multiplier.max(1.0).powi(i32::try_from(attempt.min(64)).unwrap_or(64));
        let secs = (self.initial.as_secs_f64() * factor).min(self.max.as_secs_f64());
        Duration::from_secs_f64(secs.max(0.0))
    }

    /// The delay before retry number `attempt` (0-based).
    pub fn delay(&self, attempt: u32) -> Duration {
        let ceiling = self.ceiling(attempt);
        if self.jitter && !ceiling.is_zero() {
            Duration::from_secs_f64(rand::rng().random_range(0.0..=ceiling.as_secs_f64()))
        } else {
            ceiling
        }
    }
}

/// Settings for fanferro-level retries (deserialized from Python).
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetrySettings {
    /// Maximum attempts including the first.
    pub max_attempts: u32,
    /// Initial backoff in seconds.
    pub initial_backoff: f64,
    /// Maximum backoff in seconds.
    pub max_backoff: f64,
}

impl Default for RetrySettings {
    fn default() -> Self {
        Self { max_attempts: 4, initial_backoff: 0.1, max_backoff: 5.0 }
    }
}

impl RetrySettings {
    /// The backoff described by these settings.
    pub fn backoff(&self) -> Result<Backoff> {
        Ok(Backoff::new(
            duration_from_secs(self.initial_backoff, "initial_backoff")?,
            duration_from_secs(self.max_backoff, "max_backoff")?,
        ))
    }
}

/// Sleeps for `duration` unless `token` is cancelled first. Returns `false` if cancelled.
pub async fn sleep_or_cancel(duration: Duration, token: &CancellationToken) -> bool {
    tokio::select! {
        () = token.cancelled() => false,
        () = tokio::time::sleep(duration) => true,
    }
}

/// The longest single wait of [`RateLimiter::acquire_up_to`] before it checks the bucket again.
const MAX_WAIT: Duration = Duration::from_secs(3600);

/// A token bucket limiting how many messages per second are received.
#[derive(Debug)]
pub struct RateLimiter {
    rate: f64,
    burst: f64,
    state: Mutex<(f64, Instant)>,
}

impl RateLimiter {
    /// Allows `per_second` messages per second with bursts up to `burst`.
    pub fn new(per_second: f64, burst: u32) -> Self {
        let burst = f64::from(burst.max(1));
        Self { rate: per_second.max(f64::MIN_POSITIVE), burst, state: Mutex::new((burst, Instant::now())) }
    }

    fn refill(&self, state: &mut (f64, Instant)) {
        let now = Instant::now();
        let elapsed = now.duration_since(state.1).as_secs_f64();
        state.0 = (state.0 + elapsed * self.rate).min(self.burst);
        state.1 = now;
    }

    /// Waits until at least one token is available, then takes up to `max` whole tokens.
    /// Returns `None` if `token` is cancelled while waiting.
    pub async fn acquire_up_to(&self, max: u32, token: &CancellationToken) -> Option<u32> {
        loop {
            let wait = {
                let mut state = self.state.lock();
                self.refill(&mut state);
                if state.0 >= 1.0 {
                    // Truncation is intended: whole tokens only, bounded by `max`.
                    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                    let take = (state.0.floor() as u32).min(max.max(1));
                    state.0 -= f64::from(take);
                    return Some(take);
                }
                // Waits are capped (the loop refills and waits again): a tiny rate would overflow.
                Duration::try_from_secs_f64((1.0 - state.0) / self.rate).map_or(MAX_WAIT, |wait| wait.min(MAX_WAIT))
            };
            if !sleep_or_cancel(wait, token).await {
                return None;
            }
        }
    }

    /// Returns unused tokens.
    pub fn refund(&self, tokens: u32) {
        if tokens == 0 {
            return;
        }
        let mut state = self.state.lock();
        state.0 = (state.0 + f64::from(tokens)).min(self.burst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let backoff = Backoff { jitter: false, ..Backoff::new(Duration::from_millis(100), Duration::from_secs(1)) };
        assert_eq!(backoff.delay(0), Duration::from_millis(100));
        assert_eq!(backoff.delay(1), Duration::from_millis(200));
        assert_eq!(backoff.delay(3), Duration::from_millis(800));
        assert_eq!(backoff.delay(10), Duration::from_secs(1));
        let jittered = Backoff::new(Duration::from_millis(100), Duration::from_secs(1));
        for attempt in 0..10 {
            assert!(jittered.delay(attempt) <= jittered.ceiling(attempt));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limiter_enforces_rate() {
        let limiter = RateLimiter::new(10.0, 5);
        let token = CancellationToken::new();
        assert_eq!(limiter.acquire_up_to(10, &token).await, Some(5));
        let started = Instant::now();
        assert_eq!(limiter.acquire_up_to(10, &token).await, Some(1));
        assert!(started.elapsed() >= Duration::from_millis(99));
        limiter.refund(3);
        assert_eq!(limiter.acquire_up_to(2, &token).await, Some(2));
        token.cancel();
        let empty = RateLimiter::new(0.001, 1);
        assert_eq!(empty.acquire_up_to(1, &token).await, Some(1));
        assert_eq!(empty.acquire_up_to(1, &token).await, None);
    }

    #[tokio::test(start_paused = true)]
    async fn tiny_rates_wait_without_overflowing() {
        let limiter = RateLimiter::new(1e-30, 1);
        let token = CancellationToken::new();
        assert_eq!(limiter.acquire_up_to(1, &token).await, Some(1));
        let waiting = limiter.acquire_up_to(1, &token);
        tokio::pin!(waiting);
        assert!(tokio::time::timeout(Duration::from_secs(2 * 3600), &mut waiting).await.is_err());
        token.cancel();
        assert_eq!(waiting.await, None);
    }
}
