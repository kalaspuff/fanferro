//! Small helpers shared across modules.

use std::time::Duration;

use crate::error::{Error, Result};

/// Converts a non-negative, finite number of seconds into a [`Duration`].
pub fn duration_from_secs(secs: f64, what: &str) -> Result<Duration> {
    if !secs.is_finite() || secs < 0.0 {
        return Err(Error::config(format!("{what} must be a non-negative number of seconds, got {secs}")));
    }
    Duration::try_from_secs_f64(secs).map_err(|err| Error::config(format!("{what} is out of range: {err}")))
}

/// Rounds `duration` up to whole seconds: SQS takes visibility timeouts and delays in seconds, and
/// a timeout rounded down could make a message visible again early (0.5 s would become 0).
pub fn whole_seconds(duration: Duration) -> Duration {
    let secs = duration.as_secs();
    Duration::from_secs(if duration.subsec_nanos() > 0 { secs.saturating_add(1) } else { secs })
}

/// Converts an optional number of seconds into an optional [`Duration`].
pub fn optional_duration(secs: Option<f64>, what: &str) -> Result<Option<Duration>> {
    secs.map(|secs| duration_from_secs(secs, what)).transpose()
}

/// Serde helpers for durations expressed as floating point seconds.
pub mod serde_secs {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer};

    /// Deserializes `f64` seconds into a `Duration`.
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let secs = f64::deserialize(deserializer)?;
        super::duration_from_secs(secs, "duration").map_err(serde::de::Error::custom)
    }

    /// Optional variant.
    pub mod option {
        use std::time::Duration;

        use serde::{Deserialize, Deserializer};

        /// Deserializes optional `f64` seconds into an optional `Duration`.
        pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Duration>, D::Error> {
            Option::<f64>::deserialize(deserializer)?
                .map(|secs| crate::util::duration_from_secs(secs, "duration").map_err(serde::de::Error::custom))
                .transpose()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_seconds() {
        assert_eq!(duration_from_secs(1.5, "x").unwrap(), Duration::from_millis(1500));
        assert!(duration_from_secs(-1.0, "x").is_err());
        assert!(duration_from_secs(f64::NAN, "x").is_err());
        assert_eq!(optional_duration(None, "x").unwrap(), None);
    }

    #[test]
    fn rounds_up_to_whole_seconds() {
        assert_eq!(whole_seconds(Duration::ZERO), Duration::ZERO);
        assert_eq!(whole_seconds(Duration::from_millis(1)), Duration::from_secs(1));
        assert_eq!(whole_seconds(Duration::from_millis(1500)), Duration::from_secs(2));
        assert_eq!(whole_seconds(Duration::from_secs(30)), Duration::from_secs(30));
        assert_eq!(whole_seconds(Duration::MAX), Duration::from_secs(u64::MAX));
    }
}
