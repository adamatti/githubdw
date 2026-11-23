//! Primary rate-limit tracking from GraphQL `rateLimit` blocks.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;

/// Tracks the most recent primary rate-limit observation.
#[derive(Debug, Default)]
pub struct RateLimitTracker {
    last_cost: Option<i64>,
    remaining: Option<i64>,
    reset_at: Option<DateTime<Utc>>,
}

impl RateLimitTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the `rateLimit { limit cost remaining resetAt }` block.
    pub fn observe(&mut self, block: &Value) {
        let cost = block.get("cost").and_then(Value::as_i64);
        let remaining = block.get("remaining").and_then(Value::as_i64);
        let reset_at = block
            .get("resetAt")
            .and_then(Value::as_str)
            .and_then(|text| text.parse::<DateTime<Utc>>().ok());

        // Parallel calls can complete out of order. For the same reset window,
        // keep the lowest observed remaining quota and highest cost so an
        // older response cannot make the shared tracker look healthier.
        if self
            .reset_at
            .as_ref()
            .zip(reset_at.as_ref())
            .is_some_and(|(current, incoming)| incoming < current)
        {
            return;
        }
        if self
            .reset_at
            .as_ref()
            .zip(reset_at.as_ref())
            .is_some_and(|(current, incoming)| incoming == current)
        {
            self.last_cost = self.last_cost.max(cost);
            self.remaining = match (self.remaining, remaining) {
                (Some(current), Some(incoming)) => Some(current.min(incoming)),
                (current, incoming) => current.or(incoming),
            };
            return;
        }

        self.last_cost = cost;
        self.remaining = remaining;
        self.reset_at = reset_at;
    }

    /// If remaining budget is below the safety floor, how long to sleep.
    pub fn wait_needed(&self) -> Option<Duration> {
        let remaining = self.remaining?;
        let floor = self.last_cost.unwrap_or(1).max(1) * 2;
        if remaining >= floor {
            return None;
        }
        let reset_at = self.reset_at?;
        let now = Utc::now();
        if reset_at <= now {
            return None;
        }
        (reset_at - now).to_std().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn no_wait_when_budget_is_healthy() {
        let mut tracker = RateLimitTracker::new();
        tracker.observe(&json!({
            "limit": 5000, "cost": 1, "remaining": 4900,
            "resetAt": "2099-01-01T00:00:00Z"
        }));
        assert!(tracker.wait_needed().is_none());
    }

    #[test]
    fn waits_when_remaining_below_floor() {
        let mut tracker = RateLimitTracker::new();
        tracker.observe(&json!({
            "limit": 5000, "cost": 10, "remaining": 5,
            "resetAt": "2099-01-01T00:00:00Z"
        }));
        assert!(tracker.wait_needed().is_some());
    }

    #[test]
    fn no_wait_when_reset_is_in_the_past() {
        let mut tracker = RateLimitTracker::new();
        tracker.observe(&json!({
            "limit": 5000, "cost": 10, "remaining": 0,
            "resetAt": "2000-01-01T00:00:00Z"
        }));
        assert!(tracker.wait_needed().is_none());
    }
}
