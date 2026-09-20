//! Rate gate for work that should happen at most once per period

use std::time::Duration;

use tokio::time::Instant;

/// Periodic gate on a loop that would otherwise pay its cost per item, so a
/// checkpoint costs one fsync per period whatever the tuple rate
pub struct Ticker {
    period: Duration,
    next: Instant,
}

impl Ticker {
    pub fn new(period: Duration) -> Self {
        Self {
            period,
            next: Instant::now() + period,
        }
    }

    /// True at most once per period, false until the first one elapses
    pub fn fire(&mut self) -> bool {
        let now = Instant::now();
        if now < self.next {
            return false;
        }
        self.next = now + self.period;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticker_fires_once_per_period() {
        let mut ticker = Ticker::new(Duration::from_secs(30));
        assert!(!ticker.fire());
        ticker.next = Instant::now();
        assert!(ticker.fire());
        assert!(!ticker.fire());
    }
}
