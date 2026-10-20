//! How many of a driver's pollers poll at once.

use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

/// Scales a driver's pollers between one and all of them by whether their
/// polls bring work.
///
/// A poll that returns work lets one more poller poll, up to all of them; a
/// poll that comes back empty retires the highest-numbered poller still
/// polling, down to one. Pollers are numbered from 0, and the ones polling
/// are always 0 to [`active`](Self::active) - 1, so an idle driver holds one
/// poll and a busy one holds all of them.
#[derive(Debug)]
pub struct PollerScaler {
    active: AtomicUsize,
    max: usize,
    changed: Notify,
}

impl PollerScaler {
    /// A scaler for `max` pollers (at least one), starting with one polling.
    pub fn new(max: usize) -> Self {
        Self {
            active: AtomicUsize::new(1),
            max: max.max(1),
            changed: Notify::new(),
        }
    }

    /// How many pollers may poll now.
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    /// Whether poller `index` may poll now.
    pub fn may_poll(&self, index: usize) -> bool {
        index < self.active()
    }

    /// A poll brought work: let one more poller poll, if any is waiting.
    pub fn worked(&self) {
        let mut active = self.active.load(Ordering::Acquire);
        while active < self.max {
            match self.active.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.changed.notify_waiters();
                    return;
                }
                Err(now) => active = now,
            }
        }
    }

    /// Poller `index`'s poll came back empty: retire it if it is the
    /// highest-numbered poller polling and not the last one.
    pub fn idle(&self, index: usize) {
        if index == 0 {
            return;
        }
        // Only the highest-numbered poller retires, so the pollers polling
        // stay 0 to active - 1.
        let _ = self
            .active
            .compare_exchange(index + 1, index, Ordering::AcqRel, Ordering::Acquire);
    }

    /// Wait until poller `index` may poll.
    pub async fn turn(&self, index: usize) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            // Registered before the check, so a change between the check and
            // the wait is not missed.
            notified.as_mut().enable();
            if self.may_poll(index) {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn an_idle_driver_holds_one_poll() {
        let scaler = PollerScaler::new(4);
        assert_eq!(scaler.active(), 1);
        assert!(scaler.may_poll(0));
        assert!(!scaler.may_poll(1));
        scaler.idle(0);
        assert_eq!(scaler.active(), 1, "the last poller never retires");
    }

    #[test]
    fn work_lets_one_more_poller_poll_up_to_all() {
        let scaler = PollerScaler::new(3);
        scaler.worked();
        assert_eq!(scaler.active(), 2);
        scaler.worked();
        scaler.worked();
        assert_eq!(scaler.active(), 3, "never more than all of them");
    }

    #[test]
    fn only_the_highest_poller_polling_retires() {
        let scaler = PollerScaler::new(4);
        for _ in 0..3 {
            scaler.worked();
        }
        assert_eq!(scaler.active(), 4);
        scaler.idle(1);
        assert_eq!(scaler.active(), 4, "a lower poller keeps polling");
        scaler.idle(3);
        assert_eq!(scaler.active(), 3);
        scaler.idle(2);
        scaler.idle(1);
        assert_eq!(scaler.active(), 1);
    }

    #[test]
    fn at_least_one_poller() {
        let scaler = PollerScaler::new(0);
        scaler.worked();
        assert_eq!(scaler.active(), 1);
    }

    #[tokio::test]
    async fn a_waiting_poller_is_let_in_when_work_arrives() {
        let scaler = Arc::new(PollerScaler::new(2));
        let waiting = tokio::spawn({
            let scaler = Arc::clone(&scaler);
            async move { scaler.turn(1).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished(), "poller 1 waits while one polls");
        scaler.worked();
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("let in")
            .unwrap();
    }
}
