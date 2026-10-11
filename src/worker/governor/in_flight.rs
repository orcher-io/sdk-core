//! The reports a driver is still sending.

use std::future::Future;

use super::SlotPermit;

/// The reports being sent, each from its own task.
pub(crate) struct InFlight {
    set: tokio::task::JoinSet<()>,
    /// Which driver sends them, capitalized to start a log sentence:
    /// "Workflow driver".
    driver: &'static str,
    /// What they are, in lowercase for the middle of a log sentence:
    /// "workflow completions".
    what: &'static str,
}

impl InFlight {
    pub(crate) fn new(driver: &'static str, what: &'static str) -> Self {
        Self {
            set: tokio::task::JoinSet::new(),
            driver,
            what,
        }
    }

    /// Run `send` in a task of its own, holding `slot` until it has sent its
    /// report or given up on it.
    pub(crate) fn spawn(
        &mut self,
        send: impl Future<Output = ()> + Send + 'static,
        slot: Option<SlotPermit>,
    ) {
        self.set.spawn(async move {
            send.await;
            drop(slot);
        });
    }

    /// Whether no report is being sent.
    pub(crate) fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// Wait for the next report to finish, and hand back how its task ended
    /// for [`reaped`](Self::reaped). `None` when there are none.
    pub(crate) async fn join_next(
        &mut self,
    ) -> Option<std::result::Result<(), tokio::task::JoinError>> {
        self.set.join_next().await
    }

    pub(crate) fn reaped(&self, finished: std::result::Result<(), tokio::task::JoinError>) {
        if let Err(e) = finished {
            if e.is_panic() {
                tracing::error!(error = %e, "Sending {} panicked", self.what);
            }
        }
    }

    /// Give reports still being sent until `stop_by` to finish, then abort the rest.
    ///
    /// Each report stops retrying once its own grace is up. This deadline also
    /// bounds the wait for a report stuck inside a call, and for one started
    /// late in the shutdown.
    pub(crate) async fn finish(mut self, stop_by: tokio::time::Instant) {
        if self.set.is_empty() {
            return;
        }
        tracing::info!(
            pending = self.set.len(),
            grace_ms = stop_by
                .saturating_duration_since(tokio::time::Instant::now())
                .as_millis() as u64,
            "Waiting for {} still being sent",
            self.what
        );
        let what = self.what;
        let set = &mut self.set;
        let drained = tokio::time::timeout_at(stop_by, async {
            while let Some(finished) = set.join_next().await {
                if let Err(e) = finished {
                    if e.is_panic() {
                        tracing::error!(error = %e, "Sending {what} panicked");
                    }
                }
            }
        })
        .await;
        if drained.is_err() {
            tracing::warn!(
                abandoned = self.set.len(),
                "Shutdown grace over; abandoning {} still being sent",
                self.what
            );
            self.set.abort_all();
            self.set.detach_all();
        }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        // Reports are still outstanding here only when the driver's run was
        // dropped rather than shut down: its task was aborted, or the runtime
        // stopped. Dropping the set aborts them, so log it; otherwise they
        // would vanish silently.
        if !self.set.is_empty() {
            tracing::warn!(
                abandoned = self.set.len(),
                "{} stopped without a shutdown; abandoning {} still being sent. Stop it \
                 through its shutdown handle to let them finish",
                self.driver,
                self.what
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::governor::SlotBudget;
    use std::time::Duration;

    #[tokio::test]
    async fn a_report_holds_its_slot_until_it_is_sent() {
        let budget = SlotBudget::new(1);
        let mut in_flight = InFlight::new("Test driver", "test reports");
        let (sent, send) = tokio::sync::oneshot::channel::<()>();
        in_flight.spawn(
            async move {
                let _ = send.await;
            },
            Some(budget.acquire().await),
        );
        assert_eq!(budget.in_flight(), 1);
        assert!(!in_flight.is_empty());

        sent.send(()).unwrap();
        let finished = in_flight.join_next().await.unwrap();
        in_flight.reaped(finished);
        assert!(in_flight.is_empty());
        assert_eq!(budget.in_flight(), 0);
    }

    #[tokio::test]
    async fn a_report_that_panics_gives_its_slot_back() {
        let budget = SlotBudget::new(1);
        let mut in_flight = InFlight::new("Test driver", "test reports");
        in_flight.spawn(
            async { panic!("sending the report failed") },
            Some(budget.acquire().await),
        );
        let finished = in_flight.join_next().await.unwrap();
        assert!(finished.as_ref().unwrap_err().is_panic());
        in_flight.reaped(finished);
        assert_eq!(budget.in_flight(), 0);
    }

    #[tokio::test]
    async fn finish_waits_for_reports_within_the_deadline() {
        let budget = SlotBudget::new(2);
        let mut in_flight = InFlight::new("Test driver", "test reports");
        for _ in 0..2 {
            in_flight.spawn(
                tokio::time::sleep(Duration::from_millis(20)),
                Some(budget.acquire().await),
            );
        }
        in_flight
            .finish(tokio::time::Instant::now() + Duration::from_secs(5))
            .await;
        assert_eq!(budget.in_flight(), 0);
    }

    #[tokio::test]
    async fn finish_abandons_reports_still_sending_at_the_deadline() {
        let budget = SlotBudget::new(1);
        let mut in_flight = InFlight::new("Test driver", "test reports");
        in_flight.spawn(std::future::pending(), Some(budget.acquire().await));
        let started = tokio::time::Instant::now();
        in_flight.finish(started + Duration::from_millis(50)).await;
        assert!(started.elapsed() < Duration::from_secs(5));
        // The aborted report gives its slot back once its task is torn down.
        tokio::time::timeout(Duration::from_secs(5), budget.drained())
            .await
            .expect("the abandoned report's slot is back");
    }
}
