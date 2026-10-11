//! A driver's slots.

use std::sync::Arc;

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

/// A fixed number of slots, each held by one piece of work until it is done.
///
/// Cloning shares the budget. A slot is taken with [`acquire`](Self::acquire)
/// or [`try_acquire`](Self::try_acquire) and given back when its
/// [`SlotPermit`] is dropped, which also happens when the task holding it
/// panics or is aborted.
#[derive(Clone, Debug)]
pub(crate) struct SlotBudget {
    slots: Arc<Semaphore>,
    limit: usize,
    /// Woken whenever a slot is given back, for [`drained`](Self::drained).
    released: Arc<Notify>,
}

impl SlotBudget {
    /// A budget of `limit` slots, and never fewer than one: with none, a
    /// driver could take no work at all.
    pub(crate) fn new(limit: usize) -> Self {
        let limit = limit.max(1);
        Self {
            slots: Arc::new(Semaphore::new(limit)),
            limit,
            released: Arc::new(Notify::new()),
        }
    }

    /// The budget for a work driver's reports: `max_in_flight` when set,
    /// otherwise the driver's `max_concurrent_executions`.
    pub(crate) fn for_reports(
        max_in_flight: Option<usize>,
        max_concurrent_executions: usize,
    ) -> Self {
        Self::new(max_in_flight.unwrap_or(max_concurrent_executions))
    }

    /// Take a slot, waiting for one to be given back if none is free.
    ///
    /// Cancel safe: dropping the future before it completes takes no slot.
    pub(crate) async fn acquire(&self) -> SlotPermit {
        let permit = Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .expect("a slot budget's semaphore is never closed");
        self.permit(permit)
    }

    /// Take a slot if one is free now.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn try_acquire(&self) -> Option<SlotPermit> {
        Arc::clone(&self.slots)
            .try_acquire_owned()
            .ok()
            .map(|permit| self.permit(permit))
    }

    /// How many slots there are in all.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn limit(&self) -> usize {
        self.limit
    }

    /// How many slots are free.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn available(&self) -> usize {
        self.slots.available_permits()
    }

    /// How many slots are held.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn in_flight(&self) -> usize {
        self.limit.saturating_sub(self.available())
    }

    /// Wait until no slot is held.
    ///
    /// Slots may be taken again meanwhile; this returns at a moment when
    /// every slot was free, and does not stop new work being admitted.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn drained(&self) {
        loop {
            // Registered before the check, so a slot given back between the
            // check and the wait still wakes it.
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            if self.in_flight() == 0 {
                return;
            }
            released.await;
        }
    }

    fn permit(&self, permit: OwnedSemaphorePermit) -> SlotPermit {
        SlotPermit {
            permit: Some(permit),
            released: Arc::clone(&self.released),
        }
    }
}

/// One slot of a [`SlotBudget`], given back when dropped.
#[derive(Debug)]
pub(crate) struct SlotPermit {
    permit: Option<OwnedSemaphorePermit>,
    released: Arc<Notify>,
}

impl Drop for SlotPermit {
    fn drop(&mut self) {
        // The slot is given back before anyone waiting on a drain is woken,
        // so they find it free.
        drop(self.permit.take());
        self.released.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const SOON: Duration = Duration::from_millis(50);

    #[test]
    fn a_budget_has_at_least_one_slot() {
        assert_eq!(SlotBudget::new(0).limit(), 1);
        assert_eq!(SlotBudget::new(3).limit(), 3);
    }

    #[test]
    fn reports_are_bounded_by_max_in_flight_when_set() {
        assert_eq!(SlotBudget::for_reports(None, 100).limit(), 100);
        assert_eq!(SlotBudget::for_reports(Some(4), 100).limit(), 4);
        assert_eq!(SlotBudget::for_reports(Some(0), 100).limit(), 1);
        assert_eq!(SlotBudget::for_reports(None, 0).limit(), 1);
    }

    #[tokio::test]
    async fn a_slot_is_given_back_when_its_permit_drops() {
        let budget = SlotBudget::new(2);
        let first = budget.acquire().await;
        let second = budget.acquire().await;
        assert_eq!((budget.available(), budget.in_flight()), (0, 2));

        drop(first);
        assert_eq!((budget.available(), budget.in_flight()), (1, 1));
        drop(second);
        assert_eq!((budget.available(), budget.in_flight()), (2, 0));
    }

    #[tokio::test]
    async fn try_acquire_fails_at_the_limit_and_succeeds_once_a_slot_is_back() {
        let budget = SlotBudget::new(1);
        let held = budget.try_acquire().expect("one slot is free");
        assert!(budget.try_acquire().is_none());

        drop(held);
        assert!(budget.try_acquire().is_some());
    }

    #[tokio::test]
    async fn acquire_waits_at_the_limit_until_a_slot_is_back() {
        let budget = SlotBudget::new(1);
        let held = budget.acquire().await;
        assert!(tokio::time::timeout(SOON, budget.acquire()).await.is_err());

        let waiting = tokio::spawn({
            let budget = budget.clone();
            async move { budget.acquire().await }
        });
        drop(held);
        let permit = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("the waiter gets the slot given back")
            .unwrap();
        assert_eq!(budget.in_flight(), 1);
        drop(permit);
        assert_eq!(budget.in_flight(), 0);
    }

    #[tokio::test]
    async fn clones_share_one_budget() {
        let budget = SlotBudget::new(1);
        let clone = budget.clone();
        let _held = budget.acquire().await;
        assert!(clone.try_acquire().is_none());
        assert_eq!(clone.in_flight(), 1);
    }

    #[tokio::test]
    async fn drained_returns_at_once_with_nothing_in_flight() {
        let budget = SlotBudget::new(3);
        tokio::time::timeout(SOON, budget.drained())
            .await
            .expect("nothing is in flight");
    }

    #[tokio::test]
    async fn drained_waits_for_every_slot_in_flight() {
        let budget = SlotBudget::new(3);
        let first = budget.acquire().await;
        let second = budget.acquire().await;

        let drain = tokio::spawn({
            let budget = budget.clone();
            async move { budget.drained().await }
        });
        tokio::time::sleep(SOON).await;
        assert!(!drain.is_finished(), "two slots are still held");

        drop(first);
        tokio::time::sleep(SOON).await;
        assert!(!drain.is_finished(), "one slot is still held");

        drop(second);
        tokio::time::timeout(Duration::from_secs(5), drain)
            .await
            .expect("the drain ends once the last slot is back")
            .unwrap();
    }

    #[tokio::test]
    async fn a_slot_is_given_back_when_its_holder_panics() {
        let budget = SlotBudget::new(1);
        let permit = budget.acquire().await;
        let holder = tokio::spawn(async move {
            let _permit = permit;
            panic!("the work holding the slot failed");
        });
        assert!(holder.await.unwrap_err().is_panic());

        assert_eq!(budget.in_flight(), 0);
        assert!(budget.try_acquire().is_some());
        tokio::time::timeout(SOON, budget.drained())
            .await
            .expect("the panicked holder's slot is back");
    }

    #[tokio::test]
    async fn a_slot_is_given_back_when_its_holder_is_aborted() {
        let budget = SlotBudget::new(1);
        let permit = budget.acquire().await;
        let holder = tokio::spawn(async move {
            let _permit = permit;
            std::future::pending::<()>().await;
        });
        holder.abort();
        assert!(holder.await.unwrap_err().is_cancelled());

        assert_eq!(budget.in_flight(), 0);
    }
}
