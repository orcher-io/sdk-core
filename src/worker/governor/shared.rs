//! One budget the work drivers share.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Units of work capacity shared by the drivers of one worker.
///
/// Each driver draws on it with its own weight: a credit of a driver whose
/// weight is 2 takes two units. A driver's own limit stays a ceiling within
/// the budget, so the budget only ever lowers what a driver may hold, never
/// raises it.
///
/// Cloning shares the budget.
#[derive(Clone, Debug)]
pub(crate) struct SharedBudget {
    units: Arc<Semaphore>,
    total: u32,
}

impl SharedBudget {
    /// A budget of `total` units, and never fewer than one.
    pub(crate) fn new(total: u32) -> Self {
        let total = total.max(1);
        Self {
            units: Arc::new(Semaphore::new(total as usize)),
            total,
        }
    }

    /// A driver's share of the budget: each of its credits takes `weight`
    /// units. A weight of zero counts as one, and one above the whole
    /// budget as the whole budget, so a credit can always be had once
    /// everything else is given back.
    pub(crate) fn share(&self, weight: u32) -> BudgetShare {
        BudgetShare {
            budget: self.clone(),
            weight: weight.clamp(1, self.total),
        }
    }

    /// How many units there are in all.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn total(&self) -> u32 {
        self.total
    }

    /// How many units are free.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn available(&self) -> usize {
        self.units.available_permits()
    }
}

/// What one driver draws from a [`SharedBudget`] for each credit.
#[derive(Clone, Debug)]
pub(crate) struct BudgetShare {
    budget: SharedBudget,
    weight: u32,
}

impl BudgetShare {
    /// Take one credit's units, waiting for them if they are not free.
    ///
    /// Cancel safe: dropping the future before it completes takes nothing.
    pub(crate) async fn take(&self) -> OwnedSemaphorePermit {
        Arc::clone(&self.budget.units)
            .acquire_many_owned(self.weight)
            .await
            .expect("a shared budget's semaphore is never closed")
    }

    /// Take one credit's units if they are free now.
    pub(crate) fn try_take(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.budget.units)
            .try_acquire_many_owned(self.weight)
            .ok()
    }

    /// The units each credit takes.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn weight(&self) -> u32 {
        self.weight
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_budget_has_at_least_one_unit() {
        assert_eq!(SharedBudget::new(0).total(), 1);
        assert_eq!(SharedBudget::new(4).total(), 4);
    }

    #[test]
    fn a_weight_is_kept_within_the_budget() {
        let budget = SharedBudget::new(4);
        assert_eq!(budget.share(0).weight(), 1);
        assert_eq!(budget.share(3).weight(), 3);
        assert_eq!(budget.share(9).weight(), 4);
    }

    #[test]
    fn shares_draw_on_the_same_units() {
        let budget = SharedBudget::new(3);
        let heavy = budget.share(2);
        let light = budget.share(1);
        let held = heavy.try_take().expect("two of three units are free");
        assert!(heavy.try_take().is_none(), "one unit left, two needed");
        let one = light.try_take().expect("one unit is free");
        assert!(light.try_take().is_none());
        drop(held);
        assert_eq!(budget.available(), 2);
        drop(one);
        assert_eq!(budget.available(), 3);
    }

    #[tokio::test]
    async fn a_take_waits_for_units_to_come_back() {
        let budget = SharedBudget::new(2);
        let share = budget.share(2);
        let held = share.take().await;
        let waiting = tokio::spawn({
            let share = share.clone();
            async move { share.take().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(held);
        let _permit = waiting.await.expect("the waiting take completes");
        assert_eq!(budget.available(), 0);
    }
}
