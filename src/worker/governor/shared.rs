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
            reserve: 0,
        }
    }

    /// How many units there are in all.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn total(&self) -> u32 {
        self.total
    }

    /// How many units are free.
    pub(crate) fn available(&self) -> usize {
        self.units.available_permits()
    }
}

/// What one driver draws from a [`SharedBudget`] for each credit.
#[derive(Clone, Debug)]
pub(crate) struct BudgetShare {
    budget: SharedBudget,
    weight: u32,
    /// Units a batch's extra credits must leave free: what the other
    /// drivers' polls need to hold one credit each. See
    /// [`try_take_extra`](Self::try_take_extra).
    reserve: u32,
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

    /// This share, whose batches leave `reserve` units free for the other
    /// drivers' polls.
    pub(crate) fn reserving(mut self, reserve: u32) -> Self {
        self.reserve = reserve;
        self
    }

    /// Take one more credit's units for a batch, if they are free now and
    /// taking them leaves the reserve free.
    ///
    /// A poll holds every credit it was made with for as long as it waits,
    /// up to a whole long poll. A batch that took the units the other
    /// drivers' polls need would keep those drivers from polling at all
    /// until it ended, with their work waiting in the engine. The first
    /// credit of a poll is not held to this: every poll needs one.
    pub(crate) fn try_take_extra(&self) -> Option<OwnedSemaphorePermit> {
        let taken = self.try_take()?;
        // Taken, then checked: two batches racing can each see the other's
        // units still free, and neither may leave less than the reserve.
        if self.budget.available() < self.reserve as usize {
            return None;
        }
        Some(taken)
    }

    /// The units each credit takes.
    pub(crate) fn weight(&self) -> u32 {
        self.weight
    }
}

/// How a worker's budget and polls are laid out once they fit each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Fitted {
    /// Units in the budget.
    pub(crate) units: u32,
    /// Polls the workflow driver keeps open.
    pub(crate) workflow_pollers: usize,
    /// Polls the task driver keeps open.
    pub(crate) task_pollers: usize,
    /// Units a workflow credit takes, as the fit counted it: share the
    /// budget by this, not by the configured weight, which a raised budget
    /// would clamp differently.
    pub(crate) workflow_weight: u32,
    /// Units a task credit takes, as the fit counted it.
    pub(crate) task_weight: u32,
}

/// Fit a budget of `units` to the polls the drivers keep open.
///
/// Every open poll holds a credit, and its driver's weight in units, for as
/// long as it waits. A budget smaller than that lets one driver's idle polls
/// hold all of it, and the other driver cannot poll until one of them ends,
/// up to a whole long poll, with its work waiting in the engine.
///
/// So the polls are cut, the larger holder first and never below one a
/// driver, until they fit within the budget; only when one poll a driver
/// does not fit is the budget raised, to just what one poll each needs. A
/// driver the worker does not run (`None`) holds nothing. Weights are taken
/// as [`SharedBudget::share`] takes them: within one and the whole budget.
pub(crate) fn fit(
    units: u32,
    workflow_weight: u32,
    task_weight: u32,
    workflow_pollers: Option<usize>,
    task_pollers: Option<usize>,
) -> Fitted {
    let units = units.max(1);
    let w = u64::from(workflow_weight.clamp(1, units));
    let t = u64::from(task_weight.clamp(1, units));
    let mut p = workflow_pollers.map_or(0, |n| n.max(1)) as u64;
    let mut q = task_pollers.map_or(0, |n| n.max(1)) as u64;
    let floor = |p: u64, q: u64| p * w + q * t;
    while floor(p, q) > u64::from(units) && (p > 1 || q > 1) {
        if (p * w >= q * t && p > 1) || q <= 1 {
            p -= 1;
        } else {
            q -= 1;
        }
    }
    Fitted {
        units: units.max(u32::try_from(floor(p, q)).unwrap_or(u32::MAX)),
        workflow_pollers: p as usize,
        task_pollers: q as usize,
        workflow_weight: w as u32,
        task_weight: t as u32,
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

    #[test]
    fn a_batch_leaves_the_reserve_free() {
        let budget = SharedBudget::new(6);
        let workflows = budget.share(1).reserving(2);
        let first = workflows.try_take().expect("a poll's first credit");
        let extras: Vec<_> = std::iter::from_fn(|| workflows.try_take_extra()).collect();
        assert_eq!(extras.len(), 3, "6 units, 1 taken, 2 kept back");
        assert_eq!(budget.available(), 2);
        // The first credit of a poll is not held to the reserve.
        let tasks = budget.share(1).reserving(5);
        let task = tasks.try_take().expect("a poll's first credit");
        assert!(tasks.try_take_extra().is_none());
        assert_eq!(budget.available(), 1, "a refused extra is given back");
        drop((first, extras, task));
    }

    #[test]
    fn a_budget_that_holds_every_poll_is_left_alone() {
        assert_eq!(
            fit(20, 1, 2, Some(4), Some(4)),
            Fitted {
                units: 20,
                workflow_pollers: 4,
                task_pollers: 4,
                workflow_weight: 1,
                task_weight: 2
            }
        );
    }

    #[test]
    fn polls_are_cut_to_fit_the_budget_largest_holder_first() {
        // Four polls a driver hold 4 + 8 = 12 units. The task polls hold
        // more, so they are cut first, then whichever holds more: three
        // workflow polls and one task poll, 5 units, fit in 6.
        assert_eq!(
            fit(6, 1, 2, Some(4), Some(4)),
            Fitted {
                units: 6,
                workflow_pollers: 3,
                task_pollers: 1,
                workflow_weight: 1,
                task_weight: 2
            }
        );
        assert_eq!(
            fit(4, 1, 1, Some(4), Some(4)),
            Fitted {
                units: 4,
                workflow_pollers: 2,
                task_pollers: 2,
                workflow_weight: 1,
                task_weight: 1
            }
        );
    }

    #[test]
    fn a_budget_too_small_for_one_poll_a_driver_is_raised_to_that() {
        assert_eq!(
            fit(1, 1, 1, Some(4), Some(4)),
            Fitted {
                units: 2,
                workflow_pollers: 1,
                task_pollers: 1,
                workflow_weight: 1,
                task_weight: 1
            }
        );
        // A weight above the budget counts as the whole budget.
        assert_eq!(
            fit(3, 9, 1, Some(2), Some(2)),
            Fitted {
                units: 4,
                workflow_pollers: 1,
                task_pollers: 1,
                workflow_weight: 3,
                task_weight: 1
            }
        );
    }

    #[test]
    fn a_driver_the_worker_does_not_run_holds_nothing() {
        assert_eq!(
            fit(3, 1, 1, None, Some(4)),
            Fitted {
                units: 3,
                workflow_pollers: 0,
                task_pollers: 3,
                workflow_weight: 1,
                task_weight: 1
            }
        );
    }
}
