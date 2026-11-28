//! A driver's credits: how much work it may have claimed from the engine.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use super::shared::BudgetShare;

/// The work a driver may claim, one credit per item.
///
/// A poller takes a credit before it polls, so a driver claims work only
/// while it has room to run it. A poll that brings nothing gives its credit
/// back at once. One that brings work spends the credit on it: the credit is
/// held under the work's token until the driver has reported the work, and
/// then given back. A credit is also given back when the engine's claim on
/// the work lapses, since by then the engine has given the work to someone
/// else: so work the language SDK never answers holds its credit only as
/// long as the engine would wait for it.
///
/// A driver that shares the worker's budget with the others
/// ([`sharing`](Self::sharing)) takes its share of that budget with each
/// credit, and gives it back with the credit: its own limit is then a
/// ceiling within the shared budget.
///
/// Shared by a driver and its pollers.
#[derive(Debug)]
pub(crate) struct Credits {
    slots: Arc<Semaphore>,
    limit: usize,
    /// The worker's shared budget, when the driver draws on one.
    share: Option<BudgetShare>,
    /// Credits spent on claimed work, by the work's token.
    spent: Mutex<HashMap<Vec<u8>, Spent>>,
}

#[derive(Debug)]
struct Spent {
    credit: Credit,
    /// When the engine's claim on the work lapses, if it does.
    lapses: Option<Instant>,
}

/// A credit taken for a poll; given back when dropped, unless spent.
#[derive(Debug)]
pub(crate) struct Credit {
    _own: OwnedSemaphorePermit,
    /// The units of the shared budget taken with it, if the driver shares one.
    _shared: Option<OwnedSemaphorePermit>,
}

impl Credits {
    /// `limit` credits, and never fewer than one: with none, a driver could
    /// claim no work at all.
    pub(crate) fn new(limit: usize) -> Self {
        let limit = limit.max(1);
        Self {
            slots: Arc::new(Semaphore::new(limit)),
            limit,
            share: None,
            spent: Mutex::new(HashMap::new()),
        }
    }

    /// `limit` credits, each also taking `share` of the worker's shared
    /// budget; [`new`](Self::new) when there is none.
    pub(crate) fn sharing(limit: usize, share: Option<BudgetShare>) -> Self {
        Self {
            share,
            ..Self::new(limit)
        }
    }

    /// Whether the driver draws on a budget shared with other drivers.
    pub(crate) fn is_shared(&self) -> bool {
        self.share.is_some()
    }

    /// Take a credit, waiting for one to be given back if none is free.
    ///
    /// Cancel safe: dropping the future before it completes takes no credit.
    pub(crate) async fn acquire(&self) -> Credit {
        loop {
            let acquire = async {
                let own = Arc::clone(&self.slots)
                    .acquire_owned()
                    .await
                    .expect("a credit semaphore is never closed");
                let shared = match &self.share {
                    Some(share) => Some(share.take().await),
                    None => None,
                };
                Credit {
                    _own: own,
                    _shared: shared,
                }
            };
            // A claim that lapses meanwhile gives its credit, and its share
            // of the budget, back: then try again.
            return match self.reclaim_lapsed() {
                None => acquire.await,
                Some(lapses) => tokio::select! {
                    credit = acquire => credit,
                    () = tokio::time::sleep_until(lapses) => continue,
                },
            };
        }
    }

    /// Take a credit if one is free now, and its share of the budget with
    /// it, without giving back those whose claim has lapsed.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn try_acquire(&self) -> Option<Credit> {
        let own = Arc::clone(&self.slots).try_acquire_owned().ok()?;
        let shared = match &self.share {
            Some(share) => Some(share.try_take()?),
            None => None,
        };
        Some(Credit {
            _own: own,
            _shared: shared,
        })
    }

    /// Take one more credit for a poll's batch if one is free now, leaving
    /// the shared budget's reserve for the other drivers' polls; see
    /// [`BudgetShare::try_take_extra`]. The same as
    /// [`try_acquire`](Self::try_acquire) for a driver with no shared budget.
    pub(crate) fn try_acquire_extra(&self) -> Option<Credit> {
        let own = Arc::clone(&self.slots).try_acquire_owned().ok()?;
        let shared = match &self.share {
            Some(share) => Some(share.try_take_extra()?),
            None => None,
        };
        Some(Credit {
            _own: own,
            _shared: shared,
        })
    }

    /// Spend `credit` on the work named by `token`, until
    /// [`give_back`](Self::give_back) or until the engine's claim lapses
    /// `lapse` from now.
    ///
    /// Work claimed again under a token already held gives the earlier
    /// credit back: it is the same work.
    pub(crate) fn spend(&self, credit: Credit, token: Vec<u8>, lapse: Option<Duration>) {
        let spent = Spent {
            credit,
            lapses: lapse.map(|lapse| Instant::now() + lapse),
        };
        self.ledger().insert(token, spent);
    }

    /// The credit spent on the work named by `token`, to be dropped once its
    /// report is sent; `None` for work claimed without one.
    pub(crate) fn give_back(&self, token: &[u8]) -> Option<Credit> {
        self.ledger().remove(token).map(|spent| spent.credit)
    }

    /// How many credits there are in all.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn limit(&self) -> usize {
        self.limit
    }

    /// How many credits are free.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn available(&self) -> usize {
        self.slots.available_permits()
    }

    /// Give back every credit whose claim has lapsed, and return when the
    /// next one lapses.
    fn reclaim_lapsed(&self) -> Option<Instant> {
        let now = Instant::now();
        let mut ledger = self.ledger();
        ledger.retain(|_, spent| spent.lapses.is_none_or(|lapses| lapses > now));
        ledger.values().filter_map(|spent| spent.lapses).min()
    }

    fn ledger(&self) -> std::sync::MutexGuard<'_, HashMap<Vec<u8>, Spent>> {
        // The ledger is only ever inserted into and removed from, so one
        // left by a panicking holder is still whole.
        self.spent
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Polls, SharedBudget};
    use super::*;

    const SOON: Duration = Duration::from_millis(50);

    #[test]
    fn there_is_at_least_one_credit() {
        assert_eq!(Credits::new(0).limit(), 1);
        assert_eq!(Credits::new(3).limit(), 3);
    }

    #[tokio::test]
    async fn a_credit_not_spent_is_given_back_when_dropped() {
        let credits = Credits::new(1);
        let credit = credits.acquire().await;
        assert_eq!(credits.available(), 0);
        drop(credit);
        assert_eq!(credits.available(), 1);
    }

    #[tokio::test]
    async fn a_spent_credit_is_held_until_given_back() {
        let credits = Credits::new(1);
        let credit = credits.acquire().await;
        credits.spend(credit, b"tok".to_vec(), None);
        assert!(tokio::time::timeout(SOON, credits.acquire()).await.is_err());

        let back = credits.give_back(b"tok").expect("spent on tok");
        assert_eq!(credits.available(), 0, "held until the report is sent");
        drop(back);
        assert_eq!(credits.available(), 1);
        assert!(credits.give_back(b"tok").is_none());
    }

    #[tokio::test]
    async fn work_claimed_again_under_its_token_holds_one_credit() {
        let credits = Credits::new(2);
        let first = credits.acquire().await;
        credits.spend(first, b"tok".to_vec(), None);
        let again = credits.acquire().await;
        credits.spend(again, b"tok".to_vec(), None);
        assert_eq!(credits.available(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_credit_comes_back_when_its_claim_lapses() {
        let credits = Arc::new(Credits::new(1));
        let credit = credits.acquire().await;
        credits.spend(
            credit,
            b"never-answered".to_vec(),
            Some(Duration::from_secs(300)),
        );

        let waiting = tokio::spawn({
            let credits = Arc::clone(&credits);
            async move { credits.acquire().await }
        });
        tokio::time::sleep(Duration::from_secs(299)).await;
        assert!(!waiting.is_finished(), "the claim has not lapsed yet");

        tokio::time::sleep(Duration::from_secs(2)).await;
        let credit = tokio::time::timeout(SOON, waiting)
            .await
            .expect("the lapsed claim's credit is back")
            .unwrap();
        drop(credit);
        assert!(credits.give_back(b"never-answered").is_none());
    }

    #[tokio::test]
    async fn drivers_sharing_a_budget_take_their_units_from_it() {
        let budget = SharedBudget::new(3);
        let workflows = Credits::sharing(10, Some(budget.share(1)));
        let tasks = Credits::sharing(10, Some(budget.share(2)));

        let task = tasks.acquire().await;
        tasks.spend(task, b"task".to_vec(), None);
        assert_eq!(budget.available(), 1);
        assert!(
            tasks.try_acquire().is_none(),
            "one unit left, a task takes two"
        );
        assert_eq!(tasks.available(), 9, "a refused credit is not kept");

        let workflow = workflows.try_acquire().expect("one unit is enough");
        assert!(workflows.try_acquire().is_none(), "the budget is spent");
        assert!(tokio::time::timeout(SOON, tasks.acquire()).await.is_err());

        // Given back, the credit's units are free for either driver.
        drop(tasks.give_back(b"task"));
        drop(workflow);
        assert_eq!(budget.available(), 3);
        assert!(tasks.try_acquire().is_some());
    }

    /// Once the budget and the polls are fitted, every poll both drivers
    /// keep open can hold its credit at once, so neither driver's idle polls
    /// keep the other from polling. Unfitted, the first of these could not.
    #[test]
    fn every_open_poll_can_hold_a_credit_once_fitted() {
        for (units, w, t, p, q) in [
            (4, 1, 1, 4, 4),
            (6, 1, 2, 4, 4),
            (1, 1, 1, 4, 4),
            (3, 9, 1, 2, 2),
            (100, 1, 1, 4, 4),
        ] {
            let fitted = super::super::fit(
                units,
                [
                    Some(Polls {
                        pollers: p,
                        weight: w,
                    }),
                    Some(Polls {
                        pollers: q,
                        weight: t,
                    }),
                ],
            );
            let [workflow, task] = fitted.drivers;
            let budget = SharedBudget::new(fitted.units);
            let workflows = Credits::sharing(100, Some(budget.share(workflow.weight)));
            let tasks = Credits::sharing(100, Some(budget.share(task.weight)));
            let held: Vec<_> = (0..workflow.pollers)
                .map(|_| workflows.try_acquire())
                .chain((0..task.pollers).map(|_| tasks.try_acquire()))
                .collect();
            assert!(
                held.iter().all(Option::is_some),
                "a poll could not hold a credit with {units} units, weights {w}/{t}, polls {p}/{q}: {fitted:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_driver_limit_stays_a_ceiling_within_the_budget() {
        let budget = SharedBudget::new(10);
        let credits = Credits::sharing(2, Some(budget.share(1)));
        let _a = credits.try_acquire().unwrap();
        let _b = credits.try_acquire().unwrap();
        assert!(credits.try_acquire().is_none());
        assert_eq!(budget.available(), 8);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lapsed_claim_gives_its_units_of_the_budget_back() {
        let budget = SharedBudget::new(1);
        let credits = Arc::new(Credits::sharing(5, Some(budget.share(1))));
        let credit = credits.acquire().await;
        credits.spend(credit, b"lost".to_vec(), Some(Duration::from_secs(60)));

        let waiting = tokio::spawn({
            let credits = Arc::clone(&credits);
            async move { credits.acquire().await }
        });
        tokio::time::sleep(Duration::from_secs(61)).await;
        let credit = tokio::time::timeout(SOON, waiting)
            .await
            .expect("the lapsed claim's units are back")
            .unwrap();
        assert_eq!(budget.available(), 0, "the new credit holds them");
        drop(credit);
        assert_eq!(budget.available(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_claim_without_a_lapse_holds_its_credit_until_given_back() {
        let credits = Credits::new(1);
        let credit = credits.acquire().await;
        credits.spend(credit, b"long-running".to_vec(), None);
        assert!(
            tokio::time::timeout(Duration::from_secs(3600), credits.acquire())
                .await
                .is_err()
        );
    }
}
