//! A driver's credits: how much work it may have claimed from the engine.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

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
/// Shared by a driver and its pollers.
#[derive(Debug)]
pub(crate) struct Credits {
    slots: Arc<Semaphore>,
    limit: usize,
    /// Credits spent on claimed work, by the work's token.
    spent: Mutex<HashMap<Vec<u8>, Spent>>,
}

#[derive(Debug)]
struct Spent {
    permit: OwnedSemaphorePermit,
    /// When the engine's claim on the work lapses, if it does.
    lapses: Option<Instant>,
}

/// A credit taken for a poll; given back when dropped, unless spent.
#[derive(Debug)]
pub(crate) struct Credit(OwnedSemaphorePermit);

impl Credits {
    /// `limit` credits, and never fewer than one: with none, a driver could
    /// claim no work at all.
    pub(crate) fn new(limit: usize) -> Self {
        let limit = limit.max(1);
        Self {
            slots: Arc::new(Semaphore::new(limit)),
            limit,
            spent: Mutex::new(HashMap::new()),
        }
    }

    /// Take a credit, waiting for one to be given back if none is free.
    ///
    /// Cancel safe: dropping the future before it completes takes no credit.
    pub(crate) async fn acquire(&self) -> Credit {
        loop {
            let acquire = Arc::clone(&self.slots).acquire_owned();
            let permit = match self.reclaim_lapsed() {
                None => acquire.await,
                Some(lapses) => tokio::select! {
                    permit = acquire => permit,
                    () = tokio::time::sleep_until(lapses) => continue,
                },
            };
            return Credit(permit.expect("a credit semaphore is never closed"));
        }
    }

    /// Spend `credit` on the work named by `token`, until
    /// [`give_back`](Self::give_back) or until the engine's claim lapses
    /// `lapse` from now.
    ///
    /// Work claimed again under a token already held gives the earlier
    /// credit back: it is the same work.
    pub(crate) fn spend(&self, credit: Credit, token: Vec<u8>, lapse: Option<Duration>) {
        let spent = Spent {
            permit: credit.0,
            lapses: lapse.map(|lapse| Instant::now() + lapse),
        };
        self.ledger().insert(token, spent);
    }

    /// The credit spent on the work named by `token`, to be dropped once its
    /// report is sent; `None` for work claimed without one.
    pub(crate) fn give_back(&self, token: &[u8]) -> Option<Credit> {
        self.ledger()
            .remove(token)
            .map(|spent| Credit(spent.permit))
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
