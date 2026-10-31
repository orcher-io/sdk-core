//! The journals of the runs this worker process has activated, kept between
//! activations so the engine can send only a run's new entries.
//!
//! A poll says the process keeps them (`history_cached`). The engine then may
//! send an activation carrying only the entries after the last one it sent
//! this process for that run (`history_after_entry_id`), and they are joined
//! here to the entries held, so the SDK still receives the run's whole
//! journal and replays it as before. One process, one store: the engine
//! tracks what it sent per worker process, and every poller of the process
//! may be handed any run.
//!
//! Holding nothing is never wrong, only slower. A run evicted, or never seen
//! by this process, cannot be joined; its activation is handed back with
//! `history_missing`, and the engine sends the whole journal next time.

use std::sync::OnceLock;

use lru::LruCache;
use orcher_proto::orcher::v1::{JournalEntry, PollWorkflowExecutionResponse};
use parking_lot::Mutex;
use prost::Message;

/// The process's store; see [`RunHistories::of_process`].
static PROCESS: OnceLock<RunHistories> = OnceLock::new();

/// What the store may hold, in encoded bytes, when nothing else is said.
pub const DEFAULT_HISTORY_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// The activation carried only a run's new entries, and the store does not
/// hold the ones before them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Missing;

struct Held {
    entries: Vec<JournalEntry>,
    bytes: usize,
}

/// The journals of the runs this process has activated, least recently used
/// first out once over budget.
pub(crate) struct RunHistories {
    inner: Mutex<Store>,
}

struct Store {
    runs: LruCache<String, Held>,
    budget: usize,
    used: usize,
}

impl RunHistories {
    pub(crate) fn new(budget: usize) -> Self {
        RunHistories {
            inner: Mutex::new(Store {
                runs: LruCache::unbounded(),
                budget,
                used: 0,
            }),
        }
    }

    /// The process's store. The first poller to ask sets its budget; a
    /// budget of 0 keeps nothing, and its pollers do not say they keep
    /// journals.
    pub(crate) fn of_process(budget: usize) -> &'static RunHistories {
        PROCESS.get_or_init(|| RunHistories::new(budget))
    }

    /// The process's store, if a poller has made one.
    pub(crate) fn of_process_if_any() -> Option<&'static RunHistories> {
        PROCESS.get()
    }

    /// Whether this store keeps anything.
    pub(crate) fn keeps(&self) -> bool {
        self.inner.lock().budget > 0
    }

    /// Make `response` carry its run's whole journal, and keep that journal.
    ///
    /// A whole journal is kept as it came. Only the entries after N are
    /// joined to entries 1 to N held for the run: exactly those, numbered
    /// from 1, and the new ones following on. Anything else is `Missing`,
    /// and the run's entries are dropped.
    pub(crate) fn join(&self, response: &mut PollWorkflowExecutionResponse) -> Result<(), Missing> {
        let run = response.execution_id.clone();
        let after = response.history_after_entry_id;
        let mut store = self.inner.lock();
        if after > 0 {
            let held = store.take(&run).ok_or(Missing)?;
            if held.entries.len() as i64 != after || !follows(&response.journal, after) {
                return Err(Missing);
            }
            let mut whole = held.entries;
            whole.extend(std::mem::take(&mut response.journal));
            response.journal = whole;
            response.history_after_entry_id = 0;
        } else {
            store.take(&run);
        }
        if numbered_from_one(&response.journal) {
            store.keep(run, response.journal.clone());
        }
        Ok(())
    }

    /// Drop what is held for `run`, which will not be activated again.
    pub(crate) fn forget(&self, run: &str) {
        self.inner.lock().take(run);
    }
}

impl Store {
    fn take(&mut self, run: &str) -> Option<Held> {
        let held = self.runs.pop(run)?;
        self.used -= held.bytes;
        Some(held)
    }

    fn keep(&mut self, run: String, entries: Vec<JournalEntry>) {
        let bytes: usize = entries.iter().map(Message::encoded_len).sum();
        if bytes > self.budget {
            return;
        }
        while self.used + bytes > self.budget {
            match self.runs.pop_lru() {
                Some((_, evicted)) => self.used -= evicted.bytes,
                None => break,
            }
        }
        self.used += bytes;
        self.runs.put(run, Held { entries, bytes });
    }
}

/// Whether `entries` are numbered 1, 2, 3 … in order.
fn numbered_from_one(entries: &[JournalEntry]) -> bool {
    entries
        .iter()
        .enumerate()
        .all(|(i, entry)| entry.entry_id == i as i64 + 1)
}

/// Whether `entries` are numbered on from entry `after`.
fn follows(entries: &[JournalEntry], after: i64) -> bool {
    entries
        .iter()
        .enumerate()
        .all(|(i, entry)| entry.entry_id == after + i as i64 + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(ids: std::ops::RangeInclusive<i64>) -> Vec<JournalEntry> {
        ids.map(|entry_id| JournalEntry {
            entry_id,
            ..Default::default()
        })
        .collect()
    }

    fn activation(
        run: &str,
        after: i64,
        journal: Vec<JournalEntry>,
    ) -> PollWorkflowExecutionResponse {
        PollWorkflowExecutionResponse {
            execution_id: run.to_string(),
            history_after_entry_id: after,
            journal,
            ..Default::default()
        }
    }

    fn ids(response: &PollWorkflowExecutionResponse) -> Vec<i64> {
        response.journal.iter().map(|e| e.entry_id).collect()
    }

    /// New entries are joined to those held, and the whole journal kept for
    /// the next activation.
    #[test]
    fn new_entries_join_those_held() {
        let store = RunHistories::new(DEFAULT_HISTORY_CACHE_BYTES);
        let mut first = activation("run", 0, entries(1..=3));
        store.join(&mut first).unwrap();
        assert_eq!(ids(&first), vec![1, 2, 3]);

        let mut second = activation("run", 3, entries(4..=5));
        store.join(&mut second).unwrap();
        assert_eq!(ids(&second), vec![1, 2, 3, 4, 5]);
        assert_eq!(second.history_after_entry_id, 0);

        let mut third = activation("run", 5, entries(6..=6));
        store.join(&mut third).unwrap();
        assert_eq!(ids(&third), (1..=6).collect::<Vec<_>>());
    }

    /// A run never seen, or held up to another entry, is missing, and what
    /// was held for it is dropped.
    #[test]
    fn what_cannot_be_joined_is_missing() {
        let store = RunHistories::new(DEFAULT_HISTORY_CACHE_BYTES);
        assert_eq!(
            store.join(&mut activation("unseen", 3, entries(4..=4))),
            Err(Missing)
        );

        store
            .join(&mut activation("run", 0, entries(1..=3)))
            .unwrap();
        assert_eq!(
            store.join(&mut activation("run", 4, entries(5..=5))),
            Err(Missing)
        );
        // Dropped: even the right position is missing now.
        assert_eq!(
            store.join(&mut activation("run", 3, entries(4..=4))),
            Err(Missing)
        );

        store
            .join(&mut activation("gap", 0, entries(1..=3)))
            .unwrap();
        assert_eq!(
            store.join(&mut activation("gap", 3, entries(5..=5))),
            Err(Missing)
        );
    }

    /// A whole journal replaces whatever was held for the run.
    #[test]
    fn a_whole_journal_replaces_what_was_held() {
        let store = RunHistories::new(DEFAULT_HISTORY_CACHE_BYTES);
        store
            .join(&mut activation("run", 0, entries(1..=5)))
            .unwrap();
        store
            .join(&mut activation("run", 0, entries(1..=2)))
            .unwrap();
        let mut next = activation("run", 2, entries(3..=3));
        store.join(&mut next).unwrap();
        assert_eq!(ids(&next), vec![1, 2, 3]);
    }

    /// Over budget, the least recently used run goes first; a budget of 0
    /// keeps nothing.
    #[test]
    fn the_least_recently_used_run_goes_first() {
        let one = entries(1..=1);
        let size = one.iter().map(Message::encoded_len).sum::<usize>();
        let store = RunHistories::new(size * 2);
        store.join(&mut activation("a", 0, one.clone())).unwrap();
        store.join(&mut activation("b", 0, one.clone())).unwrap();
        store.join(&mut activation("c", 0, one.clone())).unwrap();
        assert_eq!(
            store.join(&mut activation("a", 1, entries(2..=2))),
            Err(Missing)
        );
        store.join(&mut activation("c", 1, entries(2..=2))).unwrap();

        let none = RunHistories::new(0);
        assert!(!none.keeps());
        none.join(&mut activation("a", 0, one)).unwrap();
        assert_eq!(
            none.join(&mut activation("a", 1, entries(2..=2))),
            Err(Missing)
        );
    }

    #[test]
    fn a_forgotten_run_is_missing() {
        let store = RunHistories::new(DEFAULT_HISTORY_CACHE_BYTES);
        store
            .join(&mut activation("run", 0, entries(1..=2)))
            .unwrap();
        store.forget("run");
        assert_eq!(
            store.join(&mut activation("run", 2, entries(3..=3))),
            Err(Missing)
        );
    }
}
