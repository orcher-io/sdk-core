//! The order of a run's journal: what the replayer checks of each entry
//! before anything else.

use crate::error::{Error, Result};
use crate::proto::orcher::v1::{EntryType, JournalEntry};

/// Accepts a run's journal entries one at a time, in order.
///
/// An entry is accepted when its id is the next in sequence, starting at 1,
/// and its type is one this crate knows. A gap or a repeat means the journal
/// is incomplete or out of order, so replaying it would not reproduce the
/// original run.
#[derive(Debug, Default)]
pub(crate) struct JournalSequence {
    /// The id of the last accepted entry; 0 before any.
    last_entry_id: i64,
}

impl JournalSequence {
    /// Accepts the next entry.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidExecutionJournal`] if its id is not the next in
    /// sequence, or [`Error::InvalidEvent`] if its type is unknown. Nothing
    /// changes on error.
    pub(crate) fn accept(&mut self, entry: &JournalEntry) -> Result<()> {
        if entry.entry_id != self.last_entry_id + 1 {
            return Err(Error::InvalidExecutionJournal {
                reason: format!(
                    "Entry ID out of sequence: expected {}, got {}",
                    self.last_entry_id + 1,
                    entry.entry_id
                ),
            });
        }
        EntryType::try_from(entry.entry_type).map_err(|_| Error::InvalidEvent {
            reason: format!("Unknown entry type: {}", entry.entry_type),
        })?;
        self.last_entry_id = entry.entry_id;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(entry_id: i64, entry_type: EntryType) -> JournalEntry {
        JournalEntry {
            entry_id,
            entry_type: entry_type as i32,
            ..Default::default()
        }
    }

    #[test]
    fn entries_in_sequence_are_accepted() {
        let mut journal = JournalSequence::default();
        assert!(journal
            .accept(&entry(1, EntryType::WorkflowExecutionStarted))
            .is_ok());
        assert!(journal.accept(&entry(2, EntryType::TaskScheduled)).is_ok());
        assert!(journal
            .accept(&entry(3, EntryType::WorkflowExecutionCompleted))
            .is_ok());
    }

    #[test]
    fn a_gap_or_a_repeat_is_refused_and_changes_nothing() {
        let mut journal = JournalSequence::default();
        journal
            .accept(&entry(1, EntryType::WorkflowExecutionStarted))
            .unwrap();
        assert!(matches!(
            journal.accept(&entry(3, EntryType::TaskScheduled)),
            Err(Error::InvalidExecutionJournal { .. })
        ));
        assert!(journal.accept(&entry(1, EntryType::TaskScheduled)).is_err());
        assert!(journal.accept(&entry(2, EntryType::TaskScheduled)).is_ok());
    }

    #[test]
    fn an_unknown_entry_type_is_refused() {
        let mut journal = JournalSequence::default();
        let unknown = JournalEntry {
            entry_id: 1,
            entry_type: 9_999,
            ..Default::default()
        };
        assert!(matches!(
            journal.accept(&unknown),
            Err(Error::InvalidEvent { .. })
        ));
        assert!(journal
            .accept(&entry(1, EntryType::WorkflowExecutionStarted))
            .is_ok());
    }
}
