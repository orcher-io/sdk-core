//! Step ids under the kernel's own scheme, `KERNEL_V1`.
//!
//! A step id is the stable key that matches a durable call to its recorded
//! outcome. Under `KERNEL_V1` the kernel assigns every id from the call's kind
//! and its call sequence, so ids are identical whatever language ran the code.
//! Runs started before this scheme keep the ids their SDK supplied (see
//! `docs/kernel-protocol-v1.md` §8.3).
//!
//! The rules:
//!
//! - Every durable call takes the next number of one sequence, from 0, in the
//!   order the workflow code issued it: the command's `seq`.
//! - A step's id is its kind and that number, `task-3`, `timer-4`. The task
//!   type is not part of the id: it is compared on its own, so a changed type
//!   is reported as a changed type, not as another step.
//! - A child started without a workflow id gets one from the run's history
//!   lineage and the number, `{lineage}-child-5`. The lineage is unique per
//!   history, copied by reset and new for a fresh history, so concurrent
//!   parents never share a child id, a reset run reproduces its children's
//!   ids, and a retry never reuses the ids of children its previous attempt
//!   may have left running.
//! - Commands that end the run, cancel a step, or set labels are not steps and
//!   have no id.
//!
//! The scheme is frozen: changing any rule here would change the ids of
//! histories already written under it. A different rule is a new scheme.

use crate::proto::kernel::v1::command;

/// The kind of step a durable call makes, as its id names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StepKind {
    /// A task run: `task-{seq}`.
    Task,
    /// A timer started: `timer-{seq}`.
    Timer,
    /// A child workflow started: `child-{seq}`. The child's workflow id is
    /// separate; see [`default_child_workflow_id`].
    Child,
    /// An event sent to another run: `send-{seq}`.
    SendEvent,
    /// A recorded value: `value-{seq}`.
    Value,
    /// An event waited for: `await-{seq}`.
    AwaitEvent,
}

impl StepKind {
    /// The kind's name in a step id.
    fn name(self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Timer => "timer",
            Self::Child => "child",
            Self::SendEvent => "send",
            Self::Value => "value",
            Self::AwaitEvent => "await",
        }
    }
}

/// The kind of step `call` makes, or `None` for a command that is not a step.
fn kind_of(call: &command::What) -> Option<StepKind> {
    Some(match call {
        command::What::RunTask(_) => StepKind::Task,
        command::What::StartTimer(_) => StepKind::Timer,
        command::What::StartChild(_) => StepKind::Child,
        command::What::SendEvent(_) => StepKind::SendEvent,
        command::What::RecordValue(_) => StepKind::Value,
        command::What::AwaitEvent(_) => StepKind::AwaitEvent,
        command::What::CancelTimer(_)
        | command::What::CancelChild(_)
        | command::What::CompleteRun(_)
        | command::What::FailRun(_)
        | command::What::EndCancelled(_)
        | command::What::RestartFresh(_)
        | command::What::UpsertLabels(_) => return None,
    })
}

/// The step id of a step of kind `kind` made by the durable call issued
/// `seq`-th: what a language SDK matches the call's outcome by, in a run
/// under this scheme.
pub fn id_for(kind: StepKind, seq: u64) -> String {
    format!("{}-{seq}", kind.name())
}

/// The step id of the durable call issued `seq`-th, or `None` for a command
/// that is not a step.
pub(crate) fn step_id(call: &command::What, seq: u64) -> Option<String> {
    kind_of(call).map(|kind| id_for(kind, seq))
}

/// The workflow id of a child started without one, by the call issued
/// `seq`-th in the run whose history lineage is `lineage`.
pub fn default_child_workflow_id(lineage: &str, seq: u64) -> String {
    format!("{lineage}-child-{seq}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::kernel::v1::{
        AwaitEvent, CancelTimer, CompleteRun, RecordValue, RunTask, SendEvent, StartChild,
        StartTimer, UpsertLabels,
    };

    fn task(task_type: &str) -> command::What {
        command::What::RunTask(RunTask {
            task_type: task_type.into(),
            ..Default::default()
        })
    }

    /// The scheme is frozen: these exact ids are what histories written under
    /// `KERNEL_V1` hold. If this test has to change, the change is a new
    /// scheme, not an edit to this one.
    #[test]
    fn the_scheme_is_frozen() {
        let calls: Vec<(command::What, u64, Option<&str>)> = vec![
            (task("charge"), 0, Some("task-0")),
            (
                command::What::StartTimer(StartTimer::default()),
                1,
                Some("timer-1"),
            ),
            (
                command::What::StartChild(StartChild::default()),
                2,
                Some("child-2"),
            ),
            (
                command::What::SendEvent(SendEvent::default()),
                3,
                Some("send-3"),
            ),
            (
                command::What::RecordValue(RecordValue::default()),
                4,
                Some("value-4"),
            ),
            (
                command::What::AwaitEvent(AwaitEvent::default()),
                5,
                Some("await-5"),
            ),
            (command::What::CancelTimer(CancelTimer::default()), 6, None),
            (
                command::What::UpsertLabels(UpsertLabels::default()),
                7,
                None,
            ),
            (command::What::CompleteRun(CompleteRun::default()), 8, None),
        ];
        for (call, seq, expected) in calls {
            assert_eq!(step_id(&call, seq).as_deref(), expected, "{call:?}");
        }
        // And the same ids by kind, as a language SDK asks for them.
        let by_kind = [
            (StepKind::Task, "task-0"),
            (StepKind::Timer, "timer-0"),
            (StepKind::Child, "child-0"),
            (StepKind::SendEvent, "send-0"),
            (StepKind::Value, "value-0"),
            (StepKind::AwaitEvent, "await-0"),
        ];
        for (kind, expected) in by_kind {
            assert_eq!(id_for(kind, 0), expected);
        }
        assert_eq!(
            default_child_workflow_id("3f2a9c1e-7b4d-4e8a-9c2f-1d6b5a4e3c21", 2),
            "3f2a9c1e-7b4d-4e8a-9c2f-1d6b5a4e3c21-child-2"
        );
    }

    /// The same call at the same place has the same id on every replay, in
    /// every language, whatever its type or payload.
    #[test]
    fn an_id_depends_on_kind_and_position_only() {
        assert_eq!(step_id(&task("charge"), 3), step_id(&task("charge"), 3));
        assert_eq!(step_id(&task("charge"), 3), step_id(&task("refund"), 3));
        assert_ne!(step_id(&task("charge"), 3), step_id(&task("charge"), 4));
        assert_ne!(
            step_id(&task("charge"), 3),
            step_id(&command::What::StartTimer(StartTimer::default()), 3)
        );
    }

    /// Concurrent parents have different lineages; a reset copies its
    /// lineage; a retry gets a new one.
    #[test]
    fn child_ids_are_unique_across_parents_and_stable_across_reset() {
        let parent_a = "lineage-a";
        let parent_b = "lineage-b";
        assert_ne!(
            default_child_workflow_id(parent_a, 0),
            default_child_workflow_id(parent_b, 0)
        );
        let reset_of_a = parent_a;
        assert_eq!(
            default_child_workflow_id(parent_a, 0),
            default_child_workflow_id(reset_of_a, 0)
        );
        let retry_of_a = "lineage-a-retry";
        assert_ne!(
            default_child_workflow_id(parent_a, 0),
            default_child_workflow_id(retry_of_a, 0)
        );
    }

    #[test]
    fn ids_use_only_characters_safe_in_every_store() {
        for seq in [0, 9, 10, 1_000_000] {
            let id = step_id(&task("x"), seq).unwrap();
            assert!(
                id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'),
                "{id}"
            );
        }
    }
}
