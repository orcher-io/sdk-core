//! Replay of a workflow's execution journal.
//!
//! The replayer rebuilds a workflow's state by feeding its execution journal (the
//! event-sourced history of the run) through a [`WorkflowStateMachine`], and reports
//! anything in the journal that would stop the replay from reproducing the original run.
//!
//! ## Determinism
//!
//! The replayer:
//! - replays entries in order to rebuild state;
//! - flags entry IDs that are out of sequence or duplicated;
//! - flags entries the state machine cannot apply;
//! - compares the commands the workflow code issues with the steps the journal recorded
//!   ([`Replayer::replay_with_commands`], [`Replayer::check_commands`]).
//!
//! A step is identified by the id the workflow code gives it: a task's `task_id`, a timer's
//! `timer_id`, a child workflow's `workflow_id`. The engine matches commands to what it already
//! has by that id alone, so a step id reused for different work is served the old work's
//! result. That is what the comparison catches.

use crate::bridge::Command;
use crate::error::{Error, Result};
use crate::proto::orcher::v1::{journal_entry::Attributes, EntryType, JournalEntry};
use crate::state::machine::WorkflowStateMachine;
use crate::types::WorkflowExecution;
use std::collections::{HashMap, HashSet};
use std::fmt;

/// The `failure_type` a workflow activation is failed with when its commands
/// do not match its journal.
///
/// Name it in a workflow's `non_retryable_error_types` to stop the engine
/// starting a fresh run after such a failure.
pub const NON_DETERMINISM_FAILURE_TYPE: &str = "NonDeterminismError";

/// What a [`DeterminismViolation`] is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ViolationKind {
    /// The journal itself is malformed: an entry out of sequence, an entry id
    /// seen twice, an entry that cannot be processed.
    Journal,
    /// The workflow code, replayed against the journal, issued commands that
    /// do not match the steps the journal recorded: the code changed
    /// incompatibly since the journal was written, or it is not deterministic.
    NonDeterminism,
}

/// Which of the workflow code's commands are given to
/// [`Replayer::check_commands`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CommandCoverage {
    /// Every command the workflow code issued, in order, including those for
    /// steps the journal already resolved: what a replay harness records by
    /// running the code once against the whole journal.
    ///
    /// Compared step by step with the journal, so a step changed, removed,
    /// added or moved within the replayed part is found. Steps issued past
    /// the last one the journal recorded are new work, and fine.
    Complete,
    /// Only the commands of one activation: a language SDK issues nothing
    /// for a step the journal already resolved, so an activation carries the
    /// steps still open and the new ones.
    ///
    /// Only a step whose id the journal already recorded can be checked, and
    /// only against what the journal recorded for it.
    Activation,
}

/// How serious a determinism violation is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ViolationSeverity {
    /// May not affect correctness, but should be reviewed.
    Warning,
    /// A determinism violation that may make execution incorrect. Replay continues unless
    /// strict mode is on.
    Error,
    /// A severe violation. Replay stops and the result is marked unsuccessful.
    Critical,
}

/// A determinism problem found while replaying a journal.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DeterminismViolation {
    /// What the violation is about.
    pub kind: ViolationKind,

    /// How serious the violation is.
    pub severity: ViolationSeverity,

    /// ID of the journal entry where the violation was found.
    pub event_id: i64,

    /// The id of the step the violation is about, when it is about one.
    pub step_id: Option<String>,

    /// Human-readable description.
    pub description: String,

    /// The expected value, where one applies.
    pub expected: Option<String>,

    /// The value actually found, where one applies.
    pub actual: Option<String>,
}

impl DeterminismViolation {
    /// Creates a violation of the journal's own consistency, with no expected or actual value.
    pub fn new(severity: ViolationSeverity, event_id: i64, description: impl Into<String>) -> Self {
        Self {
            kind: ViolationKind::Journal,
            severity,
            event_id,
            step_id: None,
            description: description.into(),
            expected: None,
            actual: None,
        }
    }

    /// Creates a violation of the journal's own consistency that records the expected and
    /// actual values.
    pub fn with_values(
        severity: ViolationSeverity,
        event_id: i64,
        description: impl Into<String>,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Self {
        Self {
            expected: Some(expected.into()),
            actual: Some(actual.into()),
            ..Self::new(severity, event_id, description)
        }
    }

    /// Creates a non-determinism violation: at `step_id`, recorded by journal
    /// entry `event_id`, the journal holds `expected` and the workflow code
    /// issued `actual`.
    pub fn non_determinism(
        event_id: i64,
        step_id: impl Into<String>,
        description: impl Into<String>,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Self {
        Self {
            kind: ViolationKind::NonDeterminism,
            step_id: Some(step_id.into()),
            ..Self::with_values(
                ViolationSeverity::Critical,
                event_id,
                description,
                expected,
                actual,
            )
        }
    }
}

impl fmt::Display for DeterminismViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.description)?;
        if let (Some(expected), Some(actual)) = (&self.expected, &self.actual) {
            write!(f, ": expected {expected}, actual {actual}")?;
        }
        Ok(())
    }
}

/// The kinds of step compared with the journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum StepKind {
    Task,
    Timer,
    ChildWorkflow,
}

/// A step, as the journal recorded it or as the workflow code issued it.
#[derive(Debug, Clone)]
struct Step {
    kind: StepKind,
    id: String,
    /// The task type or child workflow type, where it is known.
    work_type: Option<String>,
    /// The journal entry that recorded it; 0 for one the code issued.
    entry_id: i64,
}

impl Step {
    fn from_command(command: &Command) -> Option<Self> {
        let (kind, id, work_type) = match command {
            Command::ScheduleTask(cmd) => (StepKind::Task, &cmd.task_id, Some(&cmd.task_type)),
            Command::StartTimer(cmd) => (StepKind::Timer, &cmd.timer_id, None),
            Command::StartChildWorkflow(cmd) => (
                StepKind::ChildWorkflow,
                &cmd.workflow_id,
                Some(&cmd.workflow_type),
            ),
            _ => return None,
        };
        Some(Self {
            kind,
            id: id.clone(),
            work_type: work_type.cloned(),
            entry_id: 0,
        })
    }

    /// Whether both name the same step doing the same work. A work type the
    /// journal does not know is not held against the code.
    fn matches(&self, issued: &Step) -> bool {
        self.kind == issued.kind
            && self.id == issued.id
            && match (&self.work_type, &issued.work_type) {
                (Some(recorded), Some(issued)) => recorded == issued,
                _ => true,
            }
    }
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (what, typed) = match self.kind {
            StepKind::Task => ("task", "of type"),
            StepKind::Timer => ("timer", ""),
            StepKind::ChildWorkflow => ("child workflow", "of type"),
        };
        write!(f, "{what} {:?}", self.id)?;
        if let Some(work_type) = &self.work_type {
            write!(f, " {typed} {work_type:?}")?;
        }
        Ok(())
    }
}

/// The steps a journal recorded, ready to compare commands with.
#[derive(Debug, Clone, Default)]
pub(crate) struct JournalSteps {
    /// Tasks and timers, in the order the journal recorded them: the engine
    /// journals them as it processes each command, in command order.
    ordered: Vec<Step>,
    /// Every recorded step by kind and id, the first record of each.
    by_id: HashMap<(StepKind, String), Step>,
    /// The journal records that the workflow completed.
    completed: Option<i64>,
}

impl JournalSteps {
    pub(crate) fn from_journal(journal: &[JournalEntry]) -> Self {
        let mut steps = Self::default();
        for entry in journal {
            let step = match &entry.attributes {
                Some(Attributes::TaskScheduled(attrs)) => Step {
                    kind: StepKind::Task,
                    id: attrs.task_id.clone(),
                    work_type: Some(attrs.task_type.clone()).filter(|t| !t.is_empty()),
                    entry_id: entry.entry_id,
                },
                Some(Attributes::TimerStarted(attrs)) => Step {
                    kind: StepKind::Timer,
                    id: attrs.timer_id.clone(),
                    work_type: None,
                    entry_id: entry.entry_id,
                },
                // Not in `ordered`: what a worker reads of a child is its
                // start, journaled when the child starts rather than where
                // its command stood among the others. "unknown" stands for a
                // type the engine did not record.
                Some(Attributes::ChildWorkflowExecutionStarted(attrs)) => Step {
                    kind: StepKind::ChildWorkflow,
                    id: attrs.workflow_id.clone(),
                    work_type: Some(attrs.workflow_type.clone())
                        .filter(|t| !t.is_empty() && t != "unknown"),
                    entry_id: entry.entry_id,
                },
                _ => {
                    if entry.entry_type == EntryType::WorkflowExecutionCompleted as i32 {
                        steps.completed.get_or_insert(entry.entry_id);
                    }
                    continue;
                }
            };
            if step.id.is_empty() {
                continue;
            }
            let key = (step.kind, step.id.clone());
            if steps.by_id.contains_key(&key) {
                continue;
            }
            if step.kind != StepKind::ChildWorkflow {
                steps.ordered.push(step.clone());
            }
            steps.by_id.insert(key, step);
        }
        steps
    }

    /// Compare `commands` with the recorded steps; see [`CommandCoverage`].
    pub(crate) fn check(
        &self,
        commands: &[Command],
        coverage: CommandCoverage,
    ) -> Vec<DeterminismViolation> {
        let issued: Vec<Step> = commands.iter().filter_map(Step::from_command).collect();
        let mut violations = Vec::new();

        // A step id the journal recorded, issued for other work. Children are
        // only checked this way: their place among the steps is not journaled.
        let kinds: &[StepKind] = match coverage {
            CommandCoverage::Complete => &[StepKind::ChildWorkflow],
            CommandCoverage::Activation => {
                &[StepKind::Task, StepKind::Timer, StepKind::ChildWorkflow]
            }
        };
        for step in issued.iter().filter(|s| kinds.contains(&s.kind)) {
            if let Some(recorded) = self.by_id.get(&(step.kind, step.id.clone())) {
                if !recorded.matches(step) {
                    violations.push(DeterminismViolation::non_determinism(
                        recorded.entry_id,
                        &recorded.id,
                        format!(
                            "Non-deterministic workflow: step {:?} (journal entry {}) is \
                             issued for other work than the journal recorded",
                            recorded.id, recorded.entry_id
                        ),
                        recorded.to_string(),
                        step.to_string(),
                    ));
                }
            }
        }
        if coverage == CommandCoverage::Activation {
            return violations;
        }

        // Every step, in order: the code must issue the steps the journal
        // recorded, in the order it recorded them, before any new one. The
        // first difference is reported; everything after it has shifted.
        let ordered: Vec<&Step> = issued
            .iter()
            .filter(|s| s.kind != StepKind::ChildWorkflow)
            .collect();
        for (position, recorded) in self.ordered.iter().enumerate() {
            match ordered.get(position) {
                Some(step) if recorded.matches(step) => {}
                Some(step) => {
                    violations.push(DeterminismViolation::non_determinism(
                        recorded.entry_id,
                        &recorded.id,
                        format!(
                            "Non-deterministic workflow: step {} of the journal (entry {}) is \
                             not what the workflow code issued there",
                            position + 1,
                            recorded.entry_id
                        ),
                        recorded.to_string(),
                        step.to_string(),
                    ));
                    return violations;
                }
                None => {
                    violations.push(DeterminismViolation::non_determinism(
                        recorded.entry_id,
                        &recorded.id,
                        format!(
                            "Non-deterministic workflow: step {} of the journal (entry {}) was \
                             never issued by the workflow code",
                            position + 1,
                            recorded.entry_id
                        ),
                        recorded.to_string(),
                        "nothing",
                    ));
                    return violations;
                }
            }
        }

        // Past the recorded steps is new work, unless the journal says the
        // workflow already completed there.
        if let Some(completed) = self.completed {
            if let Some(step) = ordered.get(self.ordered.len()) {
                violations.push(DeterminismViolation::non_determinism(
                    completed,
                    &step.id,
                    format!(
                        "Non-deterministic workflow: the journal records the workflow \
                         completing (entry {completed}), and the workflow code issued a step \
                         there instead"
                    ),
                    "the workflow to complete",
                    step.to_string(),
                ));
            }
        }
        violations
    }
}

/// Outcome of a replay: the rebuilt state machine and any violations found.
#[derive(Debug)]
pub struct ReplayResult {
    /// State machine rebuilt from the entries replayed. If replay stopped early, it
    /// reflects only the entries before the stopping point.
    pub state_machine: WorkflowStateMachine,

    /// Violations found during replay, in the order found.
    pub violations: Vec<DeterminismViolation>,

    /// `false` if replay stopped early or found a critical violation.
    pub success: bool,
}

impl ReplayResult {
    /// Returns `true` if any violation is `Critical`.
    pub fn has_critical_violations(&self) -> bool {
        self.violations
            .iter()
            .any(|v| v.severity == ViolationSeverity::Critical)
    }

    /// Returns `true` if any violation is `Error`.
    pub fn has_error_violations(&self) -> bool {
        self.violations
            .iter()
            .any(|v| v.severity == ViolationSeverity::Error)
    }

    /// Returns the violations with the given severity.
    pub fn violations_by_severity(
        &self,
        severity: ViolationSeverity,
    ) -> Vec<&DeterminismViolation> {
        self.violations
            .iter()
            .filter(|v| v.severity == severity)
            .collect()
    }
}

/// Rebuilds workflow state by replaying an execution journal and checks it for
/// determinism problems.
///
/// # Determinism checks
///
/// - Sequence: entry IDs must run 1, 2, 3, ... with no gaps.
/// - Duplicates: an entry ID may appear only once.
/// - State transitions: every entry must be accepted by the [`WorkflowStateMachine`].
/// - Commands and non-deterministic operations: hooks run on each entry when enabled, but
///   report nothing, because neither can be judged from journal entries alone.
///
/// A replayer keeps tracking state between calls; [`replay`](Self::replay) clears it at the
/// start, so one replayer can replay many workflows in turn.
///
/// # Examples
///
/// ```rust,ignore
/// use orcher_sdk_core::state::{Replayer, ReplayConfig};
/// use orcher_sdk_core::types::WorkflowExecution;
///
/// let config = ReplayConfig::default();
/// let replayer = Replayer::new(config);
///
/// let execution = WorkflowExecution {
///     workflow_id: "workflow-123".to_string(),
///     run_id: "run-456".to_string(),
/// };
///
/// let journal = vec![]; // JournalEntry list
/// let result = replayer.replay(execution, journal).unwrap();
///
/// if result.has_critical_violations() {
///     eprintln!("Critical determinism violations detected!");
/// }
/// ```
pub struct Replayer {
    config: ReplayConfig,

    /// Entry IDs seen so far in the current replay, to detect duplicates.
    seen_event_ids: HashSet<i64>,
}

/// Configuration for a [`Replayer`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ReplayConfig {
    /// Stop at the first violation of any severity. Default: `false`.
    pub strict_mode: bool,

    /// Treat a gap in the entry ID sequence as `Critical`, which stops replay, rather than
    /// `Error`. Default: `true`.
    pub fail_on_missing_events: bool,

    /// Compare the workflow code's commands with the steps the journal recorded, where the
    /// commands are given: [`Replayer::replay_with_commands`], and every activation a
    /// [`WorkflowDriver`](crate::poller::WorkflowDriver) completes. Default: `true`.
    pub verify_commands: bool,

    /// Maximum number of entries a journal may have; longer journals are rejected. 0 means
    /// unlimited. Default: 0.
    pub max_events: usize,

    /// Has no effect.
    #[deprecated(
        note = "has no effect: non-determinism is found by comparing the workflow's commands \
                with its journal, which `verify_commands` turns on"
    )]
    pub track_non_deterministic_ops: bool,
}

impl Default for ReplayConfig {
    #[allow(deprecated)]
    fn default() -> Self {
        Self {
            strict_mode: false,
            fail_on_missing_events: true,
            verify_commands: true,
            max_events: 0,
            track_non_deterministic_ops: true,
        }
    }
}

impl Replayer {
    /// Creates a replayer with the given configuration.
    pub fn new(config: ReplayConfig) -> Self {
        Self {
            config,
            seen_event_ids: HashSet::new(),
        }
    }

    /// Creates a replayer in strict mode, which stops at the first violation of any
    /// severity.
    pub fn strict() -> Self {
        Self::new(ReplayConfig {
            strict_mode: true,
            ..ReplayConfig::default()
        })
    }

    /// Replays `journal` to rebuild the workflow's state, collecting any violations.
    ///
    /// Replay stops early, with `success` set to `false`, at the first `Critical` violation,
    /// or at any violation in strict mode. Otherwise it runs to the end and `success` is
    /// `true` unless a critical violation was found.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidExecutionJournal`] if the journal is longer than
    /// [`ReplayConfig::max_events`]. Violations are reported in the result, not as errors.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// # use orcher_sdk_core::state::{Replayer, ReplayConfig};
    /// # use orcher_sdk_core::types::WorkflowExecution;
    /// let replayer = Replayer::new(ReplayConfig::default());
    /// let execution = WorkflowExecution {
    ///     workflow_id: "wf-1".to_string(),
    ///     run_id: "run-1".to_string(),
    /// };
    /// let result = replayer.replay(execution, vec![]).unwrap();
    /// assert!(result.success);
    /// ```
    pub fn replay(
        &mut self,
        execution: WorkflowExecution,
        journal: Vec<JournalEntry>,
    ) -> Result<ReplayResult> {
        tracing::info!(
            workflow_id = %execution.workflow_id,
            run_id = %execution.run_id,
            event_count = journal.len(),
            "Starting execution journal replay"
        );

        // Tracking state from a previous replay must not leak into this one.
        self.seen_event_ids.clear();

        let mut violations = Vec::new();
        let mut state_machine = WorkflowStateMachine::new(execution.clone());

        if self.config.max_events > 0 && journal.len() > self.config.max_events {
            return Err(Error::InvalidExecutionJournal {
                reason: format!(
                    "Execution journal exceeds maximum entries: {} > {}",
                    journal.len(),
                    self.config.max_events
                ),
            });
        }

        for (idx, entry) in journal.iter().enumerate() {
            tracing::debug!(
                workflow_id = %execution.workflow_id,
                entry_id = entry.entry_id,
                entry_type = entry.entry_type,
                position = idx,
                "Replaying journal entry"
            );

            if let Some(violation) = self.validate_event_sequence(entry, idx) {
                violations.push(violation.clone());

                if self.config.strict_mode || violation.severity == ViolationSeverity::Critical {
                    return Ok(ReplayResult {
                        state_machine,
                        violations,
                        success: false,
                    });
                }
            }

            // A duplicate entry ID is always critical, whatever the configuration.
            if !self.seen_event_ids.insert(entry.entry_id) {
                let violation = DeterminismViolation::new(
                    ViolationSeverity::Critical,
                    entry.entry_id,
                    format!("Duplicate entry ID detected: {}", entry.entry_id),
                );
                violations.push(violation);

                return Ok(ReplayResult {
                    state_machine,
                    violations,
                    success: false,
                });
            }

            if let Err(e) = state_machine.process_entry(entry) {
                let violation = DeterminismViolation::new(
                    ViolationSeverity::Critical,
                    entry.entry_id,
                    format!("Failed to process entry: {}", e),
                );
                violations.push(violation);

                return Ok(ReplayResult {
                    state_machine,
                    violations,
                    success: false,
                });
            }
        }

        tracing::info!(
            workflow_id = %execution.workflow_id,
            events_replayed = journal.len(),
            violations = violations.len(),
            "Execution journal replay complete"
        );

        let success = violations.is_empty()
            || !violations
                .iter()
                .any(|v| v.severity == ViolationSeverity::Critical);

        Ok(ReplayResult {
            state_machine,
            violations,
            success,
        })
    }

    /// Checks that the entry at `position` (0-based) has ID `position + 1`.
    fn validate_event_sequence(
        &self,
        entry: &JournalEntry,
        position: usize,
    ) -> Option<DeterminismViolation> {
        let expected_id = (position + 1) as i64;

        if entry.entry_id != expected_id {
            return Some(DeterminismViolation::with_values(
                if self.config.fail_on_missing_events {
                    ViolationSeverity::Critical
                } else {
                    ViolationSeverity::Error
                },
                entry.entry_id,
                "Entry ID sequence violation",
                expected_id.to_string(),
                entry.entry_id.to_string(),
            ));
        }

        None
    }

    /// Replays the execution journal, and compares with it the commands the
    /// workflow code issued when run against that journal.
    ///
    /// `commands` is every command the workflow code issued, in order,
    /// including those for steps the journal already resolved
    /// ([`CommandCoverage::Complete`]). A step the code changed, removed,
    /// added or moved within the part of the workflow the journal recorded
    /// is reported as a critical [`ViolationKind::NonDeterminism`] violation,
    /// naming the step, what the journal recorded and what the code issued.
    /// Steps issued past the last one the journal recorded are new work.
    ///
    /// The commands are compared only when
    /// [`verify_commands`](ReplayConfig::verify_commands) is set, and only
    /// with a journal that replayed without a critical violation.
    pub fn replay_with_commands(
        &mut self,
        execution: WorkflowExecution,
        journal: Vec<JournalEntry>,
        commands: &[Command],
    ) -> Result<ReplayResult> {
        let steps = self
            .config
            .verify_commands
            .then(|| JournalSteps::from_journal(&journal));
        let mut result = self.replay(execution, journal)?;
        if let Some(steps) = steps {
            if result.success {
                let violations = steps.check(commands, CommandCoverage::Complete);
                if !violations.is_empty() {
                    for violation in &violations {
                        tracing::error!(
                            step_id = violation.step_id.as_deref().unwrap_or_default(),
                            entry_id = violation.event_id,
                            "{violation}"
                        );
                    }
                    result.success = false;
                    result.violations.extend(violations);
                }
            }
        }
        Ok(result)
    }

    /// Compares the workflow code's `commands` with the steps `journal`
    /// recorded, and returns what does not match.
    ///
    /// `coverage` says which of the code's commands these are; see
    /// [`CommandCoverage`]. Commands that are not steps — step results,
    /// events sent, cancellations, waits — are not compared. Every violation
    /// is of [`ViolationKind::NonDeterminism`].
    pub fn check_commands(
        journal: &[JournalEntry],
        commands: &[Command],
        coverage: CommandCoverage,
    ) -> Vec<DeterminismViolation> {
        JournalSteps::from_journal(journal).check(commands, coverage)
    }

    /// Replays `journal` and returns only the rebuilt state machine.
    ///
    /// Non-critical violations are logged as a warning and otherwise ignored.
    ///
    /// # Errors
    ///
    /// Returns the errors of [`replay`](Self::replay), and
    /// [`Error::DeterminismViolation`] listing every violation if the replay was unsuccessful
    /// or found a critical violation.
    pub fn replay_or_fail(
        &mut self,
        execution: WorkflowExecution,
        journal: Vec<JournalEntry>,
    ) -> Result<WorkflowStateMachine> {
        let result = self.replay(execution, journal)?;

        if !result.success || result.has_critical_violations() {
            let violation_desc = result
                .violations
                .iter()
                .map(|v| format!("Event {}: {}", v.event_id, v))
                .collect::<Vec<_>>()
                .join("; ");

            return Err(Error::DeterminismViolation {
                reason: format!("Replay failed with violations: {}", violation_desc),
            });
        }

        if result.has_error_violations() {
            tracing::warn!(
                violations = result.violations.len(),
                "Replay completed with non-critical violations"
            );
        }

        Ok(result.state_machine)
    }

    /// Clears all tracking state.
    ///
    /// [`replay`](Self::replay) already does this at the start of each call.
    pub fn reset(&mut self) {
        self.seen_event_ids.clear();
    }

    /// Returns the current configuration.
    pub fn config(&self) -> &ReplayConfig {
        &self.config
    }

    /// Replaces the configuration.
    pub fn set_config(&mut self, config: ReplayConfig) {
        self.config = config;
    }
}

impl Default for Replayer {
    fn default() -> Self {
        Self::new(ReplayConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::orcher::v1::WorkflowExecutionStartedEventAttributes;

    fn create_test_execution() -> WorkflowExecution {
        WorkflowExecution {
            workflow_id: "test-workflow".to_string(),
            run_id: "test-run".to_string(),
        }
    }

    fn create_test_event(entry_id: i64, entry_type: EntryType) -> JournalEntry {
        JournalEntry {
            entry_id,
            timestamp: None,
            entry_type: entry_type as i32,
            version: 1,
            task_id: 0,
            attributes: None,
        }
    }

    fn create_workflow_started_event(entry_id: i64) -> JournalEntry {
        use crate::proto::orcher::v1::journal_entry::Attributes;

        JournalEntry {
            entry_id,
            timestamp: None,
            entry_type: EntryType::WorkflowExecutionStarted as i32,
            version: 1,
            task_id: 0,
            attributes: Some(Attributes::WorkflowExecutionStarted(
                WorkflowExecutionStartedEventAttributes {
                    workflow_type: "TestWorkflow".to_string(),
                    task_queue: "test-queue".to_string(),
                    input: vec![],
                    execution_timeout: None,
                    run_timeout: None,
                    task_timeout: None,
                    retry_policy: None,
                    cron_schedule: String::new(),
                    annotations: Default::default(),
                    labels: Default::default(),
                    started_by: "test".to_string(),
                },
            )),
        }
    }

    #[test]
    fn test_replayer_creation() {
        let replayer = Replayer::new(ReplayConfig::default());
        assert!(!replayer.config.strict_mode);
    }

    #[test]
    fn test_replayer_default() {
        let replayer = Replayer::default();
        assert!(!replayer.config.strict_mode);
    }

    #[test]
    fn test_replayer_strict() {
        let replayer = Replayer::strict();
        assert!(replayer.config.strict_mode);
    }

    #[test]
    fn test_replay_empty_history() {
        let mut replayer = Replayer::new(ReplayConfig::default());
        let execution = create_test_execution();
        let result = replayer.replay(execution, vec![]).unwrap();

        assert!(result.success);
        assert_eq!(result.violations.len(), 0);
    }

    #[test]
    fn test_replay_single_event() {
        let mut replayer = Replayer::new(ReplayConfig::default());
        let execution = create_test_execution();
        let history = vec![create_workflow_started_event(1)];

        let result = replayer.replay(execution, history).unwrap();

        assert!(result.success);
        assert_eq!(result.violations.len(), 0);
    }

    #[test]
    fn test_replay_event_sequence() {
        let mut replayer = Replayer::new(ReplayConfig::default());
        let execution = create_test_execution();
        let history = vec![
            create_workflow_started_event(1),
            create_test_event(2, EntryType::ExecutionStepScheduled),
            create_test_event(3, EntryType::ExecutionStepStarted),
        ];

        let result = replayer.replay(execution, history).unwrap();

        assert!(result.success);
        assert_eq!(result.violations.len(), 0);
    }

    #[test]
    fn test_replay_out_of_sequence_events() {
        let mut replayer = Replayer::new(ReplayConfig::default());
        let execution = create_test_execution();

        // Entry IDs 1 and 3: 2 is missing.
        let history = vec![
            create_workflow_started_event(1),
            create_test_event(3, EntryType::ExecutionStepScheduled),
        ];

        let result = replayer.replay(execution, history).unwrap();

        assert!(!result.success || !result.violations.is_empty());
    }

    #[test]
    fn test_replay_duplicate_event_ids() {
        let mut replayer = Replayer::new(ReplayConfig::default());
        let execution = create_test_execution();

        let history = vec![
            create_workflow_started_event(1),
            create_workflow_started_event(1), // Duplicate
        ];

        let result = replayer.replay(execution, history).unwrap();

        assert!(!result.success);
        assert!(result.has_critical_violations());
    }

    #[test]
    fn test_replay_max_events_limit() {
        let mut replayer = Replayer::new(ReplayConfig {
            max_events: 2,
            ..Default::default()
        });
        let execution = create_test_execution();

        let history = vec![
            create_workflow_started_event(1),
            create_test_event(2, EntryType::ExecutionStepScheduled),
            create_test_event(3, EntryType::ExecutionStepStarted),
        ];

        let result = replayer.replay(execution, history);
        assert!(result.is_err());
    }

    #[test]
    fn test_replay_or_fail_success() {
        let mut replayer = Replayer::new(ReplayConfig::default());
        let execution = create_test_execution();
        let history = vec![create_workflow_started_event(1)];

        let state_machine = replayer.replay_or_fail(execution, history).unwrap();
        assert_eq!(state_machine.event_count(), 1);
    }

    #[test]
    fn test_replay_or_fail_with_violations() {
        let mut replayer = Replayer::new(ReplayConfig::default());
        let execution = create_test_execution();

        // Entry 2 is missing.
        let history = vec![
            create_workflow_started_event(1),
            create_test_event(3, EntryType::ExecutionStepScheduled),
        ];

        let result = replayer.replay_or_fail(execution, history);
        assert!(result.is_err());
    }

    #[test]
    fn test_replayer_reset() {
        let mut replayer = Replayer::new(ReplayConfig::default());
        let execution = create_test_execution();
        let history = vec![create_workflow_started_event(1)];

        let _ = replayer.replay(execution.clone(), history.clone()).unwrap();
        assert_eq!(replayer.seen_event_ids.len(), 1);

        replayer.reset();
        assert_eq!(replayer.seen_event_ids.len(), 0);

        let result = replayer.replay(execution, history).unwrap();
        assert!(result.success);
    }

    #[test]
    fn test_violation_severity_checks() {
        let violation = DeterminismViolation::new(ViolationSeverity::Critical, 1, "Test violation");

        assert_eq!(violation.severity, ViolationSeverity::Critical);
    }

    #[test]
    fn test_replay_result_violation_filtering() {
        let mut replayer = Replayer::new(ReplayConfig::default());
        let execution = create_test_execution();

        // Entry 2 is missing, which produces a sequence violation.
        let history = vec![
            create_workflow_started_event(1),
            create_test_event(3, EntryType::ExecutionStepScheduled),
        ];

        let result = replayer.replay(execution, history).unwrap();

        if !result.violations.is_empty() {
            let critical = result.violations_by_severity(ViolationSeverity::Critical);
            let errors = result.violations_by_severity(ViolationSeverity::Error);

            assert!(!critical.is_empty() || !errors.is_empty());
        }
    }

    // ── Commands against the journal ────────────────────────────────────

    use crate::bridge::{
        CompleteWorkflowCommand, RecordStepResultCommand, ScheduleTaskCommand, SendEventCommand,
        StartChildWorkflowCommand, StartTimerCommand, WaitForEventCommand,
    };
    use crate::proto::orcher::v1::{
        journal_entry::Attributes, ChildWorkflowExecutionStartedEventAttributes,
        TaskCompletedEventAttributes, TaskScheduledEventAttributes, TimerStartedEventAttributes,
    };
    use std::time::Duration;

    /// A journal: started, then each of `steps` in order, numbered from 1.
    fn journal(steps: Vec<Attributes>) -> Vec<JournalEntry> {
        std::iter::once(create_workflow_started_event(1))
            .chain(steps.into_iter().enumerate().map(|(i, attributes)| {
                let entry_type = match &attributes {
                    Attributes::TaskScheduled(_) => EntryType::TaskScheduled,
                    Attributes::TaskCompleted(_) => EntryType::TaskCompleted,
                    Attributes::TimerStarted(_) => EntryType::TimerStarted,
                    Attributes::ChildWorkflowExecutionStarted(_) => {
                        EntryType::ChildWorkflowExecutionStarted
                    }
                    Attributes::WorkflowExecutionCompleted(_) => {
                        EntryType::WorkflowExecutionCompleted
                    }
                    other => panic!("no entry type for {other:?} in this test"),
                };
                JournalEntry {
                    entry_id: i as i64 + 2,
                    timestamp: None,
                    entry_type: entry_type as i32,
                    version: 1,
                    task_id: 0,
                    attributes: Some(attributes),
                }
            }))
            .collect()
    }

    fn scheduled(task_id: &str, task_type: &str) -> Attributes {
        Attributes::TaskScheduled(TaskScheduledEventAttributes {
            task_id: task_id.into(),
            task_type: task_type.into(),
            ..Default::default()
        })
    }

    fn task_done(scheduled_event_id: i64) -> Attributes {
        Attributes::TaskCompleted(TaskCompletedEventAttributes {
            scheduled_event_id,
            ..Default::default()
        })
    }

    fn timer_started(timer_id: &str) -> Attributes {
        Attributes::TimerStarted(TimerStartedEventAttributes {
            timer_id: timer_id.into(),
            ..Default::default()
        })
    }

    fn child_started(workflow_id: &str, workflow_type: &str) -> Attributes {
        Attributes::ChildWorkflowExecutionStarted(ChildWorkflowExecutionStartedEventAttributes {
            workflow_id: workflow_id.into(),
            workflow_type: workflow_type.into(),
            ..Default::default()
        })
    }

    fn workflow_completed() -> Attributes {
        Attributes::WorkflowExecutionCompleted(Default::default())
    }

    fn task(task_id: &str, task_type: &str) -> Command {
        Command::ScheduleTask(ScheduleTaskCommand {
            sequence: 0,
            task_id: task_id.into(),
            task_type: task_type.into(),
            task_queue: String::new(),
            input: vec![],
            timeout: Duration::from_secs(10),
            queue_timeout: None,
            heartbeat_timeout: None,
            retry_policy: None,
            headers: vec![],
        })
    }

    fn timer(timer_id: &str) -> Command {
        Command::StartTimer(StartTimerCommand {
            sequence: 0,
            timer_id: timer_id.into(),
            duration: Duration::from_secs(1),
        })
    }

    fn child(workflow_id: &str, workflow_type: &str) -> Command {
        Command::StartChildWorkflow(StartChildWorkflowCommand {
            sequence: 0,
            workflow_id: workflow_id.into(),
            workflow_type: workflow_type.into(),
            task_queue: String::new(),
            input: vec![],
            timeout: None,
            orphan_policy: crate::bridge::OrphanPolicy::Abandon,
        })
    }

    fn complete() -> Command {
        Command::CompleteWorkflow(CompleteWorkflowCommand {
            result: crate::Payload::json(b"{}".to_vec()),
        })
    }

    /// Commands that are not steps, and never count as one.
    fn not_steps() -> Vec<Command> {
        vec![
            Command::RecordStepResult(RecordStepResultCommand {
                step_name: "closure_1".into(),
                step_type: 2,
                result: b"1".to_vec(),
                failure: None,
                execution_attempt: 1,
            }),
            Command::SendEvent(SendEventCommand {
                workflow_id: "other".into(),
                run_id: None,
                event_name: "ping".into(),
                payload: crate::Payload::json(b"{}".to_vec()),
                headers: vec![],
            }),
            Command::WaitForEvent(WaitForEventCommand {
                step_id: "event_go_3".into(),
                event_name: "go".into(),
                timeout_ms: None,
            }),
        ]
    }

    /// Replay `journal` with `commands` as every command the code issued.
    fn replayed(journal: Vec<JournalEntry>, commands: Vec<Command>) -> ReplayResult {
        Replayer::default()
            .replay_with_commands(create_test_execution(), journal, &commands)
            .unwrap()
    }

    /// The one non-determinism violation `result` carries.
    fn the_violation(result: &ReplayResult) -> &DeterminismViolation {
        let found: Vec<_> = result
            .violations
            .iter()
            .filter(|v| v.kind == ViolationKind::NonDeterminism)
            .collect();
        assert_eq!(
            found.len(),
            1,
            "expected one non-determinism violation, got {:?}",
            result.violations
        );
        assert!(
            !result.success,
            "a non-deterministic replay is not a success"
        );
        assert!(result.has_critical_violations());
        assert_eq!(found[0].severity, ViolationSeverity::Critical);
        found[0]
    }

    #[test]
    fn an_unchanged_workflow_replays_clean() {
        let history = journal(vec![
            scheduled("charge_0", "Charge"),
            task_done(2),
            timer_started("timer_1"),
            scheduled("ship_2", "Ship"),
            task_done(5),
            workflow_completed(),
        ]);
        let mut commands = vec![task("charge_0", "Charge"), timer("timer_1")];
        commands.extend(not_steps());
        commands.extend([task("ship_2", "Ship"), complete()]);
        let result = replayed(history, commands);
        assert!(result.success, "{:?}", result.violations);
        assert!(result.violations.is_empty(), "{:?}", result.violations);
    }

    #[test]
    fn a_different_task_type_at_the_same_step_is_reported() {
        let history = journal(vec![scheduled("task_0", "Charge"), task_done(2)]);
        let result = replayed(history, vec![task("task_0", "Refund"), complete()]);
        let violation = the_violation(&result);
        assert_eq!(violation.step_id.as_deref(), Some("task_0"));
        assert_eq!(violation.event_id, 2);
        assert_eq!(
            violation.expected.as_deref(),
            Some(r#"task "task_0" of type "Charge""#)
        );
        assert_eq!(
            violation.actual.as_deref(),
            Some(r#"task "task_0" of type "Refund""#)
        );
        let message = violation.to_string();
        assert!(message.contains("task_0"), "{message}");
        assert!(
            message.contains("Charge") && message.contains("Refund"),
            "{message}"
        );
    }

    #[test]
    fn a_removed_step_is_reported_where_the_journal_has_it() {
        let history = journal(vec![
            scheduled("validate_0", "Validate"),
            task_done(2),
            scheduled("charge_1", "Charge"),
            task_done(4),
        ]);
        let result = replayed(history, vec![task("charge_1", "Charge"), complete()]);
        let violation = the_violation(&result);
        assert_eq!(violation.step_id.as_deref(), Some("validate_0"));
        assert_eq!(violation.event_id, 2);
        assert_eq!(
            violation.expected.as_deref(),
            Some(r#"task "validate_0" of type "Validate""#)
        );
        assert_eq!(
            violation.actual.as_deref(),
            Some(r#"task "charge_1" of type "Charge""#)
        );
    }

    #[test]
    fn a_removed_last_step_is_reported_as_never_issued() {
        let history = journal(vec![
            scheduled("charge_0", "Charge"),
            task_done(2),
            timer_started("timer_1"),
        ]);
        let result = replayed(history, vec![task("charge_0", "Charge"), complete()]);
        let violation = the_violation(&result);
        assert_eq!(violation.step_id.as_deref(), Some("timer_1"));
        assert_eq!(violation.event_id, 4);
        assert_eq!(violation.expected.as_deref(), Some(r#"timer "timer_1""#));
        assert_eq!(violation.actual.as_deref(), Some("nothing"));
    }

    #[test]
    fn a_step_added_inside_the_replayed_part_is_reported() {
        let history = journal(vec![
            scheduled("charge_0", "Charge"),
            task_done(2),
            scheduled("ship_1", "Ship"),
            task_done(4),
        ]);
        let result = replayed(
            history,
            vec![
                task("charge_0", "Charge"),
                task("audit_1", "Audit"),
                task("ship_2", "Ship"),
            ],
        );
        let violation = the_violation(&result);
        assert_eq!(violation.step_id.as_deref(), Some("ship_1"));
        assert_eq!(violation.event_id, 4);
        assert_eq!(
            violation.actual.as_deref(),
            Some(r#"task "audit_1" of type "Audit""#)
        );
    }

    #[test]
    fn reordered_steps_are_reported() {
        let history = journal(vec![scheduled("a", "A"), scheduled("b", "B")]);
        let result = replayed(history, vec![task("b", "B"), task("a", "A")]);
        let violation = the_violation(&result);
        assert_eq!(violation.step_id.as_deref(), Some("a"));
        assert_eq!(
            violation.expected.as_deref(),
            Some(r#"task "a" of type "A""#)
        );
        assert_eq!(violation.actual.as_deref(), Some(r#"task "b" of type "B""#));
    }

    #[test]
    fn a_timer_where_the_journal_has_a_task_is_reported() {
        let history = journal(vec![scheduled("step_0", "Charge")]);
        let result = replayed(history, vec![timer("timer_0")]);
        let violation = the_violation(&result);
        assert_eq!(violation.step_id.as_deref(), Some("step_0"));
        assert_eq!(violation.actual.as_deref(), Some(r#"timer "timer_0""#));
    }

    #[test]
    fn new_steps_after_the_replayed_part_are_fine() {
        let history = journal(vec![scheduled("charge_0", "Charge"), task_done(2)]);
        let result = replayed(
            history,
            vec![
                task("charge_0", "Charge"),
                task("ship_1", "Ship"),
                timer("timer_2"),
                child("child_3", "Notify"),
            ],
        );
        assert!(result.success, "{:?}", result.violations);
        assert!(result.violations.is_empty(), "{:?}", result.violations);
    }

    #[test]
    fn steps_past_a_recorded_completion_are_reported() {
        let history = journal(vec![
            scheduled("charge_0", "Charge"),
            task_done(2),
            workflow_completed(),
        ]);
        let result = replayed(
            history,
            vec![
                task("charge_0", "Charge"),
                task("ship_1", "Ship"),
                complete(),
            ],
        );
        let violation = the_violation(&result);
        assert_eq!(violation.step_id.as_deref(), Some("ship_1"));
        assert_eq!(violation.event_id, 4);
    }

    #[test]
    fn a_child_workflow_of_another_type_is_reported() {
        let history = journal(vec![child_started("child_0", "Notify")]);
        let result = replayed(history, vec![child("child_0", "Invoice")]);
        let violation = the_violation(&result);
        assert_eq!(violation.step_id.as_deref(), Some("child_0"));
        assert_eq!(
            violation.expected.as_deref(),
            Some(r#"child workflow "child_0" of type "Notify""#)
        );
    }

    #[test]
    fn a_child_workflow_type_the_journal_does_not_know_is_not_held_against_the_code() {
        let history = journal(vec![child_started("child_0", "unknown")]);
        let result = replayed(history, vec![child("child_0", "Invoice")]);
        assert!(result.violations.is_empty(), "{:?}", result.violations);
    }

    #[test]
    fn commands_are_not_compared_when_verify_commands_is_off() {
        let history = journal(vec![scheduled("task_0", "Charge")]);
        let result = Replayer::new(ReplayConfig {
            verify_commands: false,
            ..ReplayConfig::default()
        })
        .replay_with_commands(
            create_test_execution(),
            history,
            &[task("task_0", "Refund")],
        )
        .unwrap();
        assert!(result.success);
        assert!(result.violations.is_empty());
    }

    #[test]
    fn a_violation_reads_as_the_step_and_both_sides() {
        let history = journal(vec![scheduled("task_0", "Charge")]);
        let result = replayed(history, vec![task("task_0", "Refund")]);
        assert_eq!(
            the_violation(&result).to_string(),
            r#"Non-deterministic workflow: step 1 of the journal (entry 2) is not what the workflow code issued there: expected task "task_0" of type "Charge", actual task "task_0" of type "Refund""#
        );
    }

    // An activation carries only the steps still open and the new ones.

    #[test]
    fn an_activation_reusing_a_step_id_for_another_task_type_is_reported() {
        let history = journal(vec![scheduled("task_0", "Charge")]);
        let violations = Replayer::check_commands(
            &history,
            &[task("task_0", "Refund")],
            CommandCoverage::Activation,
        );
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert_eq!(violations[0].kind, ViolationKind::NonDeterminism);
        assert_eq!(violations[0].step_id.as_deref(), Some("task_0"));
        assert_eq!(violations[0].event_id, 2);
        assert_eq!(
            violations[0].expected.as_deref(),
            Some(r#"task "task_0" of type "Charge""#)
        );
        assert_eq!(
            violations[0].actual.as_deref(),
            Some(r#"task "task_0" of type "Refund""#)
        );
    }

    #[test]
    fn an_activation_with_open_steps_reissued_and_new_ones_is_fine() {
        // A resolved step issues nothing; an open one is issued again; new
        // ones follow, in any order; and a race the code no longer waits on
        // is simply not issued.
        let history = journal(vec![
            scheduled("charge_0", "Charge"),
            task_done(2),
            scheduled("ship_1", "Ship"),
            timer_started("timer_2"),
            scheduled("audit_3", "Audit"),
        ]);
        let mut commands = vec![
            task("notify_4", "Notify"),
            task("ship_1", "Ship"),
            timer("timer_5"),
            child("child_6", "Invoice"),
        ];
        commands.extend(not_steps());
        commands.push(complete());
        let violations = Replayer::check_commands(&history, &commands, CommandCoverage::Activation);
        assert!(violations.is_empty(), "{violations:?}");
    }

    #[test]
    fn test_config_updates() {
        let mut replayer = Replayer::new(ReplayConfig::default());
        assert!(!replayer.config().strict_mode);

        replayer.set_config(ReplayConfig {
            strict_mode: true,
            ..Default::default()
        });
        assert!(replayer.config().strict_mode);
    }
}
