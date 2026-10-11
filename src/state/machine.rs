//! State machines that track the lifecycle of workflows and tasks.
//!
//! They enforce valid state transitions and build the commands a workflow sends to the
//! server.
//!
//! ## Terminology
//!
//! - **Worker**: a process that polls for workflows and tasks and executes them.
//! - **Task**: a unit of work scheduled by a workflow.
//! - **Execution Step**: one point at which workflow code runs and decides what to do next.

use crate::error::{Error, Result};
use crate::proto::orcher::v1::{self as proto, EntryType, JournalEntry};
use crate::types::WorkflowExecution;

/// Lifecycle state of a workflow execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowState {
    /// Created, but no start entry has been processed yet.
    Starting,
    /// Running normally.
    Running,
    /// A complete or fail command has been issued but not yet recorded in the journal.
    Completing,
    /// Completed successfully. Terminal.
    Completed,
    /// Failed. Terminal.
    Failed,
    /// Cancelled. Terminal.
    Cancelled,
    /// Terminated. Terminal.
    Terminated,
}

/// Tracks the state of one workflow execution.
///
/// Journal entries are fed in order through [`process_entry`](Self::process_entry), which
/// updates the state; the command helpers queue commands until
/// [`take_commands`](Self::take_commands) drains them.
///
/// # Examples
///
/// ```
/// use orcher_sdk_core::state::WorkflowStateMachine;
/// use orcher_sdk_core::types::WorkflowExecution;
///
/// let execution = WorkflowExecution::new("my-workflow", "run-123");
/// let state_machine = WorkflowStateMachine::new(execution);
/// ```
#[derive(Debug, Clone)]
pub struct WorkflowStateMachine {
    /// Identifiers of the workflow execution.
    pub execution: WorkflowExecution,

    /// Current state.
    pub state: WorkflowState,

    /// Journal entries processed so far, in order.
    pub execution_journal: Vec<JournalEntry>,

    /// Commands queued but not yet taken.
    pub pending_commands: Vec<proto::Command>,

    /// ID of the last processed journal entry; 0 before any entry.
    pub last_event_id: i64,

    /// Scheduled tasks, keyed by the ID of their `TaskScheduled` entry.
    pub pending_tasks: std::collections::HashMap<i64, PendingTask>,

    /// Started timers, keyed by the ID of their `TimerStarted` entry.
    pub active_timers: std::collections::HashMap<i64, ActiveTimer>,
}

/// A task seen in a `TaskScheduled` journal entry.
///
/// `task_type` and `task_id` are placeholders derived from the entry ID, since the entry's
/// attributes are not read.
#[derive(Debug, Clone)]
pub struct PendingTask {
    pub event_id: i64,
    pub task_type: String,
    pub task_id: String,
}

/// A timer seen in a `TimerStarted` journal entry.
///
/// `timer_id` is a placeholder derived from the entry ID, and `fire_time` is not filled in.
#[derive(Debug, Clone)]
pub struct ActiveTimer {
    pub event_id: i64,
    pub timer_id: String,
    pub fire_time: Option<i64>,
}

impl WorkflowStateMachine {
    /// Creates a state machine in the `Starting` state with an empty journal.
    pub fn new(execution: WorkflowExecution) -> Self {
        Self {
            execution,
            state: WorkflowState::Starting,
            execution_journal: Vec::new(),
            pending_commands: Vec::new(),
            last_event_id: 0,
            pending_tasks: std::collections::HashMap::new(),
            active_timers: std::collections::HashMap::new(),
        }
    }

    /// Builds a state machine by processing every entry of `journal` in order, for replay.
    ///
    /// # Errors
    ///
    /// Returns the first error from [`process_entry`](Self::process_entry).
    pub fn from_journal(execution: WorkflowExecution, journal: Vec<JournalEntry>) -> Result<Self> {
        let mut state_machine = Self::new(execution);

        for entry in &journal {
            state_machine.process_entry(entry)?;
        }

        Ok(state_machine)
    }

    /// Applies one journal entry to the state and appends it to the journal.
    ///
    /// Entry IDs must be consecutive, starting at 1.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidExecutionJournal`] if the entry ID is not the next in sequence,
    /// or [`Error::InvalidEvent`] if the entry type is unknown. On error the state is
    /// unchanged.
    pub fn process_entry(&mut self, entry: &JournalEntry) -> Result<()> {
        // A gap or repeat means the journal is incomplete or out of order, and replaying it
        // would not reproduce the original run.
        if entry.entry_id != self.last_event_id + 1 {
            return Err(Error::InvalidExecutionJournal {
                reason: format!(
                    "Entry ID out of sequence: expected {}, got {}",
                    self.last_event_id + 1,
                    entry.entry_id
                ),
            });
        }

        let entry_type =
            EntryType::try_from(entry.entry_type).map_err(|_| Error::InvalidEvent {
                reason: format!("Unknown entry type: {}", entry.entry_type),
            })?;

        tracing::debug!(
            workflow_id = %self.execution.workflow_id,
            entry_id = entry.entry_id,
            entry_type = ?entry_type,
            "Processing journal entry"
        );

        match entry_type {
            EntryType::WorkflowExecutionStarted => {
                self.state = WorkflowState::Running;
            }
            EntryType::WorkflowExecutionCompleted => {
                self.state = WorkflowState::Completed;
            }
            EntryType::WorkflowExecutionFailed => {
                self.state = WorkflowState::Failed;
            }
            EntryType::WorkflowExecutionCanceled => {
                self.state = WorkflowState::Cancelled;
            }
            EntryType::WorkflowExecutionTerminated => {
                self.state = WorkflowState::Terminated;
            }
            EntryType::ExecutionStepScheduled => {
                // Execution step entries do not change the tracked state.
            }
            EntryType::ExecutionStepStarted => {
                // No state change.
            }
            EntryType::ExecutionStepCompleted => {
                // No state change.
            }
            EntryType::TaskScheduled => {
                self.pending_tasks.insert(
                    entry.entry_id,
                    PendingTask {
                        event_id: entry.entry_id,
                        task_type: format!("task-{}", entry.entry_id),
                        task_id: format!("task-{}", entry.entry_id),
                    },
                );
            }
            EntryType::TaskStarted => {
                // No state change. Removing the task from `pending_tasks` would need the
                // scheduled entry's ID, which this entry is not correlated with.
            }
            EntryType::TaskCompleted => {
                // No state change.
            }
            EntryType::TaskFailed => {
                // No state change.
            }
            EntryType::TimerStarted => {
                self.active_timers.insert(
                    entry.entry_id,
                    ActiveTimer {
                        event_id: entry.entry_id,
                        timer_id: format!("timer-{}", entry.entry_id),
                        fire_time: None,
                    },
                );
            }
            EntryType::TimerFired => {
                // No state change: fired timers are not removed from `active_timers`.
            }
            EntryType::TimerCanceled => {
                // No state change: canceled timers are not removed from `active_timers`.
            }
            EntryType::EventReceived => {
                // No state change; the workflow code handles the event.
            }
            _ => {
                // Other entry types are recorded in the journal but change no state.
                tracing::debug!("Unhandled entry type: {:?}", entry_type);
            }
        }

        self.last_event_id = entry.entry_id;

        self.execution_journal.push(entry.clone());

        Ok(())
    }

    /// Queues a command for the server.
    pub fn add_command(&mut self, command: proto::Command) {
        tracing::debug!(
            workflow_id = %self.execution.workflow_id,
            command_type = command.command_type,
            "Adding command"
        );
        self.pending_commands.push(command);
    }

    /// Queues a `ScheduleTask` command.
    ///
    /// The command has no task queue, timeouts, retry policy or headers set.
    pub fn schedule_task(&mut self, task_id: String, task_type: String, input: Vec<u8>) {
        use proto::CommandType;

        let command = proto::Command {
            command_type: CommandType::ScheduleTask as i32,
            attributes: Some(proto::command::Attributes::ScheduleTask(
                proto::ScheduleTaskCommandAttributes {
                    task_id,
                    task_type,
                    task_queue: String::new(),
                    input,
                    schedule_to_start_timeout: None,
                    start_to_close_timeout: None,
                    heartbeat_timeout: None,
                    retry_policy: None,
                    headers: std::collections::HashMap::new(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        self.add_command(command);
    }

    /// Queues a `StartTimer` command that fires after `fire_after_seconds` seconds.
    pub fn start_timer(&mut self, timer_id: String, fire_after_seconds: i64) {
        use proto::CommandType;

        let command = proto::Command {
            command_type: CommandType::StartTimer as i32,
            attributes: Some(proto::command::Attributes::StartTimer(
                proto::StartTimerCommandAttributes {
                    timer_id,
                    start_to_fire_timeout: Some(prost_types::Duration {
                        seconds: fire_after_seconds,
                        nanos: 0,
                    }),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        self.add_command(command);
    }

    /// Queues a `CompleteWorkflow` command and moves to the `Completing` state.
    pub fn complete_workflow(&mut self, result: Vec<u8>) {
        use proto::CommandType;

        let command = proto::Command {
            command_type: CommandType::CompleteWorkflow as i32,
            attributes: Some(proto::command::Attributes::CompleteWorkflow(
                proto::CompleteWorkflowCommandAttributes {
                    result,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        self.add_command(command);
        self.state = WorkflowState::Completing;
    }

    /// Queues a `FailWorkflow` command and moves to the `Completing` state.
    pub fn fail_workflow(&mut self, failure: proto::Failure) {
        use proto::CommandType;

        let command = proto::Command {
            command_type: CommandType::FailWorkflow as i32,
            attributes: Some(proto::command::Attributes::FailWorkflow(
                proto::FailWorkflowCommandAttributes {
                    failure: Some(failure),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        self.add_command(command);
        self.state = WorkflowState::Completing;
    }

    /// Returns the queued commands and empties the queue.
    pub fn take_commands(&mut self) -> Vec<proto::Command> {
        std::mem::take(&mut self.pending_commands)
    }

    /// Returns the journal entries processed so far.
    pub fn execution_journal(&self) -> &[JournalEntry] {
        &self.execution_journal
    }

    /// Returns the number of journal entries processed so far.
    pub fn event_count(&self) -> usize {
        self.execution_journal.len()
    }

    /// Returns `true` if the workflow has completed, failed, been cancelled or been
    /// terminated. `Completing` is not terminal.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            WorkflowState::Completed
                | WorkflowState::Failed
                | WorkflowState::Cancelled
                | WorkflowState::Terminated
        )
    }
}

/// Lifecycle state of a task execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Scheduled but not started.
    Scheduled,
    /// Currently executing.
    Running,
    /// Completed successfully. Terminal.
    Completed,
    /// Failed. Terminal.
    Failed,
    /// Cancelled. Terminal.
    Cancelled,
    /// Timed out. Terminal.
    TimedOut,
}

/// Tracks the state of one task execution.
///
/// Allowed transitions are `Scheduled` to `Running`, then `Running` to `Completed` or
/// `Failed`.
///
/// # Examples
///
/// ```
/// use orcher_sdk_core::state::TaskStateMachine;
///
/// let state_machine = TaskStateMachine::new("task-123");
/// ```
#[derive(Debug, Clone)]
pub struct TaskStateMachine {
    /// Task ID.
    pub task_id: String,

    /// Current state.
    pub state: TaskState,

    /// ID of the schedule entry, for correlating the task with its journal entries.
    pub schedule_id: i64,

    /// Number of failed attempts so far.
    pub retry_count: u32,
}

impl TaskStateMachine {
    /// Creates a state machine in the `Scheduled` state.
    pub fn new(task_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            state: TaskState::Scheduled,
            schedule_id: 0,
            retry_count: 0,
        }
    }

    /// Moves from `Scheduled` to `Running`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidWorkflowState`] if the task is not `Scheduled`.
    pub fn start(&mut self) -> Result<()> {
        if self.state != TaskState::Scheduled {
            return Err(Error::InvalidWorkflowState {
                expected: "Scheduled".to_string(),
                actual: format!("{:?}", self.state),
            });
        }
        self.state = TaskState::Running;
        Ok(())
    }

    /// Moves from `Running` to `Completed`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidWorkflowState`] if the task is not `Running`.
    pub fn complete(&mut self) -> Result<()> {
        if self.state != TaskState::Running {
            return Err(Error::InvalidWorkflowState {
                expected: "Running".to_string(),
                actual: format!("{:?}", self.state),
            });
        }
        self.state = TaskState::Completed;
        Ok(())
    }

    /// Moves from `Running` to `Failed` and increments `retry_count`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidWorkflowState`] if the task is not `Running`.
    pub fn fail(&mut self) -> Result<()> {
        if self.state != TaskState::Running {
            return Err(Error::InvalidWorkflowState {
                expected: "Running".to_string(),
                actual: format!("{:?}", self.state),
            });
        }
        self.state = TaskState::Failed;
        self.retry_count += 1;
        Ok(())
    }

    /// Returns `true` if the task has completed, failed, been cancelled or timed out.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            TaskState::Completed | TaskState::Failed | TaskState::Cancelled | TaskState::TimedOut
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_entry(entry_id: i64, entry_type: EntryType) -> JournalEntry {
        JournalEntry {
            entry_id,
            entry_type: entry_type as i32,
            timestamp: None,
            version: 0,
            task_id: 0,
            attributes: None,
            ..Default::default()
        }
    }

    #[test]
    fn test_workflow_state_machine() {
        let execution = WorkflowExecution::new("test-workflow", "test-run");
        let state_machine = WorkflowStateMachine::new(execution);

        assert_eq!(state_machine.state, WorkflowState::Starting);
        assert!(!state_machine.is_terminal());
    }

    #[test]
    fn test_workflow_event_processing() {
        let execution = WorkflowExecution::new("test-workflow", "test-run");
        let mut state_machine = WorkflowStateMachine::new(execution);

        let start_entry = create_test_entry(1, EntryType::WorkflowExecutionStarted);
        assert!(state_machine.process_entry(&start_entry).is_ok());
        assert_eq!(state_machine.state, WorkflowState::Running);
        assert_eq!(state_machine.last_event_id, 1);

        let complete_entry = create_test_entry(2, EntryType::WorkflowExecutionCompleted);
        assert!(state_machine.process_entry(&complete_entry).is_ok());
        assert_eq!(state_machine.state, WorkflowState::Completed);
        assert!(state_machine.is_terminal());
    }

    #[test]
    fn test_command_generation() {
        let execution = WorkflowExecution::new("test-workflow", "test-run");
        let mut state_machine = WorkflowStateMachine::new(execution);

        state_machine.schedule_task("task-1".to_string(), "MyTask".to_string(), vec![1, 2, 3]);

        let commands = state_machine.take_commands();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            proto::CommandType::ScheduleTask as i32
        );
    }

    #[test]
    fn test_workflow_completion() {
        let execution = WorkflowExecution::new("test-workflow", "test-run");
        let mut state_machine = WorkflowStateMachine::new(execution);

        state_machine.complete_workflow(vec![1, 2, 3]);

        assert_eq!(state_machine.state, WorkflowState::Completing);
        let commands = state_machine.take_commands();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            proto::CommandType::CompleteWorkflow as i32
        );
    }

    #[test]
    fn test_workflow_terminal_states() {
        let execution = WorkflowExecution::new("test", "test");
        let mut sm = WorkflowStateMachine::new(execution);

        sm.state = WorkflowState::Completed;
        assert!(sm.is_terminal());

        sm.state = WorkflowState::Failed;
        assert!(sm.is_terminal());

        sm.state = WorkflowState::Running;
        assert!(!sm.is_terminal());
    }

    #[test]
    fn test_task_state_machine() {
        let mut state_machine = TaskStateMachine::new("task-123");

        assert_eq!(state_machine.state, TaskState::Scheduled);
        assert!(!state_machine.is_terminal());

        assert!(state_machine.start().is_ok());
        assert_eq!(state_machine.state, TaskState::Running);

        assert!(state_machine.complete().is_ok());
        assert_eq!(state_machine.state, TaskState::Completed);
        assert!(state_machine.is_terminal());
    }

    #[test]
    fn test_task_invalid_transitions() {
        let mut state_machine = TaskStateMachine::new("task-123");

        // A task cannot complete before it starts.
        assert!(state_machine.complete().is_err());

        assert!(state_machine.start().is_ok());
        assert!(state_machine.fail().is_ok());
        assert_eq!(state_machine.retry_count, 1);
    }
}
