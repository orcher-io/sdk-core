//! The workloads' workflow and task code, written against the kernel
//! protocol the way a language SDK runs them.
//!
//! A workflow's input is a [`Program`]. On each activation the program is
//! replayed from the start, as every SDK does (kernel protocol v1, §4): each
//! durable call is answered from the activation's resolutions if it has one,
//! and the first one that has none is issued as a new command and suspends
//! the run. Step ids follow the kernel's `KERNEL_V1` rules (`task-3`,
//! `timer-4`), supplied by the program as a language SDK supplies them today.
//!
//! A task's input is a [`TaskSpec`]: how long it runs, how big a result it
//! returns, and how often its code heartbeats.

use std::collections::HashMap;

use orcher_sdk_core::kernel::step_ids;
use orcher_sdk_core::proto::kernel::v1::{
    command, outcome, resolution, run_activation_result, AwaitEvent, Command, Commands,
    CompleteRun, FailRun, Failure, Payload, RunActivation, RunActivationResult, RunTask,
    StartTimer,
};
use orcher_sdk_core::proto::prost_types;
use serde::{Deserialize, Serialize};

/// The workflow type every workload runs.
pub const WORKFLOW_TYPE: &str = "bench.program";
/// The task type every workload runs.
pub const TASK_TYPE: &str = "bench.task";
/// The event name the event-heavy workload sends and waits for.
pub const EVENT_NAME: &str = "bench.ping";

/// What a workflow does, sent as its input.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "shape", rename_all = "snake_case")]
pub enum Program {
    /// `n` tasks, one after another.
    Sequence { n: u64, task: TaskSpec },
    /// `n` tasks all issued at once, then waited for together.
    FanOut { n: u64, task: TaskSpec },
    /// `n` timers of `ms` milliseconds, one after another.
    Timers { n: u64, ms: u64 },
    /// `n` waits for the event [`EVENT_NAME`], one after another.
    Events { n: u64 },
}

/// What a task does, sent as its input.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskSpec {
    /// How long the task runs.
    #[serde(default)]
    pub sleep_ms: u64,
    /// The size of the task's result, a JSON string of this many bytes plus
    /// its quotes.
    #[serde(default)]
    pub result_bytes: usize,
    /// How often the task's own code heartbeats while it runs; 0 never (the
    /// driver's automatic heartbeats still run).
    #[serde(default)]
    pub heartbeat_every_ms: u64,
    /// The task's heartbeat timeout; 0 leaves it to the engine.
    #[serde(default)]
    pub heartbeat_timeout_ms: u64,
    /// The task queue the task runs on; empty is the workflow's.
    #[serde(default)]
    pub task_queue: String,
}

/// What one activation of a program came to.
pub struct Answer {
    pub result: RunActivationResult,
    /// The run ended (completed or failed) with this answer.
    pub ended: bool,
    /// The run failed with this answer.
    pub failed: bool,
}

/// Runs the program an activation's run was started with against the
/// activation's resolutions, and answers it.
pub fn answer(activation: &RunActivation) -> Answer {
    let mut steps: HashMap<&str, &resolution::What> = HashMap::new();
    let mut events = 0u64;
    let mut input = None;
    for resolution in &activation.resolutions {
        match resolution.what.as_ref() {
            Some(resolution::What::Started(started)) => {
                input = started.input.first().map(|payload| payload.data.clone());
            }
            Some(resolution::What::Event(event)) if event.name == EVENT_NAME => events += 1,
            Some(what) if !resolution.step_id.is_empty() => {
                steps.insert(resolution.step_id.as_str(), what);
            }
            _ => {}
        }
    }
    let program = match input.as_deref().map(parse_program) {
        Some(Ok(program)) => program,
        Some(Err(error)) => return failed(activation, format!("bad program: {error}")),
        None => return failed(activation, "the run has no input".to_string()),
    };

    let mut run = Run {
        commands: Vec::new(),
        seq: 0,
        blocked: false,
        failure: None,
    };
    match &program {
        Program::Sequence { n, task } => {
            for _ in 0..*n {
                run.task(task, &steps);
                if run.blocked || run.failure.is_some() {
                    break;
                }
            }
        }
        Program::FanOut { n, task } => {
            for _ in 0..*n {
                run.task(task, &steps);
            }
        }
        Program::Timers { n, ms } => {
            for _ in 0..*n {
                run.timer(*ms, &steps);
                if run.blocked {
                    break;
                }
            }
        }
        Program::Events { n } => {
            for i in 0..*n {
                run.await_event(i < events);
                if run.blocked {
                    break;
                }
            }
        }
    }

    let id = activation.activation_id.clone();
    if let Some(message) = run.failure.take() {
        run.end(command::What::FailRun(FailRun {
            failure: Some(Failure {
                message,
                error_type: "BenchStepFailed".to_string(),
                non_retryable: true,
                ..Default::default()
            }),
            ..Default::default()
        }));
        return Answer {
            result: commands(id, run.commands),
            ended: true,
            failed: true,
        };
    }
    if run.blocked {
        return Answer {
            result: commands(id, run.commands),
            ended: false,
            failed: false,
        };
    }
    run.end(command::What::CompleteRun(CompleteRun {
        result: Some(json(br#"{"ok":true}"#.to_vec())),
        ..Default::default()
    }));
    Answer {
        result: commands(id, run.commands),
        ended: true,
        failed: false,
    }
}

fn parse_program(data: &[u8]) -> Result<Program, serde_json::Error> {
    // The engine hands back the input as the client sent it; some engine
    // versions wrap a run's arguments in a list.
    let value: serde_json::Value = serde_json::from_slice(data)?;
    let value = match value {
        serde_json::Value::Array(mut items) if items.len() == 1 => items.remove(0),
        other => other,
    };
    serde_json::from_value(value)
}

struct Run {
    commands: Vec<Command>,
    seq: u64,
    /// A call had no outcome yet: the run waits.
    blocked: bool,
    /// A step failed: the run fails.
    failure: Option<String>,
}

impl Run {
    fn call(&mut self, what: command::What, settled: bool) {
        let step_id = step_ids::step_id(&what, self.seq).unwrap_or_default();
        self.commands.push(Command {
            seq: self.seq,
            satisfied: settled,
            step_id,
            what: Some(what),
            ..Default::default()
        });
        self.seq += 1;
        if !settled {
            self.blocked = true;
        }
    }

    fn task(&mut self, spec: &TaskSpec, steps: &HashMap<&str, &resolution::What>) {
        let what = |full: bool| {
            command::What::RunTask(RunTask {
                task_type: TASK_TYPE.to_string(),
                task_queue: if full {
                    spec.task_queue.clone()
                } else {
                    String::new()
                },
                input: if full {
                    vec![json(serde_json::to_vec(spec).expect("a task spec encodes"))]
                } else {
                    vec![]
                },
                heartbeat_timeout: (full && spec.heartbeat_timeout_ms > 0)
                    .then(|| millis(spec.heartbeat_timeout_ms)),
                ..Default::default()
            })
        };
        let step_id = format!("task-{}", self.seq);
        match steps.get(step_id.as_str()) {
            Some(resolution::What::Step(step)) => {
                if let Some(outcome::Result::Failure(failure)) =
                    step.outcome.as_ref().and_then(|o| o.result.as_ref())
                {
                    self.failure
                        .get_or_insert_with(|| format!("{step_id}: {}", failure.message));
                }
                // A satisfied call is sent without its payload fields.
                self.call(what(false), true);
            }
            _ => self.call(what(true), false),
        }
    }

    fn timer(&mut self, ms: u64, steps: &HashMap<&str, &resolution::What>) {
        let step_id = format!("timer-{}", self.seq);
        let fired = matches!(
            steps.get(step_id.as_str()),
            Some(resolution::What::Timer(_))
        );
        self.call(
            command::What::StartTimer(StartTimer {
                duration: Some(millis(ms)),
                ..Default::default()
            }),
            fired,
        );
    }

    fn await_event(&mut self, arrived: bool) {
        self.call(
            command::What::AwaitEvent(AwaitEvent {
                name: EVENT_NAME.to_string(),
                timeout: None,
                ..Default::default()
            }),
            arrived,
        );
    }

    fn end(&mut self, what: command::What) {
        self.commands.push(Command {
            seq: self.seq,
            satisfied: false,
            what: Some(what),
            ..Default::default()
        });
    }
}

fn commands(activation_id: Vec<u8>, commands: Vec<Command>) -> RunActivationResult {
    RunActivationResult {
        activation_id,
        outcome: Some(run_activation_result::Outcome::Commands(Commands {
            commands,
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn failed(activation: &RunActivation, message: String) -> Answer {
    Answer {
        result: RunActivationResult {
            activation_id: activation.activation_id.clone(),
            outcome: Some(run_activation_result::Outcome::WorkflowFailed(Failure {
                message,
                error_type: "BenchProgram".to_string(),
                non_retryable: true,
                ..Default::default()
            })),
            ..Default::default()
        },
        ended: true,
        failed: true,
    }
}

/// A JSON payload.
pub fn json(data: Vec<u8>) -> Payload {
    Payload {
        data,
        metadata: [("encoding".to_string(), b"json".to_vec())].into(),
        ..Default::default()
    }
}

/// A task result of `size` bytes: a JSON string.
pub fn task_result(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size + 2);
    data.push(b'"');
    data.resize(size + 1, b'x');
    data.push(b'"');
    data
}

fn millis(ms: u64) -> prost_types::Duration {
    prost_types::Duration {
        seconds: (ms / 1000) as i64,
        nanos: ((ms % 1000) * 1_000_000) as i32,
    }
}
