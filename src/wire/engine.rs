//! Conversions between the engine's worker messages (`orcher.v1`) and what
//! the worker handles: the commands, query answers and update answers an
//! activation's result carries, on their way to the engine.

use std::time::Duration;

use crate::types::{StepIdScheme, WorkflowExecution};
use crate::worker::record::Recorded;
use crate::worker::{TaskExecutionTask, WorkflowExecutionTask};

use crate::error::{Error, Result};
use crate::protocol::{
    Command as BridgeCommand, ExecutionErrorType, ExecutionResult, QueryResponse,
    QueryResult as BridgeQueryResult, UpdateResponse, UpdateResult as BridgeUpdateResult,
    WorkflowRetryPolicy,
};
use orcher_proto::orcher::v1 as proto;

/// Convert bridge commands to proto commands.
pub(crate) fn commands(
    commands: Vec<BridgeCommand>,
    default_task_queue: &str,
    namespace: &str,
) -> Result<Vec<orcher_proto::orcher::v1::Command>> {
    use orcher_proto::orcher::v1::{
        command::Attributes, CancelChildWorkflowCommandAttributes, CancelTimerCommandAttributes,
        CancelWorkflowCommandAttributes, Command as ProtoCommand, CommandType,
        CompleteWorkflowCommandAttributes, FailWorkflowCommandAttributes,
        OrphanPolicy as ProtoOrphanPolicy, RecordStepResultCommandAttributes,
        RestartFreshCommandAttributes, RetryPolicy, ScheduleTaskCommandAttributes,
        SendEventCommandAttributes, StartChildWorkflowCommandAttributes,
        StartTimerCommandAttributes,
    };

    let mut proto_commands = Vec::new();

    for command in commands {
        let proto_command = match command {
            BridgeCommand::RecordStepResult(cmd) => ProtoCommand {
                command_type: CommandType::RecordStepResult as i32,
                attributes: Some(Attributes::RecordStepResult(
                    RecordStepResultCommandAttributes {
                        step_name: cmd.step_name,
                        step_type: cmd.step_type,
                        result: cmd.result,
                        failure: cmd.failure.map(|f| orcher_proto::orcher::v1::Failure {
                            message: f.message,
                            source: f.source,
                            stack_trace: f.stack_trace,
                            cause: None,
                            failure_type: f.failure_type,
                            details: vec![],
                            non_retryable: false,
                            ..Default::default()
                        }),
                        execution_attempt: cmd.execution_attempt,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },

            BridgeCommand::ScheduleTask(cmd) => {
                let retry_policy = cmd.retry_policy.as_ref().map(|rp| RetryPolicy {
                    initial_interval: Some(prost_types::Duration {
                        seconds: rp.initial_interval.as_secs() as i64,
                        nanos: (rp.initial_interval.as_nanos() % 1_000_000_000) as i32,
                    }),
                    backoff_coefficient: rp.backoff_coefficient,
                    maximum_interval: Some(prost_types::Duration {
                        seconds: rp.max_interval.as_secs() as i64,
                        nanos: (rp.max_interval.as_nanos() % 1_000_000_000) as i32,
                    }),
                    maximum_attempts: rp.max_attempts as i32,
                    non_retryable_error_types: rp.non_retryable_errors.clone(),
                    ..Default::default()
                });

                let headers = cmd
                    .headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.data.clone()))
                    .collect();

                ProtoCommand {
                    command_type: CommandType::ScheduleTask as i32,
                    attributes: Some(Attributes::ScheduleTask(ScheduleTaskCommandAttributes {
                        task_id: cmd.task_id,
                        task_type: cmd.task_type,
                        task_queue: if cmd.task_queue.is_empty() {
                            default_task_queue.to_string()
                        } else {
                            cmd.task_queue
                        },
                        input: {
                            let mut all_data = Vec::new();
                            for payload in &cmd.input {
                                all_data.extend(&payload.data);
                            }
                            all_data
                        },
                        // The queue-wait limit is a separate option. Absent, the task
                        // waits in the queue as long as it needs to. Inheriting the
                        // run limit here would double the effective total timeout.
                        schedule_to_start_timeout: cmd.queue_timeout.map(|q| {
                            prost_types::Duration {
                                seconds: q.as_secs() as i64,
                                nanos: (q.as_nanos() % 1_000_000_000) as i32,
                            }
                        }),
                        start_to_close_timeout: Some(prost_types::Duration {
                            seconds: cmd.timeout.as_secs() as i64,
                            nanos: (cmd.timeout.as_nanos() % 1_000_000_000) as i32,
                        }),
                        heartbeat_timeout: cmd.heartbeat_timeout.map(|hb| prost_types::Duration {
                            seconds: hb.as_secs() as i64,
                            nanos: (hb.as_nanos() % 1_000_000_000) as i32,
                        }),
                        retry_policy,
                        headers,
                        ..Default::default()
                    })),
                    ..Default::default()
                }
            }

            BridgeCommand::StartTimer(cmd) => ProtoCommand {
                command_type: CommandType::StartTimer as i32,
                attributes: Some(Attributes::StartTimer(StartTimerCommandAttributes {
                    timer_id: cmd.timer_id,
                    start_to_fire_timeout: Some(prost_types::Duration {
                        seconds: cmd.duration.as_secs() as i64,
                        nanos: (cmd.duration.as_nanos() % 1_000_000_000) as i32,
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            },

            BridgeCommand::CancelTimer(cmd) => ProtoCommand {
                command_type: CommandType::CancelTimer as i32,
                attributes: Some(Attributes::CancelTimer(CancelTimerCommandAttributes {
                    timer_id: cmd.timer_id,
                    ..Default::default()
                })),
                ..Default::default()
            },

            BridgeCommand::CompleteWorkflow(cmd) => ProtoCommand {
                command_type: CommandType::CompleteWorkflow as i32,
                attributes: Some(Attributes::CompleteWorkflow(
                    CompleteWorkflowCommandAttributes {
                        result: cmd.result.data,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },

            BridgeCommand::FailWorkflow(cmd) => ProtoCommand {
                command_type: CommandType::FailWorkflow as i32,
                attributes: Some(Attributes::FailWorkflow(FailWorkflowCommandAttributes {
                    failure: Some(orcher_proto::orcher::v1::Failure {
                        message: cmd.message,
                        source: "WorkflowHandler".to_string(),
                        stack_trace: String::new(),
                        cause: None,
                        failure_type: cmd.error_type,
                        details: cmd.details.map(|d| d.data).unwrap_or_default(),
                        non_retryable: false,
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            },

            BridgeCommand::CancelWorkflowExecution(cmd) => ProtoCommand {
                command_type: CommandType::CancelWorkflow as i32,
                attributes: Some(Attributes::CancelWorkflow(
                    CancelWorkflowCommandAttributes {
                        details: cmd.details.map(|d| d.data).unwrap_or_default(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },

            BridgeCommand::RestartFresh(cmd) => ProtoCommand {
                command_type: CommandType::RestartFresh as i32,
                attributes: Some(Attributes::RestartFresh(RestartFreshCommandAttributes {
                    workflow_type: cmd.workflow_type,
                    task_queue: cmd
                        .task_queue
                        .unwrap_or_else(|| default_task_queue.to_string()),
                    input: {
                        let mut all_data = Vec::new();
                        for payload in &cmd.input {
                            all_data.extend(&payload.data);
                        }
                        all_data
                    },
                    execution_timeout: cmd.timeout.map(|t| prost_types::Duration {
                        seconds: t.as_secs() as i64,
                        nanos: (t.as_nanos() % 1_000_000_000) as i32,
                    }),
                    run_timeout: None,
                    task_timeout: None,
                    retry_policy: cmd.retry_policy.as_ref().map(retry_policy),
                    annotations: Default::default(),
                    labels: Default::default(),
                    ..Default::default()
                })),
                ..Default::default()
            },

            // WaitForEvent is a client-side parking hint with no proto command.
            // The engine keeps the workflow claimed while it waits; delivering
            // the event releases the claim and the workflow is dispatched again.
            BridgeCommand::WaitForEvent(_) => continue,

            BridgeCommand::SendEvent(cmd) => {
                let payload: Vec<u8> = cmd.payload.data;
                ProtoCommand {
                    command_type: CommandType::SendEvent as i32,
                    attributes: Some(Attributes::SendEvent(SendEventCommandAttributes {
                        workflow_id: cmd.workflow_id,
                        run_id: cmd.run_id.unwrap_or_default(),
                        event_name: cmd.event_name,
                        payload,
                        headers: Default::default(),
                        ..Default::default()
                    })),
                    ..Default::default()
                }
            }

            BridgeCommand::StartChildWorkflow(cmd) => {
                let orphan_policy = match cmd.orphan_policy {
                    crate::protocol::OrphanPolicy::Terminate => ProtoOrphanPolicy::Terminate,
                    crate::protocol::OrphanPolicy::Cancel => ProtoOrphanPolicy::Cancel,
                    crate::protocol::OrphanPolicy::Abandon => ProtoOrphanPolicy::Abandon,
                };

                ProtoCommand {
                    command_type: CommandType::StartChildWorkflow as i32,
                    attributes: Some(Attributes::StartChildWorkflow(
                        StartChildWorkflowCommandAttributes {
                            workflow_id: cmd.workflow_id,
                            workflow_type: cmd.workflow_type,
                            task_queue: if cmd.task_queue.is_empty() {
                                default_task_queue.to_string()
                            } else {
                                cmd.task_queue
                            },
                            input: {
                                let mut all_data = Vec::new();
                                for payload in &cmd.input {
                                    all_data.extend(&payload.data);
                                }
                                all_data
                            },
                            execution_timeout: cmd.timeout.map(|t| prost_types::Duration {
                                seconds: t.as_secs() as i64,
                                nanos: (t.as_nanos() % 1_000_000_000) as i32,
                            }),
                            run_timeout: None,
                            task_timeout: None,
                            orphan_policy: orphan_policy as i32,
                            retry_policy: cmd.retry_policy.as_ref().map(retry_policy),
                            // A child runs in its parent worker's
                            // namespace. Left empty, the server would file
                            // the child under a namespace named "default",
                            // where no worker in this namespace could ever
                            // claim it.
                            namespace: namespace.to_string(),
                            annotations: Default::default(),
                            labels: Default::default(),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }
            }

            BridgeCommand::CancelChildWorkflow(cmd) => ProtoCommand {
                command_type: CommandType::CancelChildWorkflow as i32,
                attributes: Some(Attributes::CancelChildWorkflow(
                    CancelChildWorkflowCommandAttributes {
                        workflow_id: cmd.workflow_id,
                        run_id: cmd.run_id,
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },

            other => {
                tracing::warn!(
                    command_type = other.command_type(),
                    "Unsupported command type in conversion"
                );
                return Err(Error::internal(format!(
                    "Unsupported command type: {}",
                    other.command_type()
                )));
            }
        };

        proto_commands.push(proto_command);
    }

    Ok(proto_commands)
}

/// Convert bridge query responses to proto `QueryResult`s.
pub(crate) fn query_answers(responses: &[QueryResponse]) -> Vec<proto::QueryResult> {
    responses
        .iter()
        .map(|resp| {
            let (result_type, result) = match &resp.result {
                BridgeQueryResult::Success { output } => (
                    proto::QueryResultType::Answered as i32,
                    Some(proto::query_result::Result::Answer(output.data.clone())),
                ),
                BridgeQueryResult::Failed { message, .. } => (
                    proto::QueryResultType::Failed as i32,
                    Some(proto::query_result::Result::ErrorMessage(message.clone())),
                ),
            };

            proto::QueryResult {
                query_id: resp.query_id.clone(),
                result_type,
                result,
                ..Default::default()
            }
        })
        .collect()
}

/// Convert bridge update responses to proto `UpdateResult`s.
pub(crate) fn update_answers(responses: &[UpdateResponse]) -> Vec<proto::UpdateResult> {
    responses
        .iter()
        .map(|resp| {
            let (result_type, result) = match &resp.result {
                BridgeUpdateResult::Completed { output } => (
                    proto::UpdateResultType::Completed as i32,
                    Some(proto::update_result::Result::Answer(output.data.clone())),
                ),
                BridgeUpdateResult::Rejected { message } => (
                    proto::UpdateResultType::Rejected as i32,
                    Some(proto::update_result::Result::ErrorMessage(message.clone())),
                ),
                BridgeUpdateResult::Failed { message, .. } => (
                    proto::UpdateResultType::Failed as i32,
                    Some(proto::update_result::Result::ErrorMessage(message.clone())),
                ),
            };

            proto::UpdateResult {
                update_id: resp.update_id.clone(),
                result_type,
                result,
                ..Default::default()
            }
        })
        .collect()
}

/// What the worker sends the engine for the result of an activation of
/// `task`, as [`crate::worker::record`] records it: the completion carrying
/// the result's commands, query answers and update answers, or the report of
/// a workflow its code failed, as the workflow driver builds them. Only what
/// the activation and its result decide is set: who sends it (identity,
/// protocol version) is the worker's and is left empty.
///
/// # Errors
///
/// [`Error::DeterminismViolation`] for a result reporting that the code no
/// longer replays its history, for which the worker sends neither and hands
/// the activation back; and the errors of [`commands`].
pub(crate) fn recorded_answer(
    task: &WorkflowExecutionTask,
    result: ExecutionResult,
    namespace: &str,
) -> Result<Recorded> {
    use prost::Message;

    let workflow_id = task.execution.workflow_id.clone();
    let execution_id = task.execution.run_id.clone();
    if !result.successful {
        let (message, non_retryable) = match result.error {
            Some(error) if error.error_type == ExecutionErrorType::NonDeterminism => {
                return Err(Error::determinism_violation(error.message));
            }
            Some(error) => (error.message, !error.retryable),
            None => ("Unknown error".to_string(), false),
        };
        return Ok(Recorded::Failure(
            proto::FailWorkflowExecutionRequest {
                workflow_id,
                execution_id,
                namespace: namespace.to_string(),
                task_token: task.task_token.clone(),
                failure: Some(proto::Failure {
                    message,
                    source: "WorkflowDriver".to_string(),
                    failure_type: "WorkflowExecutionError".to_string(),
                    non_retryable,
                    ..Default::default()
                }),
                ..Default::default()
            }
            .encode_to_vec(),
        ));
    }
    Ok(Recorded::Completion(
        proto::CompleteWorkflowExecutionRequest {
            workflow_id,
            execution_id,
            namespace: namespace.to_string(),
            task_token: task.task_token.clone(),
            stream_entry_id: task.stream_entry_id.clone().unwrap_or_default(),
            query_results: query_answers(&result.query_responses),
            update_results: update_answers(&result.update_results),
            commands: commands(result.commands, &task.task_queue, namespace)?,
            ..Default::default()
        }
        .encode_to_vec(),
    ))
}

/// An activation to hand back, named by the token its poll carried.
/// A workflow retry policy as the engine reads it. A delay left unset stays
/// unset, so the engine applies its own workflow-retry default.
pub(crate) fn retry_policy(policy: &WorkflowRetryPolicy) -> proto::RetryPolicy {
    let duration = |d: Duration| crate::proto::prost_types::Duration {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    };
    proto::RetryPolicy {
        initial_interval: policy.initial_interval.map(duration),
        backoff_coefficient: policy.backoff_coefficient,
        maximum_interval: policy.max_interval.map(duration),
        maximum_attempts: i32::try_from(policy.max_attempts).unwrap_or(i32::MAX),
        non_retryable_error_types: policy.non_retryable_errors.clone(),
        ..Default::default()
    }
}

/// A step-id scheme as the engine and the kernel number it.
impl From<StepIdScheme> for i32 {
    fn from(scheme: StepIdScheme) -> Self {
        (match scheme {
            StepIdScheme::Kernel => proto::StepIdScheme::Kernel,
            StepIdScheme::RustV0 => proto::StepIdScheme::RustV0,
            StepIdScheme::PythonV0 => proto::StepIdScheme::PythonV0,
            StepIdScheme::TypescriptV0 => proto::StepIdScheme::TypescriptV0,
        }) as i32
    }
}

/// The scheme the engine numbers `number`; `None` for none bound. A number no
/// scheme has cannot have been bound: the engine binds only schemes the worker
/// reported.
pub(crate) fn step_id_scheme(number: i32) -> Option<StepIdScheme> {
    match proto::StepIdScheme::try_from(number).ok()? {
        proto::StepIdScheme::Unspecified => None,
        proto::StepIdScheme::Kernel => Some(StepIdScheme::Kernel),
        proto::StepIdScheme::RustV0 => Some(StepIdScheme::RustV0),
        proto::StepIdScheme::PythonV0 => Some(StepIdScheme::PythonV0),
        proto::StepIdScheme::TypescriptV0 => Some(StepIdScheme::TypescriptV0),
    }
}

/// The work an activation the engine sent asks for. This is what a poll
/// hands the driver, and what a recorded activation (see
/// [`crate::worker::record`]) is replayed as.
impl From<proto::PollWorkflowExecutionResponse> for WorkflowExecutionTask {
    fn from(poll_response: proto::PollWorkflowExecutionResponse) -> Self {
        // The poll response has no task token field; the engine returns the
        // activation token in `stream_entry_id` instead. It is carried as the
        // task token too, because a failure report has nowhere else to put
        // it and language SDKs hand the task token back with every result.
        // The token must name the activation, not only the workflow, so the
        // engine can tell a repeat of a completion from a new one. An engine
        // that returns no token gets an empty one back, which it ignores.
        let task_token = poll_response.stream_entry_id.clone().into_bytes();

        WorkflowExecutionTask {
            execution: WorkflowExecution::new(
                poll_response.workflow_id,
                poll_response.execution_id,
            ),
            workflow_type: poll_response.workflow_type,
            task_queue: poll_response.task_queue,
            input: serde_json::from_slice(&poll_response.input)
                .unwrap_or(serde_json::Value::Object(serde_json::Map::new())),
            journal: poll_response.journal,
            started_event_id: poll_response.started_event_id,
            previous_started_event_id: poll_response.previous_started_event_id,
            attempt: poll_response.attempt,
            task_token,
            // Echoed on completion, where engines read the activation token.
            stream_entry_id: if poll_response.stream_entry_id.is_empty() {
                None
            } else {
                Some(poll_response.stream_entry_id)
            },
            // Pending queries and updates from the server.
            queries: poll_response.queries,
            updates: poll_response.updates,
            step_ids: step_id_scheme(poll_response.step_ids),
            history_lineage: poll_response.history_lineage,
        }
    }
}

/// The task a poll handed out.
impl From<proto::PollTaskExecutionResponse> for TaskExecutionTask {
    fn from(poll_response: proto::PollTaskExecutionResponse) -> Self {
        TaskExecutionTask {
            execution: WorkflowExecution::new(
                poll_response.workflow_id,
                poll_response.execution_id,
            ),
            task_id: poll_response.task_id,
            task_type: poll_response.task_type,
            task_queue: poll_response.task_queue,
            input: poll_response.input,
            task_token: poll_response.task_token,
            attempt: poll_response.attempt,
            start_to_close_timeout: poll_response
                .start_to_close_timeout
                .map(|d| Duration::new(d.seconds as u64, d.nanos as u32)),
            heartbeat_timeout: poll_response
                .heartbeat_timeout
                .map(|d| Duration::new(d.seconds as u64, d.nanos as u32)),
        }
    }
}
