//! The client protocol's messages (`orcher.client.v1`) and the crate's own
//! client types, each way.

use std::time::{Duration, SystemTime};

use orcher_proto::client::v1 as proto;
use orcher_proto::client::v1::client_answer::Answer;
use orcher_proto::prost::Message;
use orcher_proto::prost_types;

use crate::client::StartWorkflowOpts;
use crate::error::{Error, Result};
use crate::error_code::ErrorCode;
use crate::types::{
    ListWorkflowsOptions, ListWorkflowsSortOrder, Namespace, SearchWorkflowsOptions,
    WorkflowExecutionDescription, WorkflowExecutionInfo, WorkflowIdReusePolicy, WorkflowListPage,
    WorkflowRetryPolicy, WorkflowStatus,
};

/// An encoded `ClientCall`.
pub(crate) fn decode_call(bytes: &[u8]) -> Result<proto::ClientCall> {
    proto::ClientCall::decode(bytes)
        .map_err(|e| Error::Protocol(format!("not an encoded ClientCall: {e}")))
}

/// How a call ended, as an encoded `ClientAnswer`.
pub(crate) fn encode_answer(answer: Result<Answer>) -> Vec<u8> {
    let answer = answer.unwrap_or_else(|error| Answer::Error(client_error(&error)));
    proto::ClientAnswer {
        answer: Some(answer),
        ..Default::default()
    }
    .encode_to_vec()
}

/// Why a call failed, with one code for every language.
fn client_error(error: &Error) -> proto::ClientError {
    // A call that could not be decoded, or that asked for nothing, is the
    // caller's mistake.
    let code = match error {
        Error::Protocol(_) | Error::InvalidPayload { .. } => ErrorCode::InvalidArgument,
        _ => error.result_code(),
    };
    let run_id = match error {
        Error::WorkflowAlreadyExists {
            run_id: Some(run_id),
            ..
        } => run_id.clone(),
        _ => String::new(),
    };
    proto::ClientError {
        code: error_code(code) as i32,
        message: error.to_string(),
        retryable: error.is_retryable(),
        run_id,
        status: error
            .workflow_status()
            .map_or(proto::WorkflowStatus::Unspecified, status) as i32,
        ..Default::default()
    }
}

fn error_code(code: ErrorCode) -> proto::ErrorCode {
    use proto::ErrorCode as P;
    match code {
        ErrorCode::WorkflowNotFound => P::WorkflowNotFound,
        ErrorCode::WorkflowAlreadyExists => P::WorkflowAlreadyExists,
        ErrorCode::WorkflowExecutionFailed => P::WorkflowExecutionFailed,
        ErrorCode::WorkflowCanceled => P::WorkflowCanceled,
        ErrorCode::WorkflowTerminated => P::WorkflowTerminated,
        ErrorCode::WorkflowTimeout => P::WorkflowTimeout,
        ErrorCode::Timeout => P::Timeout,
        ErrorCode::ConnectionFailed => P::ConnectionFailed,
        ErrorCode::Unauthenticated => P::Unauthenticated,
        ErrorCode::PermissionDenied => P::PermissionDenied,
        ErrorCode::InvalidArgument => P::InvalidArgument,
        ErrorCode::NotFound => P::NotFound,
        ErrorCode::AlreadyExists => P::AlreadyExists,
        ErrorCode::Unavailable => P::Unavailable,
        ErrorCode::ResourceExhausted => P::ResourceExhausted,
        ErrorCode::FailedPrecondition => P::FailedPrecondition,
        ErrorCode::Aborted => P::Aborted,
        ErrorCode::OutOfRange => P::OutOfRange,
        ErrorCode::Unimplemented => P::Unimplemented,
        ErrorCode::DataLoss => P::DataLoss,
        ErrorCode::Internal => P::Internal,
        _ => P::Unknown,
    }
}

/// A protocol duration; a negative one is none.
pub(crate) fn duration(d: prost_types::Duration) -> Duration {
    Duration::try_from(d).unwrap_or_default()
}

fn seconds(seconds: i64) -> prost_types::Duration {
    prost_types::Duration { seconds, nanos: 0 }
}

fn timestamp(t: SystemTime) -> prost_types::Timestamp {
    prost_types::Timestamp::from(t)
}

/// The workflow a call names: its id, and its run when one is named.
pub(crate) fn workflow(workflow: Option<proto::WorkflowRef>) -> Result<(String, Option<String>)> {
    let workflow = workflow
        .filter(|w| !w.workflow_id.is_empty())
        .ok_or_else(|| Error::invalid_payload("the call names no workflow"))?;
    let run_id = (!workflow.run_id.is_empty()).then_some(workflow.run_id);
    Ok((workflow.workflow_id, run_id))
}

pub(crate) fn workflow_ref(workflow_id: &str, run_id: &str) -> proto::WorkflowRef {
    proto::WorkflowRef {
        workflow_id: workflow_id.to_string(),
        run_id: run_id.to_string(),
        ..Default::default()
    }
}

pub(crate) fn status(status: WorkflowStatus) -> proto::WorkflowStatus {
    use proto::WorkflowStatus as P;
    match status {
        WorkflowStatus::Running => P::Running,
        WorkflowStatus::Completed => P::Completed,
        WorkflowStatus::Failed => P::Failed,
        WorkflowStatus::Cancelled => P::Cancelled,
        WorkflowStatus::Terminated => P::Terminated,
        WorkflowStatus::TimedOut => P::TimedOut,
        WorkflowStatus::RestartedFresh => P::RestartedFresh,
    }
}

fn status_from(status: i32) -> Option<WorkflowStatus> {
    use proto::WorkflowStatus as P;
    match P::try_from(status).ok()? {
        P::Running => Some(WorkflowStatus::Running),
        P::Completed => Some(WorkflowStatus::Completed),
        P::Failed => Some(WorkflowStatus::Failed),
        P::Cancelled => Some(WorkflowStatus::Cancelled),
        P::Terminated => Some(WorkflowStatus::Terminated),
        P::TimedOut => Some(WorkflowStatus::TimedOut),
        P::RestartedFresh => Some(WorkflowStatus::RestartedFresh),
        P::Unspecified => None,
    }
}

/// The options a start asks for.
pub(crate) fn start_opts(start: proto::StartWorkflow) -> Result<StartWorkflowOpts> {
    if start.workflow_type.is_empty() {
        return Err(Error::invalid_payload("a start names no workflow type"));
    }
    let mut opts = StartWorkflowOpts::new(
        start.workflow_id,
        start.workflow_type,
        start.task_queue,
        start.input,
    );
    opts.execution_timeout = start.execution_timeout.map(duration);
    opts.run_timeout = start.run_timeout.map(duration);
    opts.task_timeout = start.task_timeout.map(duration);
    opts.cron_schedule = (!start.cron_schedule.is_empty()).then_some(start.cron_schedule);
    opts.retry_policy = start.retry_policy.map(|policy| WorkflowRetryPolicy {
        max_attempts: policy.max_attempts,
        initial_interval: policy.initial_interval.map(duration),
        max_interval: policy.max_interval.map(duration),
        backoff_coefficient: policy.backoff_coefficient,
        non_retryable_errors: policy.non_retryable_errors,
    });
    opts.id_reuse_policy = {
        use proto::IdReusePolicy as P;
        match P::try_from(start.id_reuse_policy) {
            Ok(P::AllowDuplicate) => Some(WorkflowIdReusePolicy::AllowDuplicate),
            Ok(P::AllowDuplicateFailedOnly) => {
                Some(WorkflowIdReusePolicy::AllowDuplicateFailedOnly)
            }
            Ok(P::RejectDuplicate) => Some(WorkflowIdReusePolicy::RejectDuplicate),
            Ok(P::TerminateIfRunning) => Some(WorkflowIdReusePolicy::TerminateIfRunning),
            Ok(P::Unspecified) => None,
            Err(_) => {
                return Err(Error::invalid_payload(format!(
                    "unknown id reuse policy {}",
                    start.id_reuse_policy
                )))
            }
        }
    };
    Ok(opts)
}

pub(crate) fn list_options(list: proto::ListWorkflows) -> ListWorkflowsOptions {
    use proto::ListOrder as P;
    ListWorkflowsOptions {
        page_size: list.page_size,
        next_page_token: list.page_token,
        workflow_type: (!list.workflow_type.is_empty()).then_some(list.workflow_type),
        task_queue: (!list.task_queue.is_empty()).then_some(list.task_queue),
        status_filter: list.statuses.into_iter().filter_map(status_from).collect(),
        sort_order: match P::try_from(list.order) {
            Ok(P::StartTimeAsc) => Some(ListWorkflowsSortOrder::StartTimeAsc),
            Ok(P::StartTimeDesc) => Some(ListWorkflowsSortOrder::StartTimeDesc),
            Ok(P::CloseTimeAsc) => Some(ListWorkflowsSortOrder::CloseTimeAsc),
            Ok(P::CloseTimeDesc) => Some(ListWorkflowsSortOrder::CloseTimeDesc),
            _ => None,
        },
    }
}

pub(crate) fn search_options(search: proto::SearchWorkflows) -> SearchWorkflowsOptions {
    SearchWorkflowsOptions {
        page_size: search.page_size,
        next_page_token: search.page_token,
    }
}

fn summary(info: WorkflowExecutionInfo) -> proto::WorkflowSummary {
    proto::WorkflowSummary {
        workflow: Some(workflow_ref(&info.workflow_id, &info.execution_id)),
        workflow_type: info.workflow_type,
        task_queue: info.task_queue,
        namespace: info.namespace,
        status: status(info.status) as i32,
        start_time: info.start_time.map(timestamp),
        close_time: info.close_time.map(timestamp),
        execution_duration: info.execution_duration_seconds.map(seconds),
        journal_length: info.journal_length,
        parent: info.parent_execution.map(|parent| proto::ParentRef {
            namespace: parent.namespace,
            workflow: Some(workflow_ref(&parent.workflow_id, &parent.execution_id)),
            ..Default::default()
        }),
        labels: info.labels,
        annotations: info.annotations,
        tags: info.tags,
        started_by: info.started_by,
        attempt: info.attempt,
        cron_schedule: info.cron_schedule,
        state_transition_count: info.state_transition_count,
        ..Default::default()
    }
}

pub(crate) fn page(page: WorkflowListPage) -> proto::WorkflowPage {
    proto::WorkflowPage {
        workflows: page.executions.into_iter().map(summary).collect(),
        next_page_token: page.next_page_token,
        ..Default::default()
    }
}

pub(crate) fn description(d: WorkflowExecutionDescription) -> proto::WorkflowDescription {
    proto::WorkflowDescription {
        summary: Some(proto::WorkflowSummary {
            workflow: Some(workflow_ref(&d.execution.workflow_id, &d.execution.run_id)),
            workflow_type: d.workflow_type,
            task_queue: d.task_queue,
            namespace: String::new(),
            status: status(d.status) as i32,
            start_time: d.start_time.map(timestamp),
            close_time: d.close_time.map(timestamp),
            execution_duration: None,
            journal_length: d.journal_length,
            parent: d.parent_execution.map(|parent| proto::ParentRef {
                namespace: parent.namespace,
                workflow: Some(workflow_ref(&parent.workflow_id, &parent.execution_id)),
                ..Default::default()
            }),
            labels: d.labels,
            annotations: d.annotations,
            tags: d.tags,
            started_by: d.started_by,
            attempt: d.attempt,
            cron_schedule: d.cron_schedule,
            state_transition_count: d.state_transition_count,
            ..Default::default()
        }),
        pending_tasks: d.pending_tasks,
        pending_timers: d.pending_timers,
        pending_events: d.pending_events,
        config: d.execution_config.map(|c| proto::WorkflowConfig {
            task_queue: c.task_queue,
            execution_timeout: c.execution_timeout_seconds.map(seconds),
            run_timeout: c.run_timeout_seconds.map(seconds),
            task_timeout: c.default_task_timeout_seconds.map(seconds),
            retry_policy: c.retry_policy.map(|p| proto::RetryPolicy {
                max_attempts: u32::try_from(p.max_attempts).unwrap_or(0),
                initial_interval: Some(seconds(p.initial_interval_seconds)),
                max_interval: Some(seconds(p.max_interval_seconds)),
                backoff_coefficient: p.backoff_coefficient,
                non_retryable_errors: p.non_retryable_error_types,
                ..Default::default()
            }),
            cron_schedule: c.cron_schedule,
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub(crate) fn namespace(n: Namespace) -> proto::Namespace {
    proto::Namespace {
        name: n.name,
        status: n.status,
        description: n.description,
        owner_email: n.owner_email,
        retention_period_days: n.retention_period_days,
        data: n.data,
        created_at: n.created_at.map(timestamp),
        updated_at: n.updated_at.map(timestamp),
        ..Default::default()
    }
}
