//! Conversions between the engine's client messages (`orcher.v1`) and the
//! shared types in [`crate::types`].

use std::time::SystemTime;

use crate::error::{Error, Result};
use crate::proto::orcher::v1 as proto;
use crate::types::*;

impl From<proto::WorkflowExecution> for WorkflowExecution {
    fn from(proto: proto::WorkflowExecution) -> Self {
        Self {
            workflow_id: proto.workflow_id,
            run_id: proto.execution_id,
        }
    }
}

impl From<WorkflowExecution> for proto::WorkflowExecution {
    fn from(exec: WorkflowExecution) -> Self {
        Self {
            workflow_id: exec.workflow_id,
            execution_id: exec.run_id,
            ..Default::default()
        }
    }
}

impl From<i32> for WorkflowStatus {
    fn from(value: i32) -> Self {
        use proto::WorkflowStatus as ProtoStatus;
        match value {
            x if x == ProtoStatus::Running as i32 => Self::Running,
            x if x == ProtoStatus::Completed as i32 => Self::Completed,
            x if x == ProtoStatus::Failed as i32 => Self::Failed,
            x if x == ProtoStatus::Canceled as i32 => Self::Cancelled,
            x if x == ProtoStatus::Terminated as i32 => Self::Terminated,
            x if x == ProtoStatus::TimedOut as i32 => Self::TimedOut,
            x if x == ProtoStatus::RestartedFresh as i32 => Self::RestartedFresh,
            _ => Self::Running, // Unknown values map to the non-terminal Running
        }
    }
}

impl From<WorkflowStatus> for i32 {
    fn from(status: WorkflowStatus) -> Self {
        use proto::WorkflowStatus as ProtoStatus;
        match status {
            WorkflowStatus::Running => ProtoStatus::Running as i32,
            WorkflowStatus::Completed => ProtoStatus::Completed as i32,
            WorkflowStatus::Failed => ProtoStatus::Failed as i32,
            WorkflowStatus::Cancelled => ProtoStatus::Canceled as i32,
            WorkflowStatus::Terminated => ProtoStatus::Terminated as i32,
            WorkflowStatus::TimedOut => ProtoStatus::TimedOut as i32,
            WorkflowStatus::RestartedFresh => ProtoStatus::RestartedFresh as i32,
        }
    }
}

impl From<proto::RetryPolicy> for RetryPolicy {
    fn from(proto: proto::RetryPolicy) -> Self {
        Self {
            initial_interval_seconds: proto.initial_interval.map_or(1, |d| d.seconds),
            backoff_coefficient: proto.backoff_coefficient,
            max_interval_seconds: proto.maximum_interval.map_or(60, |d| d.seconds),
            max_attempts: proto.maximum_attempts,
            non_retryable_error_types: proto.non_retryable_error_types,
        }
    }
}

impl From<RetryPolicy> for proto::RetryPolicy {
    fn from(policy: RetryPolicy) -> Self {
        Self {
            initial_interval: Some(prost_types::Duration {
                seconds: policy.initial_interval_seconds,
                nanos: 0,
            }),
            backoff_coefficient: policy.backoff_coefficient,
            maximum_interval: Some(prost_types::Duration {
                seconds: policy.max_interval_seconds,
                nanos: 0,
            }),
            maximum_attempts: policy.max_attempts,
            non_retryable_error_types: policy.non_retryable_error_types,
            ..Default::default()
        }
    }
}

impl From<proto::Failure> for Failure {
    fn from(proto: proto::Failure) -> Self {
        Self {
            message: proto.message,
            source: proto.cause.map(|c| Box::new(Self::from(*c))),
            stack_trace: proto.stack_trace,
            error_type: proto.failure_type,
        }
    }
}

impl From<Failure> for proto::Failure {
    fn from(failure: Failure) -> Self {
        Self {
            message: failure.message,
            source: String::new(),
            stack_trace: failure.stack_trace,
            cause: failure.source.map(|s| Box::new(proto::Failure::from(*s))),
            failure_type: failure.error_type,
            details: vec![],
            non_retryable: false,
            ..Default::default()
        }
    }
}

impl From<proto::Header> for Header {
    fn from(proto: proto::Header) -> Self {
        Self {
            fields: proto.fields,
        }
    }
}

impl From<Header> for proto::Header {
    fn from(header: Header) -> Self {
        Self {
            fields: header.fields,
            ..Default::default()
        }
    }
}

impl WorkflowExecutionInfo {
    /// Converts from the proto `WorkflowExecutionInfo`.
    pub(crate) fn from_proto(info: proto::WorkflowExecutionInfo) -> Self {
        let start_time = info.start_time.map(|t| {
            SystemTime::UNIX_EPOCH + std::time::Duration::new(t.seconds as u64, t.nanos as u32)
        });
        let close_time = info.close_time.map(|t| {
            SystemTime::UNIX_EPOCH + std::time::Duration::new(t.seconds as u64, t.nanos as u32)
        });
        let execution_duration_seconds = info.execution_time.map(|d| d.seconds);
        let parent_execution = info.parent_execution.map(|p| ParentExecutionInfo {
            namespace: p.namespace,
            workflow_id: p.workflow_id,
            execution_id: p.execution_id,
        });

        Self {
            workflow_id: info.workflow_id,
            execution_id: info.execution_id,
            workflow_type: info.workflow_type,
            task_queue: info.task_queue,
            namespace: info.namespace,
            status: WorkflowStatus::from(info.status),
            start_time,
            close_time,
            execution_duration_seconds,
            journal_length: info.journal_length,
            parent_execution,
            labels: info.labels,
            annotations: info.annotations,
            tags: info.tags,
            started_by: info.started_by,
            attempt: info.attempt,
            cron_schedule: info.cron_schedule,
            state_transition_count: info.state_transition_count,
        }
    }
}

impl WorkflowListPage {
    /// Converts from the proto `ListWorkflowsResponse`.
    pub(crate) fn from_list_response(resp: proto::ListWorkflowsResponse) -> Self {
        Self {
            executions: resp
                .executions
                .into_iter()
                .map(WorkflowExecutionInfo::from_proto)
                .collect(),
            next_page_token: resp.next_page_token,
        }
    }

    /// Converts from the proto `SearchWorkflowsResponse`.
    pub(crate) fn from_search_response(resp: proto::SearchWorkflowsResponse) -> Self {
        Self {
            executions: resp
                .executions
                .into_iter()
                .map(WorkflowExecutionInfo::from_proto)
                .collect(),
            next_page_token: resp.next_page_token,
        }
    }
}

impl From<ListWorkflowsSortOrder> for i32 {
    fn from(order: ListWorkflowsSortOrder) -> Self {
        use proto::ListWorkflowsSortOrder as ProtoOrder;
        match order {
            ListWorkflowsSortOrder::StartTimeAsc => ProtoOrder::StartTimeAsc as i32,
            ListWorkflowsSortOrder::StartTimeDesc => ProtoOrder::StartTimeDesc as i32,
            ListWorkflowsSortOrder::CloseTimeAsc => ProtoOrder::CloseTimeAsc as i32,
            ListWorkflowsSortOrder::CloseTimeDesc => ProtoOrder::CloseTimeDesc as i32,
        }
    }
}

impl WorkflowExecutionDescription {
    /// Converts from the proto `DescribeWorkflowExecutionResponse`.
    ///
    /// Fails if the response has no `execution_info`.
    pub(crate) fn from_proto(proto: proto::DescribeWorkflowExecutionResponse) -> Result<Self> {
        let info = proto
            .execution_info
            .ok_or_else(|| Error::internal("DescribeWorkflowExecution: missing execution_info"))?;

        let status = WorkflowStatus::from(info.status);

        let start_time = info.start_time.map(|t| {
            SystemTime::UNIX_EPOCH + std::time::Duration::new(t.seconds as u64, t.nanos as u32)
        });
        let close_time = info.close_time.map(|t| {
            SystemTime::UNIX_EPOCH + std::time::Duration::new(t.seconds as u64, t.nanos as u32)
        });

        let parent_execution = info.parent_execution.map(|p| ParentExecutionInfo {
            namespace: p.namespace,
            workflow_id: p.workflow_id,
            execution_id: p.execution_id,
        });

        let execution_config = proto.execution_config.map(|c| WorkflowExecutionConfig {
            task_queue: c.task_queue,
            execution_timeout_seconds: c.execution_timeout.map(|d| d.seconds),
            run_timeout_seconds: c.run_timeout.map(|d| d.seconds),
            default_task_timeout_seconds: c.default_task_timeout.map(|d| d.seconds),
            retry_policy: c.retry_policy.map(RetryPolicy::from),
            cron_schedule: c.cron_schedule,
        });

        Ok(Self {
            execution: WorkflowExecution::new(info.workflow_id, info.execution_id),
            workflow_type: info.workflow_type,
            task_queue: info.task_queue,
            status,
            start_time,
            close_time,
            journal_length: info.journal_length,
            attempt: info.attempt,
            labels: info.labels,
            annotations: info.annotations,
            tags: info.tags,
            started_by: info.started_by,
            cron_schedule: info.cron_schedule,
            state_transition_count: info.state_transition_count,
            parent_execution,
            pending_tasks: proto.pending_tasks,
            pending_timers: proto.pending_timers,
            pending_events: proto.pending_events,
            execution_config,
        })
    }
}
