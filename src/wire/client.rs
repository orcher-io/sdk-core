//! Conversions between the engine's client messages (`orcher.v1`) and the
//! shared types in [`crate::types`].

use std::time::SystemTime;

use crate::error::{Error, Result};
use crate::proto::orcher::v1 as proto;
use crate::types::*;

/// A status as the engine numbers it, read as the crate's own. A number the
/// crate does not know reads as the non-terminal `Running`.
pub(crate) fn status(value: i32) -> WorkflowStatus {
    use proto::WorkflowStatus as ProtoStatus;
    match value {
        x if x == ProtoStatus::Running as i32 => WorkflowStatus::Running,
        x if x == ProtoStatus::Completed as i32 => WorkflowStatus::Completed,
        x if x == ProtoStatus::Failed as i32 => WorkflowStatus::Failed,
        x if x == ProtoStatus::Canceled as i32 => WorkflowStatus::Cancelled,
        x if x == ProtoStatus::Terminated as i32 => WorkflowStatus::Terminated,
        x if x == ProtoStatus::TimedOut as i32 => WorkflowStatus::TimedOut,
        x if x == ProtoStatus::RestartedFresh as i32 => WorkflowStatus::RestartedFresh,
        _ => WorkflowStatus::Running,
    }
}

/// A status as the engine numbers it.
pub(crate) fn status_number(status: WorkflowStatus) -> i32 {
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

/// The retry policy a described workflow runs under.
fn described_retry_policy(proto: proto::RetryPolicy) -> RetryPolicy {
    RetryPolicy {
        initial_interval_seconds: proto.initial_interval.map_or(1, |d| d.seconds),
        backoff_coefficient: proto.backoff_coefficient,
        max_interval_seconds: proto.maximum_interval.map_or(60, |d| d.seconds),
        max_attempts: proto.maximum_attempts,
        non_retryable_error_types: proto.non_retryable_error_types,
    }
}

/// An id reuse policy as the engine numbers it.
pub(crate) fn id_reuse_policy(policy: WorkflowIdReusePolicy) -> i32 {
    use proto::WorkflowIdReusePolicy as Proto;
    match policy {
        WorkflowIdReusePolicy::AllowDuplicate => Proto::AllowDuplicate as i32,
        WorkflowIdReusePolicy::AllowDuplicateFailedOnly => Proto::AllowDuplicateFailedOnly as i32,
        WorkflowIdReusePolicy::RejectDuplicate => Proto::RejectDuplicate as i32,
        WorkflowIdReusePolicy::TerminateIfRunning => Proto::TerminateIfRunning as i32,
    }
}

/// A namespace as the engine reports it.
pub(crate) fn namespace(info: proto::NamespaceInfo) -> Namespace {
    Namespace {
        name: info.name,
        status: info.status,
        description: info.description,
        owner_email: info.owner_email,
        retention_period_days: info.retention_period_days,
        data: info.data,
        created_at: info.created_at.map(time),
        updated_at: info.updated_at.map(time),
    }
}

fn time(t: prost_types::Timestamp) -> SystemTime {
    SystemTime::UNIX_EPOCH + std::time::Duration::new(t.seconds as u64, t.nanos as u32)
}

fn duration(d: std::time::Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

/// The request that starts the workflow `opts` describes, in `namespace`.
pub(crate) fn start_request(
    opts: crate::client::StartWorkflowOpts,
    namespace: &str,
) -> proto::StartWorkflowRequest {
    proto::StartWorkflowRequest {
        workflow_id: opts.workflow_id,
        workflow_type: opts.workflow_type,
        task_queue: opts.task_queue,
        namespace: namespace.to_string(),
        input: opts.input_bytes,
        execution_timeout: opts.execution_timeout.map(duration),
        run_timeout: opts.run_timeout.map(duration),
        task_timeout: opts.task_timeout.map(duration),
        retry_policy: opts
            .retry_policy
            .as_ref()
            .map(crate::wire::engine::retry_policy),
        cron_schedule: opts.cron_schedule.unwrap_or_default(),
        request_id: uuid::Uuid::new_v4().to_string(),
        workflow_id_reuse_policy: opts.id_reuse_policy.map_or(0, id_reuse_policy),
        ..Default::default()
    }
}

impl WorkflowExecutionInfo {
    /// Converts from the proto `WorkflowExecutionInfo`.
    pub(crate) fn from_proto(info: proto::WorkflowExecutionInfo) -> Self {
        let start_time = info.start_time.map(time);
        let close_time = info.close_time.map(time);
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
            status: status(info.status),
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

/// A sort order as the engine numbers it.
pub(crate) fn sort_order(order: ListWorkflowsSortOrder) -> i32 {
    use proto::ListWorkflowsSortOrder as ProtoOrder;
    match order {
        ListWorkflowsSortOrder::StartTimeAsc => ProtoOrder::StartTimeAsc as i32,
        ListWorkflowsSortOrder::StartTimeDesc => ProtoOrder::StartTimeDesc as i32,
        ListWorkflowsSortOrder::CloseTimeAsc => ProtoOrder::CloseTimeAsc as i32,
        ListWorkflowsSortOrder::CloseTimeDesc => ProtoOrder::CloseTimeDesc as i32,
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

        let status = status(info.status);

        let start_time = info.start_time.map(time);
        let close_time = info.close_time.map(time);

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
            retry_policy: c.retry_policy.map(described_retry_policy),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::StartWorkflowOpts;
    use std::time::Duration;

    /// Every option a start is given reaches the request, the retry policy's
    /// delays to the nanosecond and an unset delay left to the engine.
    #[test]
    fn a_start_carries_every_option() {
        let opts = StartWorkflowOpts::new("order-7", "Checkout", "orders", b"{}".to_vec())
            .with_cron_schedule("0 * * * * *")
            .with_execution_timeout(Duration::from_millis(90_500))
            .with_run_timeout(Duration::from_secs(60))
            .with_task_timeout(Duration::from_secs(10))
            .with_id_reuse_policy(WorkflowIdReusePolicy::RejectDuplicate)
            .with_retry_policy(WorkflowRetryPolicy {
                max_attempts: 4,
                initial_interval: Some(Duration::from_millis(250)),
                max_interval: None,
                backoff_coefficient: 1.5,
                non_retryable_errors: vec!["Declined".into()],
            });
        let request = start_request(opts, "payments");

        assert_eq!(
            (
                request.workflow_id.as_str(),
                request.workflow_type.as_str(),
                request.task_queue.as_str(),
                request.namespace.as_str(),
            ),
            ("order-7", "Checkout", "orders", "payments")
        );
        assert_eq!(request.input, b"{}");
        assert_eq!(request.cron_schedule, "0 * * * * *");
        let timeout = request.execution_timeout.unwrap();
        assert_eq!((timeout.seconds, timeout.nanos), (90, 500_000_000));
        assert_eq!(request.run_timeout.unwrap().seconds, 60);
        assert_eq!(request.task_timeout.unwrap().seconds, 10);
        assert_eq!(
            request.workflow_id_reuse_policy,
            proto::WorkflowIdReusePolicy::RejectDuplicate as i32
        );
        let retry = request.retry_policy.unwrap();
        assert_eq!(retry.maximum_attempts, 4);
        assert_eq!(retry.initial_interval.unwrap().nanos, 250_000_000);
        assert_eq!(retry.maximum_interval, None);
        assert_eq!(retry.backoff_coefficient, 1.5);
        assert_eq!(retry.non_retryable_error_types, ["Declined"]);
        assert!(!request.request_id.is_empty());
    }

    /// A start with no options leaves every one to the engine.
    #[test]
    fn a_plain_start_leaves_the_options_to_the_engine() {
        let request = start_request(
            StartWorkflowOpts::new("", "Checkout", "orders", Vec::new()),
            "default",
        );
        assert_eq!(request.retry_policy, None);
        assert_eq!(request.workflow_id_reuse_policy, 0);
        assert_eq!(request.execution_timeout, None);
        assert_eq!(request.cron_schedule, "");
    }

    /// Each reuse policy is the engine's policy of the same name.
    #[test]
    fn every_reuse_policy_is_the_engines_of_the_same_name() {
        use proto::WorkflowIdReusePolicy as Proto;
        for (policy, expected) in [
            (WorkflowIdReusePolicy::AllowDuplicate, Proto::AllowDuplicate),
            (
                WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
                Proto::AllowDuplicateFailedOnly,
            ),
            (
                WorkflowIdReusePolicy::RejectDuplicate,
                Proto::RejectDuplicate,
            ),
            (
                WorkflowIdReusePolicy::TerminateIfRunning,
                Proto::TerminateIfRunning,
            ),
        ] {
            assert_eq!(id_reuse_policy(policy), expected as i32, "{policy:?}");
        }
    }

    #[test]
    fn a_namespace_is_read_whole() {
        let read = namespace(proto::NamespaceInfo {
            name: "payments".into(),
            status: "active".into(),
            description: "money".into(),
            owner_email: "owner@example.com".into(),
            retention_period_days: 30,
            data: [("tier".to_string(), "gold".to_string())].into(),
            created_at: Some(prost_types::Timestamp {
                seconds: 10,
                nanos: 5,
            }),
            updated_at: None,
            ..Default::default()
        });
        assert_eq!(read.name, "payments");
        assert_eq!(read.status, "active");
        assert_eq!(read.description, "money");
        assert_eq!(read.owner_email, "owner@example.com");
        assert_eq!(read.retention_period_days, 30);
        assert_eq!(read.data["tier"], "gold");
        assert_eq!(
            read.created_at,
            Some(SystemTime::UNIX_EPOCH + Duration::new(10, 5))
        );
        assert_eq!(read.updated_at, None);
    }

    /// Every status keeps its number both ways.
    #[test]
    fn every_status_keeps_its_number() {
        for status_ in [
            WorkflowStatus::Running,
            WorkflowStatus::Completed,
            WorkflowStatus::Failed,
            WorkflowStatus::Cancelled,
            WorkflowStatus::Terminated,
            WorkflowStatus::TimedOut,
            WorkflowStatus::RestartedFresh,
        ] {
            assert_eq!(status(status_number(status_)), status_);
        }
    }

    /// A described retry policy reaches the language bindings in the same
    /// camelCase as the description around it.
    #[test]
    fn a_described_retry_policy_is_written_in_camel_case() {
        let json = serde_json::to_value(described_retry_policy(proto::RetryPolicy {
            maximum_attempts: 3,
            ..Default::default()
        }))
        .unwrap();
        assert_eq!(json["maxAttempts"], 3);
        assert_eq!(json["initialIntervalSeconds"], 1);
        assert!(json.get("max_attempts").is_none(), "{json}");
    }
}
