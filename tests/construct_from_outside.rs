//! Checks that every `#[non_exhaustive]` type a language SDK or user builds
//! can still be built from outside this crate.
//!
//! The attribute only restricts code in other crates, so only an integration
//! test can catch a type that has become impossible to construct.

use std::time::Duration;

use orcher_sdk_core::client::workflow::StartWorkflowOpts;
use orcher_sdk_core::poller::TlsConfig;
use orcher_sdk_core::types::RetryPolicy;
use orcher_sdk_core::{
    ActorDriverConfig, ActorPollerConfig, Error, ListWorkflowsOptions, ListWorkflowsSortOrder,
    PollerConfig, ReplayConfig, SearchWorkflowsOptions, TaskDriverConfig, WorkerRegistrationConfig,
    WorkflowDriverConfig, WorkflowStatus,
};

#[test]
fn options_build_with_their_methods() {
    let opts = StartWorkflowOpts::new("wf-1", "Order", "orders", b"{}".to_vec())
        .with_cron_schedule("* * * * *")
        .with_execution_timeout(Duration::from_secs(60));
    assert_eq!(opts.workflow_id, "wf-1");
    assert_eq!(opts.execution_timeout, Some(Duration::from_secs(60)));

    let list = ListWorkflowsOptions::default()
        .with_page_size(10)
        .with_task_queue("orders")
        .with_status_filter([WorkflowStatus::Running])
        .with_sort_order(ListWorkflowsSortOrder::StartTimeDesc);
    assert_eq!(list.page_size, 10);
    assert_eq!(list.status_filter, vec![WorkflowStatus::Running]);

    let search = SearchWorkflowsOptions::default().with_next_page_token(vec![1, 2]);
    assert_eq!(search.next_page_token, vec![1, 2]);

    let policy = RetryPolicy::default()
        .with_max_attempts(5)
        .with_non_retryable_error_types(["Invalid"]);
    assert_eq!(policy.max_attempts, 5);
    assert_eq!(
        policy.non_retryable_error_types,
        vec!["Invalid".to_string()]
    );

    let tls = TlsConfig::new()
        .with_ca_cert(b"ca".to_vec())
        .with_client_identity(b"cert".to_vec(), b"key".to_vec())
        .with_domain_name("orcher.local");
    assert_eq!(tls.domain_name.as_deref(), Some("orcher.local"));
}

#[test]
fn driver_configs_build_from_default_and_field_assignment() {
    let mut workflow = WorkflowDriverConfig::default();
    workflow.task_queue = "orders".into();
    let mut task = TaskDriverConfig::default();
    task.tls_config = Some(TlsConfig::new());
    let mut actor = ActorDriverConfig::default();
    actor.service_id = "svc".into();
    let mut poller = PollerConfig::default();
    poller.api_key = Some("key".into());
    let mut actor_poller = ActorPollerConfig::default();
    actor_poller.max_operations = 1;
    let mut registration = WorkerRegistrationConfig::default();
    registration.task_types = vec!["t".into()];
    let mut replay = ReplayConfig::default();
    replay.strict_mode = true;

    assert_eq!(workflow.task_queue, "orders");
    assert!(task.tls_config.is_some());
    assert_eq!(actor.service_id, "svc");
    assert_eq!(poller.api_key.as_deref(), Some("key"));
    assert_eq!(actor_poller.max_operations, 1);
    assert_eq!(registration.task_types, vec!["t".to_string()]);
    assert!(replay.strict_mode);
}

#[test]
fn every_error_with_fields_has_a_constructor_and_matches_with_rest_patterns() {
    let errors = [
        Error::workflow_not_found("wf"),
        Error::workflow_already_exists_with_run("wf", "run"),
        Error::workflow_execution_failed("boom"),
        Error::task_execution_failed("t", "boom"),
        Error::invalid_workflow_state("running", "closed"),
        Error::determinism_violation("r"),
        Error::invalid_execution_journal("r"),
        Error::invalid_payload("r"),
        Error::timeout("op"),
        Error::workflow_cancelled("wf"),
        Error::workflow_terminated("wf", Some("r")),
        Error::task_cancelled("t"),
        Error::invalid_command("r"),
        Error::invalid_event("r"),
        Error::resource_exhausted("quota"),
        Error::internal("x").context("ctx"),
    ];
    assert_eq!(errors.len(), 16);
    match &errors[1] {
        Error::WorkflowAlreadyExists {
            workflow_id,
            run_id,
            ..
        } => {
            assert_eq!(workflow_id, "wf");
            assert_eq!(run_id.as_deref(), Some("run"));
        }
        other => panic!("unexpected {other:?}"),
    }
}
