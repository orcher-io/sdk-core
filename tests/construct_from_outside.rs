//! Checks that every `#[non_exhaustive]` type a language SDK or user builds
//! can still be built from outside this crate.
//!
//! The attribute only restricts code in other crates, so only an integration
//! test can catch a type that has become impossible to construct.

use std::time::Duration;

use orcher_sdk_core::client::workflow::{CancelWorkflowOpts, StartWorkflowOpts};
use orcher_sdk_core::types::RetryPolicy;
use orcher_sdk_core::{
    ConnectOptions, Error, HostConfig, ListWorkflowsOptions, ListWorkflowsSortOrder, Registrations,
    SearchWorkflowsOptions, TlsConfig, WorkflowStatus,
};

#[test]
fn options_build_with_their_methods() {
    let opts = StartWorkflowOpts::new("wf-1", "Order", "orders", b"{}".to_vec())
        .with_cron_schedule("* * * * *")
        .with_execution_timeout(Duration::from_secs(60));
    assert_eq!(opts.workflow_id, "wf-1");
    assert_eq!(opts.execution_timeout, Some(Duration::from_secs(60)));

    let cancel = CancelWorkflowOpts::default().with_cleanup_timeout(Duration::from_secs(30));
    assert_eq!(cancel.cleanup_timeout, Some(Duration::from_secs(30)));

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
fn a_host_configuration_and_its_registrations_build_from_outside() {
    let config = HostConfig::new("http://localhost:50051", "orders")
        .with_namespace("shop")
        .with_tls(TlsConfig::new())
        .with_protocol_version(orcher_sdk_core::worker_protocol::CANCEL_REQUEST);
    assert_eq!(config.task_queue, "orders");
    assert_eq!(config.connection.namespace, "shop");
    assert!(config.connection.tls.is_some());

    let options = ConnectOptions::new("https://engine.example")
        .with_namespace("shop")
        .with_api_key("key")
        .with_organization_id("org-1")
        .with_tls(TlsConfig::new())
        .with_connect_timeout(Duration::from_secs(2))
        .with_max_message_bytes(1 << 20);
    let config = HostConfig::new(options.clone(), "orders");
    assert_eq!(config.connection.url, "https://engine.example");
    assert_eq!(config.connection.connect_timeout, Duration::from_secs(2));
    assert!(!format!("{options:?}").contains("\"key\""), "{options:?}");

    let registrations = Registrations::new(["checkout"], ["charge"], Vec::<String>::new());
    assert_eq!(registrations.task_types, vec!["charge".to_string()]);
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
