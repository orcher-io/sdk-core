//! Actors run through the host against a fake engine: handlers registered,
//! operations handed over by pull and reported with their failure types,
//! state read and written, every call with the worker's credentials.

mod common;

use std::time::Duration;

use common::Engine;
use orcher_sdk_core::proto::host::v1::{actor_result, ActorMode, ActorResult};
use orcher_sdk_core::proto::kernel::v1::{Failure, Payload};
use orcher_sdk_core::proto::orcher::v1::{ActorOperation, OperationMode};
use orcher_sdk_host::{ActorHandler, Host, HostConfig, Registrations, StateScope};

fn handlers() -> Registrations {
    Registrations::default().with_actor_handlers([
        ActorHandler::exclusive("Cart", "add_item"),
        ActorHandler::shared("Cart", "total"),
    ])
}

async fn actor_worker(engine: &Engine) -> Host {
    let addr = engine.serve().await;
    Host::start(
        HostConfig::new(format!("http://{addr}"), "orders")
            .with_shutdown_grace(std::time::Duration::from_millis(300))
            .with_namespace("shop")
            .with_api_key("key-1")
            .with_organization_id("org-1")
            .with_sdk("orcher-sdk-test", "1.2.3"),
        handlers(),
    )
    .await
    .unwrap()
}

async fn eventually(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..1500 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{what} did not happen in time");
}

fn operation(id: &str, mode: OperationMode) -> ActorOperation {
    ActorOperation {
        operation_id: id.into(),
        execution_id: format!("{id}-exec"),
        actor_name: "Cart".into(),
        key: "user-7".into(),
        operation: "add_item".into(),
        payload: br#"{"sku":"A1"}"#.to_vec(),
        mode: mode as i32,
        ..Default::default()
    }
}

/// The worker's handlers are registered, with their modes and what the
/// worker is, before any operation is polled for.
#[tokio::test]
async fn the_handlers_are_registered_with_their_modes() {
    let engine = Engine::default();
    let host = actor_worker(&engine).await;

    let registration = engine.script().handler_registrations[0].clone();
    assert_eq!(registration.service_id, host.config().identity);
    let handlers: Vec<(String, String, i32)> = registration
        .handlers
        .iter()
        .map(|handler| {
            (
                handler.actor_name.clone(),
                handler.operation.clone(),
                handler.mode,
            )
        })
        .collect();
    assert_eq!(
        handlers,
        [
            (
                "Cart".into(),
                "add_item".into(),
                OperationMode::Exclusive as i32
            ),
            ("Cart".into(), "total".into(), OperationMode::Shared as i32),
        ]
    );
    assert_eq!(registration.metadata["namespace"], "shop");
    assert_eq!(registration.metadata["task_queue"], "orders");
    assert_eq!(registration.metadata["sdk"], "orcher-sdk-test");
    host.shutdown().await;
}

/// An operation reaches the language whole, and its value and failures reach
/// the engine; a failure keeps the type the language gave it.
#[tokio::test]
async fn operations_and_their_results_round_trip() {
    let engine = Engine::default();
    engine.script().actor_operations = vec![
        operation("op-1", OperationMode::Shared),
        operation("op-2", OperationMode::Exclusive),
    ];
    let host = actor_worker(&engine).await;

    let first = host.next_actor_operation().await.expect("an operation");
    let second = host.next_actor_operation().await.expect("an operation");
    assert_eq!(first.operation_id, "op-1");
    assert_eq!(first.mode, ActorMode::Shared as i32);
    assert_eq!(second.mode, ActorMode::Exclusive as i32);
    assert_eq!(
        (first.actor_name.as_str(), first.key.as_str()),
        ("Cart", "user-7")
    );
    assert_eq!(first.payload.as_ref().unwrap().data, br#"{"sku":"A1"}"#);

    host.complete_actor_operation(ActorResult {
        operation_id: "op-1".into(),
        execution_id: "op-1-exec".into(),
        result: Some(actor_result::Result::Value(Payload {
            data: b"42".to_vec(),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    host.complete_actor_operation(ActorResult {
        operation_id: "op-2".into(),
        execution_id: "op-2-exec".into(),
        result: Some(actor_result::Result::Failure(Failure {
            message: "out of stock".into(),
            error_type: "OutOfStock".into(),
            ..Default::default()
        })),
    })
    .await
    .unwrap();

    eventually("both completions", || {
        engine.script().actor_completions.len() == 2
    })
    .await;
    let completions = engine.script().actor_completions.clone();
    let done = |id: &str| {
        completions
            .iter()
            .find(|done| done.operation_id == id)
            .unwrap()
    };
    assert_eq!(done("op-1").result, b"42");
    assert_eq!(done("op-2").error_code, "OutOfStock");
    assert_eq!(done("op-2").error_message, "out of stock");

    // An answer to an operation already answered is refused.
    let again = host
        .complete_actor_operation(ActorResult {
            operation_id: "op-1".into(),
            execution_id: "op-1-exec".into(),
            result: Some(actor_result::Result::Value(Payload::default())),
        })
        .await
        .unwrap_err();
    assert!(again.to_string().contains("no actor operation"), "{again}");
    host.shutdown().await;
}

/// State is read, written conditionally, listed and deleted through the
/// host.
#[tokio::test]
async fn actor_state_round_trips_through_the_host() {
    let engine = Engine::default();
    let host = actor_worker(&engine).await;
    let scope = StateScope::new("Cart", "user-7", "op-1-exec");

    assert_eq!(host.get_actor_state(&scope, "items").await.unwrap(), None);
    let version = host
        .set_actor_state(&scope, "items", b"[1]".to_vec(), None)
        .await
        .unwrap();
    host.set_actor_state(&scope, "item_count", b"1".to_vec(), None)
        .await
        .unwrap();
    let stored = host
        .get_actor_state(&scope, "items")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (stored.value.as_slice(), stored.version.as_str()),
        (&b"[1]"[..], version.as_str())
    );

    // A write expecting a version that is no longer current is refused.
    host.set_actor_state(&scope, "items", b"[1,2]".to_vec(), Some(&version))
        .await
        .unwrap();
    assert!(host
        .set_actor_state(&scope, "items", b"[9]".to_vec(), Some(&version))
        .await
        .is_err());

    assert_eq!(
        host.actor_state_keys(&scope, "item").await.unwrap(),
        ["item_count", "items"]
    );
    assert!(host.delete_actor_state(&scope, "items").await.unwrap());
    assert!(!host.delete_actor_state(&scope, "items").await.unwrap());
    host.shutdown().await;
}

/// Every call to the actor service carries the worker's API key and
/// organization: the registration, the driver's polls and completions, and
/// the state calls.
#[tokio::test]
async fn every_actor_call_carries_the_workers_credentials() {
    let engine = Engine::default();
    engine.script().actor_operations = vec![operation("op-1", OperationMode::Exclusive)];
    let host = actor_worker(&engine).await;
    let scope = StateScope::new("Cart", "user-7", "op-1-exec");

    host.next_actor_operation().await.unwrap();
    host.set_actor_state(&scope, "items", b"[]".to_vec(), None)
        .await
        .unwrap();
    host.get_actor_state(&scope, "items").await.unwrap();
    host.complete_actor_operation(ActorResult {
        operation_id: "op-1".into(),
        execution_id: "op-1-exec".into(),
        result: Some(actor_result::Result::Value(Payload::default())),
    })
    .await
    .unwrap();
    eventually("the completion", || {
        !engine.script().actor_completions.is_empty()
    })
    .await;

    let calls = engine.script().actor_credentials.clone();
    for method in [
        "register_handlers",
        "poll",
        "set_state",
        "get_state",
        "complete",
    ] {
        assert!(
            calls.iter().any(|(called, _, _)| *called == method),
            "no {method} call: {calls:?}"
        );
    }
    for (method, authorization, organization) in &calls {
        assert_eq!(
            authorization, "Bearer key-1",
            "{method} without the API key"
        );
        assert_eq!(organization, "org-1", "{method} without the organization");
    }
    host.shutdown().await;
}

/// An actor type registered with no operation is refused at start.
#[tokio::test]
async fn an_actor_type_with_no_operation_is_refused() {
    let engine = Engine::default();
    let addr = engine.serve().await;
    let error = Host::start(
        HostConfig::new(format!("http://{addr}"), "orders")
            .with_shutdown_grace(std::time::Duration::from_millis(300)),
        Registrations::new([] as [&str; 0], [] as [&str; 0], ["Cart"]),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("Cart"), "{error}");
    assert!(engine.script().handler_registrations.is_empty());
}

/// Shut down, the actor driver stops: pulls end.
#[tokio::test]
async fn actor_pulls_end_after_shutdown() {
    let engine = Engine::default();
    let host = actor_worker(&engine).await;
    let pulling = host.clone();
    let pull = tokio::spawn(async move { pulling.next_actor_operation().await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    tokio::time::timeout(Duration::from_secs(15), host.shutdown())
        .await
        .expect("shutdown in time");
    let pulled = tokio::time::timeout(Duration::from_secs(5), pull)
        .await
        .expect("the pull ended")
        .unwrap();
    assert!(pulled.is_none());
}

/// When the actor service asks for the handlers again, the host registers
/// them again. Waits out one heartbeat interval.
#[tokio::test]
async fn the_handlers_are_registered_again_when_the_actor_service_asks() {
    let engine = Engine::default();
    engine.script().re_register = true;
    let host = actor_worker(&engine).await;

    eventually("the second registration", || {
        engine.script().handler_registrations.len() >= 2
    })
    .await;
    let (heartbeats, registrations) = {
        let script = engine.script();
        (
            script.actor_heartbeats.clone(),
            script.handler_registrations.clone(),
        )
    };
    assert_eq!(heartbeats[0], "reg-1");
    assert_eq!(registrations[1].handlers, registrations[0].handlers);
    host.shutdown().await;
}
