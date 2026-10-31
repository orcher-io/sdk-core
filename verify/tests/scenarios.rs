//! `orcher-verify scenarios` refuses to run without what it needs. Running
//! the scenarios themselves needs an engine and a worker; see
//! `verify/README.md`.

use std::process::{Command, Output};

const BIN: &str = env!("CARGO_BIN_EXE_orcher-verify");

fn scenarios(args: &[&str]) -> Output {
    Command::new(BIN)
        .arg("scenarios")
        .args(args)
        .env_remove("ORCHER_API_KEY")
        .env_remove("ORCHER_CONTRACT_RESTART_CMD")
        .output()
        .expect("run orcher-verify")
}

fn refused(output: &Output, says: &str) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(2), "{stderr}");
    assert!(stderr.contains(says), "{stderr}");
}

#[test]
fn it_needs_an_engine_and_a_task_queue() {
    refused(&scenarios(&["--task-queue", "q"]), "--server-url");
    refused(
        &scenarios(&["--server-url", "http://127.0.0.1:1"]),
        "--task-queue",
    );
}

#[test]
fn it_refuses_a_scenario_the_catalog_does_not_have() {
    refused(
        &scenarios(&[
            "--server-url",
            "http://127.0.0.1:1",
            "--task-queue",
            "q",
            "--only",
            "echo,no-such-scenario",
        ]),
        "no-such-scenario",
    );
}

#[test]
fn it_refuses_a_catalog_it_cannot_read() {
    refused(
        &scenarios(&[
            "--server-url",
            "http://127.0.0.1:1",
            "--task-queue",
            "q",
            "--scenarios",
            "/nonexistent/scenarios.json",
        ]),
        "cannot read",
    );
}
