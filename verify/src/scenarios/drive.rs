//! Runs the scenarios against an engine, through core's client, and judges
//! each by what a client observes.
//!
//! The judgement is sdk-rust's contract runner's (`contract/src/main.rs`),
//! kind by kind, with two differences that follow from the worker running
//! in another process:
//!
//! - "The workflow has parked" is read from the engine, not from the
//!   catalog's in-process marks: the run is open and waits on a timer or an
//!   event (`pending_timers` or `pending_events` in its description).
//! - What a scenario asserts about the worker's own process
//!   (`expect.observed`) is judged by what it leaves behind where a client
//!   can see it, or not at all; the report says which.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::StreamExt;
use orcher_sdk_core::client::{ActorClient, StartWorkflowOpts, WorkflowClient, WorkflowHandle};
use orcher_sdk_core::{
    ConnectOptions, Connection, Error, WorkflowExecution, WorkflowIdReusePolicy, WorkflowStatus,
};
use serde_json::Value;

use super::{result_matches, within_bounds, Kind, Outcome, Scenario, Summary, Verdict};

/// How an actor operation's argument is encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorArguments {
    /// The input as it is: what sdk-py's and sdk-ts's clients send.
    Value,
    /// The input as a JSON array of one, the argument tuple sdk-rust's
    /// operations decode.
    Tuple,
}

impl ActorArguments {
    /// The names `--actor-arguments` takes.
    pub const NAMES: [&'static str; 2] = ["value", "tuple"];

    /// The encoding `name` names.
    pub fn named(name: &str) -> Option<ActorArguments> {
        match name {
            "value" => Some(ActorArguments::Value),
            "tuple" => Some(ActorArguments::Tuple),
            _ => None,
        }
    }

    /// What `sdk`'s worker decodes: a tuple for sdk-rust, the value for
    /// every other.
    pub fn of_sdk(sdk: Option<&str>) -> ActorArguments {
        match sdk {
            Some("rust") => ActorArguments::Tuple,
            _ => ActorArguments::Value,
        }
    }

    fn encode(self, input: &Value) -> Vec<u8> {
        let value = match self {
            ActorArguments::Value => input.clone(),
            ActorArguments::Tuple => Value::Array(vec![input.clone()]),
        };
        serde_json::to_vec(&value).expect("JSON encodes")
    }
}

/// Where and how to run the scenarios.
#[derive(Debug, Clone)]
pub struct Options {
    /// The engine's gRPC endpoint.
    pub server_url: String,
    /// The namespace, created when missing.
    pub namespace: String,
    /// The task queue the worker under test serves.
    pub task_queue: String,
    /// The SDK whose known gaps apply (`known_gap_sdks`).
    pub sdk: Option<String>,
    /// An API key for an engine that authenticates callers.
    pub api_key: Option<String>,
    /// How long to wait for a workflow to end.
    pub result_timeout: Duration,
    /// How many scenarios run at once.
    pub parallel: usize,
    /// The command that restarts the engine, for `kind: "engine_restart"`.
    pub restart_command: Option<String>,
    /// How an actor operation's argument is encoded.
    pub actor_arguments: ActorArguments,
}

/// Runs `scenarios` against the engine `options` names, writing a line per
/// scenario and the summary to `out`.
///
/// With `parallel` above one, the scenarios run that many at once in
/// catalog order, except the ones that restart the engine, which run alone
/// at the end. Each scenario is reported in that order, whenever the ones
/// before it have finished.
///
/// # Errors
///
/// An engine that cannot be reached, a namespace that cannot be made, or
/// writing to `out`. A scenario that goes wrong is a failed scenario, not an
/// error.
pub async fn run(
    scenarios: &[&Scenario],
    options: &Options,
    out: &mut impl std::io::Write,
) -> Result<Summary, String> {
    let mut connect =
        ConnectOptions::new(options.server_url.clone()).with_namespace(options.namespace.clone());
    if let Some(key) = &options.api_key {
        connect = connect.with_api_key(key.clone());
    }
    let connection = Connection::connect(connect)
        .await
        .map_err(|e| format!("cannot connect to {}: {e}", options.server_url))?;
    ensure_namespace(&connection, &options.namespace).await?;

    let engine = Engine {
        workflows: connection
            .workflow_client()
            .with_timeout(options.result_timeout + Duration::from_secs(10)),
        actors: connection.actor_client(),
        options,
    };

    let parallel = options.parallel.max(1);
    let restarts = |s: &&&Scenario| Kind::named(s.kind.as_deref()) == Some(Kind::EngineRestart);
    let (first, last): (Vec<&&Scenario>, Vec<&&Scenario>) = if parallel > 1 {
        scenarios.iter().partition(|s| !restarts(s))
    } else {
        (scenarios.iter().collect(), Vec::new())
    };

    let mut summary = Summary::default();
    for (batch, width) in [(first, parallel), (last, 1)] {
        let mut verdicts = futures::stream::iter(batch)
            .map(|scenario| {
                let engine = &engine;
                async move { (*scenario, engine.verdict(scenario).await) }
            })
            .buffered(width);
        while let Some((scenario, verdict)) = verdicts.next().await {
            summary
                .report(scenario, &verdict, out)
                .map_err(|e| e.to_string())?;
            out.flush().map_err(|e| e.to_string())?;
        }
    }
    summary
        .finish(scenarios.len(), out)
        .map_err(|e| e.to_string())?;
    Ok(summary)
}

/// Creates `name` unless it exists.
async fn ensure_namespace(connection: &Connection, name: &str) -> Result<(), String> {
    let namespaces = connection.namespace_client();
    if namespaces.get_namespace(name).await.is_ok() {
        return Ok(());
    }
    match namespaces.create_namespace(name, 7).await {
        Ok(_) => Ok(()),
        // Made by someone else in the meantime.
        Err(e) if e.to_string().contains("already exists") => Ok(()),
        Err(e) => Err(format!("cannot create namespace {name:?}: {e}")),
    }
}

/// A key no other scenario of this or another run has: the catalog's
/// workflows and actors keep what they observe under it.
fn fresh_key(prefix: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!(
        "{prefix}-{nanos}-{}",
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// `input` with `key` set on it, as the keyed workflows of the catalog take.
fn keyed(input: &Value, key: &str) -> Value {
    let mut input = if input.is_object() {
        input.clone()
    } else {
        serde_json::json!({})
    };
    input["key"] = Value::String(key.to_string());
    input
}

fn mismatch(what: &str, expected: &Value, actual: &Value) -> String {
    format!("{what}\n    expected: {expected}\n    actual:   {actual}")
}

/// A check's result: `Err` says what failed.
type Check = Result<Outcome, String>;

/// How long a run waiting on nothing the engine describes is given to reach
/// its wait, before an event scenario takes it as parked out of sight.
const SETTLE: Duration = Duration::from_secs(3);

/// What became of a run the scenario waits to see parked.
enum Parking {
    /// The engine describes it waiting on a timer or an event.
    Seen,
    /// Open, and waiting on nothing the engine describes, after the settle
    /// time.
    Unseen,
    /// It ended first.
    Ended(WorkflowStatus),
    /// Not parked within the result timeout.
    Never,
}

struct Engine<'a> {
    workflows: WorkflowClient,
    actors: ActorClient,
    options: &'a Options,
}

impl Engine<'_> {
    async fn verdict(&self, scenario: &Scenario) -> Verdict {
        let outcome = match self.check(scenario).await {
            Ok(outcome) => outcome,
            Err(why) => Outcome::Failed(why),
        };
        Verdict::of(
            outcome,
            scenario.applicable_gap(self.options.sdk.as_deref()),
        )
    }

    async fn check(&self, scenario: &Scenario) -> Check {
        if let Some(why) = scenario.harness_only() {
            return Ok(Outcome::Skipped(why.to_string()));
        }
        let kind = Kind::named(scenario.kind.as_deref()).ok_or_else(|| {
            format!(
                "unknown scenario kind {:?}",
                scenario.kind.as_deref().unwrap_or("")
            )
        })?;
        match kind {
            Kind::Workflow => self.workflow(scenario).await,
            Kind::Actor => self.actor(scenario).await,
            Kind::Reset => self.reset(scenario).await,
            Kind::ClientError => self.client_error(scenario).await,
            Kind::Event => self.event(scenario).await,
            Kind::Cancel => self.cancel(scenario).await,
            Kind::WorkflowIdReuse => self.workflow_id_reuse(scenario).await,
            Kind::NonDeterminismHeld => self.held(scenario).await,
            Kind::EngineRestart => self.engine_restart(scenario).await,
            Kind::CancelQueuedTask | Kind::WorkerCrash => {
                unreachable!("harness_only covers it")
            }
        }
    }

    fn deadline(&self) -> Duration {
        self.options.result_timeout
    }

    /// Starts `workflow` as `workflow_id` on the worker's queue, its input
    /// as plain JSON.
    async fn start(
        &self,
        workflow: &str,
        input: &Value,
        workflow_id: &str,
        policy: Option<WorkflowIdReusePolicy>,
    ) -> Result<WorkflowHandle, Error> {
        let bytes = serde_json::to_vec(input).expect("JSON encodes");
        let mut opts = StartWorkflowOpts::new(
            workflow_id,
            workflow,
            self.options.task_queue.clone(),
            bytes,
        );
        if let Some(policy) = policy {
            opts = opts.with_id_reuse_policy(policy);
        }
        self.workflows.start_workflow_with_options(opts).await
    }

    async fn result(&self, handle: &WorkflowHandle) -> Result<Value, Error> {
        handle.result_with_timeout::<Value>(self.deadline()).await
    }

    /// Waits until the run is open and waits on a timer or an event, as the
    /// engine describes it: what the catalog's workflows do once they have
    /// parked.
    ///
    /// The engine describes a pending timer, but not a workflow parked on an
    /// event alone. With `settle`, a run still open and seen waiting on
    /// nothing after it is taken as parked out of sight
    /// ([`Parking::Unseen`]); without, it never parks.
    async fn parked(&self, handle: &WorkflowHandle, settle: Option<Duration>) -> Parking {
        let started = Instant::now();
        while started.elapsed() < self.deadline() {
            if let Ok(description) = handle.describe().await {
                if description.status.is_terminal() {
                    return Parking::Ended(description.status);
                }
                if description.status == WorkflowStatus::Running
                    && (description.pending_timers > 0 || description.pending_events > 0)
                {
                    return Parking::Seen;
                }
            }
            if settle.is_some_and(|settle| started.elapsed() >= settle) {
                return Parking::Unseen;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Parking::Never
    }

    /// Start the workflow, wait for its end, and match status, result,
    /// failure message and wall clock.
    async fn workflow(&self, scenario: &Scenario) -> Check {
        let workflow = scenario
            .workflow
            .as_deref()
            .ok_or("workflow scenario missing `workflow`")?;
        let expect = scenario
            .expect
            .as_ref()
            .ok_or("workflow scenario missing `expect`")?;

        let started_at = Instant::now();
        let handle = self
            .start(workflow, &scenario.input, &fresh_key("verify"), None)
            .await
            .map_err(|e| format!("starting workflow {workflow}: {e}"))?;
        let outcome = self.result(&handle).await;

        match (expect.status.as_str(), outcome) {
            ("completed", Ok(result)) => {
                // A durable timer must wait, and not far longer than asked.
                let elapsed_ms = started_at.elapsed().as_millis();
                if let Some(min) = expect.min_elapsed_ms {
                    if elapsed_ms < min {
                        return Err(format!(
                            "completed too fast: {elapsed_ms}ms < {min}ms (the wait did not happen)"
                        ));
                    }
                }
                if let Some(max) = expect.max_elapsed_ms {
                    if elapsed_ms > max {
                        return Err(format!(
                            "took too long: {elapsed_ms}ms > {max}ms (waited far longer than asked)"
                        ));
                    }
                }
                if result_matches(expect, &result) {
                    Ok(Outcome::Passed)
                } else {
                    Err(mismatch("result mismatch", &expect.result, &result))
                }
            }
            ("completed", Err(e)) => Err(format!("expected completion but workflow failed: {e}")),
            ("failed", Err(e)) => {
                if let Some(wanted) = expect.error_contains.as_deref() {
                    let surfaced = e.to_string();
                    if !surfaced.contains(wanted) {
                        return Err(format!(
                            "failed as expected, but the message did not survive\n    wanted substring: {wanted}\n    surfaced:         {surfaced}"
                        ));
                    }
                }
                Ok(Outcome::Passed)
            }
            ("failed", Ok(result)) => Err(format!(
                "expected failure but workflow completed with: {result}"
            )),
            (other, _) => Err(format!("unknown expected status '{other}'")),
        }
    }

    /// Run to completion, reset to `reset_to_event_id`, and match what the
    /// successor run returns.
    async fn reset(&self, scenario: &Scenario) -> Check {
        let workflow = scenario
            .workflow
            .as_deref()
            .ok_or("reset scenario missing `workflow`")?;
        let expect = scenario
            .expect
            .as_ref()
            .ok_or("reset scenario missing `expect`")?;
        let target = scenario
            .reset_to_event_id
            .ok_or("reset scenario missing `reset_to_event_id`")?;

        let workflow_id = fresh_key("verify-reset");
        let handle = self
            .start(workflow, &scenario.input, &workflow_id, None)
            .await
            .map_err(|e| format!("starting workflow {workflow}: {e}"))?;
        self.result(&handle)
            .await
            .map_err(|e| format!("original run did not complete: {e}"))?;

        let successor_run = handle
            .reset(target, "orcher-verify reset")
            .await
            .map_err(|e| format!("reset rejected: {e}"))?;
        if successor_run.is_empty() {
            return Err(
                "reset returned an empty execution id (reported success without resetting)"
                    .to_string(),
            );
        }
        let successor = WorkflowHandle::new(
            self.workflows.clone(),
            WorkflowExecution::new(workflow_id, successor_run.clone()),
        );
        let result = self
            .result(&successor)
            .await
            .map_err(|e| format!("successor run {successor_run} did not complete: {e}"))?;

        if expect.status != "completed" {
            return Err(format!(
                "reset scenarios only support an expected status of 'completed', got '{}'",
                expect.status
            ));
        }
        if super::json_matches(&expect.result, &result) {
            Ok(Outcome::Passed)
        } else {
            Err(mismatch(
                "successor result mismatch",
                &expect.result,
                &result,
            ))
        }
    }

    /// For each step, `parallel` concurrent invocations on a fresh key, each
    /// result matched against `expect_each`.
    async fn actor(&self, scenario: &Scenario) -> Check {
        let actor = scenario
            .actor
            .as_deref()
            .ok_or("actor scenario missing `actor`")?;
        let key = fresh_key("verify");
        for step in &scenario.steps {
            let payload = self.options.actor_arguments.encode(&step.input);
            let invocations = (0..step.parallel).map(|_| {
                self.actors.invoke_operation(
                    actor.to_string(),
                    key.clone(),
                    step.operation.clone(),
                    payload.clone(),
                    None,
                    None,
                )
            });
            for outcome in futures::future::join_all(invocations).await {
                let bytes =
                    outcome.map_err(|e| format!("invoking operation {}: {e}", step.operation))?;
                let result: Value = serde_json::from_slice(&bytes)
                    .map_err(|e| format!("operation {} returned no JSON: {e}", step.operation))?;
                if !super::json_matches(&step.expect_each, &result) {
                    return Err(mismatch(
                        &format!("result mismatch on {}", step.operation),
                        &step.expect_each,
                        &result,
                    ));
                }
            }
        }
        Ok(Outcome::Passed)
    }

    /// Make the named failing call and match the error code it reports.
    async fn client_error(&self, scenario: &Scenario) -> Check {
        let expected = scenario
            .expect
            .as_ref()
            .and_then(|e| e.error_code.clone())
            .ok_or("client_error scenario missing `expect.error_code`")?;
        let call = scenario
            .client_call
            .as_deref()
            .ok_or("client_error scenario missing `client_call`")?;
        let outcome = match call {
            "status_of_missing_workflow" => self
                .workflows
                .get_workflow_status(fresh_key("verify-missing"), None)
                .await
                .map(|_| ()),
            other => return Err(format!("unknown client_call: {other}")),
        };
        match outcome {
            Ok(()) => Err(format!(
                "expected the call to fail with {expected}, but it succeeded"
            )),
            Err(e) => {
                let actual = e.code().as_str();
                if actual == expected {
                    Ok(Outcome::Passed)
                } else {
                    Err(format!(
                        "expected error code {expected}, got {actual} ({e})"
                    ))
                }
            }
        }
    }

    /// Start the workflow, wait for it to park, wake it with the event by
    /// workflow id alone, and match the result and its bounds.
    async fn event(&self, scenario: &Scenario) -> Check {
        let workflow = scenario
            .workflow
            .as_deref()
            .ok_or("event scenario missing `workflow`")?;
        let expect = scenario
            .expect
            .as_ref()
            .ok_or("event scenario missing `expect`")?;
        let event_name = scenario
            .event_name
            .as_deref()
            .ok_or("event scenario missing `event_name`")?;

        let key = fresh_key("verify");
        let workflow_id = format!("verify-event-{key}");
        let handle = self
            .start(workflow, &keyed(&scenario.input, &key), &workflow_id, None)
            .await
            .map_err(|e| format!("starting workflow {workflow}: {e}"))?;

        let seen = match self.parked(&handle, Some(SETTLE)).await {
            Parking::Seen => true,
            Parking::Unseen => false,
            Parking::Ended(status) => {
                return Err(format!(
                    "the workflow ended {status:?} before the event (the wait was not reached, or the activation failed)"
                ))
            }
            Parking::Never => {
                return Err(
                    "the workflow never parked (the wait was not reached, or the activation failed)"
                        .to_string(),
                )
            }
        };
        // Let the engine record the parking completion first, so the event
        // wakes a parked workflow rather than meeting its first activation.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // By workflow id alone: a caller holding a business key has no run id.
        self.workflows
            .send_event(
                workflow_id.clone(),
                None,
                event_name,
                scenario.event_payload.clone(),
            )
            .await
            .map_err(|e| format!("sending the event: {e}"))?;

        let result = match self.result(&handle).await {
            Ok(result) => result,
            Err(e) => {
                // Leave no parked run behind.
                let _ = handle.cancel().await;
                return Err(format!("expected completion after the event but got: {e}"));
            }
        };
        if !super::json_matches(&expect.result, &result) {
            return Err(mismatch("result mismatch", &expect.result, &result));
        }
        within_bounds(&expect.result_at_most, &result)?;
        Ok(if seen {
            Outcome::Passed
        } else {
            Outcome::PassedInPart(format!(
                "the engine describes no wait on an event, so the workflow was taken as parked once \
                 open for {}s; that the event woke a parked run, rather than reaching its first \
                 activation, is not observed",
                SETTLE.as_secs()
            ))
        })
    }

    /// Start the workflow, cancel it once it has parked, and match how it
    /// ended: cancelled, or completed with a result after cleaning up.
    async fn cancel(&self, scenario: &Scenario) -> Check {
        let workflow = scenario
            .workflow
            .as_deref()
            .ok_or("cancel scenario missing `workflow`")?;
        let expect = scenario
            .expect
            .as_ref()
            .ok_or("cancel scenario missing `expect`")?;

        let key = fresh_key("verify");
        let handle = self
            .start(
                workflow,
                &keyed(&scenario.input, &key),
                &fresh_key("verify-cancel"),
                None,
            )
            .await
            .map_err(|e| format!("starting workflow {workflow}: {e}"))?;

        // The catalog's cancel workflows park on a sleep, which the engine
        // describes; one that is never seen parked fails.
        if !matches!(self.parked(&handle, None).await, Parking::Seen) {
            let _ = handle.cancel().await;
            return Err("the workflow never parked".to_string());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        handle
            .cancel()
            .await
            .map_err(|e| format!("cancelling the workflow: {e}"))?;

        match expect.status.as_str() {
            "cancelled" => {
                let until = Instant::now() + self.deadline();
                loop {
                    match handle
                        .status()
                        .await
                        .map_err(|e| format!("reading the status: {e}"))?
                    {
                        WorkflowStatus::Cancelled => return Ok(Outcome::Passed),
                        WorkflowStatus::Running if Instant::now() < until => {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                        other => {
                            return Err(format!("expected the workflow cancelled, it is {other:?}"))
                        }
                    }
                }
            }
            "completed" => {
                let result = self
                    .result(&handle)
                    .await
                    .map_err(|e| format!("expected completion after cleanup but got: {e}"))?;
                if super::json_matches(&expect.result, &result) {
                    Ok(Outcome::Passed)
                } else {
                    Err(mismatch("result mismatch", &expect.result, &result))
                }
            }
            other => Err(format!(
                "unknown expected status '{other}' for a cancel scenario"
            )),
        }
    }

    /// Start one id, start it again while it runs (refused, naming the
    /// run), let it complete, start it again reusing only after a failure
    /// (refused the same way), then plainly (a new run, which completes).
    async fn workflow_id_reuse(&self, scenario: &Scenario) -> Check {
        let workflow = scenario
            .workflow
            .as_deref()
            .ok_or("workflow_id_reuse scenario missing `workflow`")?;
        let expect = scenario
            .expect
            .as_ref()
            .ok_or("workflow_id_reuse scenario missing `expect`")?;
        let expected_code = expect
            .error_code
            .as_deref()
            .ok_or("workflow_id_reuse scenario missing `expect.error_code`")?;

        let workflow_id = fresh_key("verify-reuse");
        let start = |policy| self.start(workflow, &scenario.input, &workflow_id, policy);
        let refused = |outcome: Result<WorkflowHandle, Error>,
                       in_the_way: &str,
                       when: &str|
         -> Result<(), String> {
            match outcome {
                Ok(handle) => Err(format!(
                    "{when}: the start was accepted as run {:?}",
                    handle.run_id()
                )),
                Err(e) => {
                    let code = e.code().as_str();
                    if code != expected_code {
                        return Err(format!(
                            "{when}: expected {expected_code}, got {code} ({e})"
                        ));
                    }
                    match e {
                        Error::WorkflowAlreadyExists {
                            run_id: Some(ref run_id),
                            ..
                        } if run_id == in_the_way => Ok(()),
                        other => Err(format!(
                            "{when}: the refusal does not name run {in_the_way}: {other:?}"
                        )),
                    }
                }
            }
        };
        let completes = |result: Result<Value, Error>, which: &str| -> Result<(), String> {
            let result = result.map_err(|e| format!("the {which} run did not complete: {e}"))?;
            if super::json_matches(&expect.result, &result) {
                Ok(())
            } else {
                Err(mismatch(
                    &format!("the {which} run's result"),
                    &expect.result,
                    &result,
                ))
            }
        };

        let first = start(None)
            .await
            .map_err(|e| format!("starting workflow {workflow}: {e}"))?;
        let first_run = first.run_id().to_string();
        if first_run.is_empty() {
            return Err("the first start returned no run id".to_string());
        }
        refused(
            start(None).await,
            &first_run,
            "a second start while it runs",
        )?;
        completes(self.result(&first).await, "first")?;
        refused(
            start(Some(WorkflowIdReusePolicy::AllowDuplicateFailedOnly)).await,
            &first_run,
            "reusing only after a failure, once it completed",
        )?;
        let again = start(Some(WorkflowIdReusePolicy::AllowDuplicate))
            .await
            .map_err(|e| format!("starting it again once it completed: {e}"))?;
        if again.run_id() == first_run {
            return Err("the new start reported the first run's id".to_string());
        }
        completes(self.result(&again).await, "second")?;
        Ok(Outcome::Passed)
    }

    /// Start a workflow whose replay names its child differently, and check
    /// the run is held rather than carried on.
    ///
    /// sdk-rust's runner reads, in the worker's process, that the workflow
    /// was replayed and how many times its child ran. A client reads what
    /// those leave in the engine instead: the catalog names the child
    /// `<key>-child-<n>`, `n` counting the parent's activations in the
    /// worker, so the first child is `<key>-child-1`, and a replay let
    /// through would start `<key>-child-2`. Once the first child has
    /// completed (the engine resumes the parent with its result: the replay
    /// under test), the run is watched for `hold_ms`; then it must still be
    /// running and no second child may exist. That the worker did replay it
    /// is not seen, which the report says. The run is terminated after.
    async fn held(&self, scenario: &Scenario) -> Check {
        let workflow = scenario
            .workflow
            .as_deref()
            .ok_or("held scenario missing `workflow`")?;
        let expect = scenario
            .expect
            .as_ref()
            .ok_or("held scenario missing `expect`")?;
        let expected_status = match expect.status.as_str() {
            "running" => WorkflowStatus::Running,
            other => {
                return Err(format!(
                    "unknown expected status '{other}' for a held scenario"
                ))
            }
        };

        let key = fresh_key("verify");
        let handle = self
            .start(
                workflow,
                &keyed(&scenario.input, &key),
                &fresh_key("verify-held"),
                None,
            )
            .await
            .map_err(|e| format!("starting workflow {workflow}: {e}"))?;

        let outcome = async {
            let first_child = format!("{key}-child-1");
            let until = Instant::now() + self.deadline();
            loop {
                match self.workflows.get_workflow_status(&first_child, None).await {
                    Ok(WorkflowStatus::Completed) => break,
                    Ok(WorkflowStatus::Running) | Err(Error::WorkflowNotFound { .. })
                        if Instant::now() < until =>
                    {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Ok(other) => {
                        return Err(format!(
                            "the child {first_child} ended {other:?}, not completed"
                        ))
                    }
                    Err(e) => return Err(format!("the child {first_child} never completed: {e}")),
                }
            }
            tokio::time::sleep(Duration::from_millis(scenario.hold_ms.unwrap_or(5_000))).await;

            let status = handle
                .status()
                .await
                .map_err(|e| format!("reading the status: {e}"))?;
            if status != expected_status {
                let ended = match status {
                    WorkflowStatus::Completed => match handle.result::<Value>().await {
                        Ok(result) => format!(" with {result}"),
                        Err(e) => format!(" ({e})"),
                    },
                    _ => String::new(),
                };
                return Err(format!(
                    "expected the run held {expected_status:?}, it is {status:?}{ended}"
                ));
            }
            let second_child = format!("{key}-child-2");
            match self.workflows.get_workflow_status(&second_child, None).await {
                Err(Error::WorkflowNotFound { .. }) => {}
                Ok(status) => {
                    return Err(format!(
                        "a second child {second_child} was started ({status:?}): the replay was let through"
                    ))
                }
                Err(e) => return Err(format!("reading the child {second_child}: {e}")),
            }
            Ok(Outcome::PassedInPart(format!(
                "`expect.observed` ({}) is in the worker's process; judged instead by the first \
                 child completing and no second child, `<key>-child-2`, starting. That the worker \
                 replayed the run is not observed.",
                expect.observed
            )))
        }
        .await;

        let _ = handle.terminate("orcher-verify: held run checked").await;
        outcome
    }

    /// Restart the engine with the restart command, then start a workflow
    /// and expect the worker, untouched, to complete it. Skipped without a
    /// restart command: nothing else can restart an engine this did not
    /// start.
    async fn engine_restart(&self, scenario: &Scenario) -> Check {
        let Some(command) = self.options.restart_command.as_deref() else {
            return Ok(Outcome::Skipped(
                "no restart command: pass --restart-command or set ORCHER_CONTRACT_RESTART_CMD"
                    .to_string(),
            ));
        };
        let workflow = scenario
            .workflow
            .as_deref()
            .ok_or("engine_restart scenario missing `workflow`")?;
        let expect = scenario
            .expect
            .as_ref()
            .ok_or("engine_restart scenario missing `expect`")?;

        let status = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .status()
            .await
            .map_err(|e| format!("running the restart command: {e}"))?;
        if !status.success() {
            return Err(format!("restart command failed with {status}"));
        }
        // Recovery is the assertion: nothing here touches the worker.
        tokio::time::sleep(Duration::from_secs(5)).await;

        let handle = self
            .start(
                workflow,
                &scenario.input,
                &fresh_key("verify-restart"),
                None,
            )
            .await
            .map_err(|e| format!("starting workflow {workflow} after the restart: {e}"))?;
        let result = self.result(&handle).await.map_err(|e| {
            format!("the worker never picked the workflow up after the engine restart: {e}")
        })?;
        if super::json_matches(&expect.result, &result) {
            Ok(Outcome::Passed)
        } else {
            Err(mismatch("result mismatch", &expect.result, &result))
        }
    }
}
