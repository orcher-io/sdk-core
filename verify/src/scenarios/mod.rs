//! The contract scenarios: the catalog every SDK's contract suite runs
//! against a live engine, and a runner that judges a runtime's worker by it
//! from the client side.
//!
//! [`scenarios.json`](https://github.com/orcher-io/sdk-core/blob/main/verify/scenarios.json)
//! names, for each scenario, a workflow (or actor) of the contract catalog,
//! the input it is started with, and what a client must observe: the terminal
//! status and the JSON the workflow returns. Every SDK's worker serves the
//! same-named catalog, so one runner judges any of them. The runtime author
//! starts their worker on a task queue; [`run`] starts each scenario's
//! workflow there through core's client and judges what comes back.
//!
//! A few scenarios assert what only the worker's own process can see, or
//! start and kill workers of their own. Those are judged in part, or skipped
//! with the reason, never passed: see [`Scenario::harness_only`] and
//! `verify/README.md`.
//!
//! - this module: the catalog, how a result is matched, and the report.
//! - [`drive`]: running the scenarios against an engine.

pub mod drive;

use std::collections::{BTreeMap, HashMap};
use std::io::{self, Write};
use std::path::PathBuf;

use serde::Deserialize;
use serde_json::Value;

pub use drive::{run, Options};

/// `scenarios.json` as built into the binary: a byte-for-byte copy of the
/// SDK contract suites' catalog (`contract/scenarios.json` in sdk-rust,
/// sdk-py and sdk-ts), of which this one is now the source.
pub static EMBEDDED: &str = include_str!("../../scenarios.json");

/// Where the catalog comes from.
#[derive(Debug, Clone)]
pub enum Source {
    /// The catalog built into the binary.
    Embedded,
    /// A catalog file laid out like `scenarios.json`.
    File(PathBuf),
}

/// The catalog.
#[derive(Debug, Clone, Deserialize)]
pub struct Catalog {
    /// The scenarios, in the order they run.
    pub scenarios: Vec<Scenario>,
}

/// Reads the catalog from `source`.
///
/// # Errors
///
/// A file that cannot be read, or is not a catalog.
pub fn load(source: &Source) -> Result<Catalog, String> {
    let text = match source {
        Source::Embedded => EMBEDDED.to_string(),
        Source::File(path) => std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?,
    };
    serde_json::from_str(&text).map_err(|e| format!("not a scenario catalog: {e}"))
}

/// One scenario. The fields are those of `scenarios.json`; which apply
/// depends on `kind`.
#[derive(Debug, Clone, Deserialize)]
pub struct Scenario {
    /// Unique in the catalog; `--only` selects by it.
    pub id: String,
    /// Absent (or `workflow`) starts a workflow and waits for its end; the
    /// others are listed on [`Kind`].
    #[serde(default)]
    pub kind: Option<String>,
    /// The catalog workflow it starts.
    #[serde(default)]
    pub workflow: Option<String>,
    /// The workflow's input.
    #[serde(default)]
    pub input: Value,
    /// What a client must observe.
    #[serde(default)]
    pub expect: Option<Expect>,
    /// The catalog actor, for `kind: "actor"`.
    #[serde(default)]
    pub actor: Option<String>,
    /// The operations invoked on the actor, for `kind: "actor"`.
    #[serde(default)]
    pub steps: Vec<Step>,
    /// The journal event to reset to (inclusive), for `kind: "reset"`.
    #[serde(default)]
    pub reset_to_event_id: Option<i64>,
    /// The named failing client call, for `kind: "client_error"`.
    #[serde(default)]
    pub client_call: Option<String>,
    /// The event sent once the workflow has parked, for `kind: "event"`.
    #[serde(default)]
    pub event_name: Option<String>,
    /// The event's payload.
    #[serde(default)]
    pub event_payload: Value,
    /// Set when the behavior is specified but not implemented: the scenario
    /// is expected to fail, and a pass is reported so the flag goes.
    #[serde(default)]
    pub known_gap: Option<String>,
    /// The SDKs the gap applies to; absent means all of them.
    #[serde(default)]
    pub known_gap_sdks: Option<Vec<String>>,
    /// How long a held run is watched once replayed, for `kind:
    /// "non_determinism_held"`.
    #[serde(default)]
    pub hold_ms: Option<u64>,
}

/// What a scenario expects.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Expect {
    /// `completed`, `failed`, `cancelled` or `running`; absent for
    /// `kind: "client_error"`.
    #[serde(default)]
    pub status: String,
    /// The result a completed workflow returns: the fields it lists must be
    /// there and equal ([`json_matches`]), unless `result_exact`.
    #[serde(default)]
    pub result: Value,
    /// The whole result must equal `result`, with no extra fields.
    #[serde(default)]
    pub result_exact: bool,
    /// The error code a refused call reports.
    #[serde(default)]
    pub error_code: Option<String>,
    /// A substring the failure a failed workflow surfaces must contain.
    #[serde(default)]
    pub error_contains: Option<String>,
    /// Bounds on the whole run's wall clock.
    #[serde(default)]
    pub min_elapsed_ms: Option<u128>,
    #[serde(default)]
    pub max_elapsed_ms: Option<u128>,
    /// Upper bounds on numeric result fields.
    #[serde(default)]
    pub result_at_most: HashMap<String, f64>,
    /// What the worker observed in its own process.
    #[serde(default)]
    pub observed: Value,
}

impl Expect {
    /// Check the whole-run wall clock against the scenario's optional bounds.
    fn check_elapsed_bounds(&self, elapsed_ms: u128) -> std::result::Result<(), String> {
        if let Some(min) = self.min_elapsed_ms {
            if elapsed_ms < min {
                return Err(format!(
                    "completed too fast: {elapsed_ms}ms < {min}ms (the wait did not happen)"
                ));
            }
        }
        if let Some(max) = self.max_elapsed_ms {
            if elapsed_ms > max {
                return Err(format!(
                    "took too long: {elapsed_ms}ms > {max}ms (waited far longer than asked)"
                ));
            }
        }
        Ok(())
    }
}

/// One step of an actor scenario: `parallel` concurrent invocations of
/// `operation`, each result matched against `expect_each`.
#[derive(Debug, Clone, Deserialize)]
pub struct Step {
    pub operation: String,
    #[serde(default)]
    pub input: Value,
    #[serde(default = "one")]
    pub parallel: usize,
    #[serde(default)]
    pub expect_each: Value,
}

fn one() -> usize {
    1
}

/// The scenario kinds, as `kind` names them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Start the workflow, wait for its end, match status and result.
    Workflow,
    /// Invoke operations on a catalog actor.
    Actor,
    /// Run to completion, reset to an event, match the successor's result.
    Reset,
    /// Make a failing client call and match its error code.
    ClientError,
    /// Wake the parked workflow with an event, match the result.
    Event,
    /// Cancel the parked workflow, match how it ended.
    Cancel,
    /// Cancel a workflow parked on a task no worker could start yet.
    CancelQueuedTask,
    /// Start one id again while it runs and after it completed.
    WorkflowIdReuse,
    /// A workflow whose replay is not deterministic is held.
    NonDeterminismHeld,
    /// Restart the engine under the worker, then dispatch to it.
    EngineRestart,
    /// A task that kills its worker process is retried on another.
    WorkerCrash,
}

impl Kind {
    /// The kind `kind` names; `None` for one this runner does not know.
    pub fn named(kind: Option<&str>) -> Option<Kind> {
        Some(match kind {
            None | Some("workflow") => Kind::Workflow,
            Some("actor") => Kind::Actor,
            Some("reset") => Kind::Reset,
            Some("client_error") => Kind::ClientError,
            Some("event") => Kind::Event,
            Some("cancel") => Kind::Cancel,
            Some("cancel_queued_task") => Kind::CancelQueuedTask,
            Some("workflow_id_reuse") => Kind::WorkflowIdReuse,
            Some("non_determinism_held") => Kind::NonDeterminismHeld,
            Some("engine_restart") => Kind::EngineRestart,
            Some("worker_crash") => Kind::WorkerCrash,
            Some(_) => return None,
        })
    }
}

impl Scenario {
    /// What it is labelled with in the report: the actor, workflow or
    /// client call it exercises.
    pub fn label(&self) -> &str {
        let name = if self.kind.as_deref() == Some("actor") {
            self.actor.as_deref()
        } else {
            self.workflow.as_deref().or(self.client_call.as_deref())
        };
        name.unwrap_or("")
    }

    /// The gap, if it applies to `sdk`. A gap scoped to some SDKs applies
    /// to none when no SDK is named.
    pub fn applicable_gap(&self, sdk: Option<&str>) -> Option<&str> {
        let applies = match (&self.known_gap_sdks, sdk) {
            (None, _) => true,
            (Some(sdks), Some(sdk)) => sdks.iter().any(|s| s == sdk),
            (Some(_), None) => false,
        };
        self.known_gap.as_deref().filter(|_| applies)
    }

    /// Why a client cannot judge this scenario at all, when it cannot: it
    /// needs the SDK's own harness, which starts workers of its own and sees
    /// inside them.
    pub fn harness_only(&self) -> Option<&'static str> {
        match Kind::named(self.kind.as_deref())? {
            Kind::CancelQueuedTask => Some(
                "needs the SDK's own harness: it serves a fresh task queue with a worker that runs \
                 workflows and no tasks, starts one that runs tasks only after the cancellation, \
                 and asserts what that worker observed in its own process",
            ),
            Kind::WorkerCrash => Some(
                "needs the SDK's own harness: it starts a worker process whose task kills it \
                 (ORCHER_CONTRACT_DOOMED_WORKER) and then another, on a queue of their own",
            ),
            _ => None,
        }
    }
}

/// The scenarios `only` names, in catalog order; all of them when it is
/// empty.
///
/// # Errors
///
/// An id `only` names that the catalog does not have.
pub fn select<'a>(catalog: &'a Catalog, only: &[String]) -> Result<Vec<&'a Scenario>, String> {
    if let Some(unknown) = only
        .iter()
        .find(|id| !catalog.scenarios.iter().any(|s| &s.id == *id))
    {
        return Err(format!("the catalog has no scenario {unknown:?}"));
    }
    Ok(catalog
        .scenarios
        .iter()
        .filter(|s| only.is_empty() || only.contains(&s.id))
        .collect())
}

/// Structural match: every field `expected` lists must be in `actual` and
/// match there, and arrays must have the same length and match item by
/// item. Extra fields in `actual` are allowed, so a scenario asserts only
/// what it lists; anything else must be equal JSON.
pub fn json_matches(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Object(exp), Value::Object(act)) => exp
            .iter()
            .all(|(k, v)| act.get(k).is_some_and(|av| json_matches(v, av))),
        (Value::Array(exp), Value::Array(act)) => {
            exp.len() == act.len() && exp.iter().zip(act).all(|(e, a)| json_matches(e, a))
        }
        _ => expected == actual,
    }
}

/// Whether `actual` is the result `expect` asks for: equal JSON with
/// `result_exact`, a [`json_matches`] match otherwise.
pub fn result_matches(expect: &Expect, actual: &Value) -> bool {
    if expect.result_exact {
        &expect.result == actual
    } else {
        json_matches(&expect.result, actual)
    }
}

/// Every field `bounds` lists must be a number in `actual` no greater than
/// its bound.
///
/// # Errors
///
/// The first field missing, not a number or over its bound.
pub fn within_bounds(bounds: &HashMap<String, f64>, actual: &Value) -> Result<(), String> {
    let object = actual
        .as_object()
        .ok_or_else(|| "result is not an object".to_string())?;
    // In name order, so the same result always reports the same field.
    let bounds: BTreeMap<_, _> = bounds.iter().collect();
    for (field, max) in bounds {
        let value = object
            .get(field)
            .and_then(Value::as_f64)
            .ok_or_else(|| format!("{field} is missing or not a number"))?;
        if value > *max {
            return Err(format!("{field} = {value}, expected at most {max}"));
        }
    }
    Ok(())
}

/// What running a scenario showed, before any known gap is applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Everything it asserts held.
    Passed,
    /// What a client can observe held; the rest is in the worker's process,
    /// out of a client's sight. Says what was not judged.
    PassedInPart(String),
    /// Not run, and so neither passed nor failed. Says why.
    Skipped(String),
    /// Something it asserts did not hold. Says what.
    Failed(String),
}

/// A scenario's outcome, with its known gap applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    Passed,
    PassedInPart(String),
    Skipped(String),
    Failed(String),
    /// Failed, as its known gap says it does; does not fail the run.
    ExpectedFailure {
        gap: String,
        why: String,
    },
    /// Passed although marked as a known gap: the flag should go. Fails the
    /// run, as it does in the SDKs' own runners.
    UnexpectedPass(String),
}

impl Verdict {
    /// `outcome`, with `gap` (the scenario's gap, if it applies) taken into
    /// account.
    pub fn of(outcome: Outcome, gap: Option<&str>) -> Verdict {
        match (gap, outcome) {
            (_, Outcome::Skipped(why)) => Verdict::Skipped(why),
            (Some(gap), Outcome::Passed | Outcome::PassedInPart(_)) => {
                Verdict::UnexpectedPass(gap.to_string())
            }
            (Some(gap), Outcome::Failed(why)) => Verdict::ExpectedFailure {
                gap: gap.to_string(),
                why,
            },
            (None, Outcome::Passed) => Verdict::Passed,
            (None, Outcome::PassedInPart(what)) => Verdict::PassedInPart(what),
            (None, Outcome::Failed(why)) => Verdict::Failed(why),
        }
    }
}

/// How a run went.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Summary {
    pub passed: usize,
    /// Of `passed`, the scenarios judged only in part.
    pub in_part: usize,
    pub failed: usize,
    pub expected_failures: usize,
    pub skipped: usize,
}

impl Summary {
    /// No scenario failed unexpectedly, and none passed unexpectedly.
    pub fn ok(&self) -> bool {
        self.failed == 0
    }

    /// Counts `verdict` and writes its line (and what it says, indented) to
    /// `out`.
    ///
    /// # Errors
    ///
    /// Writing to `out`.
    pub fn report(
        &mut self,
        scenario: &Scenario,
        verdict: &Verdict,
        out: &mut impl Write,
    ) -> io::Result<()> {
        let (tag, note) = match verdict {
            Verdict::Passed => {
                self.passed += 1;
                ("ok   ", None)
            }
            Verdict::PassedInPart(what) => {
                self.passed += 1;
                self.in_part += 1;
                ("ok   ", Some(format!("judged in part: {what}")))
            }
            Verdict::Skipped(why) => {
                self.skipped += 1;
                ("skip ", Some(why.clone()))
            }
            Verdict::Failed(why) => {
                self.failed += 1;
                ("FAIL ", Some(why.clone()))
            }
            Verdict::ExpectedFailure { gap, why } => {
                self.expected_failures += 1;
                ("xfail", Some(format!("known gap: {gap}\n{why}")))
            }
            Verdict::UnexpectedPass(gap) => {
                self.failed += 1;
                (
                    "FAIL ",
                    Some(format!(
                        "passed, but is marked known_gap: remove the flag ({gap})"
                    )),
                )
            }
        };
        writeln!(out, "{tag} {}  {}", scenario.id, scenario.label())?;
        if let Some(note) = note {
            writeln!(out, "        {}", note.replace('\n', "\n        "))?;
        }
        Ok(())
    }

    /// The closing line.
    ///
    /// # Errors
    ///
    /// Writing to `out`.
    pub fn finish(&self, total: usize, out: &mut impl Write) -> io::Result<()> {
        writeln!(
            out,
            "{total} scenarios: {} passed ({} judged in part), {} failed, {} expected failures, {} skipped",
            self.passed, self.in_part, self.failed, self.expected_failures, self.skipped
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_embedded_catalog_reads_and_every_kind_is_known() {
        let catalog = load(&Source::Embedded).expect("the embedded catalog");
        assert!(!catalog.scenarios.is_empty());
        for scenario in &catalog.scenarios {
            assert!(
                Kind::named(scenario.kind.as_deref()).is_some(),
                "{} has a kind this runner does not know",
                scenario.id
            );
        }
        let mut ids: Vec<_> = catalog.scenarios.iter().map(|s| &s.id).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), catalog.scenarios.len(), "scenario ids repeat");
    }

    #[test]
    fn a_structural_match_allows_extra_fields_but_not_other_values() {
        let expected = json!({"a": 1, "b": [true, {"c": "x"}]});
        assert!(json_matches(
            &expected,
            &json!({"a": 1, "b": [true, {"c": "x", "d": 2}], "e": null})
        ));
        assert!(!json_matches(
            &expected,
            &json!({"a": 2, "b": [true, {"c": "x"}]})
        ));
        assert!(!json_matches(&expected, &json!({"a": 1, "b": [true]})));
        assert!(!json_matches(&expected, &json!({"b": [true, {"c": "x"}]})));
        // A boolean never equals a number.
        assert!(!json_matches(&json!(false), &json!(0)));
    }

    #[test]
    fn an_exact_result_allows_nothing_extra() {
        let expect = Expect {
            result: json!({}),
            result_exact: true,
            ..Expect::default()
        };
        assert!(result_matches(&expect, &json!({})));
        assert!(!result_matches(&expect, &json!({"a": 1})));
        let loose = Expect {
            result: json!({}),
            ..Expect::default()
        };
        assert!(result_matches(&loose, &json!({"a": 1})));
    }

    #[test]
    fn bounds_need_the_field_and_a_number_under_them() {
        let bounds = HashMap::from([("activations".to_string(), 4.0)]);
        assert!(within_bounds(&bounds, &json!({"activations": 4})).is_ok());
        assert!(within_bounds(&bounds, &json!({"activations": 5})).is_err());
        assert!(within_bounds(&bounds, &json!({"activations": "4"})).is_err());
        assert!(within_bounds(&bounds, &json!({})).is_err());
        assert!(within_bounds(&bounds, &json!(4)).is_err());
    }

    #[test]
    fn elapsed_bounds_are_inclusive_and_enforced_at_both_ends() {
        let expect = Expect {
            min_elapsed_ms: Some(1_000),
            max_elapsed_ms: Some(15_000),
            ..Expect::default()
        };

        assert!(expect.check_elapsed_bounds(1_000).is_ok());
        assert!(expect.check_elapsed_bounds(15_000).is_ok());
        assert!(expect
            .check_elapsed_bounds(999)
            .unwrap_err()
            .contains("completed too fast"));
        assert!(expect
            .check_elapsed_bounds(15_001)
            .unwrap_err()
            .contains("took too long"));
    }

    fn gapped(sdks: Option<Vec<&str>>) -> Scenario {
        let mut scenario: Scenario =
            serde_json::from_value(json!({"id": "g", "known_gap": "not yet"})).unwrap();
        scenario.known_gap_sdks = sdks.map(|s| s.into_iter().map(String::from).collect());
        scenario
    }

    #[test]
    fn a_gap_applies_to_the_sdks_it_names() {
        assert_eq!(gapped(None).applicable_gap(None), Some("not yet"));
        assert_eq!(gapped(None).applicable_gap(Some("rust")), Some("not yet"));
        let scoped = gapped(Some(vec!["python"]));
        assert_eq!(scoped.applicable_gap(Some("python")), Some("not yet"));
        assert_eq!(scoped.applicable_gap(Some("rust")), None);
        assert_eq!(scoped.applicable_gap(None), None);
    }

    #[test]
    fn a_gap_turns_a_failure_expected_and_a_pass_into_a_failure() {
        assert!(matches!(
            Verdict::of(Outcome::Failed("x".into()), Some("gap")),
            Verdict::ExpectedFailure { .. }
        ));
        assert_eq!(
            Verdict::of(Outcome::Passed, Some("gap")),
            Verdict::UnexpectedPass("gap".into())
        );
        assert_eq!(
            Verdict::of(Outcome::Skipped("why".into()), Some("gap")),
            Verdict::Skipped("why".into())
        );

        let mut summary = Summary::default();
        let scenario = gapped(None);
        let mut out = Vec::new();
        for verdict in [
            Verdict::of(Outcome::Passed, None),
            Verdict::of(Outcome::Failed("x".into()), Some("gap")),
            Verdict::of(Outcome::PassedInPart("y".into()), None),
            Verdict::of(Outcome::Skipped("z".into()), None),
        ] {
            summary.report(&scenario, &verdict, &mut out).unwrap();
        }
        assert!(summary.ok(), "{summary:?}");
        summary
            .report(
                &scenario,
                &Verdict::of(Outcome::Passed, Some("gap")),
                &mut out,
            )
            .unwrap();
        assert!(!summary.ok());
        assert_eq!(
            summary,
            Summary {
                passed: 2,
                in_part: 1,
                failed: 1,
                expected_failures: 1,
                skipped: 1
            }
        );
    }

    #[test]
    fn scenarios_that_need_the_sdk_harness_are_named() {
        let catalog = load(&Source::Embedded).unwrap();
        let harness: Vec<_> = catalog
            .scenarios
            .iter()
            .filter(|s| s.harness_only().is_some())
            .map(|s| s.kind.as_deref().unwrap_or(""))
            .collect();
        assert!(harness
            .iter()
            .all(|k| *k == "cancel_queued_task" || *k == "worker_crash"));
        assert!(!harness.is_empty());
    }

    #[test]
    fn only_selects_known_ids_in_catalog_order() {
        let catalog = load(&Source::Embedded).unwrap();
        let first = catalog.scenarios[0].id.clone();
        let last = catalog.scenarios.last().unwrap().id.clone();
        let chosen = select(&catalog, &[last.clone(), first.clone()]).unwrap();
        assert_eq!(
            chosen.iter().map(|s| &s.id).collect::<Vec<_>>(),
            vec![&first, &last]
        );
        assert!(select(&catalog, &["no-such-scenario".into()]).is_err());
        assert_eq!(
            select(&catalog, &[]).unwrap().len(),
            catalog.scenarios.len()
        );
    }
}
