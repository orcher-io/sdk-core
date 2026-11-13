//! The regression gate: a run of a few scenarios judged against a committed
//! baseline (`bench/gate.json`).
//!
//! It judges counts only — journal rows, Postgres blocks, RPCs and bytes per
//! activation — which repeat closely from run to run on one kind of machine.
//! Times and worker CPU vary too much with the machine's load to gate on.
//!
//! The baseline is the median of several runs, and each metric's threshold
//! follows from how far those runs spread: a regression is a change larger
//! than the runs themselves differ by. `gate-baseline` writes the file from
//! the runs, so a new baseline is made the same way every time.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::report::Results;

/// A threshold is this many times the spread the baseline runs showed...
const SPREAD_FACTOR: f64 = 3.0;
/// ...and never tighter than this, as a fraction of the baseline, so a
/// metric that happened not to vary is not failed by its first wobble.
const MIN_TOLERANCE: f64 = 0.02;

/// Which way a metric gets worse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Worse {
    /// A higher value is worse: rows, blocks, RPCs, bytes.
    Higher,
    /// A lower value is worse: a share that should stay high.
    Lower,
}

/// The gate: what it runs, what it judges, and against what.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Gate {
    /// How to read and remake this file.
    pub about: String,
    /// The engine image the baseline was measured against. A run against
    /// another one is judged against nothing: bump this with a new baseline.
    pub engine_version: String,
    /// The scenarios the gate runs.
    pub scenarios: Vec<String>,
    /// The metrics it judges, by name, with the direction that is worse.
    pub metrics: BTreeMap<String, Worse>,
    /// How the baseline was made: its runs, and when.
    pub measured: Measured,
    /// Scenario, then metric: the baseline and how much a run may differ.
    pub baseline: BTreeMap<String, BTreeMap<String, Bound>>,
}

/// How a baseline was made.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Measured {
    pub runs: Vec<String>,
    pub date: String,
    pub machine: String,
}

/// One metric of one scenario in the baseline.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Bound {
    /// The median of the baseline runs.
    pub median: f64,
    /// The lowest and highest the baseline runs gave.
    pub min: f64,
    pub max: f64,
    /// How far a run may be from the median, in the worse direction, as a
    /// fraction of the median, before it is a regression.
    pub tolerance: f64,
}

/// The gate made from `runs`: each metric's median, its spread, and the
/// tolerance that follows from it.
pub fn baseline(template: &Gate, runs: &[(String, Results)]) -> Result<Gate, String> {
    if runs.len() < 3 {
        return Err(format!(
            "a baseline needs at least 3 runs to measure their spread, not {}",
            runs.len()
        ));
    }
    for (path, run) in runs {
        unreliable(run, &template.scenarios).map_err(|why| format!("{path}: {why}"))?;
    }
    let mut baseline = BTreeMap::new();
    for scenario in &template.scenarios {
        let mut bounds = BTreeMap::new();
        for metric in template.metrics.keys() {
            let mut values: Vec<f64> = runs
                .iter()
                .filter_map(|(_, run)| value(run, scenario, metric))
                .collect();
            if values.len() != runs.len() {
                return Err(format!(
                    "{scenario}: {metric} is missing from {} of the runs",
                    runs.len() - values.len()
                ));
            }
            values.sort_by(f64::total_cmp);
            let median = values[values.len() / 2];
            let (min, max) = (values[0], values[values.len() - 1]);
            let spread = if median == 0.0 {
                0.0
            } else {
                ((max - median).abs().max((median - min).abs())) / median.abs()
            };
            bounds.insert(
                metric.clone(),
                Bound {
                    median,
                    min,
                    max,
                    tolerance: (spread * SPREAD_FACTOR).max(MIN_TOLERANCE),
                },
            );
        }
        baseline.insert(scenario.clone(), bounds);
    }
    let first = &runs[0].1;
    Ok(Gate {
        measured: Measured {
            runs: runs
                .iter()
                .map(|(_, run)| {
                    format!(
                        "{} (sdk-core {})",
                        run.label,
                        run.meta
                            .get("sdk_core_revision")
                            .map_or("?", String::as_str)
                    )
                })
                .collect(),
            date: first.meta.get("date").cloned().unwrap_or_default(),
            machine: format!(
                "{} CPUs, {} bytes of memory for Docker, {}",
                first.meta.get("docker_cpus").map_or("?", String::as_str),
                first
                    .meta
                    .get("docker_memory_bytes")
                    .map_or("?", String::as_str),
                first.meta.get("os").map_or("?", String::as_str),
            ),
        },
        baseline,
        ..template.clone()
    })
}

/// What judging a run found.
#[derive(Debug, Default)]
pub struct Verdict {
    pub report: String,
    pub regressions: usize,
}

/// `run` judged against `gate`.
pub fn judge(gate: &Gate, run: &Results) -> Result<Verdict, String> {
    let engine = run.meta.get("engine_image").cloned().unwrap_or_default();
    if !engine.ends_with(&format!(":{}", gate.engine_version)) {
        return Err(format!(
            "the run was against {engine}, the baseline against engine {}: \
             compare like with like, or make a new baseline",
            gate.engine_version
        ));
    }
    unreliable(run, &gate.scenarios)?;
    let mut verdict = Verdict::default();
    let mut rows = String::new();
    let mut better = Vec::new();
    for (scenario, bounds) in &gate.baseline {
        for (metric, bound) in bounds {
            let worse = gate.metrics.get(metric).copied().unwrap_or(Worse::Higher);
            let Some(now) = value(run, scenario, metric) else {
                verdict.regressions += 1;
                rows.push_str(&format!(
                    "| {scenario} | {metric} | {} | missing | | REGRESSION |\n",
                    bound.median
                ));
                continue;
            };
            let change = if bound.median == 0.0 {
                if now == 0.0 {
                    0.0
                } else {
                    f64::INFINITY
                }
            } else {
                (now - bound.median) / bound.median.abs()
            };
            let worse_by = match worse {
                Worse::Higher => change,
                Worse::Lower => -change,
            };
            let status = if worse_by > bound.tolerance {
                verdict.regressions += 1;
                "REGRESSION"
            } else if -worse_by > bound.tolerance {
                better.push(format!("{scenario} {metric}"));
                "better"
            } else {
                "ok"
            };
            rows.push_str(&format!(
                "| {scenario} | {metric} | {:.2} | {now:.2} | {:+.1}% (±{:.1}%) | {status} |\n",
                bound.median,
                change * 100.0,
                bound.tolerance * 100.0
            ));
        }
    }
    verdict.report = format!(
        "# Regression gate: {} against the baseline of {}\n\n\
         | scenario | metric | baseline | now | change (allowed) | |\n\
         |---|---|---:|---:|---:|---|\n{rows}",
        run.label, gate.measured.date
    );
    if verdict.regressions > 0 {
        verdict.report.push_str(&format!(
            "\n{} regressed beyond what the baseline runs differed by.\n",
            verdict.regressions
        ));
    }
    if !better.is_empty() {
        verdict.report.push_str(&format!(
            "\nBetter than the baseline by more than its spread: {}. When that is \
             meant, make a new baseline so the gain is kept.\n",
            better.join(", ")
        ));
    }
    Ok(verdict)
}

/// Why `run`'s numbers cannot be judged, if they cannot: a scenario missing,
/// a run that did not end, or RPCs that failed, any of which changes the
/// counts for reasons that are not the code's.
fn unreliable(run: &Results, scenarios: &[String]) -> Result<(), String> {
    for name in scenarios {
        let Some(scenario) = run.scenarios.iter().find(|s| &s.result.name == name) else {
            return Err(format!("{name} is not in the run"));
        };
        if let Some(error) = &scenario.result.error {
            return Err(format!("{name} could not be run: {error}"));
        }
        if scenario.result.success_rate < 1.0 {
            return Err(format!(
                "{name}: {} of {} runs completed, so its counts are not comparable",
                scenario.result.runs_completed, scenario.result.runs_started
            ));
        }
        let errors = scenario.derived.get("rpc_errors").copied().unwrap_or(0.0);
        if errors > 0.0 {
            return Err(format!(
                "{name}: {errors} RPCs failed, so its counts are not comparable"
            ));
        }
    }
    Ok(())
}

fn value(run: &Results, scenario: &str, metric: &str) -> Option<f64> {
    run.scenarios
        .iter()
        .find(|s| s.result.name == scenario)?
        .derived
        .get(metric)
        .copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::Scenario;

    fn run(label: &str, rows: f64, share: f64) -> Results {
        let mut scenario = Scenario::default();
        scenario.result.name = "s".into();
        scenario.result.success_rate = 1.0;
        scenario.result.runs_started = 1;
        scenario.result.runs_completed = 1;
        scenario
            .derived
            .insert("journal_rows_per_activation".into(), rows);
        scenario
            .derived
            .insert("partial_activation_share".into(), share);
        Results {
            label: label.into(),
            meta: [(
                "engine_image".to_string(),
                "ghcr.io/orcher-io/orcher:0.7.0".to_string(),
            )]
            .into(),
            scenarios: vec![scenario],
        }
    }

    fn template() -> Gate {
        Gate {
            about: String::new(),
            engine_version: "0.7.0".into(),
            scenarios: vec!["s".into()],
            metrics: [
                ("journal_rows_per_activation".to_string(), Worse::Higher),
                ("partial_activation_share".to_string(), Worse::Lower),
            ]
            .into(),
            measured: Measured {
                runs: vec![],
                date: String::new(),
                machine: String::new(),
            },
            baseline: BTreeMap::new(),
        }
    }

    fn gate() -> Gate {
        let runs: Vec<(String, Results)> = [(100.0, 0.99), (104.0, 0.99), (98.0, 0.99)]
            .iter()
            .enumerate()
            .map(|(i, (rows, share))| (format!("r{i}"), run("b", *rows, *share)))
            .collect();
        baseline(&template(), &runs).unwrap()
    }

    /// The median and spread of the runs set the bound.
    #[test]
    fn a_baseline_is_the_median_and_its_spread() {
        let bound = gate().baseline["s"]["journal_rows_per_activation"];
        assert_eq!(bound.median, 100.0);
        assert_eq!((bound.min, bound.max), (98.0, 104.0));
        assert!(
            (bound.tolerance - 0.12).abs() < 1e-9,
            "3 × 4%: {}",
            bound.tolerance
        );
        // A metric that did not vary still gets the minimum tolerance.
        assert_eq!(
            gate().baseline["s"]["partial_activation_share"].tolerance,
            MIN_TOLERANCE
        );
    }

    /// Within the spread passes; beyond it in the worse direction fails, in
    /// either direction a metric can get worse.
    #[test]
    fn a_change_beyond_the_spread_in_the_worse_direction_fails() {
        let gate = gate();
        assert_eq!(
            judge(&gate, &run("ok", 110.0, 0.99)).unwrap().regressions,
            0
        );
        assert_eq!(
            judge(&gate, &run("more rows", 113.0, 0.99))
                .unwrap()
                .regressions,
            1
        );
        assert_eq!(
            judge(&gate, &run("fewer hits", 100.0, 0.90))
                .unwrap()
                .regressions,
            1
        );
        let better = judge(&gate, &run("better", 80.0, 0.99)).unwrap();
        assert_eq!(better.regressions, 0);
        assert!(better.report.contains("make a new baseline"));
    }

    /// A run whose counts cannot be compared is not judged at all.
    #[test]
    fn an_unreliable_run_is_not_judged() {
        let gate = gate();
        let mut failed = run("failed", 100.0, 0.99);
        failed.scenarios[0].result.success_rate = 0.5;
        assert!(judge(&gate, &failed).is_err());
        let mut other_engine = run("other", 100.0, 0.99);
        other_engine.meta.insert(
            "engine_image".into(),
            "ghcr.io/orcher-io/orcher:0.7.1".into(),
        );
        assert!(judge(&gate, &other_engine).is_err());
        assert!(baseline(&template(), &[("only".into(), run("x", 1.0, 1.0))]).is_err());
    }
}
