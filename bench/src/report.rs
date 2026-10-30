//! The suite's results: the JSON file, its markdown summary, and the
//! comparison of two results.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::scenarios::ScenarioResult;

/// One run of the suite.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Results {
    pub label: String,
    /// Where it ran and on what: the engine image and revision, this
    /// checkout's revision, the machine.
    pub meta: BTreeMap<String, String>,
    pub scenarios: Vec<Scenario>,
}

/// A scenario's measurements, and the numbers derived from them that are
/// compared.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Scenario {
    /// The headline numbers, each named for what it counts and per what.
    pub derived: BTreeMap<String, f64>,
    #[serde(flatten)]
    pub result: ScenarioResult,
}

fn ratio(a: u64, b: u64) -> f64 {
    if b == 0 {
        0.0
    } else {
        a as f64 / b as f64
    }
}

/// The numbers compared across runs, from a scenario's measurements.
pub fn derive(result: &ScenarioResult) -> BTreeMap<String, f64> {
    let mut d = BTreeMap::new();
    let busy = &result.busy;
    let worker = &busy.worker;
    let rpc = &busy.rpc;
    let pg = &result.pg;
    let activations = worker.activations;
    let tasks = worker.tasks;
    let journal = pg.journal_reads();
    let writes = pg.writes();
    let polls = rpc.method("PollWorkflowExecution");
    let heartbeats = rpc.method("RecordTaskHeartbeat");

    d.insert("success_rate".into(), result.success_rate);
    d.insert("activations".into(), activations as f64);
    d.insert("tasks".into(), tasks as f64);
    d.insert("rpc_calls".into(), rpc.total_calls() as f64);
    d.insert(
        "rpc_per_activation".into(),
        ratio(rpc.total_calls(), activations),
    );
    d.insert("rpc_per_task".into(), ratio(rpc.total_calls(), tasks));
    d.insert(
        "rpc_errors".into(),
        rpc.methods.values().map(|m| m.errors).sum::<u64>() as f64,
    );
    d.insert(
        "empty_polls".into(),
        rpc.methods
            .iter()
            .filter(|(name, _)| name.starts_with("Poll"))
            .map(|(_, m)| m.calls - m.with_work)
            .sum::<u64>() as f64,
    );
    d.insert(
        "worker_connections".into(),
        rpc.peak_open_connections as f64,
    );
    d.insert("pg_rows".into(), pg.total.rows as f64);
    d.insert("pg_blocks".into(), pg.total.blocks() as f64);
    d.insert("pg_calls".into(), pg.total.calls as f64);
    d.insert("pg_wal_bytes".into(), pg.total.wal_bytes as f64);
    d.insert("pg_write_rows".into(), writes.rows as f64);
    d.insert("journal_rows_read".into(), journal.rows as f64);
    d.insert("journal_blocks".into(), journal.blocks() as f64);
    d.insert(
        "journal_rows_per_activation".into(),
        ratio(journal.rows, activations),
    );
    d.insert(
        "pg_blocks_per_activation".into(),
        ratio(pg.total.blocks(), activations),
    );
    d.insert(
        "pg_wal_bytes_per_activation".into(),
        ratio(pg.total.wal_bytes, activations),
    );
    d.insert(
        "activation_bytes".into(),
        ratio(polls.response_bytes, polls.with_work),
    );
    d.insert(
        "journal_entries_per_activation".into(),
        ratio(polls.journal_entries, polls.with_work),
    );
    d.insert(
        "worker_cpu_us_per_activation".into(),
        ratio(worker.cpu_us, activations),
    );
    d.insert(
        "worker_allocs_per_activation".into(),
        ratio(worker.allocations, activations),
    );
    d.insert(
        "worker_alloc_bytes_per_activation".into(),
        ratio(worker.allocated_bytes, activations),
    );
    d.insert("worker_cpu_us_per_task".into(), ratio(worker.cpu_us, tasks));
    d.insert(
        "worker_allocs_per_task".into(),
        ratio(worker.allocations, tasks),
    );
    d.insert(
        "worker_peak_heap_bytes".into(),
        worker.peak_live_heap_bytes as f64,
    );
    d.insert(
        "worker_peak_tokio_tasks".into(),
        worker.peak_tokio_alive_tasks as f64,
    );
    d.insert(
        "claimed_waiting_peak_tasks".into(),
        result.claimed_waiting.peak_tasks as f64,
    );
    d.insert(
        "claimed_waiting_peak_activations".into(),
        result.claimed_waiting.peak_activations as f64,
    );
    d.insert("heartbeat_rpcs".into(), heartbeats.calls as f64);
    if busy.secs > 0.0 {
        d.insert(
            "heartbeat_rpcs_per_s".into(),
            heartbeats.calls as f64 / busy.secs,
        );
    }
    if heartbeats.calls > 0 {
        let task_writes: u64 = pg
            .classes
            .iter()
            .filter(|(class, _)| class.as_str() == "update task_executions")
            .map(|(_, s)| s.calls)
            .sum();
        d.insert(
            "task_updates_per_heartbeat".into(),
            ratio(task_writes, heartbeats.calls),
        );
    }
    if let Some(idle) = &result.idle {
        if idle.secs > 0.0 {
            d.insert(
                "idle_rpc_per_s".into(),
                idle.rpc.total_calls() as f64 / idle.secs,
            );
            d.insert(
                "idle_cpu_us_per_s".into(),
                idle.worker.cpu_us as f64 / idle.secs,
            );
        }
        d.insert("idle_heap_bytes".into(), idle.worker.live_heap_bytes as f64);
        d.insert(
            "idle_tokio_tasks".into(),
            idle.worker.tokio_alive_tasks as f64,
        );
    }
    if let Some(t) = &result.throughput {
        d.insert("runs_per_s".into(), t.runs_per_s);
        d.insert("tasks_per_s".into(), t.tasks_per_s);
        d.insert("end_to_end_p50_ms".into(), t.end_to_end.p50_ms);
        d.insert("end_to_end_p99_ms".into(), t.end_to_end.p99_ms);
        d.insert(
            "schedule_to_start_p50_ms".into(),
            t.schedule_to_start.p50_ms,
        );
        d.insert(
            "schedule_to_start_p99_ms".into(),
            t.schedule_to_start.p99_ms,
        );
    }
    d.retain(|_, v| v.is_finite());
    d
}

/// The columns of the summary, per scenario.
const SUMMARY: &[(&str, &str)] = &[
    ("success_rate", "success"),
    ("activations", "activations"),
    ("tasks", "tasks"),
    ("rpc_per_activation", "RPCs/activation"),
    ("journal_rows_read", "journal rows read"),
    ("journal_rows_per_activation", "journal rows/activation"),
    ("pg_blocks_per_activation", "PG blocks/activation"),
    ("activation_bytes", "bytes/activation"),
    ("worker_cpu_us_per_activation", "worker CPU µs/activation"),
    ("worker_allocs_per_activation", "allocs/activation"),
];

const TASK_SUMMARY: &[(&str, &str)] = &[
    ("success_rate", "success"),
    ("tasks", "tasks"),
    ("rpc_per_task", "RPCs/task"),
    ("heartbeat_rpcs_per_s", "heartbeat RPCs/s"),
    ("task_updates_per_heartbeat", "task row updates/heartbeat"),
    ("claimed_waiting_peak_tasks", "peak claimed-not-started"),
    ("worker_cpu_us_per_task", "worker CPU µs/task"),
    ("worker_allocs_per_task", "allocs/task"),
    ("worker_peak_heap_bytes", "peak heap bytes"),
    ("worker_peak_tokio_tasks", "peak tokio tasks"),
];

/// The markdown summary of one run of the suite.
pub fn summary(results: &Results) -> String {
    let mut out = format!("# Benchmark: {}\n\n", results.label);
    for (key, value) in &results.meta {
        out.push_str(&format!("- {key}: `{value}`\n"));
    }
    let workflow: Vec<&Scenario> = results
        .scenarios
        .iter()
        .filter(|s| s.result.busy.worker.activations > 0 && s.result.throughput.is_none())
        .collect();
    let sustained: Vec<&Scenario> = results
        .scenarios
        .iter()
        .filter(|s| s.result.throughput.is_some())
        .collect();
    let task_only: Vec<&Scenario> = results
        .scenarios
        .iter()
        .filter(|s| s.result.busy.worker.activations == 0)
        .collect();
    out.push_str("\n## Workflow workloads\n\n");
    out.push_str(&table(&workflow, SUMMARY));
    if !sustained.is_empty() {
        out.push_str("\n## Sustained load\n\n");
        out.push_str(&table(
            &sustained,
            &[
                ("success_rate", "success"),
                ("runs_per_s", "runs/s"),
                ("tasks_per_s", "tasks/s"),
                ("end_to_end_p50_ms", "end-to-end p50 ms"),
                ("end_to_end_p99_ms", "end-to-end p99 ms"),
                ("schedule_to_start_p50_ms", "schedule-to-start p50 ms"),
                ("schedule_to_start_p99_ms", "schedule-to-start p99 ms"),
                ("pg_blocks_per_activation", "PG blocks/activation"),
            ],
        ));
    }
    out.push_str("\n## Task-only, idle and heartbeat workloads\n\n");
    out.push_str(&table(&task_only, TASK_SUMMARY));
    let idle: Vec<&Scenario> = results
        .scenarios
        .iter()
        .filter(|s| s.result.idle.is_some())
        .collect();
    if !idle.is_empty() {
        out.push_str("\n## Idle\n\n");
        out.push_str(&table(
            &idle,
            &[
                ("idle_rpc_per_s", "RPCs/s"),
                ("idle_cpu_us_per_s", "CPU µs/s"),
                ("idle_heap_bytes", "heap bytes"),
                ("idle_tokio_tasks", "tokio tasks"),
                ("worker_connections", "connections"),
            ],
        ));
    }
    out.push_str("\n## Postgres by statement class (rows / blocks touched)\n\n");
    for scenario in &results.scenarios {
        let mut classes: Vec<_> = scenario.result.pg.classes.iter().collect();
        classes.sort_by_key(|(_, s)| std::cmp::Reverse(s.blocks()));
        let top: Vec<String> = classes
            .iter()
            .take(4)
            .map(|(class, s)| format!("{class} {} / {}", s.rows, s.blocks()))
            .collect();
        out.push_str(&format!(
            "- **{}**: {}\n",
            scenario.result.name,
            top.join("; ")
        ));
    }
    let failed: Vec<_> = results
        .scenarios
        .iter()
        .filter(|s| s.result.error.is_some() || s.result.success_rate < 1.0)
        .collect();
    if !failed.is_empty() {
        out.push_str("\n## Not clean\n\n");
        for s in failed {
            let r = &s.result;
            out.push_str(&format!(
                "- **{}**: {} of {} runs completed, {} failed, {} unfinished{}\n",
                r.name,
                r.runs_completed,
                r.runs_started,
                r.runs_failed,
                r.runs_unfinished,
                r.error
                    .as_ref()
                    .map(|e| format!("; error: {e}"))
                    .unwrap_or_default()
            ));
        }
    }
    out
}

fn table(scenarios: &[&Scenario], columns: &[(&str, &str)]) -> String {
    if scenarios.is_empty() {
        return "(none)\n".to_string();
    }
    let mut out = String::from("| scenario |");
    for (_, title) in columns {
        out.push_str(&format!(" {title} |"));
    }
    out.push_str("\n|---|");
    for _ in columns {
        out.push_str("---:|");
    }
    out.push('\n');
    for scenario in scenarios {
        out.push_str(&format!("| {} |", scenario.result.name));
        for (key, _) in columns {
            let value = scenario.derived.get(*key).copied();
            out.push_str(&format!(
                " {} |",
                value.map(format_number).unwrap_or_default()
            ));
        }
        out.push('\n');
    }
    out
}

/// A number as the summary shows it.
pub fn format_number(value: f64) -> String {
    if value == value.trunc() && value.abs() < 1e15 {
        let digits = format!("{}", value as i64);
        group(&digits)
    } else if value.abs() >= 100.0 {
        group(&format!("{}", value.round() as i64))
    } else if value.abs() >= 1.0 {
        format!("{value:.2}")
    } else {
        format!("{value:.3}")
    }
}

fn group(digits: &str) -> String {
    let (sign, digits) = match digits.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", digits),
    };
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    format!("{sign}{out}")
}

/// The differences between two runs of the suite, scenario by scenario.
pub fn compare(a: &Results, b: &Results) -> String {
    let mut out = format!("# {} → {}\n", a.label, b.label);
    for scenario in &b.scenarios {
        let Some(before) = a
            .scenarios
            .iter()
            .find(|s| s.result.name == scenario.result.name)
        else {
            out.push_str(&format!(
                "\n## {} (only in {})\n",
                scenario.result.name, b.label
            ));
            continue;
        };
        out.push_str(&format!(
            "\n## {}\n\n| metric | {} | {} | change |\n|---|---:|---:|---:|\n",
            scenario.result.name, a.label, b.label
        ));
        let mut keys: Vec<&String> = before.derived.keys().collect();
        for key in scenario.derived.keys() {
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        for key in keys {
            let x = before.derived.get(key).copied();
            let y = scenario.derived.get(key).copied();
            let change = match (x, y) {
                (Some(x), Some(y)) if x != 0.0 => format!("{:+.1}%", (y - x) / x * 100.0),
                (Some(x), Some(y)) if x == y => "=".to_string(),
                (Some(_), Some(_)) => "new".to_string(),
                _ => "-".to_string(),
            };
            out.push_str(&format!(
                "| {key} | {} | {} | {change} |\n",
                x.map(format_number).unwrap_or_else(|| "-".into()),
                y.map(format_number).unwrap_or_else(|| "-".into()),
            ));
        }
        if scenario.result.success_rate < 1.0 || before.result.success_rate < 1.0 {
            out.push_str(
                "\nNot every run succeeded on one side: read the per-activation numbers with that in mind.\n",
            );
        }
    }
    for scenario in &a.scenarios {
        if !b
            .scenarios
            .iter()
            .any(|s| s.result.name == scenario.result.name)
        {
            out.push_str(&format!(
                "\n## {} (only in {})\n",
                scenario.result.name, a.label
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::format_number;

    #[test]
    fn numbers_are_grouped() {
        assert_eq!(format_number(1_004_002.0), "1,004,002");
        assert_eq!(format_number(0.5), "0.500");
        assert_eq!(format_number(12.345), "12.35");
        assert_eq!(format_number(1234.6), "1,235");
    }
}
