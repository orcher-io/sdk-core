//! The ORCHER benchmark suite: what a worker built on this checkout's core
//! and host, and the engine it talks to, spend on each workload.
//!
//! `bench/run.sh` starts an isolated engine and Postgres and runs `suite`;
//! see `bench/README.md`.
//!
//! ```text
//! orcher-bench suite --engine HOST:PORT --pg URL --label NAME --out DIR
//!                    [--only a,b] [--upstream mirror|shared] [--meta key=value]...
//! orcher-bench compare A.json B.json
//! orcher-bench list
//! orcher-bench worker SETUP_JSON        (run by the suite itself)
//! ```

mod pg;
mod program;
mod proxy;
mod report;
mod scenarios;
mod stats;
mod worker;

use std::collections::BTreeMap;
use std::path::PathBuf;

#[global_allocator]
static ALLOCATOR: stats::Counting = stats::Counting;

const USAGE: &str = "usage:
  orcher-bench suite --engine HOST:PORT --pg URL --label NAME --out DIR
                     [--only a,b] [--upstream mirror|shared] [--meta key=value]...
  orcher-bench compare A.json B.json
  orcher-bench list";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime starts");
    let outcome = match args.first().map(String::as_str) {
        Some("worker") => match args.get(1).map(|setup| serde_json::from_str(setup)) {
            Some(Ok(setup)) => {
                if std::env::var_os("RUST_LOG").is_some() {
                    tracing_subscriber::fmt()
                        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                        .with_writer(std::io::stderr)
                        .init();
                }
                runtime.block_on(worker::run(setup))
            }
            _ => Err("worker needs its setup as JSON".to_string()),
        },
        Some("suite") => runtime.block_on(suite(&args[1..])),
        Some("compare") => compare(&args[1..]),
        Some("list") => {
            for scenario in scenarios::all() {
                println!("{:<24} {}", scenario.name, scenario.about);
            }
            Ok(())
        }
        _ => Err(USAGE.to_string()),
    };
    if let Err(error) = outcome {
        eprintln!("orcher-bench: {error}");
        std::process::exit(1);
    }
    // Exits without dropping the runtime, which would wait for the blocking
    // thread still reading the worker's stdin.
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
    std::process::exit(0);
}

struct SuiteArgs {
    engine: String,
    pg: String,
    label: String,
    out: PathBuf,
    only: Option<Vec<String>>,
    upstream: proxy::Upstream,
    meta: BTreeMap<String, String>,
}

fn parse_suite(args: &[String]) -> Result<SuiteArgs, String> {
    let mut engine = None;
    let mut pg = None;
    let mut label = None;
    let mut out = None;
    let mut only = None;
    let mut upstream = proxy::Upstream::Mirror;
    let mut meta = BTreeMap::new();
    let mut args = args.iter();
    while let Some(flag) = args.next() {
        let mut value = || {
            args.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))
        };
        match flag.as_str() {
            "--engine" => engine = Some(value()?),
            "--pg" => pg = Some(value()?),
            "--label" => label = Some(value()?),
            "--out" => out = Some(PathBuf::from(value()?)),
            "--only" => {
                only = Some(
                    value()?
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect(),
                )
            }
            "--upstream" => {
                upstream = match value()?.as_str() {
                    "mirror" => proxy::Upstream::Mirror,
                    "shared" => proxy::Upstream::Shared,
                    other => return Err(format!("unknown upstream {other}: mirror or shared")),
                }
            }
            "--meta" => {
                let pair = value()?;
                let (key, value) = pair
                    .split_once('=')
                    .ok_or_else(|| format!("--meta takes key=value, not {pair}"))?;
                meta.insert(key.to_string(), value.to_string());
            }
            other => return Err(format!("unknown argument {other}\n{USAGE}")),
        }
    }
    Ok(SuiteArgs {
        engine: engine.ok_or("--engine is required")?,
        pg: pg.ok_or("--pg is required")?,
        label: label.ok_or("--label is required")?,
        out: out.ok_or("--out is required")?,
        only,
        upstream,
        meta,
    })
}

async fn suite(args: &[String]) -> Result<(), String> {
    let args = parse_suite(args)?;
    let all = scenarios::all();
    let chosen: Vec<_> = match &args.only {
        Some(names) => {
            for name in names {
                if !all.iter().any(|s| s.name == name) {
                    return Err(format!("no scenario named {name}; see `orcher-bench list`"));
                }
            }
            all.into_iter()
                .filter(|s| names.iter().any(|n| n == s.name))
                .collect()
        }
        None => all,
    };
    let bench = scenarios::Bench {
        engine: args.engine.clone(),
        pg: pg::Pg::connect(&args.pg).await?,
        upstream: args.upstream,
    };
    let mut results = report::Results {
        label: args.label.clone(),
        meta: args.meta.clone(),
        scenarios: Vec::new(),
    };
    results.meta.insert(
        "upstream".into(),
        format!("{:?}", args.upstream).to_lowercase(),
    );
    results
        .meta
        .insert("os".into(), std::env::consts::OS.to_string());
    results.meta.insert(
        "host_cpus".into(),
        std::thread::available_parallelism()
            .map(|n| n.get().to_string())
            .unwrap_or_default(),
    );
    std::fs::create_dir_all(&args.out).map_err(|e| e.to_string())?;
    let json_path = args.out.join(format!("{}.json", args.label));
    let md_path = args.out.join(format!("{}.md", args.label));

    for scenario in &chosen {
        eprintln!("orcher-bench: {} ...", scenario.name);
        let result = bench.run(scenario).await;
        let derived = report::derive(&result);
        eprintln!(
            "orcher-bench: {}: {} of {} runs completed{}",
            scenario.name,
            result.runs_completed,
            result.runs_started,
            result
                .error
                .as_ref()
                .map(|e| format!(" (error: {e})"))
                .unwrap_or_default()
        );
        results.scenarios.push(report::Scenario { derived, result });
        // Written after every scenario, so a long suite stopped half-way
        // keeps what it measured.
        let json = serde_json::to_string_pretty(&results).map_err(|e| e.to_string())?;
        std::fs::write(&json_path, json + "\n").map_err(|e| e.to_string())?;
        std::fs::write(&md_path, report::summary(&results)).map_err(|e| e.to_string())?;
    }
    eprintln!(
        "orcher-bench: wrote {} and {}",
        json_path.display(),
        md_path.display()
    );
    Ok(())
}

fn compare(args: &[String]) -> Result<(), String> {
    let [a, b] = args else {
        return Err(USAGE.to_string());
    };
    let read = |path: &String| -> Result<report::Results, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
        serde_json::from_str(&text).map_err(|e| format!("{path}: {e}"))
    };
    print!("{}", report::compare(&read(a)?, &read(b)?));
    Ok(())
}
