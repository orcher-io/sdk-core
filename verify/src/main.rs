//! `orcher-verify`: checks a workflow runtime in any language against the
//! kernel protocol's replay vectors. See `verify/README.md`.

use std::process::ExitCode;
use std::time::Duration;

use orcher_verify::judge::PayloadComparison;
use orcher_verify::reference::{self, Answers};
use orcher_verify::runner::{self, Options as VectorOptions};
use orcher_verify::scenarios;
use orcher_verify::vectors::{self, Files, Vector, SDKS};

const USAGE: &str = "\
Checks a workflow runtime in any language against the ORCHER kernel protocol.

Usage:
  orcher-verify vectors [options] --runtime <command> [<arg>...]
      Runs the runtime as a child process, hands it every replay vector's
      RunActivation on stdin and judges the RunActivationResult it answers
      on stdout. Each message is framed as a 4-byte big-endian length, then
      the encoded protobuf message. Exits 1 if any vector fails.
  orcher-verify list [options]
      Lists the vectors: name and workflow type.
  orcher-verify reference-runtime [options] [--answers <answers>]
      A runtime that answers every vector with its recorded answer, the one
      the runner's own tests run; <answers> other than `recorded`
      (drop-command, change-task-type, forget-history) answer wrong on
      purpose.

  orcher-verify scenarios --server-url <url> --task-queue <queue> [options]
      Runs the contract scenarios against an engine, through core's client,
      on a task queue your worker (serving the contract catalog) already
      serves, and judges each by what a client observes. Exits 1 if any
      scenario fails unexpectedly.

Options for vectors, list and reference-runtime:
  --sdk <sdk>         Only the vectors recorded from this SDK's contract suite
                      (rust, python, typescript); may be given more than once.
  --vectors <dir>     Read the vectors from this directory (with index.json)
                      instead of the ones built in.
  --timeout <secs>    How long to wait for each answer (default 30).
  vectors only:
    --payloads <mode> How payloads are compared: exact bytes (default);
                      json, as JSON values, to check a runtime against every
                      SDK's vectors (a payload that is not JSON is compared
                      as bytes); or ignore-bytes, not at all, only to narrow
                      down a failure.

Options for scenarios:
  --namespace <ns>          Namespace to run in, created if missing
                            (default contract).
  --sdk <sdk>               The SDK whose known gaps apply (known_gap_sdks).
  --only <id>[,<id>...]     Only these scenarios; may be given more than once.
  --parallel <n>            Run n scenarios at once (default 1).
  --result-timeout <secs>   How long to wait for a workflow to end (default 30).
  --api-key <key>           For an engine that authenticates callers
                            (default: ORCHER_API_KEY).
  --restart-command <cmd>   Restarts the engine, for the engine_restart
                            scenario (default: ORCHER_CONTRACT_RESTART_CMD);
                            skipped without one.
  --scenarios <file>        Read the catalog from this file instead of the
                            one built in.
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(code) => code,
        Err(message) => {
            eprintln!("orcher-verify: {message}\n\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

fn run(args: &[String]) -> Result<ExitCode, String> {
    let (command, mut rest) = args
        .split_first()
        .ok_or_else(|| "no command given".to_string())?;
    if matches!(command.as_str(), "-h" | "--help" | "help") {
        print!("{USAGE}");
        return Ok(ExitCode::SUCCESS);
    }
    if command == "scenarios" {
        return scenarios(rest);
    }

    let mut sdks: Vec<String> = Vec::new();
    let mut files = Files::Embedded;
    let mut vector_options = VectorOptions::default();
    let mut payload_comparison_was_set = false;
    let mut answers = Answers::Recorded;
    let mut runtime: Option<Vec<String>> = None;
    while let Some((flag, after)) = rest.split_first() {
        if flag == "--runtime" {
            runtime = Some(after.to_vec());
            break;
        }
        let (value, after) = after
            .split_first()
            .ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--sdk" if SDKS.contains(&value.as_str()) => sdks.push(value.clone()),
            "--sdk" => return Err(format!("no vectors are recorded from SDK {value:?}")),
            "--vectors" => files = Files::Dir(value.into()),
            "--timeout" => {
                let secs: u64 = value
                    .parse()
                    .map_err(|_| format!("--timeout takes whole seconds, not {value:?}"))?;
                vector_options.timeout = Duration::from_secs(secs);
            }
            "--payloads" => {
                vector_options.payload_comparison = match value.as_str() {
                    "exact" => PayloadComparison::Exact,
                    "json" => PayloadComparison::Json,
                    "ignore-bytes" => PayloadComparison::IgnoreBytes,
                    _ => {
                        return Err(format!(
                            "--payloads must be exact, json, or ignore-bytes, not {value:?}"
                        ))
                    }
                };
                payload_comparison_was_set = true;
            }
            "--answers" => {
                answers = Answers::named(value).ok_or_else(|| {
                    format!(
                        "--answers is one of {}, not {value:?}",
                        Answers::NAMES.join(", ")
                    )
                })?;
            }
            _ => return Err(format!("unknown option {flag:?}")),
        }
        rest = after;
    }
    if payload_comparison_was_set && command != "vectors" {
        return Err("--payloads is only valid with the vectors command".to_string());
    }

    let (index, all) =
        vectors::load(&files).map_err(|e| format!("cannot read the vectors: {e}"))?;
    let chosen: Vec<Vector> = all
        .into_iter()
        .filter(|vector| sdks.is_empty() || sdks.contains(&vector.sdk))
        .collect();

    match command.as_str() {
        "vectors" => {
            let runtime = runtime
                .filter(|runtime| !runtime.is_empty())
                .ok_or_else(|| "vectors needs --runtime <command>".to_string())?;
            let summary = runner::run(
                &runtime,
                &chosen,
                &index.namespace,
                vector_options,
                &mut std::io::stdout().lock(),
            )
            .map_err(|e| e.to_string())?;
            Ok(if summary.ok() {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
        "list" => {
            for vector in &chosen {
                println!("{}  {}", vector.name, vector.workflow_type);
            }
            Ok(ExitCode::SUCCESS)
        }
        "reference-runtime" => {
            reference::serve(
                &chosen,
                answers,
                std::io::stdin().lock(),
                std::io::stdout().lock(),
            )
            .map_err(|e| format!("reference runtime: {e}"))?;
            Ok(ExitCode::SUCCESS)
        }
        other => Err(format!("unknown command {other:?}")),
    }
}

/// `orcher-verify scenarios`.
fn scenarios(mut rest: &[String]) -> Result<ExitCode, String> {
    let mut server_url: Option<String> = None;
    let mut task_queue: Option<String> = None;
    let mut namespace = "contract".to_string();
    let mut sdk: Option<String> = None;
    let mut only: Vec<String> = Vec::new();
    let mut parallel = 1usize;
    let mut result_timeout = Duration::from_secs(30);
    let mut api_key = std::env::var("ORCHER_API_KEY")
        .ok()
        .filter(|k| !k.is_empty());
    let mut restart_command = std::env::var("ORCHER_CONTRACT_RESTART_CMD")
        .ok()
        .filter(|c| !c.is_empty());
    let mut source = scenarios::Source::Embedded;
    while let Some((flag, after)) = rest.split_first() {
        let (value, after) = after
            .split_first()
            .ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--server-url" => server_url = Some(value.clone()),
            "--task-queue" => task_queue = Some(value.clone()),
            "--namespace" => namespace = value.clone(),
            "--sdk" => sdk = Some(value.clone()),
            "--only" => only.extend(
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
                    .map(String::from),
            ),
            "--parallel" => {
                parallel =
                    value.parse().ok().filter(|n| *n > 0).ok_or_else(|| {
                        format!("--parallel takes a count above 0, not {value:?}")
                    })?;
            }
            "--result-timeout" => {
                let secs: u64 = value
                    .parse()
                    .map_err(|_| format!("--result-timeout takes whole seconds, not {value:?}"))?;
                result_timeout = Duration::from_secs(secs);
            }
            "--api-key" => api_key = Some(value.clone()),
            "--restart-command" => restart_command = Some(value.clone()),
            "--scenarios" => source = scenarios::Source::File(value.into()),
            _ => return Err(format!("unknown option {flag:?}")),
        }
        rest = after;
    }

    let options = scenarios::Options {
        server_url: server_url.ok_or("scenarios needs --server-url <url>")?,
        task_queue: task_queue.ok_or("scenarios needs --task-queue <queue>")?,
        namespace,
        sdk,
        api_key,
        result_timeout,
        parallel,
        restart_command,
    };
    let catalog = scenarios::load(&source)?;
    let chosen = scenarios::select(&catalog, &only)?;

    let runtime = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    let summary = runtime.block_on(scenarios::run(
        &chosen,
        &options,
        &mut std::io::stdout().lock(),
    ))?;
    Ok(if summary.ok() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}
