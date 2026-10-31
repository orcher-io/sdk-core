//! `orcher-verify`: checks a workflow runtime in any language against the
//! kernel protocol's replay vectors. See `verify/README.md`.

use std::process::ExitCode;
use std::time::Duration;

use orcher_verify::reference::{self, Answers};
use orcher_verify::runner;
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

Options:
  --sdk <sdk>         Only the vectors recorded from this SDK's contract suite
                      (rust, python, typescript); may be given more than once.
  --vectors <dir>     Read the vectors from this directory (with index.json)
                      instead of the ones built in.
  --timeout <secs>    How long to wait for each answer (default 30).
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

    let mut sdks: Vec<String> = Vec::new();
    let mut files = Files::Embedded;
    let mut timeout = Duration::from_secs(30);
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
                timeout = Duration::from_secs(secs);
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
                timeout,
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
