//! Runs a runtime against the vectors: starts it as a child process, hands
//! it each vector's `RunActivation` and judges the answer it reads back, in
//! the index's order, one at a time.

use std::io::{self, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use crate::framing::{read_frame, write_frame};
use crate::judge::judge;
use crate::vectors::Vector;

/// How long the runtime has to exit once its stdin is closed.
const EXIT_GRACE: Duration = Duration::from_secs(5);

/// How a run went.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Summary {
    /// Vectors answered with the recorded answer.
    pub passed: usize,
    /// Vectors answered otherwise, or not at all.
    pub failed: usize,
    /// What went wrong with the runtime itself once every vector was
    /// handed over: it did not exit, or exited with a failure.
    pub runtime: Option<String>,
}

impl Summary {
    /// Every vector passed and the runtime exited cleanly.
    pub fn ok(&self) -> bool {
        self.failed == 0 && self.runtime.is_none()
    }
}

/// Runs `runtime` (a program and its arguments) against `vectors`, judged in
/// `namespace`, waiting at most `timeout` for each answer, and writes a line
/// per vector and the summary to `out`.
///
/// # Errors
///
/// The error of a runtime that cannot be started, or of writing to `out`.
/// Everything the runtime does wrong is a failed vector, not an error.
pub fn run(
    runtime: &[String],
    vectors: &[Vector],
    namespace: &str,
    timeout: Duration,
    out: &mut impl Write,
) -> io::Result<Summary> {
    let (program, args) = runtime
        .split_first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no runtime command given"))?;
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| io::Error::new(e.kind(), format!("cannot start {program:?}: {e}")))?;
    let mut stdin = child.stdin.take().expect("the runtime's stdin is piped");
    let mut stdout = child.stdout.take().expect("the runtime's stdout is piped");

    // Read on a thread of its own, so an answer that never comes times out.
    let (frames, answers) = mpsc::channel();
    std::thread::spawn(move || loop {
        let frame = read_frame(&mut stdout);
        let last = !matches!(frame, Ok(Some(_)));
        if frames.send(frame).is_err() || last {
            break;
        }
    });

    let mut summary = Summary::default();
    // Why the runtime can answer nothing more, once it cannot.
    let mut gone: Option<String> = None;
    for vector in vectors {
        let problems = match &gone {
            Some(why) => vec![why.clone()],
            None => match exchange(&mut stdin, &answers, vector, timeout) {
                Ok(answer) => judge(vector, &answer, namespace),
                Err(why) => {
                    gone = Some(why.clone());
                    vec![why]
                }
            },
        };
        if problems.is_empty() {
            summary.passed += 1;
            writeln!(out, "ok    {}  {}", vector.name, vector.workflow_type)?;
        } else {
            summary.failed += 1;
            writeln!(out, "FAIL  {}  {}", vector.name, vector.workflow_type)?;
            for problem in &problems {
                writeln!(out, "        {}", problem.replace('\n', "\n        "))?;
            }
        }
    }

    // The runtime's input ends with the last vector; it should exit.
    drop(stdin);
    let exited = if gone.is_some() {
        let _ = child.kill();
        let _ = child.wait();
        None
    } else {
        exit(&mut child)
    };
    summary.runtime = match exited {
        Some(Ok(status)) if status.success() => None,
        Some(Ok(status)) => Some(format!(
            "the runtime exited with {status} once its input ended"
        )),
        Some(Err(why)) => Some(why),
        None => None,
    };
    if let Some(why) = &summary.runtime {
        writeln!(out, "FAIL  {why}")?;
    }
    writeln!(
        out,
        "{} vectors: {} passed, {} failed",
        vectors.len(),
        summary.passed,
        summary.failed
    )?;
    Ok(summary)
}

/// Hands the runtime `vector`'s activation and reads its answer; why it
/// cannot, if it cannot.
fn exchange(
    stdin: &mut impl Write,
    answers: &Receiver<io::Result<Option<Vec<u8>>>>,
    vector: &Vector,
    timeout: Duration,
) -> Result<Vec<u8>, String> {
    write_frame(stdin, &vector.run_activation)
        .map_err(|e| format!("the runtime does not take the activation: {e}"))?;
    match answers.recv_timeout(timeout) {
        Ok(Ok(Some(answer))) => Ok(answer),
        Ok(Ok(None)) | Err(RecvTimeoutError::Disconnected) => {
            Err("the runtime closed its stdout without answering".to_string())
        }
        Ok(Err(e)) => Err(format!("the runtime's answer cannot be read: {e}")),
        Err(RecvTimeoutError::Timeout) => {
            Err(format!("the runtime did not answer within {timeout:?}"))
        }
    }
}

/// Waits for the runtime to exit, killing it after [`EXIT_GRACE`].
fn exit(child: &mut Child) -> Option<Result<std::process::ExitStatus, String>> {
    let deadline = Instant::now() + EXIT_GRACE;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(Ok(status)),
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(20));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Some(Err(format!(
                    "the runtime did not exit within {EXIT_GRACE:?} of its input ending"
                )));
            }
            Err(e) => return Some(Err(format!("the runtime's exit cannot be read: {e}"))),
        }
    }
}
