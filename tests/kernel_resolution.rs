//! The kernel resolves recorded activations to exactly what the SDKs
//! computed from them before it existed.
//!
//! Before the kernel, each SDK took the jobs core derived from the journal
//! (`task_to_execution_request`) and read times from the journal beside them
//! (`JournalTimes`, identical in the Python and TypeScript bridges). For every
//! recorded activation, the kernel's resolutions must carry the same outcomes,
//! decoded the same way, at the same times. `sdk_journal_times` below is that
//! bridge code, kept as the reference.
//!
//! Runs from `tests/fixtures/replay`, plus `ORCHER_REPLAY_CORPUS` when set.

use std::collections::HashMap;
use std::path::Path;

use orcher_sdk_core::kernel::resolve;
use orcher_sdk_core::proto::kernel::v1::{outcome, resolution, End, Resolution, RunActivation};
use orcher_sdk_core::proto::orcher::v1::{journal_entry::Attributes, EntryType, JournalEntry};
use orcher_sdk_core::proto::prost_types::Timestamp;
use orcher_sdk_core::worker::poll::polling::WorkflowExecutionTask;
use orcher_sdk_core::worker::record::{read_runs, Recorded};
use orcher_sdk_core::{task_to_execution_request, RequestJob};

#[test]
fn the_kernel_resolves_the_committed_recordings_as_the_sdks_did() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/replay");
    let checked: usize = ["rust", "python", "typescript"]
        .iter()
        .map(|sdk| check_all(&fixtures.join(sdk)))
        .sum();
    assert!(checked > 0, "no activations checked");
}

#[test]
fn the_kernel_resolves_the_given_corpus_as_the_sdks_did() {
    let Some(dir) = std::env::var_os("ORCHER_REPLAY_CORPUS").filter(|dir| !dir.is_empty()) else {
        eprintln!("ORCHER_REPLAY_CORPUS is not set: no outside corpus checked");
        return;
    };
    for sdk in ["rust", "python", "typescript"] {
        let dir = Path::new(&dir).join(sdk);
        if dir.is_dir() {
            check_all(&dir);
        }
    }
}

/// Checks every activation recorded under `dir`; returns how many.
fn check_all(dir: &Path) -> usize {
    let mut checked = 0;
    let mut problems = Vec::new();
    for run in read_runs(dir).expect("read the recorded runs") {
        for message in &run.messages {
            let Recorded::Activation(response) = message else {
                continue;
            };
            checked += 1;
            let task = WorkflowExecutionTask::from(response.clone());
            if let Err(problem) = check(&task) {
                problems.push(format!(
                    "{} {}/{}: {problem}",
                    task.workflow_type, run.workflow_id, run.run_id
                ));
            }
        }
    }
    eprintln!(
        "{}: {checked} activations resolved as before",
        dir.display()
    );
    assert!(problems.is_empty(), "{}", problems.join("\n"));
    checked
}

fn check(task: &WorkflowExecutionTask) -> Result<(), String> {
    let activation = resolve(task, "contract");
    let jobs = task_to_execution_request(task.clone())
        .map_err(|e| format!("today's conversion failed: {e}"))?
        .jobs;
    let times = sdk_journal_times(&task.journal);
    let resolutions = &activation.resolutions;

    // The run starts first, at the journaled start, with its input.
    let started = match resolutions.first().and_then(|r| r.what.as_ref()) {
        Some(resolution::What::Started(started)) => started,
        other => return Err(format!("first resolution is not the start: {other:?}")),
    };
    if ms(&resolutions[0].at) != times.started_at_ms {
        return Err("start time differs".into());
    }
    let input: serde_json::Value = serde_json::from_slice(&started.input[0].data)
        .map_err(|e| format!("input is not JSON: {e}"))?;
    if input != task.input {
        return Err(format!("input differs: {input} vs {}", task.input));
    }

    // Observed in recorded order.
    let ats: Vec<Option<i64>> = resolutions.iter().map(|r| ms(&r.at)).collect();
    if ats
        .windows(2)
        .any(|w| matches!((w[0], w[1]), (Some(a), Some(b)) if a > b))
    {
        return Err(format!("not in time order: {ats:?}"));
    }

    let mut steps = by_kind(&activation, |w| matches!(w, resolution::What::Step(_)));
    let mut timers = by_kind(&activation, |w| matches!(w, resolution::What::Timer(_)));
    let mut children = by_kind(&activation, |w| matches!(w, resolution::What::Child(_)));
    let mut events = by_kind(&activation, |w| matches!(w, resolution::What::Event(_)));
    let mut cancels = by_kind(&activation, |w| matches!(w, resolution::What::Cancel(_)));
    let mut event_times: HashMap<String, std::vec::IntoIter<i64>> = times
        .events
        .clone()
        .into_iter()
        .map(|(name, at)| (name, at.into_iter()))
        .collect();

    for job in &jobs {
        match job {
            RequestJob::StartWorkflow(_) | RequestJob::ProcessQuery(_) => {}
            RequestJob::UpdateState(update) => {
                let pending = activation.resolutions.iter().any(|r| {
                    matches!(&r.what, Some(resolution::What::Update(u)) if u.update_id == update.update_id)
                        && r.at.is_none()
                });
                if !pending {
                    return Err(format!("update {} is not delivered", update.update_id));
                }
            }
            RequestJob::CompleteStep(step) => {
                let r = take(&mut steps, "step")?;
                same(&r.step_id, &step.step_name, "step id")?;
                same(
                    &ms(&r.at),
                    &times.resolved_at.get(&step.step_name).copied(),
                    "step time",
                )?;
                let Some(resolution::What::Step(settled)) = &r.what else {
                    unreachable!()
                };
                let outcome = settled.outcome.as_ref().ok_or("step without outcome")?;
                same(
                    &(outcome.attempts as i32),
                    &step.execution_attempt.max(0),
                    "attempts",
                )?;
                match (&step.failure, &outcome.result) {
                    (Some(f), Some(outcome::Result::Failure(k))) => {
                        same(&k.message, &f.message, "failure message")?;
                        same(&k.error_type, &f.failure_type, "failure type")?;
                        same(&outcome.end, &(End::Failed as i32), "end")?;
                    }
                    (None, Some(outcome::Result::Value(v))) => {
                        same(&v.data, &step.result, "result")?
                    }
                    other => {
                        return Err(format!(
                            "step {} outcome differs: {other:?}",
                            step.step_name
                        ))
                    }
                }
            }
            RequestJob::FireTimer(timer) => {
                let r = take(&mut timers, "timer")?;
                same(&r.step_id, &timer.timer_id, "timer id")?;
                same(
                    &ms(&r.at),
                    &times
                        .resolved_at
                        .get(&format!("timer:{}", timer.timer_id))
                        .copied(),
                    "timer time",
                )?;
            }
            RequestJob::HandleEvent(event) => {
                let r = take(&mut events, "event")?;
                let Some(resolution::What::Event(arrived)) = &r.what else {
                    unreachable!()
                };
                same(&arrived.name, &event.event_name, "event name")?;
                same(
                    &arrived.payload.as_ref().map(|p| p.data.clone()),
                    &Some(event.payload.data.clone()),
                    "event payload",
                )?;
                same(&arrived.event_id, &event.event_id, "event id")?;
                let expected = event_times
                    .get_mut(&event.event_name)
                    .and_then(Iterator::next);
                same(&ms(&r.at), &expected, "event time")?;
            }
            RequestJob::CancelWorkflow(_) => {
                let r = take(&mut cancels, "cancel")?;
                same(&ms(&r.at), &times.cancel_requested_at, "cancel time")?;
            }
            RequestJob::ChildWorkflowCompleted(c) => {
                child(&mut children, &c.workflow_id, End::Completed, &times)?
            }
            RequestJob::ChildWorkflowFailed(c) => {
                child(&mut children, &c.workflow_id, End::Failed, &times)?
            }
            RequestJob::ChildWorkflowCanceled(c) => {
                child(&mut children, &c.workflow_id, End::Cancelled, &times)?
            }
            RequestJob::ChildWorkflowTerminated(c) => {
                child(&mut children, &c.workflow_id, End::Terminated, &times)?
            }
            RequestJob::ChildWorkflowTimedOut(c) => {
                child(&mut children, &c.workflow_id, End::TimedOut, &times)?
            }
            other => return Err(format!("a job the kernel does not resolve yet: {other:?}")),
        }
    }
    // Every child the journal records as started, in journal order, is
    // resolved as started, under the id it was started with.
    let started_in_journal: Vec<&str> = task
        .journal
        .iter()
        .filter_map(|entry| match entry.attributes.as_ref()? {
            Attributes::ChildWorkflowExecutionStarted(child) => Some(child.workflow_id.as_str()),
            _ => None,
        })
        .collect();
    let started_resolved: Vec<&str> = resolutions
        .iter()
        .filter_map(|r| match r.what.as_ref()? {
            resolution::What::ChildStarted(child) => {
                (r.step_id == child.child_workflow_id).then_some(child.child_workflow_id.as_str())
            }
            _ => None,
        })
        .collect();
    if started_resolved != started_in_journal {
        return Err(format!(
            "children started: resolved {started_resolved:?}, journal {started_in_journal:?}"
        ));
    }

    for (kind, left) in [
        ("step", steps),
        ("timer", timers),
        ("child", children),
        ("event", events),
        ("cancel", cancels),
    ] {
        if !left.is_empty() {
            return Err(format!("{} {kind} resolution(s) with no job", left.len()));
        }
    }
    Ok(())
}

fn child(
    children: &mut std::collections::VecDeque<Resolution>,
    workflow_id: &str,
    end: End,
    times: &SdkJournalTimes,
) -> Result<(), String> {
    let r = take(children, "child")?;
    same(&r.step_id, &workflow_id.to_string(), "child id")?;
    same(
        &ms(&r.at),
        &times
            .resolved_at
            .get(&format!("child:{workflow_id}"))
            .copied(),
        "child time",
    )?;
    let Some(resolution::What::Child(settled)) = &r.what else {
        unreachable!()
    };
    same(
        &settled.outcome.as_ref().map(|o| o.end),
        &Some(end as i32),
        "child end",
    )
}

fn by_kind(
    activation: &RunActivation,
    kind: impl Fn(&resolution::What) -> bool,
) -> std::collections::VecDeque<Resolution> {
    // Jobs are in journal order; compare against the kernel's in the same order.
    let mut matching: Vec<(i64, Resolution)> = Vec::new();
    for (position, r) in activation.resolutions.iter().enumerate() {
        if r.what.as_ref().is_some_and(&kind) {
            matching.push((position as i64, r.clone()));
        }
    }
    matching.into_iter().map(|(_, r)| r).collect()
}

fn take(
    queue: &mut std::collections::VecDeque<Resolution>,
    kind: &str,
) -> Result<Resolution, String> {
    queue
        .pop_front()
        .ok_or_else(|| format!("a {kind} job with no resolution"))
}

fn same<T: PartialEq + std::fmt::Debug>(kernel: &T, before: &T, what: &str) -> Result<(), String> {
    if kernel == before {
        Ok(())
    } else {
        Err(format!(
            "{what} differs: kernel {kernel:?}, before {before:?}"
        ))
    }
}

fn ms(at: &Option<Timestamp>) -> Option<i64> {
    at.as_ref().map(|t| {
        t.seconds
            .saturating_mul(1000)
            .saturating_add(i64::from(t.nanos) / 1_000_000)
    })
}

/// The SDK bridges' `JournalTimes` (sdk-py and sdk-ts `native/src/journal_times.rs`),
/// unchanged but for its name: the reference the kernel's times are checked against.
#[derive(Debug, Default)]
struct SdkJournalTimes {
    started_at_ms: Option<i64>,
    resolved_at: HashMap<String, i64>,
    events: HashMap<String, Vec<i64>>,
    cancel_requested_at: Option<i64>,
}

fn sdk_journal_times(journal: &[JournalEntry]) -> SdkJournalTimes {
    let mut times = SdkJournalTimes::default();
    for entry in journal {
        let Some(at_ms) = entry.timestamp.as_ref().map(|t| {
            t.seconds
                .saturating_mul(1000)
                .saturating_add(i64::from(t.nanos) / 1_000_000)
        }) else {
            continue;
        };
        if entry.entry_type == EntryType::WorkflowExecutionStarted as i32 {
            times.started_at_ms.get_or_insert(at_ms);
        }
        if entry.entry_type == EntryType::WorkflowExecutionCancelRequested as i32 {
            times.cancel_requested_at.get_or_insert(at_ms);
        }
        let key = match &entry.attributes {
            Some(Attributes::StepCompleted(a)) => a.step_name.clone(),
            Some(Attributes::TimerFired(a)) => format!("timer:{}", a.timer_id),
            Some(Attributes::ChildWorkflowExecutionCompleted(a)) => {
                format!("child:{}", a.workflow_id)
            }
            Some(Attributes::ChildWorkflowExecutionFailed(a)) => format!("child:{}", a.workflow_id),
            Some(Attributes::ChildWorkflowExecutionCanceled(a)) => {
                format!("child:{}", a.workflow_id)
            }
            Some(Attributes::ChildWorkflowExecutionTerminated(a)) => {
                format!("child:{}", a.workflow_id)
            }
            Some(Attributes::ChildWorkflowExecutionTimedOut(a)) => {
                format!("child:{}", a.workflow_id)
            }
            Some(Attributes::EventReceived(a)) => {
                times
                    .events
                    .entry(a.event_name.clone())
                    .or_default()
                    .push(at_ms);
                continue;
            }
            _ => continue,
        };
        times.resolved_at.entry(key).or_insert(at_ms);
    }
    times
}
