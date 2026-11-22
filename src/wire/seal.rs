//! Sealing the payloads in a worker's messages to the engine, and opening
//! those in the engine's messages to it.
//!
//! Every payload field a worker exchanges with the engine is named here and
//! nowhere else: a call site seals the message it is about to send, or opens
//! the one it just received, with one of these functions. What a function
//! does not name (ids, tokens, headers, failure messages, timeouts) crosses
//! as it is.
//!
//! Without a seal the sealing functions do nothing, and the opening ones
//! only refuse a payload that arrives sealed.

use orcher_proto::orcher::v1::{
    command, journal_entry, query_result, update_result, ActorOperation, Command,
    CompleteActorOperationRequest, CompleteTaskExecutionRequest, CompleteWorkflowExecutionRequest,
    FailTaskExecutionRequest, FailWorkflowExecutionRequest, Failure, GetStateResponse,
    JournalEntry, PollTaskExecutionResponse, PollWorkflowExecutionResponse,
    RecordTaskHeartbeatRequest, RecordTaskHeartbeatsRequest, SetStateRequest,
};

use crate::error::Result;
use crate::payload::seal::{open, seal};
use crate::payload::PayloadSeal;

/// The failure type a task attempt is failed with, and the error code an
/// actor operation is answered with, when this worker cannot open what it
/// was sent, so a retry policy can name it.
pub(crate) const PAYLOAD_UNREADABLE: &str = "PayloadCodecError";

/// Opens what a workflow activation carries: the run's input, the arguments
/// of its pending queries and updates, and every journal entry.
pub(crate) fn open_activation(
    with: Option<&PayloadSeal>,
    activation: &mut PollWorkflowExecutionResponse,
) -> Result<()> {
    open(with, &mut activation.input)?;
    for query in &mut activation.queries {
        open(with, &mut query.query_args)?;
    }
    for update in &mut activation.updates {
        open(with, &mut update.args)?;
    }
    for entry in &mut activation.journal {
        open_journal_entry(with, entry)?;
    }
    Ok(())
}

/// Opens the payloads one journal entry records.
///
/// Every kind of entry is named, the ones that record no payload too, so a
/// kind added to the protocol does not compile until it is placed here.
fn open_journal_entry(with: Option<&PayloadSeal>, entry: &mut JournalEntry) -> Result<()> {
    use journal_entry::Attributes as A;
    let Some(attributes) = entry.attributes.as_mut() else {
        return Ok(());
    };
    match attributes {
        A::WorkflowExecutionStarted(a) => open(with, &mut a.input),
        A::WorkflowExecutionCompleted(a) => open(with, &mut a.result),
        A::WorkflowExecutionFailed(a) => open_failure(with, a.failure.as_mut()),
        A::WorkflowExecutionCanceled(a) => open(with, &mut a.details),
        A::WorkflowExecutionRestartedFresh(a) => open(with, &mut a.input),
        A::WorkflowExecutionTerminated(a) => open(with, &mut a.details),
        A::ExecutionStepFailed(a) => open_failure(with, a.failure.as_mut()),
        A::TaskScheduled(a) => open(with, &mut a.input),
        A::TaskCompleted(a) => open(with, &mut a.result),
        A::TaskFailed(a) => open_failure(with, a.failure.as_mut()),
        A::TaskCanceled(a) => open(with, &mut a.details),
        A::EventReceived(a) => open(with, &mut a.payload),
        A::StepCompleted(a) => {
            open(with, &mut a.result)?;
            open_failure(with, a.failure.as_mut())
        }
        A::UpdateAccepted(a) => open(with, &mut a.args),
        A::UpdateCompleted(a) => {
            open(with, &mut a.result)?;
            open_failure(with, a.failure.as_mut())
        }
        A::ChildWorkflowExecutionCompleted(a) => open(with, &mut a.result),
        A::ChildWorkflowExecutionFailed(a) => open_failure(with, a.failure.as_mut()),
        A::ChildWorkflowExecutionCanceled(a) => open(with, &mut a.details),
        A::ChildWorkflowExecutionTerminated(a) => open(with, &mut a.details),
        // These record no payload.
        A::WorkflowExecutionTimedOut(_)
        | A::WorkflowExecutionCancelRequested(_)
        | A::ExecutionStepScheduled(_)
        | A::ExecutionStepStarted(_)
        | A::ExecutionStepCompleted(_)
        | A::ExecutionStepTimedOut(_)
        | A::TaskStarted(_)
        | A::TaskTimedOut(_)
        | A::TaskCancelRequested(_)
        | A::TimerStarted(_)
        | A::TimerFired(_)
        | A::TimerCanceled(_)
        | A::ChildWorkflowExecutionStarted(_)
        | A::ChildWorkflowExecutionTimedOut(_) => Ok(()),
    }
}

/// Opens a task's input.
pub(crate) fn open_task(
    with: Option<&PayloadSeal>,
    task: &mut PollTaskExecutionResponse,
) -> Result<()> {
    open(with, &mut task.input)
}

/// Opens an actor operation's payload.
pub(crate) fn open_actor_operation(
    with: Option<&PayloadSeal>,
    operation: &mut ActorOperation,
) -> Result<()> {
    open(with, &mut operation.payload)
}

/// Opens an actor state value that was read.
pub(crate) fn open_state(with: Option<&PayloadSeal>, state: &mut GetStateResponse) -> Result<()> {
    open(with, &mut state.value)
}

/// Seals what a workflow completion carries: every command's payload, and
/// the answers to the activation's queries and updates.
pub(crate) fn seal_workflow_completion(
    with: Option<&PayloadSeal>,
    completion: &mut CompleteWorkflowExecutionRequest,
) -> Result<()> {
    if with.is_none() {
        return Ok(());
    }
    for command in &mut completion.commands {
        seal_command(with, command)?;
    }
    for result in &mut completion.query_results {
        if let Some(query_result::Result::Answer(answer)) = result.result.as_mut() {
            seal(with, answer)?;
        }
    }
    for result in &mut completion.update_results {
        if let Some(update_result::Result::Answer(answer)) = result.result.as_mut() {
            seal(with, answer)?;
        }
    }
    Ok(())
}

/// Seals the payload one command carries.
///
/// Every kind of command is named, the ones that carry no payload too, so a
/// kind added to the protocol does not compile until it is placed here.
fn seal_command(with: Option<&PayloadSeal>, command: &mut Command) -> Result<()> {
    use command::Attributes as A;
    let Some(attributes) = command.attributes.as_mut() else {
        return Ok(());
    };
    match attributes {
        A::ScheduleTask(a) => seal(with, &mut a.input),
        A::CompleteWorkflow(a) => seal(with, &mut a.result),
        A::FailWorkflow(a) => seal_failure(with, a.failure.as_mut()),
        A::CancelWorkflow(a) => seal(with, &mut a.details),
        A::RestartFresh(a) => seal(with, &mut a.input),
        A::StartChildWorkflow(a) => seal(with, &mut a.input),
        A::SendEvent(a) => seal(with, &mut a.payload),
        A::QueryChildWorkflow(a) => seal(with, &mut a.query_args),
        A::RecordStepResult(a) => {
            seal(with, &mut a.result)?;
            seal_failure(with, a.failure.as_mut())
        }
        // These carry no payload.
        A::StartTimer(_)
        | A::CancelTimer(_)
        | A::RequestCancelTask(_)
        | A::CancelChildWorkflow(_)
        | A::RequestCancellation(_)
        | A::UpsertLabels(_) => Ok(()),
    }
}

/// Seals the details of a workflow activation's failure.
pub(crate) fn seal_workflow_failure(
    with: Option<&PayloadSeal>,
    failure: &mut FailWorkflowExecutionRequest,
) -> Result<()> {
    seal_failure(with, failure.failure.as_mut())
}

/// Seals a task's result.
pub(crate) fn seal_task_completion(
    with: Option<&PayloadSeal>,
    completion: &mut CompleteTaskExecutionRequest,
) -> Result<()> {
    seal(with, &mut completion.result)
}

/// Seals the details of a task's failure.
pub(crate) fn seal_task_failure(
    with: Option<&PayloadSeal>,
    failure: &mut FailTaskExecutionRequest,
) -> Result<()> {
    seal_failure(with, failure.failure.as_mut())
}

/// Seals the progress details of a task heartbeat sent on its own.
pub(crate) fn seal_heartbeat(
    with: Option<&PayloadSeal>,
    heartbeat: &mut RecordTaskHeartbeatRequest,
) -> Result<()> {
    seal(with, &mut heartbeat.details)
}

/// Seals the progress details of every heartbeat in a batch.
pub(crate) fn seal_heartbeats(
    with: Option<&PayloadSeal>,
    heartbeats: &mut RecordTaskHeartbeatsRequest,
) -> Result<()> {
    for heartbeat in &mut heartbeats.heartbeats {
        seal(with, &mut heartbeat.details)?;
    }
    Ok(())
}

/// Seals an actor operation's result.
pub(crate) fn seal_actor_completion(
    with: Option<&PayloadSeal>,
    completion: &mut CompleteActorOperationRequest,
) -> Result<()> {
    seal(with, &mut completion.result)
}

/// Seals an actor state value to be written.
pub(crate) fn seal_state(with: Option<&PayloadSeal>, state: &mut SetStateRequest) -> Result<()> {
    seal(with, &mut state.value)
}

/// Seals the details of `failure` and of every failure in its chain of
/// causes. Messages, types and stack traces are sent as they are.
fn seal_failure(with: Option<&PayloadSeal>, failure: Option<&mut Failure>) -> Result<()> {
    let mut next = failure;
    while let Some(failure) = next {
        seal(with, &mut failure.details)?;
        next = failure.cause.as_deref_mut();
    }
    Ok(())
}

/// Opens the details of `failure` and of every failure in its chain of
/// causes.
fn open_failure(with: Option<&PayloadSeal>, failure: Option<&mut Failure>) -> Result<()> {
    let mut next = failure;
    while let Some(failure) = next {
        open(with, &mut failure.details)?;
        next = failure.cause.as_deref_mut();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fmt::Debug;

    use orcher_proto::orcher::v1::*;
    use prost::Message;

    use super::*;
    use crate::error::Error;
    use crate::payload::codec::{CodecChain, EncryptionPayloadCodec, PayloadCodec};
    use crate::payload::Payload;

    /// Part of every plain payload the fixtures carry, and of nothing else
    /// in them.
    const MARK: &str = "plain-marker-7f3a";

    /// A plain payload, different for every place `at` it is put.
    fn plain(at: &str) -> Vec<u8> {
        format!(r#"{{"secret":"{MARK}","at":"{at}"}}"#).into_bytes()
    }

    /// What a fixture fills its payload fields with, given where each is.
    type Fill<'a> = &'a dyn Fn(&str) -> Vec<u8>;

    fn shows_mark(message: &impl Message) -> bool {
        message
            .encode_to_vec()
            .windows(MARK.len())
            .any(|window| window == MARK.as_bytes())
    }

    fn encrypting(key: u8) -> PayloadSeal {
        PayloadSeal::new(
            CodecChain::new().with_codec(EncryptionPayloadCodec::new(&[key; 32]).with_key_id("k1")),
        )
    }

    /// Writes the bytes back to front: the same output for the same input,
    /// so a sealed message can be compared with the one expected.
    struct Reversing;

    impl PayloadCodec for Reversing {
        fn encode(&self, payload: &Payload) -> Result<Payload> {
            let mut encoded = Payload::new(
                payload.data.iter().rev().copied().collect(),
                payload.metadata.clone(),
            );
            encoded.set_metadata_string("encoding", "binary/reversed");
            Ok(encoded)
        }

        fn decode(&self, payload: &Payload) -> Result<Payload> {
            Ok(Payload::new(
                payload.data.iter().rev().copied().collect(),
                payload.metadata.clone(),
            ))
        }
    }

    fn reversing() -> PayloadSeal {
        PayloadSeal::new(CodecChain::new().with_codec(Reversing))
    }

    /// Fails to encode anything.
    struct Failing;

    impl PayloadCodec for Failing {
        fn encode(&self, _: &Payload) -> Result<Payload> {
            Err(Error::codec("the key service is unreachable"))
        }

        fn decode(&self, payload: &Payload) -> Result<Payload> {
            Ok(payload.clone())
        }
    }

    /// Looks like a payload and is not one: headers cross as they are.
    fn headers() -> HashMap<String, Vec<u8>> {
        HashMap::from([("trace".to_string(), br#"{"kept":"as it is"}"#.to_vec())])
    }

    fn failure(fill: Fill, at: &str) -> Failure {
        Failure {
            message: format!("failed at {at}"),
            source: "TaskDriver".into(),
            stack_trace: "at charge (charge.rs:12)".into(),
            failure_type: "Declined".into(),
            details: fill(at),
            non_retryable: true,
            cause: Some(Box::new(Failure {
                message: "the root cause".into(),
                details: fill(&format!("{at}/cause")),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    /// An activation with a payload in every place one can arrive, a journal
    /// entry of every kind that records one, an entry written before any
    /// seal was configured, and entries that record none.
    fn activation(fill: Fill) -> PollWorkflowExecutionResponse {
        use journal_entry::Attributes as A;
        let events = vec![
            A::WorkflowExecutionStarted(WorkflowExecutionStartedEventAttributes {
                workflow_type: "checkout".into(),
                input: fill("started"),
                ..Default::default()
            }),
            A::WorkflowExecutionCompleted(WorkflowExecutionCompletedEventAttributes {
                result: fill("completed"),
                ..Default::default()
            }),
            A::WorkflowExecutionFailed(WorkflowExecutionFailedEventAttributes {
                failure: Some(failure(fill, "failed")),
                ..Default::default()
            }),
            A::WorkflowExecutionCanceled(WorkflowExecutionCanceledEventAttributes {
                details: fill("canceled"),
                ..Default::default()
            }),
            A::WorkflowExecutionRestartedFresh(WorkflowExecutionRestartedFreshEventAttributes {
                input: fill("restarted"),
                ..Default::default()
            }),
            A::WorkflowExecutionTerminated(WorkflowExecutionTerminatedEventAttributes {
                reason: "operator".into(),
                details: fill("terminated"),
                ..Default::default()
            }),
            A::ExecutionStepFailed(ExecutionStepFailedEventAttributes {
                failure: Some(failure(fill, "execution-step-failed")),
                ..Default::default()
            }),
            A::TaskScheduled(TaskScheduledEventAttributes {
                task_id: "charge_0".into(),
                input: fill("task-scheduled"),
                headers: headers(),
                ..Default::default()
            }),
            A::TaskStarted(TaskStartedEventAttributes {
                identity: "worker-1".into(),
                ..Default::default()
            }),
            A::TaskCompleted(TaskCompletedEventAttributes {
                result: fill("task-completed"),
                identity: "worker-1".into(),
                ..Default::default()
            }),
            A::TaskFailed(TaskFailedEventAttributes {
                failure: Some(failure(fill, "task-failed")),
                ..Default::default()
            }),
            A::TaskCanceled(TaskCanceledEventAttributes {
                details: fill("task-canceled"),
                ..Default::default()
            }),
            A::TimerFired(TimerFiredEventAttributes::default()),
            A::EventReceived(EventReceivedEventAttributes {
                event_name: "approved".into(),
                payload: fill("event"),
                ..Default::default()
            }),
            // Written before a seal was configured: plain whatever `fill` is.
            A::EventReceived(EventReceivedEventAttributes {
                event_name: "approved-earlier".into(),
                payload: br#"{"written":"before the seal"}"#.to_vec(),
                ..Default::default()
            }),
            A::StepCompleted(StepCompletedEventAttributes {
                step_name: "price".into(),
                result: fill("step"),
                failure: Some(failure(fill, "step-failure")),
                ..Default::default()
            }),
            A::UpdateAccepted(UpdateAcceptedEventAttributes {
                update_id: "u1".into(),
                args: fill("update-accepted"),
                ..Default::default()
            }),
            A::UpdateCompleted(UpdateCompletedEventAttributes {
                update_id: "u1".into(),
                result: fill("update-completed"),
                failure: Some(failure(fill, "update-failure")),
                ..Default::default()
            }),
            A::ChildWorkflowExecutionCompleted(ChildWorkflowExecutionCompletedEventAttributes {
                workflow_id: "child-1".into(),
                result: fill("child-completed"),
                ..Default::default()
            }),
            A::ChildWorkflowExecutionFailed(ChildWorkflowExecutionFailedEventAttributes {
                workflow_id: "child-1".into(),
                failure: Some(failure(fill, "child-failed")),
                ..Default::default()
            }),
            A::ChildWorkflowExecutionCanceled(ChildWorkflowExecutionCanceledEventAttributes {
                workflow_id: "child-1".into(),
                details: fill("child-canceled"),
                ..Default::default()
            }),
            A::ChildWorkflowExecutionTerminated(ChildWorkflowExecutionTerminatedEventAttributes {
                workflow_id: "child-1".into(),
                details: fill("child-terminated"),
                ..Default::default()
            }),
        ];
        PollWorkflowExecutionResponse {
            workflow_id: "wf-1".into(),
            execution_id: "run-1".into(),
            workflow_type: "checkout".into(),
            stream_entry_id: "act1.token".into(),
            input: fill("input"),
            queries: vec![QueryRequest {
                query_id: "q1".into(),
                query_type: "status".into(),
                query_args: fill("query"),
                ..Default::default()
            }],
            updates: vec![UpdateRequest {
                update_id: "u1".into(),
                update_type: "set".into(),
                args: fill("update"),
                ..Default::default()
            }],
            journal: events
                .into_iter()
                .enumerate()
                .map(|(i, attributes)| JournalEntry {
                    entry_id: i as i64 + 1,
                    attributes: Some(attributes),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn task(fill: Fill) -> PollTaskExecutionResponse {
        PollTaskExecutionResponse {
            task_token: b"task-token".to_vec(),
            workflow_id: "wf-1".into(),
            task_id: "charge_0".into(),
            task_type: "charge".into(),
            input: fill("task-input"),
            headers: headers(),
            ..Default::default()
        }
    }

    fn actor_operation(fill: Fill) -> ActorOperation {
        ActorOperation {
            operation_id: "op-1".into(),
            actor_name: "Cart".into(),
            key: "user-1".into(),
            operation: "add".into(),
            payload: fill("operation"),
            metadata: HashMap::from([("trace".to_string(), "kept".to_string())]),
            ..Default::default()
        }
    }

    fn state_read(fill: Fill) -> GetStateResponse {
        GetStateResponse {
            value: fill("state"),
            exists: true,
            version: "7".into(),
            ..Default::default()
        }
    }

    /// A completion with a command of every kind that carries a payload,
    /// commands that carry none, and answers of both kinds.
    fn workflow_completion(fill: Fill) -> CompleteWorkflowExecutionRequest {
        use command::Attributes as A;
        let commands = vec![
            A::ScheduleTask(ScheduleTaskCommandAttributes {
                task_id: "charge_0".into(),
                task_type: "charge".into(),
                input: fill("schedule"),
                headers: headers(),
                ..Default::default()
            }),
            A::StartTimer(StartTimerCommandAttributes::default()),
            A::CompleteWorkflow(CompleteWorkflowCommandAttributes {
                result: fill("complete"),
                ..Default::default()
            }),
            A::FailWorkflow(FailWorkflowCommandAttributes {
                failure: Some(failure(fill, "fail")),
                ..Default::default()
            }),
            A::CancelWorkflow(CancelWorkflowCommandAttributes {
                details: fill("cancel"),
                ..Default::default()
            }),
            A::RestartFresh(RestartFreshCommandAttributes {
                workflow_type: "checkout".into(),
                input: fill("restart"),
                ..Default::default()
            }),
            A::StartChildWorkflow(StartChildWorkflowCommandAttributes {
                workflow_id: "child-1".into(),
                input: fill("child"),
                ..Default::default()
            }),
            A::SendEvent(SendEventCommandAttributes {
                workflow_id: "other".into(),
                event_name: "approved".into(),
                payload: fill("event"),
                headers: headers(),
                ..Default::default()
            }),
            A::QueryChildWorkflow(QueryChildWorkflowCommandAttributes {
                workflow_id: "child-1".into(),
                query_type: "status".into(),
                query_args: fill("query-child"),
                ..Default::default()
            }),
            A::RecordStepResult(RecordStepResultCommandAttributes {
                step_name: "price".into(),
                result: fill("step"),
                failure: Some(failure(fill, "step-failure")),
                ..Default::default()
            }),
            A::UpsertLabels(UpsertLabelsCommandAttributes {
                labels: HashMap::from([("tier".to_string(), "gold".to_string())]),
                ..Default::default()
            }),
        ];
        CompleteWorkflowExecutionRequest {
            workflow_id: "wf-1".into(),
            execution_id: "run-1".into(),
            task_token: b"act1.token".to_vec(),
            stream_entry_id: "act1.token".into(),
            binary_checksum: b"checksum".to_vec(),
            commands: commands
                .into_iter()
                .map(|attributes| Command {
                    attributes: Some(attributes),
                    ..Default::default()
                })
                .collect(),
            query_results: vec![
                QueryResult {
                    query_id: "q1".into(),
                    result: Some(query_result::Result::Answer(fill("query-answer"))),
                    ..Default::default()
                },
                QueryResult {
                    query_id: "q2".into(),
                    result: Some(query_result::Result::ErrorMessage("no such query".into())),
                    ..Default::default()
                },
            ],
            update_results: vec![
                UpdateResult {
                    update_id: "u1".into(),
                    result: Some(update_result::Result::Answer(fill("update-answer"))),
                    ..Default::default()
                },
                UpdateResult {
                    update_id: "u2".into(),
                    result: Some(update_result::Result::ErrorMessage("rejected".into())),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    fn workflow_failure(fill: Fill) -> FailWorkflowExecutionRequest {
        FailWorkflowExecutionRequest {
            workflow_id: "wf-1".into(),
            task_token: b"act1.token".to_vec(),
            binary_checksum: b"checksum".to_vec(),
            failure: Some(failure(fill, "activation-failure")),
            ..Default::default()
        }
    }

    fn task_completion(fill: Fill) -> CompleteTaskExecutionRequest {
        CompleteTaskExecutionRequest {
            task_token: b"task-token".to_vec(),
            identity: "worker-1".into(),
            result: fill("task-result"),
            ..Default::default()
        }
    }

    fn task_failure(fill: Fill) -> FailTaskExecutionRequest {
        FailTaskExecutionRequest {
            task_token: b"task-token".to_vec(),
            identity: "worker-1".into(),
            failure: Some(failure(fill, "task-failure")),
            ..Default::default()
        }
    }

    fn heartbeat(fill: Fill) -> RecordTaskHeartbeatRequest {
        RecordTaskHeartbeatRequest {
            task_token: b"task-token".to_vec(),
            identity: "worker-1".into(),
            details: fill("progress"),
            ..Default::default()
        }
    }

    fn heartbeats(fill: Fill) -> RecordTaskHeartbeatsRequest {
        RecordTaskHeartbeatsRequest {
            identity: "worker-1".into(),
            heartbeats: vec![
                TaskHeartbeatEntry {
                    task_token: b"task-token-1".to_vec(),
                    details: fill("progress-1"),
                    ..Default::default()
                },
                // No details: nothing to seal.
                TaskHeartbeatEntry {
                    task_token: b"task-token-2".to_vec(),
                    ..Default::default()
                },
                TaskHeartbeatEntry {
                    task_token: b"task-token-3".to_vec(),
                    details: fill("progress-3"),
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    fn actor_completion(fill: Fill) -> CompleteActorOperationRequest {
        CompleteActorOperationRequest {
            service_id: "worker-1".into(),
            operation_id: "op-1".into(),
            execution_id: "exec-1".into(),
            result: fill("operation-result"),
            ..Default::default()
        }
    }

    fn state_write(fill: Fill) -> SetStateRequest {
        SetStateRequest {
            actor_name: "Cart".into(),
            key: "user-1".into(),
            state_key: "items".into(),
            value: fill("state"),
            expected_version: "7".into(),
            ..Default::default()
        }
    }

    /// `seal` hides every payload of the message `build` makes, seals each
    /// exactly once, changes nothing else, and does nothing without a seal.
    fn seals<M>(build: fn(Fill) -> M, seal: fn(Option<&PayloadSeal>, &mut M) -> Result<()>)
    where
        M: Message + Clone + PartialEq + Debug,
    {
        let message = build(&plain);
        assert!(shows_mark(&message), "the fixture carries no payload");

        let mut encrypted = message.clone();
        seal(Some(&encrypting(1)), &mut encrypted).unwrap();
        assert!(!shows_mark(&encrypted), "a payload was sent plain");

        // With a seal that always writes the same bytes, the sealed message
        // is the one built with every payload sealed once, field for field.
        let reversing = reversing();
        let mut sealed = message.clone();
        seal(Some(&reversing), &mut sealed).unwrap();
        assert_eq!(sealed, build(&|at| reversing.seal(&plain(at)).unwrap()));

        let mut untouched = message.clone();
        seal(None, &mut untouched).unwrap();
        assert_eq!(untouched.encode_to_vec(), message.encode_to_vec());

        let failing = PayloadSeal::new(CodecChain::new().with_codec(Failing));
        let error = seal(Some(&failing), &mut message.clone()).unwrap_err();
        assert!(error.to_string().contains("unreachable"), "{error}");
    }

    /// `open` restores every payload of the message `build` makes, changes
    /// nothing else, refuses a key that does not fit, and without a seal
    /// passes a plain message and refuses a sealed one.
    fn opens<M>(build: fn(Fill) -> M, open: fn(Option<&PayloadSeal>, &mut M) -> Result<()>)
    where
        M: Message + Clone + PartialEq + Debug,
    {
        let message = build(&plain);
        assert!(shows_mark(&message), "the fixture carries no payload");
        let seal = encrypting(1);
        let arrived = build(&|at| seal.seal(&plain(at)).unwrap());
        assert!(!shows_mark(&arrived));

        let mut opened = arrived.clone();
        open(Some(&seal), &mut opened).unwrap();
        assert_eq!(opened, message);

        assert!(open(Some(&encrypting(2)), &mut arrived.clone()).is_err());

        let error = open(None, &mut arrived.clone()).unwrap_err().to_string();
        assert!(error.contains("no payload codec"), "{error}");

        let mut untouched = message.clone();
        open(None, &mut untouched).unwrap();
        assert_eq!(untouched.encode_to_vec(), message.encode_to_vec());

        // A plain message stays as it is with a seal configured too.
        let mut plain_still = message.clone();
        open(Some(&seal), &mut plain_still).unwrap();
        assert_eq!(plain_still, message);
    }

    #[test]
    fn a_workflow_completion_is_sealed() {
        seals(workflow_completion, seal_workflow_completion);
    }

    #[test]
    fn a_workflow_failure_report_is_sealed() {
        seals(workflow_failure, seal_workflow_failure);
    }

    #[test]
    fn a_task_completion_is_sealed() {
        seals(task_completion, seal_task_completion);
    }

    #[test]
    fn a_task_failure_report_is_sealed() {
        seals(task_failure, seal_task_failure);
    }

    #[test]
    fn a_heartbeat_is_sealed() {
        seals(heartbeat, seal_heartbeat);
    }

    #[test]
    fn a_batch_of_heartbeats_is_sealed() {
        seals(heartbeats, seal_heartbeats);
    }

    #[test]
    fn an_actor_completion_is_sealed() {
        seals(actor_completion, seal_actor_completion);
    }

    #[test]
    fn a_state_write_is_sealed() {
        seals(state_write, seal_state);
    }

    /// The journal of an activation is opened entry by entry, whatever kind
    /// each is, and an entry written before a seal was configured is left as
    /// it is.
    #[test]
    fn an_activation_and_its_journal_are_opened() {
        opens(activation, open_activation);
    }

    #[test]
    fn a_task_is_opened() {
        opens(task, open_task);
    }

    #[test]
    fn an_actor_operation_is_opened() {
        opens(actor_operation, open_actor_operation);
    }

    #[test]
    fn a_state_read_is_opened() {
        opens(state_read, open_state);
    }

    /// Opening with the wrong key says so, and names nothing of the value.
    #[test]
    fn the_wrong_key_is_an_error() {
        let seal = encrypting(1);
        let mut arrived = activation(&|at| seal.seal(&plain(at)).unwrap());
        let error = open_activation(Some(&encrypting(2)), &mut arrived)
            .unwrap_err()
            .to_string();
        assert!(error.contains("decryption failed"), "{error}");
        assert!(!error.contains(MARK), "{error}");
    }

    fn entry(attributes: journal_entry::Attributes) -> JournalEntry {
        JournalEntry {
            attributes: Some(attributes),
            ..Default::default()
        }
    }

    /// What a worker seals comes back to a worker in other messages, where
    /// the engine put it unchanged; each opens to what was sealed.
    #[test]
    fn what_a_worker_seals_opens_where_the_engine_sends_it_back() {
        use command::Attributes as C;
        use journal_entry::Attributes as J;
        let key = encrypting(1);
        let seal = Some(&key);

        let mut completion = workflow_completion(&plain);
        seal_workflow_completion(seal, &mut completion).unwrap();
        let mut task_result = task_completion(&plain);
        seal_task_completion(seal, &mut task_result).unwrap();
        let mut task_failed = task_failure(&plain);
        seal_task_failure(seal, &mut task_failed).unwrap();
        let mut state = state_write(&plain);
        seal_state(seal, &mut state).unwrap();

        // The journal the engine writes from those, a task it hands out, a
        // child's first activation, and a query it relays.
        let mut journal = vec![
            entry(J::TaskCompleted(TaskCompletedEventAttributes {
                result: task_result.result,
                ..Default::default()
            })),
            entry(J::TaskFailed(TaskFailedEventAttributes {
                failure: task_failed.failure,
                ..Default::default()
            })),
        ];
        let mut handed_out = PollTaskExecutionResponse::default();
        let mut child = PollWorkflowExecutionResponse::default();
        let mut queries = Vec::new();
        for command in completion.commands {
            match command.attributes.unwrap() {
                C::ScheduleTask(a) => {
                    handed_out.input = a.input.clone();
                    journal.push(entry(J::TaskScheduled(TaskScheduledEventAttributes {
                        input: a.input,
                        ..Default::default()
                    })));
                }
                C::CompleteWorkflow(a) => journal.push(entry(J::WorkflowExecutionCompleted(
                    WorkflowExecutionCompletedEventAttributes {
                        result: a.result,
                        ..Default::default()
                    },
                ))),
                C::FailWorkflow(a) => journal.push(entry(J::WorkflowExecutionFailed(
                    WorkflowExecutionFailedEventAttributes {
                        failure: a.failure,
                        ..Default::default()
                    },
                ))),
                C::CancelWorkflow(a) => journal.push(entry(J::WorkflowExecutionCanceled(
                    WorkflowExecutionCanceledEventAttributes {
                        details: a.details,
                        ..Default::default()
                    },
                ))),
                C::RestartFresh(a) => journal.push(entry(J::WorkflowExecutionRestartedFresh(
                    WorkflowExecutionRestartedFreshEventAttributes {
                        input: a.input,
                        ..Default::default()
                    },
                ))),
                C::StartChildWorkflow(a) => child.input = a.input,
                C::SendEvent(a) => {
                    journal.push(entry(J::EventReceived(EventReceivedEventAttributes {
                        payload: a.payload,
                        ..Default::default()
                    })))
                }
                C::QueryChildWorkflow(a) => queries.push(QueryRequest {
                    query_args: a.query_args,
                    ..Default::default()
                }),
                C::RecordStepResult(a) => {
                    journal.push(entry(J::StepCompleted(StepCompletedEventAttributes {
                        result: a.result,
                        failure: a.failure,
                        ..Default::default()
                    })))
                }
                C::StartTimer(_) | C::UpsertLabels(_) => {}
                other => panic!("a command the fixture does not build: {other:?}"),
            }
        }
        for result in completion.update_results {
            if let Some(update_result::Result::Answer(answer)) = result.result {
                journal.push(entry(J::UpdateCompleted(UpdateCompletedEventAttributes {
                    result: answer,
                    ..Default::default()
                })));
            }
        }
        let mut activation = PollWorkflowExecutionResponse {
            journal,
            queries,
            ..Default::default()
        };
        let mut read = GetStateResponse {
            value: state.value,
            exists: true,
            ..Default::default()
        };
        for message in [&activation, &child] {
            assert!(!shows_mark(message));
        }

        open_activation(seal, &mut activation).unwrap();
        open_activation(seal, &mut child).unwrap();
        open_task(seal, &mut handed_out).unwrap();
        open_state(seal, &mut read).unwrap();

        let payloads: Vec<Vec<u8>> = activation
            .journal
            .iter()
            .flat_map(|entry| match entry.attributes.clone().unwrap() {
                J::TaskCompleted(a) => vec![a.result],
                J::TaskFailed(a) => chain(a.failure),
                J::TaskScheduled(a) => vec![a.input],
                J::WorkflowExecutionCompleted(a) => vec![a.result],
                J::WorkflowExecutionFailed(a) => chain(a.failure),
                J::WorkflowExecutionCanceled(a) => vec![a.details],
                J::WorkflowExecutionRestartedFresh(a) => vec![a.input],
                J::EventReceived(a) => vec![a.payload],
                J::StepCompleted(a) => [vec![a.result], chain(a.failure)].concat(),
                J::UpdateCompleted(a) => vec![a.result],
                other => panic!("an entry the test did not write: {other:?}"),
            })
            .collect();
        let expected: Vec<Vec<u8>> = [
            "task-result",
            "task-failure",
            "task-failure/cause",
            "schedule",
            "complete",
            "fail",
            "fail/cause",
            "cancel",
            "restart",
            "event",
            "step",
            "step-failure",
            "step-failure/cause",
            "update-answer",
        ]
        .into_iter()
        .map(plain)
        .collect();
        assert_eq!(payloads, expected);
        assert_eq!(activation.queries[0].query_args, plain("query-child"));
        assert_eq!(child.input, plain("child"));
        assert_eq!(handed_out.input, plain("schedule"));
        assert_eq!(read.value, plain("state"));
    }

    /// The details of a failure and of each of its causes, outermost first.
    fn chain(failure: Option<Failure>) -> Vec<Vec<u8>> {
        let mut details = Vec::new();
        let mut next = failure;
        while let Some(failure) = next {
            details.push(failure.details);
            next = failure.cause.map(|cause| *cause);
        }
        details
    }
}
