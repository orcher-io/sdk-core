//! The client protocol (`orcher.client.v1`): every client call as one
//! encoded message through one entry.
//!
//! A language binding makes a [`ProtocolClient`] from its [`Connection`] and
//! hands it encoded `ClientCall`s; each is answered with an encoded
//! `ClientAnswer`, a typed result or a `ClientError`. The binding wraps that
//! one function, not one per call, and a new client call is a change to the
//! protocol and to this module, with no change to any binding.

use std::time::Duration;

use orcher_proto::client::v1 as proto;
use orcher_proto::client::v1::client_answer::Answer;
use orcher_proto::client::v1::client_call::Call;

use crate::client::{ActorClient, CancelWorkflowOpts, Connection, NamespaceClient, WorkflowClient};
use crate::error::{Error, Result};
use crate::wire::client_protocol as wire;

/// Answers client protocol calls on one connection.
///
/// Cheap to clone: clones share the connection.
#[derive(Clone)]
pub struct ProtocolClient {
    workflows: WorkflowClient,
    actors: ActorClient,
    namespaces: NamespaceClient,
    timeout: Option<Duration>,
}

impl std::fmt::Debug for ProtocolClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtocolClient")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl ProtocolClient {
    pub(crate) fn new(connection: &Connection) -> Self {
        Self {
            workflows: connection.workflow_client(),
            actors: connection.actor_client(),
            namespaces: connection.namespace_client(),
            timeout: None,
        }
    }

    /// Gives each call `timeout` unless the call sets its own; see
    /// [`WorkflowClient::with_timeout`].
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Answers an encoded `orcher.client.v1.ClientCall` with an encoded
    /// `orcher.client.v1.ClientAnswer`. Never fails: a call that cannot be
    /// decoded, or that the engine refuses, is answered with a `ClientError`.
    pub async fn call(&self, call: &[u8]) -> Vec<u8> {
        let answer = match wire::decode_call(call) {
            Ok(call) => self.answer(call).await,
            Err(error) => Err(error),
        };
        wire::encode_answer(answer)
    }

    async fn answer(&self, call: proto::ClientCall) -> Result<Answer> {
        let mut workflows = self.workflows.clone();
        if !call.namespace.is_empty() {
            workflows = workflows.with_namespace(&call.namespace);
        }
        if let Some(timeout) = call.timeout.map(wire::duration).or(self.timeout) {
            workflows = workflows.with_timeout(timeout);
        }
        let what = call
            .call
            .ok_or_else(|| Error::invalid_payload("the call names no operation"))?;
        let done = |_: ()| {
            Answer::Done(proto::Done {
                ..Default::default()
            })
        };
        match what {
            Call::StartWorkflow(start) => {
                let handle = workflows
                    .start_workflow_with_options(wire::start_opts(start)?)
                    .await?;
                Ok(Answer::Started(proto::Started {
                    workflow: Some(wire::workflow_ref(handle.workflow_id(), handle.run_id())),
                    ..Default::default()
                }))
            }
            Call::WaitForResult(wait) => {
                let (workflow_id, run_id) = wire::workflow(wait.workflow)?;
                let deadline = wait
                    .wait
                    .map(|wait| tokio::time::Instant::now() + wire::duration(wait));
                let value = workflows
                    .get_workflow_result_until(workflow_id, run_id.unwrap_or_default(), deadline)
                    .await?;
                Ok(Answer::Result(proto::WorkflowResult {
                    value,
                    ..Default::default()
                }))
            }
            Call::GetStatus(get) => {
                let (workflow_id, run_id) = wire::workflow(get.workflow)?;
                let status = workflows.get_workflow_status(workflow_id, run_id).await?;
                Ok(Answer::Status(wire::status(status) as i32))
            }
            Call::Describe(describe) => {
                let (workflow_id, run_id) = wire::workflow(describe.workflow)?;
                let description = workflows.describe_workflow(workflow_id, run_id).await?;
                Ok(Answer::Description(wire::description(description)))
            }
            Call::ListWorkflows(list) => {
                let page = workflows.list_workflows(wire::list_options(list)).await?;
                Ok(Answer::Page(wire::page(page)))
            }
            Call::SearchWorkflows(search) => {
                let query = search.query.clone();
                let page = workflows
                    .search_workflows(query, wire::search_options(search))
                    .await?;
                Ok(Answer::Page(wire::page(page)))
            }
            Call::SendEvent(event) => {
                let (workflow_id, run_id) = wire::workflow(event.workflow)?;
                workflows
                    .send_event_raw(workflow_id, run_id, event.event_name, event.payload)
                    .await
                    .map(done)
            }
            Call::Query(query) => {
                let (workflow_id, run_id) = wire::workflow(query.workflow)?;
                workflows
                    .query_workflow_raw(workflow_id, run_id, query.query_name, query.args)
                    .await
                    .map(Answer::Value)
            }
            Call::Update(update) => {
                let (workflow_id, run_id) = wire::workflow(update.workflow)?;
                workflows
                    .update_workflow_raw(workflow_id, run_id, update.update_name, update.args)
                    .await
                    .map(Answer::Value)
            }
            Call::Cancel(cancel) => {
                let (workflow_id, run_id) = wire::workflow(cancel.workflow)?;
                let mut opts = CancelWorkflowOpts::default();
                if let Some(cleanup) = cancel.cleanup_timeout {
                    opts = opts.with_cleanup_timeout(wire::duration(cleanup));
                }
                workflows
                    .cancel_workflow_with(workflow_id, run_id, opts)
                    .await
                    .map(done)
            }
            Call::Terminate(terminate) => {
                let (workflow_id, run_id) = wire::workflow(terminate.workflow)?;
                workflows
                    .terminate_workflow(workflow_id, run_id, terminate.reason)
                    .await
                    .map(done)
            }
            Call::Reset(reset) => {
                let (workflow_id, run_id) = wire::workflow(reset.workflow)?;
                let new_run = workflows
                    .reset_workflow(workflow_id.clone(), run_id, reset.event_id, reset.reason)
                    .await?;
                Ok(Answer::ResetTo(wire::workflow_ref(&workflow_id, &new_run)))
            }
            Call::InvokeActor(invoke) => self
                .actors
                .invoke_operation(
                    invoke.actor_name,
                    invoke.key,
                    invoke.operation,
                    invoke.payload,
                    invoke
                        .operation_timeout
                        .map(|timeout| wire::duration(timeout).as_millis() as u64),
                    (!invoke.idempotency_key.is_empty()).then_some(invoke.idempotency_key),
                )
                .await
                .map(Answer::Value),
            Call::CreateNamespace(create) => self
                .namespaces
                .create_namespace_with_options(
                    create.name,
                    create.description,
                    create.retention_period_days,
                    create.owner_email,
                )
                .await
                .map(|namespace| Answer::Namespace(wire::namespace(namespace))),
            Call::GetNamespace(get) => self
                .namespaces
                .get_namespace(get.name)
                .await
                .map(|namespace| Answer::Namespace(wire::namespace(namespace))),
            Call::ListNamespaces(list) => {
                let (namespaces, total) = self
                    .namespaces
                    .list_namespaces(list.page_size, list.page_offset)
                    .await?;
                Ok(Answer::Namespaces(proto::NamespacePage {
                    namespaces: namespaces.into_iter().map(wire::namespace).collect(),
                    total,
                    ..Default::default()
                }))
            }
            Call::UpdateNamespace(update) => self
                .namespaces
                .update_namespace(
                    update.name,
                    update.description,
                    update.owner_email,
                    update.retention_period_days,
                )
                .await
                .map(|namespace| Answer::Namespace(wire::namespace(namespace))),
            Call::DeprecateNamespace(deprecate) => self
                .namespaces
                .deprecate_namespace(deprecate.name)
                .await
                .map(|namespace| Answer::Namespace(wire::namespace(namespace))),
            Call::DeleteNamespace(delete) => self
                .namespaces
                .delete_namespace(delete.name)
                .await
                .map(done),
        }
    }
}

impl Connection {
    /// A client that answers client protocol calls on this connection.
    pub fn protocol_client(&self) -> ProtocolClient {
        ProtocolClient::new(self)
    }
}
