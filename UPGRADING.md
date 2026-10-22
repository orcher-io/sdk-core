# Upgrading

What a language SDK, or any other user of this crate, changes when it moves to
a new minor release. Every breaking change is also listed in
[CHANGELOG.md](CHANGELOG.md).

## 0.19 to 0.20

0.20 runs the worker host from core and takes the engine's own protocol
(`orcher.v1`) and tonic out of the public API: a change to either no longer
changes this crate's API. A language SDK speaks the kernel and host protocols
with core, and core speaks to the engine on its behalf.

### Worker

| Before | Now |
| --- | --- |
| the `orcher-sdk-host` crate | `orcher_sdk_core::{Host, HostConfig, Registrations, ActorHandler, StateScope, StateValue}` |
| `HostConfig.{server_url, namespace, api_key, organization_id, tls}` | `HostConfig.connection: ConnectOptions` (`HostConfig::new` takes an address or a `ConnectOptions`; the `with_*` setters are unchanged) |
| prost encoding in a language binding | `Host::encoded()` hands out and takes encoded kernel and host messages |
| drivers, pollers, `WorkflowCache`, the request types, the old module paths | private or removed: run the worker on `Host` |

### Clients

| Before | Now |
| --- | --- |
| `WorkflowClient::connect(url)`, `WorkflowClient::from_channel(channel)` | `Connection::connect(ConnectOptions::new(url))` (or `Connection::lazy`), then `connection.workflow_client()` |
| `NamespaceClient::{connect, from_channel, with_auth}` | `connection.namespace_client()` |
| `CoreActorClient::{new, with_auth}` | `connection.actor_client()` (`ActorClient`) |
| `WorkflowClient::{with_api_key, with_organization_id, with_max_message_bytes, auth}`, `AuthInterceptor` | `ConnectOptions::{with_api_key, with_organization_id, with_max_message_bytes}`: every client on the connection carries them |
| a tonic `Channel` built by the SDK for TLS | `ConnectOptions::with_tls(TlsConfig)`, `with_connect_timeout` |
| `StartWorkflowOpts.retry_policy: orcher.v1::RetryPolicy` | `WorkflowRetryPolicy` (the one used for children and restart-fresh too) |
| `StartWorkflowOpts.id_reuse_policy: orcher.v1::WorkflowIdReusePolicy` | `WorkflowIdReusePolicy` |
| `start_workflow_request(StartWorkflowRequest)` | `start_workflow_with_options(StartWorkflowOpts)` |
| `batch_start_workflows(Vec<StartWorkflowRequest>)` | `batch_start_workflows(Vec<StartWorkflowOpts>)` |
| namespace calls return `orcher.v1::NamespaceInfo`; a refusal is `Error::Internal` | they return `Namespace`; a refusal is `Error::Status` with its code |

A connection measures every client's calls: `Connection::wire_metrics`.

### Errors

| Before | Now |
| --- | --- |
| `Error::GrpcStatus(tonic::Status)` | `Error::Status(Status)`: `status.code()` is core's `Code`, `status.message()` the message |
| `Error::Transport(tonic::transport::Error)` | removed: never produced; a failed connection is `Error::Connection` |
| `From<tonic::Status> for Error`, `Error::from_status_for_workflow` | removed |

### Protocol types and replay

| Before | Now |
| --- | --- |
| `orcher_sdk_core::proto` (all of orcher-proto) | `proto::{kernel, host, prost, prost_types}` only; depend on `orcher-proto` directly for engine messages (a fake engine in tests, for one) |
| `worker::record::Recorded::Activation(PollWorkflowExecutionResponse)` and the other variants | the same variants holding the encoded message bytes |
| `kernel::resolve(&WorkflowExecutionTask, ns)`, `Replayer::check_activation` / `kernel::check_activation` | `kernel::encoded::resolve_recorded(bytes, ns)` and `kernel::encoded::check_recorded(recorded, answer)` |
| `kernel::execution_result(run_id, RunActivationResult)` | `kernel::encoded::execution_result(run_id, bytes)` |
| `WorkflowExecutionTask`, `TaskExecutionTask` | private: a language receives activations and tasks from `Host` |

### Shared types

`types::{Failure, Header, Annotations, OrganizationContext}` are removed
(nothing used them). A described workflow's retry policy
(`WorkflowExecutionConfig.retry_policy`) is written in camelCase in JSON, like
the description around it.
