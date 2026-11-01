//! The host's work as the tests read it: the public [`EncodedHost`], with the
//! messages decoded and encoded here, as a language SDK does with generated
//! code of its own. Every host test goes through the encoded API this way.

use orcher_proto::host::v1::{
    ActorOperation, ActorResult, HeartbeatAnswer, TaskAssignment, TaskHeartbeat, TaskOutcome,
};
use orcher_proto::kernel::v1::{Payload, RunActivation, RunActivationResult};
use orcher_proto::prost::Message;
use orcher_sdk_core::{EncodedHost, Host, Result};

/// The host's work, typed for the tests.
pub struct Typed(EncodedHost);

/// A host's work, typed for the tests: `host.typed().next_task()`.
pub trait TypedWork {
    fn typed(&self) -> Typed;
}

impl TypedWork for Host {
    fn typed(&self) -> Typed {
        Typed(self.encoded())
    }
}

impl Typed {
    pub async fn next_activation(&self) -> Option<RunActivation> {
        let bytes = self.0.next_activation().await?;
        Some(RunActivation::decode(bytes.as_slice()).expect("the host sends a RunActivation"))
    }

    pub async fn complete_activation(&self, answer: RunActivationResult) -> Result<()> {
        self.0.complete_activation(&answer.encode_to_vec()).await
    }

    pub async fn next_task(&self) -> Option<TaskAssignment> {
        let bytes = self.0.next_task().await?;
        Some(TaskAssignment::decode(bytes.as_slice()).expect("the host sends a TaskAssignment"))
    }

    pub fn heartbeat(&self, token: &[u8], details: Option<Payload>) -> HeartbeatAnswer {
        let heartbeat = TaskHeartbeat {
            token: token.to_vec(),
            details,
            ..Default::default()
        };
        let answer = self
            .0
            .heartbeat(&heartbeat.encode_to_vec())
            .expect("an encoded TaskHeartbeat is accepted");
        HeartbeatAnswer::decode(answer.as_slice()).expect("the host answers a HeartbeatAnswer")
    }

    pub async fn complete_task(&self, outcome: TaskOutcome) -> Result<()> {
        self.0.complete_task(&outcome.encode_to_vec()).await
    }

    pub async fn next_actor_operation(&self) -> Option<ActorOperation> {
        let bytes = self.0.next_actor_operation().await?;
        Some(ActorOperation::decode(bytes.as_slice()).expect("the host sends an ActorOperation"))
    }

    pub async fn complete_actor_operation(&self, result: ActorResult) -> Result<()> {
        self.0
            .complete_actor_operation(&result.encode_to_vec())
            .await
    }
}
