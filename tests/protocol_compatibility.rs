//! What this crate does with an answer from a runtime built against a newer
//! kernel protocol than its own, through the encoded API every SDK uses.
//!
//! A runtime decodes and encodes the protocol with generated code of its own,
//! at the version it implements, so it may be ahead of this crate. A field
//! this crate does not know is ignored, as protobuf does, which is what lets
//! either side move to a newer protocol on its own schedule. A command of a
//! kind it does not know is refused as a protocol error, never dropped: a
//! durable call silently skipped would change what the run does.

use orcher_proto::kernel::v1::{
    command, run_activation_result, Command, Commands, CompleteRun, RunActivationResult,
};
use orcher_proto::prost::Message;
use orcher_sdk_core::kernel::encoded;
use orcher_sdk_core::{Command as CoreCommand, Error};

/// A length-delimited field: its key, its length, its bytes.
fn field(number: u32, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    orcher_proto::prost::encoding::encode_key(
        number,
        orcher_proto::prost::encoding::WireType::LengthDelimited,
        &mut out,
    );
    orcher_proto::prost::encoding::encode_varint(bytes.len() as u64, &mut out);
    out.extend_from_slice(bytes);
    out
}

/// A field number no version of `orcher.kernel.v1` this crate builds against
/// uses: what a newer protocol might add.
const FROM_A_NEWER_PROTOCOL: u32 = 900;

fn completes(answer: &[u8]) -> bool {
    let result = encoded::execution_result("run-1", answer).expect("an answer it can carry");
    matches!(
        result.commands.as_slice(),
        [CoreCommand::CompleteWorkflow(_)]
    )
}

#[test]
fn a_field_from_a_newer_protocol_is_ignored() {
    let answer = RunActivationResult {
        activation_id: b"act-1".to_vec(),
        outcome: Some(run_activation_result::Outcome::Commands(Commands {
            commands: vec![Command {
                seq: 0,
                what: Some(command::What::CompleteRun(CompleteRun::default())),
                ..Default::default()
            }],
            ..Default::default()
        })),
        ..Default::default()
    };
    let known = answer.encode_to_vec();
    assert!(completes(&known));

    // The same answer with a field this crate's protocol does not have, on
    // the result itself: carried as before.
    let mut newer = known.clone();
    newer.extend(field(FROM_A_NEWER_PROTOCOL, b"something new"));
    assert_ne!(newer, known);
    assert!(completes(&newer));
}

#[test]
fn a_command_of_a_kind_from_a_newer_protocol_is_refused() {
    // A command whose kind is a oneof variant this crate's protocol does not
    // have. It decodes as a command of no kind; the field is the only thing
    // that says what the code asked for, and this crate cannot know it.
    let mut call = Vec::new();
    orcher_proto::prost::encoding::uint64::encode(1, &3, &mut call); // seq
    call.extend(field(FROM_A_NEWER_PROTOCOL, b""));
    let mut answer = field(1, b"act-1"); // activation_id
    answer.extend(field(2, &field(1, &call))); // commands { commands: [call] }

    let decoded = RunActivationResult::decode(answer.as_slice()).expect("it decodes");
    let Some(run_activation_result::Outcome::Commands(commands)) = decoded.outcome else {
        panic!("decoded as commands");
    };
    assert_eq!(commands.commands.len(), 1);
    assert_eq!(
        commands.commands[0].what, None,
        "the new kind is unknown here"
    );

    match encoded::execution_result("run-1", &answer) {
        Err(Error::Protocol(message)) => assert!(message.contains("no kind"), "{message}"),
        other => panic!("refused as a protocol error, got {other:?}"),
    }
}
