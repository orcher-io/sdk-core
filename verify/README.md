# orcher-verify

Checks a workflow runtime, in any language, against the ORCHER kernel
protocol ([`docs/kernel-protocol-v1.md`](https://github.com/orcher-io/sdk-core/blob/main/docs/kernel-protocol-v1.md)),
without an engine and without this repository's Rust tests.

A runtime is the part of a language SDK that runs workflow code: handed a
`RunActivation` (the outcomes the run observes, resolved by the kernel), it
runs the workflow's code against them and answers with a
`RunActivationResult` (the durable calls the code made). `orcher-verify`
hands a runtime the activations recorded from the SDKs' contract suites and
judges every answer with the kernel's own checks.

```sh
cargo install --git https://github.com/orcher-io/sdk-core orcher-verify
orcher-verify vectors --runtime node my-runtime.js
```

Everything after `--runtime` is the command that starts the runtime. The
vectors are built into the binary, so nothing else is needed. The run prints
a line per vector and a summary, and exits 0 only when every vector passes.

```text
ok    rust/08af7e2e-…/3f501568-…/0  task_input_roundtrip
FAIL  rust/08af7e2e-…/3f501568-…/1  task_input_roundtrip
        command 0 differs
          recorded: Command { command_type: CompleteWorkflow, … }
          answered: Command { command_type: CompleteWorkflow, … }
55 vectors: 54 passed, 1 failed
```

Options:

- `--sdk <sdk>`: only the vectors recorded from that SDK's contract suite
  (`rust`, `python`, `typescript`); may be given more than once.
- `--timeout <secs>`: how long to wait for each answer (default 30).
- `--vectors <dir>`: read the vectors from a directory laid out like
  [`vectors/`](https://github.com/orcher-io/sdk-core/tree/main/verify/vectors) instead of the built-in ones.

`orcher-verify list` lists the vectors with the workflow type each runs.

## The framing

The runner starts the runtime as a child process and talks to it over its
stdin and stdout:

- Each message is its length in bytes as a 4-byte unsigned big-endian
  integer, then that many bytes of the encoded protobuf message. A message is
  at most 64 MiB.
- The runner writes one `orcher.kernel.v1.RunActivation` and waits for one
  `orcher.kernel.v1.RunActivationResult`, whose `activation_id` echoes the
  activation's, before it writes the next. The vectors come in order, a
  run's activations one after another, but a runtime keeps nothing between
  activations: each carries the run's whole history.
- After the last vector the runner closes the runtime's stdin. The runtime
  should exit, with status 0, within 5 seconds.
- Nothing but frames may be written to stdout. The runtime's stderr is
  passed through, for its logs.

An answer that does not come within the timeout, a closed stdout, or bytes
that are not a frame fail that vector and every one after it.

## What a runtime must do

Each vector runs one of the contract suite's workflows, named by
`RunActivation.run.workflow_type` (`orcher-verify list` shows them). The
runtime registers its own implementation of each, as the SDK's contract suite
does, and runs the activation through the same code its worker uses. The
expected answer is the recorded one, payloads included: the bytes a workflow
passes to a task or completes with must be the bytes the recording SDK
produced. Pick the vectors of the SDK whose contract suite your workflows
implement with `--sdk`.

## How an answer is judged

An answer passes when:

1. It decodes as a `RunActivationResult` for the activation it answers, and
   is not a `protocol_error`.
2. It replays the recorded history: the kernel's
   `kernel::encoded::check_recorded`, the checks a worker runs before it
   sends an answer to the engine, finds no determinism violation. A call
   answered from history (`satisfied`) sends nothing, so it is judged here
   only, by its step id.
3. The worker would send the engine for it what it sent when the run was
   recorded (`kernel::encoded::recorded_answer`): the same commands, in the
   same order and with the same payloads, and the same query and update
   answers; or, for a workflow the code failed, the same failure. Fields that
   are the worker's own (identity, protocol version, the activation token)
   are not compared.

The answer is not compared byte for byte with a recorded
`RunActivationResult`: what reaches the engine is what counts, and two
answers that send it the same thing are the same answer.

## The vectors

[`vectors/`](https://github.com/orcher-io/sdk-core/tree/main/verify/vectors) is language neutral; a runtime author can read it
without this binary.

- `replay/<sdk>/<workflow id>/<run id>/<order>-<kind>.pb` are the runs
  recorded from `<sdk>`'s contract suite (`ORCHER_RECORD_ACTIVATIONS`, see
  sdk-core's `worker::record`), one engine message per file, in the order
  `<order>` sorts them. `<kind>` is `activation` (an encoded
  `orcher.v1.PollWorkflowExecutionResponse`), `completion` (an encoded
  `orcher.v1.CompleteWorkflowExecutionRequest`) or `failure` (an encoded
  `orcher.v1.FailWorkflowExecutionRequest`).
- `replay/<sdk>/resolutions.json` maps `<workflow id>/<run id>/<n>`, the
  run's `n`th activation from 0, to the `RunActivation` the kernel resolves
  it to, encoded and in hex. sdk-core's `tests/kernel_resolution.rs` fails
  when the kernel resolves a recording differently.
- `index.json` lists every vector, one per recorded activation the worker
  answered:

```json
{
  "format": 1,
  "namespace": "contract",
  "vectors": [
    {
      "name": "rust/08af7e2e-…/3f501568-…/0",
      "sdk": "rust",
      "workflow_type": "task_input_roundtrip",
      "workflow_id": "08af7e2e-…",
      "run_id": "3f501568-…",
      "activation": 0,
      "run_activation": {
        "file": "replay/rust/resolutions.json",
        "key": "08af7e2e-…/3f501568-…/0"
      },
      "recorded_activation": "replay/rust/08af7e2e-…/3f501568-…/0001-activation.pb",
      "expected": {
        "kind": "completion",
        "file": "replay/rust/08af7e2e-…/3f501568-…/0002-completion.pb"
      }
    }
  ]
}
```

Paths are relative to `vectors/`. `run_activation` is what the runtime is
handed; `recorded_activation` is the history the answer is checked against;
`expected` is what the worker sent after it. `namespace` is the one the runs
were recorded in. A change to the format a reader must know of is a new
`format`.

The index is generated from the recordings; this crate's tests fail when it
is stale. After adding or changing recordings, write it again with

```sh
ORCHER_BLESS_VECTORS=1 cargo test -p orcher-verify
```

## The reference runtime

`orcher-verify reference-runtime` runs no workflow code: it answers each
vector with its recorded answer, put back into the kernel protocol (a
satisfied call for each step the history recorded, then a call for each
command the worker sent). It is the smallest runtime that speaks the framing,
and the runner's own tests run it:

```sh
orcher-verify vectors --runtime orcher-verify reference-runtime
```

passes every vector, and with `--answers drop-command`, `change-task-type`
or `forget-history` it answers wrong on purpose, which the tests require the
runner to fail.
