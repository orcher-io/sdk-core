# orcher-verify

Checks a workflow runtime, in any language, against the ORCHER kernel
protocol ([`docs/kernel-protocol-v1.md`](https://github.com/orcher-io/sdk-core/blob/main/docs/kernel-protocol-v1.md)),
without an engine and without this repository's Rust tests. With an engine,
it also runs the contract scenarios against the runtime's whole worker
([below](#the-contract-scenarios)).

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

## The contract scenarios

The vectors check a runtime without an engine. The contract scenarios check
a whole worker with one: [`scenarios.json`](https://github.com/orcher-io/sdk-core/blob/main/verify/scenarios.json)
names, for each scenario, a workflow (or actor) of the contract catalog, the
input it is started with, and what a client must observe: the terminal
status, the JSON the workflow returns, the error a refused call reports.
Every SDK's worker serves the same-named catalog (sdk-rust's
`contract/src/catalog.rs` is the reference), so one runner judges them all.

`scenarios.json` here is the source of truth. It was copied byte for byte
from the SDK contract suites' `contract/scenarios.json`, identical in
sdk-rust, sdk-py and sdk-ts at the time (sdk-rust commit 7789f26), and the
SDKs are to sync their copy from this one. It is built into the binary;
`--scenarios <file>` reads another.

### Running them

1. Start an engine with durable task failures on, and with short heartbeat
   timeouts, so the liveness scenarios
   (`task-outlives-heartbeat-timeout-without-heartbeat-code` among them)
   time out a silent task within seconds rather than a minute:

   ```sh
   ORCHER_DURABLE_TASK_FAILURE=true
   ORCHER_DEFAULT_HEARTBEAT_TIMEOUT_SECS=3
   ORCHER_TIMEOUT_WATCHER_POLL_INTERVAL_SECS=1
   ORCHER_TIMEOUT_WATCHER_GRACE_SECS=1
   ```

2. Start your worker, serving the contract catalog, on a task queue of its
   own in the `contract` namespace. sdk-rust's starts with
   `cargo run -p orcher-contract -- --worker-only --task-queue <queue>`.
3. Run the scenarios against the same engine and queue:

   ```sh
   orcher-verify scenarios --server-url http://localhost:50051 \
     --task-queue <queue> --sdk <your sdk>
   ```

The runner creates the namespace if it is missing, runs each scenario
through core's client (`orcher_sdk_core::Connection`), and prints a line per
scenario and a summary, in the vectors' style. It exits 1 if any scenario
fails unexpectedly, 2 on a usage error.

```text
ok    echo  echo
FAIL  task-failure-carries-its-type  task_failure_type
        result mismatch
            expected: {"cancelled":false,"caught":true,"error_type":"CardDeclined"}
            actual:   {"cancelled":false,"caught":true,"error_type":"ExecutionError"}
skip  task-retried-after-its-worker-dies  crash_mid_task
        needs the SDK's own harness: it starts a worker process whose task kills it …
ok    event-wakes-parked-workflow  wait_for_event_echo
        judged in part: the engine describes no wait on an event, …
xfail …
47 scenarios: 37 passed (1 judged in part), 6 failed, 0 expected failures, 4 skipped
```

A scenario is `ok`, `FAIL`, `xfail` (failed as its known gap says),
`skip` (not run, with the reason) or `ok` with "judged in part" (what a
client can see held; the line says what it could not see).

Options:

- `--namespace <ns>`: the namespace (default `contract`, not `default`: a
  child workflow started in the wrong namespace only shows as wrong in a
  namespace that is not `default`).
- `--sdk <name>`: whose known gaps apply. A scenario with `known_gap` (for
  every SDK, or the ones `known_gap_sdks` lists) is expected to fail: its
  failure is reported as `xfail` and does not fail the run, and a pass fails
  the run so the flag is removed. Without `--sdk` only gaps for every SDK
  apply.
- `--only <id>[,<id>…]`: only these scenarios; may be given more than once.
- `--parallel <n>`: run `n` scenarios at once (default 1). The scenarios
  that restart the engine then run alone, at the end.
- `--result-timeout <secs>`: how long to wait for a workflow to end
  (default 30).
- `--api-key <key>`: for an engine that authenticates callers (default
  `ORCHER_API_KEY`).
- `--restart-command <cmd>`: a shell command that restarts the engine, for
  `engine-restart-then-dispatch` (default `ORCHER_CONTRACT_RESTART_CMD`);
  without one it is skipped.
- `--scenarios <file>`: read the catalog from this file.
- `--actor-arguments value|tuple`: how an actor operation's input is
  encoded. sdk-rust's operations decode their arguments as a JSON array, a
  tuple of one; sdk-py's and sdk-ts's clients send the bare value. The
  default is `tuple` for `--sdk rust` and `value` otherwise.

A workflow's input is sent as plain JSON, as sdk-py's and sdk-ts's clients
send it. sdk-rust's client wraps it in its payload envelope; its worker reads
either.

### How each kind is judged

The judgement is sdk-rust's contract runner's (`contract/src/main.rs`), kind
by kind. Where that runner reads the worker's own process, this one says
what it judges instead.

| `kind` | judged by |
| --- | --- |
| absent (a workflow) | The terminal status. For `completed`, the result: the fields `expect.result` lists, or with `result_exact` the whole result as equal JSON; and `min_elapsed_ms`/`max_elapsed_ms` on the wall clock. For `failed`, that the surfaced failure contains `error_contains`. |
| `actor` | Each step's `parallel` invocations on a fresh key, every result matching `expect_each`. |
| `reset` | The run completes, is reset to `reset_to_event_id`, and the successor run's result matches. |
| `client_error` | The named call fails with core's error code `expect.error_code`. |
| `workflow_id_reuse` | A second start under the id while it runs is refused with `error_code`, naming the open run. Once it completed, `AllowDuplicateFailedOnly` is refused the same way, and `AllowDuplicate` starts a new run whose result matches. |
| `event` | Once parked, the event is sent by workflow id alone; the result, whole-run elapsed bounds, and `result_at_most` bounds are checked. |
| `cancel` | Once parked, the run is cancelled; it ends cancelled, or completes with a matching result. |
| `engine_restart` | After the restart command, a new workflow completes on the untouched worker. Skipped without a restart command. |
| `non_determinism_held` | In part, below. |
| `cancel_queued_task`, `worker_crash` | Skipped, below. |

"Parked" is read from the engine: the run is open and its description shows
a pending timer or event. The engine describes a workflow parked on an event
alone as waiting on nothing, so for `event-wakes-parked-workflow` the runner
waits 3 seconds instead and reports the scenario as judged in part: the
result and its activation bound are judged, but that the event woke a parked
run, rather than reaching its first activation, is not seen.

Scenarios a client cannot judge whole:

- `child-id-changed-on-replay-is-held` (`non_determinism_held`) is judged in
  part. Its `expect.observed`, that the worker replayed the run and how many
  times the child ran, is counted in the worker's process. The runner judges
  what that leaves in the engine instead: the catalog names the child
  `<key>-child-<n>`, `n` counting the parent's activations, so once
  `<key>-child-1` has completed (the parent is resumed with it: the replay
  under test) and `hold_ms` has passed, the run must still be running and
  `<key>-child-2` must not exist. That the replay happened is not observed.
- `cancel-stops-a-queued-task` (`cancel_queued_task`) is skipped. It serves a
  fresh queue with a worker that runs workflows and no tasks, starts one that
  runs tasks only after the cancellation, and asserts what that worker saw in
  its own process: only the SDK's harness can.
- `task-retried-after-its-worker-dies` and
  `task-timeout-on-last-attempt-reaches-workflow` (`worker_crash`) are
  skipped. They start a worker process whose task kills it, then another, on
  a queue of their own: only the SDK's harness can.

sdk-rust's runner also checks, after the scenarios, the wire metrics its
in-process worker recorded. That is not a scenario, and is not run here.

### Where this is going

Each SDK's contract suite carries a runner of its own today, driving the
same file. They are meant to become thin wrappers over this one: start the
SDK's worker, run `orcher-verify scenarios` against it, and keep in the SDK
only the scenarios that need its own harness.
