# Kernel protocol v1: design

Status: agreed design for sdk-core#25 (step 0 of #44). The messages are defined in orcher-io/protos (`orcher.kernel.v1`).

## 1. What this is

The protocol between the kernel (sdk-core) and a language SDK. **The kernel resolves durable history; the SDK executes user code.** An activation comes in carrying resolved outcomes; commands go out. One round trip per activation, and the kernel never calls back into the language while resolving.

It is a separate layer from the engine's gRPC API (orcher-proto `orcher.v1`), which the kernel speaks to the engine and which this design changes only where §11 says so.

```
engine ──orcher.v1 (gRPC)──► kernel (sdk-core) ──orcher.kernel.v1──► SDK (user code)
                             resolution, step identity,             registration, API,
                             determinism, clock/random source,      async integration,
                             history cache                          type conversion
```

Language APIs stay idiomatic: Rust keeps async fns, typed task references and macros; TypeScript keeps promises and decorators; Python keeps coroutines and its typing. They share semantics, not a programming model.

Non-goals for v1: resumable live workflow instances (§9); moving task and actor work onto this protocol (no replay semantics); choosing the C ABI or any one transport.

## 2. Why: what crosses the boundary today

- **The SDK interprets the journal.** Core turns the raw journal into jobs (`protocol::task_to_execution_request`), but every SDK calls it itself, then re-reads the raw journal for what the jobs lack: times (`journal_times.rs`, copied in py and ts; inline in rust), journaled updates (py only) and journaled children (rust, py). Each builds its own result cache and failure sentinel (`__orcher_task_failed__`, `__orcher_child_failed__`).
- **The jobs are not trustworthy as they stand.** Every job time is `SystemTime::now()`, `StartWorkflow.namespace` is hard-coded `"default"`, `CompleteTask` is never produced, and the SDKs disagree on what a failed `CompleteTask` means (rust and ts drop it; py makes a sentinel without attempts).
- **The SDKs disagree on step identity** (verified):

  | | rust | py | ts |
  |---|---|---|---|
  | sequence base | 0 | **1** | 0 |
  | default child id | `{wf}-child-{n}` (legacy `child_{n}` if journaled) | same, from 1 | **`child_{n}` only** |
  | `WaitForEvent` step id | `event_{name}_{seq}` | **`event_{name}`** | `event_{name}_{seq}` |
  | random seed | wf id + counter, `DefaultHasher` (**not stable across Rust releases**, per its own doc) | sha256(wf id, run id) | sha256(wf id + counter) |
  | journaled updates replayed | no | yes | no |

- **Determinism is checked only partly.** Core's `JournalSteps::check_activation` compares scheduled tasks, timers and children plus `reached_steps`; a task inserted before a same-type task, or a changed timer duration, replays silently (sdk-core#14). `WaitForEvent` is silently dropped on conversion and `RequestCancellation` fails the workflow as "unsupported".
- **Every activation is the whole journal.** The engine reads it all from Postgres each time (measured: 1,004,002 journal rows for one 1,000-step run), and the `WorkflowCache` is held and never read.
- **The boundary is JSON.** Bytes become JSON number arrays (3–4× size), tokens base64 on top.

## 3. Principles

1. **Resolution, identity and determinism are the kernel's.** If two SDKs doing it differently would change the journal, replay or what the engine sees, it is in the kernel.
2. **The SDK never sees the journal.** It sees resolutions, ordered and timed by the kernel.
3. **The kernel decides what history permits; the SDK reports what user code asks to do.**
4. **Nothing is silently dropped.** A message or command a side does not understand is a protocol failure that side reports, never a no-op and never a workflow failure. Today's two command-conversion behaviours (skip, and fail-the-workflow) are what this rule exists to prevent.
5. **The semantic core is small and stable.** Performance and resource features are optional capabilities that never change outcomes (§10).
6. **The same messages in every deployment**: in-process, sidecar, WASM, engine-side (§12).

## 4. Execution model: replay-based

v1 keeps the model every SDK has today. On each activation the SDK runs the handler **from the start**: each durable call returns its resolution if the activation has one, otherwise records a command and suspends; suspension unwinds the handler (rust `WorkflowError::Suspended`, py `WorkflowSuspendedError`, ts `WorkflowError.suspended`, verified), and the next activation starts it again. Local state is rebuilt by replay.

What changes is only where the resolutions come from: the kernel, already ordered, timed, decoded and keyed by kernel-assigned step ids, instead of each SDK reading the journal. So v1 fixes correctness and cross-language drift without changing how any SDK suspends.

Resumable instances (keep the coroutine alive between activations) are a different execution model with its own failure semantics; they are deferred to §9 and must not be assumed by anything in v1.

## 5. The replay contract

For each activation:

1. The kernel resolves the run's history into an ordered list of resolutions (§6), each timed and keyed by step id.
2. The SDK starts the handler and runs it until it ends or every path is blocked, answering each durable call from the resolutions.
3. The SDK returns the commands user code issued, **in issue order, including those already satisfied by a resolution**, each with its call sequence. A satisfied command is sent marked `satisfied`, **without its payload fields** (inputs, results, headers): payloads are never compared (§8.2), so leaving them out is always safe, the SDK needs no knowledge of the comparison table, and it skips encoding them for a call answered from history. New commands are sent in full.
4. The kernel compares the stream against history (§8): commands that history already records must match it; the first command past the end of history and everything after it are new.
5. The kernel accepts the new commands and sends them to the engine, or reports a **determinism violation** and sends nothing.

Returning already-satisfied commands is what makes the comparison complete: the kernel sees the whole decision sequence, not only the new tail. (Today the SDK sends `reached_steps` for the same purpose, but only ids, so kind and parameters cannot be compared.)

**Logical time.** Each resolution carries `at`, the run's time when it happened; the SDK's `now()` is the `at` of the last resolution consumed. **Order** is the order the run must observe outcomes, derived from journal order and recorded times, not from job position (which is wrong today: the engine appends step results after the journal).

**`replaying`** is diagnostic metadata (true while resolutions repeat outcomes the run already saw), for log de-duplication. It is not a safety mechanism: correctness comes from durable calls resolving from history. The SDK API must not offer it to user code as a way to decide whether to perform an external effect; external effects belong in tasks, with idempotency where needed.

**Cancellation.** A `CancelAsked` resolution is delivered once, to the first durable wait whose outcome history did not record at or before the request time, exactly as the three SDKs do today (cancel-with-cleanup); later waits run normally. The kernel computes the delivery point and puts it in the resolution list, so SDKs stop computing it.

## 6. Messages (`orcher.kernel.v1`, semantic core)

Names are this protocol's own; field numbers to be assigned in protos#14.

```proto
message WorkerHello {
  repeated uint32 protocol_versions = 1;   // every semantic version the SDK implements
  string sdk_name = 2;  string sdk_version = 3;  string language = 4;
  StepIdScheme legacy_step_ids = 5;         // deprecated by supported_step_ids
  repeated StepIdScheme supported_step_ids = 7;  // §8.3: every scheme it can replay; routing checks this
  Capabilities capabilities = 6;            // §10
}
message HelloAccepted { uint32 protocol_version = 1; Capabilities granted = 2; }

message RunActivation {
  bytes activation_id = 1;                  // opaque; echoed back; identifies this activation exactly once
  RunRef run = 2;                           // workflow_id, run_id, workflow_type, task_queue, namespace
  repeated Resolution resolutions = 3;      // always the full ordered list in v1 (§4)
  repeated QueryAsked queries = 4;          // answered after the handler has run
}

message Resolution {
  string step_id = 1;  google.protobuf.Timestamp at = 2;  bool replaying = 3;
  oneof what {
    RunStarted    started = 10;   // input, attempt, start time, scheme, lineage-derived seed, parent, headers
    StepSettled   step    = 11;   // Outcome of a task, closure/side effect or session step
    ChildSettled  child   = 12;   // Outcome of a child run
    TimerElapsed  timer   = 13;
    EventArrived  event   = 14;   // event_id, name, payload
    UpdateArrived update  = 15;   // update_id, name, args; delivered at the same wait in every SDK
    CancelAsked   cancel  = 16;   // reason, requested_at; already placed at its delivery wait (§5)
    ChildStarted  child_started = 17; // child workflow id and run id, before it settles: an unnamed child keeps the id history holds
  }
}
message Outcome {                          // one decoding for every SDK: no sentinel strings
  oneof result { bytes value = 1; Failure failure = 2; }
  End end = 3;                             // COMPLETED | FAILED | CANCELLED | TERMINATED | TIMED_OUT
  uint32 attempts = 4;
}
message Failure { string message = 1; string error_type = 2; bool non_retryable = 3;
                  ErrorCode code = 4; bytes details = 5; }   // structured code (#30)

message RunActivationResult {
  bytes activation_id = 1;
  oneof outcome {
    Commands commands = 2;                 // §5.3: every command, in issue order
    Failure workflow_failed = 3;           // user code failed (§7)
    ProtocolError protocol_error = 4;      // the SDK could not process the activation (§7)
  }
  repeated QueryAnswer queries = 5;  repeated UpdateAnswer updates = 6;
}
message Command {
  uint64 seq = 1;                          // call sequence: the order user code issued it
  bool satisfied = 2;                      // answered from history; payload fields omitted (§5.3)
  oneof what {
    RunTask run_task = 10;  StartTimer timer = 11;  CancelTimer cancel_timer = 12;
    StartChild child = 13;  CancelChild cancel_child = 14;  SendEvent send_event = 15;
    RecordValue record = 16;               // closures / side effects whose value must be journaled
    AwaitEvent await_event = 17;           // was WaitForEvent; no longer dropped
    CompleteRun complete = 18;  FailRun fail = 19;  EndCancelled cancelled = 20;
    RestartFresh restart = 21;  UpsertLabels labels = 22;
  }
}
message DeterminismViolation {             // kernel → SDK (for its error/log) and → engine (as the failure detail)
  RunRef run = 1;  uint64 seq = 2;  string step_id = 3;
  string expected = 4;  string actual = 5; // the compared fields, rendered (§8.2)
}
```

- **Commands say what, not ids.** `RunTask` carries type, queue, input, timeouts, retry policy and headers; the kernel assigns the step id (§8) and owns defaults and validation (#30), so the `?? 3` retry default and duration encoding live once.
- **`AwaitEvent` and `RecordValue` are first-class**, so waiting on an event and journaling a computed value are compared like everything else.
- **One failure shape everywhere**, replacing both sentinel markers; `Outcome.end` replaces the child-failure variants.

## 7. Activation lifecycle and failure classes

An activation is identified by `activation_id`, which today's activation token already is: the engine admits a completion against its fence and treats a repeat as already applied and a superseded one as stale (verified in the engine's completion path). The kernel keeps that as the only identity, so every retry below is safe against duplicates.

| result | meaning | kernel does | engine sees |
|---|---|---|---|
| `commands`, valid | new decisions | send them | completion |
| `commands`, violates history | **determinism violation** | send nothing; hold the run open and retry it with a growing, capped pause, logging the violation each time (today's behaviour, kept) | release; the run stays open. It is never failed for this: the fix is deploying code that replays its journal, and failing would lose the run |
| `workflow_failed` | user code failed | send as the workflow's failure | failure |
| `protocol_error` | SDK cannot process it (unknown resolution, decode error, version mismatch) | do not fail the run; release it so another worker can take it, and report loudly | release |
| no result (SDK crash, timeout) | **transient** | nothing was sent | the activation's claim times out and the run is handed out again |

Cases that must stay safe:
- **SDK dies after running user code, before returning.** Nothing was sent; the run is replayed from history by whoever gets it next. Commands are only ever sent once, by the kernel, after validation.
- **Kernel sends, then dies before telling the SDK.** In-process this is one process. For a sidecar or the engine, the activation id makes the resend idempotent (the fence answers "already applied").
- **A duplicate or late result** for an activation the kernel no longer holds is dropped with a log; it was either applied or superseded.
- **Queries and updates** are answered against the state after the handler has run on this activation's resolutions, so an answer always reflects a state the run actually reached.

## 8. Step identity and command comparison

### 8.1 Three separate things
- **Call sequence** (`Command.seq`): the order user code issued durable calls.
- **Step identity** (`step_id`): the stable key that matches a call to its recorded outcome; assigned by the kernel from the scheme (§8.3), the kind and the sequence.
- **Operation semantics**: the fields that make two calls the same operation.

A matching id at a matching position is not enough: a timer at the same position with a different duration is a different operation.

### 8.2 What is compared
The rule: **compare identity, never payloads.** A command is compared only on what makes it the same durable operation; inputs, results, headers, retry policies, queues and priorities are never compared. History is the truth for what already ran: if new code would compute a different input for a task that already completed, replay uses the recorded result. But a different task, or a timer of another length, at the same position is a different decision than the one recorded.

Comparison is a structured key per command, field by field, not a hash of the serialized command, so incidental metadata cannot cause false violations:

| kind | compared | not compared |
|---|---|---|
| `RunTask` | task type, step id | input, queue, timeouts, retry policy, headers (tracing), priority, cost |
| `StartTimer` | step id, duration | |
| `StartChild` | child workflow id, workflow type | input, options, headers |
| `AwaitEvent` | event name, timeout presence and duration | |
| `RecordValue` | step id | the value (it is replayed, not recomputed) |
| `SendEvent` | target, event name | payload |
| `CancelTimer` / `CancelChild` | target step | |
| terminal commands | kind | result payload |
| `UpsertLabels` | kind | the labels |

- The table is data in one place in the kernel, and changes only with a protocol version bump (§15), so comparison never shifts silently with a release.
- **Intended incompatible changes go through workflow versioning, not a looser table.** Runs stay bound to the release that started them, so an incompatible change ships as a new release while open runs finish on the old code.
- A `DeterminismViolation` names the field that differed, with expected and actual values, and says to ship incompatible changes as a new release.

### 8.3 Step-id schemes and history identity

**Runs already in flight carry the ids their SDK made**, and those differ (the §2 table). Changing the scheme under a running workflow breaks its replay, and some of those ids cannot be reproduced from the command stream at all: Rust names a task `{task_type}_{n}` from `execute_task` but `task_{n}` from `execute_task_with_options`, so the kernel, which sees only the command, cannot tell which one a run used.

**Schemes.** A scheme is a named, frozen rule for step ids. `KERNEL_V1` is the kernel's: the kernel assigns every id from the command's kind and call sequence, and a child's default workflow id from the history lineage (below) and the sequence, so concurrent parents never share one. `RUST_V0`, `PY_V0` and `TS_V0` are the SDKs' rules from before the kernel. Schemes are append-only: a change is a new scheme, never an edit, so every history replays under the rules that wrote it.

**Who supplies ids.**
- Under `KERNEL_V1`, the kernel assigns them.
- Under a legacy scheme, **the SDK supplies them**, from a small, frozen legacy-id module it keeps for as long as historical replay and reset are supported. The kernel does not reproduce legacy rules; it compares the ids the SDK supplies against history. The rest of each SDK's pre-kernel execution path is retired; the legacy-id module is not, because a reset or replay of an archived legacy history needs ids only that module can produce.
- Determinism validation is the kernel's in both cases, with the same full, ordered, field-by-field comparison (§8.2). Legacy ids do not weaken it: the task type and every other compared field are checked, so a changed task cannot hide behind an id format.

**Binding.** A run's scheme and its deterministic identity are bound **atomically at its first activation**, by the engine, from what the first worker to run it reports, and never change after:
- At creation the engine cannot know which worker will run the run; binding then would hand a kernel-scheme run to an older worker mid-upgrade. This is the rule workflow versioning already follows (bound on first claim, never re-bound).
- A run in flight before binding existed carries no marker. The first activation of it by a worker that reports schemes binds the legacy scheme that worker's SDK used, so from then on legacy runs are routed exactly too. A run never activated again stays unmarked and needs no routing.
- Task retries, worker migration, engine restarts and claim handovers never change the binding.

**History identity.** Two identities, kept distinct:
- The **physical history**: one run's journal, identified by its run id.
- The **history lineage**: the line of decisions the run's steps belong to. It is created with a fresh history and bound at first activation with the scheme; **reset copies it** (a reset run replays the decisions of the history it was reset from); restart-fresh and workflow-level retry start a fresh history and so a new lineage.

Everything a replay must reproduce derives from the lineage, never from the run id: default child workflow ids, and the deterministic random seed (`RunStarted.random_seed`). Deriving them from the run id would change them on reset, breaking the copied steps; deriving them from the workflow id alone would let a retry collide with a child a previous attempt left open.

**Routing.** The compatibility contract is **support for the run's recorded scheme**: the engine hands a run only to a worker that reports supporting its scheme (refuse, never fall through, as for versioning). Protocol version 2 is the initial proxy (a v2 worker supports `KERNEL_V1` plus its own legacy scheme), but workers report the list of schemes they support (`WorkerHello`), and that list is what routing checks. A worker never receives a run whose scheme it cannot replay.

**Retirement.** The kernel keeps legacy comparison indefinitely; it costs nothing beyond comparing supplied ids. Each SDK's legacy-id module stays for as long as historical replay and reset of legacy histories are supported, which is a support-policy decision, not tied to whether legacy runs are still open.

**Tested against real persisted histories, not only synthetic ones**: the replay corpus (recorded with `ORCHER_RECORD_ACTIVATIONS` from all three SDKs) checks that each legacy scheme's SDK-supplied ids compare clean against its own recorded histories, and that the `KERNEL_V1` rules are deterministic across languages.

Deterministic random: `KERNEL_V1` runs get the lineage-derived seed in `RunStarted` and draw from one generator the kernel specifies (a fixed algorithm, so every language produces the same values from the same seed and nothing needs journaling); each SDK keeps its own `random` API over it. Legacy runs keep their SDK's generator, inside its frozen legacy module. This also removes Rust's toolchain-dependent seed for new runs.

## 9. Caching: three separate optimizations

The measured cost (n² journal reads in the engine) does not need the SDK to keep anything alive.

1. **Kernel-side resolution** (v1, #26): resolution moves into Rust once, out of three languages.
2. **Kernel-side history cache + incremental fetch** (after orcher#307): the kernel keeps each run's resolved history; the engine routes the run's activations to that worker and sends only entries after the last one the kernel has; on a miss (restart, eviction, another worker) the engine sends full history. **This is invisible to the SDK**: it still gets the full resolution list and replays from the start. It removes the measured engine-side cost (n² rows read, data shipped, the activation size ceiling).
3. **SDK live instances** (later, optional, own design): keep the coroutine between activations and apply only new resolutions. It needs a resumable runtime in each SDK, which none has (§4), and defined behaviour on process loss, eviction mid-execution, cancellation and migration. Pursue only if measurements after step 2 show handler replay CPU matters; today it is mild (worker CPU 2.2 → 3.7 ms per step from 10 to 1,000 steps).

Measure each step separately (#41): journal entries read per activation, resolution CPU, bytes per activation, handler replay time, cache hit rate and memory per cached run, recovery cost after worker loss. Whatever is cached, a disagreement or loss always falls back to full history: correctness never depends on a cache hit.

## 10. Capabilities (outside the semantic core)

Negotiated in `WorkerHello`/`HelloAccepted`; each is optional, can ship in any order, and must never change a workflow's outcome.

- `binary_payloads`: payloads and messages as bytes across the boundary (#37).
- `reports_pressure`: the SDK sends `Pressure{loop_delay_ms, queue_depth}` (#39).
- Later: `max_cached_runs` (§9.3) if live instances are built.

Resource management (#33–#35, #39–#40) lives with the kernel's governor, not in the semantic messages:
- **Credits** are the unit of flow control for polls ("up to k") and the future stream. A credit is granted when a slot is free, consumed when work is claimed against it, returned when the work's report is sent or given up, and reclaimed by the engine when the worker's claim or lease expires (crash, disconnect). Nothing is lost by a crash: unreturned credits expire with the claims they were spent on.
- **Pressure is advisory**: CPU, memory and the SDK's signal inform whether to poll and how much; the kernel still enforces hard slot limits.
- **Priority** influences admission and dispatch order between runs and tasks. It never reorders operations within a run, and recorded history is never reordered.
- Order: capacity-aware polling first, then the shared budget across drivers, then priority admission.

## 11. Engine wire additions

Additive `orcher.v1` fields, each its own engine issue; none is needed for the first kernel step (v1 resolution works on today's full journal):

| need | for | engine issue |
|---|---|---|
| step results as real journal entries in order (stop appending synthetic `StepCompleted` after the journal) | correct ordering and time without heuristics; incremental history | orcher#307 |
| `history_after_entry_id` on the poll, `full_history` on the response | §9.2 | orcher#307 |
| route a run to the worker holding its history (the engine already has affinity leases internally) | cache hits | orcher#307, #292 |
| bind the step-id scheme and history lineage at first activation; route runs only to workers supporting their scheme | §8.3 | new |
| `max_items` on polls; batch claim | credits | orcher#308 |
| batched heartbeat with capacity report | §10 | orcher#309 |
| next activation in the completion response | one round trip per hot step | orcher#310 |
| priority on starts, tasks, children, actor calls | §10 | orcher#313 |
| a `RunActivation` payload on the poll response | engine-side resolution (§12) | orcher#315 |

## 12. Deployments

The same messages, different carriers:
- **In-process (default).** Rust uses the structs. napi/pyo3 pass the encoded message as one byte buffer each way per activation (no JSON, no number arrays).
- **Sidecar.** The kernel as a process; messages length-prefixed over a socket or stdio, for a language with no native module.
- **WASM.** The kernel as a component taking and returning the encoded messages.
- **Engine-side.** The engine runs the kernel's resolution and puts a `RunActivation` on the poll response, so a new language needs no Rust and serverless push fits. Possible because the protocol is serializable and never calls back. Cost is per-activation engine work: v1 keeps it possible; decide after orcher#315 measures it.

## 13. Conformance

The test that matters: **given the same recorded history and the same workflow code, every SDK produces the same commands, and the kernel rejects any stream that disagrees with history without sending anything.**

- Recorded-activation tests: feed the same `RunActivation`s to each SDK and compare its `RunActivationResult` byte for byte; no engine needed. Built **before** any SDK is ported.
- `scenarios.json` stays the end-to-end suite every implementation must pass (#42), plus the missing scenarios: saga compensation failure, changed child id on replay, falsy results, array and multi-argument inputs, cancellation at each kind of wait, event waits with and without timeouts, a task inserted before another kind of step, a changed timer duration (must be rejected).
- Legacy schemes replayed against the real persisted journals (§8.3).

## 14. Rollout

Each step is gated on every SDK suite, the conformance tests, an end-to-end run and the benchmark suite (#41).

1. **Messages** (protos#14), additive, minor release.
2. **Recorded-activation conformance harness** and the legacy-scheme replay check against real journals.
3. **Kernel resolution** (#26): the kernel builds `RunActivation` from today's full journal; SDKs opt in with protocol version 2 in `WorkerHello`. The raw-task path stays until all three SDKs have switched, then is removed in the same arc.
4. **Kernel step identity and full command comparison** (#27), with frozen legacy schemes.
5. **Binary boundary** (#37) as each SDK switches.
6. **Kernel history cache + incremental fetch** (§9.2), with orcher#307.
7. Governor items (§10) as their engine issues land.
8. SDK live instances (§9.3) only if measurements justify them.

## 15. Versioning

`protocol_version` already exists on the engine wire (0 = baseline, 1 = handles a cancel request; sent on Complete, Fail and registration). It stays **the one number** for what the worker's semantics are, and is extended: **2 = activations resolved by the kernel, kernel-assigned step ids for new runs, full command comparison.** The kernel reports to the engine the highest version it and the SDK both implement. A bump is required only for a change that alters what a replay produces, what the comparison accepts (§8.2), or what the engine must do for the worker. Capabilities (§10) are never versions.

## 16. Decisions taken and still open

Taken:
- Comparison: identity, never payloads; the §8.2 table as agreed; intended incompatible changes via workflow versioning.
- Satisfied commands: same `Command`, marked `satisfied`, payloads omitted (§5.3).
- Determinism violations hold the run, never fail it (§7; today's behaviour, verified).
- v1 is replay-based (§4); live instances deferred (§9.3).
- Caching starts kernel-side and is invisible to the SDK (§9.2).
- Task and actor work stay off this protocol in v1.
- Legacy runs: the SDK supplies step ids from a frozen legacy-id module, kept while historical replay and reset are supported; the kernel compares them (§8.3).
- Scheme and history lineage are bound atomically at first activation, preserved across reset, and route runs only to workers supporting the recorded scheme (§8.3).
- Engine-side resolution: kept possible, decided after measurement (§12).
- Queries are answered in the same activation, after the handler runs (§7).

Open:
1. **Surfacing a held run.** Determinism violations hold a run open indefinitely (§7, verified in `retry_non_deterministic`), visible only in worker logs. Proposed: report the violation to the engine with the release so the run shows as "held: not deterministic" with the detail, in the dashboard and `describe`. Needs one engine-wire field on release.
