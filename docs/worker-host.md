# Shared worker host (sdk-core#29): design

Status: agreed design for #29. Evidence: inventories of the sdk-py native module (2,148-line `worker.rs`), the sdk-ts native module (1,770-line `worker.rs`) and sdk-rust's `crates/orcher-sdk/src/worker`, all on main in October 2026.

## 1. What it is, and what it is not

**The host is one worker's plumbing, written once.** It assembles core's drivers from one config, runs the worker's lifecycle, hands work to the language and takes results back, tracks running tasks (heartbeats, cancellation), runs sessions, registers and deregisters the worker, and talks to the actor service. Today three copies of this exist: the Python bridge, the TypeScript bridge, and the Rust SDK's `worker/`.

**It is not the governor.** The governor (`sdk-core/src/worker/governor`) decides *how much work the worker takes*; it stays in core, and the host uses it through the drivers. **It is not the kernel.** Resolution, step identity and determinism stay in `sdk-core/src/kernel`; the host carries their messages.

```
sdk-core repo (workspace)
├── orcher-sdk-core   kernel (resolution, step ids, random) + worker (drivers, poll, governor)
└── orcher-sdk-host   one worker: config → drivers, lifecycle, work hand-off, running tasks,
                      sessions, registration, actor service, shutdown/drain
        ▲                       ▲                        ▲
   sdk-rust (direct)      sdk-ts (Neon binding)     sdk-py (PyO3 binding)
```

**Rust uses it too.** The Rust SDK wires core's drivers itself today, so it is the third copy; the host's native API is a Rust API, and the Python and TypeScript modules become thin bindings over the same API. One implementation, three users.

## 2. Evidence: the same layer, three times, with bugs in each

Identical policy, retyped three times: poll timeouts 60/60/30 s, workflow cache 1000, `strict_determinism=false`, `protocol_version = CANCEL_REQUEST`, task `auto_heartbeat` with a 30 s interval, a 10 s driver stop timeout. None of the three uses core's `NeededDrivers`; the workflow and task drivers always start.

Bugs found in this layer (each a host responsibility the host fixes once):

| bug | py | ts | rust |
|---|---|---|---|
| worker never deregisters on shutdown | | ✗ | |
| actor driver never stopped (keeps polling, claimed operations stranded) | ✗ | ✗ | aborted |
| actor RPCs without the worker's TLS / API key / organization | partly (own interceptor) | ✗ | ✗ (new connection per operation) |
| shutdown does not drain in-flight work (results sent after the drivers stop are lost; work stays claimed until timeout) | | | ✗ |
| an activation that fails conversion is dropped and stays claimed until timeout | ✗ | ✗ | |
| session heartbeats never sent; sessions not closed on shutdown | | | ✗ |
| a task's own error reported as "Internal SDK error" | | | ✗ |
| workflow-activation and actor failures drop the error type the language gave | ✗ | | |
| zero pollers panics (`slot % 0`) | ✗ | | |
| identity regenerated on each call when unset | | ✗ | |
| one full slot channel blocks distribution to every slot | ✗ | ✗ | |
| `block_on` while holding the GIL; two tokio runtimes | ✗ | | |
| worker/actor errors untagged; language matches message text | | ✗ | |
| actor re-registration on `ReRegistrationRequired` | ✓ | logged only | ✓ (metadata differs) |
| dead config (poll intervals, actor concurrency, force-shutdown timeout) | | ✗ | ✗ |

Sessions are split three ways: Rust runs them natively; Python and TypeScript keep session logic in the language and only add or remove queues through the bridge.

## 3. Principles

1. **Pull, never callback.** The language asks the host for the next piece of work and returns its result; the host never calls into the language. This is what works the same over Rust async, a Node promise, a Python awaitable, a sidecar socket and WASM, and it is the kernel protocol's rule (one round trip, no callbacks).
2. **A pull is a credit.** The language pulls only when it has room to run something, so a pending pull is exactly the governor's unit of capacity (#33): the host claims work from the engine only against outstanding pulls. Per-slot channels and fan-out distributors go away, and with them the hoarding (claimed work buffered behind a busy language) and the one-full-slot-blocks-all bug.
3. **Messages at the edge are the protocol's.** Workflow activations cross as the kernel protocol's `RunActivation` / `RunActivationResult` (protobuf bytes over FFI, structs in Rust). Tasks and actor operations, which the kernel protocol leaves out in v1, cross as host messages defined the same way (protobuf, §5), so the boundary is binary everywhere (#37) and a sidecar carries the same bytes.
4. **One config, one set of defaults.** The host owns every default listed in §2, the stable identity (generated once), TLS normalization, credentials for every connection it opens, and registration metadata (the binding supplies the SDK name and version).
5. **Nothing claimed is stranded.** Every piece of work the host takes is either handed to the language, completed, failed, or released back to the engine at once (conversion failure, shutdown), never left to a claim timeout.

## 4. The host API (Rust, which the bindings mirror)

```rust
let host = Host::start(HostConfig { … }, Registrations { workflow_types, task_types, actor_types }).await?;
// decides drivers with NeededDrivers; refuses to start when nothing is registered

host.next_activation().await -> Option<RunActivation>          // None: draining, no more
host.complete_activation(RunActivationResult).await?

host.next_task().await      -> Option<TaskAssignment>
host.heartbeat(&token, details) -> HeartbeatAnswer             // { cancel_requested }
host.task_cancelled(&token).await -> bool                       // true: cancel; false: already reported
host.complete_task(&token, TaskOutcome).await?                  // value | TaskFailure (type, non-retryable)

host.next_actor_operation().await -> Option<ActorOperation>
host.complete_actor_operation(…).await?
host.actor_state(…)                                             // get/set/delete/list, on the host's channel

host.report_pressure(Pressure { loop_delay_ms, queue_depth })   // the language's signal for the governor (#39)
host.metrics() -> WorkerMetrics

host.shutdown(Drain { grace }).await                            // §6
```

- Sessions are the host's: the built-in create/complete session tasks are handled inside the host, never surfaced to the language; a task on a session queue reaches the language like any other task.
- Errors cross with core's `ErrorCode` (#61), never as text the language parses.
- `next_*` returning `None` is the only end-of-work signal; there is no 30-second empty wake-up (the bindings cancel a pending pull instead).

## 5. Host messages for tasks and actors

A small protobuf package beside the kernel protocol, `orcher.host.v1` (in protos), carrying what crosses for work the kernel protocol does not cover: `TaskAssignment` (token, task type, run, input payloads, attempt, timeouts), `TaskOutcome` (value or failure), `HeartbeatAnswer`, `ActorOperation` / `ActorResult`. Kept out of `orcher.kernel.v1` so the kernel protocol stays the semantic contract, as agreed; carried the same way.

## 6. Shutdown and drain, once

1. Stop claiming: drivers stop polling; pending pulls stay open.
2. Hand over what was already claimed; the language finishes it within the grace period.
3. When the grace ends: release every claimed-but-unfinished workflow activation and actor operation to the engine; fail nothing silently.
4. Close sessions; stop the actor driver; deregister the worker (bounded wait).
5. `next_*` return `None`; the bindings end their loops.

## 7. Rollout

Each SDK switches in one release, with no period where its old wiring and the host both run.

1. **Host crate** in the sdk-core workspace, with protos `orcher.host.v1`. Tested against a live engine and the recorded corpus; the bugs in §2 each get a test.
2. **sdk-rust** switches first (no FFI): deletes its driver wiring, keeps its macros, contexts and handler model. Gate: its suite, the contract suite, the corpus, the benchmark.
3. **sdk-ts**, then **sdk-py**: their native modules become bindings over the host (target: a few hundred lines each, from ~1,800 and ~2,100), and switching to the host is also when each adopts the kernel protocol for workflows (§3.3), so they migrate once, not twice.
4. Then the governor work (#33, #39, #40) lands in core and reaches every SDK through the host.

## 8. Decisions

1. **Rust uses the host**: otherwise the Rust copy stays and drifts.
2. **Pull-based API, a pull is a credit**: it is the only shape that works across every deployment, and it removes the hoarding and fan-out bugs.
3. **Host messages as a separate `orcher.host.v1` package** rather than adding tasks and actors to the kernel protocol.
4. **Sessions move into the host** for all three SDKs: Python and TypeScript stop implementing session semantics.
5. **Migrate to the host and the kernel protocol in one step per SDK**, rather than two migrations.
6. **Crate in the sdk-core workspace, versioned with core**: the host and core change together.
