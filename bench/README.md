# Benchmark suite

An on-demand suite that measures what a worker built on this checkout's
`orcher-sdk-core` and `orcher-sdk-host`, and the engine it talks to, spend on
a fixed set of workloads. It exists so that each kernel and governor change
(the worker cache #36, capacity-aware polling #33, batched heartbeats #35,
binary encoding #37, a shared connection #38) lands with before and after
numbers (#41).

It measures resources, not speed: Postgres work from `pg_stat_statements`
and the table statistics, RPCs counted on the wire, and the worker's CPU
time and allocations. Wall-clock time is recorded only to compute rates; a
laptop's timings are not compared. Every scenario reports its success rate
next to its numbers, and a scenario where not every run succeeded says so in
the summary.

It is not run in CI: it needs Docker and takes a while. CI builds it and runs
its unit tests (it is a workspace member, `publish = false`), so it keeps up
with the core and host APIs it uses.

## Running it

```bash
bench/run.sh my-change                       # every scenario, engine :latest
bench/run.sh my-change --only sequential-100,heartbeat-100
bench/run.sh my-change --engine-version 0.5.5
bench/run.sh my-change-shared --upstream shared   # see "#38" below
bench/compare.sh bench/results/baseline-2026-10-18.json bench/results/my-change.json
```

On a hosted runner, which is quieter and more repeatable than a laptop
shared with other work, run the `Benchmark` workflow
(`.github/workflows/bench.yml`) from the Actions tab or with
`gh workflow run bench.yml -f label=my-change [-f only=a,b] [-f engine_version=0.5.5]`;
the results are the run's artifact. The committed baseline was made this
way; compare it only with runs made on the same kind of runner.

`run.sh` needs `docker`, `curl` and `cargo`. It starts Postgres 17 with
`pg_stat_statements` and the engine image
`ghcr.io/orcher-io/orcher:<version>` (default `latest`, the latest release)
on a Docker network of their own, publishes them on free localhost ports,
gives the engine every database setting explicitly, and removes the
containers, their volumes and the network when it ends, however it ends. It
never touches a database or engine already running on the machine. It then
builds `orcher-bench` in release mode from this checkout and writes
`bench/results/<label>.json` (everything measured) and
`bench/results/<label>.md` (a summary), after every scenario, so a run
stopped half-way keeps what it measured.

`orcher-bench list` names every scenario. Set `RUST_LOG` (for example `RUST_LOG=orcher_sdk_core=warn`) to see the measured workers' logs on stderr. A full run takes from tens of
minutes to a few hours depending on the machine, most of it in
`sequential-1000` and `fanout-10000`, whose cost is the point.

To compare a change, run the suite on main and on the change, on the same
machine and engine version, and compare the two files. Keep machine load
low: `load_average_at_start` is recorded in each result. Results are
counts, so they are much steadier than timings, but an overloaded engine
times out claims and polls, and retries change the counts; check
`rpc_errors` and the success rate before reading a difference.

## How it measures

```
orcher-bench suite ──client──────────────────────────────► engine ──► Postgres 17
     │                                                       ▲        (pg_stat_statements,
     │ spawns, drives over stdin/stdout                      │         pg_stat_user_tables)
     ▼                                                       │
orcher-bench worker ──gRPC──► counting proxy (in the suite) ─┘
 (this checkout's host,
  workloads as its "language")
```

- **The worker** is a separate process running `orcher_sdk_host::Host` with
  the workloads' code as its language, exactly as a language SDK uses the
  host: it pulls activations (`RunActivation`) and answers with the kernel
  protocol's commands, and pulls tasks and reports their outcomes. Being its
  own process, its CPU time (`getrusage`), allocations (a counting global
  allocator) and memory are the worker's alone. A change to the kernel,
  host, drivers or governor is therefore what the worker numbers measure.
  The workflow code replays from the start on every activation (kernel
  protocol v1, §4) and supplies `KERNEL_V1` step ids (`task-3`, `timer-4`).
- **The proxy** sits between the worker and the engine and reads every RPC
  whole: per method it counts calls, bytes each way and non-OK statuses, and
  from the responses the activations and tasks claimed (eager tasks
  included) and the journal entries each activation carried. It also counts
  the worker's connections and RPCs in flight. Every RPC the worker makes is
  unary, so nothing is streamed.
- **Postgres** statistics are reset once the worker has registered and read
  when the workload ends. Statements are grouped into classes by verb and
  first table (`select workflow_execution_journal`), which survives the
  engine's SQL changing; the eight costliest statements are kept with their
  text. A statement that starts with a `WITH` is classed by the first verb
  in it.
- **Sampling**: five times a second the suite asks the worker for its
  counters, to find the work claimed from the engine but not yet handed to
  the language, and the peaks of heap and tokio tasks. The sampling's own
  cost is a few allocations per sample in the worker.

Each scenario runs on a fresh task queue with a fresh worker (100
workflow and 100 task slots, 4 pollers each: the host's defaults), against
one engine and database for the whole run, in a fixed order.

## Workloads

| scenario | what runs | what it is for |
|---|---|---|
| `idle` | a worker with a workflow and a task type registered, nothing to do, 10 s | the cost of waiting: polls, heartbeats, CPU, heap, tokio tasks, connections |
| `sequential-10/100/300/1000` | one run of n no-op tasks in sequence | the O(n²) journal reads: every activation reads and ships the whole journal (#36) |
| `payload-1k/10k/100k/1m` | one run of 5 tasks in sequence, each returning a result of that size | boundary encoding and payload shipping per activation (#37) |
| `timers` | 20 runs of 10 timers of 1 s in sequence | timer-heavy runs: activations with no task. The timers are 1 s, not shorter, because on engine 0.5.5 a timer shorter than the engine's delay in storing it fails the completion (orcher-io/orcher#375) |
| `events` | 20 runs each waiting for 20 events in sequence; an event is sent to each run every 50 ms until it ends | event-heavy runs. How many events one activation picks up depends on timing, so `events_sent` is recorded; events are sent until the run ends because on engine 0.5.5 an event that arrives before the run waits for it does not wake the run (orcher-io/orcher#376) |
| `fanout-1000`, `fanout-10000` | one run issuing n no-op tasks at once and waiting for all | one run's activations as its tasks complete; eager dispatch; claims in bulk (#33) |
| `concurrent-100x10` | 100 runs at once of 10 tasks each | many runs at once against 100 slots |
| `footprint-task-worker` | a task-only worker, 10 s idle, then 1,000 tasks of 20 ms through 100 slots | a task-only worker's footprint idle and busy, and claimed-but-not-started tasks (#33) |
| `heartbeat-100/1000` | k tasks running at once for 15 s, heartbeat timeout 3 s, on a task-only worker | heartbeat RPC rate and Postgres writes per heartbeat against concurrency (#35) |

In the footprint and heartbeat scenarios the runs' workflows are run by a
second worker that is not measured (its RPCs are recorded as `helper_rpc`, for diagnosis); the
measured worker runs only tasks. Elsewhere one worker runs both, as an
application's worker usually does, so the worker's CPU and allocations per
activation include its tasks' share (the tasks are no-ops).

## Metrics

Every scenario's `derived` numbers are what `compare.sh` compares; the rest
of its JSON is the raw material (per-method RPCs, per-class and per-table
Postgres numbers, the top statements, the phase's worker cost).

| metric | meaning |
|---|---|
| `success_rate` | runs completed / runs started. Never read a number without it. |
| `activations`, `tasks` | workflow activations and tasks the worker's language ran |
| `rpc_calls`, `rpc_per_activation`, `rpc_per_task` | RPCs the worker made, in all and per unit of work |
| `rpc_errors` | RPCs that ended with a non-OK status (engine timeouts under load show here) |
| `empty_polls` | polls that came back with no work |
| `worker_connections` | the most connections the worker had open to the engine (#38) |
| `journal_rows_read`, `journal_blocks` | rows returned by, and shared buffers touched by, statements reading `workflow_execution_journal` |
| `journal_rows_per_activation` | the same per activation: grows with n while every activation is the whole journal |
| `pg_rows`, `pg_blocks`, `pg_calls`, `pg_wal_bytes` | every statement the engine ran: rows returned or written, shared buffers hit + read, calls, WAL |
| `pg_write_rows` | rows written by insert, update and delete statements |
| `pg_blocks_per_activation`, `pg_wal_bytes_per_activation` | Postgres work per activation |
| `activation_bytes` | bytes of a workflow poll response that carried an activation, on average |
| `journal_entries_per_activation` | journal entries an activation carried, on average |
| `worker_cpu_us_per_activation`, `worker_allocs_per_activation`, `worker_alloc_bytes_per_activation` | the worker's CPU time and allocations per activation (including its tasks' share where it runs both) |
| `worker_cpu_us_per_task`, `worker_allocs_per_task` | the same per task |
| `worker_peak_heap_bytes`, `worker_peak_tokio_tasks` | the most live heap and tokio tasks sampled |
| `claimed_waiting_peak_tasks`, `claimed_waiting_peak_activations` | the most work sampled that the engine had handed the worker and its language had not started: claimed and waiting in the worker's buffers (#33) |
| `heartbeat_rpcs`, `heartbeat_rpcs_per_s` | `RecordTaskHeartbeat` calls, in all and per second (#35) |
| `task_updates_per_heartbeat` | `update task_executions` statements per heartbeat RPC: the Postgres writes heartbeats cost (#35) |
| `idle_rpc_per_s`, `idle_cpu_us_per_s`, `idle_heap_bytes`, `idle_tokio_tasks` | the idle phase of `idle` and `footprint-task-worker` |

## What each lever should move

- **#36 worker cache and incremental activations**: `journal_rows_per_activation`,
  `journal_entries_per_activation` and `activation_bytes` in `sequential-*`
  and `fanout-*` should stop growing with n; `pg_blocks_per_activation`
  should fall with them.
- **#37 binary encoding**: `activation_bytes` in `payload-*`, and the worker's
  CPU and allocations per activation.
- **#33 capacity-aware polling**: `claimed_waiting_*` and `empty_polls` in
  `fanout-*`, `concurrent-100x10` and `footprint-task-worker`; `idle_rpc_per_s`.
- **#35 batched heartbeats**: `heartbeat_rpcs_per_s` and
  `task_updates_per_heartbeat` in `heartbeat-*`; today both scale with k.
- **#38 shared connection**: see below.

## #38: one connection per driver, or one shared

Each driver builds its own channel, and the registration driver its own, so
the worker opens several connections (`worker_connections`). Sharing one in
the worker needs a change to the drivers, which this suite does not make. It
can try the engine's side without one: `--upstream shared` makes the proxy
carry every worker connection over one engine connection, so every long
poll, completion and heartbeat shares one HTTP/2 connection's flow control
into the engine, while `--upstream mirror` (the default) opens one engine
connection per worker connection. Comparing a run of each, for example
`--only sequential-100,concurrent-100x10,heartbeat-1000`, shows whether
sharing starves anything (success rate, errors, claimed-but-waiting work)
on the engine's side. It does not show the worker's side of sharing (the
worker-to-proxy hop stays one connection per driver), and the engine's
connection count across a fleet is `worker_connections` times the fleet.
Measuring the worker's side needs a hook that builds every driver from one
channel; when the drivers have one, run the suite with it on and off.

## Not measured yet

- **Engine resolution (orcher#315), eager versus polled activations
  (orcher#310) and cache hit versus miss (#36)**: there is nothing to switch
  until those land; the numbers above are what they should move.
- **Per-SDK payload sweeps**: the suite drives the Rust host only. The
  Python and TypeScript bindings will cross the same host, so the same
  scenarios run through a binding would add the binding's own encoding cost.
- **Engine CPU**: the engine runs in Docker, where its CPU time depends on
  the host; its Postgres work is what is compared. Its `/metrics` endpoint
  has no per-RPC counters in 0.5.5, which is why the proxy counts them.
