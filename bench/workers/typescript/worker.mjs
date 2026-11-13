// The bench's workloads on the TypeScript SDK.
//
// The suite starts this as a measured worker (`bench/run.sh --worker
// typescript`): it runs the same workflow (`bench.program`) and task
// (`bench.task`) as the Rust worker in bench/src/worker.rs, written against
// the SDK's public API the way a user writes them, so what the suite measures
// is what a TypeScript user's worker costs: the SDK, its native module and
// core together.
//
// It is driven like the Rust worker, over stdin and stdout: it prints
// `BENCH ready <snapshot>` once it is serving, answers `snapshot` with
// `BENCH snapshot <snapshot>`, and on `stop` shuts the worker down and prints
// `BENCH stopped <snapshot>`. A snapshot carries what this process can count:
// its CPU time and peak memory, runs ended and tasks run. The SDK does not
// say how many activations it ran; the suite counts those at its proxy.

import { createRequire } from 'node:module';
import { createInterface } from 'node:readline';

const require = createRequire(import.meta.url);
const { Duration, Worker, task, workflow } = require('@orcher/sdk');

// As in bench/src/program.rs.
const WORKFLOW_TYPE = 'bench.program';
const TASK_TYPE = 'bench.task';
const EVENT_NAME = 'bench.ping';

const counters = {
  runs_ended: 0,
  tasks_started: 0,
  tasks_completed: 0,
  heartbeats_recorded: 0,
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Runs for `sleep_ms`, heartbeating every `heartbeat_every_ms` if set, and
 * returns a string of `result_bytes` bytes. */
const benchTask = task({
  name: TASK_TYPE,
  execute: async (ctx, spec = {}) => {
    counters.tasks_started += 1;
    let remaining = spec.sleep_ms ?? 0;
    const every = spec.heartbeat_every_ms ?? 0;
    if (every > 0 && remaining > 0) {
      while (remaining > 0) {
        const step = Math.min(every, remaining);
        await sleep(step);
        remaining -= step;
        await ctx.heartbeat();
        counters.heartbeats_recorded += 1;
      }
    } else if (remaining > 0) {
      await sleep(remaining);
    }
    counters.tasks_completed += 1;
    return 'x'.repeat(spec.result_bytes ?? 0);
  },
});

const taskOptions = (spec) =>
  spec.heartbeat_timeout_ms ? { heartbeatTimeout: spec.heartbeat_timeout_ms } : undefined;

/** The program its input names, as `Program` in bench/src/program.rs. */
const benchProgram = workflow({
  name: WORKFLOW_TYPE,
  run: async (ctx, program) => {
    const spec = program.task ?? {};
    const n = program.n ?? 0;
    switch (program.shape) {
      case 'sequence':
        for (let i = 0; i < n; i += 1) await ctx.executeTask(benchTask, spec, taskOptions(spec));
        break;
      case 'fan_out':
        await Promise.all(
          Array.from({ length: n }, () => ctx.executeTask(benchTask, spec, taskOptions(spec)))
        );
        break;
      case 'timers':
        for (let i = 0; i < n; i += 1) await ctx.sleep(Duration.fromMilliseconds(program.ms ?? 0));
        break;
      case 'events':
        for (let i = 0; i < n; i += 1) await ctx.waitForEvent(EVENT_NAME);
        break;
      default:
        throw new Error(`bad program: unknown shape ${program.shape}`);
    }
    counters.runs_ended += 1;
    return { ok: true };
  },
});

function snapshot() {
  const cpu = process.cpuUsage();
  return {
    process: {
      cpu_user_us: cpu.user,
      cpu_system_us: cpu.system,
      // In kilobytes.
      max_rss_bytes: process.resourceUsage().maxRSS * 1024,
    },
    ...counters,
  };
}

function say(kind, value = snapshot()) {
  process.stdout.write(`BENCH ${kind} ${JSON.stringify(value)}\n`);
}

const setup = JSON.parse(process.argv[2]);
const worker = new Worker({
  serverUrl: setup.server_url,
  namespace: 'default',
  taskQueue: setup.task_queue,
  workflows: setup.workflows ? [benchProgram] : [],
  tasks: setup.tasks ? [benchTask] : [],
  maxConcurrentWorkflows: Math.max(1, setup.workflow_concurrency),
  maxConcurrentTasks: Math.max(1, setup.task_concurrency),
});
await worker.run();
say('ready');

const lines = createInterface({ input: process.stdin });
let stopping = false;
const stop = async () => {
  if (stopping) return;
  stopping = true;
  const before = snapshot();
  await worker.shutdown();
  say('stopped', before);
  process.exit(0);
};
lines.on('line', (line) => {
  const command = line.trim();
  if (command === 'snapshot') say('snapshot');
  else if (command === 'stop') stop();
});
lines.on('close', stop);
