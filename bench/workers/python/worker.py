"""The bench's workloads on the Python SDK.

The suite starts this as a measured worker (``bench/run.sh --worker python``):
it runs the same workflow (``bench.program``) and task (``bench.task``) as the
Rust worker in ``bench/src/worker.rs``, written against the SDK's public API
the way a user writes them, so what the suite measures is what a Python
user's worker costs — the SDK, its native module and core together.

It is driven like the Rust worker, over stdin and stdout: it prints
``BENCH ready <snapshot>`` once it is serving, answers ``snapshot`` with
``BENCH snapshot <snapshot>``, and on ``stop`` shuts the worker down and
prints ``BENCH stopped <snapshot>``. A snapshot carries what this process can
count: its CPU time and peak memory, runs ended and tasks run. The SDK does
not say how many activations it ran; the suite counts those at its proxy.
"""

from __future__ import annotations

import asyncio
import json
import os
import resource
import sys
from datetime import timedelta
from typing import Any

from orcher import TaskContext, Worker, WorkflowContext, task, workflow

# As in bench/src/program.rs.
WORKFLOW_TYPE = "bench.program"
TASK_TYPE = "bench.task"
EVENT_NAME = "bench.ping"

COUNTERS = {
    "runs_ended": 0,
    "tasks_started": 0,
    "tasks_completed": 0,
    "heartbeats_recorded": 0,
}


@task(name=TASK_TYPE)
async def bench_task(
    ctx: TaskContext,
    sleep_ms: int = 0,
    result_bytes: int = 0,
    heartbeat_every_ms: int = 0,
    heartbeat_timeout_ms: int = 0,
    task_queue: str = "",
) -> str:
    """Runs for ``sleep_ms``, heartbeating every ``heartbeat_every_ms`` if
    set, and returns a string of ``result_bytes`` bytes."""
    COUNTERS["tasks_started"] += 1
    remaining = sleep_ms / 1000
    if heartbeat_every_ms > 0 and remaining > 0:
        every = heartbeat_every_ms / 1000
        while remaining > 0:
            step = min(every, remaining)
            await asyncio.sleep(step)
            remaining -= step
            await ctx.heartbeat()
            COUNTERS["heartbeats_recorded"] += 1
    elif remaining > 0:
        await asyncio.sleep(remaining)
    COUNTERS["tasks_completed"] += 1
    return "x" * result_bytes


def _task_options(spec: dict[str, Any]) -> dict[str, Any]:
    timeout_ms = spec.get("heartbeat_timeout_ms") or 0
    return (
        {"heartbeat_timeout": timedelta(milliseconds=timeout_ms)} if timeout_ms else {}
    )


@workflow(name=WORKFLOW_TYPE)
async def bench_program(
    ctx: WorkflowContext,
    shape: str,
    n: int = 0,
    task: dict[str, Any] | None = None,
    ms: int = 0,
) -> dict:
    """The program its input names, as ``Program`` in bench/src/program.rs."""
    spec = task or {}
    if shape == "sequence":
        for _ in range(n):
            await ctx.execute_task(bench_task, **_task_options(spec), **spec)
    elif shape == "fan_out":
        await asyncio.gather(
            *(
                ctx.execute_task(bench_task, **_task_options(spec), **spec)
                for _ in range(n)
            )
        )
    elif shape == "timers":
        for _ in range(n):
            await ctx.sleep(timedelta(milliseconds=ms))
    elif shape == "events":
        for _ in range(n):
            await ctx.wait_for_event(EVENT_NAME)
    else:
        raise ValueError(f"bad program: unknown shape {shape!r}")
    COUNTERS["runs_ended"] += 1
    return {"ok": True}


def snapshot() -> dict[str, Any]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    # ru_maxrss is in kilobytes on Linux and in bytes on macOS.
    max_rss = usage.ru_maxrss * (1 if sys.platform == "darwin" else 1024)
    return {
        "process": {
            "cpu_user_us": int(usage.ru_utime * 1_000_000),
            "cpu_system_us": int(usage.ru_stime * 1_000_000),
            "max_rss_bytes": max_rss,
        },
        **COUNTERS,
    }


def say(kind: str) -> None:
    sys.stdout.write(f"BENCH {kind} {json.dumps(snapshot())}\n")
    sys.stdout.flush()


async def main(setup: dict[str, Any]) -> None:
    worker = (
        Worker.builder()
        .server_url(setup["server_url"])
        .namespace("default")
        .task_queue(setup["task_queue"])
        .max_concurrent_workflows(max(1, setup["workflow_concurrency"]))
        .max_concurrent_tasks(max(1, setup["task_concurrency"]))
        .build()
    )
    # The SDK registers every decorated function it finds; a worker set up
    # for only one kind serves only that kind.
    if not setup["tasks"]:
        worker._task_handlers.clear()
    if not setup["workflows"]:
        worker._workflow_handlers.clear()
    running = asyncio.create_task(worker.run())
    # Give it a moment to connect and start polling, as the Rust worker
    # reports ready once its host has started.
    await asyncio.sleep(0.5)
    if running.done():
        running.result()
    say("ready")

    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        command = line.strip()
        if command == "snapshot":
            say("snapshot")
        elif command == "stop" or line == "":
            break
    before = snapshot()
    await worker.shutdown()
    sys.stdout.write(f"BENCH stopped {json.dumps(before)}\n")
    sys.stdout.flush()


if __name__ == "__main__":
    asyncio.run(main(json.loads(sys.argv[1])))
    # The executor thread reading stdin would hold the interpreter open.
    os._exit(0)
