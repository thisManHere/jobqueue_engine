"""
worker.py — the process you run (possibly many copies of!) to
actually execute jobs.

Loop, forever:
  1. ask the queue for the next job (dequeue_one)
  2. if there isn't one, sleep briefly and try again (polling)
  3. if there is one, look up its handler function by task_name
     and call it with the job's payload
  4. on success -> mark_succeeded, then let the DAG resolver
     unlock any child jobs that were waiting on this one
  5. on exception -> mark_failed, which schedules a retry or
     gives up permanently depending on attempts left

You can run several worker processes (even on different machines,
as long as they point at the same Postgres) for more throughput --
SKIP LOCKED guarantees they never step on each other.
"""

import socket
import time
import traceback
import uuid

from src.db import pool, init_pool
from src.queue import dequeue_one, mark_succeeded
from src.retry import mark_failed
from src.registry import get_task
from src.dag import unlock_children


def run_worker(queue: str = "default", poll_interval: float = 1.0, max_loops: int | None = None):
    worker_id = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
    init_pool()
    print(f"[worker {worker_id}] started, watching queue='{queue}'")

    loops = 0
    while max_loops is None or loops < max_loops:
        loops += 1
        job = dequeue_one(queue, worker_id)

        if job is None:
            time.sleep(poll_interval)
            continue

        print(f"[worker {worker_id}] picked up job {job['id']} ({job['task_name']}), "
              f"attempt {job['attempts']}/{job['max_attempts']}")

        try:
            handler = get_task(job["task_name"])
            handler(job["payload"])
        except Exception as exc:
            error_text = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()}"
            mark_failed(job["id"], job["attempts"], job["max_attempts"], error_text)
            print(f"[worker {worker_id}] job {job['id']} FAILED (attempt "
                  f"{job['attempts']}/{job['max_attempts']}): {exc}")
        else:
            mark_succeeded(job["id"])
            unlock_children(job["id"])
            print(f"[worker {worker_id}] job {job['id']} SUCCEEDED")

    print(f"[worker {worker_id}] stopping (max_loops reached)")


if __name__ == "__main__":
    run_worker()