"""
queue.py — enqueueing jobs and safely dequeueing them for workers.

The core trick for concurrency-safe dequeue is Postgres's
    SELECT ... FOR UPDATE SKIP LOCKED
This locks the row(s) it picks, and any other transaction running
the same query at the same time will simply skip past rows that
are already locked instead of blocking and waiting. That means
N workers can poll in parallel and never double-process a job,
with no extra coordination service (no Redis, no Zookeeper) needed.

Docs on this pattern: https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE
"""

import json
from datetime import datetime, timezone
from src.db import pool


def enqueue(
    task_name: str,
    payload: dict | None = None,
    *,
    queue: str = "default",
    priority: int = 100,
    run_at: datetime | None = None,
    max_attempts: int = 3,
    workflow_id: int | None = None,
    status: str = "pending",
) -> int:
    """
    Insert a new job row. Returns the new job's id.

    priority: lower number = runs first (like Unix 'nice').
    run_at: leave as None to run ASAP, or pass a future datetime to
            delay/schedule the job.
    status: normally 'pending'. DAG child jobs get inserted as
            'waiting' until their parent jobs succeed (see dag.py).
    """
    run_at = run_at or datetime.now(timezone.utc)
    with pool.connection() as conn:
        row = conn.execute(
            """
            INSERT INTO jobs
                (task_name, payload, queue, priority, run_at,
                 max_attempts, workflow_id, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                task_name,
                json.dumps(payload or {}),
                queue,
                priority,
                run_at,
                max_attempts,
                workflow_id,
                status,
            ),
        ).fetchone()
        return row[0]


def dequeue_one(queue: str, worker_id: str) -> dict | None:
    """
    Atomically claim the single best next job for this worker, or
    return None if nothing is ready.

    "Best" = lowest priority number, then oldest run_at, among jobs
    that are pending and whose run_at has arrived (so scheduled /
    delayed jobs stay invisible until their time comes).

    This whole thing runs as ONE transaction:
      1. find the best candidate row and lock it (SKIP LOCKED means
         we never fight another worker for the same row)
      2. flip it to 'running' and stamp who owns it
      3. commit -- the row is now safely "ours"
    """
    with pool.connection() as conn:
        row = conn.execute(
            """
            UPDATE jobs
            SET status = 'running',
                locked_by = %(worker_id)s,
                locked_at = now(),
                started_at = now(),
                attempts = attempts + 1
            WHERE id = (
                SELECT id FROM jobs
                WHERE status = 'pending'
                  AND queue = %(queue)s
                  AND run_at <= now()
                ORDER BY priority ASC, run_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, task_name, payload, attempts, max_attempts, workflow_id
            """,
            {"queue": queue, "worker_id": worker_id},
        ).fetchone()

        if row is None:
            return None

        return {
            "id": row[0],
            "task_name": row[1],
            "payload": row[2],
            "attempts": row[3],
            "max_attempts": row[4],
            "workflow_id": row[5],
        }


def mark_succeeded(job_id: int) -> None:
    with pool.connection() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET status = 'succeeded', finished_at = now()
            WHERE id = %s
            """,
            (job_id,),
        )