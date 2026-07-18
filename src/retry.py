"""
retry.py — what happens when a job's handler raises an exception.

Strategy: exponential backoff with a cap, plus jitter.
  attempt 1 fails -> retry in  ~2s
  attempt 2 fails -> retry in  ~4s
  attempt 3 fails -> retry in  ~8s
  ... capped at max_backoff_seconds

Jitter (a small random offset) matters at scale: if 1000 jobs all
failed at the same instant (e.g. a downstream API went down), you
don't want all 1000 retrying at the exact same millisecond and
hammering it again. Spreading them out avoids that "thundering herd".
"""

import random
from datetime import datetime, timedelta, timezone
from src.db import pool


def compute_backoff_seconds(attempt: int, base: float = 2.0, cap: float = 300.0) -> float:
    """attempt is 1-indexed (this was the 1st, 2nd, 3rd... failure)."""
    raw = base * (2 ** (attempt - 1))
    capped = min(raw, cap)
    jitter = random.uniform(0, capped * 0.2)  # up to 20% jitter
    return capped + jitter


def mark_failed(job_id: int, attempts: int, max_attempts: int, error: str) -> None:
    """
    Called by a worker when a task raised an exception.

    If we've used up all attempts -> permanently 'failed'.
    Otherwise -> back to 'pending' with a future run_at, so it
    naturally re-enters the normal dequeue flow after the delay
    (no separate "retry queue" needed -- it's the same mechanism
    that powers scheduling).
    """
    with pool.connection() as conn:
        if attempts >= max_attempts:
            conn.execute(
                """
                UPDATE jobs
                SET status = 'failed', last_error = %s, finished_at = now()
                WHERE id = %s
                """,
                (error, job_id),
            )
        else:
            delay = compute_backoff_seconds(attempts)
            next_run = datetime.now(timezone.utc) + timedelta(seconds=delay)
            conn.execute(
                """
                UPDATE jobs
                SET status = 'pending',
                    last_error = %s,
                    run_at = %s,
                    locked_by = NULL,
                    locked_at = NULL
                WHERE id = %s
                """,
                (error, next_run, job_id),
            )