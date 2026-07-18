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
