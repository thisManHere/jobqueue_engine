import time
from datetime import datetime, timezone
from croniter import croniter

from src.db import pool, init_pool
from src.queue import enqueue


def add_cron_schedule(name: str, task_name: str, cron_expr: str,
                       payload: dict | None = None, queue: str = "default",
                       priority: int = 100) -> None:
    """Register a recurring job, e.g. add_cron_schedule('daily-report', 'send_report', '0 9 * * *')"""
    first_run = croniter(cron_expr, datetime.now(timezone.utc)).get_next(datetime)
    with pool.connection() as conn:
        conn.execute(
            """
            INSERT INTO cron_schedules (name, task_name, payload, cron_expr, queue, priority, next_run_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (name) DO UPDATE
                SET task_name = EXCLUDED.task_name,
                    payload = EXCLUDED.payload,
                    cron_expr = EXCLUDED.cron_expr,
                    enabled = true
            """,
            (name, task_name, __import__("json").dumps(payload or {}),
             cron_expr, queue, priority, first_run),
        )


def run_scheduler(tick_seconds: float = 15.0, max_ticks: int | None = None):
    init_pool()
    print("[scheduler] started")
    ticks = 0
    while max_ticks is None or ticks < max_ticks:
        ticks += 1
        with pool.connection() as conn:
            due = conn.execute(
                """
                SELECT id, task_name, payload, cron_expr, queue, priority
                FROM cron_schedules
                WHERE enabled = true AND next_run_at <= now()
                """
            ).fetchall()

            for (sid, task_name, payload, cron_expr, queue, priority) in due:
                enqueue(task_name, payload, queue=queue, priority=priority)

                next_run = croniter(cron_expr, datetime.now(timezone.utc)).get_next(datetime)
                conn.execute(
                    "UPDATE cron_schedules SET next_run_at = %s WHERE id = %s",
                    (next_run, sid),
                )
                print(f"[scheduler] enqueued '{task_name}' from schedule '{cron_expr}', "
                      f"next run at {next_run}")

        time.sleep(tick_seconds)


if __name__ == "__main__":
    run_scheduler()
