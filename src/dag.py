"""
dag.py — turns individual jobs into a workflow (a DAG: directed
acyclic graph of tasks).

The idea:
  - each task in the workflow is still just a row in `jobs`
  - a job that depends on others is inserted with status='waiting'
    instead of 'pending', so workers will never pick it up
  - `job_dependencies` stores the edges: (child_job_id, parent_job_id)
  - whenever a job SUCCEEDS, we call unlock_children(): look at every
    job that listed it as a parent, and for each of those children,
    check "have ALL of your parents now succeeded?" If yes, flip that
    child from 'waiting' to 'pending' -- now a worker can pick it up.

This means the DAG needs no separate scheduler or engine process --
each worker naturally advances the graph as it finishes jobs.
"""

from src.db import pool
from src.queue import enqueue


class Workflow:
    """
    Small builder for defining a DAG of jobs in one call.

    Usage:
        wf = Workflow("nightly-etl")
        extract = wf.add_task("extract", {"source": "orders"})
        transform = wf.add_task("transform", {}, depends_on=[extract])
        load = wf.add_task("load", {}, depends_on=[transform])
        wf.submit()
    """

    def __init__(self, run_key: str, queue: str = "default"):
        self.run_key = run_key
        self.queue = queue
        self._pending_tasks = []  # list of dicts, not yet inserted
        self._next_temp_id = 0

    def add_task(self, task_name: str, payload: dict | None = None,
                 depends_on: list[int] | None = None, priority: int = 100,
                 max_attempts: int = 3) -> int:
        """Returns a temporary id you can pass as depends_on for later tasks."""
        temp_id = self._next_temp_id
        self._next_temp_id += 1
        self._pending_tasks.append({
            "temp_id": temp_id,
            "task_name": task_name,
            "payload": payload or {},
            "depends_on": depends_on or [],
            "priority": priority,
            "max_attempts": max_attempts,
        })
        return temp_id

    def submit(self) -> int:
        """
        Insert every task as a real row, wire up job_dependencies,
        and flip tasks with no dependencies straight to 'pending'
        so they can start immediately. Returns the workflow_id.
        """
        with pool.connection() as conn:
            # workflow_id groups all these jobs together for querying/reporting
            workflow_id = conn.execute(
                "SELECT nextval(pg_get_serial_sequence('jobs','id'))"
            ).fetchone()[0]

            temp_to_real = {}
            for t in self._pending_tasks:
                status = "waiting" if t["depends_on"] else "pending"
                real_id = enqueue(
                    t["task_name"], t["payload"],
                    queue=self.queue, priority=t["priority"],
                    max_attempts=t["max_attempts"],
                    workflow_id=workflow_id, status=status,
                )
                temp_to_real[t["temp_id"]] = real_id

            for t in self._pending_tasks:
                child_real_id = temp_to_real[t["temp_id"]]
                for parent_temp_id in t["depends_on"]:
                    parent_real_id = temp_to_real[parent_temp_id]
                    conn.execute(
                        """
                        INSERT INTO job_dependencies (child_job_id, parent_job_id)
                        VALUES (%s, %s)
                        """,
                        (child_real_id, parent_real_id),
                    )
            return workflow_id


def unlock_children(finished_job_id: int) -> None:
    """
    Call this after a job succeeds. Finds every job that depends on
    it, and for each one, checks whether ALL of its parents have now
    succeeded. If so, promotes it from 'waiting' to 'pending'.
    """
    with pool.connection() as conn:
        children = conn.execute(
            """
            SELECT child_job_id FROM job_dependencies
            WHERE parent_job_id = %s
            """,
            (finished_job_id,),
        ).fetchall()

        for (child_id,) in children:
            still_blocked = conn.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM job_dependencies d
                    JOIN jobs p ON p.id = d.parent_job_id
                    WHERE d.child_job_id = %s
                      AND p.status <> 'succeeded'
                )
                """,
                (child_id,),
            ).fetchone()[0]

            if not still_blocked:
                conn.execute(
                    """
                    UPDATE jobs SET status = 'pending'
                    WHERE id = %s AND status = 'waiting'
                    """,
                    (child_id,),
                )