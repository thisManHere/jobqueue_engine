"""
run_worker.py — the script you actually run to start processing jobs.

Run: python -m examples.run_worker
(Ctrl+C to stop. In production you'd run several of these, even on
different machines, all pointed at the same DATABASE_URL.)
"""

import examples.tasks  # noqa: F401  (side-effect import: registers @task handlers)
from src.worker import run_worker

if __name__ == "__main__":
    run_worker(queue="default", poll_interval=1.0)