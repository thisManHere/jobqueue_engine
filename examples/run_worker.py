"""
run_worker.py the script you actually run to start processing jobs.

Run: python -m examples.run_worker
"""

import examples.tasks  # noqa: F401  (side-effect import: registers @task handlers)
from src.worker import run_worker

if __name__ == "__main__":
    run_worker(queue="default", poll_interval=1.0)
