"""
dag_example.py — builds a small ETL-style DAG:

        extract
           |
        transform
           |
          load
           |
        notify

Each arrow means "waits for the one above it to succeed".
Run: python -m examples.dag_example
Then start a worker to watch it execute in the correct order.
"""

from src.db import init_pool
from src.dag import Workflow

init_pool()

wf = Workflow(run_key="etl-demo")
t_extract = wf.add_task("extract", {"source": "orders_db"})
t_transform = wf.add_task("transform", {}, depends_on=[t_extract])
t_load = wf.add_task("load", {}, depends_on=[t_transform])
t_notify = wf.add_task("notify", {"message": "ETL run complete"}, depends_on=[t_load])

workflow_id = wf.submit()
print(f"Submitted workflow_id={workflow_id} with 4 tasks (extract -> transform -> load -> notify)")
print("Start a worker to watch it execute in order:")
print("  python -m examples.run_worker")