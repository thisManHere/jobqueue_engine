"""
registry.py — maps a task_name (a plain string stored in Postgres)
to an actual Python function to run.

We store task_name as a string in the DB (not a function reference,
since you can't put code in a database column). Each worker process
imports your task modules, which register themselves here via the
@task decorator, building up this dictionary at startup.
"""

TASKS: dict[str, callable] = {}


def task(name: str):
    """Decorator: @task("send_email") def send_email(payload): ..."""
    def decorator(fn):
        TASKS[name] = fn
        return fn
    return decorator


def get_task(name: str) -> callable:
    if name not in TASKS:
        raise KeyError(
            f"No task registered for '{name}'. "
            f"Known tasks: {list(TASKS.keys())}"
        )
    return TASKS[name]