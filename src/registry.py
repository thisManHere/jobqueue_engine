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
