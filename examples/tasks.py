import random
from src.registry import task


@task("send_email")
def send_email(payload: dict):
    print(f"  -> sending email to {payload['to']}: '{payload['subject']}'")


@task("flaky_task")
def flaky_task(payload: dict):
    """Fails 60% of the time, to demonstrate retries + backoff."""
    if random.random() < 0.6:
        raise RuntimeError("simulated transient failure (e.g. API timeout)")
    print(f"  -> flaky_task finally succeeded with payload {payload}")


@task("extract")
def extract(payload: dict):
    print(f"  -> extracting data from {payload.get('source', 'unknown source')}")


@task("transform")
def transform(payload: dict):
    print("  -> transforming extracted data")


@task("load")
def load(payload: dict):
    print("  -> loading transformed data into warehouse")


@task("notify")
def notify(payload: dict):
    print(f"  -> notifying: {payload.get('message', 'workflow complete')}")
