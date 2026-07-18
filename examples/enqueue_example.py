"""
enqueue_example.py — shows off priority and delayed scheduling.

Run: python -m examples.enqueue_example
"""

from datetime import datetime, timedelta, timezone
from src.db import init_pool
from src.queue import enqueue

init_pool()

# A normal job, runs ASAP at default priority (100)
enqueue("send_email", {"to": "team@example.com", "subject": "Normal priority"})

# A HIGH priority job (lower number = more urgent) -- jumps the queue
enqueue("send_email", {"to": "ceo@example.com", "subject": "URGENT"}, priority=1)

# A job that shouldn't run for 10 seconds (delayed / scheduled job)
run_later = datetime.now(timezone.utc) + timedelta(seconds=10)
enqueue("send_email", {"to": "later@example.com", "subject": "Delayed 10s"}, run_at=run_later)

# A task that fails most of the time -- watch it retry with backoff
enqueue("flaky_task", {"note": "will probably fail a few times"}, max_attempts=5)

print("Enqueued 4 jobs. Start a worker to process them:")
print("  python -m examples.run_worker")