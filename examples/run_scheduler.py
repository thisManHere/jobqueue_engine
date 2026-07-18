"""
run_scheduler.py — run exactly ONE of these per deployment (unlike
workers, where you want several). It watches cron_schedules and
enqueues real jobs when they're due.

Run: python -m examples.run_scheduler
"""

from src.scheduler import run_scheduler

if __name__ == "__main__":
    run_scheduler(tick_seconds=15.0)