from src.db import init_pool
from src.scheduler import add_cron_schedule

init_pool()

add_cron_schedule(
    name="minutely-report",
    task_name="notify",
    cron_expr="* * * * *",  
    payload={"message": "this is the recurring cron job"},
)

print("Registered cron schedule 'minutely-report' (runs every minute).")
print("Start the scheduler process:")
print("  python -m examples.run_scheduler")
