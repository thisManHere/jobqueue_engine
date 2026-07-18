"""
db.py — one shared connection pool for the whole app.

We use psycopg3's ConnectionPool so that workers, the scheduler,
and the DAG resolver don't each open a brand-new TCP connection to
Postgres every time they touch the database. That's expensive and
doesn't scale. Instead everyone borrows a connection from this pool
and gives it back when done.

Docs: https://www.psycopg.org/psycopg3/docs/advanced/pool.html
"""

import os
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/jobqueue",
)

# min_size/max_size: how many real connections the pool keeps ready.
# open=False means "don't connect yet" -- we open it explicitly with
# pool.open() so errors surface clearly at startup instead of on first use.
pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=10, open=False)


def init_pool():
    pool.open(wait=True, timeout=10)


def close_pool():
    pool.close()