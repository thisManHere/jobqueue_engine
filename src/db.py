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
