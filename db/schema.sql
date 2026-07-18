-- =========================================================
-- JOB QUEUE + WORKFLOW ENGINE SCHEMA
-- =========================================================
-- One table (`jobs`) is the entire queue. Rows move through
-- statuses: pending -> running -> succeeded / failed / retrying
-- A second table (`job_dependencies`) turns individual jobs
-- into a DAG (workflow) by saying "job B waits on job A".
-- A third table (`cron_schedules`) tells a scheduler process
-- which jobs to create automatically, and when.
-- =========================================================

CREATE TYPE job_status AS ENUM (
    'pending',    -- waiting to be picked up by a worker
    'running',    -- a worker currently has it locked and is executing it
    'succeeded',  -- finished with no error
    'failed',     -- ran out of retries, permanently failed
    'cancelled',  -- manually cancelled
    'waiting'     -- part of a DAG, blocked on other jobs finishing first
);

CREATE TABLE jobs (
    id              BIGSERIAL PRIMARY KEY,

    -- WHAT to run: the name of a registered task function,
    -- plus JSON arguments for it.
    task_name       TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- QUEUE MECHANICS
    status          job_status NOT NULL DEFAULT 'pending',
    priority        INT NOT NULL DEFAULT 100,   -- LOWER number = runs first
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(), -- don't run before this time (scheduling/delay)
    queue           TEXT NOT NULL DEFAULT 'default', -- lets you run separate worker pools per queue

    -- RETRIES
    attempts        INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 3,
    last_error      TEXT,

    -- WORKER OWNERSHIP (who currently holds this job, for crash detection)
    locked_by       TEXT,
    locked_at       TIMESTAMPTZ,

    -- DAG / WORKFLOW GROUPING
    workflow_id     BIGINT,             -- groups jobs that belong to one workflow run
    workflow_run_key TEXT,              -- human label for this run, e.g. "etl-2026-07-09"

    -- TIMESTAMPS
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,

    CONSTRAINT valid_attempts CHECK (attempts <= max_attempts + 1)
);

-- The single most important index in this whole system.
-- Every worker's "give me the next job" query filters and
-- sorts on exactly these columns. Without this index, that
-- query becomes a full table scan under load.
CREATE INDEX idx_jobs_dequeue
    ON jobs (queue, priority, run_at)
    WHERE status = 'pending';

CREATE INDEX idx_jobs_workflow ON jobs (workflow_id);
CREATE INDEX idx_jobs_status ON jobs (status);


-- =========================================================
-- DAG edges: "child_job_id cannot start until parent_job_id succeeds"
-- =========================================================
CREATE TABLE job_dependencies (
    child_job_id    BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    parent_job_id   BIGINT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    PRIMARY KEY (child_job_id, parent_job_id)
);

CREATE INDEX idx_deps_parent ON job_dependencies (parent_job_id);


-- =========================================================
-- Cron-style recurring schedules. A separate "scheduler"
-- process reads this table and enqueues jobs on time.
-- =========================================================
CREATE TABLE cron_schedules (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    task_name       TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    cron_expr       TEXT NOT NULL,      -- e.g. '*/5 * * * *'
    queue           TEXT NOT NULL DEFAULT 'default',
    priority        INT NOT NULL DEFAULT 100,
    next_run_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    enabled         BOOLEAN NOT NULL DEFAULT true
);