CREATE TABLE IF NOT EXISTS runners (
    id              TEXT PRIMARY KEY,
    name            TEXT        NOT NULL,
    token_hash      TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    labels          TEXT[]      NOT NULL DEFAULT '{}',
    architecture    TEXT        NOT NULL,
    operating_system TEXT       NOT NULL,
    current_job_id  TEXT,
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_assigned_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
    id           TEXT PRIMARY KEY,
    repository   TEXT        NOT NULL,
    clone_url    TEXT        NOT NULL,
    commit_sha   TEXT        NOT NULL,
    branch       TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    config_ref   TEXT        NOT NULL DEFAULT 'forgerun.yml',
    labels       TEXT[]      NOT NULL DEFAULT '{}',
    status       TEXT        NOT NULL,
    runner_id    TEXT REFERENCES runners(id) ON DELETE SET NULL,
    attempts     INT         NOT NULL DEFAULT 0,
    exit_code    INT,
    error        TEXT        NOT NULL DEFAULT '',
    logs         TEXT        NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

-- Idempotency: GitHub retries webhook deliveries. At most one unfinished job may
-- exist per (repo, sha, event), so a redelivery cannot double-build a commit.
CREATE UNIQUE INDEX IF NOT EXISTS jobs_active_unique
    ON jobs (repository, commit_sha, event_type)
    WHERE status IN ('QUEUED', 'ASSIGNED', 'RUNNING');

CREATE INDEX IF NOT EXISTS jobs_status_created ON jobs (status, created_at DESC);
CREATE INDEX IF NOT EXISTS runners_status ON runners (status);
