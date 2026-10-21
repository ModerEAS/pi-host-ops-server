CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY NOT NULL,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    payload TEXT NOT NULL,
    result TEXT,
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    claimed_at TEXT,
    finished_at TEXT,
    agent_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs (status, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs (created_at DESC);
