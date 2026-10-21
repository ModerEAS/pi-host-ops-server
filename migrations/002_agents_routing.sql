-- Multi-agent registry + job targeting / delivery_mode / leases

CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    project TEXT,
    cwd TEXT,
    machine TEXT,
    capabilities TEXT NOT NULL DEFAULT '[]',
    max_concurrency INTEGER NOT NULL DEFAULT 1,
    last_seen_at TEXT,
    status TEXT NOT NULL DEFAULT 'offline',
    created_at TEXT NOT NULL,
    revoked_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_agents_token_hash ON agents (token_hash);
CREATE INDEX IF NOT EXISTS idx_agents_project ON agents (project);

-- Extend jobs for targeting, sticky threads, delivery mode, leases
ALTER TABLE jobs ADD COLUMN target_agent_id TEXT;
ALTER TABLE jobs ADD COLUMN target_filter TEXT;
ALTER TABLE jobs ADD COLUMN thread_id TEXT;
ALTER TABLE jobs ADD COLUMN delivery_mode TEXT NOT NULL DEFAULT 'steer';
ALTER TABLE jobs ADD COLUMN lease_expires_at TEXT;
ALTER TABLE jobs ADD COLUMN timeout_secs INTEGER;

CREATE INDEX IF NOT EXISTS idx_jobs_target_agent ON jobs (target_agent_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_thread ON jobs (thread_id, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_lease ON jobs (status, lease_expires_at);
