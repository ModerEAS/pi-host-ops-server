-- Worker identity + agent bindings (one agent belongs to at most one worker)

CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    created_at TEXT NOT NULL,
    revoked_at TEXT,
    last_seen_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_workers_token_hash ON workers (token_hash);

CREATE TABLE IF NOT EXISTS worker_agents (
    worker_id TEXT NOT NULL REFERENCES workers(id),
    agent_id TEXT NOT NULL UNIQUE REFERENCES agents(id),
    created_at TEXT NOT NULL,
    PRIMARY KEY (worker_id, agent_id)
);

CREATE INDEX IF NOT EXISTS idx_worker_agents_agent ON worker_agents (agent_id);
CREATE INDEX IF NOT EXISTS idx_worker_agents_worker ON worker_agents (worker_id);
