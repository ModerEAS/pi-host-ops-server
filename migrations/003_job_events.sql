-- Incremental feedback from claiming agent → dispatcher

CREATE TABLE IF NOT EXISTS job_events (
    id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    message TEXT,
    data TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_job_events_job_seq ON job_events (job_id, seq);
CREATE INDEX IF NOT EXISTS idx_job_events_job_id ON job_events (job_id);
