-- Migration 0002: add the daemon-assigned monotonic sequence (seq).
-- Adds seq INTEGER PRIMARY KEY
-- AUTOINCREMENT as the events PK and demotes delivery_id from PRIMARY KEY
-- to NOT NULL UNIQUE. SQLite cannot ALTER TABLE ADD a PRIMARY KEY /
-- AUTOINCREMENT column, so this is a table rebuild: create the new shape,
-- copy every row ordered by event_id (so seq is backfilled in historical
-- order), drop the old table, rename, and recreate every index.
--
-- seq is monotonic and never reused even after prune deletes the highest
-- row (a plain rowid would reuse it). It is the daemon's internal ordering
-- authority; event_id stays the public cursor.

CREATE TABLE events_new (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    owner TEXT NOT NULL,
    repo TEXT NOT NULL,
    run_id INTEGER,
    workflow_name TEXT,
    head_branch TEXT,
    head_sha TEXT,
    status TEXT,
    conclusion TEXT,
    received_at INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    ingest_method TEXT NOT NULL,
    job_id INTEGER,
    job_name TEXT,
    parent_run_id INTEGER,
    alert_name TEXT,
    alert_severity TEXT,
    alert_fingerprint TEXT,
    msg_to TEXT,
    msg_from TEXT,
    msg_correlation_id TEXT,
    msg_reply_to TEXT,
    msg_thread TEXT,
    msg_body TEXT,
    event_id TEXT
);

INSERT INTO events_new (
    delivery_id, source, event_type, owner, repo, run_id, workflow_name,
    head_branch, head_sha, status, conclusion, received_at, payload_json,
    ingest_method, job_id, job_name, parent_run_id, alert_name, alert_severity,
    alert_fingerprint, msg_to, msg_from, msg_correlation_id, msg_reply_to,
    msg_thread, msg_body, event_id
)
SELECT
    delivery_id, source, event_type, owner, repo, run_id, workflow_name,
    head_branch, head_sha, status, conclusion, received_at, payload_json,
    ingest_method, job_id, job_name, parent_run_id, alert_name, alert_severity,
    alert_fingerprint, msg_to, msg_from, msg_correlation_id, msg_reply_to,
    msg_thread, msg_body, event_id
FROM events
ORDER BY event_id;

DROP TABLE events;

ALTER TABLE events_new RENAME TO events;

CREATE INDEX IF NOT EXISTS idx_owner_repo_event
    ON events(owner, repo, event_type, received_at DESC);

CREATE INDEX IF NOT EXISTS idx_run_id
    ON events(run_id) WHERE run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_parent_run_id
    ON events(parent_run_id) WHERE parent_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_job_id
    ON events(job_id) WHERE job_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_alert_lookup
    ON events(event_type, alert_name, received_at DESC)
    WHERE event_type IN ('prometheus_alert', 'prometheus_watchdog');

CREATE INDEX IF NOT EXISTS idx_alert_fingerprint
    ON events(alert_fingerprint) WHERE alert_fingerprint IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_id
    ON events(event_id) WHERE event_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_msg_to
    ON events(msg_to, received_at DESC) WHERE msg_to IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_msg_correlation
    ON events(msg_correlation_id) WHERE msg_correlation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_workflow_job_head_sha
    ON events(head_sha, owner, repo, received_at DESC)
    WHERE event_type = 'workflow_job' AND head_sha IS NOT NULL;
