-- Migration 0001: initial schema baseline.
-- Mirrors waitbus/schema.sql at the time the migrations tooling
-- landed. On a fresh DB waitbus init invokes ensure_schema (which
-- materialises schema.sql) and then records this migration as applied
-- via mark_baseline_applied so the next evolutionary migration runs
-- against a tracked starting point.
--
-- waitbus event store schema
-- Single events table keyed by delivery_id for idempotent INSERT OR IGNORE.
-- The `source` column is a v2 extension point (linux-journal, slack, pagerduty).

CREATE TABLE IF NOT EXISTS events (
    delivery_id TEXT PRIMARY KEY,
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
    received_at INTEGER NOT NULL,  -- epoch nanoseconds; see waitbus/_types.py
    payload_json TEXT NOT NULL,
    -- ingest_method: renamed from source_method 2026-05-10.
    ingest_method TEXT NOT NULL,
    -- workflow_job event extensions. For fresh DBs these are part of the
    -- CREATE TABLE; for pre-existing DBs the listener applies an
    -- idempotent ALTER TABLE ADD COLUMN diff at startup.
    job_id INTEGER,
    job_name TEXT,
    parent_run_id INTEGER,
    -- prometheus_alert / prometheus_watchdog event extensions. These
    -- rows are not GitHub-bound; owner/repo carry synthetic labels
    -- resolved at insert time from waitbus._config (default
    -- 'prometheus' / 'alerts' on a fresh install). alert_fingerprint
    -- is Alertmanager's stable identifier across re-fires; delivery_id
    -- remains the per-delivery dedup key (the upstream signer is
    -- expected to synthesise it from groupKey|status|earliestStartsAt).
    alert_name TEXT,
    alert_severity TEXT,
    alert_fingerprint TEXT,
    -- event_id: locally-generated ULID stamped at insert time. Forms the
    -- broadcast wire identity and resumable subscriber cursor; opaque to
    -- consumers so the daemon-internal rowid layout stays hidden. PK
    -- stays as delivery_id — the upstream-correlation and
    -- INSERT OR IGNORE dedup key. event_id is additive and NULL on rows
    -- inserted before this column landed; the partial UNIQUE index
    -- below excludes those NULLs so historical rows neither collide on
    -- uniqueness nor surface to subscribers' cursor scans.
    event_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_owner_repo_event
    ON events(owner, repo, event_type, received_at DESC);

CREATE INDEX IF NOT EXISTS idx_run_id
    ON events(run_id) WHERE run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_parent_run_id
    ON events(parent_run_id) WHERE parent_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_job_id
    ON events(job_id) WHERE job_id IS NOT NULL;

-- prometheus_alert / prometheus_watchdog query path: by alert name + recency.
CREATE INDEX IF NOT EXISTS idx_alert_lookup
    ON events(event_type, alert_name, received_at DESC)
    WHERE event_type IN ('prometheus_alert', 'prometheus_watchdog');

-- Alertmanager fingerprint dedup across re-fires. Distinct from delivery_id
-- (which dedups same-fire retries via the am-signer-synthesised key).
CREATE INDEX IF NOT EXISTS idx_alert_fingerprint
    ON events(alert_fingerprint) WHERE alert_fingerprint IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_id
    ON events(event_id) WHERE event_id IS NOT NULL;

-- Partial covering index for per-run job rollup in read_events and the
-- pr_monitor AGG_SQL path. Filtering to event_type='workflow_job' with
-- a non-null head_sha keeps the index compact (only rows those consumers
-- care about). Ordering by received_at DESC means the window function's
-- first-per-job selection lands on the index without a separate sort pass.
CREATE INDEX IF NOT EXISTS idx_workflow_job_head_sha
    ON events(head_sha, owner, repo, received_at DESC)
    WHERE event_type = 'workflow_job' AND head_sha IS NOT NULL;
