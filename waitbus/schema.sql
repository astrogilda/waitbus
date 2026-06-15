-- waitbus event store schema
-- Single events table keyed by delivery_id for idempotent INSERT OR IGNORE.
-- The `source` column is a v2 extension point (linux-journal, slack, pagerduty).

CREATE TABLE IF NOT EXISTS events (
    -- seq: daemon-assigned monotonic sequence.
    -- INTEGER PRIMARY KEY AUTOINCREMENT so it is monotonic and NEVER reused even after
    -- `waitbus db prune` deletes the highest row -- a plain rowid would reuse it (verified
    -- vs sqlite.org/autoinc.html) and break since-replay. seq is the daemon's INTERNAL
    -- ordering authority for since-replay, the caught_up_at watermark, and the fan-out
    -- replay-dedup; because the single daemon is the sole writer, seq is the true causal
    -- order across producer processes (cross-process ULID event_id ordering is not, which
    -- is the soundness gap this column closes). seq is auto-assigned at INSERT (never in
    -- EVENT_COLUMNS) and stays daemon-internal -- NOT projected to the wire or MCP; the
    -- PUBLIC cursor remains the ULID event_id, translated to seq daemon-side.
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    -- delivery_id: the upstream-correlation + INSERT OR IGNORE idempotency token. Was the
    -- table PRIMARY KEY before seq landed; now NOT NULL UNIQUE (the OR IGNORE dedup fires
    -- on the UNIQUE constraint exactly as it did on the PK).
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
    received_at INTEGER NOT NULL,  -- epoch nanoseconds; see waitbus/_types.py
    -- payload_json: the event content blob (CI/alert sources store their payload
    -- here). agent_message rows set it to "{}" -- an unused NOT NULL sentinel --
    -- because the message body rides the typed msg_body column so it reaches the
    -- recipient on the wire (the lean frame drops payload_json). The body is
    -- therefore single-homed in msg_body; the CloudEvents `data` projection
    -- carries it from there.
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
    -- agent-message addressing facet.
    -- First-class typed columns -- exactly like the alert_* facet above -- so the
    -- addressing keys project into the broadcast wire `fields` and are
    -- predicate-matchable via `fields.msg_to=...`. The `msg_` prefix is
    -- load-bearing: bare `to`/`from` are SQL reserved words. msg_to/msg_from are
    -- SELF-ASSERTED agent names under the same-UID trust model -- addresses, not
    -- credentials. msg_correlation_id pairs a reply to its request; msg_reply_to
    -- is the unique-per-requestor return address; msg_thread groups a conversation.
    msg_to TEXT,
    msg_from TEXT,
    msg_correlation_id TEXT,
    msg_reply_to TEXT,
    msg_thread TEXT,
    -- msg_body is the message content itself, carried on the wire (the lean
    -- frame drops payload_json, so an agent message stores its body here so the
    -- recipient receives it without a DB round-trip). Subject to the 64 KiB
    -- frame cap; an oversize body is dropped to a truncated stub carrying the
    -- correlation id, and request() re-fetches the full body by event_id (via
    -- waitbus._db.fetch_event_by_id) -- the degenerate Claim-Check, since
    -- the body is already durable here in SQLite.
    msg_body TEXT,
    -- event_id: locally-generated ULID stamped at insert time. The PUBLIC
    -- wire identity and resumable subscriber cursor (consumers pass
    -- since=<event_id>); the daemon translates it to the internal seq for
    -- ordering, so cross-process event_id non-monotonicity never affects
    -- replay correctness. event_id is additive and NULL on rows inserted
    -- before this column landed; the partial UNIQUE index below excludes
    -- those NULLs so historical rows neither collide on uniqueness nor
    -- surface to subscribers' cursor scans.
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

-- Addressed-messaging routing paths: a recipient's inbox (by msg_to + recency)
-- and reply correlation (by msg_correlation_id). Partial so they stay compact
-- on a bus whose traffic is mostly non-addressed CI/alert events.
CREATE INDEX IF NOT EXISTS idx_msg_to
    ON events(msg_to, received_at DESC) WHERE msg_to IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_msg_correlation
    ON events(msg_correlation_id) WHERE msg_correlation_id IS NOT NULL;

-- Partial covering index for per-run job rollup in read_events and the
-- pr_monitor AGG_SQL path. Filtering to event_type='workflow_job' with
-- a non-null head_sha keeps the index compact (only rows those consumers
-- care about). Ordering by received_at DESC means the window function's
-- first-per-job selection lands on the index without a separate sort pass.
CREATE INDEX IF NOT EXISTS idx_workflow_job_head_sha
    ON events(head_sha, owner, repo, received_at DESC)
    WHERE event_type = 'workflow_job' AND head_sha IS NOT NULL;
