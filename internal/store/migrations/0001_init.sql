-- 0001_init: core schema (SPEC §5). Objects are created in the session search_path schema.

CREATE TABLE IF NOT EXISTS queue_state (
  id          boolean     PRIMARY KEY DEFAULT true CHECK (id),
  draining    boolean     NOT NULL DEFAULT false,
  drain_mode  text        CHECK (drain_mode IN ('pause','cancel_pending')),
  updated_at  timestamptz NOT NULL DEFAULT now()
);
INSERT INTO queue_state (id) VALUES (true) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS schedules (
  id               uuid        PRIMARY KEY,
  name             text        CHECK (char_length(name) <= 128),
  cron_expr        text        NOT NULL CHECK (char_length(cron_expr) BETWEEN 1 AND 128),
  type             text        NOT NULL CHECK (char_length(type) BETWEEN 1 AND 64),
  payload          jsonb       NOT NULL DEFAULT '{}'::jsonb,
  priority         integer     NOT NULL DEFAULT 0  CHECK (priority BETWEEN -1000 AND 1000),
  max_retries      integer     NOT NULL DEFAULT 3  CHECK (max_retries BETWEEN 0 AND 25),
  timeout_seconds  integer     NOT NULL DEFAULT 30 CHECK (timeout_seconds BETWEEN 1 AND 3600),
  enabled          boolean     NOT NULL DEFAULT true,
  next_run_at      timestamptz NOT NULL,
  last_run_at      timestamptz,
  last_job_id      uuid,
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS schedules_due_idx ON schedules (next_run_at) WHERE enabled;

CREATE TABLE IF NOT EXISTS jobs (
  id               uuid        PRIMARY KEY,
  type             text        NOT NULL CHECK (char_length(type) BETWEEN 1 AND 64),
  payload          jsonb       NOT NULL DEFAULT '{}'::jsonb,
  priority         integer     NOT NULL DEFAULT 0  CHECK (priority BETWEEN -1000 AND 1000),
  status           text        NOT NULL DEFAULT 'queued'
                   CHECK (status IN ('queued','running','succeeded','failed','dead-lettered','cancelled')),
  attempt_count    integer     NOT NULL DEFAULT 0  CHECK (attempt_count >= 0),
  max_retries      integer     NOT NULL DEFAULT 3  CHECK (max_retries BETWEEN 0 AND 25),
  timeout_seconds  integer     NOT NULL DEFAULT 30 CHECK (timeout_seconds BETWEEN 1 AND 3600),
  run_at           timestamptz NOT NULL DEFAULT now(),
  idempotency_key  text        UNIQUE CHECK (char_length(idempotency_key) BETWEEN 1 AND 255),
  request_hash     text,
  worker_id        text,
  lease_expires_at timestamptz,
  last_error       text,
  result           jsonb,
  schedule_id      uuid        REFERENCES schedules(id) ON DELETE SET NULL,
  replay_of        uuid        REFERENCES jobs(id),
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  started_at       timestamptz,
  finished_at      timestamptz,
  CONSTRAINT jobs_attempt_bound  CHECK (attempt_count <= max_retries + 1),
  CONSTRAINT jobs_running_leased CHECK (status <> 'running' OR (worker_id IS NOT NULL AND lease_expires_at IS NOT NULL))
);

-- Claim hot path: only queued rows, same ordering as the claim query.
CREATE INDEX IF NOT EXISTS jobs_claim_idx ON jobs (priority DESC, run_at, id) WHERE status = 'queued';
-- Lease reaper: only running rows, ordered by expiry.
CREATE INDEX IF NOT EXISTS jobs_lease_idx ON jobs (lease_expires_at) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS jobs_status_created_idx ON jobs (status, created_at DESC);
CREATE INDEX IF NOT EXISTS jobs_finished_idx ON jobs (finished_at) WHERE finished_at IS NOT NULL;

-- Queue rows churn through several updates each; vacuum early so dead tuples don't bloat the claim index.
ALTER TABLE jobs SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.02,
                      autovacuum_vacuum_cost_limit = 2000, fillfactor = 90);

CREATE TABLE IF NOT EXISTS job_attempts (
  id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  job_id          uuid        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  attempt_number  integer     NOT NULL CHECK (attempt_number >= 1),
  worker_id       text        NOT NULL,
  status          text        NOT NULL
                  CHECK (status IN ('running','succeeded','failed','timed_out','lease_expired','fenced')),
  error           text,
  started_at      timestamptz NOT NULL DEFAULT now(),
  finished_at     timestamptz,
  duration_ms     bigint      CHECK (duration_ms >= 0),
  CONSTRAINT job_attempts_unique_attempt UNIQUE (job_id, attempt_number)
);
CREATE INDEX IF NOT EXISTS job_attempts_finished_idx ON job_attempts (finished_at) WHERE finished_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS dead_letter_jobs (
  job_id           uuid        PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
  type             text        NOT NULL,
  payload          jsonb       NOT NULL,
  priority         integer     NOT NULL,
  max_retries      integer     NOT NULL,
  timeout_seconds  integer     NOT NULL,
  attempt_count    integer     NOT NULL,
  last_error       text,
  snapshot         jsonb       NOT NULL,
  dead_lettered_at timestamptz NOT NULL DEFAULT now(),
  replayed_at      timestamptz,
  replay_job_id    uuid        REFERENCES jobs(id)
);
CREATE INDEX IF NOT EXISTS dead_letter_jobs_at_idx ON dead_letter_jobs (dead_lettered_at DESC);

CREATE TABLE IF NOT EXISTS workers (
  worker_id          text        PRIMARY KEY,
  hostname           text        NOT NULL,
  concurrency        integer     NOT NULL CHECK (concurrency > 0),
  active_jobs        integer     NOT NULL DEFAULT 0 CHECK (active_jobs >= 0),
  status             text        NOT NULL CHECK (status IN ('running','draining','stopped')),
  version            text,
  started_at         timestamptz NOT NULL DEFAULT now(),
  last_heartbeat_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS workers_heartbeat_idx ON workers (last_heartbeat_at);
