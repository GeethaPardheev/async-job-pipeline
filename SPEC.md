# SPEC — Async Job Processing Pipeline (`jobq`)

Status: **DRAFT v1 — awaiting approval**. This document is the contract. Code, tests, and docs must match it; if they diverge, fix the code or amend this file in the same commit.

---

## 1. Scope

A REST service that accepts jobs, persists them in PostgreSQL 18, and executes them on horizontally scaled worker containers with at-least-once semantics, bounded retries, a dead-letter queue (DLQ), cron schedules, and queue observability.

One Go binary, two modes:

| Mode | Command | Role | DO App Platform component |
|---|---|---|---|
| `MODE=api` | `server api` | Control plane: HTTP API, validation, idempotency, stats, drain/cancel, schedules CRUD, DLQ replay | `services: api` (HTTP, public route) |
| `MODE=worker` | `server worker` | Data plane: poller, executor, retries, lease reaper, cron tick loop, heartbeats | `workers: worker` (no route, `instance_count: 2`) |
| `MODE=migrate` | `server migrate` | One-shot: apply migrations and exit 0 (CI and manual ops) | optional pre-deploy job |

The subcommand argument wins over `MODE` when both are set. `api` and `worker` also run migrations on startup unless `MIGRATE_ON_START=false` (idempotent, advisory-locked).

**Non-goals (v1):** multi-tenant auth/RBAC (one optional shared bearer token only), job retention/archival, `LISTEN/NOTIFY` wakeups, job dependencies/DAGs, exactly-once side effects.

---

## 2. Tech Stack (pinned)

| Concern | Choice |
|---|---|
| Language | Go **1.22.2**, `CGO_ENABLED=0`. Target was Go 1.27, capped at the local toolchain (`go version` → go1.22.2). The same version is pinned in `go.mod` (`go 1.22.2`), the Dockerfile (`golang:1.22.2-bookworm`), and CI (`go-version-file: go.mod`) |
| HTTP | stdlib `net/http` with Go 1.22 method+pattern routing (`mux.HandleFunc("POST /v1/jobs/{id}/cancel", …)`) — **no chi** |
| DB driver | `github.com/jackc/pgx/v5 v5.7.4` + `pgxpool` |
| Cron | `github.com/robfig/cron/v3 v3.0.1` (`cron.ParseStandard`: 5-field + `@every`/`@hourly` descriptors, UTC) |
| IDs | `github.com/google/uuid v1.6.0` (`uuid.NewV7()` for B-tree locality) |
| Logging | `log/slog` JSON handler to stdout |
| Metrics | Hand-written Prometheus text exposition (v0.0.4) — no client library |
| DB | PostgreSQL **18** (native locally via the PGDG apt repo, `postgres:18` in CI/compose, DO Dev Database `version: "18"` in prod). The schema uses no PG18-only features (UUIDs are generated in Go), so it also runs on PG 16/17 |

---

## 3. Repository Layout

```
cmd/server/main.go                # mode dispatch, signal handling, wiring
internal/config/                  # env parsing + validation (fail fast)
internal/domain/                  # Job/Status/Attempt types, transition table, ErrPermanent
internal/store/                   # pgxpool, migrations (embed.FS), all SQL, fenced transitions
  migrations/0001_init.sql
internal/controlplane/            # HTTP server, middleware, handlers, validation, stats, /metrics
internal/dataplane/               # worker pool, poller, executor, backoff, reaper, scheduler, heartbeat
  handlers/                       # registry + echo, sleep, flaky, always_fail, permanent_fail, http_webhook
internal/integration/             # //go:build integration — real Postgres tests
.github/workflows/ci.yml
.do/app.yaml
docs/architecture.svg
Dockerfile, docker-compose.yml, Makefile, README.md, SPEC.md, AGENTS.md
```

Dependency direction: `controlplane → store, domain`; `dataplane → store, domain`; `store → domain`. `controlplane` never imports `dataplane`; `main` passes the handler registry's type names into the API config for submission validation.

---

## 4. Job State Machine

### 4.1 States (exact strings)

`queued`, `running`, `succeeded`, `failed`, `dead-lettered`, `cancelled`

Terminal: `succeeded`, `failed`, `dead-lettered`, `cancelled`. A terminal row is **never** updated again. DLQ replay creates a **new** job (`replay_of = <old id>`).

### 4.2 Diagram

```
   submit / cron tick / DLQ replay
                │
                ▼
  ┌────────► queued ──── cancel, drain(cancel_pending) ────► cancelled
  │             │
  │             │ claim: SKIP LOCKED, attempt_count += 1, lease set
  │             ▼
  │          running ── handler ok ─────────────────────────► succeeded
  │             │
  │             ├── ErrPermanent / bad payload / unknown type ► failed
  │             │
  │             └── transient error | attempt timeout | lease expired
  │                   │
  │   attempt_count ≤ max_retries      attempt_count > max_retries
  └───────────────────┘                        │
     (run_at = now() + backoff)                ▼
                                   dead-lettered (+ dead_letter_jobs row, same statement)
```

### 4.3 Transition table

| # | From | To | Actor | Guard (in SQL `WHERE`) | Side effects |
|---|---|---|---|---|---|
| T1 | — | `queued` | API / cron / replay | queue not draining; idempotency key unique | `run_at = COALESCE(req.run_at, now())` |
| T2 | `queued` | `running` | worker poller | `status='queued' AND run_at<=now() AND NOT draining`, `FOR UPDATE OF j SKIP LOCKED` | `attempt_count+1`, `worker_id`, `lease_expires_at`, insert `job_attempts(status='running')` |
| T3 | `running` | `succeeded` | worker | fence: `status='running' AND attempt_count=$a AND worker_id=$w` | `result`, `finished_at`; attempt `succeeded` |
| T4 | `running` | `failed` | worker | fence | `last_error`, `finished_at`; attempt `failed` |
| T5 | `running` | `queued` | worker / reaper | fence `AND attempt_count <= max_retries` | `run_at = now() + backoff`, `worker_id=NULL`, `lease_expires_at=NULL`; attempt `failed`/`timed_out`/`lease_expired` |
| T6 | `running` | `dead-lettered` | worker / reaper | fence `AND attempt_count > max_retries` | `finished_at`; **same statement** inserts `dead_letter_jobs`; attempt row updated in same tx |
| T7 | `queued` | `cancelled` | API cancel / drain | `status='queued'` | `finished_at` |

Total attempts = `1 + max_retries`. Example `max_retries=3`: attempts 1–3 fail → `queued` (T5); attempt 4 fails → `dead-lettered` (T6).

### 4.4 Error classification (data plane)

| Outcome | Classification | Attempt status |
|---|---|---|
| handler returns `nil` | success → T3 | `succeeded` |
| `errors.Is(err, domain.ErrPermanent)` (incl. payload decode errors, unknown handler) | permanent → T4 | `failed` |
| `ctx.Err() == context.DeadlineExceeded` after `context.WithTimeout(timeout_seconds)` | transient → T5/T6 | `timed_out` |
| handler panic (recovered) | transient → T5/T6 | `failed` (error `panic: …`) |
| worker shutdown cancelled the handler | transient → T5/T6 | `failed` (error `worker shutdown`) |
| any other error | transient → T5/T6 | `failed` |
| lease expired (reaper) | transient → T5/T6 | `lease_expired` |
| zombie write rejected by fence | — (no job change) | `fenced` |

### 4.5 Backoff + jitter

```
d     = min(BACKOFF_CAP, BACKOFF_BASE * 2^(attempt-1))     # attempt = attempt_count just finished (≥1); shift clamped at 30
delay = d + U[0, d)                                         # full jitter added on top → delay ∈ [d, 2d)
run_at = now() + delay                                      # now() is Postgres time, delay passed as interval
```

Defaults: `BACKOFF_BASE=1s`, `BACKOFF_CAP=5m` (so the max delay is < 10m). The random source is injectable so unit tests can check exact bounds.

---

## 5. PostgreSQL Schema (`internal/store/migrations/0001_init.sql`)

Migrations: `embed.FS`, applied in version order inside `pg_advisory_lock(727274001, hashtext(<schema>))` held on a dedicated connection (per-schema key, so replicas sharing a schema serialize while isolated test schemas migrate in parallel); each file runs in its own transaction and is recorded in `schema_migrations`. DDL also uses `IF NOT EXISTS`. **Never `CREATE DATABASE`.** Optional `DB_SCHEMA` (default `public`): if it isn't `public`, run `CREATE SCHEMA IF NOT EXISTS` and set `search_path` via pgx `RuntimeParams`. This exists for test isolation and as a fallback if the DO Dev DB rejects writes to `public`.

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
  version     integer     PRIMARY KEY,
  name        text        NOT NULL,
  applied_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS queue_state (
  id          boolean     PRIMARY KEY DEFAULT true CHECK (id),       -- singleton row
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
  request_hash     text,                                   -- sha256 of normalized submit body
  worker_id        text,
  lease_expires_at timestamptz,
  last_error       text,
  result           jsonb,
  schedule_id      uuid        REFERENCES schedules(id) ON DELETE SET NULL,
  replay_of        uuid        REFERENCES jobs(id),
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  started_at       timestamptz,                            -- first claim
  finished_at      timestamptz,                            -- set on any terminal transition
  CONSTRAINT jobs_attempt_bound   CHECK (attempt_count <= max_retries + 1),
  CONSTRAINT jobs_running_leased  CHECK (status <> 'running' OR (worker_id IS NOT NULL AND lease_expires_at IS NOT NULL))
);

-- Hot path: claim. Partial so it only holds queued rows; matches the ORDER BY of the claim query exactly.
CREATE INDEX IF NOT EXISTS jobs_claim_idx   ON jobs (priority DESC, run_at, id) WHERE status = 'queued';
-- Reaper: only running rows, ordered by expiry.
CREATE INDEX IF NOT EXISTS jobs_lease_idx   ON jobs (lease_expires_at)          WHERE status = 'running';
-- GET /v1/jobs?status=… and per-status counts.
CREATE INDEX IF NOT EXISTS jobs_status_created_idx ON jobs (status, created_at DESC);
-- Stats window (dead_letter_rate).
CREATE INDEX IF NOT EXISTS jobs_finished_idx ON jobs (finished_at) WHERE finished_at IS NOT NULL;

-- Queue rows are updated several times each; vacuum early so dead tuples don't pile up in the claim index.
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
  CONSTRAINT job_attempts_unique_attempt UNIQUE (job_id, attempt_number)   -- double-claim detector
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
  snapshot         jsonb       NOT NULL,                  -- to_jsonb(jobs row) at dead-letter time
  dead_lettered_at timestamptz NOT NULL DEFAULT now(),
  replayed_at      timestamptz,
  replay_job_id    uuid        REFERENCES jobs(id)
);
CREATE INDEX IF NOT EXISTS dead_letter_jobs_at_idx ON dead_letter_jobs (dead_lettered_at DESC);

CREATE TABLE IF NOT EXISTS workers (
  worker_id          text        PRIMARY KEY,             -- <hostname>-<8 hex>, new per process start
  hostname           text        NOT NULL,
  concurrency        integer     NOT NULL CHECK (concurrency > 0),
  active_jobs        integer     NOT NULL DEFAULT 0 CHECK (active_jobs >= 0),
  status             text        NOT NULL CHECK (status IN ('running','draining','stopped')),
  version            text,
  started_at         timestamptz NOT NULL DEFAULT now(),
  last_heartbeat_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS workers_heartbeat_idx ON workers (last_heartbeat_at);
```

---

## 6. Canonical SQL (store package)

All timestamps come from Postgres `now()`. The worker's wall clock is **never** used for `run_at`, leases, or heartbeat freshness, so clock skew between containers can't corrupt scheduling.

### 6.1 Claim (T2) — one statement

```sql
WITH cand AS (
  SELECT j.id FROM jobs j
  WHERE j.status = 'queued' AND j.run_at <= now()
    AND NOT (SELECT draining FROM queue_state WHERE id)
  ORDER BY j.priority DESC, j.run_at, j.id
  LIMIT $1                                  -- min(free slots, CLAIM_BATCH_SIZE)
  FOR UPDATE OF j SKIP LOCKED
), claimed AS (
  UPDATE jobs j
     SET status = 'running', attempt_count = j.attempt_count + 1, worker_id = $2,
         lease_expires_at = now() + make_interval(secs => j.timeout_seconds) + $3::interval,  -- $3 = LEASE_GRACE
         started_at = COALESCE(j.started_at, now()), updated_at = now()
    FROM cand WHERE j.id = cand.id
  RETURNING j.*
), att AS (
  INSERT INTO job_attempts (job_id, attempt_number, worker_id, status, started_at)
  SELECT id, attempt_count, $2, 'running', now() FROM claimed
)
SELECT * FROM claimed;
```

### 6.2 Fenced completion (T3) — tx

```sql
UPDATE jobs SET status='succeeded', result=$4, last_error=NULL, lease_expires_at=NULL,
                finished_at=now(), updated_at=now()
 WHERE id=$1 AND status='running' AND attempt_count=$2 AND worker_id=$3;
-- RowsAffected == 1 → UPDATE job_attempts SET status='succeeded', finished_at=now(), duration_ms=$5
--                      WHERE job_id=$1 AND attempt_number=$2 AND worker_id=$3;
-- RowsAffected == 0 → fenced path (6.5)
```

### 6.3 Retry (T5) / Fail (T4)

```sql
-- T5
UPDATE jobs SET status='queued', run_at = now() + $4::interval, worker_id=NULL, lease_expires_at=NULL,
                last_error=$5, updated_at=now()
 WHERE id=$1 AND status='running' AND attempt_count=$2 AND worker_id=$3 AND attempt_count <= max_retries;
-- T4
UPDATE jobs SET status='failed', lease_expires_at=NULL, last_error=$4, finished_at=now(), updated_at=now()
 WHERE id=$1 AND status='running' AND attempt_count=$2 AND worker_id=$3;
```

### 6.4 Dead-letter (T6) — job status and DLQ row in one statement

```sql
WITH upd AS (
  UPDATE jobs SET status='dead-lettered', lease_expires_at=NULL, last_error=$4,
                  finished_at=now(), updated_at=now()
   WHERE id=$1 AND status='running' AND attempt_count=$2 AND worker_id=$3 AND attempt_count > max_retries
  RETURNING *
)
INSERT INTO dead_letter_jobs (job_id, type, payload, priority, max_retries, timeout_seconds,
                              attempt_count, last_error, snapshot)
SELECT id, type, payload, priority, max_retries, timeout_seconds, attempt_count, last_error, to_jsonb(upd)
  FROM upd
RETURNING job_id;
```

### 6.5 Fenced (zombie) path

If a fenced `UPDATE jobs` affects 0 rows, the worker does **not** touch `jobs`. In the same tx it runs
`UPDATE job_attempts SET status='fenced', error='stale attempt rejected: <outcome>', finished_at=now(), duration_ms=$d WHERE job_id=$1 AND attempt_number=$2 AND worker_id=$3 AND status IN ('running','lease_expired')`,
commits, and returns `store.ErrFenced`. The data plane logs this at WARN level. Each `fenced` row is evidence that a handler ran twice.

### 6.6 Lease reaper — tx, batch ≤ `REAPER_BATCH` (100)

```sql
SELECT id, attempt_count, max_retries, worker_id FROM jobs
 WHERE status='running' AND lease_expires_at < now()
 ORDER BY lease_expires_at LIMIT $1
 FOR UPDATE SKIP LOCKED;
```

For each row, apply T5 or T6 using **that row's** `(attempt_count, worker_id)` as the fencing token, with error `lease expired (worker <id> presumed dead)` and attempt status `lease_expired`. All rows commit in one tx. The same loop also deletes `workers` rows with `last_heartbeat_at < now() - interval '1 hour'`.

### 6.7 Cron tick — tx, batch ≤ 20

```sql
SELECT now();                                               -- db_now
SELECT * FROM schedules WHERE enabled AND next_run_at <= now()
 ORDER BY next_run_at LIMIT 20 FOR UPDATE SKIP LOCKED;
-- per schedule s, unless draining:
INSERT INTO jobs (id, type, payload, priority, max_retries, timeout_seconds, run_at, idempotency_key, schedule_id)
VALUES ($id, s.type, s.payload, s.priority, s.max_retries, s.timeout_seconds, now(),
        'cron:' || s.id || ':' || extract(epoch FROM s.next_run_at)::bigint, s.id)
ON CONFLICT (idempotency_key) DO NOTHING RETURNING id;
UPDATE schedules SET last_run_at = s.next_run_at, next_run_at = $next, last_job_id = COALESCE($job_id, last_job_id),
                     updated_at = now() WHERE id = s.id;
```

`$next = parsed.Next(db_now)` (UTC). **Catch-up policy:** after downtime, a schedule fires once for its latest missed tick and skips the rest; there is no backfill storm. While draining, the tick advances `next_run_at` without enqueuing.

### 6.8 Submit (T1) with idempotency

```sql
INSERT INTO jobs (id, type, payload, priority, max_retries, timeout_seconds, run_at, idempotency_key, request_hash)
VALUES (...) ON CONFLICT (idempotency_key) DO NOTHING RETURNING id, status, run_at;
-- no row → SELECT id, status, run_at, request_hash FROM jobs WHERE idempotency_key = $key;
--          request_hash differs → 409 idempotency_key_conflict, else 200 replay
```

The draining check (`SELECT draining FROM queue_state`) runs first. A submit that races a drain toggle may still land; that's acceptable because the claim query re-checks `draining` atomically.

---

## 7. Concurrency Invariants (each has an integration test)

| ID | Invariant | Mechanism | Test |
|---|---|---|---|
| I1 | A queued job is claimed by at most one worker per attempt | single-statement claim with `FOR UPDATE OF j SKIP LOCKED`; `UNIQUE(job_id, attempt_number)` | 8 workers × concurrency 8, 500 jobs → exactly 500 attempt rows, each job `attempt_count=1` |
| I2 | A stale worker can never change a job it no longer owns | every worker write fenced on `(id, status='running', attempt_count, worker_id)`; `RowsAffected` checked | zombie test: A claims, lease expires, reaper requeues, B claims; A's success → `ErrFenced`, B's wins |
| I3 | A healthy worker always finishes before its lease expires | `lease = timeout_seconds + LEASE_GRACE` (30s) and executor deadline = `timeout_seconds`; no DB conn held during execution | reaper test only reaps leases that are actually expired |
| I4 | `dead-lettered` ⇔ a `dead_letter_jobs` row exists | single-statement CTE (6.4) | after the DLQ test: `count(status='dead-lettered') = count(dead_letter_jobs)` |
| I5 | Attempts never exceed `1 + max_retries` | SQL guards on T5/T6 + `CHECK (attempt_count <= max_retries + 1)` | always_fail with `max_retries=2` → exactly 3 attempts |
| I6 | Terminal states are immutable | every UPDATE guards on a non-terminal status | cancel a succeeded job → 409; fenced write on a terminal job → 0 rows |
| I7 | Each cron tick is enqueued exactly once across N workers | `FOR UPDATE SKIP LOCKED` on the schedule + `next_run_at` advanced in the same tx + unique key `cron:<id>:<unix>` | 4 schedulers racing over 3 ticks → exactly 3 jobs |
| I8 | No new work starts while draining | `NOT draining` inside the claim statement; API returns 503 | pause → submit 503, queued jobs stay queued, running jobs finish; resume → they run |
| I9 | Duplicate submits never create duplicate work | `UNIQUE(idempotency_key)` + `ON CONFLICT DO NOTHING` | 20 concurrent submits with the same key → 1 job |
| I10 | All scheduling time comes from Postgres | `now()` in SQL; Go supplies durations only | code review rule (AGENTS.md) |

---

## 8. HTTP API

Base path `/v1`. JSON bodies only (`Content-Type: application/json`). Unknown fields → 400. Max body 1 MiB, max `payload` 256 KiB. Every response carries `X-Request-ID` (echoed if the client sent one). If `API_TOKEN` is set, `/v1/*` requires `Authorization: Bearer <token>` (401 otherwise). **`/healthz` and `/readyz` MUST bypass `API_TOKEN` auth** so DigitalOcean App Platform health checks never get a 401. Auth applies only to the `/v1` prefix, so `/metrics` is also open. A unit test enforces this.

**Error envelope** (all 4xx/5xx):

```json
{ "error": { "code": "invalid_request", "message": "timeout_seconds must be between 1 and 3600" } }
```

Codes: `invalid_request` (400), `unknown_job_type` (400), `unauthorized` (401), `not_found` (404), `conflict` (409), `idempotency_key_conflict` (409), `queue_draining` (503, with `Retry-After: 30`), `unavailable` (503), `internal` (500).

### 8.1 `POST /v1/jobs` — submit

Headers: optional `Idempotency-Key` (1–255 printable ASCII; prefixes `cron:` and `dlq-replay:` are reserved → 400).

```json
{
  "type": "flaky",
  "payload": {"fail_times": 2},
  "priority": 10,
  "max_retries": 3,
  "timeout_seconds": 30,
  "run_at": "2026-11-21T10:00:00Z"
}
```

| Field | Type | Required | Default | Rule |
|---|---|---|---|---|
| `type` | string | yes | — | must be a registered handler name |
| `payload` | any JSON | no | `{}` | ≤ 256 KiB |
| `priority` | int | no | `0` | −1000…1000, higher runs first |
| `max_retries` | int | no | `3` | 0…25 (retries after the first attempt) |
| `timeout_seconds` | int | no | `30` | 1…3600, per attempt |
| `run_at` | RFC3339 | no | now | past values run immediately |

Responses:
- `202 Accepted` (new): `Location: /v1/jobs/{id}`; body `{"job_id":"…","status":"queued","run_at":"…"}`
- `200 OK` (idempotent replay, same normalized body): `Idempotent-Replayed: true`; body `{"job_id":"…","status":"<current status>","run_at":"…"}`
- `409 idempotency_key_conflict`: same key, different body
- `400`, `401`, `503 queue_draining`

### 8.2 `GET /v1/jobs/{id}`

`200`:

```json
{
  "id": "0193…", "type": "flaky", "status": "succeeded", "priority": 10,
  "payload": {"fail_times": 2}, "attempt_count": 3, "max_retries": 3, "timeout_seconds": 30,
  "last_error": null, "result": {"attempt": 3}, "run_at": "…", "idempotency_key": null,
  "worker_id": "worker-7f9c-1a2b3c4d", "schedule_id": null, "replay_of": null,
  "created_at": "…", "updated_at": "…", "started_at": "…", "finished_at": "…",
  "attempts": [
    {"attempt_number": 3, "worker_id": "…", "status": "succeeded", "error": null,
     "started_at": "…", "finished_at": "…", "duration_ms": 12}
  ]
}
```

`attempts` holds the 20 most recent, newest first. Returns `400` on a malformed UUID and `404` if the job doesn't exist.

### 8.3 `GET /v1/jobs?status=&type=&limit=`

`status` must be one of the 6 states; `limit` defaults to 50 (max 500); ordered `created_at DESC`. `200 {"jobs":[<job without attempts>], "count": n}`.

### 8.4 `POST /v1/jobs/{id}/cancel`

`200 {"job_id":"…","status":"cancelled"}` when the job was `queued` (or was already `cancelled`). `409 conflict` with `{"error":{…,"message":"job is running"}}` for any other state. `404` if missing.

### 8.5 `GET /v1/dlq?limit=`

`200 {"entries":[{"job_id","type","payload","priority","max_retries","timeout_seconds","attempt_count","last_error","dead_lettered_at","replayed_at","replay_job_id"}], "count": n}`, ordered `dead_lettered_at DESC`, limit default 50 (max 500).

### 8.6 `POST /v1/dlq/{job_id}/replay`

In one tx: lock the DLQ row (`FOR UPDATE`); if it was already replayed, return that job. Otherwise insert a new job (same type, payload, priority, max_retries, timeout; `attempt_count=0`; `replay_of=<job_id>`; `idempotency_key='dlq-replay:<job_id>'`) and set `replayed_at` and `replay_job_id`. The original job stays `dead-lettered` and keeps its history.

- `202 {"job_id":"<new>","replay_of":"<old>","status":"queued"}` on first replay
- `200` with the same body (`Idempotent-Replayed: true`) on repeat calls
- `404` if there's no DLQ entry; `503 queue_draining` while draining

### 8.7 `POST /v1/queue/drain`

Body `{"mode":"pause"}` or `{"mode":"cancel_pending"}`. Both set `queue_state.draining=true`, which stops intake (503) and stops new claims. In-flight `running` jobs finish normally.

- `pause`: backlog stays `queued` until resume. `200 {"draining":true,"mode":"pause","queued":n,"running":m}`
- `cancel_pending`: in the **same tx**, `UPDATE jobs SET status='cancelled', finished_at=now() WHERE status='queued'`. `200 {"draining":true,"mode":"cancel_pending","cancelled":n,"running":m}`

Operators poll `GET /v1/queue/stats` until `running == 0`.

### 8.8 `POST /v1/queue/resume`

Clears `draining` and `drain_mode`. `200 {"draining":false}`.

### 8.9 `GET /v1/queue/stats`

```json
{
  "generated_at": "…", "window_seconds": 300,
  "draining": false, "drain_mode": null,
  "depth": {
    "by_status": {"queued": 12, "running": 4, "succeeded": 980, "failed": 3, "dead-lettered": 2, "cancelled": 0},
    "by_priority": [{"priority": 10, "queued": 2, "running": 1}, {"priority": 0, "queued": 10, "running": 3}],
    "ready": 10, "delayed": 2
  },
  "active_workers": 2, "total_concurrency": 16, "active_jobs": 4, "worker_utilization": 0.25,
  "p50_latency_ms": 41.0, "p95_latency_ms": 2210.5,
  "dead_letter_rate": 0.002
}
```

Definitions:
- `active_workers`, `total_concurrency`, `active_jobs`: aggregates over `workers` where `status <> 'stopped' AND last_heartbeat_at > now() - WORKER_STALE_AFTER` (15s, i.e. 3 missed heartbeats).
- `worker_utilization = active_jobs / total_concurrency` (0 when no workers).
- `p50/p95_latency_ms`: `percentile_cont(0.5|0.95) WITHIN GROUP (ORDER BY duration_ms)` over `job_attempts` finished within `STATS_WINDOW` (5m). This is **attempt execution latency**. `null` when there's no data.
- `dead_letter_rate`: `dead-lettered / (succeeded + failed + dead-lettered)` over jobs with `finished_at` within `STATS_WINDOW`. `0` when there's no data.

### 8.10 `GET /metrics` (Prometheus text, `text/plain; version=0.0.4`)

Computed from the same stats query on each scrape (DB timeout 3s), plus API-process counters.

```
jobq_jobs{status="queued",priority="10"} 2          # gauge, queued/running by priority
jobq_jobs_by_status{status="succeeded"} 980         # gauge
jobq_queue_ready 10 / jobq_queue_delayed 2          # gauges
jobq_active_workers 2
jobq_total_concurrency 16
jobq_active_jobs 4
jobq_worker_utilization 0.25
jobq_attempt_latency_ms{quantile="0.5"} 41
jobq_attempt_latency_ms{quantile="0.95"} 2210.5
jobq_dead_letter_rate 0.002
jobq_attempts_window{status="fenced"} 0             # attempts finished in window, by status
jobq_draining 0
jobq_http_requests_total{method="POST",route="/v1/jobs",code="202"} 123   # counter (API process)
```

### 8.11 Schedules

`POST /v1/schedules`:

```json
{"name":"heartbeat-echo","cron_expr":"@every 30s","type":"echo","payload":{"hi":1},
 "priority":0,"max_retries":3,"timeout_seconds":30,"enabled":true}
```

The cron expression is validated with `cron.ParseStandard` (400 on error) and `type` against the registry. `next_run_at = parsed.Next(now)`. Returns `201 {schedule}`, where a schedule has `id, name, cron_expr, type, payload, priority, max_retries, timeout_seconds, enabled, next_run_at, last_run_at, last_job_id, created_at, updated_at`.

`GET /v1/schedules?limit=` → `200 {"schedules":[…],"count":n}`.
`DELETE /v1/schedules/{id}` → `204` (jobs it already enqueued keep `schedule_id` set to NULL).

### 8.12 Health

- `GET /healthz` → `200 {"status":"ok"}`. Process liveness, no DB call.
- `GET /readyz` → `200 {"status":"ready"}` if `pool.Ping` succeeds within 2s and the server isn't shutting down. Otherwise `503 {"status":"not_ready","error":"…"}`.

---

## 9. Handler Registry (data plane)

```go
type Job struct { ID uuid.UUID; Type string; Payload json.RawMessage; Attempt, MaxRetries int; TimeoutSeconds int }
type Handler func(ctx context.Context, job Job) (result json.RawMessage, err error)
```

Handlers **must** honor `ctx`. The executor runs the handler in a goroutine and returns at the deadline even if the handler ignores `ctx`. The orphaned goroutine is logged and counted. Panics are recovered and treated as transient.

| Type | Payload | Behavior |
|---|---|---|
| `echo` | any | result = payload |
| `sleep` | `{"duration_ms": int}` | sleeps with `select` on `ctx.Done()`; result `{"slept_ms": n}`; exceeding the timeout → `timed_out` |
| `flaky` | `{"fail_times": int}` | transient error while `Attempt <= fail_times`, then result `{"attempt": n}` |
| `always_fail` | any | always a transient error → retries → DLQ |
| `permanent_fail` | any | `domain.Permanent(errors.New("permanent failure requested"))` → `failed` |
| `http_webhook` | `{"url": str, "method": "POST", "headers": {}, "body": any}` | request built with `ctx`; always sets `X-Job-ID` and `X-Attempt-Number` (+ `User-Agent: jobq/<version>`). Network error, 5xx, 408, 429 → transient. Other 4xx → permanent. Invalid URL or non-http(s) scheme → permanent. Max 3 redirects; response body read capped at 64 KiB; result `{"status_code": n, "body": "<≤4 KiB>"}` |

Payload decode failures in any handler are wrapped with `domain.Permanent`.

---

## 10. Worker Runtime

- **Pool:** `WORKER_CONCURRENCY` slots (a semaphore). One poller goroutine claims `min(free, CLAIM_BATCH_SIZE)` jobs. On a full batch it re-polls immediately. On an empty batch it sleeps `POLL_INTERVAL ± 20% jitter`, which spreads N workers out and prevents synchronized polling.
- **Executor:** `ctx, cancel := context.WithTimeout(runCtx, timeout_seconds)`. Measures duration with a monotonic clock. Result writes use a fresh `context.WithTimeout(context.WithoutCancel(ctx), DB_WRITE_TIMEOUT)` so outcomes are persisted during shutdown.
- **Heartbeat:** every `HEARTBEAT_INTERVAL` (5s), upsert `workers(worker_id, hostname, concurrency, active_jobs, status, version, last_heartbeat_at=now())`.
- **Reaper:** every `REAPER_INTERVAL` (10s) ± jitter (6.6).
- **Cron:** every `CRON_TICK_INTERVAL` (1s) ± jitter (6.7). Leaderless: every worker ticks, and `SKIP LOCKED` partitions the work.
- **Graceful SIGTERM/SIGINT:**
  1. Stop the poller, cron, and reaper (cancel the loop ctx). Heartbeat reports `draining`.
  2. Wait for in-flight handlers up to `SHUTDOWN_TIMEOUT` (25s; DO `termination.grace_period_seconds: 60`).
  3. On timeout, cancel handler contexts. Each outcome is recorded via T5/T6 with `worker shutdown`.
  4. Final heartbeat `status='stopped'`, close the pool, exit 0.
- **API shutdown:** `/readyz` flips to 503, `http.Server.Shutdown` with a 15s timeout, close the pool.

---

## 11. Configuration (env)

| Var | Default | Mode | Notes |
|---|---|---|---|
| `MODE` | — | both | `api` \| `worker` (or argv[1]) |
| `DATABASE_URL` | — (required) | both | `sslmode=require` honored as-is (DO supplies it) |
| `DB_SCHEMA` | `public` | both | non-public → `CREATE SCHEMA IF NOT EXISTS` + `search_path` |
| `DB_MAX_CONNS` | api `8`, worker `5` | both | keep `api + N×worker` under the DB's `max_connections` |
| `DB_QUERY_TIMEOUT` | `5s` | both | default per-query deadline |
| `MIGRATE_ON_START` | `true` | both | |
| `PORT` | `8080` | api | DO injects `PORT` |
| `API_TOKEN` | empty | api | if set, bearer auth on `/v1/*` |
| `WORKER_ID` | `<hostname>-<8 hex>` | worker | |
| `WORKER_CONCURRENCY` | `8` | worker | |
| `CLAIM_BATCH_SIZE` | `8` | worker | |
| `POLL_INTERVAL` | `500ms` | worker | ±20% jitter |
| `LEASE_GRACE` | `30s` | worker | lease = timeout + grace |
| `REAPER_INTERVAL` / `REAPER_BATCH` | `10s` / `100` | worker | |
| `HEARTBEAT_INTERVAL` | `5s` | worker | |
| `CRON_TICK_INTERVAL` | `1s` | worker | |
| `BACKOFF_BASE` / `BACKOFF_CAP` | `1s` / `5m` | worker | |
| `SHUTDOWN_TIMEOUT` | `25s` | both | |
| `WORKER_STALE_AFTER` | `15s` | api | heartbeat freshness for stats |
| `STATS_WINDOW` | `5m` | api | latency / DLQ-rate window |
| `LOG_LEVEL` | `info` | both | `debug\|info\|warn\|error` |

Invalid config → log one JSON error line and exit 1 before opening the DB.

---

## 12. Testing Matrix

**Unit** (`go test -race ./...`, no DB): backoff bounds (and exact values with a seeded rand) plus shift clamping; transition table (allowed/forbidden); error classification (permanent/transient/timeout/panic); cron parsing and `Next`; each handler, with `httptest.Server` for `http_webhook` header propagation and status mapping; request validation; idempotency hash normalization; Prometheus text rendering.

**Integration** (`go test -race -tags=integration ./internal/integration/...`, needs `TEST_DATABASE_URL`). Each test gets its own schema `t_<rand>` (no `CREATE DATABASE`) and runs in parallel. Workers and the API (`httptest`) run in-process with fast timings (`BACKOFF_BASE=20ms`, `POLL_INTERVAL=20ms`, `LEASE_GRACE=1s`). Tests synchronize with an `eventually()` poll helper, never fixed sleeps.

1. happy path (submit → succeeded, result, 1 attempt row)
2. priority ordering (single worker, concurrency 1, claim order by priority then run_at)
3. delayed `run_at` (not claimed before; claimed after)
4. flaky retry-to-success (`fail_times=2` → 3 attempts, `succeeded`, `run_at` gaps ≥ backoff floor)
5. timeout-to-retry (`sleep` > timeout → attempt `timed_out`, requeued)
6. max-retries → DLQ (`always_fail`, `max_retries=2` → 3 attempts, `dead-lettered`, DLQ row, I4)
7. permanent → `failed` (1 attempt, no DLQ row)
8. zombie fencing rejection (I2, attempt row `fenced`)
9. lease reaper recovery (claim then "crash": no completion, expire lease → requeued → completed by another worker; and → DLQ when exhausted)
10. concurrent workers, zero double-claim (I1)
11. cancel (queued → cancelled; running → 409) and drain `pause` / `cancel_pending` / resume (I8)
12. idempotency (same key, same body → 1 job; different body → 409; 20 concurrent → 1 job) (I9)
13. DLQ replay (new job, idempotent repeat)
14. cron no double-fire with 4 concurrent schedulers (I7)
15. migrations idempotent (run twice, concurrently)

**CI** (`.github/workflows/ci.yml`, on every push and PR): `postgres:18` service with health check (`actions/setup-go` with `go-version-file: go.mod`) → `gofmt -l` must be empty → `go vet ./...` → unit tests → integration tests → `go build` → `docker build` (verifies the Dockerfile).

---

## 13. Deployment (DigitalOcean App Platform)

`.do/app.yaml`: region `blr`. `services.api` (Dockerfile, `http_port: 8080`, `health_check.http_path: /readyz`, env `MODE=api`). `workers.worker` (same Dockerfile, env `MODE=worker`, `instance_count: 2`). `databases.db` (`engine: PG`, `version: "18"`, `production: false`; App Platform supports up to PG 18). Both components get `DATABASE_URL=${db.DATABASE_URL}` and `termination.grace_period_seconds: 60`. API and workers scale independently by changing `instance_count` and running `doctl apps update <id> --spec .do/app.yaml`. `GET /v1/queue/stats.active_workers` reflects the change within one heartbeat interval.

**Known risks:**
- Dev DBs have a low `max_connections`. Pool defaults are sized for 1 API + 2–3 workers.
- Dev DBs on PG15+ can reject `CREATE` on schema `public`. The fallback is `DB_SCHEMA=jobq`; if that also fails, attach a managed PG18 cluster.

**Docker:** multi-stage. `golang:1.22.2-bookworm` builder (`-trimpath -ldflags "-s -w -X main.version=$VERSION"`) → `gcr.io/distroless/static-debian12:nonroot`. `ENTRYPOINT ["/server"]`; the mode comes from env. `docker-compose.yml` runs `postgres:18`, `api`, and `worker` (`deploy.replicas: 2`). The PG18 image stores data at `/var/lib/postgresql/18/docker`, so the compose volume mounts `/var/lib/postgresql` (not `/var/lib/postgresql/data`).
