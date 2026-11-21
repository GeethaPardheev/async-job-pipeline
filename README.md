# Async Job Processing Pipeline

[![CI](https://github.com/GeethaPardheev/async-job-pipeline/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/GeethaPardheev/async-job-pipeline/actions/workflows/ci.yml)

A production-style async job queue REST API in Go 1.22 that uses **PostgreSQL 18 as the single source of truth**. One binary runs as `MODE=api` (control plane) or `MODE=worker` (data plane), and the two scale independently on DigitalOcean App Platform.

![Architecture Diagram](docs/architecture.svg)

```mermaid
flowchart LR
    submit([POST /v1/jobs<br/>Idempotency-Key + body hash]) --> queued
    queued -->|"claim: FOR UPDATE SKIP LOCKED<br/>attempt_count++, lease set"| running
    queued -->|"cancel / drain cancel_pending"| cancelled([cancelled])
    running -->|success| succeeded([succeeded])
    running -->|permanent error| failed([failed])
    running -->|"transient error / timeout<br/>attempt_count ≤ max_retries"| backoff["backoff d + U[0,d)<br/>d = min(cap, base·2^(attempt−1))"]
    backoff -->|run_at = now() + delay| queued
    running -->|"lease expired (worker crash)"| reaper{{lease reaper}}
    reaper -->|retries left| backoff
    reaper -->|retries exhausted| dlq
    running -->|"retries exhausted"| dlq([dead-lettered])
    dlq --> dlt[(dead_letter_jobs)]
    dlt -->|"POST /v1/dlq/{id}/replay<br/>(idempotent: dlq-replay:&lt;id&gt;)"| queued
    running -.->|"every write fenced on<br/>(id, status='running', attempt_count, worker_id)<br/>0 rows → ErrFenceLost, attempt marked fenced"| fence{{fencing guard}}
```

Statuses are exactly `queued`, `running`, `succeeded`, `failed`, `dead-lettered`, `cancelled`. The contract lives in [`SPEC.md`](SPEC.md) and the engineering rules in [`AGENTS.md`](AGENTS.md).

## Live DigitalOcean deployment and scaling proof

**Live URL:** <https://async-job-pipeline-m6rfs.ondigitalocean.app> (region `blr`, spec in [`.do/app.yaml`](.do/app.yaml)).

- `/healthz`, `/readyz` and `/metrics` are open, so App Platform health checks never get a 401. There is no route at `/`, so the bare URL returns 404.
- `/v1/*` requires `Authorization: Bearer <API_TOKEN>`. The live token is injected at deploy time as an encrypted App Platform `SECRET` and is never committed.

| Component | Size | Instances | Pool cap |
|---|---|---|---|
| `api` (service, `MODE=api`) | `apps-s-1vcpu-0.5gb` | 1, untouched during scaling | 8 |
| `worker` (worker, `MODE=worker`) | `apps-s-1vcpu-1gb` | 2 → 3 | 5 per worker |
| `db` | Dev Database, PostgreSQL 18, `public` schema | 1 | — |

The worker uses `apps-s-1vcpu-1gb` because every basic size, including `apps-s-1vcpu-0.5gb`, is `single_instance_only` on App Platform, and the API rejects `instance_count > 1` for it.

**Observed live scaling** (only `workers[0].instance_count` changed through `doctl apps update`):

| Metric (`GET /v1/queue/stats` and `GET /metrics`) | Before | After |
|---|---|---|
| worker instances | 2 | 3 (deployment ACTIVE in about 30s) |
| `active_workers` / `jobq_active_workers` | 2 | 3 |
| `total_concurrency` / `jobq_total_concurrency` | 16 | 24 |
| `api` instances | 1 | 1 (no restart) |
| `scripts/smoke_test.sh` | 13/13 passed | 13/13 passed |

The existing worker pods kept running and the new one joined. Migrations ran once on the dev database under the advisory lock, and the other replicas logged `migrations up to date`.

**DigitalOcean edge behaviour during drain:** the API returns `503 queue_draining` with `Retry-After: 30`. App Platform's Cloudflare edge replaces every origin 503 with its own HTML page and status **504**. It keeps `Retry-After: 30` and `X-Request-ID` and adds `x-do-orig-status: 503`. The smoke test accepts both forms.

**Redeploying:** the app uses a public `git` source, so pushes to `main` don't auto-deploy. Redeploy manually:

```bash
doctl apps create-deployment <app-id>
```

## Local setup

Prerequisites: Go 1.22.2 and PostgreSQL 18, either native or through Docker.

```bash
# Native Postgres (what CI and local development use)
make db-setup                                   # creates role/db jobq/jobq
export DATABASE_URL='postgres://jobq:jobq@localhost:5432/jobq?sslmode=disable'
make build
./bin/server migrate                            # idempotent; also runs on start by default
MODE=api ./bin/server &                         # :8080
MODE=worker WORKER_ID=w1 ./bin/server &
MODE=worker WORKER_ID=w2 ./bin/server &

# Or everything in Docker (postgres:18 + api + 2 workers)
docker compose up --build

# Tests
make test                                       # unit tests with -race
TEST_DATABASE_URL="$DATABASE_URL" make test-integration   # HTTP + live-worker E2E, -race
```

Smoke test against any deployment (needs `bash`, `curl`, `python3`):

```bash
BASE_URL=http://localhost:8080 scripts/smoke_test.sh
BASE_URL=https://async-job-pipeline-m6rfs.ondigitalocean.app API_TOKEN=... EXPECT_WORKERS=2 scripts/smoke_test.sh
```

It asserts health, auth, idempotency (202 → 200 → 409), flaky retries, sleep timeouts, `permanent_fail` → `failed`, `always_fail` → DLQ → replay, cancel, drain/resume, and stats and metrics (`active_workers`, p50/p95, `dead_letter_count`). An exit trap resumes the queue if the script is interrupted mid-drain.

### Configuration

| Variable | Default | Applies to | Meaning |
|---|---|---|---|
| `MODE` | — (or first CLI arg) | all | `api`, `worker` or `migrate` |
| `DATABASE_URL` | — (required) | all | Postgres DSN (`${db.DATABASE_URL}` on App Platform) |
| `DB_SCHEMA` | `public` | all | schema for tables; non-public schemas are created and set as `search_path` |
| `DB_MAX_CONNS` | api `8`, worker `5` | all | pgxpool cap per process |
| `DB_QUERY_TIMEOUT` | `5s` | all | per-statement timeout |
| `MIGRATE_ON_START` | `true` | all | run embedded migrations under an advisory lock at boot |
| `SHUTDOWN_TIMEOUT` | `25s` | all | graceful drain window on SIGTERM |
| `LOG_LEVEL` | `info` | all | slog JSON level |
| `PORT` | `8080` | api | HTTP port |
| `API_TOKEN` | empty (auth off) | api | bearer token for `/v1/*` |
| `WORKER_STALE_AFTER` | `15s` | api | heartbeat age after which a worker is no longer active |
| `STATS_WINDOW` | `5m` | api | window for latency percentiles and dead-letter rate |
| `WORKER_ID` | `hostname-<8hex>` | worker | fencing identity |
| `WORKER_CONCURRENCY` | `8` | worker | max concurrent attempts per worker |
| `CLAIM_BATCH_SIZE` | `8` | worker | max rows per claim query |
| `POLL_INTERVAL` | `500ms` | worker | idle poll period |
| `LEASE_GRACE` | `30s` | worker | lease = `timeout_seconds + LEASE_GRACE` |
| `REAPER_INTERVAL` / `REAPER_BATCH` | `10s` / `100` | worker | expired-lease sweep cadence and size |
| `HEARTBEAT_INTERVAL` | `5s` | worker | worker heartbeat period |
| `CRON_TICK_INTERVAL` | `1s` | worker | schedule tick period |
| `BACKOFF_BASE` / `BACKOFF_CAP` | `1s` / `5m` | worker | retry delay `d + U[0,d)`, `d = min(cap, base·2^(attempt−1))` |

## At-least-once semantics

**Where it is guaranteed**

- **Durable accept.** `202 Accepted` is returned only after the job row is committed in Postgres. A job that was acknowledged is never lost.
- **Atomic claims.** Workers claim with `SELECT … FOR UPDATE SKIP LOCKED` inside one CTE that moves the job to `running`, increments `attempt_count`, sets the lease and inserts the `job_attempts` row. Two workers can never hold the same attempt.
- **Crash recovery.** If a worker dies, its lease (`timeout_seconds + LEASE_GRACE`) expires and the lease reaper re-queues the job with backoff, or dead-letters it if retries are exhausted.
- **Retries until a terminal state.** Transient errors and timeouts retry until `max_retries` is reached. Every job ends in exactly one of `succeeded`, `failed`, `dead-lettered` or `cancelled`.

**Where it is not guaranteed**

- **Before commit.** If the client times out before receiving the response, it cannot know whether the job was stored. It must retry with the same `Idempotency-Key`, which makes the retry safe.
- **External side effects.** A handler such as `http_webhook` can complete its side effect and then lose its lease or crash before recording success. The job then runs again, so receivers must deduplicate.

**How duplicates are bounded and detected**

- **Optimistic fencing.** Every write that leaves `running` matches `WHERE id=$1 AND status='running' AND attempt_count=$2 AND worker_id=$3`. A stale (zombie) worker's write affects 0 rows and returns `ErrFenceLost`; its attempt is recorded as `fenced` and the newer attempt wins.
- **Idempotency keys with a body hash.** The key is unique in `jobs`, and the canonical SHA-256 request hash is stored with it. The same key and body returns `200` with `Idempotent-Replayed: true`; the same key with a different body returns `409 idempotency_key_conflict`. Cron uses `cron:<schedule_id>:<unix>` and DLQ replay uses `dlq-replay:<job_id>`, so neither can create duplicates.
- **Audit trail.** `job_attempts` records every attempt (`succeeded`, `failed`, `timed_out`, `lease_expired`, `fenced`), so duplicate executions are visible after the fact.
- **Webhook dedupe headers.** `http_webhook` always sends `X-Job-ID` and `X-Attempt-Number`, set after user headers so a payload can't override them. Receivers can deduplicate on `X-Job-ID`.

## What breaks first under load

The failure modes below are ordered by when they hit as load grows on this deployment.

1. **Connection pool exhaustion.** The DO Dev Database has a low connection cap, and the caps add up: 8 for the API plus 5 per worker gives 23 at 3 workers, before admin connections. Scaling workers further, or adding API instances, will hit `too many connections` before CPU limits.
   *Mitigation:* PgBouncer in transaction-pooling mode (DO connection pools) on a managed Postgres cluster, and per-process caps sized against `max_connections`.
2. **MVCC bloat and autovacuum lag on `jobs`.** Every job is inserted and then updated at least twice per attempt (claim, then finish or retry), so dead tuples pile up and the partial claim index bloats. Claim latency then climbs.
   *Mitigation:* the migration already sets aggressive per-table autovacuum (`scale_factor 0.02`, `cost_limit 2000`) and `fillfactor=90` for HOT updates. Next steps: lower `fillfactor` to 80, partition or archive terminal rows out of `jobs`, and take advantage of PostgreSQL 18's asynchronous I/O (AIO) for vacuum and sequential reads.
3. **Polling thundering herd.** Idle workers each poll every `POLL_INTERVAL`, so N workers cost N empty claim queries per interval even when there's no work.
   *Mitigation:* adaptive polling (exponential idle backoff with jitter) plus `LISTEN/NOTIFY` wake-up hints on enqueue, keeping polling as the correctness fallback.
4. **Single-primary write ceiling (around 10k+ jobs/s).** All state transitions go through one primary's WAL and IOPS.
   *Mitigation:* move dispatch onto Kafka or NATS JetStream through a transactional outbox, and keep Postgres for job state, idempotency and audit.

## Repository layout

```
cmd/server            single binary: api | worker | migrate
internal/config       env parsing and validation
internal/domain       statuses, transitions, job model, cron parsing
internal/store        pgx/v5 queries, migrations (embed), fencing, stats
internal/controlplane HTTP server, middleware, validation, metrics
internal/dataplane    poller, executor, backoff, reaper, heartbeat, cron; handlers/
internal/integration  Postgres-backed integration and E2E tests (-tags=integration)
scripts/smoke_test.sh live deployment smoke test
.do/app.yaml          App Platform spec (no secrets)
docs/architecture.svg architecture diagram
```
