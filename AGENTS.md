# AGENTS.md — Engineering Guardrails for `jobq`

Rules for any human or AI agent changing this repo. `SPEC.md` is the contract and this file is the code standard. When they conflict, stop and amend `SPEC.md` first.

## 0. Workflow

- All files live under `/workspaces/async-job-pipeline`. Never write outside `/workspaces`.
- Work proceeds in the approved 5-step plan. Don't start a step without the explicit "Go Step N".
- Definition of done for every step:
  `gofmt -l . | wc -l` == 0, `go vet ./...` clean, `go test -race -count=1 ./...` green, and
  `go test -race -count=1 -tags=integration ./internal/integration/...` green against native Postgres. Commit, push, and confirm GitHub Actions is green.
- Local Postgres is **native** PostgreSQL 18 from the PGDG apt repo (`apt.postgresql.org`, `noble-pgdg`, `apt install postgresql-18`; Ubuntu 24.04 itself only ships 16). Do not assume Docker is available. Integration tests read `TEST_DATABASE_URL`, e.g. `postgres://jobq:jobq@localhost:5432/jobq?sslmode=disable`.
- Commits are small and imperative (`store: add fenced completion`). Never commit secrets, `.env`, or binaries.

## 1. Pinned Versions (do not change without approval)

| Item | Pin |
|---|---|
| Go | **1.22.2**: target was 1.27, capped at the local toolchain (`go1.22.2`). `go 1.22.2` in `go.mod`; CI `actions/setup-go` with `go-version-file: go.mod`; Dockerfile `golang:1.22.2-bookworm`. All three must change together |
| `github.com/jackc/pgx/v5` | `v5.7.4` (requires Go ≥ 1.21) |
| `github.com/robfig/cron/v3` | `v3.0.1` |
| `github.com/google/uuid` | `v1.6.0` |
| Postgres | **18** (native local via PGDG, `postgres:18` CI/compose, DO Dev DB `version: "18"`; App Platform's max is 18) |
| Builder / runtime images | `golang:1.22.2-bookworm` / `gcr.io/distroless/static-debian12:nonroot` |

**No other modules.** That means no chi, testify, ORMs, migration frameworks, Prometheus client, `x/sync`, or viper. Use stdlib (`testing`, `net/http`, `log/slog`, `embed`, `sync`, `crypto/sha256`). Adding a module needs explicit approval and a `SPEC.md` update.

## 2. Package Boundaries

- `internal/domain`: pure types, status constants, transition table, `ErrPermanent` / `Permanent(err)`. No I/O, no imports from other internal packages.
- `internal/store`: the **only** package that contains SQL or imports pgx. It exposes intent-named methods (`ClaimJobs`, `CompleteJob`, `RetryJob`, `FailJob`, `DeadLetterJob`, `ReapExpiredLeases`, `TickSchedules`, …), not generic query helpers.
- `internal/controlplane` and `internal/dataplane` depend on `store` and `domain`, never on each other.
- `cmd/server` only wires things together. No business logic.
- No package-level mutable state. The handler registry is built in `main` and passed in. No `init()` side effects.

## 3. Context & Timeouts (everywhere)

- `ctx context.Context` is the first parameter of every function that does I/O, blocks, or spawns goroutines.
- `context.Background()` / `context.TODO()` are allowed **only** in `main` and tests.
- Every DB call has a deadline. The store wraps calls with `DB_QUERY_TIMEOUT` (5s) unless the caller's ctx is tighter.
- Required deadlines:

| Operation | Deadline |
|---|---|
| HTTP request DB work | 5s (`DB_QUERY_TIMEOUT`) |
| `/readyz` ping | 2s |
| `/metrics` / stats queries | 3s |
| handler execution | `timeout_seconds` via `context.WithTimeout` |
| outcome write after execution | `context.WithTimeout(context.WithoutCancel(ctx), 5s)`, so it survives shutdown cancellation |
| migrations (incl. advisory lock wait) | 60s |
| outbound HTTP (`http_webhook`) | the attempt ctx; `http.Client{Timeout: 0}`, relying on ctx |
| graceful shutdown | `SHUTDOWN_TIMEOUT` (25s) |

- `http.Server` must set `ReadHeaderTimeout: 5s`, `ReadTimeout: 15s`, `WriteTimeout: 30s`, `IdleTimeout: 60s`.
- `time.Sleep` is forbidden in production code. Use `select { case <-ctx.Done(): …; case <-timer.C: … }`. In tests use the `eventually()` helper.

## 4. Errors — zero swallowed

- Never `_ = f()` or ignore a returned `error`. The only exceptions are `defer resp.Body.Close()` and `defer rows.Close()` (pgx `Rows.Close` returns nothing). Anything else that's ignored must be logged with the reason.
- Wrap errors with context: `fmt.Errorf("claim jobs: %w", err)`. Compare with `errors.Is` / `errors.As`, never by string matching.
- Sentinel errors: `domain.ErrPermanent`, `store.ErrNotFound`, `store.ErrFenceLost`, `store.ErrIdempotencyConflict`, `store.ErrConflict`, `store.ErrDraining`. Handlers map them to HTTP codes in **one** function.
- Map `pgx.ErrNoRows` to `store.ErrNotFound` inside `store`. Detect unique violations with `pgconn.PgError.Code == "23505"`.
- Transactions use a single `store.withTx(ctx, fn)` helper built on `pgx.BeginTxFunc`, which commits on nil and rolls back otherwise. No hand-rolled `Begin`/`Commit`.
- No `panic` outside `main` init failures. The HTTP middleware and the executor both `recover()`, log with a stack trace, and convert to an error.
- `log.Fatal` / `os.Exit` are allowed only in `main`.
- Never log job `payload` or `result` bodies (they may contain secrets). Log their sizes instead.

## 5. SQL Rules (atomic, fenced)

1. **Every state transition is one statement or one `withTx`.** Never SELECT-then-UPDATE without `FOR UPDATE`.
2. **Every worker-originated `UPDATE jobs`** includes `WHERE id=$1 AND status='running' AND attempt_count=$2 AND worker_id=$3`, and checks `CommandTag.RowsAffected()`. 0 rows → `store.ErrFenceLost` (and the attempt row is marked `fenced`, SPEC §6.5). The only exception is the reaper, which uses the locked row's own token.
3. Retry vs dead-letter guards (`attempt_count <= max_retries` / `> max_retries`) live **in SQL**, not only in Go.
4. Dead-lettering updates `jobs` and inserts `dead_letter_jobs` in one CTE statement (SPEC §6.4).
5. Claims, reaping, and cron ticks use `FOR UPDATE … SKIP LOCKED` with a `LIMIT`. Never use unbounded scans on hot paths.
6. **Time comes from Postgres.** Use `now()` for `run_at`, leases, heartbeats, and windows. Go passes durations as `interval` params (`$4::interval`). `time.Now()` is only for measuring `duration_ms` (monotonic) and for logs.
7. Parameterized queries only. The one exception is identifier interpolation of `DB_SCHEMA`, which must match `^[a-z_][a-z0-9_]{0,62}$` and be quoted with `pgx.Identifier{…}.Sanitize()`.
8. **Never hold a pool connection or tx while a handler runs.** Claim → release → execute → write outcome.
9. Every list endpoint has a `LIMIT` (default 50, max 500).
10. Migrations are append-only (`0002_….sql`), idempotent (`IF NOT EXISTS`), and run under the advisory lock. Never use `CREATE DATABASE`, `DROP` of existing data, or superuser-only statements (no extensions required).
11. Status strings come from `domain` constants and must match the SQL `CHECK` lists exactly (`dead-lettered` has a hyphen).

## 6. Concurrency

- Every goroutine has an owner, an exit path via `ctx.Done()`, and is tracked by a `sync.WaitGroup` that the owner waits on during shutdown.
- Only senders close channels. No unbuffered sends without a `select` on `ctx.Done()`.
- Worker concurrency is bounded by a semaphore (buffered channel) of size `WORKER_CONCURRENCY`. `active_jobs` is an `atomic.Int64`.
- Polling, reaper, and cron intervals get ±20% jitter so N workers don't synchronize.
- All tests run with `-race`. Data races are build failures.

## 7. Graceful Shutdown (SIGTERM / SIGINT)

`main` uses `signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)`.

**Worker:**
1. Cancel the loop ctx, which stops the poller, cron tick, and reaper. Claim no new jobs after this point.
2. Heartbeat `status='draining'`.
3. Wait for in-flight executions up to `SHUTDOWN_TIMEOUT`.
4. On timeout, cancel the execution ctx. Outcomes are still persisted via the `WithoutCancel` write ctx (transient → T5/T6, `worker shutdown`).
5. Final heartbeat `status='stopped'`, `pool.Close()`, exit 0.

**API:** mark not-ready (`/readyz` → 503) → `srv.Shutdown(ctx)` with a 15s timeout → `pool.Close()` → exit 0.

A missed or duplicated shutdown step must never corrupt state. The lease reaper is the backstop for SIGKILL.

## 8. HTTP Conventions

- Routing uses Go 1.22 `http.ServeMux` patterns (`"GET /v1/jobs/{id}"`, `r.PathValue("id")`).
- Middleware order: request-ID → panic recover → access log (slog: `request_id, method, path, route, status, latency_ms`; `route` is the mux pattern without its method, which is also the `jobq_http_requests_total` label) → bearer auth (if `API_TOKEN`) → body limit (1 MiB) → handler.
- Decode JSON with `json.NewDecoder` + `DisallowUnknownFields()`, and reject trailing data.
- Responses go through `writeJSON(w, status, v)` / `writeError(w, status, code, msg)` only. Errors use the envelope in SPEC §8.
- Validation lives in `controlplane/validate.go`, is pure, and is unit tested. Return the first error with the field name.

## 9. Logging

- `slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))` is set as the default once in `main`.
- Standard keys (snake_case): `mode, worker_id, job_id, type, attempt, status, duration_ms, err, request_id`.
- Levels: `error` for things an operator must act on (DB down, invariant violated); `warn` for fenced writes, abandoned handler goroutines, lease reaps, and retries exhausted; `info` for lifecycle and state transitions; `debug` for poll cycles.

## 10. Testing

- Unit tests are table-driven, stdlib `testing` only, deterministic: inject the rand source (backoff) and a clock where Go time matters.
- Integration tests:
  - Use the `//go:build integration` tag.
  - Skip with a clear message if `TEST_DATABASE_URL` is unset locally. CI sets it, so they always run there.
  - Each test creates schema `t_<random>` via `DB_SCHEMA`, runs migrations, calls `t.Parallel()`, and drops the schema in `t.Cleanup`.
  - Synchronize with `eventually(t, timeout, func() bool)`, never fixed sleeps.
  - Assert on DB state (`jobs`, `job_attempts`, `dead_letter_jobs`), not only on API responses.
- Every invariant in SPEC §7 has a named test (`TestInvariantI2_ZombieFencing`, …).
- Must pass with `-race -count=1`. A flaky test is a failing test: fix the cause instead of retrying.

## 11. Security & Ops Hygiene

- The DigitalOcean token lives **only** in `/workspaces/.secrets/digitalocean.env` (outside the repo, mode 600). Always `source` it to get `DIGITALOCEAN_ACCESS_TOKEN`. Never copy the value into files, commands or commits, and never recall it from memory.
- Secrets come only from env (`DATABASE_URL`, `API_TOKEN`). Never log them. Redact the DSN password in startup logs.
- The container runs as distroless `nonroot`. No shell, no package manager.
- `http_webhook`: http/https only, capped body read, bounded redirects. Document SSRF exposure in the README and recommend setting `API_TOKEN` on public deployments.
- `sslmode` in `DATABASE_URL` is respected as-is. Never force `disable` in code.

## 12. Forbidden

`CREATE DATABASE` · SQL outside `internal/store` · unfenced worker updates · `time.Now()` in scheduling SQL params · holding DB connections during handler execution · unbounded queries · `time.Sleep` in prod code · swallowed errors · global mutable state · new dependencies without approval · logging payloads or secrets · editing applied migrations.
