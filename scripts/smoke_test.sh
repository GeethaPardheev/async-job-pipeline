#!/usr/bin/env bash
# End-to-end smoke test against a running deployment.
#
#   BASE_URL=https://example.ondigitalocean.app [API_TOKEN=...] [EXPECT_WORKERS=2] scripts/smoke_test.sh
#
# Requires bash, curl and python3. Exits non-zero on the first failed assertion.
# The drain step pauses the shared queue briefly; an exit trap always resumes it.
set -euo pipefail

BASE_URL="${BASE_URL:?set BASE_URL, e.g. https://my-app.ondigitalocean.app}"
BASE_URL="${BASE_URL%/}"
API_TOKEN="${API_TOKEN:-}"
EXPECT_WORKERS="${EXPECT_WORKERS:-}"
WAIT_SECONDS="${WAIT_SECONDS:-90}"
RUN_ID="smoke-$(date +%s)-$RANDOM"

TMP="$(mktemp -d)"
DRAINED=0
STATUS=""
BODY=""
HEADERS=""
PASSED=0

cleanup() {
  local rc=$?
  if [[ $DRAINED == 1 ]]; then
    echo "!! resuming queue after interrupted drain step" >&2
    curl -sS -o /dev/null -X POST ${AUTH[@]+"${AUTH[@]}"} "$BASE_URL/v1/queue/resume" || echo "!! resume failed; run POST /v1/queue/resume manually" >&2
  fi
  rm -rf "$TMP"
  exit $rc
}
trap cleanup EXIT

AUTH=()
if [[ -n $API_TOKEN ]]; then
  AUTH=(-H "Authorization: Bearer $API_TOKEN")
fi

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
fail() {
  printf 'FAIL: %s\n  last response: HTTP %s %s\n' "$1" "$STATUS" "${BODY:0:600}" >&2
  exit 1
}
ok() {
  PASSED=$((PASSED + 1))
  log "ok   $*"
}

# req METHOD PATH [BODY] [extra curl args...]; sets STATUS, BODY, HEADERS.
req() {
  local method=$1 path=$2 data=${3:-}
  shift $(($# < 3 ? $# : 3))
  local args=(-sS -X "$method" -o "$TMP/body" -D "$TMP/headers" -w '%{http_code}' --max-time 30)
  if [[ -n $data ]]; then
    args+=(-H 'Content-Type: application/json' --data-binary "$data")
  fi
  STATUS="$(curl "${args[@]}" "$@" "$BASE_URL$path")" || fail "curl $method $path"
  BODY="$(cat "$TMP/body")"
  HEADERS="$(tr -d '\r' <"$TMP/headers")"
}
api() { req "$1" "$2" "${3:-}" ${AUTH[@]+"${AUTH[@]}"} "${@:4}"; }

# jget EXPR: evaluates a Python expression against the parsed JSON body bound to `j`.
jget() {
  printf '%s' "$BODY" | python3 -c '
import json, sys
j = json.load(sys.stdin)
v = eval(sys.argv[1], {}, {"j": j})
print(json.dumps(v) if isinstance(v, (dict, list, bool)) or v is None else v)' "$1"
}
header() { printf '%s\n' "$HEADERS" | awk -v k="$(echo "$1" | tr '[:upper:]' '[:lower:]')" -F': ' 'tolower($1)==k {print $2}' | tail -1; }

expect_status() {
  [[ $STATUS == "$1" ]] || fail "$2: expected HTTP $1, got $STATUS"
}

submit() { # submit BODY [extra curl args...] -> prints job_id; expects 202
  api POST /v1/jobs "$1" "${@:2}"
  expect_status 202 "submit $1"
  jget 'j["job_id"]'
}

wait_status() { # wait_status ID STATUS
  local id=$1 want=$2 deadline=$((SECONDS + WAIT_SECONDS)) got=""
  while ((SECONDS < deadline)); do
    api GET "/v1/jobs/$id"
    expect_status 200 "get job $id"
    got="$(jget 'j["status"]')"
    [[ $got == "$want" ]] && return 0
    case "$got" in succeeded | failed | dead-lettered | cancelled) fail "job $id reached $got, expected $want" ;; esac
    sleep 1
  done
  fail "job $id still $got after ${WAIT_SECONDS}s, expected $want"
}

future() { python3 -c 'import datetime as d; print((d.datetime.now(d.timezone.utc)+d.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"))'; }

log "smoke test against $BASE_URL (run $RUN_ID, auth $([[ -n $API_TOKEN ]] && echo on || echo off))"

# --- health (never behind auth) -------------------------------------------------
req GET /healthz
expect_status 200 "/healthz"
req GET /readyz
expect_status 200 "/readyz"
[[ $(jget 'j["status"]') == ready ]] || fail "/readyz status"
ok "healthz/readyz 200 without auth"

if [[ -n $API_TOKEN ]]; then
  req GET /v1/jobs
  expect_status 401 "/v1/jobs without token"
  ok "/v1/* without token -> 401"
fi

# --- idempotency ----------------------------------------------------------------
KEY="$RUN_ID-echo"
ECHO_BODY='{"type":"echo","payload":{"run":"'"$RUN_ID"'","n":1}}'
api POST /v1/jobs "$ECHO_BODY" -H "Idempotency-Key: $KEY"
expect_status 202 "first idempotent submit"
ECHO_ID="$(jget 'j["job_id"]')"
api POST /v1/jobs "$ECHO_BODY" -H "Idempotency-Key: $KEY"
expect_status 200 "repeat idempotent submit"
[[ $(jget 'j["job_id"]') == "$ECHO_ID" ]] || fail "repeat returned a different job"
[[ $(header Idempotent-Replayed) == true ]] || fail "missing Idempotent-Replayed header"
api POST /v1/jobs '{"type":"echo","payload":{"run":"'"$RUN_ID"'","n":2}}' -H "Idempotency-Key: $KEY"
expect_status 409 "same key, mutated body"
[[ $(jget 'j["error"]["code"]') == idempotency_key_conflict ]] || fail "409 code"
ok "idempotency 202 -> 200 (same job) -> 409 idempotency_key_conflict"

# --- lifecycle --------------------------------------------------------------------
FLAKY_ID="$(submit '{"type":"flaky","payload":{"fail_times":2},"max_retries":3}')"
SLEEP_ID="$(submit '{"type":"sleep","payload":{"duration_ms":4000},"timeout_seconds":1,"max_retries":1}')"
PERM_ID="$(submit '{"type":"permanent_fail"}')"
DEAD_ID="$(submit '{"type":"always_fail","max_retries":1}')"

wait_status "$ECHO_ID" succeeded
ok "echo -> succeeded"

wait_status "$FLAKY_ID" succeeded
[[ $(jget 'j["attempt_count"]') == 3 ]] || fail "flaky attempt_count"
[[ $(jget '[a["status"] for a in j["attempts"]]') == '["succeeded", "failed", "failed"]' ]] || fail "flaky attempts"
ok "flaky -> failed, failed, succeeded (3 attempts with backoff)"

wait_status "$SLEEP_ID" dead-lettered
[[ $(jget '[a["status"] for a in j["attempts"]]') == '["timed_out", "timed_out"]' ]] || fail "sleep attempts"
ok "sleep 4s with timeout 1s -> timed_out, retried, timed_out -> dead-lettered"

wait_status "$PERM_ID" failed
[[ $(jget 'j["attempt_count"]') == 1 ]] || fail "permanent_fail must not retry"
ok "permanent_fail -> failed after 1 attempt (no retry)"

wait_status "$DEAD_ID" dead-lettered
[[ $(jget 'j["attempt_count"]') == 2 ]] || fail "always_fail attempt_count"
api GET "/v1/dlq?limit=500"
expect_status 200 "list dlq"
[[ $(jget "any(e['job_id'] == '$DEAD_ID' for e in j['entries'])") == true ]] || fail "always_fail job missing from DLQ"
ok "always_fail -> 2 attempts -> dead-lettered -> listed in /v1/dlq"

api POST "/v1/dlq/$DEAD_ID/replay"
expect_status 202 "dlq replay"
REPLAY_ID="$(jget 'j["job_id"]')"
[[ $(jget 'j["replay_of"]') == "$DEAD_ID" && $REPLAY_ID != "$DEAD_ID" ]] || fail "replay body"
api POST "/v1/dlq/$DEAD_ID/replay"
expect_status 200 "repeat dlq replay"
[[ $(jget 'j["job_id"]') == "$REPLAY_ID" ]] || fail "repeat replay returned a different job"
wait_status "$REPLAY_ID" dead-lettered
ok "dlq replay -> 202 new job $REPLAY_ID; repeat -> 200 same job; replay ran (and dead-lettered again)"

# --- cancel -----------------------------------------------------------------------
CANCEL_ID="$(submit '{"type":"echo","run_at":"'"$(future)"'"}')"
api POST "/v1/jobs/$CANCEL_ID/cancel"
expect_status 200 "cancel queued job"
[[ $(jget 'j["status"]') == cancelled ]] || fail "cancel body"
api POST "/v1/jobs/$ECHO_ID/cancel"
expect_status 409 "cancel succeeded job"
ok "cancel queued -> 200 cancelled; cancel terminal -> 409"

# --- drain / resume ---------------------------------------------------------------
DRAINED=1
api POST /v1/queue/drain '{"mode":"pause"}'
expect_status 200 "drain pause"
[[ $(jget 'j["draining"]') == true ]] || fail "drain body"
api POST /v1/jobs '{"type":"echo"}'
[[ $(header Retry-After) == 30 ]] || fail "submit while draining: Retry-After"
# App Platform's edge replaces every origin 503 with its own HTML 504 and records the
# original status in x-do-orig-status; the JSON envelope only survives elsewhere.
if [[ $STATUS == 504 && $(header X-Do-Orig-Status) == 503 ]]; then
  DRAIN_REJECT="503 (rewritten by the DigitalOcean edge to 504, x-do-orig-status: 503)"
else
  expect_status 503 "submit while draining"
  [[ $(jget 'j["error"]["code"]') == queue_draining ]] || fail "503 error code"
  DRAIN_REJECT="503 queue_draining"
fi
api POST /v1/queue/resume
expect_status 200 "resume"
DRAINED=0
AFTER_ID="$(submit '{"type":"echo"}')"
wait_status "$AFTER_ID" succeeded
ok "drain pause -> submit $DRAIN_REJECT, Retry-After 30 -> resume -> submit 202 -> succeeded"

# --- observability ----------------------------------------------------------------
api GET /v1/queue/stats
expect_status 200 "stats"
WORKERS="$(jget 'j["active_workers"]')"
log "     stats: active_workers=$WORKERS total_concurrency=$(jget 'j["total_concurrency"]') utilization=$(jget 'j["worker_utilization"]') p50_ms=$(jget 'j["p50_latency_ms"]') p95_ms=$(jget 'j["p95_latency_ms"]') dead_letter_count=$(jget 'j["dead_letter_count"]') dead_letter_rate=$(jget 'j["dead_letter_rate"]')"
if [[ -n $EXPECT_WORKERS ]]; then
  [[ $WORKERS == "$EXPECT_WORKERS" ]] || fail "active_workers=$WORKERS, expected $EXPECT_WORKERS"
else
  ((WORKERS >= 1)) || fail "no active workers"
fi
[[ $(jget 'j["p50_latency_ms"] is not None and j["p95_latency_ms"] is not None and j["p95_latency_ms"] >= j["p50_latency_ms"]') == true ]] || fail "p50/p95 latency"
[[ $(jget 'j["dead_letter_count"] >= 2 and j["draining"] is False') == true ]] || fail "dead_letter_count / draining"
ok "stats: active_workers=$WORKERS, p50/p95 present, dead_letter_count >= 2"

req GET /metrics
expect_status 200 "/metrics"
[[ $(header Content-Type) == "text/plain; version=0.0.4"* ]] || fail "metrics content type"
METRICS="$BODY"
mval() { printf '%s\n' "$METRICS" | awk -v k="$1" '$1==k {print $2}'; }
[[ $(mval jobq_active_workers) == "$WORKERS" ]] || fail "jobq_active_workers != stats active_workers"
for m in 'jobq_attempt_latency_ms{quantile="0.5"}' 'jobq_attempt_latency_ms{quantile="0.95"}' jobq_dead_letter_jobs jobq_worker_utilization jobq_draining; do
  [[ -n $(mval "$m") ]] || fail "metric $m missing"
done
[[ $(mval jobq_draining) == 0 ]] || fail "jobq_draining"
ok "/metrics: jobq_active_workers=$(mval jobq_active_workers) jobq_total_concurrency=$(mval jobq_total_concurrency) jobq_dead_letter_jobs=$(mval jobq_dead_letter_jobs) p95=$(mval 'jobq_attempt_latency_ms{quantile="0.95"}')"

log "PASS: $PASSED checks against $BASE_URL"
