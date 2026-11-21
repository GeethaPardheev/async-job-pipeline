-- 0002: claim order is priority DESC, run_at, created_at (id breaks exact ties).
CREATE INDEX IF NOT EXISTS jobs_claim_order_idx ON jobs (priority DESC, run_at, created_at, id) WHERE status = 'queued';
DROP INDEX IF EXISTS jobs_claim_idx;
