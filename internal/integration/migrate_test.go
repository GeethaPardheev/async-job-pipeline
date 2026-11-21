//go:build integration

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/GeethaPardheev/async-job-pipeline/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

var expectedTables = []string{"queue_state", "schedules", "jobs", "job_attempts", "dead_letter_jobs", "workers", "schema_migrations"}

var expectedIndexes = []string{
	"jobs_claim_order_idx", "jobs_lease_idx", "jobs_status_created_idx", "jobs_finished_idx",
	"job_attempts_finished_idx", "dead_letter_jobs_at_idx", "schedules_due_idx", "workers_heartbeat_idx",
}

func TestMigrationsIdempotent(t *testing.T) {
	t.Parallel()
	schema := newSchemaName(t)
	st := openStore(t, schema)
	dropSchemaOnCleanup(t, st, schema)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	first, err := st.Migrate(ctx)
	if err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if len(first) == 0 {
		t.Fatal("first migrate applied nothing")
	}
	second, err := st.Migrate(ctx)
	if err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("second migrate applied %v, want none", second)
	}
	assertSchema(t, ctx, st.Pool(), schema, len(first))
}

func TestMigrationsConcurrent(t *testing.T) {
	t.Parallel()
	schema := newSchemaName(t)
	const replicas = 6
	stores := make([]*store.Store, replicas)
	for i := range stores {
		stores[i] = openStore(t, schema)
	}
	dropSchemaOnCleanup(t, stores[0], schema)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	results := make([][]int, replicas)
	errs := make([]error, replicas)
	for i := range stores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = stores[i].Migrate(ctx)
		}(i)
	}
	wg.Wait()

	total := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("replica %d migrate: %v", i, err)
		}
		total += len(results[i])
	}
	var versions int
	if err := stores[0].Pool().QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&versions); err != nil {
		t.Fatalf("count versions: %v", err)
	}
	if total != versions {
		t.Fatalf("replicas applied %d migrations in total, schema_migrations has %d", total, versions)
	}
	assertSchema(t, ctx, stores[0].Pool(), schema, versions)
}

func assertSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema string, wantVersions int) {
	t.Helper()
	for _, table := range expectedTables {
		var ok bool
		if err := pool.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
			schema, table).Scan(&ok); err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !ok {
			t.Errorf("table %s.%s missing", schema, table)
		}
	}
	for _, idx := range expectedIndexes {
		var ok bool
		if err := pool.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = $2)",
			schema, idx).Scan(&ok); err != nil {
			t.Fatalf("check index %s: %v", idx, err)
		}
		if !ok {
			t.Errorf("index %s.%s missing", schema, idx)
		}
	}
	var versions, queueRows int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&versions); err != nil {
		t.Fatalf("count versions: %v", err)
	}
	if versions != wantVersions {
		t.Errorf("schema_migrations rows = %d, want %d", versions, wantVersions)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM queue_state WHERE NOT draining").Scan(&queueRows); err != nil {
		t.Fatalf("count queue_state: %v", err)
	}
	if queueRows != 1 {
		t.Errorf("queue_state rows = %d, want exactly 1 non-draining row", queueRows)
	}
}
