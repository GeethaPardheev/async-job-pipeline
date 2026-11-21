package store

import (
	"strings"
	"testing"
)

func TestLoadMigrations(t *testing.T) {
	ms, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	if len(ms) == 0 || ms[0].version != 1 {
		t.Fatalf("expected migrations starting at version 1, got %+v", ms)
	}
	for i, m := range ms {
		if i > 0 && m.version <= ms[i-1].version {
			t.Fatalf("migrations not strictly ordered: %d after %d", m.version, ms[i-1].version)
		}
		if strings.Contains(strings.ToUpper(m.sql), "CREATE DATABASE") {
			t.Fatalf("migration %s must not CREATE DATABASE", m.name)
		}
	}
}
