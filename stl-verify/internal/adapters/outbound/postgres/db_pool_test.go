package postgres

import (
	"testing"
	"time"
)

// TestBuildPoolConfig_TimeoutsNotStartupParams pins the invariant behind the
// 2026-06-19 staging crashloop: lock_timeout/statement_timeout must NOT ride the
// startup packet. The indexers connect through a pgbouncer-style pooler, which
// rejects unknown startup parameters ("FATAL: unsupported startup parameter:
// lock_timeout"), so the timeouts must be applied with a post-connect SET via
// AfterConnect instead.
func TestBuildPoolConfig_TimeoutsNotStartupParams(t *testing.T) {
	cfg := WorkerDBConfig("postgres://u:p@localhost:5432/db")
	cfg.StatementTimeout = 45 * time.Second

	pc, err := buildPoolConfig(cfg)
	if err != nil {
		t.Fatalf("buildPoolConfig: %v", err)
	}

	for _, guc := range []string{"lock_timeout", "statement_timeout"} {
		if v, ok := pc.ConnConfig.RuntimeParams[guc]; ok {
			t.Errorf("%s=%q present in startup RuntimeParams; a pooler rejects unknown startup params, apply it via AfterConnect SET instead", guc, v)
		}
	}

	if pc.AfterConnect == nil {
		t.Error("AfterConnect is nil with timeouts configured; they would never be applied")
	}
}

func TestBuildPoolConfig_NoTimeoutsNoAfterConnect(t *testing.T) {
	pc, err := buildPoolConfig(DefaultDBConfig("postgres://u:p@localhost:5432/db"))
	if err != nil {
		t.Fatalf("buildPoolConfig: %v", err)
	}

	if pc.AfterConnect != nil {
		t.Error("AfterConnect set for DefaultDBConfig (no timeouts); want nil to avoid a wasted round-trip on every new connection")
	}
}
