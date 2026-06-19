package postgres

import (
	"maps"
	"testing"
	"time"
)

func TestDBConfig_ConnRuntimeParams(t *testing.T) {
	tests := []struct {
		name string
		cfg  DBConfig
		want map[string]string
	}{
		{"none", DBConfig{}, nil},
		{"lock only", DBConfig{LockTimeout: 10 * time.Second}, map[string]string{"lock_timeout": "10000"}},
		{"statement only", DBConfig{StatementTimeout: 60 * time.Second}, map[string]string{"statement_timeout": "60000"}},
		{
			"both",
			DBConfig{LockTimeout: 10 * time.Second, StatementTimeout: 60 * time.Second},
			map[string]string{"lock_timeout": "10000", "statement_timeout": "60000"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.connRuntimeParams(); !maps.Equal(got, tt.want) {
				t.Errorf("connRuntimeParams() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultDBConfig_NoTimeouts(t *testing.T) {
	cfg := DefaultDBConfig("postgres://x")
	// DefaultDBConfig leaves both timeouts unset so shared long-running callers
	// (backfillers, validators, crons) are never capped. Latency-bounded SQS
	// workers opt in via WorkerDBConfig.
	if cfg.LockTimeout != 0 {
		t.Errorf("DefaultDBConfig LockTimeout = %v, want 0 (unset)", cfg.LockTimeout)
	}
	if cfg.StatementTimeout != 0 {
		t.Errorf("DefaultDBConfig StatementTimeout = %v, want 0 (unset)", cfg.StatementTimeout)
	}
}

func TestWorkerDBConfig_SetsLockTimeout(t *testing.T) {
	cfg := WorkerDBConfig("postgres://x")
	if cfg.LockTimeout != 10*time.Second {
		t.Errorf("WorkerDBConfig LockTimeout = %v, want 10s", cfg.LockTimeout)
	}
	if cfg.StatementTimeout != 0 {
		t.Errorf("WorkerDBConfig StatementTimeout = %v, want 0 (unset)", cfg.StatementTimeout)
	}
	if cfg.MaxConns != 25 {
		t.Errorf("WorkerDBConfig MaxConns = %d, want 25 (inherited from DefaultDBConfig)", cfg.MaxConns)
	}
}
