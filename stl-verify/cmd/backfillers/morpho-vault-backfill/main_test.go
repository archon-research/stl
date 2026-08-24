package main

import (
	"context"
	"strings"
	"testing"
)

// The env-validation path resolves before any database, S3 or RPC access, so it
// runs as a plain unit test; the wiring behind it is covered by the integration
// test (main_integration_test.go).
func TestRun_RequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")

	err := run(context.Background())
	if err == nil {
		t.Fatal("missing DATABASE_URL should error, got nil")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Errorf("error %q should mention DATABASE_URL", err.Error())
	}
}
