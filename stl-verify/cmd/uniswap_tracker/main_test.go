package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRun_BadDatabaseURL(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("DATABASE_URL", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1")

	err := run(context.Background())
	if err == nil {
		t.Fatal("expected error for bad database URL")
	}
	if !strings.Contains(err.Error(), "database") && !strings.Contains(err.Error(), "connect") {
		t.Errorf("expected connection error, got: %v", err)
	}
}

func TestRun_MissingDatabaseURL(t *testing.T) {
	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("DATABASE_URL", "")

	err := run(context.Background())
	if err == nil {
		t.Fatal("expected error for missing DATABASE_URL")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Errorf("expected DATABASE_URL error, got: %v", err)
	}
}

func TestRun_MissingAlchemyKey(t *testing.T) {
	t.Setenv("ALCHEMY_API_KEY", "")
	t.Setenv("DATABASE_URL", "postgres://localhost:5432/test")

	err := run(context.Background())
	if err == nil {
		t.Fatal("expected error for missing ALCHEMY_API_KEY")
	}
	if !strings.Contains(err.Error(), "ALCHEMY_API_KEY") {
		t.Errorf("expected ALCHEMY_API_KEY error, got: %v", err)
	}
}
