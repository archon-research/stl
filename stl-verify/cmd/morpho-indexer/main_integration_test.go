//go:build integration

package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Integration tests for run()
// ---------------------------------------------------------------------------

func TestRunIntegration_BadDatabaseURL(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)

	err := run(context.Background(), []string{
		"-queue", "http://localhost/test-queue",
		"-redis", "localhost:6379",
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
	})
	if err == nil {
		t.Fatal("expected error for bad database URL")
	}
	// Could fail at Redis connection or database connection
	if !strings.Contains(err.Error(), "database") && !strings.Contains(err.Error(), "connect") && !strings.Contains(err.Error(), "Redis") {
		t.Errorf("expected connection error, got: %v", err)
	}
}

func TestRunIntegration_StartupAndShutdown(t *testing.T) {
	_, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	}))
	defer rpcServer.Close()

	sqsServer, sqsState := testutil.StartMockSQS(t)
	defer sqsServer.Close()

	redisAddr, redisCleanup := startRedisContainer(t)
	defer redisCleanup()

	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("AWS_SQS_ENDPOINT", sqsServer.URL)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{
			"-queue", "http://localhost/test-queue",
			"-db", dbURL,
			"-redis", redisAddr,
		})
	}()

	// Wait for the service to start (SQS ReceiveMessage call indicates it's polling)
	select {
	case <-sqsState.FirstCallReceived:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for service to start")
	}

	// Service is running and polling SQS. Trigger graceful shutdown.
	cancel()

	// Wait for run() to return
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}

	// Verify SQS was polled
	if receives := sqsState.Receives(); receives < 1 {
		t.Errorf("expected at least 1 ReceiveMessage call, got %d", receives)
	}
}

// startRedisContainer starts a Redis container using testcontainers.
func startRedisContainer(t *testing.T) (addr string, cleanup func()) {
	t.Helper()
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start Redis container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("get Redis host: %v", err)
	}
	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatalf("get Redis port: %v", err)
	}

	addr = fmt.Sprintf("%s:%s", host, port.Port())
	cleanup = func() { _ = container.Terminate(ctx) }
	return addr, cleanup
}
