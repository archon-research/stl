//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Mock SQS server (AWS JSON 1.0 protocol)
// ---------------------------------------------------------------------------

type mockSQSServer struct {
	mu                sync.Mutex
	messageDelivered  bool
	receiveCallCount  int
	deleteCallCount   int
	firstCallReceived chan struct{}
}

func startMockSQS(t *testing.T) (*httptest.Server, *mockSQSServer) {
	t.Helper()

	state := &mockSQSServer{
		firstCallReceived: make(chan struct{}, 1),
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		target := r.Header.Get("X-Amz-Target")

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")

		switch {
		case strings.Contains(target, "ReceiveMessage"):
			state.mu.Lock()
			state.receiveCallCount++
			delivered := state.messageDelivered
			state.messageDelivered = true
			state.mu.Unlock()

			// Signal first call received
			select {
			case state.firstCallReceived <- struct{}{}:
			default:
			}

			if !delivered {
				// First call: deliver one block event message
				blockEvent := `{"chainId":1,"blockNumber":18000000,"version":1,"blockHash":"0xabc","blockTimestamp":1700000000}`
				fmt.Fprintf(w, `{"Messages":[{"MessageId":"msg-1","ReceiptHandle":"handle-1","Body":%s}]}`,
					mustMarshal(blockEvent))
			} else {
				// Subsequent calls: no messages
				fmt.Fprint(w, `{"Messages":[]}`)
			}

		case strings.Contains(target, "DeleteMessage"):
			state.mu.Lock()
			state.deleteCallCount++
			state.mu.Unlock()
			fmt.Fprint(w, `{}`)

		default:
			// Handle any unexpected actions gracefully
			_ = body
			fmt.Fprint(w, `{}`)
		}
	}))

	return server, state
}

func mustMarshal(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// ---------------------------------------------------------------------------
// Integration tests for run()
// ---------------------------------------------------------------------------

func TestRunIntegration_HappyPath(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()

	bgCtx := context.Background()

	var tokenCount int
	if err := pool.QueryRow(bgCtx,
		`SELECT COUNT(*) FROM oracle_asset oa
		 JOIN oracle os ON os.id = oa.oracle_id
		 WHERE os.name = 'sparklend' AND oa.enabled = true`).Scan(&tokenCount); err != nil {
		t.Fatalf("count tokens: %v", err)
	}
	if tokenCount == 0 {
		t.Fatal("no seeded oracle assets found")
	}

	rpcServer := testutil.StartMockEthRPC(t, tokenCount)
	defer rpcServer.Close()

	sqsServer, sqsState := startMockSQS(t)
	defer sqsServer.Close()

	// Configure environment for run()
	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("AWS_SQS_ENDPOINT", sqsServer.URL)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	// Use a cancellable context instead of SIGINT for clean test shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{
			"-queue", "http://localhost/test-queue",
			"-db", dbURL,
		})
	}()

	// Wait for the service to start (SQS ReceiveMessage call indicates it's polling)
	select {
	case <-sqsState.firstCallReceived:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for service to start")
	}

	// Wait for the block event to be processed (prices stored in DB)
	testutil.WaitForCondition(t, 10*time.Second, func() bool {
		var count int
		pool.QueryRow(bgCtx, `SELECT COUNT(*) FROM onchain_token_price`).Scan(&count)
		return count >= tokenCount
	}, "prices to be stored in DB")

	// Cancel the context to trigger graceful shutdown.
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

	// Verify prices
	var priceCount int
	pool.QueryRow(bgCtx, `SELECT COUNT(*) FROM onchain_token_price`).Scan(&priceCount)
	if priceCount < tokenCount {
		t.Errorf("expected at least %d prices, got %d", tokenCount, priceCount)
	}

	// Verify DeleteMessage was called
	sqsState.mu.Lock()
	deletes := sqsState.deleteCallCount
	sqsState.mu.Unlock()
	if deletes < 1 {
		t.Errorf("expected at least 1 DeleteMessage call, got %d", deletes)
	}
}

func TestRunIntegration_BadDatabaseURL(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-api-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)

	err := run(context.Background(), []string{
		"-queue", "http://localhost/test-queue",
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
	})
	if err == nil {
		t.Fatal("expected error for bad database URL")
	}
	if !strings.Contains(err.Error(), "database") && !strings.Contains(err.Error(), "connect") {
		t.Errorf("expected database/connect error, got: %v", err)
	}
}
