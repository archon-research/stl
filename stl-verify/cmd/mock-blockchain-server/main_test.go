package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestRun_StartStop verifies that run starts successfully and shuts down cleanly when the context is cancelled.
func TestRun_StartStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	addrCh := make(chan string, 1)
	errCh := make(chan error, 1)

	go func() {
		errCh <- run(ctx, ":0", addrCh)
	}()

	var addr string
	select {
	case addr = <-addrCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server to start")
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for run to return after cancel")
	}

	_ = addr
}

// TestRun_HTTP verifies that the server responds to eth_blockNumber over HTTP.
func TestRun_HTTP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addrCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx, ":0", addrCh) }()

	var addr string
	select {
	case addr = <-addrCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server to start")
	}

	body := `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`
	resp, err := http.Post("http://"+addr, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("HTTP POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		Result string `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if result.Result == "" {
		t.Fatal("expected non-empty result")
	}

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for run to return after cancel")
	}
}

// TestRun_BadAddr verifies that run returns an error when given an invalid address.
func TestRun_BadAddr(t *testing.T) {
	ctx := context.Background()
	err := run(ctx, "invalid:addr:extra", nil)
	if err == nil {
		t.Fatal("expected error for invalid address, got nil")
	}
}
