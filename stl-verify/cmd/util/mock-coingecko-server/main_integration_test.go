//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestMain(m *testing.M) {
	// No services needed; RunShared still owns the goroutine leak check.
	os.Exit(testutil.RunShared(m, testutil.Shared{}))
}

// The binary must serve the mock over a real socket and stop cleanly on
// context cancellation — the lifecycle the kind Deployment relies on.
func TestIntegration_Run_ServesAndShutsDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, testutil.DiscardLogger(), "127.0.0.1:18099", 0) }()

	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:18099/simple/price?ids=ripple", nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.Header.Set("x-cg-pro-api-key", "any")

	var resp *http.Response
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err = http.DefaultClient.Do(req)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("server never came up: %v", err)
	}
	defer resp.Body.Close()

	var body map[string]map[string]float64
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if body["ripple"]["usd"] <= 0 {
		t.Errorf("expected a positive ripple price, got %v", body["ripple"])
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("run returned an error on shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Error("run did not stop within 10s of cancellation")
	}
}
