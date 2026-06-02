package alchemy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// jsonOKHandler returns a JSON-RPC handler that replies "0x10" to every call
// and atomically counts how many HTTP requests landed. Used by the rate-limit
// tests to assert both pacing and that ctx cancellation prevents the request
// from being sent in the first place.
func jsonOKHandler(t *testing.T, counter *atomic.Int64) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, _ *http.Request) {
		counter.Add(1)
		resp := jsonRPCResponse{JSONRPC: "2.0", ID: 1, Result: json.RawMessage(`"0x10"`)}
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func TestRateLimiter_PacesRequests(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(jsonOKHandler(t, &calls))
	defer srv.Close()

	// 10 rps, burst 1 means the first call goes through immediately and the
	// next four are spaced ~100ms apart. 5 calls should take at least ~400ms.
	client, err := NewClient(ClientConfig{
		HTTPURL:     srv.URL,
		MaxRetries:  0,
		RateLimiter: rate.NewLimiter(rate.Limit(10), 1),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	start := time.Now()
	for i := range 5 {
		if _, err := client.GetCurrentBlockNumber(context.Background()); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)

	// 4 waits at 100ms each = 400ms floor. Allow generous upper bound to
	// avoid CI flake under load.
	if elapsed < 350*time.Millisecond {
		t.Errorf("expected limiter to pace 5 calls to >=350ms, got %v", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Errorf("limiter took unexpectedly long: %v", elapsed)
	}
	if got := calls.Load(); got != 5 {
		t.Errorf("expected 5 HTTP calls reached server, got %d", got)
	}
}

func TestRateLimiter_DisabledHasNoWait(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(jsonOKHandler(t, &calls))
	defer srv.Close()

	// nil RateLimiter is the valid "disabled" state.
	client, err := NewClient(ClientConfig{HTTPURL: srv.URL, MaxRetries: 0})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	start := time.Now()
	for i := range 10 {
		if _, err := client.GetCurrentBlockNumber(context.Background()); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)

	// 10 unmetered local-loopback calls should complete well under any
	// rate-limit floor we'd ever set in prod. 250ms is loose enough to be
	// stable on a busy CI runner.
	if elapsed > 250*time.Millisecond {
		t.Errorf("disabled limiter should not pace: 10 calls took %v", elapsed)
	}
	if got := calls.Load(); got != 10 {
		t.Errorf("expected 10 HTTP calls, got %d", got)
	}
}

// batchJSONHandler echoes a successful JSON-RPC response for every request in
// a batched body. It atomically counts HTTP requests so the test can assert
// the batch path consumes one token per HTTP call (not one per inner method).
func batchJSONHandler(t *testing.T, httpCalls *atomic.Int64) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		httpCalls.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		var reqs []jsonRPCRequest
		if err := json.Unmarshal(body, &reqs); err != nil {
			t.Errorf("unmarshal batch: %v", err)
			return
		}
		resps := make([]jsonRPCResponse, len(reqs))
		// Result payload is intentionally non-null so extractResult does not
		// short-circuit; the actual content does not matter for limiter pacing.
		for i, req := range reqs {
			resps[i] = jsonRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: json.RawMessage(`{}`)}
		}
		_ = json.NewEncoder(w).Encode(resps)
	}
}

func TestRateLimiter_BatchPathPacesByHTTPCall(t *testing.T) {
	var httpCalls atomic.Int64
	srv := httptest.NewServer(batchJSONHandler(t, &httpCalls))
	defer srv.Close()

	// 10 rps, burst 1: first batch goes through; each subsequent batch waits
	// ~100ms. Three batches => >=200ms floor. If the limiter spent one token
	// per inner method (4 per batch) instead of per HTTP call, the elapsed
	// time would balloon to ~1.1s.
	client, err := NewClient(ClientConfig{
		HTTPURL:      srv.URL,
		MaxRetries:   0,
		EnableTraces: true,
		EnableBlobs:  true,
		RateLimiter:  rate.NewLimiter(rate.Limit(10), 1),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	start := time.Now()
	for i := range 3 {
		if _, err := client.GetBlocksBatch(context.Background(), []int64{int64(i + 1)}, false); err != nil {
			t.Fatalf("batch %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)

	if got := httpCalls.Load(); got != 3 {
		t.Errorf("expected 3 HTTP batch calls, got %d", got)
	}
	// 2 waits at 100ms each = 200ms floor.
	if elapsed < 150*time.Millisecond {
		t.Errorf("expected limiter to pace 3 batches to >=150ms, got %v", elapsed)
	}
	// Per-inner-method pacing would yield ~1.1s; assert we are well below.
	if elapsed > 500*time.Millisecond {
		t.Errorf("batch path appears to spend one token per inner method (elapsed=%v); "+
			"limiter should debit once per HTTP request", elapsed)
	}
}

func TestRateLimiter_ContextCancelledDuringWait(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(jsonOKHandler(t, &calls))
	defer srv.Close()

	// 1 rps, burst 1 — first call consumes the only token; second call will
	// wait ~1s, which the ctx cancellation will interrupt.
	limiter := rate.NewLimiter(rate.Limit(1), 1)
	client, err := NewClient(ClientConfig{
		HTTPURL:     srv.URL,
		MaxRetries:  0,
		RateLimiter: limiter,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// Burn the initial burst token.
	if _, err := client.GetCurrentBlockNumber(context.Background()); err != nil {
		t.Fatalf("priming call: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 priming call, got %d", calls.Load())
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel before the limiter would admit; the second call must abort
	// without making an HTTP request.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	_, err = client.GetCurrentBlockNumber(ctx)
	if err == nil {
		t.Fatal("expected error on cancelled wait, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("expected request to NOT be sent on cancellation, server saw %d calls", got)
	}
}
