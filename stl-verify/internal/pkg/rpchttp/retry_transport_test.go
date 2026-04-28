package rpchttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newTestClient(base http.RoundTripper, maxRetries int) *http.Client {
	return &http.Client{
		Transport: &retryTransport{
			base:        base,
			maxRetries:  maxRetries,
			baseBackoff: time.Millisecond,
			maxBackoff:  5 * time.Millisecond,
			jitterFrac:  0,
		},
	}
}

func TestRetryTransport_RetriesOn429(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	client := newTestClient(http.DefaultTransport, 5)
	resp, err := client.Post(srv.URL, "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d; want 200", resp.StatusCode)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d; want 3 (2 retries + 1 success)", got)
	}
}

func TestRetryTransport_RetriesOn5xx(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 2 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := newTestClient(http.DefaultTransport, 5)
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d; want 200", resp.StatusCode)
	}
	if got := attempts.Load(); got != 2 {
		t.Errorf("attempts = %d; want 2", got)
	}
}

func TestRetryTransport_DoesNotRetry2xxOr4xx(t *testing.T) {
	cases := []struct {
		name string
		code int
		body string
	}{
		{
			name: "2xx with JSON-RPC revert in body",
			code: http.StatusOK,
			body: `{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}}`,
		},
		{
			name: "4xx (404) is non-retriable",
			code: http.StatusNotFound,
			body: ``,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				w.WriteHeader(tc.code)
				if tc.body != "" {
					_, _ = w.Write([]byte(tc.body))
				}
			}))
			defer srv.Close()

			client := newTestClient(http.DefaultTransport, 5)
			resp, err := client.Get(srv.URL)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer resp.Body.Close()
			if got := attempts.Load(); got != 1 {
				t.Errorf("status %d must not retry — attempts = %d, want 1", tc.code, got)
			}
		})
	}
}

func TestRetryTransport_MaxRetriesExhausted(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	client := newTestClient(http.DefaultTransport, 2)
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("expected response (not error) when server keeps 429-ing; got err=%v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("after exhausting retries, caller should see the last 429; got %d", resp.StatusCode)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d; want 3 (1 initial + 2 retries)", got)
	}
}

type errRoundTripper struct{ count atomic.Int32 }

func (e *errRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	e.count.Add(1)
	return nil, errors.New("dial tcp: connection refused")
}

func TestRetryTransport_RetriesOnNetworkError(t *testing.T) {
	rt := &errRoundTripper{}
	client := newTestClient(rt, 2)
	_, err := client.Get("http://127.0.0.1:1")
	if err == nil {
		t.Fatal("expected network error after exhausting retries")
	}
	if got := rt.count.Load(); got != 3 {
		t.Errorf("attempts = %d; want 3 (1 initial + 2 retries)", got)
	}
}

// fixedErrRoundTripper returns the same configured error for every call,
// counting attempts. Used to verify shouldRetry's classification of specific
// error values (context.Canceled, context.DeadlineExceeded).
type fixedErrRoundTripper struct {
	err   error
	count atomic.Int32
}

func (f *fixedErrRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	f.count.Add(1)
	return nil, f.err
}

// TestRetryTransport_DoesNotRetryContextCanceled verifies caller-driven
// cancellation is not treated as transient. Retrying would burn the retry
// budget and surface "exhausted retries" instead of the real cancel.
func TestRetryTransport_DoesNotRetryContextCanceled(t *testing.T) {
	rt := &fixedErrRoundTripper{err: context.Canceled}
	client := newTestClient(rt, 5)
	_, err := client.Get("http://127.0.0.1:1")
	if err == nil {
		t.Fatal("expected error")
	}
	if got := rt.count.Load(); got != 1 {
		t.Errorf("attempts = %d; want 1 (no retry on context.Canceled)", got)
	}
}

// TestRetryTransport_DoesNotRetryContextDeadlineExceeded — same as above
// for deadline expiry. A new attempt would just waste time the caller no
// longer has.
func TestRetryTransport_DoesNotRetryContextDeadlineExceeded(t *testing.T) {
	rt := &fixedErrRoundTripper{err: context.DeadlineExceeded}
	client := newTestClient(rt, 5)
	_, err := client.Get("http://127.0.0.1:1")
	if err == nil {
		t.Fatal("expected error")
	}
	if got := rt.count.Load(); got != 1 {
		t.Errorf("attempts = %d; want 1 (no retry on context.DeadlineExceeded)", got)
	}
}

// TestRetryTransport_DoesNotRetryWrappedContextError verifies the
// classification climbs through error wrapping (e.g. *url.Error wrapping a
// context error from the transport).
func TestRetryTransport_DoesNotRetryWrappedContextError(t *testing.T) {
	rt := &fixedErrRoundTripper{err: fmt.Errorf("dial tcp: %w", context.Canceled)}
	client := newTestClient(rt, 5)
	_, err := client.Get("http://127.0.0.1:1")
	if err == nil {
		t.Fatal("expected error")
	}
	if got := rt.count.Load(); got != 1 {
		t.Errorf("attempts = %d; want 1 (errors.Is should unwrap context.Canceled)", got)
	}
}

func TestRetryTransport_ReplaysBody(t *testing.T) {
	var attempts atomic.Int32
	var lastBody atomic.Pointer[[]byte]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		lastBody.Store(&body)
		n := attempts.Add(1)
		if n < 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := newTestClient(http.DefaultTransport, 3)
	payload := []byte(`{"method":"eth_call","params":[]}`)
	resp, err := client.Post(srv.URL, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d; want 200", resp.StatusCode)
	}
	if got := lastBody.Load(); got == nil || !bytes.Equal(*got, payload) {
		var bodyStr string
		if got != nil {
			bodyStr = string(*got)
		}
		t.Errorf("retry sent mismatched body; got %q, want %q", bodyStr, payload)
	}
}

func TestRetryTransport_RespectsContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	client := &http.Client{
		Transport: &retryTransport{
			base:        http.DefaultTransport,
			maxRetries:  100,
			baseBackoff: 50 * time.Millisecond,
			maxBackoff:  time.Second,
		},
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL, nil)

	start := time.Now()
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	_, err := client.Do(req)
	if err == nil {
		t.Fatal("expected error after cancellation")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("cancel didn't short-circuit retry loop; took %v", elapsed)
	}
}

// TestDialEthereum_RedactsAPIKeyInDialError verifies that a dial failure
// returns an error whose string does NOT contain the API key portion of the
// URL. Alchemy URLs embed `ALCHEMY_API_KEY` as the last path segment; a raw
// `%w` wrap of the URL would leak it into stderr / log aggregators.
//
// Uses an unsupported scheme to force `rpc.DialOptions` to fail
// synchronously (HTTP/HTTPS dials are lazy and don't fail at this level).
func TestDialEthereum_RedactsAPIKeyInDialError(t *testing.T) {
	const apiKey = "super-secret-alchemy-key-do-not-leak"
	url := "unsupported-scheme://eth-mainnet.example.com/v2/" + apiKey

	_, err := DialEthereum(context.Background(), url)
	if err == nil {
		t.Fatal("expected dial to fail with an unsupported scheme")
	}
	if strings.Contains(err.Error(), apiKey) {
		t.Errorf("error must redact the API key; got %q", err)
	}
}

// TestDialEthereum_ReturnsClientWithRetryBehavior verifies DialEthereum
// returns an ethclient that retries 429 — new workers can't accidentally
// skip retry protection just by calling DialEthereum instead of ethclient.Dial.
func TestDialEthereum_ReturnsClientWithRetryBehavior(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
	}))
	defer srv.Close()

	client, err := DialEthereum(context.Background(), srv.URL,
		WithMaxRetries(3),
		WithBaseBackoff(time.Millisecond),
		WithMaxBackoff(5*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("DialEthereum: %v", err)
	}
	defer client.Close()

	blockNum, err := client.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("BlockNumber: %v", err)
	}
	if blockNum != 1 {
		t.Errorf("blockNum = %d; want 1", blockNum)
	}
	if got := attempts.Load(); got != 2 {
		t.Errorf("attempts = %d; want 2 (1 retry + 1 success)", got)
	}
}
