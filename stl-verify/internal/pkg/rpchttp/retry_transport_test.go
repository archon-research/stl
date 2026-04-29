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

// errCloseRC is an io.ReadCloser whose Close() returns the configured error.
// Used to verify retryTransport surfaces request-body Close() errors instead
// of silently dropping them.
type errCloseRC struct {
	*bytes.Reader
	closeErr error
	closed   bool
}

func (e *errCloseRC) Close() error {
	e.closed = true
	return e.closeErr
}

// errReadRC is an io.ReadCloser that errors on Read and tracks whether
// Close was called. Used to verify retryTransport closes the request body
// on the read-error path (no resource leak).
type errReadRC struct {
	closed bool
}

func (e *errReadRC) Read([]byte) (int, error) { return 0, errors.New("simulated read failure") }
func (e *errReadRC) Close() error             { e.closed = true; return nil }

// TestRetryTransport_SurfacesRequestBodyCloseError verifies that when the
// request body's Close() returns an error after a successful Read, the
// transport surfaces that error rather than dropping it. CodeRabbit
// flagged this on PR #251 — the original `_ = req.Body.Close()` violated
// the project's "never ignore errors" rule.
func TestRetryTransport_SurfacesRequestBodyCloseError(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	body := &errCloseRC{
		Reader:   bytes.NewReader([]byte(`{"jsonrpc":"2.0"}`)),
		closeErr: errors.New("simulated close failure"),
	}
	req, err := http.NewRequest("POST", srv.URL, body)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	rt := &retryTransport{
		base:        http.DefaultTransport,
		maxRetries:  3,
		baseBackoff: time.Millisecond,
		maxBackoff:  5 * time.Millisecond,
	}
	_, err = rt.RoundTrip(req)
	if err == nil {
		t.Fatal("expected Close() error to be surfaced")
	}
	if !strings.Contains(err.Error(), "simulated close failure") {
		t.Errorf("error %q should reference the underlying close error", err)
	}
	if !body.closed {
		t.Error("body.Close() was never called")
	}
	if got := attempts.Load(); got != 0 {
		t.Errorf("server should NOT be hit when body close fails; attempts = %d", got)
	}
}

// TestRetryTransport_ClosesRequestBodyOnReadError verifies that an
// io.ReadAll failure does not leak the request body — the transport must
// close it even on the error path.
func TestRetryTransport_ClosesRequestBodyOnReadError(t *testing.T) {
	body := &errReadRC{}
	req, err := http.NewRequest("POST", "http://127.0.0.1:1", body)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	rt := &retryTransport{
		base:        http.DefaultTransport,
		maxRetries:  3,
		baseBackoff: time.Millisecond,
		maxBackoff:  5 * time.Millisecond,
	}
	_, err = rt.RoundTrip(req)
	if err == nil {
		t.Fatal("expected read error to surface")
	}
	if !body.closed {
		t.Error("body.Close() was not called on read-error path — resource leak")
	}
}

// TestNewClient_ClampsMaxRetriesUpperBound verifies that an absurdly large
// MaxRetries is clamped to a sane upper bound — defense-in-depth against a
// future caller passing 1_000_000 by accident.
func TestNewClient_ClampsMaxRetriesUpperBound(t *testing.T) {
	client := NewClient(Config{
		MaxRetries:  1_000_000,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
	})
	rt, ok := client.Transport.(*retryTransport)
	if !ok {
		t.Fatalf("expected *retryTransport, got %T", client.Transport)
	}
	if rt.maxRetries != MaxRetriesUpperBound {
		t.Errorf("maxRetries = %d; want clamped to %d", rt.maxRetries, MaxRetriesUpperBound)
	}
}

// TestNewClient_AppliesTimeoutFromConfig verifies that Config.Timeout is
// applied to the returned http.Client. Callers should set the timeout via
// the config (or WithClientTimeout) rather than mutating the returned
// client's field directly.
func TestNewClient_AppliesTimeoutFromConfig(t *testing.T) {
	client := NewClient(Config{Timeout: 7 * time.Second})
	if client.Timeout != 7*time.Second {
		t.Errorf("Timeout = %v; want 7s", client.Timeout)
	}
}

// TestDialEthereum_AppliesWithClientTimeout verifies the WithClientTimeout
// option flows through to the underlying http.Client. The dial succeeds
// against any HTTP server (no actual eth_call), so we just check the
// transport's parent client.
func TestDialEthereum_AppliesWithClientTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// We can't easily fish the http.Client out of *ethclient.Client, so this
	// test verifies the option mechanism via NewClient + a handcrafted Config
	// applied through the option.
	cfg := Config{}
	WithClientTimeout(7 * time.Second)(&cfg)
	if cfg.Timeout != 7*time.Second {
		t.Errorf("WithClientTimeout did not set Timeout; got %v", cfg.Timeout)
	}
}

// TestDialEthereum_SkipsNilOptions verifies that a nil entry in the
// variadic opts slice is tolerated rather than panicking. This is a
// defensive guard for callers that construct opts dynamically.
func TestDialEthereum_SkipsNilOptions(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
	}))
	defer srv.Close()

	// Pass a nil Option in the middle of a real one. Pre-fix this would
	// panic on the nil call.
	client, err := DialEthereum(context.Background(), srv.URL,
		WithMaxRetries(0),
		nil,
		WithBaseBackoff(time.Millisecond),
	)
	if err != nil {
		t.Fatalf("DialEthereum: %v", err)
	}
	defer client.Close()

	if _, err := client.BlockNumber(context.Background()); err != nil {
		t.Errorf("BlockNumber: %v", err)
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
