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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// collectCounter sums the int64 data points of counter `name` whose attribute
// `attrKey` equals `attrVal`. Fails the test if the metric is absent or not an
// int64 sum.
func collectCounter(t *testing.T, reader sdkmetric.Reader, name, attrKey, attrVal string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %s is %T, want Sum[int64]", name, m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				if v, ok := dp.Attributes.Value(attribute.Key(attrKey)); ok && v.AsString() == attrVal {
					total += dp.Value
				}
			}
			return total
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

// newMeteredClient builds a client whose retry transport reports to a manual
// reader, with the given retry budget and base round-tripper. Only the
// meter/reader wiring is hoisted (not a fixed response sequence), so each test
// supplies its own base behavior — a real server, a stub status code, or a
// network error.
func newMeteredClient(t *testing.T, maxRetries int, base http.RoundTripper) (*http.Client, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	client := NewClient(Config{
		MaxRetries:  maxRetries,
		BaseBackoff: time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
		Transport:   base,
		Meter:       mp.Meter("rpchttp_test"),
	})
	return client, reader
}

// codeRoundTripper returns a response with a fixed status code and empty body
// without any network, for deterministically exercising status-code paths.
type codeRoundTripper struct{ code int }

func (c codeRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: c.code,
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Header:     make(http.Header),
	}, nil
}

// TestRetryTransport_RecordsAttemptCountByStatusCode verifies every HTTP
// attempt is counted under its status code, so we can distinguish "one slow
// 200" from "a storm of 429s" — invisible before this instrumentation.
func TestRetryTransport_RecordsAttemptCountByStatusCode(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) < 3 { // 429, 429, then 200
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	client, reader := newMeteredClient(t, 5, http.DefaultTransport)
	resp, err := client.Post(srv.URL, "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if got := collectCounter(t, reader, "rpc.http.attempts", "rpc.http.status_code", "429"); got != 2 {
		t.Errorf("attempts status=429 = %d; want 2", got)
	}
	if got := collectCounter(t, reader, "rpc.http.attempts", "rpc.http.status_code", "200"); got != 1 {
		t.Errorf("attempts status=200 = %d; want 1", got)
	}
}

// TestRetryTransport_CountsRetriesByReasonAndStopsAtBudget drives the transport
// against a base that never recovers, exhausting the budget. With MaxRetries=2
// that is exactly 3 attempts and 2 retries — pinning both the reason label
// (429/5xx/network) and the boundary invariant that the final, exhausted
// attempt is counted as an attempt but NOT as a retry.
func TestRetryTransport_CountsRetriesByReasonAndStopsAtBudget(t *testing.T) {
	cases := []struct {
		name    string
		base    http.RoundTripper
		reason  string
		code    string
		wantErr bool
	}{
		{"429 throttle", codeRoundTripper{http.StatusTooManyRequests}, "429", "429", false},
		{"5xx server error", codeRoundTripper{http.StatusBadGateway}, "5xx", "502", false},
		{"network error", &errRoundTripper{}, "network", "network_error", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, reader := newMeteredClient(t, 2, tc.base)
			resp, err := client.Get("http://oracle.test/")
			if resp != nil {
				_ = resp.Body.Close()
			}
			if tc.wantErr && err == nil {
				t.Fatalf("expected error after exhausting retries")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got := collectCounter(t, reader, "rpc.http.retries", "reason", tc.reason); got != 2 {
				t.Errorf("retries reason=%s = %d; want 2 (MaxRetries=2)", tc.reason, got)
			}
			if got := collectCounter(t, reader, "rpc.http.attempts", "rpc.http.status_code", tc.code); got != 3 {
				t.Errorf("attempts status=%s = %d; want 3 (final attempt counted, not retried)", tc.code, got)
			}
		})
	}
}

// TestAttemptCode covers every bucket of the attempts-counter label, including
// the synthetic error buckets that no metered HTTP test reaches.
func TestAttemptCode(t *testing.T) {
	cases := []struct {
		name string
		resp *http.Response
		err  error
		want string
	}{
		{"http 200", &http.Response{StatusCode: 200}, nil, "200"},
		{"http 429", &http.Response{StatusCode: 429}, nil, "429"},
		{"context canceled", nil, context.Canceled, "canceled"},
		{"context deadline", nil, context.DeadlineExceeded, "deadline"},
		{"wrapped canceled", nil, fmt.Errorf("dial: %w", context.Canceled), "canceled"},
		{"generic network", nil, errors.New("connection refused"), "network_error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := attemptCode(tc.resp, tc.err); got != tc.want {
				t.Errorf("attemptCode = %q; want %q", got, tc.want)
			}
		})
	}
}

// TestRetryReason covers the retry-counter label classification across the
// three triggers shouldRetry can produce.
func TestRetryReason(t *testing.T) {
	cases := []struct {
		name string
		resp *http.Response
		err  error
		want string
	}{
		{"429", &http.Response{StatusCode: 429}, nil, "429"},
		{"503", &http.Response{StatusCode: 503}, nil, "5xx"},
		{"network", nil, errors.New("connection reset"), "network"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryReason(tc.resp, tc.err); got != tc.want {
				t.Errorf("retryReason = %q; want %q", got, tc.want)
			}
		})
	}
}

// TestRetryTransport_NilMeterEmitsNothing pins the documented zero-overhead
// default: with no meter the retry path still runs (and retries) without
// panicking on the nil instruments.
func TestRetryTransport_NilMeterEmitsNothing(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) < 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(Config{MaxRetries: 3, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond})
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d; want 200", resp.StatusCode)
	}
}

// TestRetryTransport_AddsRetryEventsToActiveSpan verifies each retry adds an
// event to the caller's active span, so a slow logical RPC span (e.g.
// oracle.fetchPrices) carries inline evidence of why it was slow — without
// needing to correlate separate metrics by timestamp.
func TestRetryTransport_AddsRetryEventsToActiveSpan(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	tracer := tp.Tracer("rpchttp_test")

	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(Config{MaxRetries: 5, BaseBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond})
	ctx, span := tracer.Start(context.Background(), "rpc.call")
	req, err := http.NewRequestWithContext(ctx, "POST", srv.URL, strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()
	span.End()

	var retryEvents int
	for _, s := range sr.Ended() {
		if s.Name() != "rpc.call" {
			continue
		}
		for _, ev := range s.Events() {
			if ev.Name == "rpc.retry" {
				retryEvents++
			}
		}
	}
	if retryEvents != 2 {
		t.Errorf("rpc.retry span events = %d; want 2", retryEvents)
	}
}

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

// TestNewBackfillerClient_HasPooledTransportAndTimeoutAndRetries verifies
// the helper bundles all three ingredients backfillers depend on:
// (1) a connection-pooled http.Transport, (2) a 120s client timeout, and
// (3) the standard retry policy. Keeps the four backfillers from drifting
// apart.
func TestNewBackfillerClient_HasPooledTransportAndTimeoutAndRetries(t *testing.T) {
	client := NewBackfillerClient(8)

	if client.Timeout != 120*time.Second {
		t.Errorf("Timeout = %v; want 120s", client.Timeout)
	}

	rt, ok := client.Transport.(*retryTransport)
	if !ok {
		t.Fatalf("expected *retryTransport, got %T", client.Transport)
	}
	if rt.maxRetries != 5 {
		t.Errorf("maxRetries = %d; want 5", rt.maxRetries)
	}

	pool, ok := rt.base.(*http.Transport)
	if !ok {
		t.Fatalf("expected *http.Transport as base, got %T", rt.base)
	}
	if pool.MaxConnsPerHost != 8 {
		t.Errorf("MaxConnsPerHost = %d; want 8 (matches concurrency)", pool.MaxConnsPerHost)
	}
	if pool.MaxIdleConnsPerHost != 8 {
		t.Errorf("MaxIdleConnsPerHost = %d; want 8", pool.MaxIdleConnsPerHost)
	}
	if pool.MaxIdleConns != 16 {
		t.Errorf("MaxIdleConns = %d; want 16 (2× concurrency)", pool.MaxIdleConns)
	}
}

// TestNewBackfillerClient_ClampsConcurrencyBelowOne ensures a zero or
// negative concurrency doesn't produce a useless connection pool.
func TestNewBackfillerClient_ClampsConcurrencyBelowOne(t *testing.T) {
	client := NewBackfillerClient(0)
	rt := client.Transport.(*retryTransport)
	pool := rt.base.(*http.Transport)
	if pool.MaxConnsPerHost != 1 {
		t.Errorf("zero concurrency must clamp to 1; got MaxConnsPerHost = %d", pool.MaxConnsPerHost)
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

// TestDialEthereum_DefaultsClientTimeout verifies DialEthereum bounds the whole
// request (all retries) by default, so a hung connection or retry storm can't
// block a worker indefinitely, and that WithClientTimeout still overrides it.
func TestDialEthereum_DefaultsClientTimeout(t *testing.T) {
	if got := dialConfig().Timeout; got != defaultDialTimeout {
		t.Errorf("default Timeout = %v; want %v", got, defaultDialTimeout)
	}
	if got := dialConfig(WithClientTimeout(7 * time.Second)).Timeout; got != 7*time.Second {
		t.Errorf("WithClientTimeout override = %v; want 7s", got)
	}
}

// TestDialEthereum_EmitsRetryTelemetryByDefault verifies that DialEthereum
// wires the global meter provider in by default, so every worker that dials
// through it emits rpc_http_attempts_total without per-call opt-in — that is
// what makes the throttle-vs-slow signal fleet-wide.
func TestDialEthereum_EmitsRetryTelemetryByDefault(t *testing.T) {
	// Must not t.Parallel(): this mutates the global OTel meter provider, which
	// would race any sibling test dialing through DialEthereum.
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	defer otel.SetMeterProvider(prev)

	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) < 2 {
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
	if _, err := client.BlockNumber(context.Background()); err != nil {
		t.Fatalf("BlockNumber: %v", err)
	}

	if got := collectCounter(t, reader, "rpc.http.attempts", "rpc.http.status_code", "429"); got < 1 {
		t.Errorf("expected >=1 attempt with status=429 via default global meter; got %d", got)
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
