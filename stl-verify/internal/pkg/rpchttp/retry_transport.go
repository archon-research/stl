// Package rpchttp provides HTTP transport wrappers for Ethereum RPC clients
// and a convenience dialer that all workers should use.
//
// retryTransport retries with capped exponential backoff + jitter when:
//   - the underlying RoundTrip returned a non-context error (network, DNS,
//     TLS handshake, connection reset, server-side timeout, etc.), OR
//   - the response status is HTTP 429 (Too Many Requests), OR
//   - the response status is any HTTP 5xx.
//
// It deliberately does NOT retry on:
//   - HTTP 2xx, even when the body carries a JSON-RPC error like an EVM
//     revert — that is the contract's definitive answer and must be
//     honored. (Reverts ride HTTP 200, so this falls out by construction.)
//   - HTTP 4xx other than 429 — these signal client-side problems that
//     won't be fixed by retrying the same request.
//   - context.Canceled / context.DeadlineExceeded — the caller is asking
//     us to stop; retrying would burn the budget and produce a misleading
//     "retries exhausted" error instead of surfacing the cancellation.
//
// DialEthereum is the canonical entry-point for dialing Ethereum RPC from
// this repo. New workers should use it instead of ethclient.Dial so retry
// protection is automatic rather than opt-in. See VEC-188.
package rpchttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// meterName scopes the rpchttp transport's metrics in the global meter provider.
const meterName = "github.com/archon-research/stl/stl-verify/internal/pkg/rpchttp"

// MaxRetriesUpperBound caps Config.MaxRetries to a sane value to prevent
// runaway retry loops if a caller passes an absurdly large number. With
// the default 250ms / 10s backoff, even 20 retries can already burn ~3
// minutes of wall time per request — anything higher than that is almost
// certainly a typo. Defense-in-depth, not a soft target.
const MaxRetriesUpperBound = 20

// Config configures a retry-enabled http.Client.
type Config struct {
	// MaxRetries caps retries. 0 disables retrying (single attempt).
	// Values above MaxRetriesUpperBound are clamped down.
	MaxRetries int

	// BaseBackoff is the initial delay before the first retry.
	BaseBackoff time.Duration

	// MaxBackoff caps the per-retry delay.
	MaxBackoff time.Duration

	// Timeout, if non-zero, is applied to the returned http.Client.Timeout.
	// This is the wall-clock budget for the entire request including all
	// retries. Leave 0 to inherit the http.Client default (no timeout).
	Timeout time.Duration

	// Transport is the underlying round-tripper. nil → http.DefaultTransport.
	Transport http.RoundTripper

	// Meter, if non-nil, enables per-attempt RPC telemetry: rpc_http_attempts_total
	// (every attempt, labeled by status code) and rpc_http_retries_total (each
	// retry, labeled by reason). This is the only place that sees HTTP status
	// codes and retry counts, so without it a slow logical RPC call (which times
	// the whole client.Do including silent backoff) cannot be attributed to
	// throttling (429) vs server errors (5xx) vs a genuinely slow single
	// response. nil disables it with zero overhead.
	Meter metric.Meter
}

// NewBackfillerClient returns an *http.Client tuned for high-throughput
// backfiller workloads — pooled HTTP/1.1 connections sized to concurrency,
// 120s wall-clock timeout per request, and the standard rpchttp retry
// transport (5 retries, 250ms→10s backoff with jitter).
//
// concurrency is the worker's parallel-request budget; the underlying
// http.Transport's per-host connection pool is sized to match it (with
// 2× idle headroom). Pass at least 1.
//
// All four backfillers in cmd/backfillers/ use this — keep new backfillers
// on the same path so transport tuning lives in one place.
func NewBackfillerClient(concurrency int) *http.Client {
	if concurrency < 1 {
		concurrency = 1
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          concurrency * 2,
		MaxIdleConnsPerHost:   concurrency,
		MaxConnsPerHost:       concurrency,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return NewClient(Config{
		MaxRetries:  5,
		BaseBackoff: 250 * time.Millisecond,
		MaxBackoff:  10 * time.Second,
		Timeout:     120 * time.Second,
		Transport:   transport,
		// Match DialEthereum: emit retry telemetry on the high-throughput
		// backfill path too, so no RPC caller is a blind spot.
		Meter: otel.GetMeterProvider().Meter(meterName),
	})
}

// NewClient returns an *http.Client that retries 429/5xx/network errors.
func NewClient(cfg Config) *http.Client {
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.MaxRetries > MaxRetriesUpperBound {
		cfg.MaxRetries = MaxRetriesUpperBound
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 250 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 10 * time.Second
	}
	base := cfg.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	rt := &retryTransport{
		base:        base,
		maxRetries:  cfg.MaxRetries,
		baseBackoff: cfg.BaseBackoff,
		maxBackoff:  cfg.MaxBackoff,
		jitterFrac:  0.25,
	}
	if cfg.Meter != nil {
		// Dotted instrument names per repo convention (e.g. alchemy.client.retries.total);
		// the OTLP→Prometheus pipeline flattens to rpc_http_attempts_total /
		// rpc_http_retries_total and appends _total for monotonic counters.
		attempts, attemptsErr := cfg.Meter.Int64Counter(
			"rpc.http.attempts",
			metric.WithDescription("HTTP attempts made by the RPC retry transport, labeled by server.address and rpc.http.status_code"),
		)
		retries, retriesErr := cfg.Meter.Int64Counter(
			"rpc.http.retries",
			metric.WithDescription("HTTP retries triggered by the RPC retry transport, labeled by server.address and reason (429/5xx/network)"),
		)
		// Instrument creation only fails on a malformed instrument name — a
		// programming error our constant names cannot hit (and tests cover the
		// happy path). On the off chance it ever does, leave telemetry disabled
		// rather than break the RPC data path; emitting nothing is the safe
		// degradation for best-effort observability.
		if err := errors.Join(attemptsErr, retriesErr); err == nil {
			rt.attempts = attempts
			rt.retries = retries
		}
	}
	return &http.Client{
		Timeout:   cfg.Timeout,
		Transport: rt,
	}
}

type retryTransport struct {
	base        http.RoundTripper
	maxRetries  int
	baseBackoff time.Duration
	maxBackoff  time.Duration
	// jitterFrac in [0, 1) — fraction of backoff used as ± jitter range.
	// 0 disables jitter (tests).
	jitterFrac float64

	// attempts and retries are nil unless a Meter was configured (see Config.Meter).
	attempts metric.Int64Counter
	retries  metric.Int64Counter
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var bodyBytes []byte
	if req.Body != nil {
		// Read into memory so we can replay on retry, then close
		// unconditionally so the body isn't leaked even on read error. Surface
		// either error (or both, joined) — the project's standard prohibits
		// silently dropping a Close() return.
		b, readErr := io.ReadAll(req.Body)
		closeErr := req.Body.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return nil, fmt.Errorf("retryTransport: handle request body: %w", err)
		}
		bodyBytes = b
	}

	var lastResp *http.Response
	var lastErr error
	for attempt := 0; attempt <= t.maxRetries; attempt++ {
		if attempt > 0 {
			if lastResp != nil {
				_ = lastResp.Body.Close()
				lastResp = nil
			}
			if err := t.sleep(req, attempt); err != nil {
				return nil, err
			}
		}

		if bodyBytes != nil {
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			req.GetBody = func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(bodyBytes)), nil
			}
			req.ContentLength = int64(len(bodyBytes))
		}

		resp, err := t.base.RoundTrip(req)
		lastResp, lastErr = resp, err

		if t.attempts != nil {
			t.attempts.Add(req.Context(), 1, metric.WithAttributes(
				attribute.String("server.address", req.URL.Host),
				attribute.String("rpc.http.status_code", attemptCode(resp, err)),
			))
		}

		if !shouldRetry(resp, err) {
			return resp, err
		}

		// shouldRetry is true, but the final allowed attempt won't loop again —
		// only record a retry when another attempt actually follows.
		if attempt < t.maxRetries {
			reason := retryReason(resp, err)
			if t.retries != nil {
				t.retries.Add(req.Context(), 1, metric.WithAttributes(
					attribute.String("server.address", req.URL.Host),
					attribute.String("reason", reason),
				))
			}
			// Inline the retry on the caller's active span (e.g. oracle.fetchPrices)
			// so a slow span self-explains without timestamp-correlating metrics.
			// No-op when tracing is off or unsampled (span not recording).
			if span := trace.SpanFromContext(req.Context()); span.IsRecording() {
				span.AddEvent("rpc.retry", trace.WithAttributes(
					attribute.Int("retry.attempt", attempt+1), // 1-based: this is the Nth retry
					attribute.String("retry.reason", reason),
					attribute.String("rpc.http.status_code", attemptCode(resp, err)),
				))
			}
		}
	}
	return lastResp, lastErr
}

// attemptCode labels an attempt for rpc_http_attempts_total. A response's
// status code wins; a transport error is bucketed by kind so the counter never
// loses an attempt to a missing label.
func attemptCode(resp *http.Response, err error) string {
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			return "canceled"
		case errors.Is(err, context.DeadlineExceeded):
			return "deadline"
		default:
			return "network_error"
		}
	}
	return strconv.Itoa(resp.StatusCode)
}

// retryReason labels a retry for rpc_http_retries_total. Only called when
// shouldRetry is true, so the input is always a 429, a 5xx, or a non-context
// transport error.
func retryReason(resp *http.Response, err error) string {
	if err != nil {
		return "network"
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return "429"
	}
	return "5xx"
}

func shouldRetry(resp *http.Response, err error) bool {
	if err != nil {
		// Caller-driven cancellation / deadline expiry is not transient —
		// the caller is asking us to stop. Retrying would burn the budget
		// and produce a misleading "exhausted retries" error instead of
		// surfacing the cancellation cleanly.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return false
		}
		return true
	}
	return resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
}

func (t *retryTransport) sleep(req *http.Request, attempt int) error {
	d := t.baseBackoff << (attempt - 1)
	if d <= 0 || d > t.maxBackoff {
		d = t.maxBackoff
	}
	if t.jitterFrac > 0 {
		delta := time.Duration(float64(d) * t.jitterFrac)
		d = d - delta + time.Duration(rand.Int64N(int64(2*delta+1)))
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-req.Context().Done():
		return req.Context().Err()
	case <-timer.C:
		return nil
	}
}

// An Option configures DialEthereum.
type Option func(*Config)

// WithMaxRetries overrides the default (5).
func WithMaxRetries(n int) Option {
	return func(c *Config) { c.MaxRetries = n }
}

// WithBaseBackoff overrides the default (250ms).
func WithBaseBackoff(d time.Duration) Option {
	return func(c *Config) { c.BaseBackoff = d }
}

// WithMaxBackoff overrides the default (10s).
func WithMaxBackoff(d time.Duration) Option {
	return func(c *Config) { c.MaxBackoff = d }
}

// WithTransport overrides the default base RoundTripper.
func WithTransport(rt http.RoundTripper) Option {
	return func(c *Config) { c.Transport = rt }
}

// WithMeter overrides the meter used for retry telemetry. DialEthereum
// defaults to the global meter provider, so this is only needed to pin a
// specific meter (e.g. in tests with a manual reader).
func WithMeter(m metric.Meter) Option {
	return func(c *Config) { c.Meter = m }
}

// WithClientTimeout sets the wall-clock budget for the entire request,
// including all retries. Equivalent to setting http.Client.Timeout. Use
// this instead of mutating the returned client's Timeout field directly,
// so callers don't depend on the concrete http.Client return type.
func WithClientTimeout(d time.Duration) Option {
	return func(c *Config) { c.Timeout = d }
}

// DialEthereum dials the given Ethereum JSON-RPC URL with retry protection
// baked in. This is the canonical entry-point for ALL worker / backfiller
// mains in this repo — new code should use it instead of ethclient.Dial so
// HTTP 429 / 5xx / network retries apply automatically.
//
// Defaults: MaxRetries=5, BaseBackoff=250ms, MaxBackoff=10s, ±25% jitter.
// Override via With* options.
// defaultDialTimeout bounds the whole request including all retries. The prior
// default of 0 (no client timeout) let a single hung attempt — or a 429/5xx
// retry storm — block a per-block worker unbounded. 60s is a safety ceiling,
// generous enough to let the full retry budget complete on a transient
// throttle; lower it per service (WithClientTimeout) to trade completeness for
// freshness.
const defaultDialTimeout = 60 * time.Second

// dialConfig builds the Config DialEthereum dials with: retry defaults, a
// bounded client timeout, and the global meter for retry telemetry — each
// overridable via opts.
func dialConfig(opts ...Option) Config {
	cfg := Config{
		MaxRetries:  5,
		BaseBackoff: 250 * time.Millisecond,
		MaxBackoff:  10 * time.Second,
		Timeout:     defaultDialTimeout,
	}
	for _, opt := range opts {
		if opt == nil {
			// Defense against callers who construct opts dynamically and end
			// up with a nil entry — invoking it would panic with a less
			// informative stack than a typed-nil deref.
			continue
		}
		opt(&cfg)
	}
	// Default to the global meter provider so every worker dialing through
	// DialEthereum emits retry telemetry without opting in. It is a no-op meter
	// until the process configures OTel, which all our workers do at startup.
	if cfg.Meter == nil {
		cfg.Meter = otel.GetMeterProvider().Meter(meterName)
	}
	return cfg
}

func DialEthereum(ctx context.Context, rawURL string, opts ...Option) (*ethclient.Client, error) {
	rpcClient, err := rpc.DialOptions(ctx, rawURL, rpc.WithHTTPClient(NewClient(dialConfig(opts...))))
	if err != nil {
		return nil, fmt.Errorf("rpchttp: dial %s: %w", redactURL(rawURL), err)
	}
	return ethclient.NewClient(rpcClient), nil
}

// redactURL returns a logging-safe form of rawURL — scheme + host only,
// stripping the path, query string, and userinfo. Used in dial-error
// messages to avoid leaking the API key embedded in RPC provider URLs.
func redactURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		// Parse failed or URL has no host — return the scheme alone if we
		// have one, otherwise a generic placeholder. Don't risk echoing
		// the raw input.
		if u != nil && u.Scheme != "" {
			return u.Scheme + "://<redacted>"
		}
		return "<redacted>"
	}
	return u.Scheme + "://" + u.Host
}
