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
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

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
	return &http.Client{
		Timeout: cfg.Timeout,
		Transport: &retryTransport{
			base:        base,
			maxRetries:  cfg.MaxRetries,
			baseBackoff: cfg.BaseBackoff,
			maxBackoff:  cfg.MaxBackoff,
			jitterFrac:  0.25,
		},
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

		if !shouldRetry(resp, err) {
			return resp, err
		}
	}
	return lastResp, lastErr
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
func DialEthereum(ctx context.Context, rawURL string, opts ...Option) (*ethclient.Client, error) {
	cfg := Config{
		MaxRetries:  5,
		BaseBackoff: 250 * time.Millisecond,
		MaxBackoff:  10 * time.Second,
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
	rpcClient, err := rpc.DialOptions(ctx, rawURL, rpc.WithHTTPClient(NewClient(cfg)))
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
