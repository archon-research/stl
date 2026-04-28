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
	"net/http"
	"net/url"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// Config configures a retry-enabled http.Client.
type Config struct {
	// MaxRetries caps retries. 0 disables retrying (single attempt).
	MaxRetries int

	// BaseBackoff is the initial delay before the first retry.
	BaseBackoff time.Duration

	// MaxBackoff caps the per-retry delay.
	MaxBackoff time.Duration

	// Transport is the underlying round-tripper. nil → http.DefaultTransport.
	Transport http.RoundTripper
}

// NewClient returns an *http.Client that retries 429/5xx/network errors.
func NewClient(cfg Config) *http.Client {
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
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
		b, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, fmt.Errorf("retryTransport: read request body: %w", err)
		}
		_ = req.Body.Close()
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
// messages to avoid leaking the API key embedded in Alchemy / Infura URLs.
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
