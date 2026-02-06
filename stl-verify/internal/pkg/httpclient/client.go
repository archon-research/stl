// Package httpclient provides a shared HTTP client with retry logic for external API calls.
package httpclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/retry"
	"golang.org/x/time/rate"
)

// Config holds the configuration for the HTTP client.
type Config struct {
	Timeout        time.Duration
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
	RateLimit      rate.Limit
	RateBurst      int
}

// DefaultConfig returns sensible defaults for the HTTP client.
func DefaultConfig() Config {
	return Config{
		Timeout:        30 * time.Second,
		MaxRetries:     3,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
		BackoffFactor:  2.0,
		RateLimit:      rate.Limit(5),
		RateBurst:      1,
	}
}

// RequestConfig holds per-request configuration.
type RequestConfig struct {
	URL     string
	Headers map[string]string
}

// ErrorParser parses API-specific error responses.
// It returns an error if the response body contains an API error, or nil if no error.
type ErrorParser func(statusCode int, body []byte) error

// Client wraps an HTTP client with retry logic and rate limiting.
type Client struct {
	httpClient  *http.Client
	limiter     *rate.Limiter
	retryConfig retry.Config
	logger      *slog.Logger
	errorParser ErrorParser
}

// NewClient creates a new HTTP client with the given configuration.
func NewClient(cfg Config, logger *slog.Logger, errorParser ErrorParser) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	if errorParser == nil {
		errorParser = func(_ int, _ []byte) error { return nil }
	}

	return &Client{
		httpClient: &http.Client{Timeout: cfg.Timeout},
		limiter:    rate.NewLimiter(cfg.RateLimit, cfg.RateBurst),
		retryConfig: retry.Config{
			MaxRetries:     cfg.MaxRetries,
			InitialBackoff: cfg.InitialBackoff,
			MaxBackoff:     cfg.MaxBackoff,
			BackoffFactor:  cfg.BackoffFactor,
		},
		logger:      logger,
		errorParser: errorParser,
	}
}

// DoRequest performs an HTTP GET request with retry logic and rate limiting.
func (c *Client) DoRequest(ctx context.Context, reqCfg RequestConfig, result any) error {
	isRetryable := func(err error) bool {
		var nonRetryable *NonRetryableError
		return !isNonRetryable(err, &nonRetryable)
	}

	onRetry := func(attempt int, err error, backoff time.Duration) {
		c.logger.Warn("request failed, retrying",
			"attempt", attempt,
			"maxRetries", c.retryConfig.MaxRetries,
			"backoff", backoff,
			"error", err,
		)
	}

	return retry.DoVoid(ctx, c.retryConfig, isRetryable, onRetry, func() error {
		if err := c.limiter.Wait(ctx); err != nil {
			return WrapNonRetryable(fmt.Errorf("rate limiter: %w", err))
		}
		return c.doSingleRequest(ctx, reqCfg, result)
	})
}

func (c *Client) doSingleRequest(ctx context.Context, reqCfg RequestConfig, result any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqCfg.URL, nil)
	if err != nil {
		return WrapNonRetryable(fmt.Errorf("creating request: %w", err))
	}

	req.Header.Set("Accept", "application/json")
	for key, value := range reqCfg.Headers {
		req.Header.Set(key, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Warn("failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("rate limited (HTTP 429)")
	}

	if resp.StatusCode >= 500 {
		return fmt.Errorf("server error (HTTP %d)", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		if apiErr := c.errorParser(resp.StatusCode, body); apiErr != nil {
			return WrapNonRetryable(apiErr)
		}
		return WrapNonRetryable(fmt.Errorf("client error (HTTP %d): %s", resp.StatusCode, string(body)))
	}

	// Check for API-specific errors in successful responses
	if apiErr := c.errorParser(resp.StatusCode, body); apiErr != nil {
		// Let the error parser decide if it's retryable or not
		return apiErr
	}

	if err := json.Unmarshal(body, result); err != nil {
		return WrapNonRetryable(fmt.Errorf("parsing response: %w", err))
	}

	return nil
}

// NonRetryableError wraps errors that should not be retried.
type NonRetryableError struct {
	err error
}

func (e *NonRetryableError) Error() string {
	return e.err.Error()
}

func (e *NonRetryableError) Unwrap() error {
	return e.err
}

// WrapNonRetryable wraps an error to indicate it should not be retried.
func WrapNonRetryable(err error) error {
	return &NonRetryableError{err: err}
}

// isNonRetryable checks if an error is non-retryable using errors.As pattern.
func isNonRetryable(err error, target **NonRetryableError) bool {
	for err != nil {
		if e, ok := err.(*NonRetryableError); ok {
			*target = e
			return true
		}
		if u, ok := err.(interface{ Unwrap() error }); ok {
			err = u.Unwrap()
		} else {
			break
		}
	}
	return false
}
