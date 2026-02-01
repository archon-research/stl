// Package retry provides a reusable retry mechanism with exponential backoff.
//
// This package offers a generic retry function that can be used across different
// adapters and services to handle transient failures consistently.
package retry

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// Config holds configuration for retry behavior.
type Config struct {
	// MaxRetries is the maximum number of retry attempts (0 means no retries, just the initial attempt).
	MaxRetries int

	// InitialBackoff is the initial backoff duration before the first retry.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum backoff duration (caps exponential growth).
	MaxBackoff time.Duration

	// BackoffFactor is the multiplier applied to backoff after each retry (default: 2.0).
	BackoffFactor float64

	// Jitter adds randomness to backoff to prevent thundering herd.
	// When true, actual backoff is: backoff + rand(0, backoff)
	Jitter bool
}

// DefaultConfig returns a sensible default configuration.
func DefaultConfig() Config {
	return Config{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
		Jitter:         true,
	}
}

// IsRetryableFunc determines if an error should trigger a retry.
type IsRetryableFunc func(error) bool

// OnRetryFunc is called before each retry attempt (optional, for logging/metrics).
// attempt is 1-indexed (first retry is attempt 1).
type OnRetryFunc func(attempt int, err error, backoff time.Duration)

// Do executes the given function with retry logic.
// It returns the result of the function or the last error if all retries are exhausted.
//
// The function is called at least once. If it returns an error and isRetryable returns true,
// it will be retried up to cfg.MaxRetries additional times.
//
// Example:
//
//	result, err := retry.Do(ctx, retry.DefaultConfig(), isTransientError, nil, func() (int, error) {
//	    return someOperation()
//	})
func Do[T any](
	ctx context.Context,
	cfg Config,
	isRetryable IsRetryableFunc,
	onRetry OnRetryFunc,
	fn func() (T, error),
) (T, error) {
	var zero T
	var lastErr error

	// Apply defaults
	if cfg.BackoffFactor <= 0 {
		cfg.BackoffFactor = 2.0
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 10 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 100 * time.Millisecond
	}

	backoff := cfg.InitialBackoff

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		// Wait before retry (not on first attempt)
		if attempt > 0 {
			actualBackoff := backoff
			if cfg.Jitter {
				// Add jitter: backoff + rand(0, backoff)
				jitter := time.Duration(rand.Int63n(int64(backoff)))
				actualBackoff = backoff + jitter
			}

			if onRetry != nil {
				onRetry(attempt, lastErr, actualBackoff)
			}

			select {
			case <-ctx.Done():
				return zero, fmt.Errorf("context cancelled while retrying: %w", ctx.Err())
			case <-time.After(actualBackoff):
				// Continue with retry
			}

			// Calculate next backoff (exponential)
			backoff = time.Duration(float64(backoff) * cfg.BackoffFactor)
			if backoff > cfg.MaxBackoff {
				backoff = cfg.MaxBackoff
			}
		}

		// Execute the function
		result, err := fn()
		if err == nil {
			return result, nil
		}

		lastErr = err

		// Check if we should retry
		if !isRetryable(err) {
			return zero, err
		}
	}

	return zero, fmt.Errorf("operation failed after %d retries: %w", cfg.MaxRetries, lastErr)
}

// DoVoid is like Do but for functions that don't return a value.
//
// Example:
//
//	err := retry.DoVoid(ctx, retry.DefaultConfig(), isTransientError, nil, func() error {
//	    return someOperation()
//	})
func DoVoid(
	ctx context.Context,
	cfg Config,
	isRetryable IsRetryableFunc,
	onRetry OnRetryFunc,
	fn func() error,
) error {
	_, err := Do(ctx, cfg, isRetryable, onRetry, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}
