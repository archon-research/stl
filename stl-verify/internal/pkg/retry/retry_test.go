package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errTransient = errors.New("transient error")
var errPermanent = errors.New("permanent error")

func isTransient(err error) bool {
	return errors.Is(err, errTransient)
}

func TestDo_SucceedsFirstAttempt(t *testing.T) {
	calls := 0
	result, err := Do(context.Background(), DefaultConfig(), isTransient, nil, func() (int, error) {
		calls++
		return 42, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 42 {
		t.Errorf("expected result 42, got %d", result)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestDo_RetriesOnTransientError(t *testing.T) {
	calls := 0
	cfg := Config{
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
		Jitter:         false,
	}

	result, err := Do(context.Background(), cfg, isTransient, nil, func() (int, error) {
		calls++
		if calls < 3 {
			return 0, errTransient
		}
		return 42, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 42 {
		t.Errorf("expected result 42, got %d", result)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestDo_FailsImmediatelyOnPermanentError(t *testing.T) {
	calls := 0
	cfg := Config{
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
	}

	_, err := Do(context.Background(), cfg, isTransient, nil, func() (int, error) {
		calls++
		return 0, errPermanent
	})

	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, errPermanent) {
		t.Errorf("expected permanent error, got: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call (no retries for permanent error), got %d", calls)
	}
}

func TestDo_ExhaustsRetries(t *testing.T) {
	calls := 0
	cfg := Config{
		MaxRetries:     2,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		Jitter:         false,
	}

	_, err := Do(context.Background(), cfg, isTransient, nil, func() (int, error) {
		calls++
		return 0, errTransient
	})

	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if !errors.Is(err, errTransient) {
		t.Errorf("expected transient error wrapped, got: %v", err)
	}
	// 1 initial + 2 retries = 3 total
	if calls != 3 {
		t.Errorf("expected 3 calls (1 + 2 retries), got %d", calls)
	}
}

func TestDo_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	cfg := Config{
		MaxRetries:     10,
		InitialBackoff: 100 * time.Millisecond,
		Jitter:         false,
	}

	// Cancel after first failure
	onRetry := func(attempt int, err error, backoff time.Duration) {
		cancel()
	}

	_, err := Do(ctx, cfg, isTransient, onRetry, func() (int, error) {
		calls++
		return 0, errTransient
	})

	if err == nil {
		t.Fatal("expected error on context cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call before cancellation, got %d", calls)
	}
}

func TestDo_CallsOnRetry(t *testing.T) {
	cfg := Config{
		MaxRetries:     2,
		InitialBackoff: 1 * time.Millisecond,
		Jitter:         false,
	}

	retryAttempts := []int{}
	onRetry := func(attempt int, err error, backoff time.Duration) {
		retryAttempts = append(retryAttempts, attempt)
	}

	_, _ = Do(context.Background(), cfg, isTransient, onRetry, func() (int, error) {
		return 0, errTransient
	})

	if len(retryAttempts) != 2 {
		t.Errorf("expected 2 onRetry calls, got %d", len(retryAttempts))
	}
	if retryAttempts[0] != 1 || retryAttempts[1] != 2 {
		t.Errorf("expected attempts [1, 2], got %v", retryAttempts)
	}
}

func TestDo_ExponentialBackoff(t *testing.T) {
	cfg := Config{
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
		Jitter:         false,
	}

	backoffs := []time.Duration{}
	onRetry := func(attempt int, err error, backoff time.Duration) {
		backoffs = append(backoffs, backoff)
	}

	_, _ = Do(context.Background(), cfg, isTransient, onRetry, func() (int, error) {
		return 0, errTransient
	})

	// Expected: 10ms, 20ms, 40ms
	expected := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	for i, exp := range expected {
		if i >= len(backoffs) {
			t.Errorf("missing backoff at index %d", i)
			continue
		}
		if backoffs[i] != exp {
			t.Errorf("backoff[%d]: expected %v, got %v", i, exp, backoffs[i])
		}
	}
}

func TestDo_CapsBackoffAtMax(t *testing.T) {
	cfg := Config{
		MaxRetries:     5,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
		Jitter:         false,
	}

	backoffs := []time.Duration{}
	onRetry := func(attempt int, err error, backoff time.Duration) {
		backoffs = append(backoffs, backoff)
	}

	_, _ = Do(context.Background(), cfg, isTransient, onRetry, func() (int, error) {
		return 0, errTransient
	})

	// Expected: 50ms, 100ms (capped), 100ms (capped), 100ms (capped), 100ms (capped)
	for i, b := range backoffs {
		if b > cfg.MaxBackoff {
			t.Errorf("backoff[%d] = %v exceeds max %v", i, b, cfg.MaxBackoff)
		}
	}
}

func TestDoVoid_Success(t *testing.T) {
	calls := 0
	err := DoVoid(context.Background(), DefaultConfig(), isTransient, nil, func() error {
		calls++
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestDoVoid_RetriesAndSucceeds(t *testing.T) {
	calls := 0
	cfg := Config{
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		Jitter:         false,
	}

	err := DoVoid(context.Background(), cfg, isTransient, nil, func() error {
		calls++
		if calls < 2 {
			return errTransient
		}
		return nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls, got %d", calls)
	}
}

func TestDo_AppliesDefaults(t *testing.T) {
	// Empty config should apply defaults
	cfg := Config{}

	backoffs := []time.Duration{}
	onRetry := func(attempt int, err error, backoff time.Duration) {
		backoffs = append(backoffs, backoff)
	}

	_, _ = Do(context.Background(), cfg, isTransient, onRetry, func() (int, error) {
		return 0, errTransient
	})

	// Should use default InitialBackoff (10ms)
	if len(backoffs) > 0 && backoffs[0] < 10*time.Millisecond {
		t.Errorf("expected default initial backoff of at least 10ms, got %v", backoffs[0])
	}
}
