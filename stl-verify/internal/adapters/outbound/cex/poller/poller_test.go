package poller_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cex/poller"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

func okFetch(_ context.Context, symbol string) (entity.OrderBookSnapshot, error) {
	return entity.OrderBookSnapshot{Exchange: "test", Token: symbol, CapturedAt: time.Now()}, nil
}

func TestPoller_Name(t *testing.T) {
	p := poller.New("test", okFetch, time.Second, 1)
	if got := p.Name(); got != "test" {
		t.Errorf("Name() = %q, want %q", got, "test")
	}
}

func TestPoller_Connect_Validation(t *testing.T) {
	tests := []struct {
		name    string
		symbols []string
		wantErr bool
	}{
		{name: "valid", symbols: []string{"BTC-USD"}, wantErr: false},
		{name: "empty slice", symbols: []string{}, wantErr: true},
		{name: "nil slice", symbols: nil, wantErr: true},
		{name: "blank symbol", symbols: []string{""}, wantErr: true},
		{name: "whitespace-only symbol", symbols: []string{"   "}, wantErr: true},
		{name: "surrounding whitespace", symbols: []string{" BTC-USD "}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := poller.New("test", okFetch, time.Second, 1)
			err := p.Connect(context.Background(), tt.symbols)
			if (err != nil) != tt.wantErr {
				t.Errorf("Connect(%v) error = %v, wantErr %v", tt.symbols, err, tt.wantErr)
			}
		})
	}
}

func TestPoller_Stream_BeforeConnect_ReturnsError(t *testing.T) {
	p := poller.New("test", okFetch, time.Second, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	snapCh, errCh := p.Stream(ctx)
	select {
	case err, ok := <-errCh:
		if !ok || err == nil {
			t.Fatal("expected a non-nil error before Connect")
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for error")
	}
	if _, ok := <-snapCh; ok {
		t.Error("snapshot channel should be closed and empty")
	}
}

func TestPoller_Stream_CalledTwice_ReturnsError(t *testing.T) {
	p := poller.New("test", okFetch, 10*time.Millisecond, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := p.Connect(ctx, []string{"BTC-USD"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	p.Stream(ctx) // first call claims the stream
	_, errCh := p.Stream(ctx)

	select {
	case err, ok := <-errCh:
		if !ok || err == nil {
			t.Fatal("expected a non-nil error on second Stream call")
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for error from second Stream call")
	}
}

func TestPoller_Stream_EmitsSnapshots(t *testing.T) {
	var calls atomic.Int64
	fetch := func(_ context.Context, symbol string) (entity.OrderBookSnapshot, error) {
		calls.Add(1)
		return entity.OrderBookSnapshot{Exchange: "test", Token: symbol, CapturedAt: time.Now()}, nil
	}
	p := poller.New("test", fetch, 5*time.Millisecond, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := p.Connect(ctx, []string{"BTC-USD"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	snapCh, errCh := p.Stream(ctx)

	select {
	case snap, ok := <-snapCh:
		if !ok {
			t.Fatal("snapshot channel closed before first value")
		}
		if snap.Token != "BTC-USD" {
			t.Errorf("Token = %q, want BTC-USD", snap.Token)
		}
	case err := <-errCh:
		t.Fatalf("unexpected error: %v", err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for snapshot")
	}
}

func TestPoller_Stream_FetchError_Terminates(t *testing.T) {
	sentinel := errors.New("boom")
	fetch := func(_ context.Context, _ string) (entity.OrderBookSnapshot, error) {
		return entity.OrderBookSnapshot{}, sentinel
	}
	p := poller.New("test", fetch, 5*time.Millisecond, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := p.Connect(ctx, []string{"BTC-USD"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	snapCh, errCh := p.Stream(ctx)

	select {
	case err, ok := <-errCh:
		if !ok {
			t.Fatal("error channel closed without error")
		}
		if !errors.Is(err, sentinel) {
			t.Errorf("error = %v, want it to wrap %v", err, sentinel)
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for fetch error")
	}
	// A terminal error closes the snapshot channel.
	select {
	case _, ok := <-snapCh:
		if ok {
			t.Error("snapshot channel should be closed after terminal error")
		}
	case <-ctx.Done():
		t.Fatal("snapshot channel not closed after terminal error")
	}
}

func TestPoller_Close_StopsStreaming(t *testing.T) {
	p := poller.New("test", okFetch, 10*time.Millisecond, 4)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := p.Connect(ctx, []string{"BTC-USD"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	snapCh, _ := p.Stream(ctx)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	timeout := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-snapCh:
			if !ok {
				return // channel closed: goroutine exited
			}
		case <-timeout:
			t.Fatal("snapshot channel not closed after Close()")
		}
	}
}

func TestPoller_Close_Idempotent(t *testing.T) {
	p := poller.New("test", okFetch, 10*time.Millisecond, 1)
	if err := p.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestPoller_Stream_SlowConsumerCancellation_NoLeak pins the inner-select
// ctx.Done() case: when the consumer never reads and the send blocks, a context
// cancel must still unwind the goroutine. We deliberately never read snapCh, so
// the send case stays not-ready and the ONLY way the goroutine can exit is via
// the inner select's ctx.Done() case. The goroutine closes errCh on exit, so we
// observe that close as proof of no leak. (An earlier version of this test
// drained snapCh, which unblocked the send itself and so passed even when the
// inner cancellation cases were deleted; reading errCh instead avoids that.)
func TestPoller_Stream_SlowConsumerCancellation_NoLeak(t *testing.T) {
	fetched := make(chan struct{}, 1)
	fetch := func(_ context.Context, symbol string) (entity.OrderBookSnapshot, error) {
		select {
		case fetched <- struct{}{}:
		default:
		}
		return entity.OrderBookSnapshot{Exchange: "test", Token: symbol, CapturedAt: time.Now()}, nil
	}
	// bufSize 0 => the very first send blocks until someone reads (nobody does).
	p := poller.New("test", fetch, time.Millisecond, 0)
	ctx, cancel := context.WithCancel(context.Background())

	if err := p.Connect(ctx, []string{"BTC-USD"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_, errCh := p.Stream(ctx)

	select {
	case <-fetched:
	case <-time.After(2 * time.Second):
		t.Fatal("fetch was never called")
	}
	// The goroutine has produced a snapshot and is now blocked trying to send
	// it (no reader, unbuffered). Cancelling must unblock the inner select.
	cancel()

	select {
	case _, ok := <-errCh:
		if ok {
			t.Fatal("expected a clean exit with a closed errCh, got an error value")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine leaked: errCh not closed after ctx cancel while blocked on send")
	}
}

// TestPoller_Stream_CloseWhileBlocked_NoLeak pins the inner-select closeCh case.
// The context stays live; the only exit path while the goroutine is blocked on
// an unbuffered send (no reader) is the inner select's <-p.closeCh case. As
// above we never read snapCh and instead observe errCh closing on exit.
func TestPoller_Stream_CloseWhileBlocked_NoLeak(t *testing.T) {
	fetched := make(chan struct{}, 1)
	fetch := func(_ context.Context, symbol string) (entity.OrderBookSnapshot, error) {
		select {
		case fetched <- struct{}{}:
		default:
		}
		return entity.OrderBookSnapshot{Exchange: "test", Token: symbol, CapturedAt: time.Now()}, nil
	}
	p := poller.New("test", fetch, time.Millisecond, 0)
	ctx := t.Context()

	if err := p.Connect(ctx, []string{"BTC-USD"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_, errCh := p.Stream(ctx)

	select {
	case <-fetched:
	case <-time.After(2 * time.Second):
		t.Fatal("fetch was never called")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case _, ok := <-errCh:
		if ok {
			t.Fatal("expected a clean exit with a closed errCh, got an error value")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine leaked: errCh not closed after Close while blocked on send")
	}
}

func TestPoller_New_PanicsOnMisuse(t *testing.T) {
	tests := []struct {
		name      string
		fetch     poller.FetchFunc
		pollEvery time.Duration
	}{
		{name: "nil fetch", fetch: nil, pollEvery: time.Second},
		{name: "zero poll interval", fetch: okFetch, pollEvery: 0},
		{name: "negative poll interval", fetch: okFetch, pollEvery: -time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected New to panic on misuse")
				}
			}()
			poller.New("test", tt.fetch, tt.pollEvery, 1)
		})
	}
}

func TestPoller_Stream_ContextCancellation_WhileIdle(t *testing.T) {
	// Long poll interval => the goroutine spends its time in the outer select
	// waiting on the ticker; cancellation must still unwind it promptly.
	p := poller.New("test", okFetch, time.Hour, 1)
	ctx, cancel := context.WithCancel(context.Background())

	if err := p.Connect(ctx, []string{"BTC-USD"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	snapCh, _ := p.Stream(ctx)
	cancel()

	select {
	case _, ok := <-snapCh:
		if ok {
			t.Error("did not expect a snapshot with an hour-long poll interval")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot channel not closed after ctx cancel while idle")
	}
}
