package cex_feed_watcher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type fakeSubscriber struct {
	ch       chan entity.RawCEXMessage
	subbed   atomic.Bool
	closeErr error
	closed   atomic.Bool
}

func newFakeSubscriber() *fakeSubscriber {
	return &fakeSubscriber{ch: make(chan entity.RawCEXMessage, 10)}
}

func (f *fakeSubscriber) Subscribe(ctx context.Context) (<-chan entity.RawCEXMessage, error) {
	f.subbed.Store(true)
	return f.ch, nil
}

func (f *fakeSubscriber) Close() error {
	if !f.closed.Swap(true) {
		close(f.ch)
	}
	return f.closeErr
}

func (f *fakeSubscriber) HealthCheck() error { return nil }

type fakePublisher struct {
	mu       sync.Mutex
	received []entity.RawCEXMessage
	err      error
	closed   bool
}

func (p *fakePublisher) Publish(ctx context.Context, msg entity.RawCEXMessage) error {
	if p.err != nil {
		return p.err
	}
	p.mu.Lock()
	p.received = append(p.received, msg)
	p.mu.Unlock()
	return nil
}

func (p *fakePublisher) Close() error {
	p.closed = true
	return nil
}

func (p *fakePublisher) Snapshot() []entity.RawCEXMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]entity.RawCEXMessage, len(p.received))
	copy(out, p.received)
	return out
}

func TestNewService_NilSubscriber(t *testing.T) {
	_, err := NewService(Config{Source: "binance"}, nil, &fakePublisher{})
	if err == nil {
		t.Fatal("expected error for nil subscriber")
	}
}

func TestNewService_NilPublisher(t *testing.T) {
	_, err := NewService(Config{Source: "binance"}, newFakeSubscriber(), nil)
	if err == nil {
		t.Fatal("expected error for nil publisher")
	}
}

func TestNewService_EmptySource(t *testing.T) {
	_, err := NewService(Config{Source: ""}, newFakeSubscriber(), &fakePublisher{})
	if err == nil {
		t.Fatal("expected error for empty source")
	}
}

func TestNewService_OK(t *testing.T) {
	if _, err := NewService(Config{Source: "binance"}, newFakeSubscriber(), &fakePublisher{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestService_ForwardsMessages(t *testing.T) {
	sub := newFakeSubscriber()
	pub := &fakePublisher{}

	svc, err := NewService(Config{Source: "binance"}, sub, pub)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer svc.Stop()

	sub.ch <- entity.RawCEXMessage{Source: "binance", Payload: []byte("frame-1"), CapturedAt: time.Now()}
	sub.ch <- entity.RawCEXMessage{Source: "binance", Payload: []byte("frame-2"), CapturedAt: time.Now()}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(pub.Snapshot()) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	got := pub.Snapshot()
	if len(got) != 2 {
		t.Fatalf("expected 2 published messages, got %d", len(got))
	}
	if string(got[0].Payload) != "frame-1" || string(got[1].Payload) != "frame-2" {
		t.Errorf("unexpected payloads: %q, %q", got[0].Payload, got[1].Payload)
	}
}

func TestService_PublishErrorsAreLoggedNotFatal(t *testing.T) {
	sub := newFakeSubscriber()
	pub := &fakePublisher{err: errors.New("transient sns error")}

	svc, err := NewService(Config{Source: "binance"}, sub, pub)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// Pushing messages with a failing publisher should not block or panic.
	for i := 0; i < 5; i++ {
		sub.ch <- entity.RawCEXMessage{Source: "binance", Payload: []byte("x")}
	}
	time.Sleep(50 * time.Millisecond)

	if err := svc.Stop(); err != nil {
		t.Errorf("Stop returned error: %v", err)
	}
}

func TestService_StopClosesSubscriber(t *testing.T) {
	sub := newFakeSubscriber()
	pub := &fakePublisher{}
	svc, err := NewService(Config{Source: "binance"}, sub, pub)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := svc.Stop(); err != nil {
		t.Fatal(err)
	}
	if !sub.closed.Load() {
		t.Error("expected subscriber to be closed on Stop")
	}
}
