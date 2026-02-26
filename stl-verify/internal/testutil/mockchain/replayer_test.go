package mockchain

import (
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const testInterval = 250 * time.Millisecond // like Base chain

// newTestReplayer creates a Replayer with a fast interval and a buffered channel collector.
func newTestReplayer(t *testing.T) (*Replayer, chan outbound.BlockHeader) {
	t.Helper()
	ds := NewTestDataStore()
	received := make(chan outbound.BlockHeader, 32)
	r := NewReplayer(ds.Headers(), ds, func(h outbound.BlockHeader) {
		received <- h
	})
	r.interval = testInterval
	return r, received
}

// drain collects exactly n headers from the channel, failing if it times out.
func drain(t *testing.T, ch chan outbound.BlockHeader, n int) []outbound.BlockHeader {
	t.Helper()
	out := make([]outbound.BlockHeader, 0, n)
	for i := 0; i < n; i++ {
		select {
		case h := <-ch:
			out = append(out, h)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for emission %d/%d", i+1, n)
		}
	}
	return out
}

// TestNewReplayer verifies that the constructor sets fields correctly.
func TestNewReplayer(t *testing.T) {
	ds := NewTestDataStore()
	r := NewReplayer(ds.Headers(), ds, func(_ outbound.BlockHeader) {})

	if r.interval != defaultInterval {
		t.Errorf("expected interval %v, got %v", defaultInterval, r.interval)
	}
	if len(r.templates) != ds.Len() {
		t.Errorf("expected %d templates, got %d", ds.Len(), len(r.templates))
	}
	if r.store != ds {
		t.Error("expected store to be set")
	}
	if r.onBlock == nil {
		t.Error("expected onBlock to be set")
	}
	if r.running {
		t.Error("expected replayer to not be running after construction")
	}
}

// TestReplayer_BasicEmit verifies that the replayer emits blocks and Stop returns the count.
func TestReplayer_BasicEmit(t *testing.T) {
	r, received := newTestReplayer(t)

	r.Start()
	drain(t, received, 3)
	emitted := r.Stop()

	if emitted < 3 {
		t.Errorf("expected at least 3 blocks emitted, got %d", emitted)
	}
}

// TestReplayer_StopReturnsCount verifies Stop() return value equals the number of onBlock calls.
func TestReplayer_StopReturnsCount(t *testing.T) {
	r, received := newTestReplayer(t)

	r.Start()
	drain(t, received, 3)
	emitted := r.Stop()

	// Drain any remaining buffered emissions after stop.
	total := int64(3)
	for len(received) > 0 {
		<-received
		total++
	}

	if emitted != total {
		t.Errorf("Stop() returned %d but onBlock was called %d times", emitted, total)
	}
}

// TestReplayer_IndexWraps verifies that templateIndex wraps around after exhausting all templates.
func TestReplayer_IndexWraps(t *testing.T) {
	r, received := newTestReplayer(t)
	n := len(r.templates) // 3

	r.Start()
	drain(t, received, n+1) // one full loop plus one more
	r.Stop()

	// After n+1 emissions from n templates, index should be at 1.
	if r.templateIndex != 1 {
		t.Errorf("expected templateIndex 1 after wrap, got %d", r.templateIndex)
	}
}

// TestReplayer_StartIdempotent verifies that calling Start twice does not launch two goroutines.
func TestReplayer_StartIdempotent(t *testing.T) {
	r, received := newTestReplayer(t)

	r.Start()
	r.Start() // second call must be a no-op

	drain(t, received, 2)
	emitted := r.Stop()

	// Drain any buffered after stop to get the true total.
	total := int64(2)
	for len(received) > 0 {
		<-received
		total++
	}

	if emitted != total {
		t.Errorf("possible double-goroutine: Stop()=%d, onBlock count=%d", emitted, total)
	}
}

// TestReplayer_StopNotRunning verifies that Stop on a never-started replayer returns 0.
func TestReplayer_StopNotRunning(t *testing.T) {
	ds := NewTestDataStore()
	r := NewReplayer(ds.Headers(), ds, func(_ outbound.BlockHeader) {})

	emitted := r.Stop()
	if emitted != 0 {
		t.Errorf("expected 0 from Stop on never-started replayer, got %d", emitted)
	}
}

// TestReplayer_GetStatus verifies status transitions: before start, while running, after stop.
func TestReplayer_GetStatus(t *testing.T) {
	r, received := newTestReplayer(t)

	s := r.GetStatus()
	if s.Running {
		t.Error("expected Running=false before Start")
	}
	if s.BlocksEmitted != 0 {
		t.Errorf("expected BlocksEmitted=0 before Start, got %d", s.BlocksEmitted)
	}

	r.Start()
	drain(t, received, 1)

	s = r.GetStatus()
	if !s.Running {
		t.Error("expected Running=true while running")
	}
	if s.BlocksEmitted < 1 {
		t.Errorf("expected BlocksEmitted >= 1 while running, got %d", s.BlocksEmitted)
	}

	r.Stop()

	s = r.GetStatus()
	if s.Running {
		t.Error("expected Running=false after Stop")
	}
}

// TestReplayer_RestartResetsState verifies that starting after stopping resets the counters.
func TestReplayer_RestartResetsState(t *testing.T) {
	r, received := newTestReplayer(t)

	r.Start()
	drain(t, received, 2)
	r.Stop()

	// Drain any buffered emissions from the first run.
	for len(received) > 0 {
		<-received
	}

	r.Start()
	drain(t, received, 1)

	s := r.GetStatus()
	if s.BlocksEmitted < 1 {
		t.Errorf("expected BlocksEmitted >= 1 after restart, got %d", s.BlocksEmitted)
	}
	// blocksEmitted must have reset — it should not include the first run's count.
	if s.BlocksEmitted > 5 {
		t.Errorf("blocksEmitted looks like it wasn't reset: %d", s.BlocksEmitted)
	}

	r.Stop()
}
