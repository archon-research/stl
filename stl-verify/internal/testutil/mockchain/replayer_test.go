package mockchain

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const testInterval = 250 * time.Millisecond // like Base chain

// mustBaseBlockNumber returns the base block number of r, failing the test if it cannot be parsed.
func mustBaseBlockNumber(t *testing.T, r *Replayer) int64 {
	t.Helper()
	n, err := r.baseBlockNumber()
	if err != nil {
		t.Fatalf("baseBlockNumber: %v", err)
	}
	return n
}

// newTestReplayer creates a Replayer with a fast interval and a buffered channel collector.
func newTestReplayer(t *testing.T) (*Replayer, chan outbound.BlockHeader) {
	t.Helper()
	ds := NewFixtureDataStore()
	received := make(chan outbound.BlockHeader, 32)
	r := NewReplayer(ds.Headers(), ds, func(h outbound.BlockHeader) {
		received <- h
	}, testInterval)
	return r, received
}

// drain collects exactly n headers from the channel, failing if it times out.
func drain(t *testing.T, ch chan outbound.BlockHeader, n int) []outbound.BlockHeader {
	t.Helper()
	out := make([]outbound.BlockHeader, 0, n)
	for i := range n {
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
	ds := NewFixtureDataStore()
	r := NewReplayer(ds.Headers(), ds, func(_ outbound.BlockHeader) {}, 0)

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

	if err := r.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	drain(t, received, 3)
	emitted := r.Stop()

	if emitted < 3 {
		t.Errorf("expected at least 3 blocks emitted, got %d", emitted)
	}
}

// TestReplayer_StopReturnsCount verifies Stop() return value equals the number of onBlock calls.
func TestReplayer_StopReturnsCount(t *testing.T) {
	r, received := newTestReplayer(t)

	if err := r.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
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

	if err := r.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	drain(t, received, n+1) // one full loop plus one more
	r.Stop()

	// After n+1 emissions from n templates, index should be at 1.
	if r.getStatus().TemplateIndex != 1 {
		t.Errorf("expected templateIndex 1 after wrap, got %d", r.getStatus().TemplateIndex)
	}
}

// TestReplayer_StartIdempotent verifies that a second Start while running returns an error
// and does not launch a second goroutine.
func TestReplayer_StartIdempotent(t *testing.T) {
	r, received := newTestReplayer(t)

	if err := r.Start(); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := r.Start(); err == nil {
		t.Error("second Start while running: got nil, want error")
	}

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
	ds := NewFixtureDataStore()
	r := NewReplayer(ds.Headers(), ds, func(_ outbound.BlockHeader) {}, 0)

	emitted := r.Stop()
	if emitted != 0 {
		t.Errorf("expected 0 from Stop on never-started replayer, got %d", emitted)
	}
}

// TestReplayer_getStatus verifies status transitions: before start, while running, after stop.
func TestReplayer_getStatus(t *testing.T) {
	r, received := newTestReplayer(t)

	s := r.getStatus()
	if s.Running {
		t.Error("expected Running=false before Start")
	}
	if s.BlocksEmitted != 0 {
		t.Errorf("expected BlocksEmitted=0 before Start, got %d", s.BlocksEmitted)
	}

	if err := r.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	drain(t, received, 1)

	s = r.getStatus()
	if !s.Running {
		t.Error("expected Running=true while running")
	}
	if s.BlocksEmitted < 1 {
		t.Errorf("expected BlocksEmitted >= 1 while running, got %d", s.BlocksEmitted)
	}

	r.Stop()

	s = r.getStatus()
	if s.Running {
		t.Error("expected Running=false after Stop")
	}
}

// TestDeriveHash verifies that deriveHash is deterministic and loop-index-sensitive.
func TestDeriveHash(t *testing.T) {
	orig := "0xabcdef"

	h0 := deriveHash(orig, 0)
	h1 := deriveHash(orig, 1)

	if h0 == h1 {
		t.Error("expected different hashes for different loop indices")
	}
	if deriveHash(orig, 0) != h0 {
		t.Error("expected deriveHash to be deterministic")
	}
	if len(h0) != 66 || h0[:2] != "0x" {
		t.Errorf("expected 0x-prefixed 64-char hash, got %q", h0)
	}
}

// TestPatchHeader verifies that patchHeader sets Number, Hash, and ParentHash correctly
// and leaves all other fields unchanged.
func TestPatchHeader(t *testing.T) {
	tmpl := outbound.BlockHeader{
		Number:     "0x1",
		Hash:       "0xoriginal",
		ParentHash: "0xparent",
		Timestamp:  "0x67c00000",
	}
	parent := "0x" + strings.Repeat("a", 64)

	got := patchHeader(tmpl, 42, 3, parent)

	if got.Number != "0x2a" {
		t.Errorf("expected Number 0x2a, got %q", got.Number)
	}
	if got.Hash != deriveHash("0xoriginal", 3) {
		t.Errorf("expected Hash = deriveHash(orig, 3), got %q", got.Hash)
	}
	if got.ParentHash != parent {
		t.Errorf("expected ParentHash %q, got %q", parent, got.ParentHash)
	}
	if got.Timestamp != tmpl.Timestamp {
		t.Errorf("expected Timestamp unchanged, got %q", got.Timestamp)
	}
}

// TestReplayer_LoopContinuity emits more blocks than the number of templates and verifies
// that block numbers are sequential, no hash repeats, and parentHash[N] == hash[N-1].
func TestReplayer_LoopContinuity(t *testing.T) {
	ds := NewFixtureDataStore()
	var headers []outbound.BlockHeader
	r := NewReplayer(ds.Headers(), ds, func(h outbound.BlockHeader) {
		headers = append(headers, h)
	}, 0)

	n := len(r.templates)*2 + 1 // two full loops + one extra
	for range n {
		if err := r.emit(); err != nil {
			t.Fatalf("emit: %v", err)
		}
	}

	if len(headers) != n {
		t.Fatalf("expected %d headers, got %d", n, len(headers))
	}

	base := mustBaseBlockNumber(t, r)
	seen := make(map[string]bool)
	for i, h := range headers {
		if seen[h.Hash] {
			t.Errorf("duplicate hash %q at emission %d", h.Hash, i)
		}
		seen[h.Hash] = true

		wantNumber := fmt.Sprintf("0x%x", base+int64(i))
		if h.Number != wantNumber {
			t.Errorf("emission %d: Number %q, want %q", i, h.Number, wantNumber)
		}

		if i == 0 {
			continue
		}
		if h.ParentHash != headers[i-1].Hash {
			t.Errorf("emission %d: parentHash %q != prev hash %q", i, h.ParentHash, headers[i-1].Hash)
		}
	}
}

// TestReplayer_HeaderForHash verifies that HeaderForHash returns the correct header
// and that its ParentHash matches the previous block's Hash.
func TestReplayer_HeaderForHash(t *testing.T) {
	ds := NewFixtureDataStore()
	r := NewReplayer(ds.Headers(), ds, func(_ outbound.BlockHeader) {}, 0)

	for range 3 {
		if err := r.emit(); err != nil {
			t.Fatalf("emit: %v", err)
		}
	}

	// HeaderForHash for the third emitted block.
	thirdHash := r.prevDerivedHash
	h, ok := r.HeaderForHash(thirdHash)
	if !ok {
		t.Fatal("expected HeaderForHash to find the emitted hash")
	}
	if h.Hash != thirdHash {
		t.Errorf("expected Hash %q, got %q", thirdHash, h.Hash)
	}

	// ParentHash of block 3 must equal hash of block 2.
	h2, ok := r.HeaderForNumber(mustBaseBlockNumber(t, r) + 1)
	if !ok {
		t.Fatal("expected HeaderForNumber to find block 2")
	}
	if h.ParentHash != h2.Hash {
		t.Errorf("block 3 parentHash %q != block 2 hash %q", h.ParentHash, h2.Hash)
	}

	// Unknown hash returns false.
	if _, ok := r.HeaderForHash("0xdeadbeef"); ok {
		t.Error("expected HeaderForHash to return false for unknown hash")
	}
}

// TestReplayer_HeaderForNumber verifies that HeaderForNumber is deterministic and
// consistent with what emit produced.
func TestReplayer_HeaderForNumber(t *testing.T) {
	ds := NewFixtureDataStore()
	var emitted []outbound.BlockHeader
	r := NewReplayer(ds.Headers(), ds, func(h outbound.BlockHeader) {
		emitted = append(emitted, h)
	}, 0)

	for range 3 {
		if err := r.emit(); err != nil {
			t.Fatalf("emit: %v", err)
		}
	}

	base := mustBaseBlockNumber(t, r)
	for i, want := range emitted {
		blockNum := base + int64(i)
		got, ok := r.HeaderForNumber(blockNum)
		if !ok {
			t.Fatalf("HeaderForNumber(%d) returned false", blockNum)
		}
		if got.Hash != want.Hash {
			t.Errorf("block %d: HeaderForNumber hash %q != emitted hash %q", blockNum, got.Hash, want.Hash)
		}
		if got.Number != want.Number {
			t.Errorf("block %d: HeaderForNumber number %q != emitted number %q", blockNum, got.Number, want.Number)
		}
	}

	// Block before base and block not yet emitted return false.
	if _, ok := r.HeaderForNumber(base - 1); ok {
		t.Error("expected false for block before base")
	}
	if _, ok := r.HeaderForNumber(base + 999); ok {
		t.Error("expected false for block not yet emitted")
	}
}

// TestReplayer_SetInterval verifies that SetInterval controls the emission cadence.
func TestReplayer_SetInterval(t *testing.T) {
	r, received := newTestReplayer(t)
	if err := r.SetInterval(50 * time.Millisecond); err != nil {
		t.Fatalf("SetInterval: %v", err)
	}

	start := time.Now()
	if err := r.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	drain(t, received, 3)
	r.Stop()
	elapsed := time.Since(start)

	// 3 blocks at 50ms each: first tick at ~50ms, third at ~150ms.
	// Lower bound: at least 2 full intervals must elapse between first and third block.
	// Upper bound: generous CI allowance.
	if elapsed < 100*time.Millisecond {
		t.Errorf("interval appears uncontrolled: 3 blocks arrived in %v (want ≥100ms with 50ms interval)", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("expected 3 blocks in <500ms with 50ms interval, took %v", elapsed)
	}
}

// TestReplayer_SetInterval_WhileRunning verifies that SetInterval succeeds while running
// and that the new interval takes effect for subsequent emissions.
func TestReplayer_SetInterval_WhileRunning(t *testing.T) {
	r, received := newTestReplayer(t)
	if err := r.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	drain(t, received, 1)
	defer r.Stop()

	if err := r.SetInterval(100 * time.Millisecond); err != nil {
		t.Fatalf("SetInterval while running: %v", err)
	}

	// The first emission after the change may still arrive on the old cadence.
	// Measure the gap between the next two emissions to confirm the new interval took effect.
	drain(t, received, 1) // first post-change emission (may be on old cadence)
	t1 := time.Now()
	drain(t, received, 1)
	gap := time.Since(t1)
	if gap > 300*time.Millisecond {
		t.Errorf("inter-block gap after SetInterval: %v (want ≤300ms with 100ms interval)", gap)
	}
}

// TestReplayer_SetInterval_NonPositive verifies that SetInterval returns an error for zero and negative values.
func TestReplayer_SetInterval_NonPositive(t *testing.T) {
	for _, d := range []time.Duration{0, -1 * time.Millisecond} {
		t.Run(d.String(), func(t *testing.T) {
			ds := NewFixtureDataStore()
			r := NewReplayer(ds.Headers(), ds, func(_ outbound.BlockHeader) {}, 0)
			if err := r.SetInterval(d); err == nil {
				t.Errorf("expected error for SetInterval(%v)", d)
			}
		})
	}
}

// TestReplayer_EmptyTemplates verifies that Start returns an error when there are no templates.
func TestReplayer_EmptyTemplates(t *testing.T) {
	ds := NewDataStore() // empty — no headers
	r := NewReplayer(ds.Headers(), ds, func(_ outbound.BlockHeader) {
		t.Error("onBlock must not be called with empty templates")
	}, 0)

	if err := r.Start(); err == nil {
		t.Error("Start with empty templates: got nil, want error")
	}
	emitted := r.Stop()

	if emitted != 0 {
		t.Errorf("expected 0 emissions with empty templates, got %d", emitted)
	}
}

// TestReplayer_RestartResetsState verifies that starting after stopping resets the counters.
func TestReplayer_RestartResetsState(t *testing.T) {
	r, received := newTestReplayer(t)

	if err := r.Start(); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	drain(t, received, 2)
	r.Stop()

	// Drain any buffered emissions from the first run.
	for len(received) > 0 {
		<-received
	}

	if err := r.Start(); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	drain(t, received, 1)

	s := r.getStatus()
	if s.BlocksEmitted < 1 {
		t.Errorf("expected BlocksEmitted >= 1 after restart, got %d", s.BlocksEmitted)
	}
	// blocksEmitted must have reset — it should not include the first run's count.
	if s.BlocksEmitted > 5 {
		t.Errorf("blocksEmitted looks like it wasn't reset: %d", s.BlocksEmitted)
	}

	r.Stop()
}

// TestReplayer_StartWithInvalidTemplate verifies that Start returns an error wrapping
// errInvalidTemplateNum when any template contains an unparseable block number,
// and that no blocks are emitted.
func TestReplayer_StartWithInvalidTemplate(t *testing.T) {
	tests := []struct {
		name    string
		headers []outbound.BlockHeader
	}{
		{
			name: "empty Number field",
			headers: []outbound.BlockHeader{
				{Number: "", Hash: "0xabc", ParentHash: "0x000"},
			},
		},
		{
			name: "non-hex Number field",
			headers: []outbound.BlockHeader{
				{Number: "not-a-number", Hash: "0xabc", ParentHash: "0x000"},
			},
		},
		{
			name: "invalid Number in second template",
			headers: []outbound.BlockHeader{
				{Number: "0x1", Hash: "0xaaa", ParentHash: "0x000"},
				{Number: "0xgg", Hash: "0xbbb", ParentHash: "0xaaa"}, // "gg" is not valid hex
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := NewDataStore()
			r := NewReplayer(tt.headers, ds, func(_ outbound.BlockHeader) {
				t.Error("onBlock must not be called with invalid templates")
			}, 0)

			err := r.Start()
			if err == nil {
				t.Fatal("Start with invalid template: got nil, want error")
				r.Stop()
				return
			}
			if !errors.Is(err, errInvalidTemplateNum) {
				t.Errorf("got error %v; want it to wrap errInvalidTemplateNum", err)
			}
			if emitted := r.Stop(); emitted != 0 {
				t.Errorf("expected 0 emissions with invalid templates, got %d", emitted)
			}
		})
	}
}
