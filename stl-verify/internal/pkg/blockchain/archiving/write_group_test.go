package archiving

import (
	"context"
	"math/big"
	"sync"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// TestWriteGroupDropsWorkAfterDrain verifies the gate: once Wait has drained the
// group, a later Go is refused (returns false) and its function never runs.
func TestWriteGroupDropsWorkAfterDrain(t *testing.T) {
	var g WriteGroup

	ran := make(chan struct{}, 1)
	if !g.Go(func() { ran <- struct{}{} }) {
		t.Fatal("Go before drain must schedule the work")
	}
	<-ran

	g.Wait() // closes the gate and drains

	if g.Go(func() { t.Error("function scheduled after drain must not run") }) {
		t.Fatal("Go after drain must return false")
	}
}

// TestWriteGroupConcurrentGoAndWait exercises Go running concurrently with Wait,
// the exact shutdown hazard the gate exists to remove: a late Add must never
// race the draining Wait. Run under -race in CI, this fails if the gate
// regresses; functionally it must not panic.
func TestWriteGroupConcurrentGoAndWait(t *testing.T) {
	var g WriteGroup

	var started sync.WaitGroup
	started.Add(1)
	go func() {
		started.Done()
		for range 2000 {
			g.Go(func() {})
		}
	}()

	started.Wait()
	g.Wait() // must not panic or race even while Go calls are in flight
}

// TestExecuteAfterDrainDoesNotArchiveOrPanic proves the decorator's shutdown
// behaviour end to end: an Execute that lands after the shared group has drained
// still forwards results (the hot path is never affected) but archives nothing
// and does not panic, rather than triggering an Add-after-Wait misuse.
func TestExecuteAfterDrainDoesNotArchiveOrPanic(t *testing.T) {
	rec := &recordingArchiver{}
	var wg WriteGroup
	d := newTestDecorator(&stubInner{results: []outbound.Result{{Success: true}}}, rec, &wg)

	wg.Wait() // simulate the deferred shutdown drain firing first

	res, err := d.Execute(context.Background(), []outbound.Call{{CallData: []byte{0x01}}}, big.NewInt(1))
	if err != nil {
		t.Fatalf("Execute after drain: %v", err)
	}
	if len(res) != 1 || !res[0].Success {
		t.Fatalf("results must still be forwarded after drain: %+v", res)
	}

	wg.Wait()
	if len(rec.batches) != 0 {
		t.Fatalf("a late Execute must archive nothing after drain, got %d batches", len(rec.batches))
	}
}
