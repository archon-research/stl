package archiving

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDrainGate_RunsAndTracksWorkBeforeTheDrain(t *testing.T) {
	gate := NewDrainGate()
	ran := make(chan struct{})

	if !gate.Go(func() { close(ran) }) {
		t.Fatal("expected an open gate to accept the write")
	}

	gate.Wait()
	select {
	case <-ran:
	default:
		t.Error("expected Wait to block until the accepted write finished")
	}
}

func TestDrainGate_RefusesWorkScheduledAfterTheDrainBegan(t *testing.T) {
	gate := NewDrainGate()
	gate.Drain(time.Minute)

	if gate.Go(func() { t.Error("a refused write must not run") }) {
		t.Error("expected the gate to refuse a write scheduled after the drain began")
	}
}

func TestDrainGate_ReportsWorkThatOutlastsTheBudget(t *testing.T) {
	gate := NewDrainGate()
	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck); gate.Wait() })
	gate.Go(func() { <-stuck })

	finished, outstanding := gate.Drain(20 * time.Millisecond)

	if finished {
		t.Error("expected the drain to give up on a write that outlasts its budget")
	}
	if outstanding != 1 {
		t.Errorf("expected the abandoned write counted, got outstanding %d", outstanding)
	}
}

func TestDrainGate_ReportsWorkThatFinishesInTime(t *testing.T) {
	gate := NewDrainGate()
	gate.Go(func() {})

	finished, outstanding := gate.Drain(time.Minute)

	if !finished || outstanding != 0 {
		t.Errorf("expected a prompt write to drain, got finished=%v outstanding=%d", finished, outstanding)
	}
}

// The abandoned handler this gate exists for schedules its write while the
// deferred drain is already running; a plain WaitGroup panics on that shape.
func TestDrainGate_SurvivesWorkRacingTheDrain(t *testing.T) {
	gate := NewDrainGate()
	var accepted atomic.Int64
	var scheduling sync.WaitGroup
	scheduling.Go(func() {
		for range 500 {
			if gate.Go(func() {}) {
				accepted.Add(1)
			}
		}
	})

	finished, outstanding := gate.Drain(time.Minute)
	scheduling.Wait()

	if !finished || outstanding != 0 {
		t.Errorf("expected every accepted write drained, got finished=%v outstanding=%d", finished, outstanding)
	}
	if gate.Go(func() { t.Error("a refused write must not run") }) {
		t.Error("expected the gate closed for good once the drain began")
	}
}
