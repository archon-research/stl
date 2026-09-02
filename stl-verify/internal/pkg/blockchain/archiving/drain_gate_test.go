package archiving

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestDrainGate_RunsAndTracksWorkBeforeTheDrain(t *testing.T) {
	gate := NewDrainGate(nil)
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
	gate := NewDrainGate(nil)
	gate.Drain(time.Minute)

	if gate.Go(func() { t.Error("a refused write must not run") }) {
		t.Error("expected the gate to refuse a write scheduled after the drain began")
	}
}

func TestDrainGate_ReportsWorkThatOutlastsTheBudget(t *testing.T) {
	gate := NewDrainGate(nil)
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

// A write that recorded its outcome just before the budget expired is not lost:
// its goroutine is still winding down, but its batch already has its one status,
// and counting it again is what made the runbook's parity identity absorb holes.
func TestDrainGate_DoesNotAbandonAWriteThatAlreadyClaimedItsOutcome(t *testing.T) {
	gate := NewDrainGate(nil)
	claimed := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release); gate.Wait() })
	gate.Go(func() {
		if !gate.ClaimOutcome() {
			t.Error("expected a write running before the drain to own its outcome")
		}
		close(claimed)
		<-release
	})
	<-claimed

	clean, lost := gate.Drain(20 * time.Millisecond)

	if lost != 0 {
		t.Errorf("lost = %d, want 0: the write had already recorded its own status", lost)
	}
	if !clean {
		t.Error("expected the drain to report nothing abandoned")
	}
}

func TestDrainGate_ReportsWorkThatFinishesInTime(t *testing.T) {
	gate := NewDrainGate(nil)
	gate.Go(func() {})

	finished, outstanding := gate.Drain(time.Minute)

	if !finished || outstanding != 0 {
		t.Errorf("expected a prompt write to drain, got finished=%v outstanding=%d", finished, outstanding)
	}
}

// The abandoned handler this gate exists for schedules its write while the
// drain is already waiting, so the gate must shut before it starts waiting.
func TestDrainGate_RefusesWorkWhileItWaitsOutTheWritesInFlight(t *testing.T) {
	gate := NewDrainGate(nil)
	running := make(chan struct{})
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	gate.Go(func() { close(running); <-release })
	<-running

	drained := make(chan bool, 1)
	go func() {
		finished, _ := gate.Drain(time.Minute)
		drained <- finished
	}()

	if !refusedWithin(t, gate, time.Second) {
		t.Fatal("expected the gate shut while the drain waits on the write in flight")
	}
	select {
	case <-drained:
		t.Fatal("expected the drain to wait for the write in flight")
	default:
	}

	releaseOnce()
	if finished := <-drained; !finished {
		t.Error("expected the drain to report the write in flight finished")
	}
}

// refusedWithin polls until the gate refuses a write, so the assertion does not
// race the goroutine that calls Drain.
func refusedWithin(t *testing.T, gate *DrainGate, budget time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if !gate.Go(func() {}) {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// A wakeup only says the count reached zero at some point; a write accepted
// before the waiter reacquires the lock is still in flight, so Wait re-reads
// the count rather than trusting the wakeup.
func TestDrainGate_WaitReturnsOnlyWhenNothingIsOutstanding(t *testing.T) {
	gate := NewDrainGate(nil)
	running := make(chan struct{})
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	gate.Go(func() { close(running); <-release })
	<-running

	returned := make(chan struct{})
	go func() { defer close(returned); gate.Wait() }()

	for range 50 {
		gate.idle.Broadcast()
		select {
		case <-returned:
			t.Fatal("Wait returned with a write still outstanding")
		case <-time.After(time.Millisecond):
		}
	}

	releaseOnce()
	<-returned
}

// A Temporal activity waits out its own archive writes before it returns, and
// the next activity on the same worker must still be able to archive.
func TestDrainGate_StaysOpenAcrossBoundedWaits(t *testing.T) {
	gate := NewDrainGate(nil)
	first := make(chan struct{})
	gate.Go(func() { close(first) })

	if finished, outstanding := gate.WaitBounded(time.Minute); !finished || outstanding != 0 {
		t.Fatalf("first bounded wait = (%v, %d), want (true, 0)", finished, outstanding)
	}
	<-first

	second := make(chan struct{})
	if !gate.Go(func() { close(second) }) {
		t.Fatal("expected the gate still open for the write after a bounded wait")
	}
	if finished, outstanding := gate.WaitBounded(time.Minute); !finished || outstanding != 0 {
		t.Fatalf("second bounded wait = (%v, %d), want (true, 0)", finished, outstanding)
	}
	<-second
}

func TestDrainGate_WarnsOnASecondDrain(t *testing.T) {
	recorder := &testutil.SlogRecorder{}
	gate := NewDrainGate(slog.New(recorder))

	gate.Drain(time.Minute)
	gate.Drain(time.Minute)

	if got := recorder.CountWarn("archive drain gate was already closed"); got != 1 {
		t.Errorf("expected the second drain warned once, got %d warnings", got)
	}
}

// Operators bound a shutdown's shortfall from the outstanding count in the
// abandoned-drain warning, so a wait whose writes landed just as the budget
// expired must report them finished rather than as an uncounted loss.
func TestDrainGate_NeverReportsAnUnfinishedWaitWithNothingOutstanding(t *testing.T) {
	waits := []struct {
		name string
		wait func(*DrainGate, time.Duration) (bool, int)
	}{
		{name: "Drain", wait: (*DrainGate).Drain},
		{name: "WaitBounded", wait: (*DrainGate).WaitBounded},
	}
	for _, tt := range waits {
		t.Run(tt.name, func(t *testing.T) {
			for range 100 {
				gate := NewDrainGate(nil)
				gate.Go(func() {})
				gate.Wait()

				if finished, outstanding := tt.wait(gate, 0); !finished && outstanding == 0 {
					t.Fatal("expected a wait that gave up to count what it left behind")
				}
			}
		})
	}
}
