package archiving

import (
	"log/slog"
	"sync"
	"time"
)

// DrainGate tracks the in-flight archive writes shutdown must wait for, and
// refuses any scheduled once the drain began: a handler its message loop
// abandoned outlives Stop() and would race the drain on a plain WaitGroup.
type DrainGate struct {
	mu          sync.Mutex
	idle        *sync.Cond
	draining    bool
	outstanding int
	logger      *slog.Logger
}

// NewDrainGate builds an open gate. A nil logger falls back to slog.Default().
func NewDrainGate(logger *slog.Logger) *DrainGate {
	if logger == nil {
		logger = slog.Default()
	}
	gate := &DrainGate{logger: logger}
	gate.idle = sync.NewCond(&gate.mu)
	return gate
}

// Go runs work in a tracked goroutine. It reports false, and does not run work,
// once Drain has begun.
func (g *DrainGate) Go(work func()) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.draining {
		return false
	}
	g.outstanding++
	go func() {
		defer g.finish()
		work()
	}()
	return true
}

func (g *DrainGate) finish() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.outstanding--
	if g.outstanding == 0 {
		g.idle.Broadcast()
	}
}

// Wait blocks until the writes already running finish, leaving the gate open.
func (g *DrainGate) Wait() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.outstanding > 0 {
		g.idle.Wait()
	}
}

// WaitBounded waits up to budget for the writes already running and leaves the
// gate open, so the next unit of work on this process still archives. It
// reports whether they all finished and how many did not; the ones that did not
// keep running and still record their own outcome.
func (g *DrainGate) WaitBounded(budget time.Duration) (finished bool, outstanding int) {
	return g.waitFor(budget)
}

// Drain closes the gate for good, then waits up to budget for the writes
// already running. It reports whether they all finished and how many did not.
func (g *DrainGate) Drain(budget time.Duration) (finished bool, outstanding int) {
	if g.close() {
		g.logger.Warn("archive drain gate was already closed; this drain can only wait out what the first one left behind")
	}
	return g.waitFor(budget)
}

// close shuts the gate and reports whether it was already shut.
func (g *DrainGate) close() (alreadyClosed bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	alreadyClosed = g.draining
	g.draining = true
	return alreadyClosed
}

func (g *DrainGate) waitFor(budget time.Duration) (finished bool, outstanding int) {
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		g.Wait()
	}()

	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case <-drained:
		return true, 0
	case <-timer.C:
	}

	// The last write can land as the budget expires, and a shortfall reported
	// with nothing outstanding reads to an operator as nothing lost.
	outstanding = g.Outstanding()
	return outstanding == 0, outstanding
}

// Outstanding reports how many archive writes are still running.
func (g *DrainGate) Outstanding() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.outstanding
}
