package archiving

import (
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
}

func NewDrainGate() *DrainGate {
	gate := &DrainGate{}
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

// Drain closes the gate for good, then waits up to budget for the writes
// already running. It reports whether they all finished and how many did not.
func (g *DrainGate) Drain(budget time.Duration) (finished bool, outstanding int) {
	g.mu.Lock()
	g.draining = true
	g.mu.Unlock()

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
		return false, g.Outstanding()
	}
}

// Outstanding reports how many archive writes are still running.
func (g *DrainGate) Outstanding() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.outstanding
}
