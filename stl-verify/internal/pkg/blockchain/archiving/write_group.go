package archiving

import "sync"

// WriteGroup tracks in-flight background archive writes and drains them on
// shutdown. It is a thin wrapper over sync.WaitGroup that closes a gate before
// waiting, so a late Go (an Add) can never race the draining Wait.
//
// The hazard it prevents: the worker run loop can still be mid-handler when the
// process begins shutting down, so a handler may schedule an archive write
// (WaitGroup.Add) concurrently with the deferred drain (WaitGroup.Wait). Add
// concurrent with Wait when the counter is zero is documented misuse: it can
// panic or let the write escape the drain it exists to guarantee. Gating the Add
// behind the same lock that closes the group removes that window.
type WriteGroup struct {
	mu     sync.Mutex
	wg     sync.WaitGroup
	closed bool
}

// Go runs fn in a tracked goroutine and reports whether it was scheduled. Once
// Wait has begun draining, the group is closed and Go returns false without
// running fn, so callers can surface the dropped write rather than leak it.
func (g *WriteGroup) Go(fn func()) bool {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return false
	}
	g.wg.Add(1)
	g.mu.Unlock()

	go func() {
		defer g.wg.Done()
		fn()
	}()
	return true
}

// Wait closes the group to new work, then blocks until every scheduled write
// finishes. Because closing and every Go check share g.mu, no Add can happen
// after the gate closes, so the subsequent wg.Wait never races an Add.
func (g *WriteGroup) Wait() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()

	g.wg.Wait()
}
