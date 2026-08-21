package temporal

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/sdk/activity"
)

// ActivityProgress carries a long-running activity's progress in its Temporal
// heartbeat details, so a run whose worker dies mid-flight (a pod kill, a
// deploy) resumes where it got to instead of starting over. The details live on
// the Temporal server, so nothing has to survive on the pod.
//
// The details are readable only by a LATER ATTEMPT OF THE SAME ACTIVITY
// EXECUTION. Two consequences the callers must live with: the activity's retry
// policy has to allow more than one attempt or there is nothing to resume into,
// and a freshly started workflow is a new execution with no heartbeat history,
// so it always starts from the beginning.
//
// One instance per worker process, shared by the runner and the liveness
// heartbeat: it is the store's copy of the last record that a liveness beat
// re-sends (see Beat).
type ActivityProgress[T any] struct {
	mu     sync.Mutex
	latest []any
	// record is activity.RecordHeartbeat, narrowed to a field so a test can read
	// the details a heartbeat carries. The SDK batches heartbeats before they
	// reach a test environment's listener, which is exactly where the details of
	// a suppressed beat would go unnoticed.
	record func(ctx context.Context, details ...any)
}

func NewActivityProgress[T any]() *ActivityProgress[T] {
	return &ActivityProgress[T]{record: activity.RecordHeartbeat}
}

// SaveProgress records progress as the activity's heartbeat details, replacing
// what the previous heartbeat carried.
//
// It cannot fail: a heartbeat the server rejects (the activity was canceled or
// its timeout expired) surfaces as a canceled activity context on the runner's
// next read, not as an error here.
func (p *ActivityProgress[T]) SaveProgress(ctx context.Context, progress T) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latest = []any{progress}
	p.record(ctx, p.latest...)
	return nil
}

// LoadProgress returns what an earlier attempt of this activity recorded.
// Absence — the first attempt of a run — is (zero, false, nil); details that
// cannot be decoded are an error, because silently treating them as absence
// would restart a multi-hour sweep without saying so.
//
// A successful read also seeds what a liveness Beat re-sends: a resumed attempt
// records nothing of its own until its first unit of work lands, and Temporal
// keeps only the last heartbeat's details, so a bare ping in that window would
// erase the record this attempt is resuming from.
func (p *ActivityProgress[T]) LoadProgress(ctx context.Context) (T, bool, error) {
	var progress T
	if !activity.HasHeartbeatDetails(ctx) {
		return progress, false, nil
	}
	if err := activity.GetHeartbeatDetails(ctx, &progress); err != nil {
		return progress, false, fmt.Errorf("decoding activity heartbeat details: %w", err)
	}
	p.mu.Lock()
	p.latest = []any{progress}
	p.mu.Unlock()
	return progress, true, nil
}

// Reset drops the record this store carries. The store lives for the worker
// PROCESS while heartbeat details belong to one activity execution, so without
// this a second run on the same pod would beat the first run's position at the
// server and hand its own retry a resume point over blocks it never covered.
func (p *ActivityProgress[T]) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latest = nil
}

// Beat sends the liveness heartbeat, carrying the latest recorded progress.
//
// Temporal keeps only the LAST heartbeat's details, so a bare liveness ping
// after a progress heartbeat would erase the resume point and silently send the
// next attempt back to the start.
//
// While the store holds NO record it therefore sends nothing at all. An empty
// store spans the window from Reset (the top of every execution) to the runner's
// LoadProgress, and a bare ping there would wipe details the PREVIOUS attempt
// wrote and this one has not read yet. The tradeoff is deliberate: an attempt
// that stalls in that window is no longer reported alive, so it trips
// HeartbeatTimeout instead — which is detection, and leaves the server-side
// resume point untouched for the attempt that follows.
func (p *ActivityProgress[T]) Beat(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.latest == nil {
		return
	}
	p.record(ctx, p.latest...)
}
