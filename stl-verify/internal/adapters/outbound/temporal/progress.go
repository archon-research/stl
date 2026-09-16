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
// re-sends (see Beat). It keeps one copy PER ACTIVITY EXECUTION, because a worker
// serves as many executions at once as an operator starts — Temporal's duplicate
// guard is per Workflow ID — while heartbeat details belong to one of them.
type ActivityProgress[T any] struct {
	mu sync.Mutex
	// executions holds the runs in flight; each activity drops its own entry as
	// it ends (see Reset), so a worker's map does not grow with the runs it serves.
	executions map[executionKey]*executionProgress
	// record is activity.RecordHeartbeat, narrowed to a field so a test can read
	// the details a heartbeat carries. The SDK batches heartbeats before they
	// reach a test environment's listener, which is exactly where the details of
	// a suppressed beat would go unnoticed.
	record func(ctx context.Context, details ...any)
}

// executionKey identifies ONE activity execution. Every attempt of an execution
// shares it, which is what lets an attempt re-send what its predecessor recorded.
type executionKey struct {
	runID      string
	activityID string
}

// executionProgress is one execution's copy of the details it last heartbeated.
type executionProgress struct {
	latest []any
	// absent records that LoadProgress saw the server holding no details for THIS
	// attempt, which is what makes a bare liveness beat safe (see Beat).
	absent bool
}

func NewActivityProgress[T any]() *ActivityProgress[T] {
	return &ActivityProgress[T]{
		executions: make(map[executionKey]*executionProgress),
		record:     activity.RecordHeartbeat,
	}
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
	execution := p.executionFor(ctx)
	execution.latest = []any{progress}
	p.record(ctx, execution.latest...)
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
// erase the record this attempt is resuming from. An absent read arms the bare
// beat instead, because then there is nothing to erase.
func (p *ActivityProgress[T]) LoadProgress(ctx context.Context) (T, bool, error) {
	var progress T
	if !activity.HasHeartbeatDetails(ctx) {
		p.mu.Lock()
		p.executionFor(ctx).absent = true
		p.mu.Unlock()
		return progress, false, nil
	}
	if err := activity.GetHeartbeatDetails(ctx, &progress); err != nil {
		return progress, false, fmt.Errorf("decoding activity heartbeat details: %w", err)
	}
	p.mu.Lock()
	p.executionFor(ctx).latest = []any{progress}
	p.mu.Unlock()
	return progress, true, nil
}

// Reset drops what this store holds for the CALLING execution, so its Beat falls
// silent until the next LoadProgress. The activity calls it on the way in, where
// the record an earlier attempt left must not ride this attempt's beats before it
// has read the server's details, and on the way out, where the entry would
// otherwise outlive the run that owns it.
func (p *ActivityProgress[T]) Reset(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.executions, callingExecution(ctx))
}

// Beat sends the calling execution's liveness heartbeat, carrying the progress it
// last recorded.
//
// Temporal keeps only the LAST heartbeat's details, so a bare liveness ping
// after a progress heartbeat would erase the resume point and silently send the
// next attempt back to the start.
//
// With no record it beats BARE once LoadProgress has established that the server
// holds no details for this attempt: there is nothing left to erase, and an
// attempt whose first chunk outlasts HeartbeatTimeout must not be killed before
// it can record anything — no attempt would ever get further, since none of them
// has details to resume from either.
//
// It is silent only between Reset (the top of every execution) and that
// LoadProgress, where a bare ping would wipe details the PREVIOUS attempt wrote
// and this one has not read yet.
func (p *ActivityProgress[T]) Beat(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	execution, running := p.executions[callingExecution(ctx)]
	if !running {
		return
	}
	if execution.latest == nil {
		if execution.absent {
			p.record(ctx)
		}
		return
	}
	p.record(ctx, execution.latest...)
}

// executionFor returns the calling execution's entry, opening one on its first
// record. p.mu is held.
func (p *ActivityProgress[T]) executionFor(ctx context.Context) *executionProgress {
	key := callingExecution(ctx)
	execution, running := p.executions[key]
	if !running {
		execution = &executionProgress{}
		p.executions[key] = execution
	}
	return execution
}

// callingExecution identifies the activity execution a call arrives on behalf of.
// Off an activity context — a runner exercised without a worker — there is no
// execution to tell apart, so every caller shares the zero key.
func callingExecution(ctx context.Context) executionKey {
	if !activity.IsActivity(ctx) {
		return executionKey{}
	}
	info := activity.GetInfo(ctx)
	return executionKey{runID: info.WorkflowExecution.RunID, activityID: info.ActivityID}
}
