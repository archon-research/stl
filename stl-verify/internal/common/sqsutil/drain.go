package sqsutil

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrDrainAbandoned marks work the shutdown drain gave up on. It is a distinct
// sentinel, not context.DeadlineExceeded, so the expected drain expiry stays
// quiet without also silencing a nested deadline racing the same shutdown.
var ErrDrainAbandoned = errors.New("work abandoned when the shutdown drain expired")

// DrainBudget bounds one unit of work and the grace a shutdown gives it.
type DrainBudget struct {
	// Work bounds the work itself, shutdown or not, so a stuck dependency (e.g.
	// a Postgres lock wait) turns into a bounded failure instead of a parked
	// message loop. Zero leaves the work unbounded.
	Work time.Duration

	// Drain bounds how long work already running when ctx is cancelled may keep
	// going. Past it the work context is cancelled and the caller must hand back
	// whatever queue message the work was holding.
	Drain time.Duration
}

// DrainOutcome reports how one drained unit of work ended.
type DrainOutcome struct {
	Err error

	// BudgetExceeded marks work that returned nil only after running past
	// DrainBudget.Work, i.e. work that ignored its context.
	BudgetExceeded bool

	// Abandoned marks work still running when the drain expired: an expected
	// shutdown outcome, not a failure to log.
	Abandoned bool
}

// workResult carries a finished unit of work back over the channel, so an
// abandoned worker cannot race its caller through a shared variable.
type workResult[T any] struct {
	value T
	err   error
}

// RunDrainable invokes work on a context detached from ctx's cancellation, so a
// SIGTERM mid-work does not kill work that can still finish and whose message
// could still be deleted. Work caught mid-flight by the cancellation gets
// DrainBudget.Drain to finish; past that its context is cancelled and the
// outcome is Abandoned, which tells the caller to release the message rather
// than hold it for the queue's visibility timeout.
func RunDrainable(ctx context.Context, budget DrainBudget, work func(context.Context) error) DrainOutcome {
	_, outcome := RunDrainableValue(ctx, budget, func(wctx context.Context) (struct{}, error) {
		return struct{}{}, work(wctx)
	})
	return outcome
}

// RunDrainableValue is RunDrainable for work that also produces a value. The
// value travels back with the outcome instead of through a variable the caller
// shares with the work: abandoned work keeps running while its caller settles
// the message, so anything it writes afterwards would be a data race. Abandoned
// work therefore yields the zero value.
func RunDrainableValue[T any](ctx context.Context, budget DrainBudget, work func(context.Context) (T, error)) (T, DrainOutcome) {
	wctx, cancel := workContext(ctx, budget.Work)
	defer cancel()

	done := make(chan workResult[T], 1)
	go func() {
		value, err := work(wctx)
		done <- workResult[T]{value: value, err: err}
	}()

	select {
	case got := <-done:
		return got.value, DrainOutcome{Err: got.err, BudgetExceeded: exceededBudget(wctx)}
	case <-ctx.Done():
		return drainWork(wctx, cancel, budget.Drain, done)
	}
}

func workContext(ctx context.Context, budget time.Duration) (context.Context, context.CancelFunc) {
	detached := context.WithoutCancel(ctx)
	if budget <= 0 {
		return context.WithCancel(detached)
	}
	return context.WithTimeout(detached, budget)
}

// drainWork gives work caught mid-flight by shutdown the drain budget to
// finish, then cancels it and leaves it to unwind.
func drainWork[T any](
	wctx context.Context,
	cancelWork context.CancelFunc,
	budget time.Duration,
	done <-chan workResult[T],
) (T, DrainOutcome) {
	var zero T
	drain := time.NewTimer(budget)
	defer drain.Stop()

	select {
	case got := <-done:
		return got.value, DrainOutcome{Err: got.err, BudgetExceeded: exceededBudget(wctx)}
	case <-drain.C:
		cancelWork()
		return zero, DrainOutcome{
			Err:       fmt.Errorf("%w: still running after %s", ErrDrainAbandoned, budget),
			Abandoned: true,
		}
	}
}

func exceededBudget(wctx context.Context) bool {
	return errors.Is(wctx.Err(), context.DeadlineExceeded)
}
