package sqsutil

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrDrainAbandoned is a distinct sentinel rather than context.DeadlineExceeded,
// so the expected drain expiry stays quiet without also silencing a nested
// deadline racing the same shutdown.
var ErrDrainAbandoned = errors.New("work abandoned when the shutdown drain expired")

type DrainBudget struct {
	// Work bounds the work itself, shutdown or not. Zero leaves it unbounded.
	Work time.Duration

	// Drain bounds how long work already running when ctx is cancelled may keep
	// going. Past it the caller must hand back whatever message it was holding.
	// Zero uses DefaultDrainTimeout: an unbounded value would park shutdown, and
	// a zero one would abandon work that has already succeeded.
	Drain time.Duration
}

type DrainOutcome struct {
	Err error

	// BudgetExceeded marks work that returned nil only after outrunning
	// DrainBudget.Work, i.e. work that ignored its context.
	BudgetExceeded bool

	// Abandoned marks work still running when the drain expired: an expected
	// shutdown outcome, not a failure to log.
	Abandoned bool
}

type workResult[T any] struct {
	value T
	err   error
}

// RunDrainable invokes work on a context detached from ctx's cancellation, so a
// SIGTERM mid-work does not kill work that can still finish. Work caught
// mid-flight gets DrainBudget.Drain to finish, then is cancelled and Abandoned.
func RunDrainable(ctx context.Context, budget DrainBudget, work func(context.Context) error) DrainOutcome {
	_, outcome := RunDrainableValue(ctx, budget, func(wctx context.Context) (struct{}, error) {
		return struct{}{}, work(wctx)
	})
	return outcome
}

// RunDrainableValue is RunDrainable for work that also produces a value.
// Abandoned work keeps running while its caller settles the message, so
// anything it writes afterwards would race: it yields the zero value.
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

func drainWork[T any](
	wctx context.Context,
	cancelWork context.CancelFunc,
	budget time.Duration,
	done <-chan workResult[T],
) (T, DrainOutcome) {
	var zero T
	if budget <= 0 {
		budget = DefaultDrainTimeout
	}
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
