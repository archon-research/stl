package lifecycle

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"
)

// fakeService records the order Start and Stop are called in, through a shared
// slice the test owns.
type fakeService struct {
	name     string
	calls    *[]string
	startErr error
	stopErr  error
	blockOn  chan struct{}
}

func (f *fakeService) Start(context.Context) error {
	*f.calls = append(*f.calls, "start:"+f.name)
	return f.startErr
}

func (f *fakeService) Stop() error {
	if f.blockOn != nil {
		<-f.blockOn
	}
	*f.calls = append(*f.calls, "stop:"+f.name)
	return f.stopErr
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func assertCalls(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("call order = %v, want %v", got, want)
	}
}

func TestRun_StopsServicesInReverseStartOrder(t *testing.T) {
	var calls []string
	first := &fakeService{name: "first", calls: &calls}
	second := &fakeService{name: "second", calls: &calls}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := Run(ctx, discardLogger(), first, second); err != nil {
		t.Fatalf("Run returned an error: %v", err)
	}

	want := []string{"start:first", "start:second", "stop:second", "stop:first"}
	assertCalls(t, calls, want)
}

func TestRun_ReturnsStartErrorNamingTheService(t *testing.T) {
	var calls []string
	boom := errors.New("boom")
	failing := &fakeService{name: "failing", calls: &calls, startErr: boom}

	err := Run(context.Background(), discardLogger(), failing)

	if !errors.Is(err, boom) {
		t.Fatalf("error = %v, want it to wrap %v", err, boom)
	}
	if got := err.Error(); got != "starting *lifecycle.fakeService: boom" {
		t.Fatalf("error = %q, want the service type named", got)
	}
}

func TestRun_StopsAlreadyStartedServicesWhenAStartFails(t *testing.T) {
	var calls []string
	boom := errors.New("boom")
	started := &fakeService{name: "started", calls: &calls}
	failing := &fakeService{name: "failing", calls: &calls, startErr: boom}

	if err := Run(context.Background(), discardLogger(), started, failing); !errors.Is(err, boom) {
		t.Fatalf("error = %v, want it to wrap %v", err, boom)
	}

	want := []string{"start:started", "start:failing", "stop:started"}
	assertCalls(t, calls, want)
}

func TestRun_ReturnsErrShutdownTimedOutWhenStopHangs(t *testing.T) {
	var calls []string
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	hanging := &fakeService{name: "hanging", calls: &calls, blockOn: release}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := run(ctx, discardLogger(), time.Millisecond, nil, []Service{hanging})

	if !errors.Is(err, ErrShutdownTimedOut) {
		t.Fatalf("error = %v, want %v", err, ErrShutdownTimedOut)
	}
}

func TestRun_ReturnsNilWhenAServiceStopFails(t *testing.T) {
	var calls []string
	noisy := &fakeService{name: "noisy", calls: &calls, stopErr: errors.New("close failed")}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// A Stop failure is logged, not returned: the process is already leaving and
	// there is nothing left for the caller to do about it.
	if err := Run(ctx, discardLogger(), noisy); err != nil {
		t.Fatalf("Run returned an error: %v", err)
	}
}

func TestSignalContext_StopCancelsTheContext(t *testing.T) {
	ctx, stop := SignalContext(context.Background())

	stop()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("context was not cancelled by stop")
	}
}

func TestSignalContext_ParentCancellationPropagates(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, stop := SignalContext(parent)
	t.Cleanup(stop)

	cancelParent()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("context was not cancelled by its parent")
	}
}

func TestRunWithTimeoutGuard_ArmsTheGuardWhenStopHangs(t *testing.T) {
	var calls []string
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	hanging := &fakeService{name: "hanging", calls: &calls, blockOn: release}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	armed := make(chan struct{})
	err := run(ctx, discardLogger(), time.Millisecond, func() { close(armed) }, []Service{hanging})

	if !errors.Is(err, ErrShutdownTimedOut) {
		t.Fatalf("error = %v, want %v", err, ErrShutdownTimedOut)
	}
	select {
	case <-armed:
	default:
		t.Fatal("the guard was not armed on a shutdown timeout")
	}
}

func TestRunWithTimeoutGuard_LeavesTheGuardUnarmedOnCleanShutdown(t *testing.T) {
	var calls []string
	quick := &fakeService{name: "quick", calls: &calls}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	armed := false
	if err := RunWithTimeoutGuard(ctx, discardLogger(), func() { armed = true }, quick); err != nil {
		t.Fatalf("RunWithTimeoutGuard returned an error: %v", err)
	}
	if armed {
		t.Fatal("the guard was armed on a clean shutdown")
	}
}
