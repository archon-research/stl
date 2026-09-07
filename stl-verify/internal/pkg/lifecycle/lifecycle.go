package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"
)

// ErrShutdownTimedOut reports that the services did not stop within
// ShutdownTimeout. The caller's deferred cleanup has not run yet when this is
// returned, and it may block on the very goroutines that missed the deadline
// (pgxpool.Close waits for every acquired connection), so a binary that needs a
// bounded exit arms ForceExitAfter on this error.
var ErrShutdownTimedOut = errors.New("shutdown timed out")

// Service is implemented by any long-running service that supports
// graceful startup and shutdown.
type Service interface {
	Start(ctx context.Context) error
	Stop() error
}

// Run starts each service in order, blocks until ctx is cancelled, then stops
// them in reverse order within ShutdownTimeout.
func Run(ctx context.Context, logger *slog.Logger, services ...Service) error {
	return run(ctx, logger, ShutdownTimeout, nil, services)
}

// RunWithTimeoutGuard is Run with onShutdownTimeout called on a shutdown
// timeout, before the error reaches the caller and so before the caller's
// deferred cleanup starts unwinding — the only point at which that cleanup can
// still be bounded, since it waits on the goroutines that just missed the
// deadline. main passes ForceExitAfter; an in-process caller such as a test
// passes nil, because ForceExitAfter would take the test binary down with it.
func RunWithTimeoutGuard(ctx context.Context, logger *slog.Logger, onShutdownTimeout func(), services ...Service) error {
	return run(ctx, logger, ShutdownTimeout, onShutdownTimeout, services)
}

// run carries the timeout as a parameter so the tests do not have to wait
// ShutdownTimeout to exercise the deadline branch.
func run(ctx context.Context, logger *slog.Logger, timeout time.Duration, onShutdownTimeout func(), services []Service) error {
	if err := start(ctx, services, logger); err != nil {
		return err
	}

	<-ctx.Done()
	logger.Info("shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), timeout)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		stopAll(services, logger)
	}()

	select {
	case <-done:
		logger.Info("shutdown complete")
		return nil
	case <-shutdownCtx.Done():
		if onShutdownTimeout != nil {
			onShutdownTimeout()
		}
		return ErrShutdownTimedOut
	}
}

// start unwinds the services it already started when a later one fails: the
// caller is about to close the pool, cache and event sink those services are
// still reading and writing through.
func start(ctx context.Context, services []Service, logger *slog.Logger) error {
	for i, service := range services {
		if err := service.Start(ctx); err != nil {
			stopAll(services[:i], logger)
			return fmt.Errorf("starting %T: %w", service, err)
		}
	}
	return nil
}

// stopAll stops in reverse start order so a service is never left running
// against a dependency that has already gone away.
func stopAll(services []Service, logger *slog.Logger) {
	for _, service := range slices.Backward(services) {
		if err := service.Stop(); err != nil {
			logger.Error("error stopping service", "service", fmt.Sprintf("%T", service), "error", err)
		}
	}
}

// ForceExitAfter returns a callback that kills the process if cleanup has not
// finished within d, logging first. Stranding the pending defers is the point:
// the alternative is hanging silently until the pod's grace period expires and
// SIGKILL arrives with nothing in the logs. Only main may install it — an
// in-process caller such as a test would be killed along with the binary.
//
// It logs through slog.Default so a binary that swaps the default logger during
// startup still gets its own handler here.
func ForceExitAfter(d time.Duration) func() {
	return func() {
		time.AfterFunc(d, func() {
			slog.Default().Error("cleanup did not finish, forcing exit", "timeout", d)
			os.Exit(1)
		})
	}
}

// SignalContext returns a context cancelled on SIGINT or SIGTERM, plus a stop
// function that releases the handler. signal.NotifyContext drops which signal
// arrived, and "SIGTERM from the kubelet" versus "SIGINT from an operator" is
// the first thing worth knowing when a pod restarts unexpectedly.
func SignalContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case sig := <-signals:
			slog.Default().Info("received signal, shutting down", "signal", sig.String())
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, func() {
		signal.Stop(signals)
		cancel()
	}
}
