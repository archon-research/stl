package lifecycle

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Service is implemented by any long-running service that supports
// graceful startup and shutdown.
type Service interface {
	Start(ctx context.Context) error
	Stop() error
}

// ShutdownTimeout bounds the graceful shutdown after ctx is cancelled. It is
// derived from the longest SQS-worker shutdown path, which must complete inside
// it: an in-flight long poll finishing (SQS wait 20s + 5s slack) plus releasing
// the batch it returns (one 5s budget for the whole batch) plus Stop()
// overhead — 30s < 45s, leaving margin under PodTerminationGracePeriod.
const ShutdownTimeout = 45 * time.Second

// PodTerminationGracePeriod mirrors the terminationGracePeriodSeconds every
// worker Deployment declares, and is the hard ceiling every shutdown budget
// must fit: past it the kubelet SIGKILLs, so a worker that has not settled its
// in-flight SQS message strands it for the queue's visibility timeout. Raising
// ShutdownTimeout above this requires raising the manifests first (and, for
// anything still on ECS Fargate, that task definition's stopTimeout).
const PodTerminationGracePeriod = 60 * time.Second

// Run starts the service, blocks until ctx is cancelled, then performs a
// graceful shutdown bounded by ShutdownTimeout.
func Run(ctx context.Context, logger *slog.Logger, service Service) error {
	if err := service.Start(ctx); err != nil {
		return fmt.Errorf("starting service: %w", err)
	}

	<-ctx.Done()
	logger.Info("shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := service.Stop(); err != nil {
			logger.Error("error stopping service", "error", err)
		}
	}()

	select {
	case <-done:
		logger.Info("shutdown complete")
	case <-shutdownCtx.Done():
		return fmt.Errorf("shutdown timed out")
	}

	return nil
}
