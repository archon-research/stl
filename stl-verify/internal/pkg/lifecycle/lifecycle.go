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

// Run starts the service, blocks until ctx is cancelled, then performs a
// graceful shutdown with a 25-second timeout (matching Fargate's default
// stopTimeout of 30s, leaving headroom for cleanup logging).
func Run(ctx context.Context, logger *slog.Logger, service Service) error {
	if err := service.Start(ctx); err != nil {
		return fmt.Errorf("starting service: %w", err)
	}

	<-ctx.Done()
	logger.Info("shutting down...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
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
