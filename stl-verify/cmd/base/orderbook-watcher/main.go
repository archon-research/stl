// Package main provides the orderbook-watcher entry point.
// It connects to PostgreSQL and Redis, subscribes to CEX WebSocket feeds,
// and periodically flushes orderbook snapshots to the database.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cex"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	rediscache "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/redis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/orderbook_watcher"
)

// Build-time variables - can be set via ldflags, otherwise populated from Go's build info
var (
	GitCommit string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

func main() {
	// Parse command-line flags (none for now)
	flag.Parse()

	// Set up context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up structured logging with env-configurable log level
	logLevel := env.ParseLogLevel(slog.LevelInfo)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	// Log version info on startup
	logger.Info("starting orderbook-watcher",
		"commit", GitCommit,
		"buildTime", BuildTime,
	)

	// Connect to PostgreSQL
	postgresURL := env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")
	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(postgresURL))
	if err != nil {
		logger.Error("failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	logger.Info("PostgreSQL connected")

	// Create orderbook repository
	repo := postgres.NewOrderbookRepository(pool, logger)

	// Create Redis orderbook cache
	redisAddr := env.Get("REDIS_ADDR", "localhost:6379")
	cache := rediscache.NewOrderbookCache(rediscache.Config{
		Addr:     redisAddr,
		Password: env.Get("REDIS_PASSWORD", ""),
		DB:       0,
		TTL:      5 * time.Minute,
	}, logger)
	defer func() {
		if err := cache.Close(); err != nil {
			logger.Warn("failed to close Redis connection", "error", err)
		}
	}()
	logger.Info("Redis cache connected", "addr", redisAddr)

	// Create CEX WebSocket collector from config
	collector := cex.BuildWSCollectorFromConfig(logger)

	// Parse DB_FLUSH_INTERVAL env var (default "60s")
	flushIntervalStr := env.Get("DB_FLUSH_INTERVAL", "60s")
	flushInterval, err := time.ParseDuration(flushIntervalStr)
	if err != nil {
		logger.Error("invalid DB_FLUSH_INTERVAL", "value", flushIntervalStr, "error", err)
		os.Exit(1)
	}

	// Create orderbook watcher service
	config := orderbook_watcher.Config{
		FlushInterval: flushInterval,
		Logger:        logger,
	}
	service, err := orderbook_watcher.NewService(config, collector, repo, cache, logger)
	if err != nil {
		logger.Error("failed to create orderbook watcher service", "error", err)
		os.Exit(1)
	}

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the service
	logger.Info("starting orderbook watcher service...")
	if err := service.Start(ctx); err != nil {
		logger.Error("failed to start orderbook watcher service", "error", err)
		os.Exit(1)
	}

	logger.Info("service started, waiting for orderbook snapshots...",
		"flushInterval", flushInterval,
	)

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Info("received signal, shutting down...", "signal", sig)

	// Cancel context first to signal all goroutines to stop
	cancel()

	// Create shutdown timeout context
	// Fargate default stopTimeout is 30s; we use 25s to ensure clean logging before force-kill
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer shutdownCancel()

	// Stop service with timeout
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		if err := service.Stop(); err != nil {
			logger.Error("error stopping orderbook watcher service", "error", err)
		}
	}()

	select {
	case <-shutdownDone:
		logger.Info("shutdown complete")
	case <-shutdownCtx.Done():
		logger.Error("shutdown timed out, forcing exit")
		os.Exit(1)
	}
}
