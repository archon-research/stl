// Package main provides a test application for the Watcher service.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/application"
)

func main() {
	// Set up structured logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	slog.SetDefault(logger)

	// Get configuration from environment
	alchemyAPIKey := getEnv("ALCHEMY_API_KEY", "jVXUMPyy9Bp1S7b6h9nbI")
	postgresURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")

	// Set up PostgreSQL connection for block state tracking
	db, err := sql.Open("pgx", postgresURL)
	if err != nil {
		logger.Error("failed to connect to PostgreSQL", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		logger.Error("failed to ping PostgreSQL", "error", err)
		os.Exit(1)
	}

	blockStateRepo := postgres.NewBlockStateRepository(db)

	// Run migration
	if err := blockStateRepo.Migrate(context.Background()); err != nil {
		logger.Error("failed to migrate block_states table", "error", err)
		os.Exit(1)
	}
	logger.Info("PostgreSQL connected, block state tracking enabled")

	// Create Alchemy subscriber (WebSocket only)
	subscriberConfig := alchemy.SubscriberConfig{
		WebSocketURL:      fmt.Sprintf("wss://eth-mainnet.g.alchemy.com/v2/%s", alchemyAPIKey),
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        30 * time.Second,
		PingInterval:      30 * time.Second,
		PongTimeout:       10 * time.Second,
		ReadTimeout:       60 * time.Second,
		ChannelBufferSize: 100,
		HealthTimeout:     30 * time.Second,
		Logger:            logger,
	}
	subscriber, err := alchemy.NewSubscriber(subscriberConfig)
	if err != nil {
		logger.Error("failed to create subscriber", "error", err)
		os.Exit(1)
	}

	// Create Alchemy HTTP client
	httpURL := fmt.Sprintf("https://eth-mainnet.g.alchemy.com/v2/%s", alchemyAPIKey)
	client := alchemy.NewClient(httpURL)

	// Create in-memory cache and event sink for testing
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Create WatcherService
	watcherConfig := application.WatcherConfig{
		ChainID:              1, // Ethereum mainnet
		FinalityBlockCount:   64,
		MaxUnfinalizedBlocks: 128,
		BlockRetention:       1000,
		Logger:               logger,
	}

	watcher, err := application.NewWatcherService(
		watcherConfig,
		subscriber,
		client,
		blockStateRepo,
		cache,
		eventSink,
	)
	if err != nil {
		logger.Error("failed to create watcher service", "error", err)
		os.Exit(1)
	}

	// Wire up reconnect callback for backfill
	subscriber.SetOnReconnect(watcher.OnReconnect)

	// Set up context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the watcher service
	logger.Info("starting watcher service...")
	if err := watcher.Start(ctx); err != nil {
		logger.Error("failed to start watcher", "error", err)
		os.Exit(1)
	}

	logger.Info("watcher service started, waiting for blocks...")

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Info("received signal, shutting down...", "signal", sig)

	// Stop and cleanup
	if err := watcher.Stop(); err != nil {
		logger.Error("error during shutdown", "error", err)
	}

	logger.Info("shutdown complete")
}

// getEnv returns the value of an environment variable or a default value.
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
