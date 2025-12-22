// Package main provides a test application for the Alchemy WebSocket subscriber.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
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
	var blockStateRepo *postgres.BlockStateRepository
	db, err := sql.Open("pgx", postgresURL)
	if err != nil {
		logger.Warn("failed to connect to PostgreSQL, running without state tracking", "error", err)
	} else {
		defer db.Close()

		// Test connection
		if err := db.Ping(); err != nil {
			logger.Warn("failed to ping PostgreSQL, running without state tracking", "error", err)
		} else {
			blockStateRepo = postgres.NewBlockStateRepository(db)

			// Run migration
			if err := blockStateRepo.Migrate(context.Background()); err != nil {
				logger.Error("failed to migrate block_states table", "error", err)
				os.Exit(1)
			}
			logger.Info("PostgreSQL connected, block state tracking enabled")
		}
	}

	// Configure the Alchemy subscriber
	config := alchemy.Config{
		WebSocketURL:         fmt.Sprintf("wss://eth-mainnet.g.alchemy.com/v2/%s", alchemyAPIKey),
		HTTPURL:              fmt.Sprintf("https://eth-mainnet.g.alchemy.com/v2/%s", alchemyAPIKey),
		InitialBackoff:       1 * time.Second,
		MaxBackoff:           30 * time.Second,
		PingInterval:         30 * time.Second,
		PongTimeout:          10 * time.Second,
		ReadTimeout:          60 * time.Second,
		ChannelBufferSize:    100,
		BlockRetention:       1000,
		HealthTimeout:        30 * time.Second,
		FinalityBlockCount:   64,  // Ethereum mainnet finality (~13 minutes)
		MaxUnfinalizedBlocks: 128, // Keep 128 blocks in memory for reorg detection
		BlockStateRepo:       blockStateRepo,
		Logger:               logger,
	}

	subscriber := alchemy.NewSubscriber(config)

	// Set up context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Subscribe to new block headers
	logger.Info("subscribing to new block headers...")
	headers, err := subscriber.Subscribe(ctx)
	if err != nil {
		logger.Error("failed to subscribe", "error", err)
		os.Exit(1)
	}

	logger.Info("subscription started, waiting for new blocks...")

	// Process block headers
	go func() {
		blockCount := 0
		for header := range headers {
			blockCount++
			blockNum, _ := strconv.ParseInt(header.Number, 0, 64)

			logAttrs := []any{
				"count", blockCount,
				"number", blockNum,
				"hash", truncateHash(header.Hash),
				"parentHash", truncateHash(header.ParentHash),
				"miner", header.Miner,
				"gasUsed", header.GasUsed,
				"gasLimit", header.GasLimit,
				"timestamp", header.Timestamp,
			}

			// Add reorg and backfill indicators
			if header.IsReorg {
				logAttrs = append(logAttrs, "isReorg", true)
			}
			if header.IsBackfill {
				logAttrs = append(logAttrs, "isBackfill", true)
			}

			logger.Info("new block received", logAttrs...)
		}
		logger.Info("header channel closed")
	}()

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Info("received signal, shutting down...", "signal", sig)

	// Unsubscribe and cleanup
	if err := subscriber.Unsubscribe(); err != nil {
		logger.Error("error during unsubscribe", "error", err)
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

// truncateHash shortens a hash for display purposes.
func truncateHash(hash string) string {
	if len(hash) <= 14 {
		return hash
	}
	return fmt.Sprintf("%s...%s", hash[:8], hash[len(hash)-6:])
}
