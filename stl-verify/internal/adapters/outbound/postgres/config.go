package postgres

import (
	"database/sql"
	"log/slog"
)

// RepositoryConfig holds configuration for repository batch operations.
// Batch sizes affect performance and memory usage when processing large datasets.
//
// Usage example:
//
//	// Use default configuration
//	tokenRepo := postgres.NewTokenRepository(db, logger, 0)
//
//	// Use custom batch size
//	tokenRepo := postgres.NewTokenRepository(db, logger, 1000)
//
//	// Use configuration struct for multiple repositories
//	cfg := postgres.DefaultRepositoryConfig()
//	cfg.TokenBatchSize = 750  // Customize if needed
//
//	tokenRepo := postgres.NewTokenRepository(db, logger, cfg.TokenBatchSize)
//	userRepo := postgres.NewUserRepository(db, logger, cfg.UserBatchSize)
//	positionRepo := postgres.NewPositionRepository(db, logger, cfg.PositionBatchSize)
//	protocolRepo := postgres.NewProtocolRepository(db, logger, cfg.ProtocolBatchSize)
type RepositoryConfig struct {
	// TokenBatchSize controls the number of token records processed in a single
	// database operation. Tokens have complex metadata and multiple fields,
	// so a moderate batch size helps balance memory usage and transaction overhead.
	// Default: 500
	TokenBatchSize int

	// UserBatchSize controls the number of user records processed in a single
	// database operation. Users have moderate complexity with metadata.
	// Default: 500
	UserBatchSize int

	// PositionBatchSize controls the number of position records (borrowers,
	// collateral) processed in a single database operation. Positions are
	// simpler records with fewer fields, allowing for larger batches.
	// Default: 1000
	PositionBatchSize int

	// ProtocolBatchSize controls the number of protocol and reserve data
	// records processed in a single database operation. Protocols have
	// moderate complexity similar to tokens.
	// Default: 500
	ProtocolBatchSize int
}

// DefaultRepositoryConfig returns a RepositoryConfig with sensible defaults.
// These defaults are chosen based on:
// - PostgreSQL parameter limits (default max ~32k parameters)
// - Memory usage for typical entity sizes
// - Balance between transaction overhead and memory pressure
//
// Batch size tuning guidelines:
// - Increase batch sizes if you have ample memory and want faster bulk operations
// - Decrease batch sizes if you experience memory pressure or timeouts
// - Monitor PostgreSQL logs for "too many parameters" errors (increase if needed)
// - Consider network latency: larger batches reduce round trips but increase payload size
func DefaultRepositoryConfig() RepositoryConfig {
	return RepositoryConfig{
		// 500 tokens * 7 params = 3500 parameters per batch
		// With typical metadata, each token is ~200-500 bytes
		TokenBatchSize: 500,

		// 500 users * 6 params = 3000 parameters per batch
		// Users are similar complexity to tokens
		UserBatchSize: 500,

		// 1000 positions * 8 params = 8000 parameters per batch
		// Positions are simpler, mostly numeric fields with no complex metadata
		PositionBatchSize: 1000,

		// 500 protocols * 8 params = 4000 parameters per batch
		// Protocols are similar complexity to tokens
		ProtocolBatchSize: 500,
	}
}
