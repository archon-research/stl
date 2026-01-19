// Package postgres provides PostgreSQL implementations of repository interfaces.
package postgres

// RepositoryConfig holds configuration for repository batch operations.
// Batch sizes affect performance and memory usage when processing large datasets.
type RepositoryConfig struct {
	// TokenBatchSize controls the number of token records processed in a single
	// database operation. Tokens have complex metadata and multiple fields,
	// so a moderate batch size helps balance memory usage and transaction overhead.
	TokenBatchSize int

	// UserBatchSize controls the number of user records processed in a single
	// database operation. Users have moderate complexity with metadata.
	UserBatchSize int

	// PositionBatchSize controls the number of position records (borrowers,
	// collateral) processed in a single database operation. Positions are
	// simpler records with fewer fields, allowing for larger batches.
	PositionBatchSize int

	// ProtocolBatchSize controls the number of protocol and reserve data
	// records processed in a single database operation. Protocols have
	// moderate complexity similar to tokens.
	ProtocolBatchSize int
}

// DefaultRepositoryConfig returns a RepositoryConfig with sensible defaults.
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
