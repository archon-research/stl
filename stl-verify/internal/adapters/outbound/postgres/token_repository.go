package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum/common"
)

// Compile-time check that TokenRepository implements outbound.TokenRepository
var _ outbound.TokenRepository = (*TokenRepository)(nil)

// TokenRepository is a PostgreSQL implementation of the outbound.TokenRepository port.
type TokenRepository struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	batchSize int
}

// NewTokenRepository creates a new PostgreSQL Token repository.
// If batchSize is <= 0, the default batch size from DefaultRepositoryConfig() is used.
// Returns an error if the database pool is nil.
//
// Note: This function does not verify that the database connection is alive.
// Use a separate health check or call pool.Ping() if connection validation is needed.
func NewTokenRepository(pool *pgxpool.Pool, logger *slog.Logger, batchSize int) (*TokenRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = DefaultRepositoryConfig().TokenBatchSize
	}
	return &TokenRepository{
		pool:      pool,
		logger:    logger,
		batchSize: batchSize,
	}, nil
}

func (r *TokenRepository) GetOrCreateTokens(ctx context.Context, tx pgx.Tx, tokens []outbound.TokenInput) (map[common.Address]int64, error) {
	if len(tokens) == 0 {
		return make(map[common.Address]int64), nil
	}

	// Sort by address to ensure all concurrent transactions lock rows in the
	// same order, preventing deadlocks when multiple batches upsert the same tokens.
	slices.SortFunc(tokens, func(a, b outbound.TokenInput) int {
		return bytes.Compare(a.Address.Bytes(), b.Address.Bytes())
	})

	batch := &pgx.Batch{}
	for _, t := range tokens {
		batch.Queue(
			`INSERT INTO token (chain_id, address, symbol, decimals, created_at_block, metadata, updated_at)
			 VALUES ($1, $2, $3, $4, $5, '{}', NOW())
			 ON CONFLICT (chain_id, address) DO UPDATE SET
			     created_at_block = LEAST(token.created_at_block, EXCLUDED.created_at_block),
			     updated_at = CASE
			         WHEN EXCLUDED.created_at_block < token.created_at_block THEN NOW()
			         ELSE token.updated_at
			     END
			 RETURNING id`,
			t.ChainID, t.Address.Bytes(), t.Symbol, t.Decimals, t.CreatedAtBlock,
		)
	}

	br := tx.SendBatch(ctx, batch)

	result := make(map[common.Address]int64, len(tokens))
	for i, t := range tokens {
		var id int64
		if err := br.QueryRow().Scan(&id); err != nil {
			br.Close()
			return nil, fmt.Errorf("failed to get or create token %d (%s): %w", i, t.Address.Hex(), err)
		}
		result[t.Address] = id
	}

	if err := br.Close(); err != nil {
		return nil, fmt.Errorf("closing token batch: %w", err)
	}

	return result, nil
}

// GetOrCreateToken retrieves a token by address or creates it if it doesn't exist.
// This method participates in an external transaction.
func (r *TokenRepository) GetOrCreateToken(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, symbol string, decimals int, createdAtBlock int64) (int64, error) {
	var tokenID int64

	// Upsert: on conflict preserve the earliest created_at_block via LEAST().
	// This is safe for concurrent workers processing different blocks for the same token —
	// whichever worker wins the INSERT race, subsequent LEAST() merges still produce
	// the correct minimum created_at_block.
	err := tx.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals, created_at_block, metadata, updated_at)
		 VALUES ($1, $2, $3, $4, $5, '{}', NOW())
		 ON CONFLICT (chain_id, address) DO UPDATE SET
		     created_at_block = LEAST(token.created_at_block, EXCLUDED.created_at_block),
		     updated_at = CASE
		         WHEN EXCLUDED.created_at_block < token.created_at_block THEN NOW()
		         ELSE token.updated_at
		     END
		 RETURNING id`,
		chainID, address.Bytes(), symbol, decimals, createdAtBlock).Scan(&tokenID)
	if err != nil {
		return 0, fmt.Errorf("failed to get or create token: %w", err)
	}

	r.logger.Debug("token upserted", "address", address.Hex(), "id", tokenID, "symbol", symbol, "decimals", decimals)
	return tokenID, nil
}

// LookupTokenID returns the token ID for a given chain + address.
// Returns outbound.ErrTokenNotFound if no matching token exists.
func (r *TokenRepository) LookupTokenID(ctx context.Context, chainID int64, address common.Address) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx,
		`SELECT id FROM token WHERE chain_id = $1 AND address = $2`,
		chainID, address.Bytes(),
	).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, outbound.ErrTokenNotFound
		}
		return 0, fmt.Errorf("lookup token %s on chain %d: %w", address.Hex(), chainID, err)
	}
	return id, nil
}

// LookupTokenPrices returns the latest USD price for each token ID from onchain_token_price.
// Tokens without a price entry are omitted from the result.
func (r *TokenRepository) LookupTokenPrices(ctx context.Context, tokenIDs []int64) (map[int64]string, error) {
	if len(tokenIDs) == 0 {
		return nil, nil
	}

	rows, err := r.pool.Query(ctx, `
		SELECT DISTINCT ON (token_id) token_id, price_usd
		FROM onchain_token_price
		WHERE token_id = ANY($1)
		ORDER BY token_id, timestamp DESC, block_number DESC
	`, tokenIDs)
	if err != nil {
		return nil, fmt.Errorf("query token prices: %w", err)
	}
	defer rows.Close()

	prices := make(map[int64]string, len(tokenIDs))
	for rows.Next() {
		var tokenID int64
		var priceUSD string
		if err := rows.Scan(&tokenID, &priceUSD); err != nil {
			return nil, fmt.Errorf("scan token price: %w", err)
		}
		prices[tokenID] = priceUSD
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate token prices: %w", err)
	}

	return prices, nil
}
