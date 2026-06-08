package postgres

import (
	"bytes"
	"context"
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

// MarkTokenSymbolPending flags a token for later symbol reconciliation. It is a
// no-op when the token already has a non-empty symbol, so a redrive or a later
// sighting cannot clobber a resolved symbol.
func (r *TokenRepository) MarkTokenSymbolPending(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, anchorBlock int64) error {
	_, err := tx.Exec(ctx,
		`UPDATE token
		    SET metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('symbol_pending', true, 'symbol_anchor_block', $3::bigint)
		  WHERE chain_id = $1 AND address = $2 AND COALESCE(symbol, '') = ''`,
		chainID, address.Bytes(), anchorBlock)
	if err != nil {
		return fmt.Errorf("marking token symbol pending: %w", err)
	}
	return nil
}

// ListTokensPendingSymbol returns tokens flagged for symbol reconciliation.
func (r *TokenRepository) ListTokensPendingSymbol(ctx context.Context, chainID int64, limit int) ([]outbound.PendingTokenSymbol, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := r.pool.Query(ctx,
		`SELECT address, COALESCE((metadata->>'symbol_anchor_block')::bigint, 0)
		   FROM token
		  WHERE chain_id = $1
		    AND (metadata->>'symbol_pending') = 'true'
		  ORDER BY created_at_block
		  LIMIT $2`,
		chainID, limit)
	if err != nil {
		return nil, fmt.Errorf("listing tokens pending symbol: %w", err)
	}
	defer rows.Close()

	var out []outbound.PendingTokenSymbol
	for rows.Next() {
		var addrBytes []byte
		var anchor int64
		if err := rows.Scan(&addrBytes, &anchor); err != nil {
			return nil, fmt.Errorf("scanning pending token: %w", err)
		}
		out = append(out, outbound.PendingTokenSymbol{
			Address:     common.BytesToAddress(addrBytes),
			AnchorBlock: anchor,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating pending tokens: %w", err)
	}
	return out, nil
}

// ResolveTokenSymbol sets a resolved symbol and clears the pending flag.
func (r *TokenRepository) ResolveTokenSymbol(ctx context.Context, chainID int64, address common.Address, symbol string) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE token
		    SET symbol = $3,
		        metadata = (COALESCE(metadata, '{}'::jsonb) - 'symbol_pending') - 'symbol_anchor_block',
		        updated_at = NOW()
		  WHERE chain_id = $1 AND address = $2`,
		chainID, address.Bytes(), symbol)
	if err != nil {
		return fmt.Errorf("resolving token symbol: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("resolving token symbol %s: no matching token row", address.Hex())
	}
	return nil
}

// MarkTokenSymbolUnresolved clears the pending flag and records that the symbol
// could not be resolved within the backstop, leaving the empty symbol in place.
func (r *TokenRepository) MarkTokenSymbolUnresolved(ctx context.Context, chainID int64, address common.Address) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE token
		    SET metadata = (COALESCE(metadata, '{}'::jsonb) - 'symbol_pending' - 'symbol_anchor_block') || jsonb_build_object('symbol_unresolved', true),
		        updated_at = NOW()
		  WHERE chain_id = $1 AND address = $2`,
		chainID, address.Bytes())
	if err != nil {
		return fmt.Errorf("marking token symbol unresolved: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("marking token symbol unresolved %s: no matching token row", address.Hex())
	}
	return nil
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
