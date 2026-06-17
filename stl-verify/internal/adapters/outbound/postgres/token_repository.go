package postgres

import (
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

	// Sort by address (and dedupe) so concurrent transactions lock rows in the
	// same order, preventing deadlocks when multiple batches upsert the same
	// tokens (ADR-0002). Operates on a copy; the caller's slice is untouched.
	// Dedup is address-only: callers must resolve any per-address symbol/decimals
	// conflicts before calling (the survivor is arbitrary on collision).
	sorted := sortedByBytesKey(tokens, func(t outbound.TokenInput) []byte { return t.Address.Bytes() })
	sorted = slices.CompactFunc(sorted, func(a, b outbound.TokenInput) bool { return a.Address == b.Address })

	batch := &pgx.Batch{}
	for _, t := range sorted {
		// created_at_block merges with LEAST(): a NULL incoming block (no block
		// context) is ignored, preserving the stored block. symbol/decimals are
		// never refreshed on conflict, so RETURNING yields the stored values,
		// which the scan checks against the incoming ones.
		batch.Queue(
			`INSERT INTO token (chain_id, address, symbol, decimals, created_at_block, metadata, updated_at)
			 VALUES ($1, $2, $3, $4, $5, '{}', NOW())
			 ON CONFLICT (chain_id, address) DO UPDATE SET
			     created_at_block = LEAST(token.created_at_block, EXCLUDED.created_at_block),
			     updated_at = CASE
			         WHEN EXCLUDED.created_at_block < token.created_at_block THEN NOW()
			         ELSE token.updated_at
			     END
			 RETURNING id, symbol, decimals`,
			t.ChainID, t.Address.Bytes(), t.Symbol, t.Decimals, t.CreatedAtBlock,
		)
	}

	return collectBatchRows(ctx, tx, batch, sorted, "token",
		func(row pgx.Row, t outbound.TokenInput) (common.Address, int64, error) {
			id, err := r.scanTokenDrift(row, t)
			if err != nil {
				return common.Address{}, 0, err
			}
			return t.Address, id, nil
		})
}

// scanTokenDrift scans an upsert row and rejects token-registry drift. The
// upsert deliberately never refreshes symbol/decimals, so a stored value
// differing from the incoming one means the registry and the caller disagree.
// Decimals drift would silently mis-scale every downstream amount, so it fails
// the call loudly; symbol drift only warns, because the registry may
// legitimately hold a canonical symbol that differs from a caller's label.
func (r *TokenRepository) scanTokenDrift(row pgx.Row, t outbound.TokenInput) (int64, error) {
	var id int64
	var storedSymbol string
	var storedDecimals int
	if err := row.Scan(&id, &storedSymbol, &storedDecimals); err != nil {
		return 0, fmt.Errorf("upserting token %s: %w", t.Address.Hex(), err)
	}
	if storedDecimals != t.Decimals {
		return 0, fmt.Errorf("token %s decimals changed: stored %d, caller reported %d (token decimals are immutable; refusing the write)", t.Address.Hex(), storedDecimals, t.Decimals)
	}
	if storedSymbol != t.Symbol {
		r.logger.Warn("token symbol differs from registry; keeping the stored symbol",
			"chainID", t.ChainID,
			"address", t.Address.Hex(),
			"storedSymbol", storedSymbol,
			"callerSymbol", t.Symbol,
		)
	}
	return id, nil
}

// ListTokensMissingSymbol returns addresses of tokens with an empty symbol
// (the zero-address sentinel is excluded), ordered by created_at_block, capped
// at limit rows.
func (r *TokenRepository) ListTokensMissingSymbol(ctx context.Context, chainID int64, limit int) ([]common.Address, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("listing tokens missing symbol: limit must be positive, got %d", limit)
	}
	// COALESCE: token.symbol is nullable, and a NULL symbol is just as missing as
	// an empty one. The expression must match the idx_token_missing_symbol
	// partial-index predicate exactly for the planner to use it.
	rows, err := r.pool.Query(ctx,
		`SELECT address
		   FROM token
		  WHERE chain_id = $1
		    AND COALESCE(symbol, '') = ''
		    AND address <> $2
		  ORDER BY created_at_block
		  LIMIT $3`,
		chainID, common.Address{}.Bytes(), limit)
	if err != nil {
		return nil, fmt.Errorf("listing tokens missing symbol: %w", err)
	}
	defer rows.Close()

	var out []common.Address
	for rows.Next() {
		var addrBytes []byte
		if err := rows.Scan(&addrBytes); err != nil {
			return nil, fmt.Errorf("scanning missing-symbol token: %w", err)
		}
		out = append(out, common.BytesToAddress(addrBytes))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating missing-symbol tokens: %w", err)
	}
	return out, nil
}

// ResolveTokenSymbol sets a token's symbol. It only fills an empty symbol —
// a token that already has one is left untouched and an error is returned, so
// a resolved symbol can never be clobbered.
func (r *TokenRepository) ResolveTokenSymbol(ctx context.Context, chainID int64, address common.Address, symbol string) error {
	// An empty resolution would match the empty-symbol guard below, report
	// success, and leave the row pending — reject it outright.
	if symbol == "" {
		return fmt.Errorf("resolving token symbol %s: symbol must not be empty", address.Hex())
	}
	tag, err := r.pool.Exec(ctx,
		`UPDATE token
		    SET symbol = $3,
		        updated_at = NOW()
		  WHERE chain_id = $1 AND address = $2 AND COALESCE(symbol, '') = ''`,
		chainID, address.Bytes(), symbol)
	if err != nil {
		return fmt.Errorf("resolving token symbol: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("resolving token symbol %s: no matching empty-symbol row", address.Hex())
	}
	return nil
}

// GetOrCreateToken retrieves a token by address or creates it if it doesn't exist.
// This method participates in an external transaction.
func (r *TokenRepository) GetOrCreateToken(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, symbol string, decimals int, createdAtBlock *int64) (int64, error) {
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
