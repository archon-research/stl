package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that ReceiptTokenRepository implements outbound.ReceiptTokenRepository.
var _ outbound.ReceiptTokenRepository = (*ReceiptTokenRepository)(nil)

// ReceiptTokenRepository is a PostgreSQL implementation of the outbound.ReceiptTokenRepository port.
type ReceiptTokenRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewReceiptTokenRepository creates a new PostgreSQL ReceiptToken repository.
// Returns an error if the database pool is nil.
func NewReceiptTokenRepository(pool *pgxpool.Pool, logger *slog.Logger) (*ReceiptTokenRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &ReceiptTokenRepository{pool: pool, logger: logger}, nil
}

// GetOrCreateReceiptToken upserts a receipt token and returns its ID.
func (r *ReceiptTokenRepository) GetOrCreateReceiptToken(ctx context.Context, tx pgx.Tx, token entity.ReceiptToken) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx,
		`INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block, metadata, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, '{}', NOW())
		 ON CONFLICT (chain_id, receipt_token_address) DO UPDATE SET
		     created_at_block = LEAST(receipt_token.created_at_block, EXCLUDED.created_at_block),
		     updated_at = NOW()
		 RETURNING id`,
		token.ChainID, token.ProtocolID, token.UnderlyingTokenID, token.ReceiptTokenAddress.Bytes(), token.Symbol, token.CreatedAtBlock,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to get or create receipt token: %w", err)
	}

	r.logger.Debug("receipt token upserted",
		"chain_id", token.ChainID,
		"address", token.AddressHex(),
		"id", id,
		"symbol", token.Symbol)
	return id, nil
}
