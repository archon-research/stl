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

// Compile-time check that DebtTokenRepository implements outbound.DebtTokenRepository.
var _ outbound.DebtTokenRepository = (*DebtTokenRepository)(nil)

// DebtTokenRepository is a PostgreSQL implementation of the outbound.DebtTokenRepository port.
type DebtTokenRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewDebtTokenRepository creates a new PostgreSQL DebtToken repository.
// Returns an error if the database pool is nil.
func NewDebtTokenRepository(pool *pgxpool.Pool, logger *slog.Logger) (*DebtTokenRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &DebtTokenRepository{pool: pool, logger: logger}, nil
}

// GetOrCreateDebtToken upserts a debt token and returns its ID.
func (r *DebtTokenRepository) GetOrCreateDebtToken(ctx context.Context, tx pgx.Tx, token entity.DebtToken) (int64, error) {
	var id int64
	err := tx.QueryRow(ctx,
		`INSERT INTO debt_token (protocol_id, underlying_token_id, variable_debt_address, stable_debt_address, variable_symbol, stable_symbol, created_at_block, metadata, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, '{}', NOW())
		 ON CONFLICT (protocol_id, underlying_token_id) DO UPDATE SET
		     variable_debt_address = COALESCE(EXCLUDED.variable_debt_address, debt_token.variable_debt_address),
		     stable_debt_address = COALESCE(EXCLUDED.stable_debt_address, debt_token.stable_debt_address),
		     variable_symbol = COALESCE(NULLIF(EXCLUDED.variable_symbol, ''), debt_token.variable_symbol),
		     stable_symbol = COALESCE(NULLIF(EXCLUDED.stable_symbol, ''), debt_token.stable_symbol),
		     created_at_block = LEAST(debt_token.created_at_block, EXCLUDED.created_at_block),
		     updated_at = NOW()
		 RETURNING id`,
		token.ProtocolID, token.UnderlyingTokenID, token.VariableDebtAddress, token.StableDebtAddress, token.VariableSymbol, token.StableSymbol, token.CreatedAtBlock,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("failed to get or create debt token: %w", err)
	}

	r.logger.Debug("debt token upserted",
		"protocol_id", token.ProtocolID,
		"underlying_token_id", token.UnderlyingTokenID,
		"variable_address", token.VariableAddressHex(),
		"stable_address", token.StableAddressHex(),
		"id", id,
		"variable_symbol", token.VariableSymbol,
		"stable_symbol", token.StableSymbol)
	return id, nil
}
