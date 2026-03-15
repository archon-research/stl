package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const listTrackedReceiptTokensQuery = `
	SELECT
		rt.id,
		rt.protocol_id,
		p.address,
		rt.underlying_token_id,
		rt.receipt_token_address,
		COALESCE(rt.symbol, ''),
		p.chain_id
	FROM receipt_token rt
	JOIN protocol p ON p.id = rt.protocol_id
	WHERE p.chain_id = $1
	ORDER BY rt.id
`

// Compile-time check that ReceiptTokenRepository implements outbound.ReceiptTokenRepository.
var _ outbound.ReceiptTokenRepository = (*ReceiptTokenRepository)(nil)

// ReceiptTokenRepository is a PostgreSQL implementation of the outbound.ReceiptTokenRepository port.
type ReceiptTokenRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewReceiptTokenRepository creates a new PostgreSQL receipt token repository.
func NewReceiptTokenRepository(pool *pgxpool.Pool, logger *slog.Logger, _ int) (*ReceiptTokenRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &ReceiptTokenRepository{
		pool:   pool,
		logger: logger,
	}, nil
}

// ListTrackedReceiptTokens returns all tracked receipt tokens for a chain.
func (r *ReceiptTokenRepository) ListTrackedReceiptTokens(ctx context.Context, chainID int64) ([]outbound.TrackedReceiptToken, error) {
	rows, err := r.pool.Query(ctx, listTrackedReceiptTokensQuery, chainID)
	if err != nil {
		return nil, fmt.Errorf("list tracked receipt tokens for chain %d: %w", chainID, err)
	}
	defer rows.Close()

	tokens := make([]outbound.TrackedReceiptToken, 0)
	for rows.Next() {
		token, err := scanTrackedReceiptToken(rows)
		if err != nil {
			return nil, fmt.Errorf("scan tracked receipt token for chain %d: %w", chainID, err)
		}

		tokens = append(tokens, token)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tracked receipt tokens for chain %d: %w", chainID, err)
	}

	r.logger.Debug("listed tracked receipt tokens", "chain_id", chainID, "count", len(tokens))
	return tokens, nil
}

func scanTrackedReceiptToken(rows pgx.Rows) (outbound.TrackedReceiptToken, error) {
	var token outbound.TrackedReceiptToken
	var protocolAddress []byte
	var receiptTokenAddress []byte

	err := rows.Scan(
		&token.ID,
		&token.ProtocolID,
		&protocolAddress,
		&token.UnderlyingTokenID,
		&receiptTokenAddress,
		&token.Symbol,
		&token.ChainID,
	)
	if err != nil {
		return outbound.TrackedReceiptToken{}, err
	}

	token.ProtocolAddress = common.BytesToAddress(protocolAddress)
	token.ReceiptTokenAddress = common.BytesToAddress(receiptTokenAddress)

	return token, nil
}
