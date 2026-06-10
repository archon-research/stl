package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
)

// TokenInput represents a token to upsert in a batch operation.
type TokenInput struct {
	ChainID        int64
	Address        common.Address
	Symbol         string
	Decimals       int
	CreatedAtBlock int64
}

// TokenRepository defines the interface for token-related data persistence.
// This aggregate includes base tokens and their derivatives (receipt/debt tokens).
type TokenRepository interface {
	// GetOrCreateToken retrieves a token by address or creates it if it doesn't exist.
	// This method participates in an external transaction.
	GetOrCreateToken(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, symbol string, decimals int, createdAtBlock int64) (int64, error)

	// GetOrCreateTokens bulk-upserts multiple tokens and returns a map of address to token ID.
	GetOrCreateTokens(ctx context.Context, tx pgx.Tx, tokens []TokenInput) (map[common.Address]int64, error)

	// ListTokensMissingSymbol returns addresses of tokens on the chain whose symbol
	// is still empty (the zero-address sentinel is excluded), capped at limit rows.
	// limit must be positive.
	ListTokensMissingSymbol(ctx context.Context, chainID int64, limit int) ([]common.Address, error)

	// ResolveTokenSymbol sets a token's symbol. It only fills an empty symbol —
	// a token that already has one is left untouched and an error is returned, so
	// a resolved symbol can never be clobbered.
	ResolveTokenSymbol(ctx context.Context, chainID int64, address common.Address, symbol string) error
}
