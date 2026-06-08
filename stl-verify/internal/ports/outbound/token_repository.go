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

// PendingTokenSymbol is a token awaiting symbol reconciliation.
type PendingTokenSymbol struct {
	Address     common.Address
	AnchorBlock int64
}

// TokenRepository defines the interface for token-related data persistence.
// This aggregate includes base tokens and their derivatives (receipt/debt tokens).
type TokenRepository interface {
	// GetOrCreateToken retrieves a token by address or creates it if it doesn't exist.
	// This method participates in an external transaction.
	GetOrCreateToken(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, symbol string, decimals int, createdAtBlock int64) (int64, error)

	// GetOrCreateTokens bulk-upserts multiple tokens and returns a map of address to token ID.
	GetOrCreateTokens(ctx context.Context, tx pgx.Tx, tokens []TokenInput) (map[common.Address]int64, error)

	// MarkTokenSymbolPending flags a token (within the caller's tx) as needing
	// later symbol reconciliation, recording the anchor block. It is a no-op if
	// the token already has a non-empty symbol, so it never clobbers a resolved one.
	MarkTokenSymbolPending(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, anchorBlock int64) error

	// ListTokensPendingSymbol returns tokens flagged for symbol reconciliation.
	ListTokensPendingSymbol(ctx context.Context, chainID int64, limit int) ([]PendingTokenSymbol, error)

	// ResolveTokenSymbol sets a resolved symbol and clears the pending flag.
	ResolveTokenSymbol(ctx context.Context, chainID int64, address common.Address, symbol string) error

	// MarkTokenSymbolUnresolved clears the pending flag and records that the
	// symbol could not be resolved within the backstop, leaving symbol empty.
	MarkTokenSymbolUnresolved(ctx context.Context, chainID int64, address common.Address) error
}
