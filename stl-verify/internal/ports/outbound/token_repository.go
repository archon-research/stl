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

	// GetOrCreateTokens bulk-upserts multiple tokens and returns a map of address → token ID.
	GetOrCreateTokens(ctx context.Context, tx pgx.Tx, tokens []TokenInput) (map[common.Address]int64, error)

	// LookupTokenID returns the token ID for a given chain + address.
	// Returns ErrTokenNotFound if no matching token exists.
	LookupTokenID(ctx context.Context, chainID int64, address common.Address) (int64, error)

	// LookupTokenPrices returns the latest USD price for each token ID from onchain_token_price.
	// Tokens without a price entry are omitted from the result (no error).
	LookupTokenPrices(ctx context.Context, tokenIDs []int64) (map[int64]string, error)
}
