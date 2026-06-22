package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
)

// TokenInput represents a token to upsert in a batch operation.
//
// CreatedAtBlock is a pointer so "no block context" (nil) is distinct from
// "genesis block" (0). A nil block inserts NULL and the LEAST() merge preserves
// any existing stored block; passing 0 would clobber it.
type TokenInput struct {
	ChainID        int64
	Address        common.Address
	Symbol         string
	Decimals       int
	CreatedAtBlock *int64
}

// TokenRepository defines the interface for token-related data persistence.
// This aggregate includes base tokens and their derivatives (receipt/debt tokens).
type TokenRepository interface {
	// GetOrCreateToken retrieves a token by address or creates it if it doesn't exist.
	// This method participates in an external transaction. createdAtBlock is nil
	// when the caller has no block context (NULL is preserved by the LEAST() merge;
	// a 0 would clobber an existing block).
	//
	// Unlike GetOrCreateTokens, this single-row path does not run the decimals
	// drift guard: its callers (on-chain indexers) read decimals fresh from the
	// chain per event, where a stored-vs-incoming mismatch is not a meaningful
	// signal. The batch path guards drift because its multi-source callers
	// (e.g. the Maple GraphQL indexer) reconcile against an external API.
	GetOrCreateToken(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, symbol string, decimals int, createdAtBlock *int64) (int64, error)

	// GetOrCreateTokens bulk-upserts multiple tokens and returns a map of address to token ID.
	// Decimals are immutable per token: a stored value differing from the incoming
	// one fails the call. A differing symbol only warns (the registry may hold a
	// canonical symbol that differs from a caller's label).
	GetOrCreateTokens(ctx context.Context, tx pgx.Tx, tokens []TokenInput) (map[common.Address]int64, error)

	// ListTokensMissingSymbol returns addresses of tokens on the chain whose symbol
	// is still missing — empty or NULL (the zero-address sentinel is excluded) —
	// capped at limit rows. limit must be positive.
	ListTokensMissingSymbol(ctx context.Context, chainID int64, limit int) ([]common.Address, error)

	// ResolveTokenSymbol sets a token's symbol. It only fills a missing (empty or
	// NULL) symbol — a token that already has one is left untouched and an error
	// is returned, so a resolved symbol can never be clobbered. The new symbol
	// must be non-empty.
	ResolveTokenSymbol(ctx context.Context, chainID int64, address common.Address, symbol string) error
}
