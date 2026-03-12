package entity

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// BlockQuerier abstracts block number lookups from an Ethereum node.
type BlockQuerier interface {
	BlockNumber(ctx context.Context) (uint64, error)
}

// DebtQuery represents a single vault to query debt for.
type DebtQuery struct {
	Ilk          [32]byte
	VaultAddress common.Address
}

// DebtResult holds the on-chain debt data for one vault.
type DebtResult struct {
	VaultAddress common.Address
	Rate         *big.Int // cumulative stability fee rate (ray, 1e27)
	Art          *big.Int // normalized debt (wad, 1e18)
	Err          error    // per-vault error (nil on success)
}
