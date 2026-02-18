package outbound

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// AllocationPosition represents a position snapshot ready for persistence.
type AllocationPosition struct {
	ChainID        int64
	TokenAddress   common.Address
	TokenSymbol    string
	TokenDecimals  int
	Star           string
	ProxyAddress   common.Address
	AssetDecimals  int
	Balance        *big.Int
	ScaledBalance  *big.Int
	BlockNumber    int64
	BlockVersion   int
	TxHash         string
	LogIndex       int
	TxAmount       *big.Int
	Direction      string
	CreatedAtBlock int64
}

// AllocationRepository defines the interface for allocation position persistence.
type AllocationRepository interface {
	// SavePositions persists a batch of allocation position snapshots.
	// Resolves token IDs via GetOrCreateToken internally.
	// Uses upsert semantics on (chain_id, block_number, block_version, tx_hash, log_index, direction).
	SavePositions(ctx context.Context, positions []*AllocationPosition) error
}
