package outbound

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

// VatCaller is the port for reading debt data from the MakerDAO/Sky Vat contract.
// All methods batch their RPC calls via Multicall3.
type VatCaller interface {
	// ResolveIlks reads the ilk identifier from each vault contract in a single
	// multicall at the given block. Returns a map of vault address → ilk bytes32.
	ResolveIlks(ctx context.Context, vaults []common.Address, blockNumber *big.Int) (map[common.Address][32]byte, error)

	// ReadDebts reads rate and art for each query in a single multicall at the
	// given block. Individual vault failures are reported via DebtResult.Err
	// rather than failing the entire batch.
	ReadDebts(ctx context.Context, queries []DebtQuery, blockNumber *big.Int) ([]DebtResult, error)
}
