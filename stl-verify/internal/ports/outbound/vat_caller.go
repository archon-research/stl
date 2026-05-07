package outbound

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// VatCaller reads debt data from the MakerDAO/Sky Vat contract.
// All methods batch their RPC calls via Multicall3.
type VatCaller interface {
	// ResolveIlks reads the ilk identifier from each vault contract in a single
	// multicall at the given block. Returns a map of vault address → ilk bytes32.
	ResolveIlks(ctx context.Context, vaults []common.Address, blockNumber *big.Int) (map[common.Address][32]byte, error)

	// ReadDebts reads rate and art for each query in a single multicall at the
	// given block. Individual vault failures are reported via DebtResult.Err
	// rather than failing the entire batch.
	ReadDebts(ctx context.Context, queries []entity.DebtQuery, blockNumber *big.Int) ([]entity.DebtResult, error)
}
