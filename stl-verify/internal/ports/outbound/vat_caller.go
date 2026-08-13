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
	// Number-pinned: a vault's ilk is a structurally static identifier and this
	// runs once at startup with no BlockEvent to source a hash from.
	ResolveIlks(ctx context.Context, vaults []common.Address, blockNumber *big.Int) (map[common.Address][32]byte, error)

	// ReadDebts reads rate and art (versioned per-block debt state) for each
	// query in a single multicall pinned to blockHash. Pinning by hash (not
	// number) keeps the read on the exact block being processed so a reorg
	// can't return another fork's debt (see Multicaller.ExecuteAtHash / VEC-471).
	// Individual vault failures are reported via DebtResult.Err rather than
	// failing the entire batch.
	ReadDebts(ctx context.Context, queries []entity.DebtQuery, blockHash common.Hash) ([]entity.DebtResult, error)
}
