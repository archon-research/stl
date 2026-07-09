package outbound

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// PSM3Caller reads Spark PSM3 reserve state from one per-chain deployment.
// All methods batch their RPC calls via Multicall3.
type PSM3Caller interface {
	// ResolveImmutables reads rateProvider() plus the usds/susds/usdc token
	// addresses from the PSM3 contract at the given block, fails hard if the
	// tokens do not match the configured addresses, and caches the rate
	// provider for ReadState. Number-pinned: immutables are structurally static
	// and this runs once at startup with no BlockEvent to source a hash from.
	ResolveImmutables(ctx context.Context, blockNumber *big.Int) error

	// ReadState reads the reserve state pinned to blockHash: USDS/sUSDS
	// balances at the PSM3, the USDC balance at the current pocket (resolved
	// in the same sweep — it is governance-settable), totalAssets, and the
	// sUSDS conversion rate. Pinning by hash (not number) keeps the read on the
	// exact block being processed so a reorg can't return another fork's
	// reserves (see Multicaller.ExecuteAtHash / VEC-471).
	ReadState(ctx context.Context, blockHash common.Hash) (*entity.PSM3State, error)
}

// PSM3ReservesRepository persists PSM3 reserve snapshots.
type PSM3ReservesRepository interface {
	// SaveReserves appends one snapshot row.
	SaveReserves(ctx context.Context, snap *entity.PSM3Reserves) error
}
