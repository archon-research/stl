package outbound

import (
	"context"
	"math/big"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// PSM3Caller reads Spark PSM3 reserve state from one per-chain deployment.
// All methods batch their RPC calls via Multicall3.
type PSM3Caller interface {
	// ResolveImmutables reads rateProvider() plus the usds/susds/usdc token
	// addresses from the PSM3 contract at the given block, fails hard if the
	// tokens do not match the configured addresses, and caches the rate
	// provider for ReadState.
	ResolveImmutables(ctx context.Context, blockNumber *big.Int) error

	// ReadState reads the reserve state pinned to blockNumber: USDS/sUSDS
	// balances at the PSM3, the USDC balance at the current pocket (resolved
	// in the same sweep — it is governance-settable), totalAssets, and the
	// sUSDS conversion rate.
	ReadState(ctx context.Context, blockNumber *big.Int) (*entity.PSM3State, error)
}

// PSM3SnapshotRepository persists PSM3 reserve snapshots.
type PSM3SnapshotRepository interface {
	// SaveSnapshot appends one snapshot row.
	SaveSnapshot(ctx context.Context, snap *entity.PSM3Snapshot) error
}
