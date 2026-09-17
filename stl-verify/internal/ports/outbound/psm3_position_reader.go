package outbound

import (
	"context"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Psm3AlmShareSnapshot is one tracked ALM proxy's PSM3 LP stake as of a block:
// the raw internal shares and their on-chain par valuation
// (PSM3.convertToAssetValue), both raw 1e18 "USD-like" integers per
// psm3_alm_shares' column comments -- neither is a token amount.
type Psm3AlmShareSnapshot struct {
	Shares     *big.Int
	AssetValue *big.Int
}

// Psm3PositionReader supplies the allocation tracker's read path for valuing a
// Spark PSM3 ALM stake as of a historical block (VEC-833). PSM3 shares are a
// non-transferable internal mapping with no ERC20 surface, so this reads the
// psm3-indexer's already-written state rather than a live chain call.
type Psm3PositionReader interface {
	// AlmShareAtBlock returns the latest psm3_alm_shares row for
	// (chainID, psm3Address, almAddress) at or before blockNumber, or nil when
	// the ALM has no share reading at or below blockNumber. blockTimestamp is
	// the caller's block's own timestamp, used only to bound the hypertable
	// scan (see the adapter's SQL); it is not the snapshot's own timestamp.
	AlmShareAtBlock(
		ctx context.Context,
		chainID int64,
		psm3Address common.Address,
		almAddress common.Address,
		blockNumber int64,
		blockTimestamp time.Time,
	) (*Psm3AlmShareSnapshot, error)
}
