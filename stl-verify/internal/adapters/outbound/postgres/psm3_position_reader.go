package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Psm3PositionReaderRepo implements outbound.Psm3PositionReader (VEC-833) by
// reading the psm3-indexer's already-written state: psm3_alm_shares already
// carries PSM3.convertToAssetValue(shares), so valuing an ALM's stake needs no
// on-chain read and no join with psm3_reserves.
type Psm3PositionReaderRepo struct {
	pool *pgxpool.Pool
}

func NewPsm3PositionReaderRepo(pool *pgxpool.Pool) *Psm3PositionReaderRepo {
	return &Psm3PositionReaderRepo{pool: pool}
}

var _ outbound.Psm3PositionReader = (*Psm3PositionReaderRepo)(nil)

// almShareAtBlockSQL takes the latest row at or below blockNumber, the same
// "value as of a point" convention poolStateAtBlockSQL uses. The +/-1 day
// block_timestamp band is what prunes psm3_alm_shares' hypertable chunks
// (7-day intervals); this runs once per PSM3 entry per processed block (the
// hot path, not a boot-time read), so filtering on block_number alone would
// scan every chunk on each call (VEC-541).
const almShareAtBlockSQL = `
	SELECT shares, asset_value
	FROM psm3_alm_shares
	WHERE chain_id = $1
	  AND address = $2
	  AND alm_address = $3
	  AND block_number <= $4
	  AND block_timestamp BETWEEN $5::timestamptz - INTERVAL '1 day'
	                          AND $5::timestamptz + INTERVAL '1 day'
	ORDER BY block_number DESC, block_version DESC, processing_version DESC
	LIMIT 1`

// AlmShareAtBlock returns nil, nil when almAddress has no share reading at or
// below blockNumber.
func (r *Psm3PositionReaderRepo) AlmShareAtBlock(
	ctx context.Context,
	chainID int64,
	psm3Address common.Address,
	almAddress common.Address,
	blockNumber int64,
	blockTimestamp time.Time,
) (*outbound.Psm3AlmShareSnapshot, error) {
	var sharesRaw, assetValueRaw pgtype.Numeric
	err := r.pool.QueryRow(ctx, almShareAtBlockSQL,
		chainID, psm3Address.Bytes(), almAddress.Bytes(), blockNumber, blockTimestamp,
	).Scan(&sharesRaw, &assetValueRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying psm3 alm share for %s at block %d: %w", almAddress.Hex(), blockNumber, err)
	}

	shares, err := NumericToNullableBigInt(sharesRaw)
	if err != nil {
		return nil, fmt.Errorf("decoding shares for %s at block %d: %w", almAddress.Hex(), blockNumber, err)
	}
	if shares == nil {
		return nil, fmt.Errorf("shares for %s at block %d decoded as NULL, want a value (shares is NOT NULL)", almAddress.Hex(), blockNumber)
	}

	assetValue, err := NumericToNullableBigInt(assetValueRaw)
	if err != nil {
		return nil, fmt.Errorf("decoding asset_value for %s at block %d: %w", almAddress.Hex(), blockNumber, err)
	}
	if assetValue == nil {
		return nil, fmt.Errorf("asset_value for %s at block %d decoded as NULL, want a value (asset_value is NOT NULL)", almAddress.Hex(), blockNumber)
	}

	return &outbound.Psm3AlmShareSnapshot{Shares: shares, AssetValue: assetValue}, nil
}
