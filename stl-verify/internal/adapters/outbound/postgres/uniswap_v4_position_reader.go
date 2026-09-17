package postgres

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var _ outbound.UniswapV4PositionValuationReader = (*UniswapV4Repository)(nil)

// These reads serve the allocation tracker's posm LP valuation (VEC-829): the
// position's identity and liquidity come from PoolManager-level state
// (uniswap_v4_position, uniswap_v4_pool_state), keyed by the PositionManager
// contract as owner; the actual holder comes from the PositionManager's
// ERC-721 transfer log (uniswap_v4_position_nft_transfer), per the "Who holds
// a posm position NFT" runbook recipe (docs/runbooks/vector-indexers.md).
// idx_uniswap_v4_position_owner_salt_block and
// idx_uniswap_v4_position_nft_transfer_to_address (20260917_120000) serve the
// two queries below that otherwise have no matching index prefix.

// heldTokenIDsAtBlockSQL mirrors the runbook's holder-at-block recipe exactly
// (join every uniswap_v4_position_manager version by chain_id, never the
// current one alone; exclude orphaned (block_number, block_version) pairs;
// order by block_number/block_version/log_index/processing_version DESC), run
// from the wallet side: candidates limits the scan to tokens ever transferred
// to wallet before resolving each one's newest holder.
const heldTokenIDsAtBlockSQL = `
	WITH candidates AS (
	    SELECT DISTINCT t.token_id
	    FROM uniswap_v4_position_nft_transfer t
	    JOIN uniswap_v4_position_manager m ON m.id = t.position_manager_id
	    WHERE m.chain_id = $1 AND t.to_address = $2 AND t.block_number <= $3
	),
	latest AS (
	    SELECT DISTINCT ON (t.token_id) t.token_id, t.to_address
	    FROM uniswap_v4_position_nft_transfer t
	    JOIN uniswap_v4_position_manager m ON m.id = t.position_manager_id
	    JOIN candidates c ON c.token_id = t.token_id
	    WHERE m.chain_id = $1
	      AND t.block_number <= $3
	      AND NOT EXISTS (
	          SELECT 1 FROM block_states b
	          WHERE b.chain_id = $1 AND b.number = t.block_number
	            AND b.version = t.block_version AND b.is_orphaned)
	    ORDER BY t.token_id, t.block_number DESC, t.block_version DESC,
	             t.log_index DESC, t.processing_version DESC
	)
	SELECT token_id FROM latest WHERE to_address = $2
	ORDER BY token_id`

func (r *UniswapV4Repository) HeldTokenIDsAtBlock(ctx context.Context, chainID int64, wallet common.Address, blockNumber int64) ([]*big.Int, error) {
	rows, err := r.pool.Query(ctx, heldTokenIDsAtBlockSQL, chainID, wallet.Bytes(), blockNumber)
	if err != nil {
		return nil, fmt.Errorf("querying held posm token ids for wallet %s at block %d: %w", wallet.Hex(), blockNumber, err)
	}
	defer rows.Close()

	var tokenIDs []*big.Int
	for rows.Next() {
		var raw pgtype.Numeric
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scanning held posm token id for wallet %s at block %d: %w", wallet.Hex(), blockNumber, err)
		}
		tokenID, err := NumericToNullableBigInt(raw)
		if err != nil {
			return nil, fmt.Errorf("decoding held posm token id for wallet %s at block %d: %w", wallet.Hex(), blockNumber, err)
		}
		if tokenID == nil {
			return nil, fmt.Errorf("held posm token id for wallet %s at block %d decoded as NULL, want a value (token_id is NOT NULL)", wallet.Hex(), blockNumber)
		}
		tokenIDs = append(tokenIDs, tokenID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating held posm token ids for wallet %s at block %d: %w", wallet.Hex(), blockNumber, err)
	}
	return tokenIDs, nil
}

// positionForTokenAtBlockSQL searches every pool on chainID because a posm
// token id does not name its pool; salt = bytes32(tokenID) is the caller's
// discriminator and, combined with owner = the PositionManager, identifies
// the position uniquely regardless of which pool it belongs to.
const positionForTokenAtBlockSQL = `
	SELECT pos.pool_id, pos.tick_lower, pos.tick_upper, pos.liquidity
	FROM uniswap_v4_position pos
	JOIN uniswap_v4_pool p ON p.id = pos.pool_id
	WHERE p.chain_id = $1
	  AND pos.owner = $2
	  AND pos.salt = $3
	  AND pos.block_number <= $4
	ORDER BY pos.block_number DESC, pos.block_version DESC, pos.processing_version DESC
	LIMIT 1`

// PositionForTokenAtBlock returns nil, nil when tokenID has no indexed
// position at or below blockNumber.
func (r *UniswapV4Repository) PositionForTokenAtBlock(
	ctx context.Context,
	chainID int64,
	positionManager common.Address,
	tokenID *big.Int,
	blockNumber int64,
) (*outbound.UniswapV4PositionSnapshot, error) {
	salt := common.BigToHash(tokenID)

	var (
		poolID int64
		tl, tu int32
		liqRaw pgtype.Numeric
	)
	err := r.pool.QueryRow(ctx, positionForTokenAtBlockSQL,
		chainID, positionManager.Bytes(), salt.Bytes(), blockNumber,
	).Scan(&poolID, &tl, &tu, &liqRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying posm position for token %s at block %d: %w", tokenID, blockNumber, err)
	}

	liquidity, err := NumericToNullableBigInt(liqRaw)
	if err != nil {
		return nil, fmt.Errorf("decoding liquidity for posm token %s at block %d: %w", tokenID, blockNumber, err)
	}
	if liquidity == nil {
		return nil, fmt.Errorf("liquidity for posm token %s at block %d decoded as NULL, want a value (liquidity is NOT NULL)", tokenID, blockNumber)
	}
	return &outbound.UniswapV4PositionSnapshot{
		PoolID:    poolID,
		TickLower: int(tl),
		TickUpper: int(tu),
		Liquidity: liquidity,
	}, nil
}

// poolStateAtBlockSQL takes the latest row at or below blockNumber, the same
// "value as of a point" convention as readLatestPositionsV4.
const poolStateAtBlockSQL = `
	SELECT sqrt_price_x96
	FROM uniswap_v4_pool_state
	WHERE pool_id = $1 AND block_number <= $2
	ORDER BY block_number DESC, block_version DESC, processing_version DESC
	LIMIT 1`

// PoolStateAtBlock returns nil, nil when poolID has no state snapshot at or
// below blockNumber.
func (r *UniswapV4Repository) PoolStateAtBlock(ctx context.Context, poolID int64, blockNumber int64) (*big.Int, error) {
	var raw pgtype.Numeric
	err := r.pool.QueryRow(ctx, poolStateAtBlockSQL, poolID, blockNumber).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying pool state for pool %d at block %d: %w", poolID, blockNumber, err)
	}

	sqrtPriceX96, err := NumericToNullableBigInt(raw)
	if err != nil {
		return nil, fmt.Errorf("decoding sqrt_price_x96 for pool %d at block %d: %w", poolID, blockNumber, err)
	}
	if sqrtPriceX96 == nil {
		return nil, fmt.Errorf("sqrt_price_x96 for pool %d at block %d decoded as NULL, want a value (sqrt_price_x96 is NOT NULL)", poolID, blockNumber)
	}
	return sqrtPriceX96, nil
}
