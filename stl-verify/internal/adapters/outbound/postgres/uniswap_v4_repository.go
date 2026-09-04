package postgres

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var _ outbound.UniswapV4Repository = (*UniswapV4Repository)(nil)

// address(0) is not usable as native ETH: the token registry already holds it
// as a "no token" sentinel with 0 decimals, so ETH amounts would scale by 10^0.
var nativeETHPlaceholder = common.HexToAddress("0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE")

type UniswapV4Repository struct {
	pool    *pgxpool.Pool
	buildID buildregistry.BuildID
}

func NewUniswapV4Repository(pool *pgxpool.Pool, buildID buildregistry.BuildID) *UniswapV4Repository {
	return &UniswapV4Repository{pool: pool, buildID: buildID}
}

// Every join right of the pool subquery is LEFT and chain-scoped so a registry
// defect becomes a named scan error, not a dropped or cross-chain pool;
// manager_id separates an absent manager row from an off-chain protocol.
const loadUniswapV4PoolsSQL = `
	SELECT p.id, m.manager_id, m.protocol_id, m.pool_manager_address, m.state_view_address,
	       pm.position_manager_id, pm.position_manager_protocol_id, pm.position_manager_address,
	       p.pool_id, p.currency0, p.currency1,
	       t0.address, t0.decimals, t1.address, t1.decimals,
	       p.fee, p.tick_spacing, p.hooks, p.deploy_block, p.snapshot_supported
	FROM (
	    SELECT DISTINCT ON (pool_id)
	           id, pool_id, currency0, currency1,
	           currency0_token_id, currency1_token_id,
	           fee, tick_spacing, hooks, deploy_block, snapshot_supported
	    FROM uniswap_v4_pool
	    WHERE chain_id = $1
	    ORDER BY pool_id, processing_version DESC
	) p
	LEFT JOIN LATERAL (
	    SELECT mgr.id AS manager_id, mgr.protocol_id,
	           pr.address AS pool_manager_address, mgr.state_view_address
	    FROM uniswap_v4_pool_manager mgr
	    LEFT JOIN protocol pr ON pr.id = mgr.protocol_id AND pr.chain_id = $1
	    WHERE mgr.chain_id = $1
	    ORDER BY mgr.processing_version DESC
	    LIMIT 1
	) m ON TRUE
	LEFT JOIN LATERAL (
	    SELECT posm.id AS position_manager_id, posm.protocol_id AS position_manager_protocol_id,
	           pr.address AS position_manager_address
	    FROM uniswap_v4_position_manager posm
	    LEFT JOIN protocol pr ON pr.id = posm.protocol_id AND pr.chain_id = $1
	    WHERE posm.chain_id = $1
	    ORDER BY posm.processing_version DESC
	    LIMIT 1
	) pm ON TRUE
	LEFT JOIN token t0 ON t0.id = p.currency0_token_id AND t0.chain_id = $1
	LEFT JOIN token t1 ON t1.id = p.currency1_token_id AND t1.chain_id = $1
	ORDER BY p.id`

func (r *UniswapV4Repository) LoadPools(ctx context.Context, chainID int64) ([]outbound.UniswapV4PoolRow, error) {
	rows, err := r.pool.Query(ctx, loadUniswapV4PoolsSQL, chainID)
	if err != nil {
		return nil, fmt.Errorf("querying uniswap_v4 pools: %w", err)
	}
	defer rows.Close()

	var result []outbound.UniswapV4PoolRow
	for rows.Next() {
		pool, scanErr := scanUniswapV4PoolRow(rows, chainID)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, pool)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating uniswap_v4 pools: %w", err)
	}
	return result, nil
}

func scanUniswapV4PoolRow(rows pgx.Rows, chainID int64) (outbound.UniswapV4PoolRow, error) {
	var (
		id                     int64
		managerID              *int64
		protocolID             *int64
		poolManager, stateView []byte
		positionManagerID      *int64
		positionManagerProtoID *int64
		positionManagerAddress []byte
		onchainPoolID          []byte
		currency0, currency1   []byte
		token0, token1         []byte
		decimals0, decimals1   *int
		fee, tickSpacing       int
		hooks                  []byte
		deployBlock            int64
		snapshotSupported      bool
		row                    outbound.UniswapV4PoolRow
	)
	if err := rows.Scan(&id, &managerID, &protocolID, &poolManager, &stateView,
		&positionManagerID, &positionManagerProtoID, &positionManagerAddress,
		&onchainPoolID, &currency0, &currency1,
		&token0, &decimals0, &token1, &decimals1,
		&fee, &tickSpacing, &hooks, &deployBlock, &snapshotSupported); err != nil {
		return row, fmt.Errorf("scanning uniswap_v4 pool row: %w", err)
	}
	if managerID == nil {
		return row, fmt.Errorf("chain %d has uniswap_v4 pools (e.g. %d) but no uniswap_v4_pool_manager row", chainID, id)
	}
	if poolManager == nil {
		return row, fmt.Errorf("uniswap_v4_pool_manager row %d for chain %d references protocol %d, which is not on chain %d", *managerID, chainID, *protocolID, chainID)
	}
	if positionManagerID == nil {
		return row, fmt.Errorf("chain %d has uniswap_v4 pools (e.g. %d) but no uniswap_v4_position_manager row", chainID, id)
	}
	if positionManagerAddress == nil {
		return row, fmt.Errorf("uniswap_v4_position_manager row %d for chain %d references protocol %d, which is not on chain %d", *positionManagerID, chainID, *positionManagerProtoID, chainID)
	}

	currency0Decimals, err := currencyTokenDecimals(id, "currency0", common.BytesToAddress(currency0), token0, decimals0)
	if err != nil {
		return row, err
	}
	currency1Decimals, err := currencyTokenDecimals(id, "currency1", common.BytesToAddress(currency1), token1, decimals1)
	if err != nil {
		return row, err
	}

	return outbound.UniswapV4PoolRow{
		ID:                id,
		ProtocolID:        *protocolID,
		PoolManager:       common.BytesToAddress(poolManager),
		StateView:         common.BytesToAddress(stateView),
		PositionManagerID: *positionManagerID,
		PositionManager:   common.BytesToAddress(positionManagerAddress),
		PoolIDHash:        common.BytesToHash(onchainPoolID),
		Currency0:         common.BytesToAddress(currency0),
		Currency1:         common.BytesToAddress(currency1),
		Currency0Decimals: currency0Decimals,
		Currency1Decimals: currency1Decimals,
		Fee:               fee,
		TickSpacing:       tickSpacing,
		Hooks:             common.BytesToAddress(hooks),
		DeployBlock:       deployBlock,
		SnapshotSupported: snapshotSupported,
	}, nil
}

// token is the raw column, so an absent row stays distinguishable from the zero
// address — itself a meaningful currency value.
func currencyTokenDecimals(poolID int64, field string, currency common.Address, token []byte, decimals *int) (int, error) {
	want := currency
	if currency == (common.Address{}) {
		want = nativeETHPlaceholder
	}
	if token == nil {
		return 0, fmt.Errorf("uniswap_v4 pool %d %s %s has no token row on its own chain: the currency token id points at another chain", poolID, field, currency)
	}
	if got := common.BytesToAddress(token); got != want {
		return 0, fmt.Errorf("uniswap_v4 pool %d %s %s resolves to token %s, want %s", poolID, field, currency, got, want)
	}
	if decimals == nil {
		return 0, fmt.Errorf("uniswap_v4 pool %d %s token %s has NULL decimals: amounts cannot be scaled", poolID, field, want)
	}
	return *decimals, nil
}

type v4StateConverted struct {
	s                *entity.UniswapV4PoolState
	sqrtPriceX96     pgtype.Numeric
	liquidity        pgtype.Numeric
	feeGrowthGlobal0 pgtype.Numeric
	feeGrowthGlobal1 pgtype.Numeric
}

type v4SwapConverted struct {
	s            *entity.UniswapV4Swap
	amount0      pgtype.Numeric
	amount1      pgtype.Numeric
	sqrtPriceX96 pgtype.Numeric
	liquidity    pgtype.Numeric
}

type v4LiquidityEventConverted struct {
	e              *entity.UniswapV4LiquidityEvent
	liquidityDelta pgtype.Numeric
}

// SaveBlock persists a block's uniswap_v4 rows in one pgx.Batch within tx,
// except ticks and positions, which the append-on-change writers take instead.
func (r *UniswapV4Repository) SaveBlock(ctx context.Context, tx pgx.Tx, w outbound.UniswapV4BlockWrites) (stateRows outbound.StateRowCounts, err error) {
	rows, err := convertV4BlockWrites(w)
	if err != nil {
		return outbound.StateRowCounts{}, err
	}

	batch := &pgx.Batch{}
	queueUniswapV4Batch(batch, rows, r.buildID)

	stateRows, err = sendUniswapV4Batch(ctx, tx, batch, rows)
	if err != nil {
		return stateRows, err
	}

	// pgx forbids new queries while a batch result reader is open. Ticks before
	// positions is a fixed order: the two lock domains are disjoint, so a varying
	// phase order would deadlock concurrent writers across them.
	if err := uniswapV4TickWriter.writeTicks(ctx, tx, uniswapV4TickRows(w.Ticks), r.buildID); err != nil {
		return stateRows, err
	}
	if _, err := r.writePositions(ctx, tx, w.Positions); err != nil {
		return stateRows, err
	}

	return stateRows, nil
}

func (r *UniswapV4Repository) SavePositions(ctx context.Context, tx pgx.Tx, positions []*entity.UniswapV4Position) (int64, error) {
	return r.writePositions(ctx, tx, positions)
}

// currentUniswapV4PoolCTE maps a superseded registry surrogate forward to the
// current one for its (chain_id, pool_id); fact rows keep the retired id.
const currentUniswapV4PoolCTE = `
	WITH cur AS (
	    SELECT DISTINCT ON (chain_id, pool_id) id, chain_id, pool_id
	    FROM uniswap_v4_pool
	    ORDER BY chain_id, pool_id, processing_version DESC
	)`

// The ±1 day block_timestamp band is what prunes chunks; filtering on
// block_number alone scans every chunk of the hypertable on each reorg (VEC-541).
// Natural keys, not surrogate ids: rows written under a superseded registry
// version name the same PoolId, and a version appended after a worker booted
// must not change what that worker resolves — it loaded the registry once.
const poolIDsWithStateAtBlockSQL = `
	SELECT DISTINCT p.pool_id
	FROM uniswap_v4_pool_state s
	JOIN uniswap_v4_pool p ON p.id = s.pool_id
	WHERE p.chain_id = $1
	  AND s.block_number = $2
	  AND s.block_timestamp BETWEEN $3::timestamptz - INTERVAL '1 day'
	                            AND $3::timestamptz + INTERVAL '1 day'
	ORDER BY p.pool_id`

func (r *UniswapV4Repository) PoolIDsWithStateAtBlock(ctx context.Context, chainID int64, blockNumber int64, blockTimestamp time.Time) ([]common.Hash, error) {
	rows, err := r.pool.Query(ctx, poolIDsWithStateAtBlockSQL, chainID, blockNumber, blockTimestamp)
	if err != nil {
		return nil, fmt.Errorf("querying pools with state at block %d: %w", blockNumber, err)
	}
	defer rows.Close()

	var poolIDs []common.Hash
	for rows.Next() {
		var poolID []byte
		if err := rows.Scan(&poolID); err != nil {
			return nil, fmt.Errorf("scanning pool id with state at block %d: %w", blockNumber, err)
		}
		poolIDs = append(poolIDs, common.BytesToHash(poolID))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating pools with state at block %d: %w", blockNumber, err)
	}
	return poolIDs, nil
}

// Unbounded by block_timestamp, so it prunes no chunks (VEC-541): boot only,
// never a per-block path. The pool_state side also stops at the 1-year tiering
// horizon (enable_tiered_reads defaults off); the plain tick table is exact.
const poolIDsEverSnapshottedSQL = currentUniswapV4PoolCTE + `
	SELECT DISTINCT cur.id
	FROM uniswap_v4_pool p
	JOIN cur ON cur.chain_id = p.chain_id AND cur.pool_id = p.pool_id
	WHERE p.chain_id = $1
	  AND (EXISTS (SELECT 1 FROM uniswap_v4_tick t WHERE t.pool_id = p.id)
	       OR EXISTS (SELECT 1 FROM uniswap_v4_pool_state s WHERE s.pool_id = p.id))
	ORDER BY cur.id`

func (r *UniswapV4Repository) PoolIDsEverSnapshotted(ctx context.Context, chainID int64) ([]int64, error) {
	rows, err := r.pool.Query(ctx, poolIDsEverSnapshottedSQL, chainID)
	if err != nil {
		return nil, fmt.Errorf("querying pools ever snapshotted on chain %d: %w", chainID, err)
	}
	defer rows.Close()

	var poolIDs []int64
	for rows.Next() {
		var poolID int64
		if err := rows.Scan(&poolID); err != nil {
			return nil, fmt.Errorf("scanning pool id ever snapshotted on chain %d: %w", chainID, err)
		}
		poolIDs = append(poolIDs, poolID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating pools ever snapshotted on chain %d: %w", chainID, err)
	}
	return poolIDs, nil
}

// idx_uniswap_v4_tick_block_lookup serves the fact side: both filters become
// boundary quals and tick comes out sorted, which the PK's order cannot give.
const ticksForPoolAtBlockSQL = currentUniswapV4PoolCTE + `
	SELECT DISTINCT t.tick
	FROM uniswap_v4_tick t
	JOIN uniswap_v4_pool p ON p.id = t.pool_id
	JOIN cur ON cur.chain_id = p.chain_id AND cur.pool_id = p.pool_id
	WHERE p.chain_id = $1 AND cur.id = $2 AND t.block_number = $3
	ORDER BY t.tick`

func (r *UniswapV4Repository) TicksForPoolAtBlock(ctx context.Context, chainID int64, poolID int64, blockNumber int64) ([]int32, error) {
	rows, err := r.pool.Query(ctx, ticksForPoolAtBlockSQL, chainID, poolID, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("querying ticks for pool %d at block %d: %w", poolID, blockNumber, err)
	}
	defer rows.Close()

	var ticks []int32
	for rows.Next() {
		var tick int32
		if err := rows.Scan(&tick); err != nil {
			return nil, fmt.Errorf("scanning tick for pool %d at block %d: %w", poolID, blockNumber, err)
		}
		ticks = append(ticks, tick)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating ticks for pool %d at block %d: %w", poolID, blockNumber, err)
	}
	return ticks, nil
}

func convertV4States(states []*entity.UniswapV4PoolState) ([]v4StateConverted, error) {
	out := make([]v4StateConverted, 0, len(states))
	for i, s := range states {
		sqrtPriceX96, convErr := BigIntToNumericRequired(s.SqrtPriceX96, "sqrt_price_x96")
		if convErr != nil {
			return nil, fmt.Errorf("state %d converting sqrt_price_x96: %w", i, convErr)
		}
		liquidity, convErr := BigIntToNumericRequired(s.Liquidity, "liquidity")
		if convErr != nil {
			return nil, fmt.Errorf("state %d converting liquidity: %w", i, convErr)
		}
		feeGrowthGlobal0, convErr := BigIntToNumericRequired(s.FeeGrowthGlobal0X128, "fee_growth_global0_x128")
		if convErr != nil {
			return nil, fmt.Errorf("state %d converting fee_growth_global0_x128: %w", i, convErr)
		}
		feeGrowthGlobal1, convErr := BigIntToNumericRequired(s.FeeGrowthGlobal1X128, "fee_growth_global1_x128")
		if convErr != nil {
			return nil, fmt.Errorf("state %d converting fee_growth_global1_x128: %w", i, convErr)
		}
		out = append(out, v4StateConverted{
			s: s, sqrtPriceX96: sqrtPriceX96, liquidity: liquidity,
			feeGrowthGlobal0: feeGrowthGlobal0, feeGrowthGlobal1: feeGrowthGlobal1,
		})
	}
	return out, nil
}

func convertV4Swaps(swaps []*entity.UniswapV4Swap) ([]v4SwapConverted, error) {
	out := make([]v4SwapConverted, 0, len(swaps))
	for i, s := range swaps {
		amount0, convErr := BigIntToNumericRequired(s.Amount0, "amount0")
		if convErr != nil {
			return nil, fmt.Errorf("swap %d converting amount0: %w", i, convErr)
		}
		amount1, convErr := BigIntToNumericRequired(s.Amount1, "amount1")
		if convErr != nil {
			return nil, fmt.Errorf("swap %d converting amount1: %w", i, convErr)
		}
		sqrtPriceX96, convErr := BigIntToNumericRequired(s.SqrtPriceX96, "sqrt_price_x96")
		if convErr != nil {
			return nil, fmt.Errorf("swap %d converting sqrt_price_x96: %w", i, convErr)
		}
		liquidity, convErr := BigIntToNumericRequired(s.Liquidity, "liquidity")
		if convErr != nil {
			return nil, fmt.Errorf("swap %d converting liquidity: %w", i, convErr)
		}
		out = append(out, v4SwapConverted{
			s: s, amount0: amount0, amount1: amount1, sqrtPriceX96: sqrtPriceX96, liquidity: liquidity,
		})
	}
	return out, nil
}

func convertV4LiquidityEvents(events []*entity.UniswapV4LiquidityEvent) ([]v4LiquidityEventConverted, error) {
	out := make([]v4LiquidityEventConverted, 0, len(events))
	for i, e := range events {
		liquidityDelta, convErr := BigIntToNumericRequired(e.LiquidityDelta, "liquidity_delta")
		if convErr != nil {
			return nil, fmt.Errorf("liquidity event %d converting liquidity_delta: %w", i, convErr)
		}
		out = append(out, v4LiquidityEventConverted{e: e, liquidityDelta: liquidityDelta})
	}
	return out, nil
}

type v4BatchRows struct {
	states       []v4StateConverted
	swaps        []v4SwapConverted
	liqs         []v4LiquidityEventConverted
	poolEvents   []*entity.UniswapV4PoolEvent
	nftTransfers []v4NFTTransferConverted
}

type v4NFTTransferConverted struct {
	t       *entity.UniswapV4PositionNFTTransfer
	tokenID pgtype.Numeric
}

func convertV4NFTTransfers(transfers []*entity.UniswapV4PositionNFTTransfer) ([]v4NFTTransferConverted, error) {
	out := make([]v4NFTTransferConverted, 0, len(transfers))
	for i, t := range transfers {
		tokenID, convErr := BigIntToNumericRequired(t.TokenID, "token_id")
		if convErr != nil {
			return nil, fmt.Errorf("nft transfer %d converting token_id: %w", i, convErr)
		}
		out = append(out, v4NFTTransferConverted{t: t, tokenID: tokenID})
	}
	return out, nil
}

func convertV4BlockWrites(w outbound.UniswapV4BlockWrites) (v4BatchRows, error) {
	states, err := convertV4States(w.States)
	if err != nil {
		return v4BatchRows{}, err
	}
	swaps, err := convertV4Swaps(w.Swaps)
	if err != nil {
		return v4BatchRows{}, err
	}
	liqs, err := convertV4LiquidityEvents(w.LiquidityEvents)
	if err != nil {
		return v4BatchRows{}, err
	}
	nftTransfers, err := convertV4NFTTransfers(w.NFTTransfers)
	if err != nil {
		return v4BatchRows{}, err
	}
	return v4BatchRows{
		states: states, swaps: swaps, liqs: liqs,
		poolEvents: w.PoolEvents, nftTransfers: nftTransfers,
	}, nil
}

type v4BatchSection struct {
	name            string
	count           int
	countsStateRows bool
}

// Order must match queueUniswapV4Batch: pgx returns batch results positionally,
// so a reordering silently mis-attributes the row counts and the error messages.
func (rows v4BatchRows) sections() []v4BatchSection {
	return []v4BatchSection{
		{name: "state", count: len(rows.states), countsStateRows: true},
		{name: "swap", count: len(rows.swaps)},
		{name: "liquidity event", count: len(rows.liqs)},
		{name: "pool event", count: len(rows.poolEvents)},
		{name: "nft transfer", count: len(rows.nftTransfers)},
	}
}

// processing_version comes from each table's next_processing_version_* function, not
// its trigger: on a columnstored chunk the arbiter resolves before triggers fire (VEC-615).
func queueUniswapV4Batch(batch *pgx.Batch, rows v4BatchRows, buildID buildregistry.BuildID) {
	queueV4States(batch, rows.states, buildID)
	queueV4Swaps(batch, rows.swaps, buildID)
	queueV4LiquidityEvents(batch, rows.liqs, buildID)
	queueV4PoolEvents(batch, rows.poolEvents, buildID)
	queueV4NFTTransfers(batch, rows.nftTransfers, buildID)
}

func queueV4States(batch *pgx.Batch, states []v4StateConverted, buildID buildregistry.BuildID) {
	for _, c := range states {
		s := c.s
		batch.Queue(
			`INSERT INTO uniswap_v4_pool_state
			   (pool_id, block_number, block_version, block_timestamp,
			    sqrt_price_x96, tick, protocol_fee, lp_fee, liquidity,
			    fee_growth_global0_x128, fee_growth_global1_x128, processing_version, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
			         next_processing_version_uniswap_v4_pool_state($1,$2,$3,$12), $12)
			 ON CONFLICT (pool_id, block_timestamp, block_number, block_version, processing_version) DO NOTHING`,
			s.PoolID, s.BlockNumber, s.BlockVersion, s.BlockTimestamp,
			c.sqrtPriceX96, s.Tick, s.ProtocolFee, s.LpFee, c.liquidity,
			c.feeGrowthGlobal0, c.feeGrowthGlobal1, int(buildID),
		)
	}
}

func queueV4Swaps(batch *pgx.Batch, swaps []v4SwapConverted, buildID buildregistry.BuildID) {
	for _, c := range swaps {
		s := c.s
		batch.Queue(
			`INSERT INTO uniswap_v4_swap
			   (pool_id, block_number, block_version, block_timestamp,
			    tx_hash, log_index, sender, amount0, amount1,
			    sqrt_price_x96, liquidity, tick, fee, processing_version, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
			         next_processing_version_uniswap_v4_swap($1,$2,$3,$6,$14), $14)
			 ON CONFLICT (pool_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
			s.PoolID, s.BlockNumber, s.BlockVersion, s.BlockTimestamp,
			s.TxHash.Bytes(), s.LogIndex, s.Sender.Bytes(), c.amount0, c.amount1,
			c.sqrtPriceX96, c.liquidity, s.Tick, s.Fee, int(buildID),
		)
	}
}

func queueV4LiquidityEvents(batch *pgx.Batch, liqs []v4LiquidityEventConverted, buildID buildregistry.BuildID) {
	for _, c := range liqs {
		e := c.e
		batch.Queue(
			`INSERT INTO uniswap_v4_liquidity_event
			   (pool_id, block_number, block_version, block_timestamp,
			    tx_hash, log_index, sender, tick_lower, tick_upper,
			    liquidity_delta, salt, processing_version, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
			         next_processing_version_uniswap_v4_liquidity_event($1,$2,$3,$6,$12), $12)
			 ON CONFLICT (pool_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
			e.PoolID, e.BlockNumber, e.BlockVersion, e.BlockTimestamp,
			e.TxHash.Bytes(), e.LogIndex, e.Sender.Bytes(), e.TickLower, e.TickUpper,
			c.liquidityDelta, e.Salt.Bytes(), int(buildID),
		)
	}
}

func queueV4PoolEvents(batch *pgx.Batch, poolEvents []*entity.UniswapV4PoolEvent, buildID buildregistry.BuildID) {
	for _, e := range poolEvents {
		batch.Queue(
			`INSERT INTO uniswap_v4_pool_event
			   (pool_id, block_number, block_version, block_timestamp,
			    tx_hash, log_index, event_name, params, processing_version, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,
			         next_processing_version_uniswap_v4_pool_event($1,$2,$3,$6,$9), $9)
			 ON CONFLICT (pool_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
			e.PoolID, e.BlockNumber, e.BlockVersion, e.BlockTimestamp,
			e.TxHash.Bytes(), e.LogIndex, string(e.EventName), []byte(e.Params), int(buildID),
		)
	}
}

func queueV4NFTTransfers(batch *pgx.Batch, transfers []v4NFTTransferConverted, buildID buildregistry.BuildID) {
	for _, c := range transfers {
		t := c.t
		batch.Queue(
			`INSERT INTO uniswap_v4_position_nft_transfer
			   (position_manager_id, token_id, block_number, block_version, block_timestamp,
			    tx_hash, log_index, from_address, to_address, processing_version, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,
			         next_processing_version_uniswap_v4_position_nft_transfer($1,$3,$4,$7,$10), $10)
			 ON CONFLICT (position_manager_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
			t.PositionManagerID, c.tokenID, t.BlockNumber, t.BlockVersion, t.BlockTimestamp,
			t.TxHash.Bytes(), t.LogIndex, t.From.Bytes(), t.To.Bytes(), int(buildID),
		)
	}
}

func sendUniswapV4Batch(ctx context.Context, tx pgx.Tx, batch *pgx.Batch, rows v4BatchRows) (stateRows outbound.StateRowCounts, err error) {
	br := tx.SendBatch(ctx, batch)
	defer func() {
		if closeErr := br.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing uniswap_v4 SaveBlock batch: %w", closeErr))
		}
	}()

	for _, section := range rows.sections() {
		for i := range section.count {
			tag, readErr := br.Exec()
			if readErr != nil {
				return stateRows, fmt.Errorf("batch %s %d: %w", section.name, i, readErr)
			}
			if section.countsStateRows {
				stateRows.Attempted++
				stateRows.Persisted += tag.RowsAffected()
			}
		}
	}

	return stateRows, nil
}

func uniswapV4TickRows(ticks []*entity.UniswapV4Tick) []uniswapTickRow {
	rows := make([]uniswapTickRow, len(ticks))
	for i, t := range ticks {
		rows[i] = uniswapTickRow{
			poolID:                t.PoolID,
			tick:                  t.Tick,
			blockNumber:           t.BlockNumber,
			blockVersion:          t.BlockVersion,
			blockTimestamp:        t.BlockTimestamp,
			liquidityGross:        t.LiquidityGross,
			liquidityNet:          t.LiquidityNet,
			feeGrowthOutside0X128: t.FeeGrowthOutside0X128,
			feeGrowthOutside1X128: t.FeeGrowthOutside1X128,
		}
	}
	return rows
}

// PositionsForPoolAtBlock queries the connection pool for committed rows, so it
// is safe to call before the write transaction opens.
func (r *UniswapV4Repository) PositionsForPoolAtBlock(ctx context.Context, poolID int64, blockNumber int64) ([]entity.UniswapV4PositionKey, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT DISTINCT owner, tick_lower, tick_upper, salt FROM uniswap_v4_position
		 WHERE pool_id = $1 AND block_number = $2
		 ORDER BY owner, tick_lower, tick_upper, salt`,
		poolID, blockNumber,
	)
	if err != nil {
		return nil, fmt.Errorf("querying positions for pool %d at block %d: %w", poolID, blockNumber, err)
	}
	defer rows.Close()

	var keys []entity.UniswapV4PositionKey
	for rows.Next() {
		var (
			owner                []byte
			tickLower, tickUpper int32
			salt                 []byte
		)
		if err := rows.Scan(&owner, &tickLower, &tickUpper, &salt); err != nil {
			return nil, fmt.Errorf("scanning position for pool %d at block %d: %w", poolID, blockNumber, err)
		}
		key := entity.UniswapV4PositionKey{
			Owner:     common.BytesToAddress(owner),
			TickLower: int(tickLower),
			TickUpper: int(tickUpper),
			Salt:      common.BytesToHash(salt),
		}
		if err := key.Validate(); err != nil {
			return nil, fmt.Errorf("reading position for pool %d at block %d: %w", poolID, blockNumber, err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating positions for pool %d at block %d: %w", poolID, blockNumber, err)
	}
	return keys, nil
}

// sharedBlockNumber returns the one block every row belongs to; rows must be
// non-empty. The read-latest queries bound on that height, so a mixed batch
// would compare a row against another block's state.
func sharedBlockNumber[T any](kind string, rows []T, blockNumberOf func(T) int64) (int64, error) {
	blockNumber := blockNumberOf(rows[0])
	for _, row := range rows[1:] {
		if got := blockNumberOf(row); got != blockNumber {
			return 0, fmt.Errorf("uniswap_v4 %s write spans blocks %d and %d: one SaveBlock is one block", kind, blockNumber, got)
		}
	}
	return blockNumber, nil
}

func (r *UniswapV4Repository) writePositions(ctx context.Context, tx pgx.Tx, positions []*entity.UniswapV4Position) (int64, error) {
	if len(positions) == 0 {
		return 0, nil
	}

	blockNumber, err := sharedBlockNumber("position", positions, func(p *entity.UniswapV4Position) int64 { return p.BlockNumber })
	if err != nil {
		return 0, err
	}

	keys := distinctSortedV4PositionKeys(positions)
	// A duplicate slot would compare both rows against the same prior state and
	// let ON CONFLICT DO NOTHING drop the second one's values in silence.
	if len(keys) != len(positions) {
		return 0, fmt.Errorf("uniswap_v4 position write has %d rows for %d distinct slots: one block must touch a position once", len(positions), len(keys))
	}

	if err := lockPositionKeysV4(ctx, tx, keys); err != nil {
		return 0, err
	}

	latest, err := readLatestPositionsV4(ctx, tx, keys, blockNumber)
	if err != nil {
		return 0, err
	}

	return r.insertChangedPositionsV4(ctx, tx, positions, latest)
}

type v4PositionKey struct {
	poolID int64
	key    entity.UniswapV4PositionKey
}

func distinctSortedV4PositionKeys(positions []*entity.UniswapV4Position) []v4PositionKey {
	seen := make(map[v4PositionKey]struct{}, len(positions))
	for _, p := range positions {
		seen[v4PositionKey{poolID: p.PoolID, key: p.Key()}] = struct{}{}
	}
	keys := make([]v4PositionKey, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b v4PositionKey) int {
		return cmp.Or(cmp.Compare(a.poolID, b.poolID), a.key.Compare(b.key))
	})
	return keys
}

// lockPositionKeysV4 takes every slot's advisory lock in one round-trip. keys
// must be in distinctSortedV4PositionKeys order or overlapping writers deadlock.
// The lock domain is deliberately not the pv-trigger's "u4pos|…|block|version".
func lockPositionKeysV4(ctx context.Context, tx pgx.Tx, keys []v4PositionKey) error {
	lockKeys := make([]string, len(keys))
	for i, k := range keys {
		lockKeys[i] = fmt.Sprintf("uniswap_v4_position|%d|%s|%d|%d|%s",
			k.poolID, k.key.Owner.Hex(), k.key.TickLower, k.key.TickUpper, k.key.Salt.Hex())
	}
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended(k, 0))
		 FROM unnest($1::text[]) WITH ORDINALITY AS u(k, ord)
		 ORDER BY ord`,
		lockKeys,
	); err != nil {
		return fmt.Errorf("locking %d uniswap_v4 position slots: %w", len(keys), err)
	}
	return nil
}

func positionKeyArrays(keys []v4PositionKey) (poolIDs []int64, owners [][]byte, tickLowers, tickUppers []int32, salts [][]byte) {
	poolIDs = make([]int64, len(keys))
	owners = make([][]byte, len(keys))
	tickLowers = make([]int32, len(keys))
	tickUppers = make([]int32, len(keys))
	salts = make([][]byte, len(keys))
	for i, k := range keys {
		poolIDs[i] = k.poolID
		owners[i] = k.key.Owner.Bytes()
		tickLowers[i] = int32(k.key.TickLower)
		tickUppers[i] = int32(k.key.TickUpper)
		salts[i] = k.key.Salt.Bytes()
	}
	return poolIDs, owners, tickLowers, tickUppers, salts
}

// readLatestPositionsV4 fetches the latest row per slot at or below blockNumber;
// slots with no prior row are absent. The height bound keeps an out-of-order
// backfill write from being dropped as unchanged against a newer row.
func readLatestPositionsV4(ctx context.Context, tx pgx.Tx, keys []v4PositionKey, blockNumber int64) (map[v4PositionKey]v4PositionValues, error) {
	poolIDs, owners, tickLowers, tickUppers, salts := positionKeyArrays(keys)

	rows, err := tx.Query(ctx,
		`SELECT DISTINCT ON (p.pool_id, p.owner, p.tick_lower, p.tick_upper, p.salt)
		        p.pool_id, p.owner, p.tick_lower, p.tick_upper, p.salt,
		        p.block_number, p.block_version,
		        p.liquidity, p.fee_growth_inside0_last_x128, p.fee_growth_inside1_last_x128
		 FROM uniswap_v4_position p
		 JOIN unnest($1::bigint[], $2::bytea[], $3::int[], $4::int[], $5::bytea[])
		      AS k(pool_id, owner, tick_lower, tick_upper, salt)
		   ON p.pool_id = k.pool_id AND p.owner = k.owner
		  AND p.tick_lower = k.tick_lower AND p.tick_upper = k.tick_upper
		  AND p.salt = k.salt
		 WHERE p.block_number <= $6
		 ORDER BY p.pool_id, p.owner, p.tick_lower, p.tick_upper, p.salt,
		          p.block_number DESC, p.block_version DESC, p.processing_version DESC`,
		poolIDs, owners, tickLowers, tickUppers, salts, blockNumber,
	)
	if err != nil {
		return nil, fmt.Errorf("querying latest uniswap_v4 positions for %d slots: %w", len(keys), err)
	}
	defer rows.Close()

	latest := make(map[v4PositionKey]v4PositionValues, len(keys))
	for rows.Next() {
		var (
			poolID               int64
			owner, salt          []byte
			tickLower, tickUpper int32
			priorBlockNumber     int64
			blockVersion         int
			liquidity            pgtype.Numeric
			feeGrowthInside0     pgtype.Numeric
			feeGrowthInside1     pgtype.Numeric
		)
		if err := rows.Scan(&poolID, &owner, &tickLower, &tickUpper, &salt,
			&priorBlockNumber, &blockVersion,
			&liquidity, &feeGrowthInside0, &feeGrowthInside1); err != nil {
			return nil, fmt.Errorf("scanning latest uniswap_v4 position row: %w", err)
		}
		slot := v4PositionKey{poolID: poolID, key: entity.UniswapV4PositionKey{
			Owner:     common.BytesToAddress(owner),
			TickLower: int(tickLower),
			TickUpper: int(tickUpper),
			Salt:      common.BytesToHash(salt),
		}}
		values, convErr := toV4PositionValues(priorBlockNumber, blockVersion, liquidity, feeGrowthInside0, feeGrowthInside1)
		if convErr != nil {
			return nil, fmt.Errorf("reading latest uniswap_v4 position for pool=%d %+v: %w", poolID, slot.key, convErr)
		}
		latest[slot] = values
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating latest uniswap_v4 positions: %w", err)
	}
	return latest, nil
}

func (r *UniswapV4Repository) insertChangedPositionsV4(
	ctx context.Context, tx pgx.Tx,
	positions []*entity.UniswapV4Position,
	latest map[v4PositionKey]v4PositionValues,
) (inserted int64, err error) {
	batch := &pgx.Batch{}
	var queued []v4QueuedPosition
	for i, p := range positions {
		slot := v4PositionKey{poolID: p.PoolID, key: p.Key()}
		prior, hasPrior := latest[slot]
		if hasPrior && v4PositionUnchanged(prior, p) {
			continue
		}
		converted, convErr := convertV4Position(p)
		if convErr != nil {
			return 0, fmt.Errorf("position %d: converting pool=%d %+v: %w", i, p.PoolID, slot.key, convErr)
		}
		batch.Queue(
			`INSERT INTO uniswap_v4_position
			   (pool_id, owner, tick_lower, tick_upper, salt,
			    block_number, block_version, block_timestamp,
			    liquidity, fee_growth_inside0_last_x128, fee_growth_inside1_last_x128, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			 ON CONFLICT (pool_id, owner, tick_lower, tick_upper, salt,
			              block_number, block_version, processing_version) DO NOTHING`,
			p.PoolID, p.Owner.Bytes(), p.TickLower, p.TickUpper, p.Salt.Bytes(),
			p.BlockNumber, p.BlockVersion, p.BlockTimestamp,
			converted.liquidity, converted.feeGrowthInside0LastX128,
			converted.feeGrowthInside1LastX128, int(r.buildID),
		)
		queued = append(queued, v4QueuedPosition{
			slot:          slot,
			blockNumber:   p.BlockNumber,
			supersedesRow: hasPrior && prior.blockNumber == p.BlockNumber && prior.blockVersion == p.BlockVersion,
		})
	}

	if len(queued) == 0 {
		return 0, nil
	}

	br := tx.SendBatch(ctx, batch)
	defer func() {
		if closeErr := br.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing uniswap_v4 position batch: %w", closeErr))
		}
	}()
	for _, q := range queued {
		tag, execErr := br.Exec()
		if execErr != nil {
			return 0, fmt.Errorf("inserting uniswap_v4 position pool=%d %+v at block %d: %w", q.slot.poolID, q.slot.key, q.blockNumber, execErr)
		}
		if err := q.assertInserted(tag.RowsAffected()); err != nil {
			return 0, err
		}
		inserted += tag.RowsAffected()
	}
	return inserted, nil
}

type v4QueuedPosition struct {
	slot        v4PositionKey
	blockNumber int64
	// supersedesRow marks values differing from a row at the SAME (block_number,
	// block_version): the one case where a discarded insert is a real
	// disagreement rather than a replay of an older version.
	supersedesRow bool
}

func (q v4QueuedPosition) assertInserted(rowsAffected int64) error {
	if q.supersedesRow && rowsAffected == 0 {
		return fmt.Errorf("uniswap_v4 position pool=%d %+v at block %d already stored with different values under this build: the authoritative read disagrees with itself",
			q.slot.poolID, q.slot.key, q.blockNumber)
	}
	return nil
}

type v4PositionConverted struct {
	liquidity                pgtype.Numeric
	feeGrowthInside0LastX128 pgtype.Numeric
	feeGrowthInside1LastX128 pgtype.Numeric
}

func convertV4Position(p *entity.UniswapV4Position) (v4PositionConverted, error) {
	liquidity, err := BigIntToNumericRequired(p.Liquidity, "liquidity")
	if err != nil {
		return v4PositionConverted{}, err
	}
	feeGrowthInside0LastX128, err := BigIntToNumericRequired(p.FeeGrowthInside0LastX128, "fee_growth_inside0_last_x128")
	if err != nil {
		return v4PositionConverted{}, err
	}
	feeGrowthInside1LastX128, err := BigIntToNumericRequired(p.FeeGrowthInside1LastX128, "fee_growth_inside1_last_x128")
	if err != nil {
		return v4PositionConverted{}, err
	}
	return v4PositionConverted{
		liquidity:                liquidity,
		feeGrowthInside0LastX128: feeGrowthInside0LastX128,
		feeGrowthInside1LastX128: feeGrowthInside1LastX128,
	}, nil
}

type v4PositionValues struct {
	blockNumber              int64
	blockVersion             int
	liquidity                *big.Int
	feeGrowthInside0LastX128 *big.Int
	feeGrowthInside1LastX128 *big.Int
}

func toV4PositionValues(
	blockNumber int64, blockVersion int,
	liquidity, feeGrowthInside0LastX128, feeGrowthInside1LastX128 pgtype.Numeric,
) (v4PositionValues, error) {
	var v v4PositionValues
	var err error
	if v.liquidity, err = NumericToNullableBigInt(liquidity); err != nil {
		return v, fmt.Errorf("liquidity: %w", err)
	}
	if v.feeGrowthInside0LastX128, err = NumericToNullableBigInt(feeGrowthInside0LastX128); err != nil {
		return v, fmt.Errorf("fee_growth_inside0_last_x128: %w", err)
	}
	if v.feeGrowthInside1LastX128, err = NumericToNullableBigInt(feeGrowthInside1LastX128); err != nil {
		return v, fmt.Errorf("fee_growth_inside1_last_x128: %w", err)
	}
	v.blockNumber = blockNumber
	v.blockVersion = blockVersion
	return v, nil
}

// v4PositionUnchanged reports whether the stored row already makes p redundant.
// Within one height a differing block_version is a reorg re-observation and must
// append; across heights the versions count different blocks, so only values decide.
func v4PositionUnchanged(latest v4PositionValues, p *entity.UniswapV4Position) bool {
	if latest.blockNumber == p.BlockNumber && latest.blockVersion != p.BlockVersion {
		return false
	}
	return bigIntEqual(latest.liquidity, p.Liquidity) &&
		bigIntEqual(latest.feeGrowthInside0LastX128, p.FeeGrowthInside0LastX128) &&
		bigIntEqual(latest.feeGrowthInside1LastX128, p.FeeGrowthInside1LastX128)
}
