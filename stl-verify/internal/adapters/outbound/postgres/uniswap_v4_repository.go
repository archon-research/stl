package postgres

import (
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

// Compile-time check that UniswapV4Repository implements outbound.UniswapV4Repository.
var _ outbound.UniswapV4Repository = (*UniswapV4Repository)(nil)

// nativeETHPlaceholder is the token row a currency of address(0) must resolve
// to. address(0) is NOT usable as native ETH here: it already exists in the
// token registry as a distinct "no token" sentinel (empty symbol, 0 decimals)
// written by another worker, so reusing it would silently scale ETH amounts by
// 10^0. 0xEeee… is the registry's ETH row (curve_pool_coin uses the same one).
var nativeETHPlaceholder = common.HexToAddress("0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE")

// UniswapV4Repository is a PostgreSQL implementation of the outbound.UniswapV4Repository port.
type UniswapV4Repository struct {
	pool    *pgxpool.Pool
	buildID buildregistry.BuildID
}

// NewUniswapV4Repository creates a new PostgreSQL Uniswap V4 repository.
func NewUniswapV4Repository(pool *pgxpool.Pool, buildID buildregistry.BuildID) *UniswapV4Repository {
	return &UniswapV4Repository{pool: pool, buildID: buildID}
}

// loadUniswapV4PoolsSQL reads the current registry for one chain. Both registry
// tables are append-only version histories, so "current" is the highest
// processing_version per natural key: DISTINCT ON (pool_id) for the pools, and
// the single newest uniswap_v4_pool_manager row for the chain. The PoolManager
// address is the FK'd protocol row's, never a column of its own.
//
// Every join to the right of the pool subquery is LEFT so a registry defect
// surfaces as a named error from the scan rather than as a silently missing
// pool: no manager row, or a currency whose token_id belongs to another chain,
// would otherwise drop the pool and leave the indexer looking healthy.
const loadUniswapV4PoolsSQL = `
	SELECT p.id, m.protocol_id, m.pool_manager_address, m.state_view_address,
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
	    SELECT mgr.protocol_id, pr.address AS pool_manager_address, mgr.state_view_address
	    FROM uniswap_v4_pool_manager mgr
	    JOIN protocol pr ON pr.id = mgr.protocol_id
	    WHERE mgr.chain_id = $1
	    ORDER BY mgr.processing_version DESC
	    LIMIT 1
	) m ON TRUE
	LEFT JOIN token t0 ON t0.id = p.currency0_token_id AND t0.chain_id = $1
	LEFT JOIN token t1 ON t1.id = p.currency1_token_id AND t1.chain_id = $1
	ORDER BY p.id`

// LoadPools returns the current version of every registered pool on chainID,
// with the chain's current PoolManager / StateView addresses and the currency
// decimals. Every registry defect the port documents (a missing PoolManager,
// NULL decimals, a currency that disagrees with its token row) is an error
// rather than a skipped pool.
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
		protocolID             *int64
		poolManager, stateView []byte
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
	if err := rows.Scan(&id, &protocolID, &poolManager, &stateView,
		&onchainPoolID, &currency0, &currency1,
		&token0, &decimals0, &token1, &decimals1,
		&fee, &tickSpacing, &hooks, &deployBlock, &snapshotSupported); err != nil {
		return row, fmt.Errorf("scanning uniswap_v4 pool row: %w", err)
	}
	if protocolID == nil {
		return row, fmt.Errorf("chain %d has uniswap_v4 pools (e.g. %d) but no uniswap_v4_pool_manager row", chainID, id)
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

// currencyTokenDecimals returns the decimals of the token row a currency
// resolves to, rejecting a registry row whose token is absent from this chain,
// is not that currency (or, for native ETH, not the placeholder), or has NULL
// decimals. token is the raw column so an absent row is distinguishable from
// the zero address, which is itself a meaningful currency value.
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

// v4StateConverted holds pre-converted numeric values for a uniswap_v4_pool_state insert.
type v4StateConverted struct {
	s                *entity.UniswapV4PoolState
	sqrtPriceX96     pgtype.Numeric
	liquidity        pgtype.Numeric
	feeGrowthGlobal0 pgtype.Numeric
	feeGrowthGlobal1 pgtype.Numeric
}

// v4SwapConverted holds pre-converted numeric values for a uniswap_v4_swap insert.
type v4SwapConverted struct {
	s            *entity.UniswapV4Swap
	amount0      pgtype.Numeric
	amount1      pgtype.Numeric
	sqrtPriceX96 pgtype.Numeric
	liquidity    pgtype.Numeric
}

// v4LiquidityEventConverted holds pre-converted numeric values for a
// uniswap_v4_liquidity_event insert.
type v4LiquidityEventConverted struct {
	e              *entity.UniswapV4LiquidityEvent
	liquidityDelta pgtype.Numeric
}

// v4TickConverted holds pre-converted numeric values for a uniswap_v4_tick insert.
type v4TickConverted struct {
	liquidityGross        pgtype.Numeric
	liquidityNet          pgtype.Numeric
	feeGrowthOutside0X128 pgtype.Numeric
	feeGrowthOutside1X128 pgtype.Numeric
}

// SaveBlock persists all of a block's uniswap_v4 rows in one pgx.Batch within
// tx, except ticks (append-on-change, see writeTicks), and returns the count of
// state rows inserted.
func (r *UniswapV4Repository) SaveBlock(ctx context.Context, tx pgx.Tx, w outbound.UniswapV4BlockWrites) (stateRows int64, err error) {
	rows, err := convertV4BlockWrites(w)
	if err != nil {
		return 0, err
	}

	batch := &pgx.Batch{}
	queueUniswapV4Batch(batch, rows, r.buildID)

	stateRows, err = sendUniswapV4Batch(ctx, tx, batch, rows)
	if err != nil {
		return stateRows, err
	}

	// pgx forbids new queries while a batch result reader is open, and each tick
	// insert depends on a prior read of its slot (see writeTicks).
	if err := r.writeTicks(ctx, tx, w.Ticks); err != nil {
		return stateRows, err
	}

	return stateRows, nil
}

// poolIDsWithStateAtBlockSQL resolves each state row's pool through the
// registry twice over. uniswap_v4_pool_state carries no chain_id, so the
// worker's chain filter has to come from the pool row the fact FKs; and a
// registry correction mints a new surrogate while the old fact rows keep the
// old one, so uniswap_v4_pool_current maps that superseded id forward to the
// current version of its (chain_id, pool_id) natural key — the only id the
// caller's in-memory registry knows.
//
// The block_timestamp band is what lets TimescaleDB exclude chunks: filtering
// on block_number alone scans every chunk of the hypertable on each reorg
// (VEC-541). One day either side is far wider than any block's own timestamp
// needs and still prunes to a couple of chunks.
const poolIDsWithStateAtBlockSQL = `
	SELECT DISTINCT cur.id
	FROM uniswap_v4_pool_state s
	JOIN uniswap_v4_pool p ON p.id = s.pool_id
	JOIN uniswap_v4_pool_current cur
	  ON cur.chain_id = p.chain_id AND cur.pool_id = p.pool_id
	WHERE p.chain_id = $1
	  AND s.block_number = $2
	  AND s.block_timestamp BETWEEN $3::timestamptz - INTERVAL '1 day'
	                            AND $3::timestamptz + INTERVAL '1 day'
	ORDER BY cur.id`

// PoolIDsWithStateAtBlock returns the current registry ids of the pools on
// chainID that already have a uniswap_v4_pool_state row at blockNumber,
// ascending. A reorg redelivery unions them into its due set so the orphaned
// fork's snapshot is superseded even when the in-memory tracker was lost to a
// restart. Queries the connection pool directly (committed rows), not a write
// transaction.
func (r *UniswapV4Repository) PoolIDsWithStateAtBlock(ctx context.Context, chainID int64, blockNumber int64, blockTimestamp time.Time) ([]int64, error) {
	rows, err := r.pool.Query(ctx, poolIDsWithStateAtBlockSQL, chainID, blockNumber, blockTimestamp)
	if err != nil {
		return nil, fmt.Errorf("querying pools with state at block %d: %w", blockNumber, err)
	}
	defer rows.Close()

	var poolIDs []int64
	for rows.Next() {
		var poolID int64
		if err := rows.Scan(&poolID); err != nil {
			return nil, fmt.Errorf("scanning pool id with state at block %d: %w", blockNumber, err)
		}
		poolIDs = append(poolIDs, poolID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating pools with state at block %d: %w", blockNumber, err)
	}
	return poolIDs, nil
}

// TicksForPoolAtBlock returns the distinct tick positions with a row for pool at
// blockNumber, ascending. A reorg redelivery uses this to re-read exactly the
// ticks a prior version wrote at this height and supersede them at the new
// version. Queries the connection pool directly (committed rows), not a write
// transaction.
func (r *UniswapV4Repository) TicksForPoolAtBlock(ctx context.Context, poolID int64, blockNumber int64) ([]int32, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT DISTINCT tick FROM uniswap_v4_tick
		 WHERE pool_id = $1 AND block_number = $2
		 ORDER BY tick`,
		poolID, blockNumber,
	)
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

// v4BatchRows is one block's converted rows in the canonical batch order that
// sendUniswapV4Batch drains them in.
type v4BatchRows struct {
	states     []v4StateConverted
	swaps      []v4SwapConverted
	liqs       []v4LiquidityEventConverted
	poolEvents []*entity.UniswapV4PoolEvent
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
	return v4BatchRows{states: states, swaps: swaps, liqs: liqs, poolEvents: w.PoolEvents}, nil
}

// v4BatchSection names one contiguous run of queued statements so the drain can
// walk the batch without re-listing every slice.
type v4BatchSection struct {
	name            string
	count           int
	countsStateRows bool
}

// sections must stay in the order queueUniswapV4Batch queues them: pgx returns
// batch results positionally, so a reordering here silently mis-attributes both
// the row counts and the error messages.
func (rows v4BatchRows) sections() []v4BatchSection {
	return []v4BatchSection{
		{name: "state", count: len(rows.states), countsStateRows: true},
		{name: "swap", count: len(rows.swaps)},
		{name: "liquidity event", count: len(rows.liqs)},
		{name: "pool event", count: len(rows.poolEvents)},
	}
}

func queueUniswapV4Batch(batch *pgx.Batch, rows v4BatchRows, buildID buildregistry.BuildID) {
	queueV4States(batch, rows.states, buildID)
	queueV4Swaps(batch, rows.swaps, buildID)
	queueV4LiquidityEvents(batch, rows.liqs, buildID)
	queueV4PoolEvents(batch, rows.poolEvents, buildID)
}

func queueV4States(batch *pgx.Batch, states []v4StateConverted, buildID buildregistry.BuildID) {
	for _, c := range states {
		s := c.s
		batch.Queue(
			`INSERT INTO uniswap_v4_pool_state
			   (pool_id, block_number, block_version, block_timestamp,
			    sqrt_price_x96, tick, protocol_fee, lp_fee, liquidity,
			    fee_growth_global0_x128, fee_growth_global1_x128, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
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
			    sqrt_price_x96, liquidity, tick, fee, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
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
			    liquidity_delta, salt, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
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
			    tx_hash, log_index, event_name, params, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			 ON CONFLICT (pool_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
			e.PoolID, e.BlockNumber, e.BlockVersion, e.BlockTimestamp,
			e.TxHash.Bytes(), e.LogIndex, string(e.EventName), []byte(e.Params), int(buildID),
		)
	}
}

// sendUniswapV4Batch executes the queued batch and drains every result in queue
// order, returning the count of state rows inserted. The batch reader is always
// closed before returning so the caller may issue further queries on tx
// (writeTicks runs after this).
func sendUniswapV4Batch(ctx context.Context, tx pgx.Tx, batch *pgx.Batch, rows v4BatchRows) (stateRows int64, err error) {
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
				stateRows += tag.RowsAffected()
			}
		}
	}

	return stateRows, nil
}

// writeTicks persists the append-on-change uniswap_v4_tick rows. It locks every
// affected (pool_id, tick) slot (sorted ascending) so the SELECT-latest then
// INSERT decision is serialized against concurrent writers, then writes a new
// row only when no prior row exists, any field differs from the latest one, or
// block_version differs (a reorg re-observation must always insert, even with
// identical values).
//
// Each phase is one round-trip for the whole tick set — locks in a single
// unnest() Exec, latest-row reads in a single DISTINCT ON query, inserts in a
// single pgx.Batch — because a dense pool's first touch covers O(10³) ticks
// and per-tick round-trips would hold the advisory locks for all of them.
//
// Ticks in one block are deduplicated per (pool_id, tick) upstream, so each key
// appears at most once here and needs no in-batch same-key sequencing.
func (r *UniswapV4Repository) writeTicks(ctx context.Context, tx pgx.Tx, ticks []*entity.UniswapV4Tick) error {
	if len(ticks) == 0 {
		return nil
	}

	blockNumber, err := sharedBlockNumber(ticks)
	if err != nil {
		return err
	}

	keys := distinctSortedV4TickKeys(ticks)
	if err := lockTickKeysV4(ctx, tx, keys); err != nil {
		return err
	}

	latest, err := readLatestTicksV4(ctx, tx, keys, blockNumber)
	if err != nil {
		return err
	}

	return r.insertChangedTicksV4(ctx, tx, ticks, latest)
}

// sharedBlockNumber returns the block every tick in the write belongs to. One
// SaveBlock is one block, and readLatestTicksV4 compares against rows at or
// below that height, so a mixed batch would silently compare across blocks.
func sharedBlockNumber(ticks []*entity.UniswapV4Tick) (int64, error) {
	blockNumber := ticks[0].BlockNumber
	for _, t := range ticks[1:] {
		if t.BlockNumber != blockNumber {
			return 0, fmt.Errorf("uniswap_v4 tick write spans blocks %d and %d: one SaveBlock is one block", blockNumber, t.BlockNumber)
		}
	}
	return blockNumber, nil
}

// lockTickKeysV4 acquires the per-slot advisory lock for every key in one
// round-trip via unnest(). keys must already be in the canonical sorted order
// (distinctSortedV4TickKeys) so concurrent SaveBlock transactions touching
// overlapping ticks acquire overlapping locks in the same order and never
// deadlock (db/migrations/AGENTS.md read-then-write rule).
//
// The "uniswap_v4_tick|<pool>|<tick>" lock domain is deliberately distinct from
// the pv-trigger's row-identity key ("u4t|pool|tick|block|version"): this guards
// the app-level read-latest-then-insert decision, the trigger guards
// processing_version assignment. They must not be harmonized.
func lockTickKeysV4(ctx context.Context, tx pgx.Tx, keys []v4TickKey) error {
	lockKeys := make([]string, len(keys))
	for i, k := range keys {
		lockKeys[i] = fmt.Sprintf("uniswap_v4_tick|%d|%d", k.poolID, k.tick)
	}
	// pg_advisory_xact_lock is acquired left-to-right as unnest() yields rows,
	// preserving the sorted-key lock order.
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended(k, 0))
		 FROM unnest($1::text[]) WITH ORDINALITY AS u(k, ord)
		 ORDER BY ord`,
		lockKeys,
	); err != nil {
		return fmt.Errorf("locking %d uniswap_v4 tick slots: %w", len(keys), err)
	}
	return nil
}

// readLatestTicksV4 fetches the latest row per (pool_id, tick) at or below
// blockNumber for every key in one query. Keys with no prior row are absent
// from the returned map, which the caller treats as "insert unconditionally".
// The height bound keeps an out-of-order (backfill) write from being compared
// against — and silently dropped as unchanged against — a newer row.
func readLatestTicksV4(ctx context.Context, tx pgx.Tx, keys []v4TickKey, blockNumber int64) (map[v4TickKey]v4TickValues, error) {
	poolIDs := make([]int64, len(keys))
	tickNums := make([]int32, len(keys))
	for i, k := range keys {
		poolIDs[i] = k.poolID
		tickNums[i] = int32(k.tick)
	}

	rows, err := tx.Query(ctx,
		`SELECT DISTINCT ON (t.pool_id, t.tick)
		        t.pool_id, t.tick, t.block_version,
		        t.liquidity_gross, t.liquidity_net,
		        t.fee_growth_outside0_x128, t.fee_growth_outside1_x128
		 FROM uniswap_v4_tick t
		 JOIN unnest($1::bigint[], $2::int[]) AS k(pool_id, tick)
		   ON t.pool_id = k.pool_id AND t.tick = k.tick
		 WHERE t.block_number <= $3
		 ORDER BY t.pool_id, t.tick,
		          t.block_number DESC, t.block_version DESC, t.processing_version DESC`,
		poolIDs, tickNums, blockNumber,
	)
	if err != nil {
		return nil, fmt.Errorf("querying latest uniswap_v4 ticks for %d slots: %w", len(keys), err)
	}
	defer rows.Close()

	latest := make(map[v4TickKey]v4TickValues, len(keys))
	for rows.Next() {
		var (
			poolID                int64
			tick                  int
			blockVersion          int
			liquidityGross        pgtype.Numeric
			liquidityNet          pgtype.Numeric
			feeGrowthOutside0X128 pgtype.Numeric
			feeGrowthOutside1X128 pgtype.Numeric
		)
		if err := rows.Scan(&poolID, &tick, &blockVersion,
			&liquidityGross, &liquidityNet, &feeGrowthOutside0X128, &feeGrowthOutside1X128); err != nil {
			return nil, fmt.Errorf("scanning latest uniswap_v4 tick row: %w", err)
		}
		values, convErr := toV4TickValues(blockVersion, liquidityGross, liquidityNet,
			feeGrowthOutside0X128, feeGrowthOutside1X128)
		if convErr != nil {
			return nil, fmt.Errorf("reading latest uniswap_v4 tick for pool=%d tick=%d: %w", poolID, tick, convErr)
		}
		latest[v4TickKey{poolID: poolID, tick: tick}] = values
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating latest uniswap_v4 ticks: %w", err)
	}
	return latest, nil
}

// insertChangedTicksV4 queues an INSERT for every tick whose latest row is
// absent (no prior row) or differs from it, then sends them in one pgx.Batch.
// The INSERTs run through the table's BEFORE INSERT ROW trigger, so the per-row
// processing_version assignment happens exactly as for a single-row insert.
func (r *UniswapV4Repository) insertChangedTicksV4(
	ctx context.Context, tx pgx.Tx,
	ticks []*entity.UniswapV4Tick,
	latest map[v4TickKey]v4TickValues,
) (err error) {
	batch := &pgx.Batch{}
	var queued int
	for i, t := range ticks {
		prior, hasPrior := latest[v4TickKey{poolID: t.PoolID, tick: t.Tick}]
		if hasPrior && v4TickUnchanged(prior, t) {
			continue
		}
		converted, convErr := convertV4Tick(t)
		if convErr != nil {
			return fmt.Errorf("tick %d: converting tick pool=%d tick=%d: %w", i, t.PoolID, t.Tick, convErr)
		}
		batch.Queue(
			`INSERT INTO uniswap_v4_tick
			   (pool_id, tick, block_number, block_version, block_timestamp,
			    liquidity_gross, liquidity_net, fee_growth_outside0_x128,
			    fee_growth_outside1_x128, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			 ON CONFLICT (pool_id, tick, block_number, block_version, processing_version) DO NOTHING`,
			t.PoolID, t.Tick, t.BlockNumber, t.BlockVersion, t.BlockTimestamp,
			converted.liquidityGross, converted.liquidityNet,
			converted.feeGrowthOutside0X128, converted.feeGrowthOutside1X128, int(r.buildID),
		)
		queued++
	}

	if queued == 0 {
		return nil
	}

	br := tx.SendBatch(ctx, batch)
	defer func() {
		if closeErr := br.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing uniswap_v4 tick batch: %w", closeErr))
		}
	}()
	for i := range queued {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("inserting uniswap_v4 tick batch entry %d: %w", i, err)
		}
	}
	return nil
}

type v4TickKey struct {
	poolID int64
	tick   int
}

// distinctSortedV4TickKeys returns the affected slots in the canonical lock
// order, so concurrent SaveBlock transactions touching overlapping ticks never
// deadlock.
func distinctSortedV4TickKeys(ticks []*entity.UniswapV4Tick) []v4TickKey {
	seen := make(map[v4TickKey]struct{}, len(ticks))
	for _, t := range ticks {
		seen[v4TickKey{poolID: t.PoolID, tick: t.Tick}] = struct{}{}
	}
	keys := make([]v4TickKey, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b v4TickKey) int {
		if a.poolID != b.poolID {
			return int(a.poolID - b.poolID)
		}
		return a.tick - b.tick
	})
	return keys
}

func convertV4Tick(t *entity.UniswapV4Tick) (v4TickConverted, error) {
	liquidityGross, err := BigIntToNumericRequired(t.LiquidityGross, "liquidity_gross")
	if err != nil {
		return v4TickConverted{}, err
	}
	liquidityNet, err := BigIntToNumericRequired(t.LiquidityNet, "liquidity_net")
	if err != nil {
		return v4TickConverted{}, err
	}
	feeGrowthOutside0X128, err := BigIntToNumericRequired(t.FeeGrowthOutside0X128, "fee_growth_outside0_x128")
	if err != nil {
		return v4TickConverted{}, err
	}
	feeGrowthOutside1X128, err := BigIntToNumericRequired(t.FeeGrowthOutside1X128, "fee_growth_outside1_x128")
	if err != nil {
		return v4TickConverted{}, err
	}
	return v4TickConverted{
		liquidityGross: liquidityGross, liquidityNet: liquidityNet,
		feeGrowthOutside0X128: feeGrowthOutside0X128, feeGrowthOutside1X128: feeGrowthOutside1X128,
	}, nil
}

// v4TickValues holds the latest tick row's fields decoded to *big.Int for
// comparison against a candidate entity.UniswapV4Tick.
type v4TickValues struct {
	blockVersion          int
	liquidityGross        *big.Int
	liquidityNet          *big.Int
	feeGrowthOutside0X128 *big.Int
	feeGrowthOutside1X128 *big.Int
}

func toV4TickValues(
	blockVersion int,
	liquidityGross, liquidityNet, feeGrowthOutside0X128, feeGrowthOutside1X128 pgtype.Numeric,
) (v4TickValues, error) {
	var v v4TickValues
	var err error
	if v.liquidityGross, err = NumericToNullableBigInt(liquidityGross); err != nil {
		return v, fmt.Errorf("liquidity_gross: %w", err)
	}
	if v.liquidityNet, err = NumericToNullableBigInt(liquidityNet); err != nil {
		return v, fmt.Errorf("liquidity_net: %w", err)
	}
	if v.feeGrowthOutside0X128, err = NumericToNullableBigInt(feeGrowthOutside0X128); err != nil {
		return v, fmt.Errorf("fee_growth_outside0_x128: %w", err)
	}
	if v.feeGrowthOutside1X128, err = NumericToNullableBigInt(feeGrowthOutside1X128); err != nil {
		return v, fmt.Errorf("fee_growth_outside1_x128: %w", err)
	}
	v.blockVersion = blockVersion
	return v, nil
}

// v4TickUnchanged reports whether the latest tick row already reflects t: same
// block_version (a different version means a reorg re-observation, which must
// always insert regardless of value equality) and every field equal.
func v4TickUnchanged(latest v4TickValues, t *entity.UniswapV4Tick) bool {
	return latest.blockVersion == t.BlockVersion &&
		bigIntEqual(latest.liquidityGross, t.LiquidityGross) &&
		bigIntEqual(latest.liquidityNet, t.LiquidityNet) &&
		bigIntEqual(latest.feeGrowthOutside0X128, t.FeeGrowthOutside0X128) &&
		bigIntEqual(latest.feeGrowthOutside1X128, t.FeeGrowthOutside1X128)
}
