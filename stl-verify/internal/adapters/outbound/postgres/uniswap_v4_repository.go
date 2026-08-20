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

// SaveBlock persists all of a block's uniswap_v4 rows in one pgx.Batch within
// tx, except ticks and positions (append-on-change, see uniswapTickWriter and
// writePositions), and returns the count of state rows inserted.
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

	// pgx forbids new queries while a batch result reader is open, and each
	// append-on-change insert depends on a prior read of its slot (see
	// uniswapTickWriter). Ticks before positions is a fixed order: the two lock
	// domains are disjoint, so a varying phase order would deadlock concurrent
	// writers across them.
	if err := uniswapV4TickWriter.writeTicks(ctx, tx, uniswapV4TickRows(w.Ticks), r.buildID); err != nil {
		return stateRows, err
	}
	if err := r.writePositions(ctx, tx, w.Positions); err != nil {
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

// poolIDsEverSnapshottedSQL asks, per registered pool, whether any fact row was
// ever written for it. "Ever" admits no block_timestamp bound, so unlike
// poolIDsWithStateAtBlockSQL this cannot prune chunks (VEC-541) and probes every
// chunk of the uniswap_v4_pool_state hypertable for a pool that has none. That
// is affordable only because it runs once per process boot over a registry of
// tens of pools — do not reuse it on a per-block path.
//
// The uniswap_v4_tick side is exact (a plain table, never tiered); the
// pool_state side stops at the 1-year tiering policy, since
// timescaledb.enable_tiered_reads defaults off. So "ever" is only literally
// true for a pool that has ever had an initialized tick — which is every pool
// that has ever held liquidity.
const poolIDsEverSnapshottedSQL = `
	SELECT DISTINCT cur.id
	FROM uniswap_v4_pool p
	JOIN uniswap_v4_pool_current cur
	  ON cur.chain_id = p.chain_id AND cur.pool_id = p.pool_id
	WHERE p.chain_id = $1
	  AND (EXISTS (SELECT 1 FROM uniswap_v4_tick t WHERE t.pool_id = p.id)
	       OR EXISTS (SELECT 1 FROM uniswap_v4_pool_state s WHERE s.pool_id = p.id))
	ORDER BY cur.id`

// PoolIDsEverSnapshotted queries the connection pool directly (committed rows),
// not a write transaction.
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

// uniswapV4TickRows maps the V4 entities onto the shared writer's row shape.
// The V4 table has no initialized column, so the field stays unset.
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

// PositionsForPoolAtBlock returns the distinct position keys with a row for pool
// at blockNumber, in entity.UniswapV4PositionKey.Compare order. A reorg
// redelivery uses this to re-read exactly the positions a prior version wrote at
// this height and supersede them at the new version. Queries the connection pool
// directly (committed rows), not a write transaction.
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
		// The key leaves here straight into an int24 ABI argument, so a corrupt
		// stored row must stop the read rather than pack silently out of range.
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

// sharedBlockNumber returns the block every row in an append-on-change write
// belongs to. One SaveBlock is one block, and the read-latest queries compare
// against rows at or below that height, so a mixed batch would silently compare
// across blocks. rows must be non-empty.
func sharedBlockNumber[T any](kind string, rows []T, blockNumberOf func(T) int64) (int64, error) {
	blockNumber := blockNumberOf(rows[0])
	for _, row := range rows[1:] {
		if got := blockNumberOf(row); got != blockNumber {
			return 0, fmt.Errorf("uniswap_v4 %s write spans blocks %d and %d: one SaveBlock is one block", kind, blockNumber, got)
		}
	}
	return blockNumber, nil
}

// writePositions persists the append-on-change uniswap_v4_position rows,
// mirroring uniswapTickWriter: lock every affected slot in the canonical sorted
// order, read the latest row per slot in one query, then insert only where no
// prior row exists or v4PositionUnchanged says the stored row does not already
// reflect it.
func (r *UniswapV4Repository) writePositions(ctx context.Context, tx pgx.Tx, positions []*entity.UniswapV4Position) error {
	if len(positions) == 0 {
		return nil
	}

	blockNumber, err := sharedBlockNumber("position", positions, func(p *entity.UniswapV4Position) int64 { return p.BlockNumber })
	if err != nil {
		return err
	}

	keys := distinctSortedV4PositionKeys(positions)
	// insertChangedPositionsV4 walks positions, not keys: a duplicate slot would
	// compare both rows against the same prior state and let ON CONFLICT DO
	// NOTHING silently drop the second one's values.
	if len(keys) != len(positions) {
		return fmt.Errorf("uniswap_v4 position write has %d rows for %d distinct slots: one block must touch a position once", len(positions), len(keys))
	}

	if err := lockPositionKeysV4(ctx, tx, keys); err != nil {
		return err
	}

	latest, err := readLatestPositionsV4(ctx, tx, keys, blockNumber)
	if err != nil {
		return err
	}

	return r.insertChangedPositionsV4(ctx, tx, positions, latest)
}

type v4PositionKey struct {
	poolID int64
	key    entity.UniswapV4PositionKey
}

// distinctSortedV4PositionKeys returns the affected slots in the canonical lock
// order, so concurrent SaveBlock transactions touching overlapping positions
// never deadlock.
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

// lockPositionKeysV4 acquires the per-slot advisory lock for every key in one
// round-trip via unnest(). keys must already be in the canonical sorted order
// (distinctSortedV4PositionKeys) so concurrent SaveBlock transactions touching
// overlapping positions acquire overlapping locks in the same order and never
// deadlock (db/migrations/AGENTS.md read-then-write rule).
//
// The "uniswap_v4_position|…" lock domain is deliberately distinct from the
// pv-trigger's row-identity key ("u4pos|…|block|version"): this guards the
// app-level read-latest-then-insert decision, the trigger guards
// processing_version assignment. They must not be harmonized.
func lockPositionKeysV4(ctx context.Context, tx pgx.Tx, keys []v4PositionKey) error {
	lockKeys := make([]string, len(keys))
	for i, k := range keys {
		lockKeys[i] = fmt.Sprintf("uniswap_v4_position|%d|%s|%d|%d|%s",
			k.poolID, k.key.Owner.Hex(), k.key.TickLower, k.key.TickUpper, k.key.Salt.Hex())
	}
	// pg_advisory_xact_lock is acquired left-to-right as unnest() yields rows,
	// preserving the sorted-key lock order.
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

// positionKeyArrays splays the slots into the five parallel arrays the unnest()
// join takes, one per natural-key column.
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

// readLatestPositionsV4 fetches the latest row per slot at or below blockNumber
// for every key in one query. Keys with no prior row are absent from the
// returned map, which the caller treats as "insert unconditionally". The height
// bound keeps an out-of-order (backfill) write from being compared against — and
// silently dropped as unchanged against — a newer row.
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

// insertChangedPositionsV4 queues an INSERT for every position whose latest row
// is absent (no prior row) or differs from it, then sends them in one pgx.Batch.
// The INSERTs run through the table's BEFORE INSERT ROW trigger, so the per-row
// processing_version assignment happens exactly as for a single-row insert.
func (r *UniswapV4Repository) insertChangedPositionsV4(
	ctx context.Context, tx pgx.Tx,
	positions []*entity.UniswapV4Position,
	latest map[v4PositionKey]v4PositionValues,
) (err error) {
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
			return fmt.Errorf("position %d: converting pool=%d %+v: %w", i, p.PoolID, slot.key, convErr)
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
		return nil
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
			return fmt.Errorf("inserting uniswap_v4 position pool=%d %+v at block %d: %w", q.slot.poolID, q.slot.key, q.blockNumber, execErr)
		}
		if err := q.assertInserted(tag.RowsAffected()); err != nil {
			return err
		}
	}
	return nil
}

// v4QueuedPosition remembers which slot a queued INSERT belongs to, so a driver
// error or a discarded row names the position instead of a batch ordinal.
type v4QueuedPosition struct {
	slot        v4PositionKey
	blockNumber int64
	// supersedesRow marks an insert queued because the values differ from a row
	// at the SAME (block_number, block_version): the one case where the insert
	// cannot legitimately be discarded.
	supersedesRow bool
}

// assertInserted rejects the one way ON CONFLICT DO NOTHING can hide a real
// disagreement: the same block hash read back different values than the row
// already stored for it. Every other zero-row outcome is a legitimate replay of
// an older version alongside a newer one, so only supersedesRow is checked.
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

// v4PositionValues holds the latest position row's fields decoded to *big.Int
// for comparison against a candidate entity.UniswapV4Position.
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
// always append, even with identical values; across heights the two versions
// count different blocks' reorgs and comparing them would append a row claiming
// a change the chain never made, so only the values decide.
func v4PositionUnchanged(latest v4PositionValues, p *entity.UniswapV4Position) bool {
	if latest.blockNumber == p.BlockNumber && latest.blockVersion != p.BlockVersion {
		return false
	}
	return bigIntEqual(latest.liquidity, p.Liquidity) &&
		bigIntEqual(latest.feeGrowthInside0LastX128, p.FeeGrowthInside0LastX128) &&
		bigIntEqual(latest.feeGrowthInside1LastX128, p.FeeGrowthInside1LastX128)
}
