package postgres

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that UniswapV3Repository implements outbound.UniswapV3Repository.
var _ outbound.UniswapV3Repository = (*UniswapV3Repository)(nil)

// UniswapV3Repository is a PostgreSQL implementation of the outbound.UniswapV3Repository port.
type UniswapV3Repository struct {
	pool    *pgxpool.Pool
	buildID buildregistry.BuildID
}

// NewUniswapV3Repository creates a new PostgreSQL Uniswap V3 repository.
func NewUniswapV3Repository(pool *pgxpool.Pool, buildID buildregistry.BuildID) *UniswapV3Repository {
	return &UniswapV3Repository{pool: pool, buildID: buildID}
}

// LoadPools returns all pools for the given chain with their token addresses
// and decimals. Errors on any row with a NULL deploy_block: unlike Curve
// (which backfills deploy height after registration), uniswap_v3_pool is
// migration-seeded with deploy_block always populated, so a NULL here means
// the reorg deploy-gate would be defeated and the row must not be used.
func (r *UniswapV3Repository) LoadPools(ctx context.Context, chainID int64) ([]outbound.UniswapV3PoolRow, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT p.id, p.protocol_id, p.pool_address, t0.address, t1.address,
		        t0.decimals, t1.decimals, p.fee, p.tick_spacing, p.deploy_block
		 FROM uniswap_v3_pool p
		 JOIN token t0 ON t0.id = p.token0_id
		 JOIN token t1 ON t1.id = p.token1_id
		 WHERE p.chain_id = $1
		 ORDER BY p.id`,
		chainID,
	)
	if err != nil {
		return nil, fmt.Errorf("querying uniswap_v3 pools: %w", err)
	}
	defer rows.Close()

	var result []outbound.UniswapV3PoolRow
	for rows.Next() {
		var (
			poolID                         int64
			protocolID                     int64
			poolAddress, token0, token1    []byte
			token0Decimals, token1Decimals int
			fee, tickSpacing               int
			deployBlock                    *int64
		)
		if err := rows.Scan(&poolID, &protocolID, &poolAddress, &token0, &token1,
			&token0Decimals, &token1Decimals, &fee, &tickSpacing, &deployBlock); err != nil {
			return nil, fmt.Errorf("scanning uniswap_v3 pool row: %w", err)
		}
		if deployBlock == nil {
			return nil, fmt.Errorf("uniswap_v3 pool %d has NULL deploy_block: defeats the reorg deploy-gate", poolID)
		}
		result = append(result, outbound.UniswapV3PoolRow{
			ID:             poolID,
			ProtocolID:     protocolID,
			Address:        common.BytesToAddress(poolAddress),
			Token0:         common.BytesToAddress(token0),
			Token1:         common.BytesToAddress(token1),
			Token0Decimals: token0Decimals,
			Token1Decimals: token1Decimals,
			Fee:            fee,
			TickSpacing:    tickSpacing,
			DeployBlock:    *deployBlock,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating uniswap_v3 pools: %w", err)
	}
	return result, nil
}

// stateConverted holds pre-converted numeric values for a uniswap_v3_pool_state insert.
type stateConverted struct {
	s                  *entity.UniswapV3PoolState
	sqrtPriceX96       pgtype.Numeric
	liquidity          pgtype.Numeric
	feeGrowthGlobal0   pgtype.Numeric
	feeGrowthGlobal1   pgtype.Numeric
	protocolFeesToken0 pgtype.Numeric
	protocolFeesToken1 pgtype.Numeric
	balance0           pgtype.Numeric
	balance1           pgtype.Numeric
}

// swapConvertedV3 holds pre-converted numeric values for a uniswap_v3_swap insert.
type swapConvertedV3 struct {
	s            *entity.UniswapV3Swap
	amount0      pgtype.Numeric
	amount1      pgtype.Numeric
	sqrtPriceX96 pgtype.Numeric
	liquidity    pgtype.Numeric
}

// liquidityEventConverted holds pre-converted numeric values for a
// uniswap_v3_liquidity_event insert.
type liquidityEventConverted struct {
	e       *entity.UniswapV3LiquidityEvent
	amount  pgtype.Numeric // nullable: absent for collect
	amount0 pgtype.Numeric
	amount1 pgtype.Numeric
}

// tickConverted holds pre-converted numeric values for a uniswap_v3_tick insert.
type tickConverted struct {
	t                     *entity.UniswapV3Tick
	liquidityGross        pgtype.Numeric
	liquidityNet          pgtype.Numeric
	feeGrowthOutside0X128 pgtype.Numeric
	feeGrowthOutside1X128 pgtype.Numeric
}

// SaveBlock persists all of a block's uniswap_v3 rows in one pgx.Batch within
// tx, except ticks (append-on-change, see writeTicks), and returns the count
// of state rows inserted.
func (r *UniswapV3Repository) SaveBlock(ctx context.Context, tx pgx.Tx, w outbound.UniswapV3BlockWrites) (stateRows int64, err error) {
	states, err := convertStates(w.States)
	if err != nil {
		return 0, err
	}
	swaps, err := convertSwapsV3(w.Swaps)
	if err != nil {
		return 0, err
	}
	liqs, err := convertLiquidityEvents(w.LiquidityEvents)
	if err != nil {
		return 0, err
	}

	batch := &pgx.Batch{}
	queueUniswapV3Batch(batch, states, swaps, liqs, w.PoolEvents, r.buildID)

	stateRows, err = sendUniswapV3Batch(ctx, tx, batch, states, swaps, liqs, w.PoolEvents)
	if err != nil {
		return stateRows, err
	}

	// Ticks are written after the batch reader is fully closed: pgx forbids
	// issuing new queries on a connection while a batch result reader is open,
	// and each tick insert depends on a prior read of the latest row for that
	// (pool_id, tick) slot (a read-then-write race ON CONFLICT cannot guard,
	// ADR-0002 §3).
	if err := r.writeTicks(ctx, tx, w.Ticks); err != nil {
		return stateRows, err
	}

	return stateRows, nil
}

// TicksForPoolAtBlock returns the distinct tick positions with a row for pool at
// blockNumber, ascending. A reorg redelivery uses this to re-read exactly the
// ticks a prior version wrote at this height and supersede them at the new
// version (VEC-487). Queries the connection pool directly (committed rows), not
// a write transaction.
func (r *UniswapV3Repository) TicksForPoolAtBlock(ctx context.Context, poolID int64, blockNumber int64) ([]int32, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT DISTINCT tick FROM uniswap_v3_tick
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

func convertStates(states []*entity.UniswapV3PoolState) ([]stateConverted, error) {
	out := make([]stateConverted, 0, len(states))
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
		protocolFeesToken0, convErr := BigIntToNumericRequired(s.ProtocolFeesToken0, "protocol_fees_token0")
		if convErr != nil {
			return nil, fmt.Errorf("state %d converting protocol_fees_token0: %w", i, convErr)
		}
		protocolFeesToken1, convErr := BigIntToNumericRequired(s.ProtocolFeesToken1, "protocol_fees_token1")
		if convErr != nil {
			return nil, fmt.Errorf("state %d converting protocol_fees_token1: %w", i, convErr)
		}
		balance0, convErr := BigIntToNumericRequired(s.Balance0, "balance0")
		if convErr != nil {
			return nil, fmt.Errorf("state %d converting balance0: %w", i, convErr)
		}
		balance1, convErr := BigIntToNumericRequired(s.Balance1, "balance1")
		if convErr != nil {
			return nil, fmt.Errorf("state %d converting balance1: %w", i, convErr)
		}
		out = append(out, stateConverted{
			s: s, sqrtPriceX96: sqrtPriceX96, liquidity: liquidity,
			feeGrowthGlobal0: feeGrowthGlobal0, feeGrowthGlobal1: feeGrowthGlobal1,
			protocolFeesToken0: protocolFeesToken0, protocolFeesToken1: protocolFeesToken1,
			balance0: balance0, balance1: balance1,
		})
	}
	return out, nil
}

func convertSwapsV3(swaps []*entity.UniswapV3Swap) ([]swapConvertedV3, error) {
	out := make([]swapConvertedV3, 0, len(swaps))
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
		out = append(out, swapConvertedV3{
			s: s, amount0: amount0, amount1: amount1, sqrtPriceX96: sqrtPriceX96, liquidity: liquidity,
		})
	}
	return out, nil
}

func convertLiquidityEvents(events []*entity.UniswapV3LiquidityEvent) ([]liquidityEventConverted, error) {
	out := make([]liquidityEventConverted, 0, len(events))
	for i, e := range events {
		amount0, convErr := BigIntToNumericRequired(e.Amount0, "amount0")
		if convErr != nil {
			return nil, fmt.Errorf("liquidity event %d converting amount0: %w", i, convErr)
		}
		amount1, convErr := BigIntToNumericRequired(e.Amount1, "amount1")
		if convErr != nil {
			return nil, fmt.Errorf("liquidity event %d converting amount1: %w", i, convErr)
		}
		out = append(out, liquidityEventConverted{
			e: e, amount: BigIntToNullableNumeric(e.Amount), amount0: amount0, amount1: amount1,
		})
	}
	return out, nil
}

// queueUniswapV3Batch adds all converted rows to batch in the canonical order
// that sendUniswapV3Batch expects to drain them: states, swaps, liquidity
// events, pool events.
func queueUniswapV3Batch(
	batch *pgx.Batch,
	states []stateConverted,
	swaps []swapConvertedV3,
	liqs []liquidityEventConverted,
	poolEvents []*entity.UniswapV3PoolEvent,
	buildID buildregistry.BuildID,
) {
	for _, c := range states {
		s := c.s
		batch.Queue(
			`INSERT INTO uniswap_v3_pool_state
			   (pool_id, block_number, block_version, block_timestamp,
			    sqrt_price_x96, tick, observation_index, observation_cardinality,
			    observation_cardinality_next, fee_protocol, unlocked, liquidity,
			    fee_growth_global0_x128, fee_growth_global1_x128,
			    protocol_fees_token0, protocol_fees_token1, balance0, balance1,
			    twap_tick, twap_window_secs, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
			 ON CONFLICT (pool_id, block_timestamp, block_number, block_version, processing_version) DO NOTHING`,
			s.PoolID, s.BlockNumber, s.BlockVersion, s.BlockTimestamp,
			c.sqrtPriceX96, s.Tick, s.ObservationIndex, s.ObservationCardinality,
			s.ObservationCardinalityNext, s.FeeProtocol, s.Unlocked, c.liquidity,
			c.feeGrowthGlobal0, c.feeGrowthGlobal1,
			c.protocolFeesToken0, c.protocolFeesToken1, c.balance0, c.balance1,
			s.TwapTick, s.TwapWindowSecs, int(buildID),
		)
	}

	for _, c := range swaps {
		s := c.s
		batch.Queue(
			`INSERT INTO uniswap_v3_swap
			   (pool_id, block_number, block_version, block_timestamp,
			    tx_hash, log_index, sender, recipient, amount0, amount1,
			    sqrt_price_x96, liquidity, tick, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
			 ON CONFLICT (pool_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
			s.PoolID, s.BlockNumber, s.BlockVersion, s.BlockTimestamp,
			s.TxHash.Bytes(), s.LogIndex, s.Sender.Bytes(), s.Recipient.Bytes(),
			c.amount0, c.amount1, c.sqrtPriceX96, c.liquidity, s.Tick, int(buildID),
		)
	}

	for _, c := range liqs {
		e := c.e
		batch.Queue(
			`INSERT INTO uniswap_v3_liquidity_event
			   (pool_id, block_number, block_version, block_timestamp,
			    tx_hash, log_index, event_name, owner, sender, recipient,
			    tick_lower, tick_upper, amount, amount0, amount1, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
			 ON CONFLICT (pool_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
			e.PoolID, e.BlockNumber, e.BlockVersion, e.BlockTimestamp,
			e.TxHash.Bytes(), e.LogIndex, string(e.EventName), e.Owner.Bytes(),
			addressBytesOrNil(e.Sender), addressBytesOrNil(e.Recipient),
			e.TickLower, e.TickUpper, c.amount, c.amount0, c.amount1, int(buildID),
		)
	}

	for _, e := range poolEvents {
		batch.Queue(
			`INSERT INTO uniswap_v3_pool_event
			   (pool_id, block_number, block_version, block_timestamp,
			    tx_hash, log_index, event_name, params, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			 ON CONFLICT (pool_id, block_timestamp, block_number, block_version, log_index, processing_version) DO NOTHING`,
			e.PoolID, e.BlockNumber, e.BlockVersion, e.BlockTimestamp,
			e.TxHash.Bytes(), e.LogIndex, string(e.EventName), []byte(e.Params), int(buildID),
		)
	}
}

// addressBytesOrNil returns nil for a nil *common.Address so the column
// serializes as SQL NULL, matching the mint/burn/collect field-presence rules
// in entity.UniswapV3LiquidityEvent.Validate.
func addressBytesOrNil(a *common.Address) []byte {
	if a == nil {
		return nil
	}
	return a.Bytes()
}

// sendUniswapV3Batch executes the queued batch and drains every result in
// queue order, returning the count of state rows inserted. The batch reader
// is always closed before returning so the caller may issue further queries
// on tx (writeTicks runs after this).
func sendUniswapV3Batch(
	ctx context.Context,
	tx pgx.Tx,
	batch *pgx.Batch,
	states []stateConverted,
	swaps []swapConvertedV3,
	liqs []liquidityEventConverted,
	poolEvents []*entity.UniswapV3PoolEvent,
) (stateRows int64, err error) {
	br := tx.SendBatch(ctx, batch)
	defer func() {
		if closeErr := br.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("closing uniswap_v3 SaveBlock batch: %w", closeErr)
		}
	}()

	for i := range states {
		tag, readErr := br.Exec()
		if readErr != nil {
			return stateRows, fmt.Errorf("batch state %d: %w", i, readErr)
		}
		stateRows += tag.RowsAffected()
	}

	for i := range swaps {
		if _, readErr := br.Exec(); readErr != nil {
			return stateRows, fmt.Errorf("batch swap %d: %w", i, readErr)
		}
	}

	for i := range liqs {
		if _, readErr := br.Exec(); readErr != nil {
			return stateRows, fmt.Errorf("batch liquidity event %d: %w", i, readErr)
		}
	}

	for i := range poolEvents {
		if _, readErr := br.Exec(); readErr != nil {
			return stateRows, fmt.Errorf("batch pool event %d: %w", i, readErr)
		}
	}

	return stateRows, nil
}

// writeTicks persists the append-on-change uniswap_v3_tick rows. It locks
// every affected (pool_id, tick) slot (sorted ascending) so the SELECT-latest
// then INSERT decision is serialized against concurrent writers, then writes a
// new row only when no prior row exists, any field differs from the latest one,
// or block_version differs (a reorg re-observation must always insert, even
// with identical values).
//
// All three phases are batched into one round-trip each — the locks in a single
// unnest() Exec, the latest-row reads in a single DISTINCT ON query, the
// changed-tick inserts in a single pgx.Batch — instead of ~4 sequential
// round-trips per tick, which for a dense pool's first touch (O(100+) ticks)
// was hundreds of round-trips under the advisory locks. Semantics are
// unchanged: same keys locked in the same sorted order, same latest-row
// ordering, same per-tick append-on-change decision.
//
// Ticks within one block share one (block_number, block_version) and are
// deduplicated per (pool_id, tick) upstream (TouchedTicks / mergeTickSets), so
// each (pool_id, tick) appears at most once here; the per-key decision below
// therefore needs no in-batch same-key sequencing.
func (r *UniswapV3Repository) writeTicks(ctx context.Context, tx pgx.Tx, ticks []*entity.UniswapV3Tick) error {
	if len(ticks) == 0 {
		return nil
	}

	keys := distinctSortedTickKeys(ticks)
	if err := lockTickKeys(ctx, tx, keys); err != nil {
		return err
	}

	latest, err := readLatestTicks(ctx, tx, keys)
	if err != nil {
		return err
	}

	return r.insertChangedTicks(ctx, tx, ticks, latest)
}

// lockTickKeys acquires the per-slot advisory lock for every key in one
// round-trip via unnest(). keys must already be in the canonical sorted order
// (distinctSortedTickKeys) so concurrent SaveBlock transactions touching
// overlapping ticks acquire overlapping locks in the same order and never
// deadlock (CLAUDE.md read-then-write rule).
//
// The "uniswap_v3_tick|<pool>|<tick>" lock domain is deliberately distinct from
// the pv-trigger's row-identity key ("uvt|pool|tick|block|version"): this guards
// the app-level read-latest-then-insert decision, the trigger guards
// processing_version assignment. They must not be harmonized (same curve_config
// precedent, see curve_repository.go).
func lockTickKeys(ctx context.Context, tx pgx.Tx, keys []tickKey) error {
	lockKeys := make([]string, len(keys))
	for i, k := range keys {
		lockKeys[i] = fmt.Sprintf("uniswap_v3_tick|%d|%d", k.poolID, k.tick)
	}
	// pg_advisory_xact_lock is acquired left-to-right as unnest() yields rows,
	// preserving the sorted-key lock order the single-lock loop used.
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended(k, 0))
		 FROM unnest($1::text[]) WITH ORDINALITY AS u(k, ord)
		 ORDER BY ord`,
		lockKeys,
	); err != nil {
		return fmt.Errorf("locking %d uniswap_v3 tick slots: %w", len(keys), err)
	}
	return nil
}

// readLatestTicks fetches the latest row per (pool_id, tick) for every key in
// one query. The DISTINCT ON ... ORDER BY block_number DESC, block_version DESC,
// processing_version DESC per key is exactly the ordering the old per-tick
// single-row SELECT ... LIMIT 1 used, so it selects the identical latest row.
// Keys with no prior row are simply absent from the returned map (the caller
// treats absence as "no prior row" — insert unconditionally).
func readLatestTicks(ctx context.Context, tx pgx.Tx, keys []tickKey) (map[tickKey]tickValues, error) {
	poolIDs := make([]int64, len(keys))
	tickNums := make([]int32, len(keys))
	for i, k := range keys {
		poolIDs[i] = k.poolID
		tickNums[i] = int32(k.tick)
	}

	rows, err := tx.Query(ctx,
		`SELECT DISTINCT ON (t.pool_id, t.tick)
		        t.pool_id, t.tick, t.block_number, t.block_version,
		        t.liquidity_gross, t.liquidity_net,
		        t.fee_growth_outside0_x128, t.fee_growth_outside1_x128, t.initialized
		 FROM uniswap_v3_tick t
		 JOIN unnest($1::bigint[], $2::int[]) AS k(pool_id, tick)
		   ON t.pool_id = k.pool_id AND t.tick = k.tick
		 ORDER BY t.pool_id, t.tick,
		          t.block_number DESC, t.block_version DESC, t.processing_version DESC`,
		poolIDs, tickNums,
	)
	if err != nil {
		return nil, fmt.Errorf("querying latest ticks for %d slots: %w", len(keys), err)
	}
	defer rows.Close()

	latest := make(map[tickKey]tickValues, len(keys))
	for rows.Next() {
		var (
			poolID                int64
			tick                  int
			blockNumber           int64
			blockVersion          int
			liquidityGross        pgtype.Numeric
			liquidityNet          pgtype.Numeric
			feeGrowthOutside0X128 pgtype.Numeric
			feeGrowthOutside1X128 pgtype.Numeric
			initialized           bool
		)
		if err := rows.Scan(&poolID, &tick, &blockNumber, &blockVersion,
			&liquidityGross, &liquidityNet, &feeGrowthOutside0X128, &feeGrowthOutside1X128,
			&initialized); err != nil {
			return nil, fmt.Errorf("scanning latest tick row: %w", err)
		}
		values, convErr := toTickValues(blockNumber, blockVersion, liquidityGross, liquidityNet,
			feeGrowthOutside0X128, feeGrowthOutside1X128, initialized)
		if convErr != nil {
			return nil, fmt.Errorf("reading latest tick for pool=%d tick=%d: %w", poolID, tick, convErr)
		}
		latest[tickKey{poolID: poolID, tick: tick}] = values
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating latest ticks: %w", err)
	}
	return latest, nil
}

// insertChangedTicks queues an INSERT for every tick whose latest row is absent
// (no prior row) or differs from it, then sends them in one pgx.Batch. The
// per-tick decision (insert vs skip) is byte-for-byte the same as the old
// per-tick AppendOnChange path: no prior row inserts, an unchanged tick is
// skipped, a changed tick (or a bumped block_version reorg) inserts. The
// INSERTs run through the table's BEFORE INSERT ROW trigger — pgx.Batch issues
// ordinary INSERT statements, so the per-row processing_version trigger fires
// exactly as it did for the single-row path.
func (r *UniswapV3Repository) insertChangedTicks(
	ctx context.Context, tx pgx.Tx,
	ticks []*entity.UniswapV3Tick,
	latest map[tickKey]tickValues,
) (err error) {
	batch := &pgx.Batch{}
	var queued int
	for i, t := range ticks {
		prior, hasPrior := latest[tickKey{poolID: t.PoolID, tick: t.Tick}]
		if hasPrior && tickUnchanged(prior, t) {
			continue
		}
		converted, convErr := convertTick(t)
		if convErr != nil {
			return fmt.Errorf("tick %d: converting tick pool=%d tick=%d: %w", i, t.PoolID, t.Tick, convErr)
		}
		batch.Queue(
			`INSERT INTO uniswap_v3_tick
			   (pool_id, tick, block_number, block_version, block_timestamp,
			    liquidity_gross, liquidity_net, fee_growth_outside0_x128,
			    fee_growth_outside1_x128, initialized, build_id)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			 ON CONFLICT (pool_id, tick, block_number, block_version, processing_version) DO NOTHING`,
			t.PoolID, t.Tick, t.BlockNumber, t.BlockVersion, t.BlockTimestamp,
			converted.liquidityGross, converted.liquidityNet,
			converted.feeGrowthOutside0X128, converted.feeGrowthOutside1X128, t.Initialized, int(r.buildID),
		)
		queued++
	}

	if queued == 0 {
		return nil
	}

	br := tx.SendBatch(ctx, batch)
	defer func() {
		if closeErr := br.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing uniswap_v3 tick batch: %w", closeErr))
		}
	}()
	for i := 0; i < queued; i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("inserting tick batch entry %d: %w", i, err)
		}
	}
	return nil
}

type tickKey struct {
	poolID int64
	tick   int
}

// distinctSortedTickKeys mirrors curve's distinctSortedConfigPoolIDs: acquire
// locks in sorted key order so concurrent SaveBlock transactions touching
// overlapping ticks never deadlock.
func distinctSortedTickKeys(ticks []*entity.UniswapV3Tick) []tickKey {
	seen := make(map[tickKey]struct{}, len(ticks))
	for _, t := range ticks {
		seen[tickKey{poolID: t.PoolID, tick: t.Tick}] = struct{}{}
	}
	keys := make([]tickKey, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b tickKey) int {
		if a.poolID != b.poolID {
			return int(a.poolID - b.poolID)
		}
		return a.tick - b.tick
	})
	return keys
}

func convertTick(t *entity.UniswapV3Tick) (tickConverted, error) {
	liquidityGross, err := BigIntToNumericRequired(t.LiquidityGross, "liquidity_gross")
	if err != nil {
		return tickConverted{}, err
	}
	liquidityNet, err := BigIntToNumericRequired(t.LiquidityNet, "liquidity_net")
	if err != nil {
		return tickConverted{}, err
	}
	feeGrowthOutside0X128, err := BigIntToNumericRequired(t.FeeGrowthOutside0X128, "fee_growth_outside0_x128")
	if err != nil {
		return tickConverted{}, err
	}
	feeGrowthOutside1X128, err := BigIntToNumericRequired(t.FeeGrowthOutside1X128, "fee_growth_outside1_x128")
	if err != nil {
		return tickConverted{}, err
	}
	return tickConverted{
		t: t, liquidityGross: liquidityGross, liquidityNet: liquidityNet,
		feeGrowthOutside0X128: feeGrowthOutside0X128, feeGrowthOutside1X128: feeGrowthOutside1X128,
	}, nil
}

// tickValues holds the latest tick row's fields decoded to *big.Int for
// comparison against a candidate entity.UniswapV3Tick.
type tickValues struct {
	blockNumber           int64
	blockVersion          int
	liquidityGross        *big.Int
	liquidityNet          *big.Int
	feeGrowthOutside0X128 *big.Int
	feeGrowthOutside1X128 *big.Int
	initialized           bool
}

func toTickValues(
	blockNumber int64, blockVersion int,
	liquidityGross, liquidityNet, feeGrowthOutside0X128, feeGrowthOutside1X128 pgtype.Numeric,
	initialized bool,
) (tickValues, error) {
	var v tickValues
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
	v.blockNumber = blockNumber
	v.blockVersion = blockVersion
	v.initialized = initialized
	return v, nil
}

// tickUnchanged reports whether the latest tick row already reflects t: same
// block_version (a different version means a reorg re-observation, which must
// always insert regardless of value equality) and every field equal.
func tickUnchanged(latest tickValues, t *entity.UniswapV3Tick) bool {
	return latest.blockVersion == t.BlockVersion &&
		bigIntEqual(latest.liquidityGross, t.LiquidityGross) &&
		bigIntEqual(latest.liquidityNet, t.LiquidityNet) &&
		bigIntEqual(latest.feeGrowthOutside0X128, t.FeeGrowthOutside0X128) &&
		bigIntEqual(latest.feeGrowthOutside1X128, t.FeeGrowthOutside1X128) &&
		latest.initialized == t.Initialized
}
