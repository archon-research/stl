package postgres

import (
	"context"
	"fmt"

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

// SaveBlock persists all of a block's uniswap_v3 rows in one pgx.Batch within
// tx, except ticks (append-on-change, see uniswapTickWriter), and returns the
// count of state rows inserted.
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
	if err := uniswapV3TickWriter.writeTicks(ctx, tx, uniswapV3TickRows(w.Ticks), r.buildID); err != nil {
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

// uniswapV3TickRows maps the V3 entities onto the shared writer's row shape.
func uniswapV3TickRows(ticks []*entity.UniswapV3Tick) []uniswapTickRow {
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
			initialized:           t.Initialized,
		}
	}
	return rows
}
