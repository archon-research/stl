-- Column-level COMMENT metadata for the Uniswap V3 DEX tables (created in
-- 20260701_100000_create_uniswap_v3_tables.sql). Consumed by the metadata
-- catalogue. Conventions used below (mirrors 20260521_120000_curve_column_
-- comments.sql):
--   [Type]: Dimension (seeded/read-only registry) | Hypertable (time-series
--           facts) | Snapshot (append-on-change, not partitioned)
--   Roles:  PK | FK->table.col | Partition | Audit | Derived
--   Scale:  numeric amounts state their unit explicitly. sqrt_price_x96 is
--           Q64.96 fixed point: price(token1/token0) = (sqrtPriceX96/2^96)^2,
--           then adjust by 10^(dec0-dec1). fee_growth_*_x128 columns are
--           Q128.128 fixed point. fee is hundredths of a bip (1e6 = 100%,
--           3000 = 0.30%). tick is a plain integer where price = 1.0001^tick.
--           liquidity/liquidity_gross/liquidity_net are raw on-chain L
--           (sqrt(xy) scaled by the pool's token decimals; not comparable
--           across pools with different token decimals). balance0/balance1
--           and amount0/amount1 are raw native-decimal integers (scale by
--           the relevant token's decimals); amounts may be signed (negative
--           = outflow from the pool).

-- ===========================================================================
-- uniswap_v3_pool (registry)
-- ===========================================================================
COMMENT ON TABLE uniswap_v3_pool IS
  '[Dimension] Registry of indexed Uniswap V3 pools, one row per deployed pool; FK target for every uniswap_v3_* fact table. Migration-seeded and read-only at runtime.';
COMMENT ON COLUMN uniswap_v3_pool.id IS
  'PK. Surrogate pool ID; FK target for all uniswap_v3_* fact tables.';
COMMENT ON COLUMN uniswap_v3_pool.chain_id IS
  'FK->chain.chain_id. Network the pool is deployed on.';
COMMENT ON COLUMN uniswap_v3_pool.protocol_id IS
  'FK->protocol.id. The UniswapV3 protocol (factory) row this pool belongs to.';
COMMENT ON COLUMN uniswap_v3_pool.pool_address IS
  'On-chain pool contract address, 20 bytes. Unique per chain.';
COMMENT ON COLUMN uniswap_v3_pool.token0_id IS
  'FK->token.id. The pool''s token0 as returned by token0(); not assumed from the pair listing (a given symbol may be token0 in one pool and token1 in another).';
COMMENT ON COLUMN uniswap_v3_pool.token1_id IS
  'FK->token.id. The pool''s token1 as returned by token1().';
COMMENT ON COLUMN uniswap_v3_pool.fee IS
  'Pool fee tier fee(), hundredths of a bip (1e6 = 100%; e.g. 3000 = 0.30%, 500 = 0.05%, 100 = 0.01%).';
COMMENT ON COLUMN uniswap_v3_pool.tick_spacing IS
  'tickSpacing(): minimum tick granularity for this fee tier (e.g. 1, 10, 60, 200); ticks usable by positions must be a multiple of this value.';
COMMENT ON COLUMN uniswap_v3_pool.max_liquidity_per_tick IS
  'maxLiquidityPerTick(): maximum raw liquidity L (see liquidity scale above) that can be attributed to a single tick, derived from tick_spacing.';
COMMENT ON COLUMN uniswap_v3_pool.deploy_block IS
  'Block at which the pool contract was deployed (PoolCreated). Used to gate state reads: skip a pool entirely for blocks before this height. May be NULL for pools backfilled without a known deploy block.';
COMMENT ON COLUMN uniswap_v3_pool.created_at IS
  'Audit. Row insertion timestamp (bookkeeping only; not an on-chain value).';

-- ===========================================================================
-- uniswap_v3_pool_state (per-touched-block snapshot)
-- ===========================================================================
COMMENT ON TABLE uniswap_v3_pool_state IS
  '[Hypertable] Per-touched-block snapshot of pool slot0 / liquidity / fee-growth / protocol-fee / balance state, taken only on blocks that touch the pool (V3 state is piecewise-constant; no periodic heartbeat). Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN uniswap_v3_pool_state.pool_id IS
  'PK, FK->uniswap_v3_pool.id. Pool the snapshot is for.';
COMMENT ON COLUMN uniswap_v3_pool_state.block_number IS
  'PK. Block height at which the snapshot was read (via multicall pinned to this block''s hash).';
COMMENT ON COLUMN uniswap_v3_pool_state.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v3_pool_state.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN uniswap_v3_pool_state.sqrt_price_x96 IS
  'slot0().sqrtPriceX96: Q64.96 fixed point. price(token1/token0) = (sqrt_price_x96/2^96)^2, then adjust by 10^(token0.decimals-token1.decimals).';
COMMENT ON COLUMN uniswap_v3_pool_state.tick IS
  'slot0().tick: current pool tick (plain integer); price(token1/token0) = 1.0001^tick before decimal adjustment.';
COMMENT ON COLUMN uniswap_v3_pool_state.observation_index IS
  'slot0().observationIndex: index of the most recently written oracle observation in the observations ring buffer.';
COMMENT ON COLUMN uniswap_v3_pool_state.observation_cardinality IS
  'slot0().observationCardinality: number of populated oracle observation slots.';
COMMENT ON COLUMN uniswap_v3_pool_state.observation_cardinality_next IS
  'slot0().observationCardinalityNext: cardinality the oracle ring buffer will grow to on its next write.';
COMMENT ON COLUMN uniswap_v3_pool_state.fee_protocol IS
  'slot0().feeProtocol: packed protocol fee, 4 bits per token (feeProtocol0 in the low nibble, feeProtocol1 in the high nibble); 0 means the protocol takes no share of that token''s swap fee, otherwise the fee is divided by this value.';
COMMENT ON COLUMN uniswap_v3_pool_state.unlocked IS
  'slot0().unlocked: reentrancy-lock flag from the contract; false only mid-callback, always true in a settled post-block read.';
COMMENT ON COLUMN uniswap_v3_pool_state.liquidity IS
  'liquidity(): in-range raw liquidity L active at the current tick (sqrt(xy) scaled by the pool''s token decimals; not comparable across pools with different token decimals).';
COMMENT ON COLUMN uniswap_v3_pool_state.fee_growth_global0_x128 IS
  'feeGrowthGlobal0X128(): cumulative token0 fees earned per unit of liquidity over the pool''s lifetime, Q128.128 fixed point.';
COMMENT ON COLUMN uniswap_v3_pool_state.fee_growth_global1_x128 IS
  'feeGrowthGlobal1X128(): cumulative token1 fees earned per unit of liquidity over the pool''s lifetime, Q128.128 fixed point.';
COMMENT ON COLUMN uniswap_v3_pool_state.protocol_fees_token0 IS
  'protocolFees().token0: accrued protocol-owned token0 fees awaiting collection, raw native decimals of token0.';
COMMENT ON COLUMN uniswap_v3_pool_state.protocol_fees_token1 IS
  'protocolFees().token1: accrued protocol-owned token1 fees awaiting collection, raw native decimals of token1.';
COMMENT ON COLUMN uniswap_v3_pool_state.balance0 IS
  'ERC20(token0).balanceOf(pool): actual token0 reserves held by the pool contract, raw native decimals of token0 (the pool exposes no balance getter of its own).';
COMMENT ON COLUMN uniswap_v3_pool_state.balance1 IS
  'ERC20(token1).balanceOf(pool): actual token1 reserves held by the pool contract, raw native decimals of token1.';
COMMENT ON COLUMN uniswap_v3_pool_state.twap_tick IS
  'Derived. Arithmetic-mean tick over twap_window_secs from observe([twap_window_secs,0]); NULL when the call reverts with OLD (observation cardinality cannot yet cover the requested window). Convert to price the same way as tick.';
COMMENT ON COLUMN uniswap_v3_pool_state.twap_window_secs IS
  'Window length in seconds used for the twap_tick observe() call; NULL alongside twap_tick when the read was skipped or reverted.';
COMMENT ON COLUMN uniswap_v3_pool_state.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v3_pool_state.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

-- ===========================================================================
-- uniswap_v3_swap (event facts)
-- ===========================================================================
COMMENT ON TABLE uniswap_v3_swap IS
  '[Hypertable] One row per on-chain Swap event. Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN uniswap_v3_swap.pool_id IS
  'PK, FK->uniswap_v3_pool.id. Pool that emitted the swap.';
COMMENT ON COLUMN uniswap_v3_swap.block_number IS
  'PK. Block height at which the swap was emitted.';
COMMENT ON COLUMN uniswap_v3_swap.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v3_swap.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN uniswap_v3_swap.tx_hash IS
  'Transaction hash, 32 bytes.';
COMMENT ON COLUMN uniswap_v3_swap.log_index IS
  'PK. Index of the event log within the block.';
COMMENT ON COLUMN uniswap_v3_swap.sender IS
  'Address that initiated the swap (event field `sender`, typically a router), 20 bytes.';
COMMENT ON COLUMN uniswap_v3_swap.recipient IS
  'Address that received the swap output (event field `recipient`), 20 bytes.';
COMMENT ON COLUMN uniswap_v3_swap.amount0 IS
  'Signed change in the pool''s token0 balance from this swap, raw native decimals of token0 (positive = token0 flowed into the pool, negative = out).';
COMMENT ON COLUMN uniswap_v3_swap.amount1 IS
  'Signed change in the pool''s token1 balance from this swap, raw native decimals of token1 (positive = token1 flowed into the pool, negative = out).';
COMMENT ON COLUMN uniswap_v3_swap.sqrt_price_x96 IS
  'Pool sqrtPriceX96 immediately after the swap, Q64.96 fixed point (see uniswap_v3_pool_state.sqrt_price_x96 for the price conversion).';
COMMENT ON COLUMN uniswap_v3_swap.liquidity IS
  'Pool in-range liquidity() immediately after the swap, raw L (see uniswap_v3_pool_state.liquidity for the scale).';
COMMENT ON COLUMN uniswap_v3_swap.tick IS
  'Pool tick immediately after the swap (plain integer; price = 1.0001^tick before decimal adjustment).';
COMMENT ON COLUMN uniswap_v3_swap.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v3_swap.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

-- ===========================================================================
-- uniswap_v3_liquidity_event (event facts)
-- ===========================================================================
COMMENT ON TABLE uniswap_v3_liquidity_event IS
  '[Hypertable] One row per Mint / Burn / Collect event (adding, removing, or withdrawing accrued fees from a position''s liquidity range). Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.pool_id IS
  'PK, FK->uniswap_v3_pool.id. Pool that emitted the event.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.block_number IS
  'PK. Block height at which the event was emitted.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v3_liquidity_event.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.tx_hash IS
  'Transaction hash, 32 bytes.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.log_index IS
  'PK. Index of the event log within the block.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.event_name IS
  'Event variant: mint, burn, or collect.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.owner IS
  'Position owner address (event field `owner`), 20 bytes.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.sender IS
  'Address that supplied the liquidity (event field `sender`), 20 bytes; populated for mint only, NULL for burn/collect.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.recipient IS
  'Address that received the collected tokens (event field `recipient`), 20 bytes; populated for collect only, NULL for mint/burn.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.tick_lower IS
  'Lower tick bound of the position''s liquidity range (plain integer, inclusive).';
COMMENT ON COLUMN uniswap_v3_liquidity_event.tick_upper IS
  'Upper tick bound of the position''s liquidity range (plain integer, exclusive).';
COMMENT ON COLUMN uniswap_v3_liquidity_event.amount IS
  'Raw liquidity delta L added (mint) or removed (burn) from the range (see uniswap_v3_pool_state.liquidity for the scale); NULL for collect, which has no liquidity field.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.amount0 IS
  'Token0 amount moved by the event, raw native decimals of token0: owed/paid for mint/burn, collected for collect.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.amount1 IS
  'Token1 amount moved by the event, raw native decimals of token1.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v3_liquidity_event.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

-- ===========================================================================
-- uniswap_v3_tick (append-on-change authoritative per-tick state)
-- ===========================================================================
COMMENT ON TABLE uniswap_v3_tick IS
  '[Snapshot] Append-on-change authoritative per-tick state from ticks(int24) reads. A new row is written only when a touched tick''s state changes (or on first-seen baseline enumeration for the pool); not a hypertable, since ticks are written on-change rather than on every touched block.';
COMMENT ON COLUMN uniswap_v3_tick.pool_id IS
  'PK, FK->uniswap_v3_pool.id. Pool the tick belongs to.';
COMMENT ON COLUMN uniswap_v3_tick.tick IS
  'PK. Tick index (plain integer, a multiple of the pool''s tick_spacing); price = 1.0001^tick before decimal adjustment.';
COMMENT ON COLUMN uniswap_v3_tick.block_number IS
  'PK. Block height at which this tick state was read.';
COMMENT ON COLUMN uniswap_v3_tick.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v3_tick.block_timestamp IS
  'Block timestamp (UTC) at which this tick state was read.';
COMMENT ON COLUMN uniswap_v3_tick.liquidity_gross IS
  'ticks(tick).liquidityGross: total raw liquidity L referencing this tick as a boundary, regardless of direction (see uniswap_v3_pool_state.liquidity for the scale).';
COMMENT ON COLUMN uniswap_v3_tick.liquidity_net IS
  'ticks(tick).liquidityNet: signed raw liquidity L added (positive) or removed (negative) when the pool price crosses this tick left-to-right; sign flips for a right-to-left crossing.';
COMMENT ON COLUMN uniswap_v3_tick.fee_growth_outside0_x128 IS
  'ticks(tick).feeGrowthOutside0X128: token0 fee growth on the outside of this tick at the time it was last crossed, Q128.128 fixed point.';
COMMENT ON COLUMN uniswap_v3_tick.fee_growth_outside1_x128 IS
  'ticks(tick).feeGrowthOutside1X128: token1 fee growth on the outside of this tick at the time it was last crossed, Q128.128 fixed point.';
COMMENT ON COLUMN uniswap_v3_tick.initialized IS
  'ticks(tick).initialized: true while liquidity_gross > 0 for this tick (i.e. at least one position boundary references it).';
COMMENT ON COLUMN uniswap_v3_tick.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v3_tick.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

-- ===========================================================================
-- uniswap_v3_pool_event (typed low-frequency events)
-- ===========================================================================
COMMENT ON TABLE uniswap_v3_pool_event IS
  '[Hypertable] Decoded low-frequency pool events (Initialize, Flash, SetFeeProtocol, CollectProtocol, IncreaseObservationCardinalityNext); typed counterpart to the raw protocol_event mirror. Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN uniswap_v3_pool_event.pool_id IS
  'PK, FK->uniswap_v3_pool.id. Pool that emitted the event.';
COMMENT ON COLUMN uniswap_v3_pool_event.block_number IS
  'PK. Block height at which the event was emitted.';
COMMENT ON COLUMN uniswap_v3_pool_event.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN uniswap_v3_pool_event.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN uniswap_v3_pool_event.tx_hash IS
  'Transaction hash, 32 bytes.';
COMMENT ON COLUMN uniswap_v3_pool_event.log_index IS
  'PK. Index of the event log within the block.';
COMMENT ON COLUMN uniswap_v3_pool_event.event_name IS
  'Decoded event type: one of initialize, flash, set_fee_protocol, collect_protocol, increase_observation_cardinality_next.';
COMMENT ON COLUMN uniswap_v3_pool_event.params IS
  'JSONB of decoded event fields keyed by name. Keys per event_name: initialize {sqrtPriceX96,tick} (sqrtPriceX96 is Q64.96, tick a plain integer); flash {sender,recipient,amount0,amount1,paid0,paid1} (raw native decimals, signed where applicable); set_fee_protocol {feeProtocol0Old,feeProtocol1Old,feeProtocol0New,feeProtocol1New} (plain integers); collect_protocol {sender,recipient,amount0,amount1} (raw native decimals); increase_observation_cardinality_next {observationCardinalityNextOld,observationCardinalityNextNew} (plain integers).';
COMMENT ON COLUMN uniswap_v3_pool_event.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN uniswap_v3_pool_event.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

INSERT INTO migrations (filename)
VALUES ('20260701_100100_uniswap_v3_comments.sql')
ON CONFLICT (filename) DO NOTHING;
