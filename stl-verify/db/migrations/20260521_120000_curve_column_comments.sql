-- Column-level COMMENT metadata for the Curve DEX tables (created in
-- 20260521_110000_create_curve_dex_tables.sql). Consumed by the metadata
-- catalogue. Conventions used below:
--   [Type]: Dimension (seeded registry) | Hypertable (time-series facts)
--   Roles:  PK | FK->table.col | Partition | Audit | Derived
--   Scale:  numeric amounts state their unit explicitly. Token amounts are the
--           raw on-chain integer in the token's native decimals (USDC=1e6,
--           WBTC=1e8, most=1e18). Curve fixed-point: virtual_price / prices /
--           gamma / D / xcp_profit are 1e18; A is a plain integer; fee uses the
--           Curve fee denominator where 1e10 = 100%.

-- ===========================================================================
-- curve_pool (registry)
-- ===========================================================================
COMMENT ON TABLE curve_pool IS
  '[Dimension] Registry of indexed Curve AMM pools, one row per deployed pool. Migration-seeded and read-only at runtime; FK target for every curve_* fact table.';
COMMENT ON COLUMN curve_pool.id IS
  'PK. Surrogate pool ID; FK target for all curve_* fact tables.';
COMMENT ON COLUMN curve_pool.chain_id IS
  'FK->chain.chain_id. Network the pool is deployed on.';
COMMENT ON COLUMN curve_pool.protocol_id IS
  'FK->protocol.id. The Curve protocol row this pool belongs to.';
COMMENT ON COLUMN curve_pool.pool_address IS
  'On-chain pool (swap) contract address, 20 bytes. Unique per chain.';
COMMENT ON COLUMN curve_pool.pool_kind IS
  'Pool implementation variant, drives decode/snapshot dispatch: plain_pre_ng (legacy plain stableswap), plain_ng (Stableswap-NG plain), or cryptoswap (Cryptoswap / Tricrypto-NG).';
COMMENT ON COLUMN curve_pool.n_coins IS
  'Number of coins in the pool (>= 2).';
COMMENT ON COLUMN curve_pool.lp_token_address IS
  'LP/share token contract, 20 bytes. A separate contract for pre-NG pools (where totalSupply lives); NULL when the pool is its own LP token (NG pools).';
COMMENT ON COLUMN curve_pool.deploy_block IS
  'Advisory deployment block; may be NULL or approximate for pools backfilled later. Not read by the indexer at runtime.';
COMMENT ON COLUMN curve_pool.created_at IS
  'Audit. Row insertion timestamp (bookkeeping only; not an on-chain value).';

-- ===========================================================================
-- curve_pool_coin (registry)
-- ===========================================================================
COMMENT ON TABLE curve_pool_coin IS
  '[Dimension] Coins within each pool, one row per (pool, coin index). Migration-seeded and read-only at runtime.';
COMMENT ON COLUMN curve_pool_coin.curve_pool_id IS
  'PK, FK->curve_pool.id. Pool this coin belongs to.';
COMMENT ON COLUMN curve_pool_coin.coin_index IS
  'PK. On-chain coin index i (0-based, matches the pool coins(i) ordering).';
COMMENT ON COLUMN curve_pool_coin.token_id IS
  'FK->token.id. The ERC-20 at this coin index. Resolve by (chain_id, address), never by symbol.';

-- ===========================================================================
-- curve_swap (event facts)
-- ===========================================================================
COMMENT ON TABLE curve_swap IS
  '[Hypertable] One row per on-chain TokenExchange / TokenExchangeUnderlying event. Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN curve_swap.curve_pool_id IS
  'FK->curve_pool.id. Pool that emitted the swap.';
COMMENT ON COLUMN curve_swap.block_number IS
  'Block height at which the swap was emitted.';
COMMENT ON COLUMN curve_swap.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented when a block hash is replaced by a chain reorg).';
COMMENT ON COLUMN curve_swap.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN curve_swap.tx_hash IS
  'Transaction hash, 32 bytes.';
COMMENT ON COLUMN curve_swap.log_index IS
  'PK. Index of the event log within the block.';
COMMENT ON COLUMN curve_swap.buyer IS
  'Address that initiated the exchange (event field `buyer`), 20 bytes.';
COMMENT ON COLUMN curve_swap.sold_id IS
  'Coin index sold into the pool (coins(sold_id)).';
COMMENT ON COLUMN curve_swap.bought_id IS
  'Coin index bought out of the pool (coins(bought_id)).';
COMMENT ON COLUMN curve_swap.tokens_sold IS
  'Amount of coins(sold_id) sold, raw on-chain integer in that token native decimals.';
COMMENT ON COLUMN curve_swap.tokens_bought IS
  'Amount of coins(bought_id) bought, raw on-chain integer in that token native decimals.';
COMMENT ON COLUMN curve_swap.fee IS
  'Swap fee from the event when present (Cryptoswap / NG TokenExchange carries a fee field), raw on-chain units; NULL for classic pools whose TokenExchange has no fee field.';
COMMENT ON COLUMN curve_swap.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites the same key; prior versions are retained.';
COMMENT ON COLUMN curve_swap.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

-- ===========================================================================
-- curve_liquidity_event (event facts)
-- ===========================================================================
COMMENT ON TABLE curve_liquidity_event IS
  '[Hypertable] One row per AddLiquidity / RemoveLiquidity / RemoveLiquidityOne / RemoveLiquidityImbalance event. Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN curve_liquidity_event.curve_pool_id IS
  'FK->curve_pool.id. Pool that emitted the event.';
COMMENT ON COLUMN curve_liquidity_event.block_number IS
  'Block height at which the event was emitted.';
COMMENT ON COLUMN curve_liquidity_event.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented on a chain reorg).';
COMMENT ON COLUMN curve_liquidity_event.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN curve_liquidity_event.tx_hash IS
  'Transaction hash, 32 bytes.';
COMMENT ON COLUMN curve_liquidity_event.log_index IS
  'PK. Index of the event log within the block.';
COMMENT ON COLUMN curve_liquidity_event.provider IS
  'Liquidity provider address (indexed event topic), 20 bytes.';
COMMENT ON COLUMN curve_liquidity_event.kind IS
  'Event variant: add, remove, remove_one (single-coin removal), or remove_imbalance.';
COMMENT ON COLUMN curve_liquidity_event.token_amounts IS
  'Per-coin token amounts, raw native decimals, index-aligned to coins(i), for add/remove/remove_imbalance. For remove_one it holds [LP tokens burned, single-coin amount received].';
COMMENT ON COLUMN curve_liquidity_event.coin_index IS
  'For remove_one: the coin index withdrawn (cryptoswap emits it; NULL for classic remove_one and for all other kinds).';
COMMENT ON COLUMN curve_liquidity_event.fees IS
  'Per-coin fees charged, raw native decimals, index-aligned to coins(i), when the event carries them; NULL when absent.';
COMMENT ON COLUMN curve_liquidity_event.invariant IS
  'Pool invariant D after the event when emitted (add / remove_imbalance), in the pool internal units; NULL otherwise.';
COMMENT ON COLUMN curve_liquidity_event.token_supply IS
  'LP token total supply after the event as emitted; NULL when the event has no supply field.';
COMMENT ON COLUMN curve_liquidity_event.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites it; prior versions retained.';
COMMENT ON COLUMN curve_liquidity_event.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

-- ===========================================================================
-- curve_stableswap_state (multicall state snapshots)
-- ===========================================================================
COMMENT ON TABLE curve_stableswap_state IS
  '[Hypertable] Periodic multicall snapshots of stableswap (plain_pre_ng / plain_ng) pool state: taken when a pool is touched by a block and on a configurable block heartbeat. Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN curve_stableswap_state.curve_pool_id IS
  'FK->curve_pool.id. Pool the snapshot is for.';
COMMENT ON COLUMN curve_stableswap_state.block_number IS
  'Block height at which the snapshot was read (via multicall at this block).';
COMMENT ON COLUMN curve_stableswap_state.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented on a chain reorg).';
COMMENT ON COLUMN curve_stableswap_state.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN curve_stableswap_state.balances IS
  'Per-coin pool balances from balances(i), raw native decimals, index-aligned to coins(i).';
COMMENT ON COLUMN curve_stableswap_state.virtual_price IS
  'get_virtual_price(): LP token virtual price, 1e18-scaled.';
COMMENT ON COLUMN curve_stableswap_state.total_supply IS
  'LP token totalSupply() (read from the LP token contract), raw 1e18.';
COMMENT ON COLUMN curve_stableswap_state.a IS
  'Amplification coefficient A() (plain integer, dimensionless).';
COMMENT ON COLUMN curve_stableswap_state.fee IS
  'Swap fee fee(), in Curve fee units where 1e10 = 100% (e.g. 1000000 = 0.01%).';
COMMENT ON COLUMN curve_stableswap_state.spot_dy IS
  'Derived. Marginal output from get_dy(i,j,10^decimals[i]) for each ordered coin pair i!=j (i asc, then j asc), raw native decimals of coin j: the spot price of one unit of coin i in coin j.';
COMMENT ON COLUMN curve_stableswap_state.last_price IS
  'last_price(): latest spot price feeding the EMA, 1e18; non-NULL only for plain_ng pools (plain_pre_ng has no such getter).';
COMMENT ON COLUMN curve_stableswap_state.price_oracle IS
  'price_oracle(): EMA oracle price, 1e18; non-NULL only for plain_ng pools (NULL for plain_pre_ng).';
COMMENT ON COLUMN curve_stableswap_state.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites it; prior versions retained.';
COMMENT ON COLUMN curve_stableswap_state.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

-- ===========================================================================
-- curve_cryptoswap_state (multicall state snapshots)
-- ===========================================================================
COMMENT ON TABLE curve_cryptoswap_state IS
  '[Hypertable] Periodic multicall snapshots of cryptoswap (Cryptoswap / Tricrypto-NG) pool state: taken when a pool is touched and on a configurable block heartbeat. Partitioned on block_timestamp (1-day chunks); append-only via the processing_version trigger.';
COMMENT ON COLUMN curve_cryptoswap_state.curve_pool_id IS
  'FK->curve_pool.id. Pool the snapshot is for.';
COMMENT ON COLUMN curve_cryptoswap_state.block_number IS
  'Block height at which the snapshot was read (via multicall at this block).';
COMMENT ON COLUMN curve_cryptoswap_state.block_version IS
  'PK. Reorg version of the block (0 = first/canonical; incremented on a chain reorg).';
COMMENT ON COLUMN curve_cryptoswap_state.block_timestamp IS
  'PK, Partition. Block timestamp (UTC); hypertable partition column.';
COMMENT ON COLUMN curve_cryptoswap_state.balances IS
  'Per-coin pool balances from balances(i), raw native decimals, index-aligned to coins(i).';
COMMENT ON COLUMN curve_cryptoswap_state.virtual_price IS
  'get_virtual_price(): LP token virtual price, 1e18-scaled.';
COMMENT ON COLUMN curve_cryptoswap_state.total_supply IS
  'LP token totalSupply(), raw 1e18.';
COMMENT ON COLUMN curve_cryptoswap_state.a IS
  'Amplification coefficient A() (cryptoswap units).';
COMMENT ON COLUMN curve_cryptoswap_state.gamma IS
  'Cryptoswap gamma() parameter, 1e18.';
COMMENT ON COLUMN curve_cryptoswap_state.fee IS
  'Current dynamic fee fee(), in Curve fee units where 1e10 = 100% (interpolated between mid_fee and out_fee by pool balance).';
COMMENT ON COLUMN curve_cryptoswap_state.d IS
  'Pool invariant D(), 1e18-normalized internal units; NULL if the call reverted.';
COMMENT ON COLUMN curve_cryptoswap_state.xcp_profit IS
  'xcp_profit(): cumulative profit metric (1e18) used by the pool repegging logic; NULL if the call reverted.';
COMMENT ON COLUMN curve_cryptoswap_state.price_scale IS
  'price_scale(i) for i in [0, n-2]: the price the pool currently uses to concentrate liquidity, coin (i+1) relative to coin 0, 1e18-scaled.';
COMMENT ON COLUMN curve_cryptoswap_state.price_oracle IS
  'price_oracle(i) for i in [0, n-2]: manipulation-resistant EMA oracle price of coin (i+1) relative to coin 0, 1e18-scaled.';
COMMENT ON COLUMN curve_cryptoswap_state.last_prices IS
  'last_prices(i) for i in [0, n-2]: most-recent spot price of coin (i+1) relative to coin 0, 1e18-scaled.';
COMMENT ON COLUMN curve_cryptoswap_state.spot_dy IS
  'Derived. Marginal output from get_dy(i,j,10^decimals[i]) for each ordered coin pair i!=j, raw native decimals of coin j.';
COMMENT ON COLUMN curve_cryptoswap_state.processing_version IS
  'PK, Audit. Per-build reprocessing counter (ADR-0002): 0 for the first write of a key under a build_id, bumped only when a later build rewrites it; prior versions retained.';
COMMENT ON COLUMN curve_cryptoswap_state.build_id IS
  'Audit. ID of the indexer build (code+config) that wrote this row.';

INSERT INTO migrations (filename)
VALUES ('20260521_120000_curve_column_comments.sql')
ON CONFLICT (filename) DO NOTHING;
