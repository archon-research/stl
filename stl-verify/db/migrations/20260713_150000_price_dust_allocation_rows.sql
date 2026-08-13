-- Price the remaining dust allocation rows whose amount_usd is NULL in the
-- allocations API (ER calculation), Ethereum mainnet (chain_id = 1):
--   aEthUSDC/aEthUSDS/aEthUSDT/aEthRLUSD (Aave V3 receipts), spDAI (SparkLend
--   receipt), sparkUSDS (MetaMorpho vault share), and the PYUSDUSDS and
--   sUSDSUSDT Curve StableSwap-NG LP tokens (held bare).
-- No backfill is run as part of this rollout: new feed rows and the two curve
-- oracles start pricing at the next oracle-price-worker restart (units load
-- once at startup; the deploy that applies this migration also rolls the
-- pods), and oracle-pricing-backfill supports every type used here if history
-- is ever needed.
-- All contracts (pool coins, get_virtual_price, stored_rates, vault probes)
-- and feeds (description/decimals/latestRoundData) re-verified live via cast
-- on 2026-07-13; DB state (bindings, feed coverage, staleness) re-verified
-- against the warehouse the same day.
-- Six steps, ordered by dependency:
--   1. Seed the two Curve LP tokens (fresh-DB determinism; live rows no-op).
--   2. protocol_oracle binding Aave V3 -> chainlink.
--   3. protocol_oracle binding SparkLend -> chainlink; retire the six frozen
--      sparklend oracle_asset rows.
--   4. chainlink oracle_asset row for RLUSD (aEthRLUSD needs RLUSD priced
--      under an Aave-V3-bound oracle).
--   5. sparkUSDS morpho_vault + receipt_token rows (receipt-path home).
--   6. curve_lp_ng oracle registry rows for the PYUSD/USDS and sUSDS/USDT
--      pools (shipped enabled).

-- ============================================================================
-- 1. Token seeds. Curve StableSwap-NG pools are their own LP token; symbols
--    and 18 decimals cast-verified on the pool contracts. Live DBs already
--    carry both rows (runtime indexers); fresh DBs need them for the FK
--    resolution below.
-- ============================================================================
INSERT INTO token (chain_id, address, symbol, decimals)
VALUES
    (1, '\xa632d59b9b804a956bfaa9b48af3a1b74808fc1f'::bytea, 'PYUSDUSDS', 18),
    (1, '\x00836Fe54625BE242BcFA286207795405ca4fD10'::bytea, 'sUSDSUSDT', 18)
ON CONFLICT (chain_id, address) DO NOTHING;

-- ============================================================================
-- 2. Bind Aave V3 -> chainlink. Protocol 'Aave V3' binds only the aave_v3
--    oracle, whose asset set carries no stables (AAVE/CRV/EURC/LDO/LINK/PTs/
--    USDe/sUSDe/syrupUSDT, verified live 2026-07-13), so the aEthUSDC,
--    aEthUSDS, aEthUSDT and aEthRLUSD receipt positions price to NULL in the
--    receipt path. chainlink has enabled USDC/USDT/USDS rows (20260212) and
--    gains RLUSD in step 4. Same precedent as the maple -> chainlink binding
--    in 20260709_120000_add_er_missing_price_feeds.sql.
--    from_block = Aave V3 deploy block 16291127 (later-of-deploys convention;
--    chainlink, 10606501, predates it).
--    Consequence: Aave V3 receipts now resolve the LATEST price across
--    aave_v3 + chainlink rows; both are USD prices of the same underlying, so
--    whichever block is newer is the better answer.
-- ============================================================================
INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT p.id, o.id, 16291127
FROM protocol p, oracle o
WHERE p.chain_id = 1 AND p.name = 'Aave V3' AND o.name = 'chainlink'
ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING;

-- ============================================================================
-- 3. Bind SparkLend -> chainlink, and retire the six frozen sparklend
--    oracle_asset rows. When SparkLend offboarded DAI, GNO, PYUSD, USDC,
--    USDS and USDT, their oracle sources were pinned to constants
--    (cast-verified 2026-07-13: getAssetPrice(DAI) returns a fixed
--    1.00000000, source 0x42a03F81dd8A1cEcD746dc262e4d1CD9fD39F777), so the
--    worker's change-suppression stopped writing rows for them at block
--    24419890 (2026-02-09 14:45:23 UTC, warehouse-verified) and the receipt
--    path has resolved those stale fixed-$1 rows ever since. Republished
--    (reorged) blocks bypass change-suppression, so the frozen assets still
--    re-emit fixed-$1 rows at fresh block numbers (warehouse-verified at
--    block 25524078, block_version 1, 2026-07-13); left enabled they would
--    intermittently outrank chainlink in the LATEST-across-oracles
--    resolution below. Disabling the six rows retires them effective
--    IMMEDIATELY at read time, not merely for future collection: every
--    current/latest onchain_token_price read across the API repositories
--    excludes a price whose (oracle_id, token_id) lacks an enabled
--    oracle_asset mapping (see the enabled-mapping filter on
--    _DIRECT_ASSET_HOLDINGS_SQL in allocation_position_repository.py), so the
--    already-written frozen rows — including any reorg-republished at fresh
--    block heights — stop resolving at once. It also stops fresh collection: the row drops out of the unit's
--    aave-style multicall (the batch is registry-driven, not reserve-list
--    driven). The sparklend oracle keeps its 12 live reserves (WETH,
--    BTC-class, sDAI, sUSDS, ...). chainlink DAI is fresh (1h heartbeat,
--    verified live).
--    from_block = SparkLend deploy block 16776401 (later-of-deploys;
--    chainlink predates it).
--    Consequence: SparkLend receipts now resolve the LATEST price across
--    sparklend + chainlink rows (both USD): the offboarded stables resolve
--    to live chainlink prices, and still-live reserves keep whichever row is
--    newest.
-- ============================================================================
INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
SELECT p.id, o.id, 16776401
FROM protocol p, oracle o
WHERE p.chain_id = 1 AND p.name = 'SparkLend' AND o.name = 'chainlink'
ON CONFLICT (protocol_id, oracle_id, from_block) DO NOTHING;

UPDATE oracle_asset oa
SET enabled = false
FROM oracle o, token t
WHERE o.name = 'sparklend' AND oa.oracle_id = o.id
  AND t.id = oa.token_id AND t.chain_id = 1
  AND t.address IN (
      '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea, -- DAI
      '\x6810e776880C02933D47DB1b9fc05908e5386b96'::bytea, -- GNO
      '\x6c3ea9036406852006290770BEdFcAbA0e23A0e8'::bytea, -- PYUSD
      '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea, -- USDC
      '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea, -- USDS
      '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea  -- USDT
  );

-- protocol_oracle semantics, restated: the comments installed by 20260609
-- described the table as a temporal assignment ("active row = highest
-- from_block <= query block"), which steps 2 and 3 deliberately break: a
-- protocol may bind several oracles CONCURRENTLY, and every consumer
-- (the allocations receipt join, the backfiller's valid-from computation)
-- resolves bindings as a union, taking the latest price row across all of a
-- protocol's bound oracles.
COMMENT ON TABLE public.protocol_oracle IS
  '[Configuration] Binds a protocol to the oracles that price its assets. A protocol may '
  'bind several oracles concurrently; consumers resolve the set as a union, taking the '
  'latest price row across all bound oracles.';
COMMENT ON COLUMN public.protocol_oracle.from_block IS
  'Earliest block at which this binding is meaningful: the later of the protocol''s and '
  'the oracle''s deploy blocks. A lower bound only (the backfiller clamps ranges to it), '
  'not a supersession marker; concurrent bindings all stay live.';

-- ============================================================================
-- 4. RLUSD / Chainlink "RLUSD / USD" (8 decimals). The aave_v3_rwa RLUSD row
--    (20260709) only serves protocols bound to aave_v3_rwa; aEthRLUSD sits
--    under 'Aave V3', so RLUSD needs a row under an Aave-V3-bound oracle.
--    Feed from the Chainlink reference data directory (canonical rlusd-usd
--    entry, not hidden or deprecated), cast-verified 2026-07-13:
--    description() = 'RLUSD / USD', decimals() = 8, answer 0.99998, updated
--    2026-07-12 19:53:47 UTC against a 24h heartbeat. Token row seeded in
--    20260709.
-- ============================================================================
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true, '\x26C46B7aD0012cA71F2298ada567dC9Af14E7f2A'::bytea, 8, 'USD'
FROM oracle o, token t
WHERE o.name = 'chainlink' AND t.chain_id = 1
  AND t.address = '\x8292Bb45bf1Ee4d140127049757C2E0fF06317eD'::bytea
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- ============================================================================
-- 5. Register sparkUSDS (Spark USDS Vault, MetaMorpho v1.1) under Morpho Blue
--    with USDS underlying, mirroring morpho_indexer's discoverAndRegisterVault
--    upserts, same precedent as sparkUSDCbc in 20260709. Identification
--    cast-verified 2026-07-13: MORPHO() = the Morpho Blue singleton,
--    asset() = USDS, runtime bytecode identical to sparkUSDCbc modulo
--    constructor immutables, and the v1.1 factory 0x1897A899...535c24 emitted
--    CreateMetaMorpho for it at block 22932160 (tx 0x45e15c0208bbd56a03b719
--    d55555aa0b57e7907f8522b6db273455d8a894bb5e). Live DBs already carry the
--    morpho_vault row (no-op here) but NOT the receipt_token row, which is
--    the missing piece: without it the position falls to the direct-holdings
--    path, where a vault share has no oracle price. The allocation tracker
--    already populates underlying_value and underlying_token_id (USDS) for
--    it, so the receipt path prices it via Morpho Blue -> chainlink (20260709)
--    and the chainlink USDS feed (20260212) as soon as this row exists.
--    vault_version 2 = MetaMorpho V1.1 (entity.MorphoVaultV1_1);
--    created_at_block 23076416 = the discovery block on the live morpho_vault
--    row, so the indexer's LEAST(created_at_block) receipt upsert and
--    DO UPDATE vault upsert both no-op.
-- ============================================================================
INSERT INTO morpho_vault (chain_id, protocol_id, address, name, symbol, asset_token_id, vault_version, created_at_block)
SELECT 1, p.id, '\xe41A0583334f0dc4e023Acd0BFEF3667F6FE0597'::bytea,
       'Spark USDS Vault', 'sparkUSDS', t.id, 2, 23076416
FROM protocol p, token t
WHERE p.chain_id = 1 AND p.name = 'Morpho Blue'
  AND t.chain_id = 1 AND t.address = '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea
ON CONFLICT (chain_id, address) DO NOTHING;

INSERT INTO receipt_token (chain_id, protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block, metadata, updated_at)
SELECT 1, p.id, t.id, '\xe41A0583334f0dc4e023Acd0BFEF3667F6FE0597'::bytea,
       'sparkUSDS', 23076416, '{}'::jsonb, NOW()
FROM protocol p, token t
WHERE p.chain_id = 1 AND p.name = 'Morpho Blue'
  AND t.chain_id = 1 AND t.address = '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea
ON CONFLICT (chain_id, receipt_token_address) DO NOTHING;

-- ============================================================================
-- 6. Two curve_lp_ng oracles, following the AUSDUSDC pattern in 20260709.
--    Both pools cast-verified 2026-07-13 as 2-coin StableSwap-NG (coins(2)
--    reverts; stored_rates/D_ma_time/ma_exp_time present; the pool is its own
--    LP ERC-20). Shipped enabled: the gate that kept 20260709's oracle
--    disabled is met, staging and prod both run a curve_lp_ng-capable image
--    (see 20260713_120000_enable_curve_ausdusdc_lp_oracle.sql).
--    deployment_block for each found by bisecting eth_getCode over an archive
--    node (empty at N-1, code at N).
--
--    PYUSD/USDS pool ("Spark.fi PYUSD Reserve"): coin0 PYUSD (6 dec), coin1
--    USDS (18 dec), stored_rates = [1e30, 1e18], pure decimals scaling, no
--    rate oracle. Formula check at live balances: vp x min(coin USD feeds) =
--    1.00107599 vs balances-based value per share 1.00107649 (-0.00005%).
--
--    sUSDS/USDT pool ("Spark.fi USDT Reserve"): coin0 sUSDS (18 dec, ERC4626
--    wrapper of USDS), coin1 USDT (6 dec). This pool carries a live rate
--    oracle: stored_rates[0] = sUSDS.convertToAssets(1e18) (1.102861e18 at
--    verification), so get_virtual_price() is denominated in UNDERLYING
--    (USDS-equivalent) units. The correct min() is therefore over
--    underlying-unit USD prices, USDS/USD for the sUSDS leg (no sUSDS/USD
--    feed exists in the Chainlink reference data directory anyway, checked
--    2026-07-13). Empirical check at live balances: vp x min(USDS, USDT) =
--    1.0324790 vs the balances-x-rates value per share 1.0325948 (-0.011%,
--    the safe direction: min() can only underprice). Residual trust: the pool
--    reports the wrapper's CONTRACT rate, so a lying convertToAssets is
--    outside this formula's guard, the same trust model as the erc4626_share
--    oracle type (20260626).
-- ============================================================================
INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, enabled, price_decimals)
VALUES
    ('curve_pyusdusds_lp', 'Curve PYUSD/USDS LP (StableSwap-NG)', 1,
     '\xa632d59b9b804a956bfaa9b48af3a1b74808fc1f'::bytea, 'curve_lp_ng', 23301123, true, 8),
    ('curve_susdsusdt_lp', 'Curve sUSDS/USDT LP (StableSwap-NG)', 1,
     '\x00836Fe54625BE242BcFA286207795405ca4fD10'::bytea, 'curve_lp_ng', 22219093, true, 8)
ON CONFLICT (name) DO NOTHING;

-- Each LP token appears TWICE under its oracle, once per pool coin feed (the
-- (oracle_id, token_id, feed_address) uniqueness supports this); the worker
-- prices the LP as get_virtual_price() x min(coin USD feeds). All feeds are
-- the canonical Chainlink proxies already carried by the chainlink oracle
-- rows (PYUSD/USDS/USDT, 20260212, re-verified against the reference data
-- directory and cast 2026-07-13), all 8 decimals.
-- PYUSD/USDS pool: coin0 PYUSD via PYUSD/USD, coin1 USDS via USDS/USD.
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true, f.feed::bytea, 8, 'USD'
FROM (VALUES
    ('\x8f1dF6D7F2db73eECE86a18b4381F4707b918FB1'),
    ('\xfF30586cD0F29eD462364C7e81375FC0C71219b1')
) AS f(feed)
CROSS JOIN oracle o
JOIN token t ON t.chain_id = 1 AND t.address = '\xa632d59b9b804a956bfaa9b48af3a1b74808fc1f'::bytea
WHERE o.name = 'curve_pyusdusds_lp'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- sUSDS/USDT pool: coin0 sUSDS via USDS/USD (the underlying's feed, see the
-- rate-oracle rationale in the step comment), coin1 USDT via USDT/USD.
INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
SELECT o.id, t.id, true, f.feed::bytea, 8, 'USD'
FROM (VALUES
    ('\xfF30586cD0F29eD462364C7e81375FC0C71219b1'),
    ('\x3E7d1eAB13ad0104d2750B8863b489D65364e32D')
) AS f(feed)
CROSS JOIN oracle o
JOIN token t ON t.chain_id = 1 AND t.address = '\x00836Fe54625BE242BcFA286207795405ca4fD10'::bytea
WHERE o.name = 'curve_susdsusdt_lp'
ON CONFLICT (oracle_id, token_id, feed_address) WHERE feed_address IS NOT NULL DO NOTHING;

-- feed_address's per-type role, extended for rate-oracled NG coins (full text
-- restated; last stated in 20260709).
COMMENT ON COLUMN public.oracle_asset.feed_address IS
  'Specific feed contract for this token (20 bytes). Null for Aave-style oracles. '
  'For chainlink_feed / chronicle / redstone: the priced token''s own feed, denominated in '
  'quote_currency (non-USD quotes are converted to USD via USD reference feeds). '
  'For erc4626_share: the UNDERLYING token''s USD feed (not the share token''s own feed); '
  'the binary reads this address via DirectCaller to obtain the underlying spot price. '
  'For curve_lp_ng: one row per pool coin, each carrying that coin''s USD feed; the '
  'worker prices the LP token as get_virtual_price() x min(coin feed prices). If the '
  'pool rate-oracles a coin (stored_rates applies an on-chain exchange rate, e.g. an '
  'ERC4626 wrapper), that coin''s row carries its UNDERLYING token''s USD feed instead: '
  'get_virtual_price is denominated in underlying units, so min() must compare '
  'underlying-unit prices.';

-- ============================================================================
-- Resolution assertions (precedent: 20260709). Every INSERT above resolves
-- FKs by natural key with ON CONFLICT DO NOTHING, so a typoed address or
-- missing registry row would silently insert nothing. Fail the migration
-- instead of shipping a silent hole. Where a VALUES join does not pin every
-- key of a uniqueness constraint, COUNT(DISTINCT ...) keeps a duplicated
-- match from masking a missing one.
-- ============================================================================
DO $$
DECLARE cnt INT;
BEGIN
    SELECT COUNT(*) INTO cnt
    FROM protocol_oracle po
    JOIN protocol p ON p.id = po.protocol_id AND p.chain_id = 1 AND p.name = 'Aave V3'
    JOIN oracle o ON o.id = po.oracle_id AND o.name = 'chainlink'
    WHERE po.from_block = 16291127;
    IF cnt < 1 THEN
        RAISE EXCEPTION 'Aave V3 -> chainlink protocol_oracle binding missing';
    END IF;

    SELECT COUNT(*) INTO cnt
    FROM protocol_oracle po
    JOIN protocol p ON p.id = po.protocol_id AND p.chain_id = 1 AND p.name = 'SparkLend'
    JOIN oracle o ON o.id = po.oracle_id AND o.name = 'chainlink'
    WHERE po.from_block = 16776401;
    IF cnt < 1 THEN
        RAISE EXCEPTION 'SparkLend -> chainlink protocol_oracle binding missing';
    END IF;

    -- The bindings only help if chainlink actually prices every underlying the
    -- fixed receipts resolve through: USDC, USDT, USDS, RLUSD (Aave V3) and
    -- DAI, PYUSD (SparkLend; spPYUSD is the largest re-routed position),
    -- resolved by token ADDRESS (labels are not authoritative).
    -- COUNT(DISTINCT t.id): (oracle_id, token_id, feed_address) uniqueness
    -- allows several enabled feed rows per token, so a raw row count could
    -- reach 6 with one token duplicated and another missing.
    SELECT COUNT(DISTINCT t.id) INTO cnt
    FROM (VALUES
        ('\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'::bytea),
        ('\xdac17f958d2ee523a2206206994597c13d831ec7'::bytea),
        ('\xdc035d45d973e3ec169d2276ddab16f1e407384f'::bytea),
        ('\x8292bb45bf1ee4d140127049757c2e0ff06317ed'::bytea),
        ('\x6b175474e89094c44da98b954eedeac495271d0f'::bytea),
        ('\x6c3ea9036406852006290770bedfcaba0e23a0e8'::bytea)
    ) AS x(token_address)
    JOIN token t ON t.chain_id = 1 AND t.address = x.token_address
    JOIN oracle o ON o.name = 'chainlink'
    JOIN oracle_asset oa ON oa.oracle_id = o.id AND oa.token_id = t.id AND oa.enabled;
    IF cnt <> 6 THEN
        RAISE EXCEPTION 'expected enabled chainlink oracle_asset rows for USDC/USDT/USDS/RLUSD/DAI/PYUSD, found %', cnt;
    END IF;

    -- RLUSD row strictness: enabled with the exact verified feed and decimals
    -- (a pre-existing row with the same key but enabled=false or wrong
    -- decimals would make the ON CONFLICT DO NOTHING a silent no-op).
    SELECT COUNT(*) INTO cnt
    FROM oracle_asset oa
    JOIN oracle o ON o.id = oa.oracle_id AND o.name = 'chainlink'
    JOIN token t ON t.id = oa.token_id AND t.chain_id = 1
     AND t.address = '\x8292Bb45bf1Ee4d140127049757C2E0fF06317eD'::bytea
    WHERE oa.feed_address = '\x26C46B7aD0012cA71F2298ada567dC9Af14E7f2A'::bytea
      AND oa.enabled AND oa.feed_decimals = 8 AND oa.quote_currency = 'USD';
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'expected 1 enabled USD chainlink RLUSD oracle_asset row (feed decimals 8), found %', cnt;
    END IF;

    -- Step 3's retirement: none of the six frozen reserves may remain enabled
    -- under sparklend (fresh DBs may lack some of the rows entirely; zero
    -- enabled is the invariant either way).
    SELECT COUNT(*) INTO cnt
    FROM oracle_asset oa
    JOIN oracle o ON o.id = oa.oracle_id AND o.name = 'sparklend'
    JOIN token t ON t.id = oa.token_id AND t.chain_id = 1
    WHERE oa.enabled AND t.address IN (
        '\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea,
        '\x6810e776880C02933D47DB1b9fc05908e5386b96'::bytea,
        '\x6c3ea9036406852006290770BEdFcAbA0e23A0e8'::bytea,
        '\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea,
        '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea,
        '\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea
    );
    IF cnt <> 0 THEN
        RAISE EXCEPTION 'expected 0 enabled sparklend oracle_asset rows for the offboarded reserves, found %', cnt;
    END IF;

    -- The sparklend unit must keep live assets: the worker fails startup on
    -- an enabled oracle with zero enabled assets.
    SELECT COUNT(*) INTO cnt
    FROM oracle_asset oa
    JOIN oracle o ON o.id = oa.oracle_id AND o.name = 'sparklend'
    WHERE oa.enabled;
    IF cnt < 1 THEN
        RAISE EXCEPTION 'sparklend oracle left with no enabled assets';
    END IF;

    -- sparkUSDS: both registry rows must resolve with the USDS underlying.
    SELECT COUNT(*) INTO cnt
    FROM morpho_vault mv
    JOIN protocol p ON p.id = mv.protocol_id AND p.chain_id = 1 AND p.name = 'Morpho Blue'
    JOIN token t ON t.id = mv.asset_token_id
     AND t.address = '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea
    WHERE mv.chain_id = 1 AND mv.address = '\xe41A0583334f0dc4e023Acd0BFEF3667F6FE0597'::bytea;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'sparkUSDS morpho_vault row missing or mis-linked, found %', cnt;
    END IF;

    SELECT COUNT(*) INTO cnt
    FROM receipt_token rt
    JOIN protocol p ON p.id = rt.protocol_id AND p.chain_id = 1 AND p.name = 'Morpho Blue'
    JOIN token t ON t.id = rt.underlying_token_id
     AND t.address = '\xdC035D45d973E3EC169d2276DDab16f1e407384F'::bytea
    WHERE rt.chain_id = 1 AND rt.receipt_token_address = '\xe41A0583334f0dc4e023Acd0BFEF3667F6FE0597'::bytea;
    IF cnt <> 1 THEN
        RAISE EXCEPTION 'sparkUSDS receipt_token row missing or mis-linked, found %', cnt;
    END IF;

    -- Curve oracles: enabled, correct type, exact pool address and deploy
    -- block (an ON CONFLICT (name) no-op against a divergent pre-existing row
    -- must fail here, not stall the worker at startup).
    SELECT COUNT(*) INTO cnt
    FROM (VALUES
        ('curve_pyusdusds_lp', '\xa632d59b9b804a956bfaa9b48af3a1b74808fc1f'::bytea, 23301123),
        ('curve_susdsusdt_lp', '\x00836Fe54625BE242BcFA286207795405ca4fD10'::bytea, 22219093)
    ) AS x(name, address, deployment_block)
    JOIN oracle o ON o.name = x.name AND o.oracle_type = 'curve_lp_ng'
     AND o.chain_id = 1 AND o.address = x.address
     AND o.deployment_block = x.deployment_block
     AND o.enabled AND o.price_decimals = 8;
    IF cnt <> 2 THEN
        RAISE EXCEPTION 'expected 2 enabled curve_lp_ng oracles with verified pool address and deploy block, found %', cnt;
    END IF;

    -- Per-pool coin feeds: exactly the 2 expected feeds, enabled, 8 decimals,
    -- attached to the pool's own LP token (the unit builder hard-fails
    -- startup on fewer than 2 feeds or a token mismatch; fail the migration
    -- instead). COUNT(DISTINCT oa.feed_address): a duplicated feed must not
    -- mask a missing one.
    SELECT COUNT(DISTINCT oa.feed_address) INTO cnt
    FROM (VALUES
        ('\x8f1dF6D7F2db73eECE86a18b4381F4707b918FB1'::bytea),
        ('\xfF30586cD0F29eD462364C7e81375FC0C71219b1'::bytea)
    ) AS f(feed_address)
    JOIN oracle o ON o.name = 'curve_pyusdusds_lp'
    JOIN oracle_asset oa ON oa.oracle_id = o.id AND oa.feed_address = f.feed_address
     AND oa.enabled AND oa.feed_decimals = 8 AND oa.quote_currency = 'USD'
    JOIN token t ON t.id = oa.token_id AND t.chain_id = 1
     AND t.address = '\xa632d59b9b804a956bfaa9b48af3a1b74808fc1f'::bytea AND t.decimals = 18;
    IF cnt <> 2 THEN
        RAISE EXCEPTION 'expected 2 distinct enabled curve_pyusdusds_lp coin feed rows on the LP token, found %', cnt;
    END IF;

    SELECT COUNT(DISTINCT oa.feed_address) INTO cnt
    FROM (VALUES
        ('\xfF30586cD0F29eD462364C7e81375FC0C71219b1'::bytea),
        ('\x3E7d1eAB13ad0104d2750B8863b489D65364e32D'::bytea)
    ) AS f(feed_address)
    JOIN oracle o ON o.name = 'curve_susdsusdt_lp'
    JOIN oracle_asset oa ON oa.oracle_id = o.id AND oa.feed_address = f.feed_address
     AND oa.enabled AND oa.feed_decimals = 8 AND oa.quote_currency = 'USD'
    JOIN token t ON t.id = oa.token_id AND t.chain_id = 1
     AND t.address = '\x00836Fe54625BE242BcFA286207795405ca4fD10'::bytea AND t.decimals = 18;
    IF cnt <> 2 THEN
        RAISE EXCEPTION 'expected 2 distinct enabled curve_susdsusdt_lp coin feed rows on the LP token, found %', cnt;
    END IF;
END $$;

INSERT INTO migrations (filename)
VALUES ('20260713_150000_price_dust_allocation_rows.sql')
ON CONFLICT (filename) DO NOTHING;
