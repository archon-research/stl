-- Curate, per pool, the Curve ABI facts the snapshot cannot discover at runtime,
-- and make the stableswap config row able to hold what the later NG pools
-- actually expose (VEC-330/331).
--
-- Three implementation generations are in play, each dropping getters the one
-- before it had (all measured on mainnet 2026-09-16):
--
--                        pre-NG      NG (original)   NG (later)
--   no-arg oracle getters  n/a          yes             NO
--   calc_token_amount      uint256[N]   uint256[N]      uint256[]
--   future_fee()           yes          yes             NO
--   future_admin_fee()     yes          NO              NO
--   offpeg_fee_multiplier() NO          NO              yes
--
-- pool_kind already distinguishes pre-NG from NG, but not the two NG
-- generations, and every row above that the snapshot reads unconditionally is a
-- stalled chain on the pools that lack it: an issued read that reverts is an
-- error, not a NULL, and one pool's error aborts the whole block.
--
-- The Curve snapshot issues price_oracle(), last_price(), ema_price(), get_p()
-- and oracle_method() for every pool_kind='plain_ng' row. That gate is the pool
-- CLASS, but the capability is per IMPLEMENTATION: the original NG pools (of the
-- seeded set, only stETH-ng 0x21E27a5E) answer all five, while later NG
-- implementations expose only the indexed price_oracle(uint256) form and REVERT
-- on the no-arg selector. An issued snapshot read that reverts is an error, not
-- a NULL (no swallowed failures), and one pool's error aborts the whole block --
-- so seeding one such pool stalls Curve indexing for every pool on the chain.
--
-- This column moves that decision from the class to the pool, exactly as
-- has_a_precise already does for A_precise(). It is one flag for all five
-- getters because they are one implementation trait, not five: measured on
-- mainnet 2026-09-16, all five answer on stETH-ng and all five revert on each of
-- the five ARCT-384 pools. Set it TRUE only when every one of the five answers;
-- a pool that answers some is a new case that splits this column.
--
-- DEFAULT FALSE is the safe direction: an unprobed pool issues no call that
-- could revert, and loses at most the five values. The one existing pool that
-- does expose them is backfilled below.

ALTER TABLE curve_pool
    ADD COLUMN IF NOT EXISTS has_no_arg_oracle_getters BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN curve_pool.has_no_arg_oracle_getters IS
  'Configuration. Whether the pool exposes the no-arg oracle getters price_oracle(), last_price(), ema_price(), get_p() and oracle_method(), curated at seed time by calling all five on chain and used to gate those five snapshot reads. TRUE only for plain_ng pools on the original Stableswap-NG implementation (they answer all five); FALSE for later plain_ng implementations, which serve only the indexed price_oracle(uint256) form and revert on the no-arg selector, and FALSE for every non-NG pool, which never issues them.';

-- stETH-ng is the only pool_kind='plain_ng' row seeded before this column
-- existed, and all five getters answer on it (verified on mainnet), so it keeps
-- the reads it has always issued.
UPDATE curve_pool
SET has_no_arg_oracle_getters = TRUE
WHERE chain_id = 1
  AND pool_address = '\x21E27a5E5513D6e65C4f830167390997aA84843a'::bytea
  AND pool_kind = 'plain_ng';

DO $$
DECLARE
    ng_without_capability TEXT;
BEGIN
    -- Any other plain_ng pool that predates this column would silently lose its
    -- five reads to the FALSE default; there is none today, and a future one
    -- must be probed and curated rather than defaulted.
    SELECT string_agg(encode(pool_address, 'hex'), ', ') INTO ng_without_capability
    FROM curve_pool
    WHERE pool_kind = 'plain_ng'
      AND NOT has_no_arg_oracle_getters;
    IF ng_without_capability IS NOT NULL THEN
        RAISE EXCEPTION 'plain_ng pools exist that this migration did not curate: %. Probe price_oracle()/last_price()/ema_price()/get_p()/oracle_method() on each and set has_no_arg_oracle_getters explicitly', ng_without_capability;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- calc_token_amount argument shape.
--
-- calc_token_amount(uint256[N],bool) and calc_token_amount(uint256[],bool) are
-- different selectors, and a pool that implements one REVERTS on the other. The
-- snapshot has to choose before it calls, and nothing in a return value reveals
-- the choice, so it is curated. Measured on mainnet 2026-09-16: the pre-NG pools
-- (stETH classic, 3pool), the cryptoswap pool (TricryptoUSDC) and the original
-- NG pool (stETH-ng) all take the fixed array; later NG implementations take the
-- DynArray.
--
-- NULL means "not probed", and the snapshot then issues no calc_token_amount
-- call at all. Unlike the flag above there is no safe boolean default here --
-- either shape reverts on the pools that use the other -- so absence has to be
-- representable, and it costs one nullable column instead of a stalled chain.
ALTER TABLE curve_pool
    ADD COLUMN IF NOT EXISTS calc_token_amount_dyn_array BOOLEAN;

COMMENT ON COLUMN curve_pool.calc_token_amount_dyn_array IS
  'Configuration. Argument shape of the pool''s calc_token_amount: TRUE = dynamic calc_token_amount(uint256[],bool), FALSE = fixed calc_token_amount(uint256[N],bool), NULL = not probed. The two are different selectors and each reverts on a pool implementing the other, and the shape is not discoverable from any return value, so it is curated at seed time by calling both. NULL gates the calc_token_amount snapshot read out entirely, leaving a structural NULL rather than risking a revert that would stop every block on the chain.';

UPDATE curve_pool
SET calc_token_amount_dyn_array = FALSE
WHERE chain_id = 1
  AND pool_address IN (
      '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'::bytea,  -- stETH classic, plain_pre_ng
      '\x21E27a5E5513D6e65C4f830167390997aA84843a'::bytea,  -- stETH-ng,      plain_ng
      '\xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7'::bytea,  -- 3pool,         plain_pre_ng
      '\x7F86Bf177Dd4F3494b841a37e810A34dD56c829B'::bytea   -- TricryptoUSDC, cryptoswap
  );

DO $$
DECLARE
    unprobed TEXT;
BEGIN
    -- Every pool seeded before this column existed was issuing calc_token_amount
    -- and must keep doing so; a NULL left here would silently drop the read.
    SELECT string_agg(encode(pool_address, 'hex'), ', ') INTO unprobed
    FROM curve_pool
    WHERE calc_token_amount_dyn_array IS NULL;
    IF unprobed IS NOT NULL THEN
        RAISE EXCEPTION 'curve pools exist whose calc_token_amount argument shape this migration did not curate: %. Call calc_token_amount(uint256[N],bool) and calc_token_amount(uint256[],bool) on each and set calc_token_amount_dyn_array explicitly', unprobed;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Fee-schedule getters: future_fee() vs offpeg_fee_multiplier().
--
-- The later NG implementations dropped future_fee() for offpeg_fee_multiplier()
-- and revert on the old selector. No pool exposes both, so each getter is gated
-- on its own flag rather than one flag being read as "the other one, then":
-- cryptoswap pools expose neither and issue neither.
--
-- FALSE is the safe default for both -- an unprobed pool issues no call that
-- could revert -- so the pools that do expose future_fee() are backfilled below.
ALTER TABLE curve_pool
    ADD COLUMN IF NOT EXISTS has_future_fee            BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS has_offpeg_fee_multiplier BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN curve_pool.has_future_fee IS
  'Configuration. Whether the pool exposes the no-arg future_fee() getter, curated at seed time and used to gate that snapshot read. TRUE for the pre-NG pools and the original Stableswap-NG implementation; FALSE for later NG implementations, which dropped it for offpeg_fee_multiplier() and revert on it, and FALSE for cryptoswap pools, which expose neither.';
COMMENT ON COLUMN curve_pool.has_offpeg_fee_multiplier IS
  'Configuration. Whether the pool exposes the no-arg offpeg_fee_multiplier() getter, curated at seed time and used to gate that snapshot read. TRUE only for the later Stableswap-NG implementations, where it replaces future_fee(); FALSE everywhere else, including the pre-NG and original NG pools that revert on it.';

UPDATE curve_pool
SET has_future_fee = TRUE
WHERE chain_id = 1
  AND pool_address IN (
      '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'::bytea,  -- stETH classic, plain_pre_ng
      '\x21E27a5E5513D6e65C4f830167390997aA84843a'::bytea,  -- stETH-ng,      plain_ng
      '\xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7'::bytea   -- 3pool,         plain_pre_ng
  );

DO $$
DECLARE
    missing TEXT;
BEGIN
    -- Every stableswap pool seeded before this column existed was issuing
    -- future_fee() into a NOT NULL column, so all of them must keep it.
    SELECT string_agg(encode(pool_address, 'hex'), ', ') INTO missing
    FROM curve_pool
    WHERE pool_kind IN ('plain_pre_ng', 'plain_ng')
      AND NOT has_future_fee;
    IF missing IS NOT NULL THEN
        RAISE EXCEPTION 'stableswap pools exist that this migration did not curate for future_fee(): %. Call future_fee() and offpeg_fee_multiplier() on each and set the flags explicitly', missing;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- curve_stableswap_config: hold both fee-schedule shapes.
--
-- future_fee was NOT NULL because every pool seeded so far answers future_fee().
-- The later NG pools do not, and the column has to be able to say so: a
-- structural NULL is the honest value for a getter the contract does not have.
-- Existing rows all carry a value and are untouched.
ALTER TABLE curve_stableswap_config
    ALTER COLUMN future_fee DROP NOT NULL;

ALTER TABLE curve_stableswap_config
    ADD COLUMN IF NOT EXISTS offpeg_fee_multiplier NUMERIC;

COMMENT ON COLUMN curve_stableswap_config.future_fee IS
  'Queued swap fee from future_fee(), in Curve fee units where 1e10 = 100%. NULL on the later Stableswap-NG pools, which expose no future_fee() (see curve_pool.has_future_fee) and carry offpeg_fee_multiplier instead -- a NULL here is that structural absence, never a failed read.';
COMMENT ON COLUMN curve_stableswap_config.offpeg_fee_multiplier IS
  'Off-peg fee multiplier from offpeg_fee_multiplier(), raw contract units (1e10 = 1x, so 2e11 = 20x), the later Stableswap-NG replacement for the queued-fee mechanism. NULL on every pool that exposes no such getter (see curve_pool.has_offpeg_fee_multiplier) -- a structural absence, never a failed read.';

INSERT INTO migrations (filename)
VALUES ('20260831_110000_curve_pool_abi_capabilities.sql')
ON CONFLICT (filename) DO NOTHING;
