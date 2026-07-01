# VEC-261 — Uniswap V3 Indexer: Design

**Ticket:** VEC-261 (parent VEC-368 DEX pipeline; enables VEC-79 Galaxy position data)
**Date:** 2026-07-01
**Base branch:** `vec-261-uniswap-v3-indexer`, stacked on `vec-260-curve-indexer`
**Status:** Approved design, pending implementation plan

---

## 1. Goal & context

Index Uniswap V3 pool state to provide an **independent on-chain price + depth signal** for the Galaxy deal's wstETH/stETH (LST) collateral, and to detect LST de-pegs. This is one of three per-DEX workers under VEC-368 (Curve = VEC-260, Balancer = VEC-262), all stacking on the shared DEX base (VEC-328) and consuming the watcher's block stream over SQS. The watcher itself is untouched (repo convention: per-protocol logic lives in workers).

"Get all the data": capture **every** event the pool contract emits and **every** economically-meaningful state value, including the full per-tick liquidity map — verified against Uniswap v3-core source and mainnet, not the summary spec doc.

## 2. Scope

**In scope**
- New worker `cmd/workers/uniswap-v3-indexer` + service `internal/services/uniswapv3indexer`.
- All 9 pool events → typed fact tables + shared `protocol_event` capture-net (complete superset).
- Per-touched-block pool-level state snapshot (full slot0, liquidity, fee growth, protocol fees, real token balances, TWAP).
- Full per-tick liquidity map (authoritative `ticks()` reads).
- 18 active wstETH/stETH pools (see §4), data-driven from a registry table.
- Hoist genuinely cross-DEX plumbing out of `curveindexer` into the shared layer (§9).
- Fold two Curve correctness fixes found in review (§10): deploy-block gating (A1) and block-hash-pinned state reads (A2).

**Out of scope** (own tickets)
- Per-NFT LP positions via NonfungiblePositionManager (V3 analogue of VEC-377).
- Watchlist registry + auto-discovery (VEC-330 / VEC-331) — this worker is registry-driven, so those are additive.
- USD/TVL derivations and cross-pool price synthesis (read-time concern).
- Non-mainnet chains; historical backfill beyond what the shared SQS/backfill path already provides.

## 3. Architecture

Mirrors the Curve worker (hexagonal, BlockHandler model) and reuses the VEC-328 shared base verbatim:

- **Entrypoint** — a single `cmd/workers/dex-indexer/main.go` binary for ALL DEX indexers, selecting which DEX to run from config (`DEX` env/flag) via a per-DEX **factory registry** (`map[string]Factory`, wired explicitly — no global init). Deployed as one Deployment/pod-set per DEX from the same image. Curve migrates onto it (its `curve-indexer/main.go` is removed). The `Factory` interface lives in the `cmd` package (only `cmd` may import both postgres adapters and service packages, so per-DEX repo↔service wiring belongs there). Flow: `ParseConfig`(+Dex) → `Bootstrap` → `registry[cfg.Dex]` → `ResolveProtocolID(factory.Protocol())` (UniswapV3 factory protocol row already seeded in `20260521_100000_create_dex_prereqs.sql`) → `NewProtocolEventWriter` → `factory.BuildHandler(...)` (loads pools, builds repo+service) → `NewBlockProcessor` → `sqsutil.RunLoop`. `run(ctx, args)` for testability; `main()` at top.
- **Service** `internal/services/uniswapv3indexer`: implements `dexconsumer.BlockHandler`. V3 has a **single pool class**, so no capability probe and a trivial handler registry (unlike Curve's stableswap/cryptoswap split). Orchestration reads like prose: `decodeBlockEvents → buildSnapshotSet → snapshotPools (before tx) → quiet-block skip → buildBlockWrites → persistBlock (one tx)`.
- **Ports/adapters**: reuse `Multicaller`, `BlockCacheReader`, `TxManager`, `ProtocolRepository`, `TokenRepository`, `EventRepository`. New `outbound.UniswapV3Repository` (`LoadPools`, `SaveBlock(tx, BlockWrites)`) with a postgres adapter.
- **Data flow**: watcher SNS → SQS → worker; per block: decode logs for watched pools, multicall pool-level + touched-tick state (pinned to block hash), persist swaps/liquidity events/tick rows/state/pool events + protocol_event mirror in one transaction.

## 4. Pool set (18; cast-verified mainnet 2026-07-01)

Enumerated authoritatively via factory `PoolCreated` scan (116 pools contain wstETH/stETH; 26 active). **Selection principle:** counterparty ∈ {ETH, major USD stablecoin, recognized ETH LST/LRT}. The worker reads `token0()/token1()` at load and must **not** assume wstETH is token0 (it is token1 in mstETH/urLRT/boxETH pools).

| # | Pair · fee | Pool | token0 | token1 | deploy |
|---|-----------|------|--------|--------|--------|
| 1 | wstETH/WETH 0.01% | `0x109830a1AAaD605BbF02a9dFA7B0B92EC2FB7dAa` | wstETH | WETH | 15384250 |
| 2 | wstETH/WETH 0.05% | `0xD340B57AAcDD10F96FC1CF10e15921936F41E29c` | wstETH | WETH | 12376093 |
| 3 | wstETH/WETH 0.30% | `0xC12aF0C4AA39D3061c56cD3CB19f5e62dEeaeBdE` | wstETH | WETH | 14943576 |
| 4 | wstETH/USDC 0.05% | `0x4622Df6fB2d9Bee0DCDaCF545aCDB6a2b2f4f863` | wstETH | USDC | 16065412 |
| 5 | wstETH/USDC 0.30% | `0x173821f6aD4c5324cd35753A9FD12D92f2eaAB29` | wstETH | USDC | 14420259 |
| 6 | wstETH/USDT 0.05% | `0xeC5055067d60292Ab2c514A1090Bc8E014e4aBAA` | wstETH | USDT | 18461625 |
| 7 | stETH/WETH 1% ⚠️ | `0x63818BbDd21E69bE108A23aC1E84cBf66399Bd7D` | stETH | WETH | 14937573 |
| 8 | wstETH/weETH 0.05% | `0xf47F04a8605bE181E525D6391233cbA1f7474182` | wstETH | weETH | 19629516 |
| 9 | wstETH/ETHx 0.01% | `0xC9eD5de354D4BE9fd576D3108C7637a71C01faA1` | wstETH | ETHx | 20525114 |
| 10 | wstETH/rsETH 0.05% | `0x7a27c7b7E2536e452c57d3E8b909d9ecba2e2eee` | wstETH | rsETH | 19970144 |
| 11 | wstETH/ezETH 0.05% | `0x1b9d58bEa5eD5d935cc2E818Dde1D796abFf0bc0` | wstETH | ezETH | 20011672 |
| 12 | wstETH/pzETH 0.01% | `0xfc354f5cf57125a7d85E1165f4FCDfd3006db61a` | wstETH | pzETH | 20192842 |
| 13 | wstETH/inETH 0.05% | `0x3c0a1a9e0E22b9Acc9248D9f358286e9e9205b0a` | wstETH | inETH | 19232685 |
| 14 | wstETH/mstETH 0.05% | `0x7f13847459450236d2D233f1c08D742Ed69D2997` | mstETH | wstETH | 20011699 |
| 15 | wstETH/fstETH 1% | `0x526389df2DCc8c5F7af69E93aD9E0d8FC21799f6` | wstETH | fstETH | 16478531 |
| 16 | wstETH/fstETH 0.30% | `0x39683566C148851464781f0673112eF0746B9578` | wstETH | fstETH | 16486285 |
| 17 | urLRT/wstETH 0.01% | `0x104b3e3aCd2396a7292223b5778EA1CACdb68eC9` | urLRT | wstETH | 20832240 |
| 18 | boxETH/wstETH 1% | `0x703A177Fcb4dEf281d180d3619a5edbAE67Ec7b5` | boxETH | wstETH | 23506288 |

Counterparty token addresses to seed into `token` (many LST/LRTs are not yet registered): WETH `0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2`, USDC `0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48`, USDT `0xdAC17F958D2ee523a2206206994597C13D831ec7`, stETH `0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84`, wstETH `0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0`, weETH `0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee`, ETHx `0xA35b1B31Ce002FBF2058D22F30f95D405200A15b`, rsETH `0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7`, ezETH `0xbf5495Efe5DB9ce00f80364C8B423567e58d2110`, pzETH `0x8c9532a60E0e7C6Bbd2b2c1303F63aCE1c3E9811`, inETH `0xf073bAC22DAb7FaF4a3Dd6c6189a70D54110525C`, mstETH `0x49446A0874197839D15395B908328a74ccc96Bc0`, fstETH `0xe2237a34246eeDcEB43283073Ba9adEF0450351E`, urLRT `0x4f3cc6359364004b245ad5be36e6ad4e805dc961`, boxETH `0x7690202e2C2297bcD03664e31116D1dFFE7e3B73`. Each pool's exact decimals confirmed at seed time via `decimals()`.

**Excluded** (active but not a signal): wstETH/HARAMBE (memecoin), wstETH/LRC, NFAi, kpSYM-1, kpZRC-2, JUSD, wUSDM, yETH, wstETH/ankrETH (dust); plus ~90 zero-liquidity pools. All are one-row registry adds later / auto-discovered by VEC-331.

## 5. Complete data surface (verified vs v3-core)

**Events (9, all on the pool contract; filter by pool address):** `Initialize(sqrtPriceX96, tick)`, `Mint(sender, owner, tickLower, tickUpper, amount, amount0, amount1)`, `Burn(owner, tickLower, tickUpper, amount, amount0, amount1)`, `Collect(owner, recipient, tickLower, tickUpper, amount0, amount1)`, `Swap(sender, recipient, amount0, amount1, sqrtPriceX96, liquidity, tick)`, `Flash(sender, recipient, amount0, amount1, paid0, paid1)`, `IncreaseObservationCardinalityNext(old, new)`, `SetFeeProtocol(f0Old, f1Old, f0New, f1New)`, `CollectProtocol(sender, recipient, amount0, amount1)`.

**Pool-level state (per touched block):** `slot0()` → (sqrtPriceX96, tick, observationIndex, observationCardinality, observationCardinalityNext, feeProtocol, unlocked); `liquidity()`; `feeGrowthGlobal0X128()`; `feeGrowthGlobal1X128()`; `protocolFees()` → (token0, token1); `ERC20.balanceOf(pool)` for token0 & token1; `observe([window,0])` → TWAP tick (nullable).

**Per-tick state:** `ticks(int24)` → (liquidityGross, liquidityNet, feeGrowthOutside0X128, feeGrowthOutside1X128, tickCumulativeOutside, secondsPerLiquidityOutsideX128, secondsOutside, initialized). Read only for touched ticks + one-time baseline (§7).

**Immutables (cache once):** factory, token0, token1, fee, tickSpacing, maxLiquidityPerTick.

## 6. Key design decisions (where we deliberately diverge from Curve)

1. **Snapshot on touched blocks only.** V3 pool state is piecewise-constant — it changes only when one of the pool's own functions is called (verified in `UniswapV3Pool.sol`). No periodic "heartbeat" snapshot (Curve's heartbeat would re-write byte-identical rows for V3). Liveness is a metric, not a row.
2. **Tick map via authoritative `ticks()` reads, not delta reconstruction.** Each block, touched ticks = `{tickLower, tickUpper}` of every Mint/Burn/Collect; read `ticks(t)` for exactly those (bounded, a few per block). One-time baseline: on first index of a pool, enumerate initialized ticks by walking `tickBitmap` words across the pool's tick range and read each. No running sums, no genesis backfill, reorg-safe.
3. **Core pool-level columns are NOT NULL** (every V3 pool always has slot0/liquidity/feeGrowth/protocolFees). Do not spread Curve's nullable+AllowFailure pattern over them. Only `observe()`/TWAP is nullable + AllowFailure (reverts with `OLD` when cardinality can't cover the window).
4. **Real reserves via `ERC20.balanceOf(pool)`** (the pool exposes no balances).
5. **Spot price derived from `sqrtPriceX96`** with deterministic math — no `get_dy`-style RPC.
6. **State multicall pinned to the block hash** of the `(number, version)` being processed (see A2, §10) — reorg-correct from day one.

## 7. Database schema

All hypertables carry `processing_version` (correction version) + `build_id` (audit) and an advisory-locked `assign_processing_version_*` BEFORE INSERT trigger; all inserts `ON CONFLICT DO NOTHING`. Every table and column gets a `COMMENT` (data-catalogue requirement) in a sibling comments migration, matching the `[Type]`/`Roles`/unit-scale style. Migration files are new (never modify applied migrations).

- **`uniswap_v3_pool`** (Dimension; read-only at runtime): `id` PK, `chain_id` FK→chain, `protocol_id` FK→protocol, `pool_address` bytea, `token0_id`/`token1_id` FK→token, `fee` int (hundredths of a bip), `tick_spacing` int, `max_liquidity_per_tick` numeric, `deploy_block` bigint, `created_at`. UNIQUE(chain_id, pool_address).
- **`uniswap_v3_pool_state`** (Hypertable; per touched block): keys `(pool_id, block_number, block_version, block_timestamp)` + pv/build. Cols (NOT NULL unless noted): `sqrt_price_x96` numeric, `tick` int, `observation_index` int, `observation_cardinality` int, `observation_cardinality_next` int, `fee_protocol` smallint, `unlocked` bool, `liquidity` numeric, `fee_growth_global0_x128` numeric, `fee_growth_global1_x128` numeric, `protocol_fees_token0` numeric, `protocol_fees_token1` numeric, `balance0` numeric, `balance1` numeric, `twap_tick` int NULL, `twap_window_secs` int NULL. PK `(pool_id, block_timestamp, block_number, block_version, processing_version)`.
- **`uniswap_v3_swap`** (Hypertable; fact): identity + `tx_hash`, `log_index`, `sender` bytea, `recipient` bytea, `amount0` numeric (signed), `amount1` numeric (signed), `sqrt_price_x96` numeric, `liquidity` numeric, `tick` int. PK includes `log_index`.
- **`uniswap_v3_liquidity_event`** (Hypertable; fact): `event_name` varchar CHECK IN ('mint','burn','collect'), `owner` bytea, `sender` bytea NULL (mint only), `recipient` bytea NULL (collect only), `tick_lower` int, `tick_upper` int, `amount` numeric NULL (liquidity delta; null for collect), `amount0` numeric, `amount1` numeric. PK includes `log_index`.
- **`uniswap_v3_tick`** (append-on-change; authoritative per-tick): keys `(pool_id, tick, block_number, block_version, block_timestamp)` + pv/build. Cols: `liquidity_gross` numeric, `liquidity_net` numeric (signed), `fee_growth_outside0_x128` numeric, `fee_growth_outside1_x128` numeric, `initialized` bool. Append-on-change via the shared advisory-locked read-latest-compare-insert helper (§9). PK `(pool_id, tick, block_number, block_version, processing_version)`.
- **`uniswap_v3_pool_event`** (Hypertable; typed low-frequency events): `event_name` varchar CHECK IN ('initialize','flash','set_fee_protocol','collect_protocol','increase_observation_cardinality_next'), `params` jsonb (documented keys per event_name). PK includes `log_index`.

Every log on a watched pool is additionally written to the shared `protocol_event` mirror (complete superset; unknown/edge logs preserved as `{topics,data}`).

## 8. Numeric units (documented in COMMENTs)

`sqrt_price_x96` = Q64.96 fixed point; price(token1/token0) = (sqrtPriceX96/2^96)^2, then adjust by 10^(dec0−dec1). `liquidity`/`liquidity_gross`/`liquidity_net` = raw L (√(xy) scaled by token decimals; **not comparable across pairs**). `fee_growth_*_x128` = Q128.128. `fee` = hundredths of a bip (3000 = 0.30%). `tick` integer; price = 1.0001^tick. `balance0/1` = raw native-decimal ints (scale by the token's decimals). `twap_tick` = arithmetic-mean tick over `twap_window_secs`.

## 9. Consolidation / hoist plan (maintainability-first; reviewer-vetted)

Hoist from `curveindexer` into the shared layer (both DEXes then use one source of truth); leave protocol-shaped code in place.

| Item | To | Note |
|------|----|------|
| ABI-log plumbing: `logBelongsToPool` (via small `{Address}` iface), `decodeLog`, field extractors, `parseHexUint`, capture-net append helpers | `internal/services/shared` (or new `dexdecode`) | zero protocol knowledge |
| ABI unpack utils: type builders, `unpackSingleUint`, `unpackUintArray`, `optionalUintResult` (encodes the no-swallow AllowFailure rule) | `internal/services/shared` | beside existing `UnpackUint` |
| Append-on-change algorithm (advisory-lock → read-latest → compare → insert) | postgres helper `appendOnChange(tx, lockKey, latestReader, equalFn, insertFn)` | field lists stay per-DEX; `uniswap_v3_tick` uses it |
| Snapshot-set bookkeeping (touched + reorg-aware) **with the A1 deploy-block gate folded in** | `dexconsumer` | takes a small `{ID, DeployBlock}` iface |
| BlockHandler orchestrator skeleton | generalize via generics `Decoder[Acc]`/`Snapshotter[Snap]`/`Writer[Acc,Snap]` | riskiest hoist — validate it doesn't force a leaky abstraction; if it does, keep duplicated (maintainability > DRY) |

Keep curve-specific: parameter/LP-token decoders, liquidity word-slicing, capability probe, `Curve*` entities/state model.

## 10. Curve fixes folded in (we own Curve)

- **A1 (Blocking) — deploy-block gating.** `buildSnapshotSet` never consults `RegisteredPool.DeployBlock`; on the worker's first block (all pools `!seen`) or any backfilled/pre-deployment block, core `AllowFailure:false` reads hit a not-yet-deployed contract → Multicall3 reverts → block retries forever. Fix in the (now shared) snapshot-set builder: skip any pool with `block < deploy_block`. Both DEXes inherit the fix.
- **A2 (Blocking) — reorg/version RPC pinning.** State multicall is pinned to block *number* only while receipts are keyed by `(number, version)`; after a reorg the archive node answers `eth_call` with new-canonical state stamped as the old version. Fix: pin the multicall to the specific block **hash** (thread it through `BlockEvent`/snapshot). Uniswap builds this in from the start.
- **A3 (Should-fix, Curve)** — range-check `sold_id`/`bought_id` before `int(bigInt.Int64())`. **A6 (Nice-to-have, Curve)** — consider `get_dy` AllowFailure + nullable to decouple a derived quote from liveness. Flag/file for Curve; not required for Uniswap.

## 11. Reorg / processing_version handling

Append-only everywhere; `block_version` threads from the block event; the BEFORE INSERT trigger assigns `processing_version` under an advisory lock on the natural key in sorted order (per ADR-0002). Latest row per entity selected by snapshot-time key `DESC` then `processing_version DESC` (never `build_id`). State/tick reads are pinned to the block hash (A2) so a reorged version stores its own consistent state.

## 12. Observability (ships in this PR)

- Metrics via `dextelemetry.NewTelemetry("uniswap_v3", chainID)`: `uniswap_v3_blocks_processed_total{status,chain}`, `_errors_total{operation,chain}`, `_block_duration_seconds`, `_state_rows_written_total`.
- Alert group in `alerts/vector-indexers.yaml` (copy the Curve group): stall (critical, `runbook_url`), high error rate (warning), processing-but-not-writing (warning). Add a data-quality alert if a new silent-empty mode exists (e.g. TWAP always NULL across pools = observe misconfig).
- Runbook sections in `docs/runbooks/vector-indexers.md` for each alert.

## 13. Testing strategy

- **Unit** (mock outbound ports, table-driven, one behavior per test): event decoders (all 9), sqrtPriceX96→price math, tick-set derivation from touched events, baseline tick enumeration, snapshot-call builder, state mapping, pool-event JSONB marshalling.
- **Integration** (real Postgres via real migrations; mock only the RPC/Alchemy data source): `LoadPools`, `SaveBlock` round-trips for each table, append-on-change tick behavior (insert-on-change, idempotent replay, reorg version), full-block exercise touching every table, deploy-block gating (A1), block-hash pinning (A2).
- **main_test.go**: config-fail-fast env validation only.
- **Local real-mainnet validation**: LOCAL kind cluster + real Alchemy, short run; confirm each column/table populates with non-NULL data (catch silent AllowFailure NULLs / signature mismatches). Services + main.go target 100% coverage.

## 14. Open questions / follow-ups

- File the V3 per-NFT-position ticket (analogue of VEC-377) for NonfungiblePositionManager indexing.
- File Curve A3/A6 as their own items.
- Baseline tick enumeration cost for the deep 0.01% pool (tickSpacing 1): bound the `tickBitmap` walk; if the initialized-tick count is large, batch via multicall and log the count. Could bound to a price band if cost warrants (knob, default full).
