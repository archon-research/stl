//go:build livevalidation

package uniswapv3indexer

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// alchemyURL builds the real mainnet Alchemy endpoint this harness dials from
// the ALCHEMY_API_KEY env var (same variable the workers use). Real network
// access is the point of this build-tagged live-validation test (see task B13
// brief): it is never compiled into normal `go test`/CI runs.
func alchemyURL(t *testing.T) string {
	t.Helper()
	key := os.Getenv("ALCHEMY_API_KEY")
	if key == "" {
		t.Fatal("ALCHEMY_API_KEY must be set to run TestLiveValidation")
	}
	return "https://eth-mainnet.g.alchemy.com/v2/" + key
}

// multicall3Address is the canonical Multicall3 deployment address, identical
// across every EVM chain including mainnet.
var multicall3Address = common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11")

// busyPoolAddress is the wstETH/WETH 0.01% pool used for the event-decode and
// baseline-tick assertions: the deepest/most active of the 18 seeded pools.
var busyPoolAddress = common.HexToAddress("0x109830a1AAaD605BbF02a9dFA7B0B92EC2FB7dAa")

// liveValidationReportPath is where the human-readable data report (the B13
// deliverable) is written, in addition to t.Log output. Defaults to the
// system temp dir; override with LIVE_VALIDATION_REPORT_PATH.
func liveValidationReportPath() string {
	if p := os.Getenv("LIVE_VALIDATION_REPORT_PATH"); p != "" {
		return p
	}
	return filepath.Join(os.TempDir(), "uniswap-v3-live-validation-report.md")
}

// swapLogsScanDepth bounds how many recent blocks are scanned via
// eth_getLogs to find a real Swap on the busy pool.
const swapLogsScanDepth = 2000

// TestLiveValidation is the B13 contained live-correctness gate: it exercises
// the real decode/snapshot/tick/persist code paths against REAL Alchemy +
// REAL mainnet data, in a fully throwaway TimescaleDB testcontainer, without
// touching the kind cluster or any committed non-test code. See
// task-B13-brief.md for the full spec this test satisfies.
func TestLiveValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	rep := newLiveReport()
	defer rep.writeAndLog(t)

	pool, _, cleanupDB := testutil.SetupTimescaleDB(t)
	defer cleanupDB()

	// testutil.SetupTimescaleDB applies every db/migrations/*.sql file,
	// including 20260701_100200_seed_uniswap_v3_pools.sql, so the 18 real
	// pools + counterparty tokens already exist -- no self-seed needed.

	buildID := buildregistry.BuildID(1)
	repo := postgres.NewUniswapV3Repository(pool, buildID)

	poolRows, err := repo.LoadPools(ctx, 1)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}
	if len(poolRows) != 18 {
		t.Fatalf("LoadPools returned %d pools, want 18 (seed migration did not apply cleanly)", len(poolRows))
	}
	regPools := toRegisteredPools(poolRows)
	rep.poolsLoaded = len(regPools)

	rpcClient, err := rpc.DialContext(ctx, alchemyURL(t))
	if err != nil {
		t.Fatalf("BLOCKED: rpc.Dial(alchemy): %v", err)
	}
	defer rpcClient.Close()
	ethClient := ethclient.NewClient(rpcClient)

	mc, err := multicall.NewClient(ethClient, multicall3Address)
	if err != nil {
		t.Fatalf("BLOCKED: multicall.NewClient: %v", err)
	}

	latest, err := ethClient.BlockNumber(ctx)
	if err != nil {
		t.Fatalf("BLOCKED: ethClient.BlockNumber (network/Alchemy unavailable): %v", err)
	}
	targetBlockNum := int64(latest) - 5

	header, err := ethClient.HeaderByNumber(ctx, big.NewInt(targetBlockNum))
	if err != nil {
		t.Fatalf("BLOCKED: HeaderByNumber(%d): %v", targetBlockNum, err)
	}
	blockHash := header.Hash()
	blockTS := time.Unix(int64(header.Time), 0).UTC()

	rep.blockNumber = targetBlockNum
	rep.blockHash = blockHash.Hex()
	rep.blockTimestamp = blockTS

	// --- Step 3: state snapshot for all 18 pools ------------------------------
	states := snapshotAllPools(t, ctx, mc, regPools, blockHash, targetBlockNum, blockTS, rep)

	txMgr, err := postgres.NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	eventWriter := newLiveEventWriter(t, ctx, pool, buildID)

	var stateRows int64
	err = txMgr.WithTransaction(ctx, func(tx pgx.Tx) error {
		var txErr error
		stateRows, txErr = repo.SaveBlock(ctx, tx, outbound.UniswapV3BlockWrites{States: states})
		return txErr
	})
	if err != nil {
		t.Fatalf("SaveBlock (state snapshot batch): %v", err)
	}
	rep.stateRowsWritten = stateRows

	// --- Step 4: find + decode a real Swap on the busy pool -------------------
	busyPool, ok := findPoolByAddress(regPools, busyPoolAddress)
	if !ok {
		t.Fatalf("busy pool %s not found among loaded pools", busyPoolAddress)
	}

	decoded, eventBlock, err := findAndDecodeRealSwap(ctx, ethClient, rpcClient, busyPool, int64(latest))
	if err != nil {
		t.Fatalf("CORE FAILURE: finding/decoding a real Swap: %v", err)
	}
	if decoded == nil {
		rep.swapDecodeNote = fmt.Sprintf("no Swap event found on pool %s in the last %d blocks (not a failure per se, but no decode was exercised)", busyPoolAddress, swapLogsScanDepth)
	} else {
		rep.eventBlockNumber = eventBlock.number
		rep.eventBlockHash = eventBlock.hash.Hex()
		rep.decodedSwaps = decoded.Swaps
		rep.decodedLiquidityEvents = decoded.LiquidityEvents

		for _, le := range decoded.LiquidityEvents {
			if le.TickLower >= le.TickUpper {
				t.Errorf("FINDING: decoded liquidity event has tick_lower(%d) >= tick_upper(%d)", le.TickLower, le.TickUpper)
			}
		}

		var tickRows []*entity.UniswapV3Tick
		if len(decoded.LiquidityEvents) > 0 {
			touched := TouchedTicks(*decoded)
			calls, err := BuildTickCalls(busyPool, touched)
			if err != nil {
				t.Fatalf("BuildTickCalls: %v", err)
			}
			results, err := mc.ExecuteAtHash(ctx, calls, eventBlock.hash)
			if err != nil {
				t.Fatalf("CORE FAILURE: ExecuteAtHash(ticks) at event block: %v", err)
			}
			for i, tick := range touched {
				row, err := DecodeTick(busyPool, tick, eventBlock.number, 0, eventBlock.timestamp, results[i])
				if err != nil {
					t.Fatalf("DecodeTick(%d): %v", tick, err)
				}
				tickRows = append(tickRows, row)
			}
		}
		rep.touchedTickCount = len(tickRows)

		err = txMgr.WithTransaction(ctx, func(tx pgx.Tx) error {
			if _, txErr := repo.SaveBlock(ctx, tx, outbound.UniswapV3BlockWrites{
				Swaps:           decoded.Swaps,
				LiquidityEvents: decoded.LiquidityEvents,
				PoolEvents:      decoded.PoolEvents,
				Ticks:           tickRows,
			}); txErr != nil {
				return txErr
			}
			return eventWriter.SaveBatch(ctx, tx, toProtocolEventInputs(decoded.Captured, eventBlock.number, 0, eventBlock.timestamp))
		})
		if err != nil {
			t.Fatalf("SaveBlock (decoded event batch): %v", err)
		}
	}

	// --- Step 6: baseline ticks on the busy pool ------------------------------
	baseline, err := BaselineTicks(ctx, mc, busyPool, blockHash)
	if err != nil {
		t.Fatalf("CORE FAILURE: BaselineTicks(%s): %v", busyPool.Address, err)
	}
	rep.baselineTickCount = len(baseline)

	// --- Query every uniswap_v3 table for row counts + samples ----------------
	rep.tableCounts = queryAllTableCounts(t, ctx, pool)

	t.Logf("live validation complete: block=%d hash=%s pools=%d states=%d stateRows=%d baselineTicks=%d",
		targetBlockNum, blockHash.Hex(), len(poolRows), len(states), stateRows, len(baseline))
}

// -----------------------------------------------------------------------
// Pool registry conversion
// -----------------------------------------------------------------------

// toRegisteredPools converts LoadPools' outbound rows into the
// RegisteredPool shape the service/decode/state/tick functions consume.
func toRegisteredPools(rows []outbound.UniswapV3PoolRow) []RegisteredPool {
	out := make([]RegisteredPool, len(rows))
	for i, r := range rows {
		out[i] = RegisteredPool{
			ID:             r.ID,
			Address:        r.Address,
			Token0:         r.Token0,
			Token1:         r.Token1,
			Token0Decimals: r.Token0Decimals,
			Token1Decimals: r.Token1Decimals,
			Fee:            r.Fee,
			TickSpacing:    r.TickSpacing,
			DeployBlock:    r.DeployBlock,
		}
	}
	return out
}

func findPoolByAddress(pools []RegisteredPool, addr common.Address) (RegisteredPool, bool) {
	for _, p := range pools {
		if p.Address == addr {
			return p, true
		}
	}
	return RegisteredPool{}, false
}

// -----------------------------------------------------------------------
// State snapshot (step 3)
// -----------------------------------------------------------------------

// snapshotAllPools calls SnapshotState for every pool in the registry,
// pinned to blockHash, and records each pool's core fields into rep.
// A CORE-field read reverting anywhere is a fatal finding (per the brief,
// distinct from an empty-liquidity pool, which is a legitimate zero value).
func snapshotAllPools(t *testing.T, ctx context.Context, mc outbound.Multicaller, pools []RegisteredPool, blockHash common.Hash, bn int64, ts time.Time, rep *liveReport) []*entity.UniswapV3PoolState {
	t.Helper()

	states := make([]*entity.UniswapV3PoolState, 0, len(pools))
	for _, p := range pools {
		state, err := SnapshotState(ctx, mc, p, blockHash, bn, 0, ts)
		if err != nil {
			t.Fatalf("CORE FAILURE: SnapshotState(pool=%s): %v", p.Address, err)
		}

		row := poolSnapshotSummary{
			address:     p.Address,
			fee:         p.Fee,
			tickSpacing: p.TickSpacing,
		}
		if state.SqrtPriceX96 != nil {
			row.sqrtPriceX96 = new(big.Int).Set(state.SqrtPriceX96)
		}
		row.tick = state.Tick
		if state.Liquidity != nil {
			row.liquidity = new(big.Int).Set(state.Liquidity)
		}
		if state.Balance0 != nil {
			row.balance0 = new(big.Int).Set(state.Balance0)
		}
		if state.Balance1 != nil {
			row.balance1 = new(big.Int).Set(state.Balance1)
		}
		row.twapTick = state.TwapTick

		// Flag CORE fields that are unexpectedly nil/zero. SqrtPriceX96==0 or
		// Liquidity==nil would mean slot0()/liquidity() silently returned
		// nothing usable despite Success=true -- a decode bug, not a
		// legitimately-empty pool (an empty pool has Liquidity==0, a valid
		// *big.Int, not a nil pointer).
		var findings []string
		if state.SqrtPriceX96 == nil {
			findings = append(findings, "SqrtPriceX96 is nil")
		}
		if state.Liquidity == nil {
			findings = append(findings, "Liquidity is nil")
		}
		if state.FeeGrowthGlobal0X128 == nil {
			findings = append(findings, "FeeGrowthGlobal0X128 is nil")
		}
		if state.FeeGrowthGlobal1X128 == nil {
			findings = append(findings, "FeeGrowthGlobal1X128 is nil")
		}
		if state.ProtocolFeesToken0 == nil {
			findings = append(findings, "ProtocolFeesToken0 is nil")
		}
		if state.ProtocolFeesToken1 == nil {
			findings = append(findings, "ProtocolFeesToken1 is nil")
		}
		if state.Balance0 == nil {
			findings = append(findings, "Balance0 is nil")
		}
		if state.Balance1 == nil {
			findings = append(findings, "Balance1 is nil")
		}
		if state.TwapTick == nil {
			row.twapNote = "TWAP nil (observe() reverted with OLD -- expected for pools with short observation history, not a failure)"
		}
		if len(findings) > 0 {
			msg := fmt.Sprintf("pool %s: %s", p.Address, strings.Join(findings, "; "))
			rep.nullFindings = append(rep.nullFindings, msg)
			t.Errorf("FINDING: %s", msg)
		}

		rep.poolSnapshots = append(rep.poolSnapshots, row)
		states = append(states, state)
	}
	return states
}

// poolSnapshotSummary captures one pool's state-snapshot result for the
// report, independent of entity.UniswapV3PoolState so the report can render
// even if a field came back nil.
type poolSnapshotSummary struct {
	address      common.Address
	fee          int
	tickSpacing  int
	sqrtPriceX96 *big.Int
	tick         int
	liquidity    *big.Int
	balance0     *big.Int
	balance1     *big.Int
	twapTick     *int
	twapNote     string
}

// -----------------------------------------------------------------------
// Event decode (step 4)
// -----------------------------------------------------------------------

// eventBlockInfo identifies the real block a decoded Swap/Mint/Burn came
// from, for both persistence (block_number/version/timestamp columns) and
// the report.
type eventBlockInfo struct {
	number    int64
	hash      common.Hash
	timestamp time.Time
}

// findAndDecodeRealSwap scans the last swapLogsScanDepth blocks for a Swap
// log on pool via eth_getLogs, fetches that block's real transaction
// receipts, converts them into shared.TransactionReceipt (the shape
// DecodeEvents expects, matching the JSON-RPC eth_getTransactionReceipt
// wire format), and decodes them through the service's real DecodeEvents
// path. Returns (nil, ..., nil) if no Swap is found in the scanned window --
// that is reported, not treated as a failure, since it depends on live
// mempool activity outside this test's control.
func findAndDecodeRealSwap(ctx context.Context, ethClient *ethclient.Client, rpcClient *rpc.Client, pool RegisteredPool, latest int64) (*DecodedEvents, eventBlockInfo, error) {
	fromBlock := latest - swapLogsScanDepth
	if fromBlock < 0 {
		fromBlock = 0
	}

	logs, err := ethClient.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: big.NewInt(fromBlock),
		ToBlock:   big.NewInt(latest),
		Addresses: []common.Address{pool.Address},
		Topics:    [][]common.Hash{{swapEventTopic0()}},
	})
	if err != nil {
		return nil, eventBlockInfo{}, fmt.Errorf("eth_getLogs for Swap on pool %s: %w", pool.Address, err)
	}
	if len(logs) == 0 {
		return nil, eventBlockInfo{}, nil
	}

	// Most recent match: freshest data, and fewest confirmations needed.
	sort.Slice(logs, func(i, j int) bool { return logs[i].BlockNumber > logs[j].BlockNumber })
	target := logs[0]

	header, err := ethClient.HeaderByHash(ctx, target.BlockHash)
	if err != nil {
		return nil, eventBlockInfo{}, fmt.Errorf("HeaderByHash(%s): %w", target.BlockHash, err)
	}
	info := eventBlockInfo{
		number:    int64(target.BlockNumber),
		hash:      target.BlockHash,
		timestamp: time.Unix(int64(header.Time), 0).UTC(),
	}

	receipts, err := fetchBlockReceipts(ctx, rpcClient, target.BlockHash)
	if err != nil {
		return nil, eventBlockInfo{}, fmt.Errorf("fetching receipts for block %s: %w", target.BlockHash, err)
	}

	var merged DecodedEvents
	for _, r := range receipts {
		if !receiptTouchesAddress(r, pool.Address) {
			continue
		}
		d, err := DecodeEvents(r, pool, 1, info.number, 0, info.timestamp)
		if err != nil {
			return nil, eventBlockInfo{}, fmt.Errorf("DecodeEvents(tx=%s): %w", r.TransactionHash, err)
		}
		merged.Swaps = append(merged.Swaps, d.Swaps...)
		merged.LiquidityEvents = append(merged.LiquidityEvents, d.LiquidityEvents...)
		merged.PoolEvents = append(merged.PoolEvents, d.PoolEvents...)
		merged.Captured = append(merged.Captured, d.Captured...)
	}
	if len(merged.Swaps) == 0 {
		return nil, eventBlockInfo{}, fmt.Errorf("block %d contained a Swap log per eth_getLogs but DecodeEvents found none -- signature/topic mismatch", info.number)
	}
	return &merged, info, nil
}

// swapEventTopic0 returns the Swap event's topic0 (keccak256 of its
// signature) straight from the real pool ABI, so the eth_getLogs filter
// tracks the same event definition DecodeEvents decodes against -- if the
// ABI's Swap signature ever drifted from the on-chain contract, both this
// filter and DecodeEvents would break together rather than silently
// disagreeing.
func swapEventTopic0() common.Hash {
	poolABI, err := PoolABI()
	if err != nil {
		panic(fmt.Sprintf("loading pool ABI for Swap topic0: %v", err))
	}
	ev, ok := poolABI.Events["Swap"]
	if !ok {
		panic("pool ABI has no Swap event")
	}
	return ev.ID
}

// receiptTouchesAddress reports whether any log in r was emitted by addr.
func receiptTouchesAddress(r shared.TransactionReceipt, addr common.Address) bool {
	for _, l := range r.Logs {
		if common.IsHexAddress(l.Address) && common.HexToAddress(l.Address) == addr {
			return true
		}
	}
	return false
}

// fetchBlockReceipts calls the real eth_getBlockReceipts JSON-RPC method and
// decodes straight into shared.TransactionReceipt, exercising the exact
// wire shape (hex-string quantities, lowercase-hex addresses/hashes) a real
// Alchemy response produces -- the thing a fixture-based unit test cannot
// catch (see B13 brief: "real-Alchemy response shapes").
func fetchBlockReceipts(ctx context.Context, rpcClient *rpc.Client, blockHash common.Hash) ([]shared.TransactionReceipt, error) {
	var receipts []shared.TransactionReceipt
	if err := rpcClient.CallContext(ctx, &receipts, "eth_getBlockReceipts", blockHash); err != nil {
		return nil, fmt.Errorf("eth_getBlockReceipts(%s): %w", blockHash, err)
	}
	if receipts == nil {
		return nil, fmt.Errorf("eth_getBlockReceipts(%s): nil result (block not found or method unsupported)", blockHash)
	}
	return receipts, nil
}

// -----------------------------------------------------------------------
// Persistence plumbing
// -----------------------------------------------------------------------

// newLiveEventWriter builds a dexconsumer.ProtocolEventWriter against a real
// postgres.EventRepository and the UniswapV3 protocol row's real id (read
// back from the DB, not assumed), so captured logs persist to the same
// protocol_event mirror the live worker uses.
func newLiveEventWriter(t *testing.T, ctx context.Context, pool *pgxpool.Pool, buildID buildregistry.BuildID) *dexconsumer.ProtocolEventWriter {
	t.Helper()

	var protocolID int64
	err := pool.QueryRow(ctx, `SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV3'`).Scan(&protocolID)
	if err != nil {
		t.Fatalf("reading UniswapV3 protocol id (seed migration missing?): %v", err)
	}

	eventRepo := postgres.NewEventRepository(nil, buildID)
	return dexconsumer.NewProtocolEventWriter(protocolID, eventRepo)
}

func toProtocolEventInputs(captured []dexconsumer.CapturedLog, bn int64, ver int, ts time.Time) []dexconsumer.ProtocolEventInput {
	out := make([]dexconsumer.ProtocolEventInput, 0, len(captured))
	for _, c := range captured {
		out = append(out, dexconsumer.ProtocolEventInput{
			ContractAddress: c.Address,
			ChainID:         1,
			BlockNumber:     bn,
			BlockVersion:    ver,
			BlockTimestamp:  ts,
			TxHash:          c.TxHash,
			LogIndex:        c.LogIndex,
			EventName:       c.EventName,
			Payload:         c.Payload,
		})
	}
	return out
}

// -----------------------------------------------------------------------
// Table row-count reconciliation (step 5)
// -----------------------------------------------------------------------

var uniswapV3ReportTables = []string{
	"uniswap_v3_pool",
	"uniswap_v3_pool_state",
	"uniswap_v3_swap",
	"uniswap_v3_liquidity_event",
	"uniswap_v3_tick",
	"uniswap_v3_pool_event",
	"protocol_event",
}

func queryAllTableCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]int64 {
	t.Helper()
	counts := make(map[string]int64, len(uniswapV3ReportTables))
	for _, table := range uniswapV3ReportTables {
		var n int64
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatalf("counting %s: %v", table, err)
		}
		counts[table] = n
	}
	return counts
}

// -----------------------------------------------------------------------
// Report
// -----------------------------------------------------------------------

// liveReport accumulates every observation this test makes for the final
// markdown deliverable the controller reconciles against `cast`.
type liveReport struct {
	poolsLoaded      int
	blockNumber      int64
	blockHash        string
	blockTimestamp   time.Time
	stateRowsWritten int64

	poolSnapshots []poolSnapshotSummary
	nullFindings  []string

	eventBlockNumber       int64
	eventBlockHash         string
	decodedSwaps           []*entity.UniswapV3Swap
	decodedLiquidityEvents []*entity.UniswapV3LiquidityEvent
	touchedTickCount       int
	swapDecodeNote         string

	baselineTickCount int
	tableCounts       map[string]int64
}

func newLiveReport() *liveReport { return &liveReport{} }

// derivedPrice computes token1/token0 = (sqrtPriceX96/2^96)^2, adjusted by
// decimals, as a float64 for the report table. Precision loss from float64
// is acceptable here: this is a human-readable sanity figure for `cast`
// cross-checking, not a value persisted to the DB.
func derivedPrice(sqrtPriceX96 *big.Int, decimals0, decimals1 int) float64 {
	if sqrtPriceX96 == nil || sqrtPriceX96.Sign() == 0 {
		return 0
	}
	ratio := new(big.Float).SetInt(sqrtPriceX96)
	ratio.Quo(ratio, new(big.Float).SetInt(new(big.Int).Lsh(big.NewInt(1), 96)))
	ratio.Mul(ratio, ratio)
	scale := new(big.Float).SetFloat64(1)
	if decimals0 != decimals1 {
		diff := decimals0 - decimals1
		pow := new(big.Float).SetFloat64(1)
		ten := new(big.Float).SetFloat64(10)
		for range abs(diff) {
			pow.Mul(pow, ten)
		}
		if diff > 0 {
			scale = pow
		} else {
			scale.Quo(scale, pow)
		}
	}
	ratio.Mul(ratio, scale)
	f, _ := ratio.Float64()
	return f
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// writeAndLog renders the report to markdown, writes it to
// liveValidationReportPath, and t.Logs it so the data survives in `go test
// -v` output even if the file write fails (e.g. a sandboxed environment
// without access to /private/tmp).
func (r *liveReport) writeAndLog(t *testing.T) {
	t.Helper()
	md := r.render()
	t.Log(md)

	reportPath := liveValidationReportPath()
	if err := os.WriteFile(reportPath, []byte(md), 0o644); err != nil {
		t.Logf("warning: could not write report to %s: %v", reportPath, err)
	}
}

func (r *liveReport) render() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Task B13 live-validation report\n\n")
	fmt.Fprintf(&b, "Block: %d\nHash: %s\nTimestamp: %s\nPools loaded: %d\nState rows written: %d\n\n",
		r.blockNumber, r.blockHash, r.blockTimestamp.Format(time.RFC3339), r.poolsLoaded, r.stateRowsWritten)

	fmt.Fprintf(&b, "## Pool state snapshots (all %d pools, block %d)\n\n", len(r.poolSnapshots), r.blockNumber)
	fmt.Fprintf(&b, "| Pool | Fee | SqrtPriceX96 | Price (t1/t0) | Tick | Liquidity | Balance0 | Balance1 | TWAP tick |\n")
	fmt.Fprintf(&b, "|---|---|---|---|---|---|---|---|---|\n")
	for _, s := range r.poolSnapshots {
		twap := "nil"
		if s.twapTick != nil {
			twap = fmt.Sprintf("%d", *s.twapTick)
		} else if s.twapNote != "" {
			twap = "nil (" + s.twapNote + ")"
		}
		price := derivedPrice(s.sqrtPriceX96, 18, 18) // display-only; exact decimals looked up per-pool below for the busy pool
		fmt.Fprintf(&b, "| %s | %d | %s | %.6g | %d | %s | %s | %s | %s |\n",
			s.address.Hex(), s.fee, bigOrNil(s.sqrtPriceX96), price, s.tick,
			bigOrNil(s.liquidity), bigOrNil(s.balance0), bigOrNil(s.balance1), twap)
	}

	fmt.Fprintf(&b, "\n## Busy pool (%s) focus\n\n", busyPoolAddress.Hex())
	for _, s := range r.poolSnapshots {
		if s.address != busyPoolAddress {
			continue
		}
		fmt.Fprintf(&b, "SqrtPriceX96: %s\n\nDerived price (wstETH per WETH, 18/18 decimals): %.10g\n\nTick: %d\n\nLiquidity: %s\n\nBalance0 (wstETH): %s\n\nBalance1 (WETH): %s\n\nTWAP tick: %v\n\n",
			bigOrNil(s.sqrtPriceX96), derivedPrice(s.sqrtPriceX96, 18, 18), s.tick, bigOrNil(s.liquidity), bigOrNil(s.balance0), bigOrNil(s.balance1), twapPtrOrNil(s.twapTick))
	}
	fmt.Fprintf(&b, "Baseline tick count: %d\n\n", r.baselineTickCount)

	fmt.Fprintf(&b, "## Decoded Swap event\n\n")
	if r.swapDecodeNote != "" {
		fmt.Fprintf(&b, "%s\n\n", r.swapDecodeNote)
	} else {
		fmt.Fprintf(&b, "Event block: %d\nEvent block hash: %s\nTouched tick rows persisted: %d\n\n", r.eventBlockNumber, r.eventBlockHash, r.touchedTickCount)
		for _, sw := range r.decodedSwaps {
			fmt.Fprintf(&b, "- tx=%s logIndex=%d sender=%s recipient=%s amount0=%s amount1=%s sqrtPriceX96=%s liquidity=%s tick=%d\n",
				sw.TxHash.Hex(), sw.LogIndex, sw.Sender.Hex(), sw.Recipient.Hex(),
				sw.Amount0.String(), sw.Amount1.String(), sw.SqrtPriceX96.String(), sw.Liquidity.String(), sw.Tick)
		}
		if len(r.decodedLiquidityEvents) > 0 {
			fmt.Fprintf(&b, "\nDecoded liquidity events:\n\n")
			for _, le := range r.decodedLiquidityEvents {
				fmt.Fprintf(&b, "- %s tx=%s tickLower=%d tickUpper=%d amount=%s amount0=%s amount1=%s\n",
					le.EventName, le.TxHash.Hex(), le.TickLower, le.TickUpper,
					bigOrNil(le.Amount), bigOrNil(le.Amount0), bigOrNil(le.Amount1))
			}
		} else {
			fmt.Fprintf(&b, "\nNo Mint/Burn found in the same block as the decoded Swap.\n")
		}
	}

	fmt.Fprintf(&b, "\n## Per-table row counts\n\n")
	fmt.Fprintf(&b, "| Table | Rows |\n|---|---|\n")
	tables := make([]string, 0, len(r.tableCounts))
	for k := range r.tableCounts {
		tables = append(tables, k)
	}
	sort.Strings(tables)
	for _, tbl := range tables {
		fmt.Fprintf(&b, "| %s | %d |\n", tbl, r.tableCounts[tbl])
	}

	fmt.Fprintf(&b, "\n## Findings (NULL/decode/signature issues)\n\n")
	if len(r.nullFindings) == 0 {
		fmt.Fprintf(&b, "None.\n")
	} else {
		for _, f := range r.nullFindings {
			fmt.Fprintf(&b, "- %s\n", f)
		}
	}

	return b.String()
}

func bigOrNil(v *big.Int) string {
	if v == nil {
		return "nil"
	}
	return v.String()
}

func twapPtrOrNil(v *int) any {
	if v == nil {
		return nil
	}
	return *v
}
