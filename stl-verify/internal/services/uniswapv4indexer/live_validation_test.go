//go:build livevalidation

package uniswapv4indexer

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/tickbitmap"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// alchemyURL builds the real mainnet Alchemy endpoint this harness dials from
// the ALCHEMY_API_KEY env var (the same variable the workers use). Real network
// access is the whole point of this build-tagged live-validation gate: it is
// never compiled into normal `go test`/CI runs.
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

// busyPoolID is the ETH/wstETH 0.01% pool (tickSpacing 1) — the most active of
// the 21 seeded pools, and the tickSpacing=1 pool the baseline-tick enumeration
// is exercised against.
var busyPoolID = common.HexToHash("0x1d5b2949ece8754c2d736991c62c5162bd144f497b2212182401b9bae77e2d76")

// fallbackSwapPoolID is the PYUSD/USDS 0.0005% pool, used to find a real Swap
// when the busy pool happens to have been quiet across the whole scan window.
var fallbackSwapPoolID = common.HexToHash("0xe63e32b2ae40601662f760d6bf5d771057324fbd97784fe1d3717069f7b75d45")

// slot0CrossCheckPools are the pools whose multicall-read slot0 is re-read
// through a direct eth_call with hand-rolled calldata (see crossCheckSlot0):
// the busy pool, the hook-bearing dynamic-fee-capable pool, and a 6-decimal
// stable pool, so the check spans the three shapes in the registry.
var slot0CrossCheckPools = []common.Hash{
	busyPoolID,
	common.HexToHash("0x904e8ad11c6f8abb44ea77c507355900e7f9d2907ab0a29353cb1ef0f06b0852"),
	fallbackSwapPoolID,
}

// wantSeededPools is the pool count the V4 seed migration installs for mainnet.
// A different count means the migration did not apply cleanly, which would make
// every downstream assertion meaningless.
const wantSeededPools = 21

// swapLogsScanDepth bounds how many recent blocks are scanned via eth_getLogs
// to find a real Swap on the target pool.
const swapLogsScanDepth = 2000

// baselineTickSampleSize bounds how many enumerated ticks the report renders.
// Every tick is read and asserted on (the completeness invariant needs all of
// them); this only keeps the markdown table readable for a dense pool.
const baselineTickSampleSize = 25

// maxTickMagnitude is TickMath.MAX_TICK; every decoded tick must fall inside
// [-maxTickMagnitude, maxTickMagnitude].
const maxTickMagnitude = 887272

// liveValidationReportPath is where the human-readable data report is written,
// in addition to t.Log output. Defaults to the system temp dir; override with
// LIVE_VALIDATION_REPORT_PATH.
func liveValidationReportPath() string {
	if p := os.Getenv("LIVE_VALIDATION_REPORT_PATH"); p != "" {
		return p
	}
	return filepath.Join(os.TempDir(), "uniswap-v4-live-validation-report.md")
}

// TestLiveValidation is the contained live-correctness gate for the Uniswap V4
// indexer: it exercises the real registry/snapshot/decode/tick/persist code
// paths against REAL Alchemy + REAL mainnet data, in a throwaway schema on the
// package's TimescaleDB testcontainer, without touching any cluster.
func TestLiveValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()

	rep := newLiveReport()
	defer rep.writeAndLog(t)

	dbPool, _, cleanupDB := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanupDB()

	buildID := buildregistry.BuildID(1)
	repo := postgres.NewUniswapV4Repository(dbPool, buildID)

	regPools := loadRegistry(t, ctx, repo, rep)
	poolsByID := indexPoolsByHash(regPools)
	poolManager := regPools[0].PoolManager
	rep.poolManager = poolManager.Hex()
	rep.stateView = regPools[0].StateView.Hex()

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
	target := blockAt(t, ctx, ethClient, int64(latest)-5)
	rep.blockNumber = target.number
	rep.blockHash = target.hash.Hex()
	rep.blockTimestamp = target.timestamp

	txMgr, err := postgres.NewTxManager(dbPool, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	eventWriter := newLiveEventWriter(t, ctx, dbPool, buildID)

	states := snapshotAllPools(t, ctx, mc, regPools, target, rep)
	rep.stateRowsWritten = persistStates(t, ctx, txMgr, repo, states)

	crossCheckSlot0(t, ctx, ethClient, regPools, states, target, rep)

	h := &liveHarness{
		eth: ethClient, rpc: rpcClient, mc: mc, txMgr: txMgr, repo: repo, eventWriter: eventWriter,
		pools: regPools, poolsByID: poolsByID, poolManager: poolManager, latest: int64(latest),
	}

	h.decodeAndPersistRealSwap(t, ctx, rep)
	h.decodeAndPersistRealLiquidityEvent(t, ctx, rep)

	baselineTickCheck(t, ctx, mc, regPools, states, target, rep)

	rep.tableCounts = queryAllTableCounts(t, ctx, dbPool)

	t.Logf("live validation complete: block=%d hash=%s pools=%d states=%d stateRows=%d baselineTicks=%d",
		target.number, target.hash, len(regPools), len(states), rep.stateRowsWritten, rep.baselineTickCount)
}

// -----------------------------------------------------------------------
// Registry
// -----------------------------------------------------------------------

// loadRegistry reads the seeded registry through the real repository and runs
// the same startup gate the worker runs (ValidatePoolKeys), so a seed row whose
// stored PoolId disagrees with its PoolKey fails here rather than silently
// indexing nothing.
func loadRegistry(t *testing.T, ctx context.Context, repo *postgres.UniswapV4Repository, rep *liveReport) []RegisteredPool {
	t.Helper()

	poolRows, err := repo.LoadPools(ctx, 1)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}
	if len(poolRows) != wantSeededPools {
		t.Fatalf("LoadPools returned %d pools, want %d (seed migration did not apply cleanly)", len(poolRows), wantSeededPools)
	}
	pools := RegisteredPoolsFromRows(poolRows)
	if err := ValidatePoolKeys(pools); err != nil {
		t.Fatalf("CORE FAILURE: ValidatePoolKeys on the seeded registry: %v", err)
	}
	rep.poolsLoaded = len(pools)
	return pools
}

func findPoolByID(pools []RegisteredPool, id common.Hash) (RegisteredPool, bool) {
	for _, p := range pools {
		if p.PoolIDHash == id {
			return p, true
		}
	}
	return RegisteredPool{}, false
}

// -----------------------------------------------------------------------
// Block identity
// -----------------------------------------------------------------------

// blockInfo identifies one real block for both the pinned state reads and the
// block_number/version/timestamp columns every persisted row carries.
type blockInfo struct {
	number    int64
	hash      common.Hash
	timestamp time.Time
}

func blockAt(t *testing.T, ctx context.Context, ethClient *ethclient.Client, number int64) blockInfo {
	t.Helper()
	header, err := ethClient.HeaderByNumber(ctx, big.NewInt(number))
	if err != nil {
		t.Fatalf("BLOCKED: HeaderByNumber(%d): %v", number, err)
	}
	return blockInfo{number: number, hash: header.Hash(), timestamp: time.Unix(int64(header.Time), 0).UTC()}
}

func blockAtHash(t *testing.T, ctx context.Context, ethClient *ethclient.Client, hash common.Hash) blockInfo {
	t.Helper()
	header, err := ethClient.HeaderByHash(ctx, hash)
	if err != nil {
		t.Fatalf("BLOCKED: HeaderByHash(%s): %v", hash, err)
	}
	return blockInfo{number: header.Number.Int64(), hash: hash, timestamp: time.Unix(int64(header.Time), 0).UTC()}
}

// -----------------------------------------------------------------------
// State snapshot
// -----------------------------------------------------------------------

// snapshotAllPools calls SnapshotState for every registered pool, pinned to the
// target block hash, and records each pool's core fields into rep. Any read
// reverting or any bound being violated is a fatal finding: StateView answers an
// unknown PoolId with zeros rather than reverting, so a zero price would mean
// the registry points at a pool this PoolManager never initialized.
func snapshotAllPools(t *testing.T, ctx context.Context, mc outbound.Multicaller, pools []RegisteredPool, target blockInfo, rep *liveReport) []*entity.UniswapV4PoolState {
	t.Helper()

	states := make([]*entity.UniswapV4PoolState, 0, len(pools))
	for _, p := range pools {
		state, err := SnapshotState(ctx, mc, p, target.hash, target.number, 0, target.timestamp)
		if err != nil {
			t.Fatalf("CORE FAILURE: SnapshotState(pool=%s): %v", p.PoolIDHash, err)
		}
		states = append(states, state)
		rep.poolSnapshots = append(rep.poolSnapshots, summarisePool(p, state))

		for _, finding := range stateFindings(p, state) {
			rep.findings = append(rep.findings, finding)
			t.Errorf("FINDING: %s", finding)
		}
	}
	return states
}

// stateFindings re-asserts the on-chain bounds independently of
// entity.Validate, so a bound this test cares about cannot be silently relaxed
// in the entity without the gate noticing.
func stateFindings(p RegisteredPool, s *entity.UniswapV4PoolState) []string {
	var out []string
	prefix := fmt.Sprintf("pool %s (fee %d, tickSpacing %d)", p.PoolIDHash, p.Fee, p.TickSpacing)

	switch {
	case s.SqrtPriceX96 == nil:
		out = append(out, prefix+": SqrtPriceX96 is nil")
	case s.SqrtPriceX96.Sign() <= 0:
		out = append(out, prefix+": SqrtPriceX96 is not positive ("+s.SqrtPriceX96.String()+") — an uninitialized PoolId")
	}
	if s.Liquidity == nil {
		out = append(out, prefix+": Liquidity is nil")
	}
	if s.FeeGrowthGlobal0X128 == nil {
		out = append(out, prefix+": FeeGrowthGlobal0X128 is nil")
	}
	if s.FeeGrowthGlobal1X128 == nil {
		out = append(out, prefix+": FeeGrowthGlobal1X128 is nil")
	}
	if s.Tick < -maxTickMagnitude || s.Tick > maxTickMagnitude {
		out = append(out, fmt.Sprintf("%s: tick %d outside [-%d, %d]", prefix, s.Tick, maxTickMagnitude, maxTickMagnitude))
	}
	if s.LpFee < 0 || s.LpFee > 1_000_000 {
		out = append(out, fmt.Sprintf("%s: lpFee %d outside [0, 1000000]", prefix, s.LpFee))
	}
	if s.ProtocolFee < 0 || s.ProtocolFee > 0xFFFFFF {
		out = append(out, fmt.Sprintf("%s: protocolFee %d outside uint24", prefix, s.ProtocolFee))
	} else {
		if zeroForOne := s.ProtocolFee & 0xFFF; zeroForOne > 1000 {
			out = append(out, fmt.Sprintf("%s: zeroForOne protocol fee %d exceeds MAX_PROTOCOL_FEE (1000)", prefix, zeroForOne))
		}
		if oneForZero := s.ProtocolFee >> 12; oneForZero > 1000 {
			out = append(out, fmt.Sprintf("%s: oneForZero protocol fee %d exceeds MAX_PROTOCOL_FEE (1000)", prefix, oneForZero))
		}
	}
	return out
}

// poolSnapshotSummary captures one pool's snapshot for the report, independent
// of entity.UniswapV4PoolState so the report renders even if a field came back
// nil.
type poolSnapshotSummary struct {
	poolID       common.Hash
	currency0    common.Address
	currency1    common.Address
	fee          int
	tickSpacing  int
	hooks        common.Address
	sqrtPriceX96 *big.Int
	tick         int
	protocolFee  int
	lpFee        int
	liquidity    *big.Int
	feeGrowth0   *big.Int
	feeGrowth1   *big.Int
}

func summarisePool(p RegisteredPool, s *entity.UniswapV4PoolState) poolSnapshotSummary {
	return poolSnapshotSummary{
		poolID:       p.PoolIDHash,
		currency0:    p.Currency0,
		currency1:    p.Currency1,
		fee:          p.Fee,
		tickSpacing:  p.TickSpacing,
		hooks:        p.Hooks,
		sqrtPriceX96: s.SqrtPriceX96,
		tick:         s.Tick,
		protocolFee:  s.ProtocolFee,
		lpFee:        s.LpFee,
		liquidity:    s.Liquidity,
		feeGrowth0:   s.FeeGrowthGlobal0X128,
		feeGrowth1:   s.FeeGrowthGlobal1X128,
	}
}

func persistStates(t *testing.T, ctx context.Context, txMgr outbound.TxManager, repo *postgres.UniswapV4Repository, states []*entity.UniswapV4PoolState) int64 {
	t.Helper()

	var stateRows int64
	err := txMgr.WithTransaction(ctx, func(tx pgx.Tx) error {
		var txErr error
		stateRows, txErr = repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{States: states})
		return txErr
	})
	if err != nil {
		t.Fatalf("SaveBlock (state snapshot batch): %v", err)
	}
	if stateRows != int64(len(states)) {
		t.Errorf("FINDING: SaveBlock persisted %d state rows, want %d", stateRows, len(states))
	}
	return stateRows
}

// -----------------------------------------------------------------------
// Independent slot0 cross-check
// -----------------------------------------------------------------------

// crossCheckSlot0 re-reads three pools' slot0 through a direct eth_call pinned
// to the same block hash, with hand-rolled calldata and a hand-rolled tuple
// decode. It shares no code with state.go, so a wrong function selector, a
// wrong return-tuple order, or a mis-signed int24 in the production path shows
// up as a value mismatch here rather than as plausible-looking bad data.
func crossCheckSlot0(t *testing.T, ctx context.Context, ethClient *ethclient.Client, pools []RegisteredPool, states []*entity.UniswapV4PoolState, target blockInfo, rep *liveReport) {
	t.Helper()

	byPoolID := make(map[common.Hash]*entity.UniswapV4PoolState, len(states))
	for i, p := range pools {
		byPoolID[p.PoolIDHash] = states[i]
	}

	for _, wantID := range slot0CrossCheckPools {
		pool, ok := findPoolByID(pools, wantID)
		if !ok {
			t.Fatalf("cross-check pool %s is not in the seeded registry", wantID)
		}
		direct, err := directGetSlot0(ctx, ethClient, pool.StateView, pool.PoolIDHash, target.hash)
		if err != nil {
			t.Fatalf("CORE FAILURE: direct eth_call getSlot0(%s): %v", pool.PoolIDHash, err)
		}

		indexed := byPoolID[pool.PoolIDHash]
		row := slot0CrossCheck{
			poolID:   pool.PoolIDHash,
			indexed:  *indexed,
			direct:   direct,
			matching: indexed.SqrtPriceX96.Cmp(direct.sqrtPriceX96) == 0 && indexed.Tick == direct.tick && indexed.ProtocolFee == direct.protocolFee && indexed.LpFee == direct.lpFee,
		}
		rep.slot0CrossChecks = append(rep.slot0CrossChecks, row)
		if !row.matching {
			finding := fmt.Sprintf("pool %s slot0 mismatch: indexed (sqrtPriceX96=%s tick=%d protocolFee=%d lpFee=%d) vs direct eth_call (sqrtPriceX96=%s tick=%d protocolFee=%d lpFee=%d)",
				pool.PoolIDHash, indexed.SqrtPriceX96, indexed.Tick, indexed.ProtocolFee, indexed.LpFee,
				direct.sqrtPriceX96, direct.tick, direct.protocolFee, direct.lpFee)
			rep.findings = append(rep.findings, finding)
			t.Errorf("FINDING: %s", finding)
		}
	}
}

// slot0Values is a slot0 tuple decoded without the production ABI.
type slot0Values struct {
	sqrtPriceX96 *big.Int
	tick         int
	protocolFee  int
	lpFee        int
}

type slot0CrossCheck struct {
	poolID   common.Hash
	indexed  entity.UniswapV4PoolState
	direct   slot0Values
	matching bool
}

// directGetSlot0 issues a raw eth_call to StateView.getSlot0 at blockHash with
// calldata built from the keccak of the literal signature, and decodes the four
// returned words by hand.
func directGetSlot0(ctx context.Context, ethClient *ethclient.Client, stateView common.Address, poolID common.Hash, blockHash common.Hash) (slot0Values, error) {
	selector := crypto.Keccak256([]byte("getSlot0(bytes32)"))[:4]
	calldata := append(append([]byte{}, selector...), poolID.Bytes()...)

	out, err := ethClient.CallContractAtHash(ctx, ethereum.CallMsg{To: &stateView, Data: calldata}, blockHash)
	if err != nil {
		return slot0Values{}, fmt.Errorf("eth_call: %w", err)
	}
	if len(out) != 4*32 {
		return slot0Values{}, fmt.Errorf("returned %d bytes, want 128", len(out))
	}
	return slot0Values{
		sqrtPriceX96: new(big.Int).SetBytes(out[0:32]),
		tick:         int(twosComplement(out[32:64]).Int64()),
		protocolFee:  int(new(big.Int).SetBytes(out[64:96]).Int64()),
		lpFee:        int(new(big.Int).SetBytes(out[96:128]).Int64()),
	}, nil
}

// twosComplement interprets a 32-byte ABI word as a signed 256-bit integer,
// which is how a negative int24 arrives on the wire (sign-extended).
func twosComplement(word []byte) *big.Int {
	v := new(big.Int).SetBytes(word)
	if word[0]&0x80 != 0 {
		v.Sub(v, new(big.Int).Lsh(big.NewInt(1), 256))
	}
	return v
}

// -----------------------------------------------------------------------
// Real event decode + persist
// -----------------------------------------------------------------------

// liveHarness carries the clients, registry and writers that every decode step
// needs, so each step takes its subject rather than an eleven-argument tail.
type liveHarness struct {
	eth         *ethclient.Client
	rpc         *rpc.Client
	mc          outbound.Multicaller
	txMgr       outbound.TxManager
	repo        *postgres.UniswapV4Repository
	eventWriter *dexconsumer.ProtocolEventWriter
	pools       []RegisteredPool
	poolsByID   map[common.Hash]RegisteredPool
	poolManager common.Address
	latest      int64
}

// persistDecoded writes one receipt's typed rows, its tick reads and its
// protocol_event mirror in a single transaction — the same shape the worker's
// dexconsumer.PersistBlock uses, so a constraint the live pipeline would hit is
// hit here too.
func (h *liveHarness) persistDecoded(t *testing.T, ctx context.Context, decoded DecodedEvents, tickRows []*entity.UniswapV4Tick, block blockInfo) {
	t.Helper()

	err := h.txMgr.WithTransaction(ctx, func(tx pgx.Tx) error {
		if _, txErr := h.repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
			Swaps:           decoded.Swaps,
			LiquidityEvents: decoded.LiquidityEvents,
			PoolEvents:      decoded.PoolEvents,
			Ticks:           tickRows,
		}); txErr != nil {
			return txErr
		}
		return h.eventWriter.SaveBatch(ctx, tx, dexconsumer.ToProtocolEventInputs(decoded.Captured, 1, block.number, 0, block.timestamp))
	})
	if err != nil {
		t.Fatalf("SaveBlock + protocol_event mirror at block %d: %v", block.number, err)
	}
}

// decodeAndPersistRealSwap finds a real Swap on the busy pool (falling back to
// the stable pool if it was quiet), decodes its whole transaction receipt
// through the production DecodeEvents, asserts the V4-specific invariants, reads
// any touched ticks at that block's hash, and persists everything — typed rows
// and the protocol_event capture mirror — in one transaction.
func (h *liveHarness) decodeAndPersistRealSwap(t *testing.T, ctx context.Context, rep *liveReport) {
	t.Helper()

	swapLog, poolID, err := findRecentSwapLog(ctx, h.eth, h.poolManager, h.latest)
	if err != nil {
		t.Fatalf("CORE FAILURE: scanning for a real Swap log: %v", err)
	}
	if swapLog == nil {
		rep.swapDecodeNote = fmt.Sprintf("no Swap found on either target pool in the last %d blocks; no decode was exercised", swapLogsScanDepth)
		return
	}
	rep.swapPoolID = poolID.Hex()

	eventBlock := blockAtHash(t, ctx, h.eth, swapLog.BlockHash)
	rep.eventBlockNumber = eventBlock.number
	rep.eventBlockHash = eventBlock.hash.Hex()
	rep.eventTxHash = swapLog.TxHash.Hex()

	receipt, err := fetchReceipt(ctx, h.rpc, swapLog.TxHash)
	if err != nil {
		t.Fatalf("CORE FAILURE: %v", err)
	}

	decoded, touched, err := DecodeEvents(receipt, h.poolsByID, h.poolManager, eventBlock.number, 0, eventBlock.timestamp)
	if err != nil {
		t.Fatalf("CORE FAILURE: DecodeEvents(tx=%s): %v", swapLog.TxHash, err)
	}
	rep.decodedSwaps = decoded.Swaps
	rep.decodedLiquidityEvents = decoded.LiquidityEvents
	rep.decodedPoolEvents = decoded.PoolEvents
	rep.capturedCount = len(decoded.Captured)
	rep.touchedPoolCount = len(touched)

	if len(decoded.Swaps) == 0 {
		t.Fatalf("CORE FAILURE: eth_getLogs found a Swap on pool %s in tx %s but DecodeEvents produced none — signature/topic/routing mismatch", poolID, swapLog.TxHash)
	}

	assertSwapInvariants(t, decoded.Swaps, rep)
	rep.untrackedPoolLogs = assertUntrackedPoolsNotCaptured(t, []shared.TransactionReceipt{receipt}, []DecodedEvents{decoded}, h.poolsByID, h.poolManager, rep)
	h.assertBlockWideUntrackedFilter(t, ctx, eventBlock, rep)

	tickRows := h.readTouchedTicks(t, ctx, decoded, eventBlock)
	rep.touchedTickCount = len(tickRows)
	h.persistDecoded(t, ctx, decoded, tickRows, eventBlock)
}

// decodeAndPersistRealLiquidityEvent exercises the second write path end to end
// on real data: a real ModifyLiquidity log is decoded, its position bounds are
// checked against the pool's own tick spacing, the ticks it touched are read
// from StateView at that block's hash, and both the event and the tick rows are
// persisted. Waiting for a ModifyLiquidity to happen to share a receipt with the
// Swap above would leave this path validated only by luck.
func (h *liveHarness) decodeAndPersistRealLiquidityEvent(t *testing.T, ctx context.Context, rep *liveReport) {
	t.Helper()

	poolIDs := make([]common.Hash, 0, len(h.pools))
	for _, p := range h.pools {
		poolIDs = append(poolIDs, p.PoolIDHash)
	}

	logs, err := h.eth.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: big.NewInt(max(h.latest-swapLogsScanDepth, 0)),
		ToBlock:   big.NewInt(h.latest),
		Addresses: []common.Address{h.poolManager},
		Topics:    [][]common.Hash{{poolManagerEventTopic0("ModifyLiquidity")}, poolIDs},
	})
	if err != nil {
		t.Fatalf("CORE FAILURE: eth_getLogs ModifyLiquidity across the registry: %v", err)
	}
	if len(logs) == 0 {
		rep.liquidityDecodeNote = fmt.Sprintf("no ModifyLiquidity on any of the %d registered pools in the last %d blocks; the tick write path was not exercised", len(h.pools), swapLogsScanDepth)
		return
	}
	sort.Slice(logs, func(i, j int) bool { return logs[i].BlockNumber > logs[j].BlockNumber })
	target := newestLiquidityChangingLog(logs)

	eventBlock := blockAtHash(t, ctx, h.eth, target.BlockHash)
	receipt, err := fetchReceipt(ctx, h.rpc, target.TxHash)
	if err != nil {
		t.Fatalf("CORE FAILURE: %v", err)
	}

	decoded, _, err := DecodeEvents(receipt, h.poolsByID, h.poolManager, eventBlock.number, 0, eventBlock.timestamp)
	if err != nil {
		t.Fatalf("CORE FAILURE: DecodeEvents(liquidity tx=%s): %v", target.TxHash, err)
	}
	if len(decoded.LiquidityEvents) == 0 {
		t.Fatalf("CORE FAILURE: eth_getLogs found a ModifyLiquidity in tx %s but DecodeEvents produced none — signature/topic/routing mismatch", target.TxHash)
	}
	rep.liquidityBlockNumber = eventBlock.number
	rep.liquidityTxHash = target.TxHash.Hex()
	rep.liquidityEvents = decoded.LiquidityEvents

	assertLiquidityInvariants(t, decoded.LiquidityEvents, h.poolsByID, rep)

	tickRows := h.readTouchedTicks(t, ctx, decoded, eventBlock)
	rep.liquidityTickRows = len(tickRows)
	if len(tickRows) == 0 {
		t.Logf("note: every ModifyLiquidity in tx %s carried liquidityDelta == 0 (a fee-collecting poke), so no tick was re-read", target.TxHash)
	}
	h.persistDecoded(t, ctx, decoded, tickRows, eventBlock)
}

// newestLiquidityChangingLog picks the newest ModifyLiquidity that actually
// moves liquidity. Most V4 ModifyLiquidity traffic is zero-delta fee-collecting
// pokes, which TouchedTicks deliberately ignores — picking one would leave the
// tick read path unexercised. liquidityDelta is the third non-indexed word of
// the event data (tickLower, tickUpper, liquidityDelta, salt); this hand-decode
// only selects a candidate, and the production decoder still produces every
// value asserted on afterwards. Falls back to the newest log if the whole window
// is pokes.
func newestLiquidityChangingLog(logs []types.Log) types.Log {
	for _, l := range logs {
		if len(l.Data) >= 96 && twosComplement(l.Data[64:96]).Sign() != 0 {
			return l
		}
	}
	return logs[0]
}

// assertLiquidityInvariants checks what v4-core guarantees about a real
// ModifyLiquidity: an ordered, in-range tick pair, and bounds that are exact
// multiples of the pool's own tick spacing (Pool.checkTicks plus the
// tickSpacing validation in modifyLiquidity). A bound that is not a multiple
// would mean the log was routed to the wrong pool.
func assertLiquidityInvariants(t *testing.T, events []*entity.UniswapV4LiquidityEvent, poolsByID map[common.Hash]RegisteredPool, rep *liveReport) {
	t.Helper()

	spacingByRowID := make(map[int64]int, len(poolsByID))
	for _, p := range poolsByID {
		spacingByRowID[p.ID] = p.TickSpacing
	}

	for _, e := range events {
		prefix := fmt.Sprintf("liquidity event tx=%s logIndex=%d", e.TxHash, e.LogIndex)
		if e.TickLower >= e.TickUpper {
			addFinding(t, rep, fmt.Sprintf("%s: tickLower(%d) >= tickUpper(%d)", prefix, e.TickLower, e.TickUpper))
		}
		if e.TickLower < -maxTickMagnitude || e.TickUpper > maxTickMagnitude {
			addFinding(t, rep, fmt.Sprintf("%s: ticks [%d, %d] outside [-%d, %d]", prefix, e.TickLower, e.TickUpper, maxTickMagnitude, maxTickMagnitude))
		}
		if e.Sender == (common.Address{}) {
			addFinding(t, rep, prefix+": sender is the zero address")
		}
		spacing, known := spacingByRowID[e.PoolID]
		if !known {
			addFinding(t, rep, fmt.Sprintf("%s: decoded against pool row id %d, which is not in the registry", prefix, e.PoolID))
			continue
		}
		if e.TickLower%spacing != 0 || e.TickUpper%spacing != 0 {
			addFinding(t, rep, fmt.Sprintf("%s: ticks [%d, %d] are not multiples of the pool's tickSpacing (%d) — the log was routed to the wrong pool", prefix, e.TickLower, e.TickUpper, spacing))
		}
	}
}

// findRecentSwapLog scans the last swapLogsScanDepth blocks for a Swap on the
// busy pool and, if that pool was quiet, on the stable fallback pool. It filters
// on the PoolManager address plus topics[1] = PoolId, which is exactly how the
// singleton is queried for one pool's events.
func findRecentSwapLog(ctx context.Context, ethClient *ethclient.Client, poolManager common.Address, latest int64) (*swapLogRef, common.Hash, error) {
	fromBlock := max(latest-swapLogsScanDepth, 0)

	for _, poolID := range []common.Hash{busyPoolID, fallbackSwapPoolID} {
		logs, err := ethClient.FilterLogs(ctx, ethereum.FilterQuery{
			FromBlock: big.NewInt(fromBlock),
			ToBlock:   big.NewInt(latest),
			Addresses: []common.Address{poolManager},
			Topics:    [][]common.Hash{{poolManagerEventTopic0("Swap")}, {poolID}},
		})
		if err != nil {
			return nil, common.Hash{}, fmt.Errorf("eth_getLogs Swap on pool %s: %w", poolID, err)
		}
		if len(logs) == 0 {
			continue
		}
		// Most recent match: freshest data, and the receipt is still trivially
		// fetchable.
		sort.Slice(logs, func(i, j int) bool { return logs[i].BlockNumber > logs[j].BlockNumber })
		l := logs[0]
		return &swapLogRef{BlockHash: l.BlockHash, BlockNumber: int64(l.BlockNumber), TxHash: l.TxHash, LogIndex: l.Index}, poolID, nil
	}
	return nil, common.Hash{}, nil
}

// swapLogRef is the minimal identity of the located Swap log — everything the
// receipt fetch and the report need, without carrying a full types.Log around.
type swapLogRef struct {
	BlockHash   common.Hash
	BlockNumber int64
	TxHash      common.Hash
	LogIndex    uint
}

// poolManagerEventTopic0 resolves an event's topic0 straight from the
// production PoolManager ABI, so the eth_getLogs filter and DecodeEvents track
// the same event definition: were the ABI signature to drift from the on-chain
// contract, both would break together rather than silently disagreeing.
func poolManagerEventTopic0(name string) common.Hash {
	a, err := PoolManagerABI()
	if err != nil {
		panic(fmt.Sprintf("loading PoolManager ABI: %v", err))
	}
	ev, ok := a.Events[name]
	if !ok {
		panic("PoolManager ABI has no event " + name)
	}
	return ev.ID
}

// fetchReceipt calls the real eth_getTransactionReceipt and decodes straight
// into shared.TransactionReceipt, exercising the exact wire shape (hex-string
// quantities, lowercase-hex addresses/hashes) a real Alchemy response produces —
// the thing a fixture-based unit test cannot catch.
func fetchReceipt(ctx context.Context, rpcClient *rpc.Client, txHash common.Hash) (shared.TransactionReceipt, error) {
	var receipt shared.TransactionReceipt
	if err := rpcClient.CallContext(ctx, &receipt, "eth_getTransactionReceipt", txHash); err != nil {
		return shared.TransactionReceipt{}, fmt.Errorf("eth_getTransactionReceipt(%s): %w", txHash, err)
	}
	if receipt.TransactionHash == "" {
		return shared.TransactionReceipt{}, fmt.Errorf("eth_getTransactionReceipt(%s): empty result", txHash)
	}
	return receipt, nil
}

// assertSwapInvariants checks the V4-specific properties of every decoded swap:
// a real router sender, the swapper-perspective BalanceDelta sign convention
// (one side paid in, the other received), a total fee inside LPFeeLibrary's
// ceiling, and a tick inside TickMath's range.
func assertSwapInvariants(t *testing.T, swaps []*entity.UniswapV4Swap, rep *liveReport) {
	t.Helper()

	for _, sw := range swaps {
		prefix := fmt.Sprintf("swap tx=%s logIndex=%d", sw.TxHash, sw.LogIndex)

		if sw.Sender == (common.Address{}) {
			addFinding(t, rep, prefix+": sender is the zero address")
		}
		negatives := 0
		if sw.Amount0.Sign() < 0 {
			negatives++
		}
		if sw.Amount1.Sign() < 0 {
			negatives++
		}
		if negatives != 1 {
			addFinding(t, rep, fmt.Sprintf("%s: sign convention violated — amount0=%s amount1=%s has %d negative sides, want exactly 1", prefix, sw.Amount0, sw.Amount1, negatives))
		}
		if sw.Fee < 0 || sw.Fee > 1_000_000 {
			addFinding(t, rep, fmt.Sprintf("%s: fee %d outside [0, 1000000]", prefix, sw.Fee))
		}
		if sw.Tick < -maxTickMagnitude || sw.Tick > maxTickMagnitude {
			addFinding(t, rep, fmt.Sprintf("%s: tick %d outside [-%d, %d]", prefix, sw.Tick, maxTickMagnitude, maxTickMagnitude))
		}
		if sw.SqrtPriceX96 == nil || sw.SqrtPriceX96.Sign() <= 0 {
			addFinding(t, rep, prefix+": sqrtPriceX96 is not positive")
		}
	}
}

// assertUntrackedPoolsNotCaptured proves the high-volume filter works on real
// data: the singleton emits for thousands of pools, and a log keyed by a PoolId
// outside the registry must produce neither a typed row nor a protocol_event
// mirror. This is the assertion that would catch a routing regression that
// captured everything the PoolManager emits. Returns how many untracked-pool
// logs the receipts actually contained, so a vacuous run (a receipt that
// happened to touch only tracked pools) is visible in the report rather than
// passing silently.
func assertUntrackedPoolsNotCaptured(t *testing.T, receipts []shared.TransactionReceipt, decoded []DecodedEvents, poolsByID map[common.Hash]RegisteredPool, poolManager common.Address, rep *liveReport) int {
	t.Helper()

	events, err := eventsByID()
	if err != nil {
		t.Fatalf("eventsByID: %v", err)
	}

	untracked := 0
	for i, receipt := range receipts {
		capturedIndexes := make(map[uint]struct{}, len(decoded[i].Captured))
		for _, c := range decoded[i].Captured {
			capturedIndexes[c.LogIndex] = struct{}{}
		}

		for _, l := range receipt.Logs {
			if !common.IsHexAddress(l.Address) || common.HexToAddress(l.Address) != poolManager || len(l.Topics) < 2 {
				continue
			}
			ev, known := events[common.HexToHash(l.Topics[0])]
			if !known {
				continue
			}
			if _, keyed := poolKeyedEvents[ev.Name]; !keyed {
				continue
			}
			if _, tracked := poolsByID[common.HexToHash(l.Topics[1])]; tracked {
				continue
			}
			untracked++

			idx, err := shared.ParseHexUint(l.LogIndex)
			if err != nil {
				t.Fatalf("parsing log index %q: %v", l.LogIndex, err)
			}
			if _, captured := capturedIndexes[idx]; captured {
				addFinding(t, rep, fmt.Sprintf("log index %d belongs to untracked pool %s but was captured into protocol_event", idx, l.Topics[1]))
			}
		}
	}
	return untracked
}

// assertBlockWideUntrackedFilter re-runs the untracked-pool assertion over every
// receipt in the event block. One transaction can easily touch only tracked
// pools, which would leave the per-receipt check vacuous; a whole mainnet block
// reliably carries PoolManager traffic for pools outside the 21-pool registry,
// so this is where the filter is actually put under load.
func (h *liveHarness) assertBlockWideUntrackedFilter(t *testing.T, ctx context.Context, eventBlock blockInfo, rep *liveReport) {
	t.Helper()

	var receipts []shared.TransactionReceipt
	if err := h.rpc.CallContext(ctx, &receipts, "eth_getBlockReceipts", eventBlock.hash); err != nil {
		t.Fatalf("CORE FAILURE: eth_getBlockReceipts(%s): %v", eventBlock.hash, err)
	}

	decoded := make([]DecodedEvents, 0, len(receipts))
	for _, r := range receipts {
		d, _, err := DecodeEvents(r, h.poolsByID, h.poolManager, eventBlock.number, 0, eventBlock.timestamp)
		if err != nil {
			t.Fatalf("CORE FAILURE: DecodeEvents(tx=%s) in block-wide sweep: %v", r.TransactionHash, err)
		}
		decoded = append(decoded, d)
	}
	rep.blockReceipts = len(receipts)
	rep.blockUntrackedPoolLogs = assertUntrackedPoolsNotCaptured(t, receipts, decoded, h.poolsByID, h.poolManager, rep)
	if rep.blockUntrackedPoolLogs == 0 {
		t.Logf("note: block %d carried no PoolManager logs for untracked pools; the high-volume filter assertion was vacuous", eventBlock.number)
	}
}

// readTouchedTicks reads, per pool, the tick bounds this receipt's
// liquidity-changing events touched, pinned to the event block's hash. Grouping
// by pool is mandatory in V4: one receipt can carry ModifyLiquidity for several
// pools, and a tick position only means something relative to its own pool.
func (h *liveHarness) readTouchedTicks(t *testing.T, ctx context.Context, decoded DecodedEvents, eventBlock blockInfo) []*entity.UniswapV4Tick {
	t.Helper()
	if len(decoded.LiquidityEvents) == 0 {
		return nil
	}

	byPool := make(map[int64][]*entity.UniswapV4LiquidityEvent)
	for _, e := range decoded.LiquidityEvents {
		byPool[e.PoolID] = append(byPool[e.PoolID], e)
	}

	var rows []*entity.UniswapV4Tick
	for _, pool := range h.pools {
		events := byPool[pool.ID]
		if len(events) == 0 {
			continue
		}
		ticks := TouchedTicks(events)
		if len(ticks) == 0 {
			continue
		}
		calls, err := BuildTickCalls(pool, ticks)
		if err != nil {
			t.Fatalf("BuildTickCalls(pool=%s): %v", pool.PoolIDHash, err)
		}
		results, err := h.mc.ExecuteAtHash(ctx, calls, eventBlock.hash)
		if err != nil {
			t.Fatalf("CORE FAILURE: ExecuteAtHash(touched ticks, pool=%s): %v", pool.PoolIDHash, err)
		}
		for i, tick := range ticks {
			row, err := DecodeTick(pool, tick, eventBlock.number, 0, eventBlock.timestamp, results[i])
			if err != nil {
				t.Fatalf("DecodeTick(pool=%s, tick=%d): %v", pool.PoolIDHash, tick, err)
			}
			rows = append(rows, row)
		}
	}
	return rows
}

// -----------------------------------------------------------------------
// Baseline tick enumeration
// -----------------------------------------------------------------------

// baselineTickCheck enumerates every initialized tick on the tickSpacing=1 busy
// pool from its real bitmap, asserts the result is a strictly ascending set,
// re-reads every entry through getTickInfo (an "initialized" tick that reads
// back with liquidityGross == 0 would mean the bitmap→tick math is wrong), and
// closes the loop with the AMM's own completeness invariant:
//
//	sum(liquidityNet) over all initialized ticks <= currentTick == active liquidity
//
// That identity is what makes this more than a spot check — a tick the bitmap
// scan MISSED cannot be detected by re-reading the ticks it did find, but it
// breaks the sum. Reading every tick rather than a sample is what buys it.
func baselineTickCheck(t *testing.T, ctx context.Context, mc outbound.Multicaller, pools []RegisteredPool, states []*entity.UniswapV4PoolState, target blockInfo, rep *liveReport) {
	t.Helper()

	pool, ok := findPoolByID(pools, busyPoolID)
	if !ok {
		t.Fatalf("busy pool %s not found among loaded pools", busyPoolID)
	}
	if pool.TickSpacing != 1 {
		t.Fatalf("busy pool %s tickSpacing = %d, want 1 (the widest bitmap scan)", busyPoolID, pool.TickSpacing)
	}
	state := stateForPool(t, pools, states, pool.ID)

	baseline, err := BaselineTicks(ctx, mc, pool, target.hash)
	if err != nil {
		t.Fatalf("CORE FAILURE: BaselineTicks(%s): %v", pool.PoolIDHash, err)
	}
	rep.baselineTickCount = len(baseline)
	if len(baseline) == 0 {
		addFinding(t, rep, fmt.Sprintf("BaselineTicks(%s) returned an empty set at block %d — a live pool always has initialized ticks", pool.PoolIDHash, target.number))
		return
	}
	assertAscendingUnique(t, baseline, rep)
	rep.baselineTickMin, rep.baselineTickMax = baseline[0], baseline[len(baseline)-1]

	rows := readAllTicks(t, ctx, mc, pool, baseline, target)
	activeLiquidity := big.NewInt(0)
	for _, row := range rows {
		if row.LiquidityGross.Sign() <= 0 {
			addFinding(t, rep, fmt.Sprintf("baseline tick %d on pool %s reads back liquidityGross=0 — the bitmap said it was initialized", row.Tick, pool.PoolIDHash))
		}
		if row.Tick <= state.Tick {
			activeLiquidity.Add(activeLiquidity, row.LiquidityNet)
		}
	}
	rep.baselineLiquiditySum = activeLiquidity
	rep.baselinePoolLiquidity = state.Liquidity
	if activeLiquidity.Cmp(state.Liquidity) != 0 {
		addFinding(t, rep, fmt.Sprintf("baseline enumeration is incomplete on pool %s: sum(liquidityNet) over the %d initialized ticks at or below the current tick (%d) is %s, but StateView.getLiquidity reports %s",
			pool.PoolIDHash, len(rows), state.Tick, activeLiquidity, state.Liquidity))
	}

	for _, row := range sampleTicks(rows, baselineTickSampleSize) {
		rep.baselineSamples = append(rep.baselineSamples, *row)
	}
}

// stateForPool returns the snapshot taken for poolID in this run; snapshotAllPools
// keeps states index-aligned with pools.
func stateForPool(t *testing.T, pools []RegisteredPool, states []*entity.UniswapV4PoolState, poolID int64) *entity.UniswapV4PoolState {
	t.Helper()
	for i, p := range pools {
		if p.ID == poolID {
			return states[i]
		}
	}
	t.Fatalf("no snapshot taken for pool row id %d", poolID)
	return nil
}

func assertAscendingUnique(t *testing.T, ticks []int32, rep *liveReport) {
	t.Helper()
	if !slices.IsSortedFunc(ticks, func(a, b int32) int { return int(a - b) }) {
		addFinding(t, rep, "BaselineTicks returned an unsorted set")
	}
	for i := 1; i < len(ticks); i++ {
		if ticks[i] == ticks[i-1] {
			addFinding(t, rep, fmt.Sprintf("BaselineTicks returned duplicate tick %d", ticks[i]))
			return
		}
	}
}

// readAllTicks reads every given tick position through getTickInfo in bounded
// multicall batches, mirroring how the service chunks its own tick reads.
func readAllTicks(t *testing.T, ctx context.Context, mc outbound.Multicaller, pool RegisteredPool, ticks []int32, target blockInfo) []*entity.UniswapV4Tick {
	t.Helper()

	rows := make([]*entity.UniswapV4Tick, 0, len(ticks))
	for chunk := range slices.Chunk(ticks, tickbitmap.TicksPerCall) {
		calls, err := BuildTickCalls(pool, chunk)
		if err != nil {
			t.Fatalf("BuildTickCalls(baseline chunk): %v", err)
		}
		results, err := mc.ExecuteAtHash(ctx, calls, target.hash)
		if err != nil {
			t.Fatalf("CORE FAILURE: ExecuteAtHash(baseline chunk): %v", err)
		}
		for i, tick := range chunk {
			row, err := DecodeTick(pool, tick, target.number, 0, target.timestamp, results[i])
			if err != nil {
				t.Fatalf("DecodeTick(baseline tick %d): %v", tick, err)
			}
			rows = append(rows, row)
		}
	}
	return rows
}

// sampleTicks takes an evenly spread sample of at most n entries, so the report
// table spans the whole tick range rather than only its densest end.
func sampleTicks(ticks []*entity.UniswapV4Tick, n int) []*entity.UniswapV4Tick {
	if len(ticks) <= n {
		return ticks
	}
	out := make([]*entity.UniswapV4Tick, 0, n)
	step := len(ticks) / n
	for i := 0; i < len(ticks) && len(out) < n; i += step {
		out = append(out, ticks[i])
	}
	return out
}

// -----------------------------------------------------------------------
// Persistence plumbing + table counts
// -----------------------------------------------------------------------

// newLiveEventWriter builds a dexconsumer.ProtocolEventWriter against a real
// postgres.EventRepository and the UniswapV4 protocol row's real id (read back
// from the DB, not assumed), so captured logs persist to the same protocol_event
// mirror the live worker uses.
func newLiveEventWriter(t *testing.T, ctx context.Context, dbPool *pgxpool.Pool, buildID buildregistry.BuildID) *dexconsumer.ProtocolEventWriter {
	t.Helper()

	var protocolID int64
	if err := dbPool.QueryRow(ctx, `SELECT id FROM protocol WHERE chain_id = 1 AND name = 'UniswapV4'`).Scan(&protocolID); err != nil {
		t.Fatalf("reading UniswapV4 protocol id (seed migration missing?): %v", err)
	}
	return dexconsumer.NewProtocolEventWriter(protocolID, postgres.NewEventRepository(nil, buildID))
}

var uniswapV4ReportTables = []string{
	"uniswap_v4_pool_manager",
	"uniswap_v4_pool",
	"uniswap_v4_pool_state",
	"uniswap_v4_swap",
	"uniswap_v4_liquidity_event",
	"uniswap_v4_tick",
	"uniswap_v4_pool_event",
	"protocol_event",
}

func queryAllTableCounts(t *testing.T, ctx context.Context, dbPool *pgxpool.Pool) map[string]int64 {
	t.Helper()
	counts := make(map[string]int64, len(uniswapV4ReportTables))
	for _, table := range uniswapV4ReportTables {
		var n int64
		if err := dbPool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatalf("counting %s: %v", table, err)
		}
		counts[table] = n
	}
	return counts
}

// -----------------------------------------------------------------------
// Report
// -----------------------------------------------------------------------

// addFinding records a finding in both the test result and the report, so the
// markdown deliverable never disagrees with the pass/fail signal.
func addFinding(t *testing.T, rep *liveReport, msg string) {
	t.Helper()
	rep.findings = append(rep.findings, msg)
	t.Errorf("FINDING: %s", msg)
}

// liveReport accumulates every observation this test makes for the final
// markdown deliverable.
type liveReport struct {
	poolsLoaded      int
	poolManager      string
	stateView        string
	blockNumber      int64
	blockHash        string
	blockTimestamp   time.Time
	stateRowsWritten int64

	poolSnapshots    []poolSnapshotSummary
	slot0CrossChecks []slot0CrossCheck

	swapPoolID             string
	eventBlockNumber       int64
	eventBlockHash         string
	eventTxHash            string
	decodedSwaps           []*entity.UniswapV4Swap
	decodedLiquidityEvents []*entity.UniswapV4LiquidityEvent
	decodedPoolEvents      []*entity.UniswapV4PoolEvent
	capturedCount          int
	touchedPoolCount       int
	untrackedPoolLogs      int
	blockReceipts          int
	blockUntrackedPoolLogs int
	touchedTickCount       int
	swapDecodeNote         string

	liquidityBlockNumber int64
	liquidityTxHash      string
	liquidityEvents      []*entity.UniswapV4LiquidityEvent
	liquidityTickRows    int
	liquidityDecodeNote  string

	baselineTickCount     int
	baselineTickMin       int32
	baselineTickMax       int32
	baselineSamples       []entity.UniswapV4Tick
	baselineLiquiditySum  *big.Int
	baselinePoolLiquidity *big.Int

	tableCounts map[string]int64
	findings    []string
}

func newLiveReport() *liveReport { return &liveReport{} }

// writeAndLog renders the report to markdown, writes it to
// liveValidationReportPath, and t.Logs it so the data survives in `go test -v`
// output even if the file write fails.
func (r *liveReport) writeAndLog(t *testing.T) {
	t.Helper()
	md := r.render()
	t.Log(md)

	reportPath := liveValidationReportPath()
	if err := os.WriteFile(reportPath, []byte(md), 0o644); err != nil {
		t.Logf("warning: could not write report to %s: %v", reportPath, err)
		return
	}
	t.Logf("report written to %s", reportPath)
}

func (r *liveReport) render() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Uniswap V4 indexer — live validation report\n\n")
	fmt.Fprintf(&b, "PoolManager: %s\nStateView: %s\nBlock: %d\nHash: %s\nTimestamp: %s\nPools loaded: %d\nState rows written: %d\n\n",
		r.poolManager, r.stateView, r.blockNumber, r.blockHash, r.blockTimestamp.Format(time.RFC3339), r.poolsLoaded, r.stateRowsWritten)

	r.renderSnapshots(&b)
	r.renderCrossChecks(&b)
	r.renderSwap(&b)
	r.renderLiquidity(&b)
	r.renderBaseline(&b)
	r.renderCounts(&b)
	r.renderFindings(&b)
	return b.String()
}

func (r *liveReport) renderSnapshots(b *strings.Builder) {
	fmt.Fprintf(b, "## Pool state snapshots (all %d pools, block %d)\n\n", len(r.poolSnapshots), r.blockNumber)
	fmt.Fprintf(b, "| PoolId | currency0 | currency1 | fee | ts | hooks | sqrtPriceX96 | tick | protocolFee | lpFee | liquidity | feeGrowth0X128 | feeGrowth1X128 |\n")
	fmt.Fprintf(b, "|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, s := range r.poolSnapshots {
		fmt.Fprintf(b, "| %s | %s | %s | %d | %d | %s | %s | %d | %d | %d | %s | %s | %s |\n",
			s.poolID.Hex(), s.currency0.Hex(), s.currency1.Hex(), s.fee, s.tickSpacing, s.hooks.Hex(),
			bigOrNil(s.sqrtPriceX96), s.tick, s.protocolFee, s.lpFee,
			bigOrNil(s.liquidity), bigOrNil(s.feeGrowth0), bigOrNil(s.feeGrowth1))
	}

	fmt.Fprintf(b, "\nPools with a non-zero protocol fee:\n\n")
	any := false
	for _, s := range r.poolSnapshots {
		if s.protocolFee == 0 {
			continue
		}
		any = true
		fmt.Fprintf(b, "- %s protocolFee=%d (zeroForOne=%d, oneForZero=%d)\n", s.poolID.Hex(), s.protocolFee, s.protocolFee&0xFFF, s.protocolFee>>12)
	}
	if !any {
		fmt.Fprintf(b, "None — every seeded pool has protocolFee == 0 at this block.\n")
	}
	fmt.Fprintln(b)
}

func (r *liveReport) renderCrossChecks(b *strings.Builder) {
	fmt.Fprintf(b, "## slot0 cross-check (multicall vs direct eth_call, same block hash)\n\n")
	fmt.Fprintf(b, "| PoolId | indexed sqrtPriceX96 / tick / protocolFee / lpFee | direct sqrtPriceX96 / tick / protocolFee / lpFee | match |\n|---|---|---|---|\n")
	for _, c := range r.slot0CrossChecks {
		fmt.Fprintf(b, "| %s | %s / %d / %d / %d | %s / %d / %d / %d | %t |\n",
			c.poolID.Hex(), bigOrNil(c.indexed.SqrtPriceX96), c.indexed.Tick, c.indexed.ProtocolFee, c.indexed.LpFee,
			bigOrNil(c.direct.sqrtPriceX96), c.direct.tick, c.direct.protocolFee, c.direct.lpFee, c.matching)
	}
	fmt.Fprintln(b)
}

func (r *liveReport) renderSwap(b *strings.Builder) {
	fmt.Fprintf(b, "## Decoded real transaction\n\n")
	if r.swapDecodeNote != "" {
		fmt.Fprintf(b, "%s\n\n", r.swapDecodeNote)
		return
	}
	fmt.Fprintf(b, "Target pool: %s\nEvent block: %d\nEvent block hash: %s\nTx: %s\nCaptured logs (protocol_event mirror): %d\nRegistered pools touched: %d\nPoolManager logs for UNTRACKED pools in the same receipt (must not be captured): %d\nWhole-block sweep: %d receipts, %d PoolManager logs for untracked pools (none captured)\nTouched tick rows persisted: %d\n\n",
		r.swapPoolID, r.eventBlockNumber, r.eventBlockHash, r.eventTxHash, r.capturedCount, r.touchedPoolCount, r.untrackedPoolLogs, r.blockReceipts, r.blockUntrackedPoolLogs, r.touchedTickCount)

	fmt.Fprintf(b, "Swaps:\n\n")
	for _, sw := range r.decodedSwaps {
		fmt.Fprintf(b, "- poolRowID=%d logIndex=%d sender=%s amount0=%s amount1=%s sqrtPriceX96=%s liquidity=%s tick=%d fee=%d\n",
			sw.PoolID, sw.LogIndex, sw.Sender.Hex(), sw.Amount0, sw.Amount1, sw.SqrtPriceX96, sw.Liquidity, sw.Tick, sw.Fee)
	}
	if len(r.decodedLiquidityEvents) > 0 {
		fmt.Fprintf(b, "\nModifyLiquidity:\n\n")
		for _, le := range r.decodedLiquidityEvents {
			fmt.Fprintf(b, "- poolRowID=%d logIndex=%d sender=%s tickLower=%d tickUpper=%d liquidityDelta=%s salt=%s\n",
				le.PoolID, le.LogIndex, le.Sender.Hex(), le.TickLower, le.TickUpper, bigOrNil(le.LiquidityDelta), le.Salt.Hex())
		}
	} else {
		fmt.Fprintf(b, "\nNo ModifyLiquidity for a tracked pool in this receipt.\n")
	}
	if len(r.decodedPoolEvents) > 0 {
		fmt.Fprintf(b, "\nPool events:\n\n")
		for _, pe := range r.decodedPoolEvents {
			fmt.Fprintf(b, "- poolRowID=%d logIndex=%d event=%s params=%s\n", pe.PoolID, pe.LogIndex, pe.EventName, string(pe.Params))
		}
	}
	fmt.Fprintln(b)
}

func (r *liveReport) renderLiquidity(b *strings.Builder) {
	fmt.Fprintf(b, "## Decoded real ModifyLiquidity\n\n")
	if r.liquidityDecodeNote != "" {
		fmt.Fprintf(b, "%s\n\n", r.liquidityDecodeNote)
		return
	}
	fmt.Fprintf(b, "Block: %d\nTx: %s\nTick rows read at that block's hash and persisted: %d\n\n", r.liquidityBlockNumber, r.liquidityTxHash, r.liquidityTickRows)
	for _, e := range r.liquidityEvents {
		fmt.Fprintf(b, "- poolRowID=%d logIndex=%d sender=%s tickLower=%d tickUpper=%d liquidityDelta=%s salt=%s\n",
			e.PoolID, e.LogIndex, e.Sender.Hex(), e.TickLower, e.TickUpper, bigOrNil(e.LiquidityDelta), e.Salt.Hex())
	}
	fmt.Fprintln(b)
}

func (r *liveReport) renderBaseline(b *strings.Builder) {
	fmt.Fprintf(b, "## Baseline tick enumeration (pool %s, tickSpacing 1, block %d)\n\n", busyPoolID.Hex(), r.blockNumber)
	fmt.Fprintf(b, "Initialized ticks found: %d (min %d, max %d)\n\n", r.baselineTickCount, r.baselineTickMin, r.baselineTickMax)
	fmt.Fprintf(b, "Completeness invariant: sum(liquidityNet) over initialized ticks <= current tick = %s; StateView.getLiquidity = %s (equal: %t)\n\n",
		bigOrNil(r.baselineLiquiditySum), bigOrNil(r.baselinePoolLiquidity), r.baselineLiquiditySum != nil && r.baselinePoolLiquidity != nil && r.baselineLiquiditySum.Cmp(r.baselinePoolLiquidity) == 0)
	if len(r.baselineSamples) == 0 {
		return
	}
	fmt.Fprintf(b, "| tick | liquidityGross | liquidityNet | feeGrowthOutside0X128 | feeGrowthOutside1X128 |\n|---|---|---|---|---|\n")
	for _, s := range r.baselineSamples {
		fmt.Fprintf(b, "| %d | %s | %s | %s | %s |\n", s.Tick, bigOrNil(s.LiquidityGross), bigOrNil(s.LiquidityNet), bigOrNil(s.FeeGrowthOutside0X128), bigOrNil(s.FeeGrowthOutside1X128))
	}
	fmt.Fprintln(b)
}

func (r *liveReport) renderCounts(b *strings.Builder) {
	fmt.Fprintf(b, "## Per-table row counts\n\n| Table | Rows |\n|---|---|\n")
	tables := make([]string, 0, len(r.tableCounts))
	for k := range r.tableCounts {
		tables = append(tables, k)
	}
	sort.Strings(tables)
	for _, tbl := range tables {
		fmt.Fprintf(b, "| %s | %d |\n", tbl, r.tableCounts[tbl])
	}
	fmt.Fprintln(b)
}

func (r *liveReport) renderFindings(b *strings.Builder) {
	fmt.Fprintf(b, "## Findings\n\n")
	if len(r.findings) == 0 {
		fmt.Fprintf(b, "None.\n")
		return
	}
	for _, f := range r.findings {
		fmt.Fprintf(b, "- %s\n", f)
	}
}

func bigOrNil(v *big.Int) string {
	if v == nil {
		return "nil"
	}
	return v.String()
}
