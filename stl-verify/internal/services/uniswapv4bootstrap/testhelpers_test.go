package uniswapv4bootstrap

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4indexer"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const (
	testChainID     = int64(1)
	poolManagerAddr = "0x000000000004444c5dc75cB358380D2e3dE08A90"
	stateViewAddr   = "0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227"
	// Three real mainnet pools, transcribed from the V4 migration's verified
	// seed. The whole PoolKey has to be real, not just the id: the constructor
	// re-derives keccak256(abi.encode(key)) and rejects any disagreement.
	poolAIDHash = "0x1d5b2949ece8754c2d736991c62c5162bd144f497b2212182401b9bae77e2d76"
	poolBIDHash = "0x84a2753546221b6aedf1b96098235f8eb5494b1ddd7d57583d99b2d174cd2103"
	poolCIDHash = "0xef3a1d51982c20ee2f125e6d6d1f9d3a10c1e94391b828040943005a1ea27e14"
	wstethAddr  = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"
	usdcAddr    = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
	wbtcAddr    = "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599"
	ownerA      = "0x1111111111111111111111111111111111111111"
	ownerB      = "0x2222222222222222222222222222222222222222"
	saltA       = "0x00000000000000000000000000000000000000000000000000000000000000aa"
	saltB       = "0x00000000000000000000000000000000000000000000000000000000000000bb"
	txHashA     = "0xfeed000000000000000000000000000000000000000000000000000000000001"
	pinHash     = "0x2222222222222222222222222222222222222222222222222222222222222222"
	forkHash    = "0x3333333333333333333333333333333333333333333333333333333333333333"
	// 2025-08-19T04:09:36Z, the timestamp every fixture header carries.
	pinTimestampHex  = "0x68a3f900"
	pinTimestampUnix = int64(0x68a3f900)
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// poolFixture is one seeded mainnet pool's identity, from which testPool builds
// a RegisteredPool whose PoolId the constructor can re-derive.
type poolFixture struct {
	id          int64
	poolIDHash  string
	currency0   string
	currency1   string
	fee         int
	tickSpacing int
	deployBlock int64
}

var (
	poolAFixture = poolFixture{7, poolAIDHash, "0x0000000000000000000000000000000000000000", wstethAddr, 100, 1, 21743144}
	poolBFixture = poolFixture{9, poolBIDHash, wstethAddr, usdcAddr, 3000, 60, 22962297}
	poolCFixture = poolFixture{11, poolCIDHash, wbtcAddr, wstethAddr, 3000, 60, 22552041}
)

func testPool(f poolFixture, snapshotSupported bool) uniswapv4indexer.RegisteredPool {
	return uniswapv4indexer.RegisteredPool{
		ID:                f.id,
		PoolManager:       common.HexToAddress(poolManagerAddr),
		StateView:         common.HexToAddress(stateViewAddr),
		PoolIDHash:        common.HexToHash(f.poolIDHash),
		Currency0:         common.HexToAddress(f.currency0),
		Currency1:         common.HexToAddress(f.currency1),
		Currency0Decimals: 18,
		Currency1Decimals: 18,
		Fee:               f.fee,
		TickSpacing:       f.tickSpacing,
		DeployBlock:       f.deployBlock,
		SnapshotSupported: snapshotSupported,
	}
}

func testPools() []uniswapv4indexer.RegisteredPool {
	return []uniswapv4indexer.RegisteredPool{
		testPool(poolAFixture, true),
		testPool(poolBFixture, true),
	}
}

// poolManagerABIForTest independently parses the ModifyLiquidity event so
// fixtures are built without reaching into the indexer package's ABI.
func poolManagerABIForTest(t *testing.T) *abi.ABI {
	t.Helper()
	a, err := uniswapv4indexer.PoolManagerABI()
	if err != nil {
		t.Fatalf("PoolManagerABI: %v", err)
	}
	return a
}

// modifyLiquidityFilteredLog builds one wire-shaped ModifyLiquidity log, as
// eth_getLogs would return it.
func modifyLiquidityFilteredLog(t *testing.T, poolIDHash, owner string, tickLower, tickUpper int64, salt string, blockNumber int64, logIndex int) outbound.FilteredLog {
	t.Helper()
	ev := poolManagerABIForTest(t).Events["ModifyLiquidity"]

	var nonIndexed abi.Arguments
	for _, arg := range ev.Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}
	data, err := nonIndexed.Pack(big.NewInt(tickLower), big.NewInt(tickUpper), big.NewInt(1000), common.HexToHash(salt))
	if err != nil {
		t.Fatalf("packing ModifyLiquidity data: %v", err)
	}

	return outbound.FilteredLog{
		Address: poolManagerAddr,
		Topics: []string{
			ev.ID.Hex(),
			common.HexToHash(poolIDHash).Hex(),
			common.BytesToHash(common.HexToAddress(owner).Bytes()).Hex(),
		},
		Data:             "0x" + hex.EncodeToString(data),
		BlockHash:        pinHash,
		BlockNumber:      "0x" + strconv.FormatInt(blockNumber, 16),
		TransactionHash:  txHashA,
		TransactionIndex: "0x0",
		LogIndex:         "0x" + strconv.FormatInt(int64(logIndex), 16),
	}
}

// fakeLogScanClient is the outbound.LogScanClient double every test in this
// package drives. GetLogsFn owns the whole windowing contract, so a test says
// which ranges succeed and which are refused; unset, it answers no logs.
type fakeLogScanClient struct {
	mu sync.Mutex

	Head    int64
	HeadErr error
	// HeadCalls counts head reads, which is how "an explicit pin is still
	// checked against the head" is distinguished from "the head is skipped".
	HeadCalls int
	// HeaderByNumber answers GetBlockHeaderByNumber. A missing height is an
	// error, so a test never silently pins a block it did not configure.
	HeaderByNumber map[int64]*outbound.BlockHeader
	HeaderErr      error
	// HeaderCalls counts header reads, which is how the pin-stability re-read is
	// distinguished from the initial pin.
	HeaderCalls int

	GetLogsFn func(outbound.LogFilter) ([]outbound.FilteredLog, error)
	Filters   []outbound.LogFilter
}

func newFakeLogScanClient(head int64, headers map[int64]*outbound.BlockHeader) *fakeLogScanClient {
	return &fakeLogScanClient{Head: head, HeaderByNumber: headers}
}

func (f *fakeLogScanClient) GetCurrentBlockNumber(_ context.Context) (int64, error) {
	f.mu.Lock()
	f.HeadCalls++
	f.mu.Unlock()

	if f.HeadErr != nil {
		return 0, f.HeadErr
	}
	return f.Head, nil
}

func (f *fakeLogScanClient) GetBlockHeaderByNumber(_ context.Context, blockNumber int64) (*outbound.BlockHeader, error) {
	f.mu.Lock()
	f.HeaderCalls++
	call := f.HeaderCalls
	f.mu.Unlock()

	if f.HeaderErr != nil {
		return nil, f.HeaderErr
	}
	header, ok := f.HeaderByNumber[blockNumber]
	if !ok {
		return nil, fmt.Errorf("fake: no header configured for block %d (call %d)", blockNumber, call)
	}
	return header, nil
}

func (f *fakeLogScanClient) GetLogs(_ context.Context, filter outbound.LogFilter) ([]outbound.FilteredLog, error) {
	f.mu.Lock()
	f.Filters = append(f.Filters, filter)
	f.mu.Unlock()

	if f.GetLogsFn == nil {
		return nil, nil
	}
	return f.GetLogsFn(filter)
}

// header returns a fixture header for blockNumber carrying hash.
func header(blockNumber int64, hash string) *outbound.BlockHeader {
	return &outbound.BlockHeader{
		Number:    "0x" + strconv.FormatInt(blockNumber, 16),
		Hash:      hash,
		Timestamp: pinTimestampHex,
	}
}

// fakeUniswapV4Repository records SavePositions' batches. Every other port
// method errors: the bootstrap writes positions and nothing else, and must not
// reach for the per-block reorg helpers — a silent nil would hide it doing so.
type fakeUniswapV4Repository struct {
	// SavePositionsFn overrides one batch's outcome. The returned count stands in
	// for the rows the append-on-change writer actually inserted.
	SavePositionsFn func([]*entity.UniswapV4Position) (int64, error)
	SavedBatches    [][]*entity.UniswapV4Position
}

func (f *fakeUniswapV4Repository) SavePositions(_ context.Context, _ pgx.Tx, positions []*entity.UniswapV4Position) (int64, error) {
	f.SavedBatches = append(f.SavedBatches, positions)
	if f.SavePositionsFn != nil {
		return f.SavePositionsFn(positions)
	}
	return int64(len(positions)), nil
}

func (f *fakeUniswapV4Repository) SaveBlock(context.Context, pgx.Tx, outbound.UniswapV4BlockWrites) (outbound.StateRowCounts, error) {
	return outbound.StateRowCounts{}, errors.New("fake: SaveBlock writes a whole block; the bootstrap owns positions only")
}

func (f *fakeUniswapV4Repository) LoadPools(context.Context, int64) ([]outbound.UniswapV4PoolRow, error) {
	return nil, errors.New("fake: LoadPools is the caller's job, not the bootstrap's")
}

func (f *fakeUniswapV4Repository) PoolIDsWithStateAtBlock(context.Context, int64, int64, time.Time) ([]int64, error) {
	return nil, errors.New("fake: PoolIDsWithStateAtBlock is a reorg-path read the bootstrap must not make")
}

func (f *fakeUniswapV4Repository) TicksForPoolAtBlock(context.Context, int64, int64, int64) ([]int32, error) {
	return nil, errors.New("fake: TicksForPoolAtBlock is a reorg-path read the bootstrap must not make")
}

func (f *fakeUniswapV4Repository) PositionsForPoolAtBlock(context.Context, int64, int64) ([]entity.UniswapV4PositionKey, error) {
	return nil, errors.New("fake: PositionsForPoolAtBlock is a reorg-path read the bootstrap must not make")
}

func (f *fakeUniswapV4Repository) PoolIDsEverSnapshotted(context.Context, int64) ([]int64, error) {
	return nil, errors.New("fake: PoolIDsEverSnapshotted is a live-service read the bootstrap must not make")
}

// savedPositions flattens every batch's rows in write order.
func (f *fakeUniswapV4Repository) savedPositions() []*entity.UniswapV4Position {
	var all []*entity.UniswapV4Position
	for _, batch := range f.SavedBatches {
		all = append(all, batch...)
	}
	return all
}

// capturedLogs is a slog.Handler that keeps every record, so a test can assert
// on a per-batch log field the Summary deliberately does not carry.
type capturedLogs struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *capturedLogs) Enabled(context.Context, slog.Level) bool { return true }

func (c *capturedLogs) Handle(_ context.Context, record slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, record.Clone())
	return nil
}

func (c *capturedLogs) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *capturedLogs) WithGroup(string) slog.Handler      { return c }

// int64Field returns key's value from every record whose message is msg, in
// emission order.
func (c *capturedLogs) int64Field(msg, key string) []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	var values []int64
	for _, record := range c.records {
		if record.Message != msg {
			continue
		}
		record.Attrs(func(attr slog.Attr) bool {
			if attr.Key == key {
				values = append(values, attr.Value.Int64())
			}
			return true
		})
	}
	return values
}

// positionReturningMulticaller answers every getPositionInfo sub-call with the
// same liquidity and fee-growth checkpoints.
func positionReturningMulticaller(t *testing.T, liquidity int64) *testutil.MockMulticaller {
	t.Helper()
	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i := range results {
			results[i] = outbound.Result{Success: true, ReturnData: packPositionInfoReturn(t, big.NewInt(liquidity), big.NewInt(0), big.NewInt(0))}
		}
		return results, nil
	}
	return mc
}

// packPositionInfoReturn encodes a getPositionInfo return tuple.
func packPositionInfoReturn(t *testing.T, liquidity, feeGrowthInside0, feeGrowthInside1 *big.Int) []byte {
	t.Helper()
	const j = `[
		{"name":"getPositionInfo","type":"function","stateMutability":"view","inputs":[
			{"name":"poolId","type":"bytes32"},
			{"name":"owner","type":"address"},
			{"name":"tickLower","type":"int24"},
			{"name":"tickUpper","type":"int24"},
			{"name":"salt","type":"bytes32"}
		],"outputs":[
			{"name":"liquidity","type":"uint128"},
			{"name":"feeGrowthInside0LastX128","type":"uint256"},
			{"name":"feeGrowthInside1LastX128","type":"uint256"}
		]}
	]`
	a, err := abi.JSON(strings.NewReader(j))
	if err != nil {
		t.Fatalf("parsing position view test ABI: %v", err)
	}
	packed, err := a.Methods["getPositionInfo"].Outputs.Pack(liquidity, feeGrowthInside0, feeGrowthInside1)
	if err != nil {
		t.Fatalf("packing getPositionInfo return: %v", err)
	}
	return packed
}
