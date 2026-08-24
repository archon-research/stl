package uniswapv4bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4indexer"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const (
	testHead     = int64(23_000_064)
	testPinned   = int64(23_000_000)
	testFromMin  = int64(21_743_144) // the lower of the two fixture pools' deploy blocks
	testMaxRange = int64(2_000_000)
)

// bootstrapFixture wires a Service over fakes and hands the test the doubles it
// asserts on.
type bootstrapFixture struct {
	svc    *Service
	client *fakeLogScanClient
	repo   *fakeUniswapV4Repository
	mc     *testutil.MockMulticaller
	txMgr  *testutil.MockTxManager
}

// newFixture builds a Service whose scan covers the fixture pools and whose
// pin resolves to testPinned. mutate adjusts the deps before construction.
func newFixture(t *testing.T, mutate func(*Deps)) *bootstrapFixture {
	t.Helper()

	client := newFakeLogScanClient(testHead, map[int64]*outbound.BlockHeader{
		testPinned: header(testPinned, pinHash),
	})
	repo := &fakeUniswapV4Repository{}
	mc := positionReturningMulticaller(t, 5000)
	txMgr := &testutil.MockTxManager{}

	deps := Deps{
		Pools:       testPools(),
		LogScan:     client,
		Multicaller: mc,
		Repo:        repo,
		TxManager:   txMgr,
		Logger:      testLogger(),
		Config: Config{
			ChainID:       testChainID,
			InitialWindow: testMaxRange,
			MaxWindow:     testMaxRange,
		},
	}
	if mutate != nil {
		mutate(&deps)
	}

	svc, err := New(deps)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return &bootstrapFixture{svc: svc, client: client, repo: repo, mc: mc, txMgr: txMgr}
}

func TestNew_RejectsIncompleteDeps(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Deps)
		wantErr string
	}{
		{"no pools", func(d *Deps) { d.Pools = nil }, "pool"},
		{"no log client", func(d *Deps) { d.LogScan = nil }, "log scan client"},
		{"no multicaller", func(d *Deps) { d.Multicaller = nil }, "multicaller"},
		{"no repo", func(d *Deps) { d.Repo = nil }, "repo"},
		{"no tx manager", func(d *Deps) { d.TxManager = nil }, "txManager"},
		{"no logger", func(d *Deps) { d.Logger = nil }, "logger"},
		{"bad config", func(d *Deps) { d.Config.ChainID = 0 }, "chainID"},
		{
			name: "no snapshottable pool",
			mutate: func(d *Deps) {
				d.Pools = []uniswapv4indexer.RegisteredPool{testPool(poolAFixture, false)}
			},
			wantErr: "snapshot",
		},
		{
			name: "pool id disagrees with its key",
			mutate: func(d *Deps) {
				pools := testPools()
				pools[0].Fee = 3000
				d.Pools = pools
			},
			wantErr: "registry bug",
		},
		{
			name: "two PoolManager deployments",
			mutate: func(d *Deps) {
				pools := testPools()
				pools[1].PoolManager = common.HexToAddress("0x000000000000000000000000000000000000dead")
				d.Pools = pools
			},
			wantErr: "PoolManager",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := Deps{
				Pools:       testPools(),
				LogScan:     newFakeLogScanClient(testHead, nil),
				Multicaller: testutil.NewMockMulticaller(),
				Repo:        &fakeUniswapV4Repository{},
				TxManager:   &testutil.MockTxManager{},
				Logger:      testLogger(),
				Config:      Config{ChainID: testChainID},
			}
			tt.mutate(&deps)

			_, err := New(deps)
			if err == nil {
				t.Fatalf("expected an error for %s", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %v, want it to name %q", err, tt.wantErr)
			}
		})
	}
}

func TestNew_ScansOnlySnapshotSupportedPools(t *testing.T) {
	pools := append(testPools(), testPool(poolCFixture, false))
	f := newFixture(t, func(d *Deps) { d.Pools = pools })

	if got := len(f.svc.pools); got != 2 {
		t.Fatalf("scanned pools = %d, want 2: the registry's snapshot gate excludes the third", got)
	}
}

func TestRun_PersistsOnePositionPerDiscoveredKeyAtThePinnedBlock(t *testing.T) {
	f := newFixture(t, nil)
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0),
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_900_000, 1),
			modifyLiquidityFilteredLog(t, poolBIDHash, ownerB, -60, 60, saltB, 22_990_000, 2),
		}, nil
	}

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	rows := f.repo.savedPositions()
	if len(rows) != 2 {
		t.Fatalf("persisted positions = %d, want 2 (the repeated key collapses)", len(rows))
	}
	if summary.PositionsRead != 2 || summary.Keys != 2 {
		t.Errorf("summary keys/positions = %d/%d, want 2/2", summary.Keys, summary.PositionsRead)
	}
	if summary.PinnedBlock != testPinned || summary.PinnedHash != common.HexToHash(pinHash) {
		t.Errorf("summary pin = (%d, %s), want (%d, %s)", summary.PinnedBlock, summary.PinnedHash, testPinned, pinHash)
	}
	for _, row := range rows {
		if row.BlockNumber != testPinned {
			t.Errorf("row %+v block = %d, want %d", row.Key(), row.BlockNumber, testPinned)
		}
		if row.BlockVersion != 0 {
			t.Errorf("row %+v version = %d, want 0: a finality-pinned block has one version", row.Key(), row.BlockVersion)
		}
		if row.BlockTimestamp.Unix() != pinTimestampUnix {
			t.Errorf("row %+v timestamp = %s, want the pinned header's", row.Key(), row.BlockTimestamp)
		}
	}
}

func TestRun_PinsEveryStateReadToThePinnedHash(t *testing.T) {
	f := newFixture(t, nil)
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0)}, nil
	}

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(f.mc.Invocations) == 0 {
		t.Fatal("no multicall was issued")
	}
	for i, inv := range f.mc.Invocations {
		if !inv.ViaHash || inv.BlockHash != common.HexToHash(pinHash) {
			t.Errorf("invocation %d pinned to %s (viaHash=%v), want %s", i, inv.BlockHash, inv.ViaHash, pinHash)
		}
	}
}

func TestRun_PersistsAZeroLiquidityPositionAsAClosedRow(t *testing.T) {
	f := newFixture(t, func(d *Deps) { d.Multicaller = positionReturningMulticaller(t, 0) })
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0)}, nil
	}

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	rows := f.repo.savedPositions()
	if len(rows) != 1 {
		t.Fatalf("persisted positions = %d, want 1: an all-zero row records a closed position", len(rows))
	}
	if rows[0].Liquidity.Sign() != 0 {
		t.Errorf("liquidity = %s, want 0", rows[0].Liquidity)
	}
}

func TestRun_ScansFromTheLowestDeployBlockToThePin(t *testing.T) {
	f := newFixture(t, nil)

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if summary.FromBlock != testFromMin {
		t.Errorf("summary.FromBlock = %d, want %d", summary.FromBlock, testFromMin)
	}
	if len(f.client.Filters) == 0 {
		t.Fatal("no log query was issued")
	}
	first := f.client.Filters[0]
	last := f.client.Filters[len(f.client.Filters)-1]
	if first.FromBlock != testFromMin {
		t.Errorf("first window fromBlock = %d, want %d", first.FromBlock, testFromMin)
	}
	if last.ToBlock != testPinned {
		t.Errorf("last window toBlock = %d, want the pinned block %d", last.ToBlock, testPinned)
	}
}

func TestRun_FromBlockOverrideNarrowsTheScan(t *testing.T) {
	f := newFixture(t, func(d *Deps) { d.Config.FromBlock = 22_500_000 })

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.FromBlock != 22_500_000 {
		t.Errorf("summary.FromBlock = %d, want the override", summary.FromBlock)
	}
	if f.client.Filters[0].FromBlock != 22_500_000 {
		t.Errorf("first window fromBlock = %d, want the override", f.client.Filters[0].FromBlock)
	}
}

func TestRun_FiltersOnThePoolManagerAndTheRegisteredPoolIDs(t *testing.T) {
	f := newFixture(t, nil)

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	topic0, err := uniswapv4indexer.ModifyLiquidityTopic0()
	if err != nil {
		t.Fatalf("ModifyLiquidityTopic0: %v", err)
	}
	for i, filter := range f.client.Filters {
		if filter.Address != common.HexToAddress(poolManagerAddr) {
			t.Errorf("window %d address = %s, want the PoolManager", i, filter.Address)
		}
		if filter.Topic0 != topic0 {
			t.Errorf("window %d topic0 = %s, want ModifyLiquidity", i, filter.Topic0)
		}
		if len(filter.Topic1) != 2 {
			t.Errorf("window %d topic1 = %v, want both registered pool ids", i, filter.Topic1)
		}
	}
}

func TestRun_ChecksThePinIsStillCanonicalAfterTheScan(t *testing.T) {
	f := newFixture(t, nil)

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if f.client.HeaderCalls != 2 {
		t.Errorf("header reads = %d, want 2 (pin, then the stability re-read)", f.client.HeaderCalls)
	}
}

func TestRun_ReorgedPinStopsBeforeAnyWrite(t *testing.T) {
	f := newFixture(t, nil)
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		// The pin moves under the scan, as a reorg past finality would look.
		f.client.HeaderByNumber[testPinned] = header(testPinned, forkHash)
		return []outbound.FilteredLog{modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0)}, nil
	}

	_, err := f.svc.Run(context.Background())
	if err == nil {
		t.Fatal("expected an error: the pinned block was reorged")
	}
	if len(f.repo.SavedBatches) != 0 {
		t.Errorf("write batches = %d, want 0: nothing may be written against a moved pin", len(f.repo.SavedBatches))
	}
}

func TestRun_ChunksReadsAndWritesAtThePositionBatch(t *testing.T) {
	f := newFixture(t, func(d *Deps) { d.Config.PositionBatch = 2 })
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0),
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltB, 21_800_001, 1),
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerB, -100, 200, saltA, 21_800_002, 2),
		}, nil
	}

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(f.repo.SavedBatches) != 2 {
		t.Fatalf("write batches = %d, want 2 (batches of 2 and 1)", len(f.repo.SavedBatches))
	}
	if len(f.repo.SavedBatches[0]) != 2 || len(f.repo.SavedBatches[1]) != 1 {
		t.Errorf("batch sizes = %d/%d, want 2/1", len(f.repo.SavedBatches[0]), len(f.repo.SavedBatches[1]))
	}
}

func TestRun_NoDiscoveredKeysWritesNothing(t *testing.T) {
	f := newFixture(t, nil)

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.Keys != 0 || len(f.repo.SavedBatches) != 0 {
		t.Errorf("keys = %d, write batches = %d, want 0 and 0", summary.Keys, len(f.repo.SavedBatches))
	}
	if f.mc.CallCount != 0 {
		t.Errorf("multicall invocations = %d, want 0", f.mc.CallCount)
	}
}

func TestRun_FailsWhenTheScanStartIsAboveThePin(t *testing.T) {
	f := newFixture(t, func(d *Deps) { d.Config.FromBlock = testPinned + 1 })

	_, err := f.svc.Run(context.Background())
	if err == nil {
		t.Fatal("expected an error: there is nothing to scan")
	}
	if !strings.Contains(err.Error(), "pinned block") {
		t.Errorf("error = %v, want it to name the pinned block", err)
	}
}

func TestRun_PropagatesEveryStageFailure(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name   string
		mutate func(*bootstrapFixture)
	}{
		{
			name:   "pin read fails",
			mutate: func(f *bootstrapFixture) { f.client.HeadErr = boom },
		},
		{
			name: "log scan fails",
			mutate: func(f *bootstrapFixture) {
				f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) { return nil, boom }
			},
		},
		{
			name: "a scanned log does not decode",
			mutate: func(f *bootstrapFixture) {
				f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
					return []outbound.FilteredLog{{Address: poolManagerAddr, Topics: []string{"0xdeadbeef"}, LogIndex: "0x0", TransactionHash: txHashA}}, nil
				}
			},
		},
		{
			name: "a position sub-call reverts",
			mutate: func(f *bootstrapFixture) {
				f.client.GetLogsFn = oneLogFn(f)
				f.mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return make([]outbound.Result, len(calls)), nil
				}
			},
		},
		{
			name: "the multicall fails",
			mutate: func(f *bootstrapFixture) {
				f.client.GetLogsFn = oneLogFn(f)
				f.mc.ExecuteAtHashFn = func(context.Context, []outbound.Call, common.Hash) ([]outbound.Result, error) { return nil, boom }
			},
		},
		{
			name: "the write fails",
			mutate: func(f *bootstrapFixture) {
				f.client.GetLogsFn = oneLogFn(f)
				f.repo.SavePositionsFn = func([]*entity.UniswapV4Position) (int64, error) { return 0, boom }
			},
		},
		{
			name: "the transaction fails",
			mutate: func(f *bootstrapFixture) {
				f.client.GetLogsFn = oneLogFn(f)
				f.txMgr.WithTransactionFn = func(context.Context, func(pgx.Tx) error) error { return boom }
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, nil)
			tt.mutate(f)

			if _, err := f.svc.Run(context.Background()); err == nil {
				t.Fatalf("expected an error when %s", tt.name)
			}
		})
	}
}

// oneLogFn answers every window with one ModifyLiquidity log, so a test can
// reach the read-and-persist stage without restating the fixture.
func oneLogFn(f *bootstrapFixture) func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
	return func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{{
			Address: poolManagerAddr,
			Topics: []string{
				mustModifyLiquidityTopic0().Hex(),
				common.HexToHash(poolAIDHash).Hex(),
				common.BytesToHash(common.HexToAddress(ownerA).Bytes()).Hex(),
			},
			Data:            encodedModifyLiquidityData,
			TransactionHash: txHashA,
			LogIndex:        "0x0",
			BlockNumber:     "0x14cf4a0",
		}}, nil
	}
}

func mustModifyLiquidityTopic0() common.Hash {
	topic0, err := uniswapv4indexer.ModifyLiquidityTopic0()
	if err != nil {
		panic(err)
	}
	return topic0
}

// encodedModifyLiquidityData is (tickLower=-100, tickUpper=200,
// liquidityDelta=1000, salt=0x..aa) ABI-encoded.
var encodedModifyLiquidityData = "0x" +
	"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff9c" +
	"00000000000000000000000000000000000000000000000000000000000000c8" +
	"00000000000000000000000000000000000000000000000000000000000003e8" +
	"00000000000000000000000000000000000000000000000000000000000000aa"

func TestRun_CancelledContextStopsTheRun(t *testing.T) {
	f := newFixture(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := f.svc.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestRun_KeysAreCompareSortedPerPool(t *testing.T) {
	f := newFixture(t, nil)
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerB, -100, 200, saltA, 21_800_000, 0),
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltB, 21_800_001, 1),
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_002, 2),
		}, nil
	}

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	rows := f.repo.savedPositions()
	if len(rows) != 3 {
		t.Fatalf("persisted positions = %d, want 3", len(rows))
	}
	for i := 1; i < len(rows); i++ {
		if rows[i-1].Key().Compare(rows[i].Key()) >= 0 {
			t.Errorf("row %d %+v is not before row %d %+v", i-1, rows[i-1].Key(), i, rows[i].Key())
		}
	}
}

func TestRun_MergesKeysAcrossWindows(t *testing.T) {
	f := newFixture(t, func(d *Deps) {
		d.Config.InitialWindow = 500_000
		d.Config.MaxWindow = 500_000
	})
	window := 0
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		window++
		switch window {
		case 1:
			return []outbound.FilteredLog{modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0)}, nil
		case 2:
			// Same key again in a later window, plus a new one.
			return []outbound.FilteredLog{
				modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 22_300_000, 0),
				modifyLiquidityFilteredLog(t, poolAIDHash, ownerB, -100, 200, saltA, 22_300_001, 1),
			}, nil
		}
		return nil, nil
	}

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.Keys != 2 {
		t.Errorf("keys = %d, want 2: the repeated key must collapse across windows", summary.Keys)
	}
	if summary.ScanWindows < 2 {
		t.Errorf("scan windows = %d, want at least 2", summary.ScanWindows)
	}
}

func TestRun_ReportsTheScanCounters(t *testing.T) {
	f := newFixture(t, func(d *Deps) {
		d.Config.InitialWindow = 500_000
		d.Config.MaxWindow = 500_000
		d.Config.MinWindow = 1
	})
	refused := false
	f.client.GetLogsFn = func(filter outbound.LogFilter) ([]outbound.FilteredLog, error) {
		if !refused {
			refused = true
			return nil, fmt.Errorf("too wide: %w", outbound.ErrLogRangeTooLarge)
		}
		return []outbound.FilteredLog{
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0),
		}, nil
	}

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if summary.ScanNarrowings != 1 {
		t.Errorf("ScanNarrowings = %d, want 1", summary.ScanNarrowings)
	}
	if summary.ScanWindows != len(f.client.Filters)-summary.ScanNarrowings {
		t.Errorf("ScanWindows = %d, want one per accepted query (%d queries, %d refused)",
			summary.ScanWindows, len(f.client.Filters), summary.ScanNarrowings)
	}
	if summary.ScanLogs != summary.ScanWindows {
		t.Errorf("ScanLogs = %d, want %d: every accepted window answered one log", summary.ScanLogs, summary.ScanWindows)
	}
}

func TestRun_ReportsThePositionRowsTheWriterInserted(t *testing.T) {
	f := newFixture(t, nil)
	f.repo.SavePositionsFn = func([]*entity.UniswapV4Position) (int64, error) { return 0, nil }
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0),
			modifyLiquidityFilteredLog(t, poolBIDHash, ownerB, -60, 60, saltB, 22_990_000, 1),
		}, nil
	}

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if summary.PositionsRead != 2 {
		t.Errorf("PositionsRead = %d, want 2", summary.PositionsRead)
	}
	if summary.PositionsWritten != 0 {
		t.Errorf("PositionsWritten = %d, want 0: an already-covered rerun appends nothing", summary.PositionsWritten)
	}
}

func TestRun_BatchLogCountsPositionsWithinTheirOwnPool(t *testing.T) {
	logs := &capturedLogs{}
	f := newFixture(t, func(d *Deps) {
		d.Logger = slog.New(logs)
		d.Config.PositionBatch = 1
	})
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0),
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerB, -100, 200, saltA, 21_800_001, 1),
			modifyLiquidityFilteredLog(t, poolBIDHash, ownerB, -60, 60, saltB, 22_990_000, 2),
		}, nil
	}

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := logs.int64Field("persisted uniswap-v4 position batch", "poolPositionsDone")
	want := []int64{1, 2, 1}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("poolPositionsDone per batch = %v, want %v: the second pool's counter restarts at 1", got, want)
	}
}

func TestRun_ReportsPerPoolCounts(t *testing.T) {
	f := newFixture(t, nil)
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0),
			modifyLiquidityFilteredLog(t, poolBIDHash, ownerB, -60, 60, saltB, 22_990_000, 1),
		}, nil
	}

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.KeysByPool[7] != 1 || summary.KeysByPool[9] != 1 {
		t.Errorf("KeysByPool = %v, want one key for each of pools 7 and 9", summary.KeysByPool)
	}
}

func TestRun_ReadsEachPoolAgainstItsOwnStateView(t *testing.T) {
	f := newFixture(t, nil)
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{modifyLiquidityFilteredLog(t, poolBIDHash, ownerB, -60, 60, saltB, 22_990_000, 0)}, nil
	}

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for i, inv := range f.mc.Invocations {
		for j, call := range inv.Calls {
			if call.Target != common.HexToAddress(stateViewAddr) {
				t.Errorf("invocation %d call %d target = %s, want the StateView", i, j, call.Target)
			}
			if call.AllowFailure {
				t.Errorf("invocation %d call %d allows failure; an authoritative read must not", i, j)
			}
		}
	}
}

func TestRun_WritesEveryRowUnderTheSamePoolAndBlock(t *testing.T) {
	f := newFixture(t, nil)
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{
			modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0),
			modifyLiquidityFilteredLog(t, poolBIDHash, ownerB, -60, 60, saltB, 22_990_000, 1),
		}, nil
	}

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	byPool := map[int64]int{}
	for _, row := range f.repo.savedPositions() {
		byPool[row.PoolID]++
	}
	if byPool[7] != 1 || byPool[9] != 1 {
		t.Errorf("rows per pool = %v, want one each for 7 and 9", byPool)
	}
}

// TestRun_DecodedKeyMatchesTheLog pins that the persisted natural key is the
// log's tuple, not a re-derivation.
func TestRun_DecodedKeyMatchesTheLog(t *testing.T) {
	f := newFixture(t, nil)
	f.client.GetLogsFn = func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
		return []outbound.FilteredLog{modifyLiquidityFilteredLog(t, poolAIDHash, ownerA, -100, 200, saltA, 21_800_000, 0)}, nil
	}

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	rows := f.repo.savedPositions()
	if len(rows) != 1 {
		t.Fatalf("persisted positions = %d, want 1", len(rows))
	}
	want := entity.UniswapV4PositionKey{
		Owner:     common.HexToAddress(ownerA),
		TickLower: -100,
		TickUpper: 200,
		Salt:      common.HexToHash(saltA),
	}
	if rows[0].Key() != want {
		t.Errorf("key = %+v, want %+v", rows[0].Key(), want)
	}
	if rows[0].Liquidity.Cmp(big.NewInt(5000)) != 0 {
		t.Errorf("liquidity = %s, want 5000", rows[0].Liquidity)
	}
}
