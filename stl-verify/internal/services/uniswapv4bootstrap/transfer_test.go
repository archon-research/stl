package uniswapv4bootstrap

import (
	"context"
	"errors"
	"log/slog"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4indexer"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const (
	posmAddr           = "0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e"
	posmRowID          = int64(3)
	posmDeployBlock    = int64(21_689_089)
	transferHolderAddr = "0xe588dDd10E5Ca07c0Cf6a1F0e0e6b0e1d1b2C3d4"
	zeroAddress        = "0x0000000000000000000000000000000000000000"
)

func testPositionManager() uniswapv4indexer.RegisteredPositionManager {
	return uniswapv4indexer.RegisteredPositionManager{
		ID: posmRowID, Address: common.HexToAddress(posmAddr), DeployBlock: posmDeployBlock,
	}
}

func transferFilteredLog(tokenID, blockNumber int64, logIndex int, from, to string) outbound.FilteredLog {
	return outbound.FilteredLog{
		Address: posmAddr,
		Topics: []string{
			abis.TransferTopic0().Hex(),
			common.BytesToHash(common.HexToAddress(from).Bytes()).Hex(),
			common.BytesToHash(common.HexToAddress(to).Bytes()).Hex(),
			common.BigToHash(big.NewInt(tokenID)).Hex(),
		},
		Data:             "0x",
		BlockHash:        pinHash,
		BlockNumber:      "0x" + strconv.FormatInt(blockNumber, 16),
		BlockTimestamp:   pinTimestampHex,
		TransactionHash:  txHashA,
		TransactionIndex: "0x0",
		LogIndex:         "0x" + strconv.FormatInt(int64(logIndex), 16),
	}
}

type fakeNFTTransferRepo struct {
	mu           sync.Mutex
	SaveFn       func([]*entity.UniswapV4PositionNFTTransfer) (int64, error)
	SavedBatches [][]*entity.UniswapV4PositionNFTTransfer
}

func (f *fakeNFTTransferRepo) SaveNFTTransfersIfAbsent(_ context.Context, _ pgx.Tx, transfers []*entity.UniswapV4PositionNFTTransfer) (int64, error) {
	f.mu.Lock()
	f.SavedBatches = append(f.SavedBatches, transfers)
	f.mu.Unlock()
	if f.SaveFn != nil {
		return f.SaveFn(transfers)
	}
	return int64(len(transfers)), nil
}

func (f *fakeNFTTransferRepo) saved() []*entity.UniswapV4PositionNFTTransfer {
	var all []*entity.UniswapV4PositionNFTTransfer
	for _, batch := range f.SavedBatches {
		all = append(all, batch...)
	}
	return all
}

type fakeTransferProgressStore struct {
	mu       sync.Mutex
	Recorded TransferProgress
	Found    bool
	Saves    []TransferProgress
	LoadErr  error
}

func (f *fakeTransferProgressStore) SaveProgress(_ context.Context, progress TransferProgress) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Saves = append(f.Saves, progress)
	f.Recorded, f.Found = progress, true
	return nil
}

func (f *fakeTransferProgressStore) LoadProgress(_ context.Context) (TransferProgress, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.LoadErr != nil {
		return TransferProgress{}, false, f.LoadErr
	}
	return f.Recorded, f.Found, nil
}

type recordedNFTTransferRows struct {
	mu        sync.Mutex
	Attempted int
	Written   int
	Calls     int
}

func (r *recordedNFTTransferRows) RecordNFTTransferRows(_ context.Context, attempted, written int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Attempted += attempted
	r.Written += written
	r.Calls++
}

type transferFixture struct {
	svc       *TransferService
	client    *fakeLogScanClient
	repo      *fakeNFTTransferRepo
	progress  *fakeTransferProgressStore
	telemetry *recordedNFTTransferRows
}

func newTransferFixture(t *testing.T, mutate func(*TransferDeps)) *transferFixture {
	t.Helper()

	client := newFakeLogScanClient(testHead, map[int64]*outbound.BlockHeader{
		testPinned: header(testPinned, pinHash),
	})
	repo := &fakeNFTTransferRepo{}
	progress := &fakeTransferProgressStore{}
	telemetry := &recordedNFTTransferRows{}

	deps := TransferDeps{
		PositionManager: testPositionManager(),
		LogScan:         client,
		Repo:            repo,
		TxManager:       &testutil.MockTxManager{},
		Progress:        progress,
		Telemetry:       telemetry,
		Logger:          testLogger(),
		Config: Config{
			ChainID:       testChainID,
			InitialWindow: testMaxRange,
			MaxWindow:     testMaxRange,
		},
	}
	if mutate != nil {
		mutate(&deps)
	}

	svc, err := NewTransferService(deps)
	if err != nil {
		t.Fatalf("NewTransferService: %v", err)
	}
	return &transferFixture{svc: svc, client: client, repo: repo, progress: progress, telemetry: telemetry}
}

// logsAt serves each configured block's logs to whichever window covers it, so a
// test states log positions and the scanner's windowing stays incidental.
func logsAt(logs ...outbound.FilteredLog) func(outbound.LogFilter) ([]outbound.FilteredLog, error) {
	return func(filter outbound.LogFilter) ([]outbound.FilteredLog, error) {
		var window []outbound.FilteredLog
		for _, l := range logs {
			blockNumber, err := strconv.ParseInt(strings.TrimPrefix(l.BlockNumber, "0x"), 16, 64)
			if err != nil {
				return nil, err
			}
			if blockNumber >= filter.FromBlock && blockNumber <= filter.ToBlock {
				window = append(window, l)
			}
		}
		return window, nil
	}
}

func TestTransferRun_ScansFromTheDeployBlockToThePin(t *testing.T) {
	f := newTransferFixture(t, nil)
	f.client.GetLogsFn = logsAt()

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.FromBlock != posmDeployBlock {
		t.Errorf("FromBlock = %d, want the posm deploy block %d", summary.FromBlock, posmDeployBlock)
	}
	if summary.PinnedBlock != testPinned {
		t.Errorf("PinnedBlock = %d, want %d", summary.PinnedBlock, testPinned)
	}
	if len(f.client.Filters) == 0 {
		t.Fatal("no eth_getLogs call was made")
	}
	first, last := f.client.Filters[0], f.client.Filters[len(f.client.Filters)-1]
	if first.FromBlock != posmDeployBlock {
		t.Errorf("first window starts at %d, want %d", first.FromBlock, posmDeployBlock)
	}
	if last.ToBlock != testPinned {
		t.Errorf("last window ends at %d, want the pin %d: the live indexer owns everything above", last.ToBlock, testPinned)
	}
}

// Topic1 narrowing would restrict the scan to whichever tokens were named, and
// the question is every token's whole history.
func TestTransferRun_FiltersOnTheAddressAndTopic0Only(t *testing.T) {
	f := newTransferFixture(t, nil)
	f.client.GetLogsFn = logsAt()

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	for i, filter := range f.client.Filters {
		if filter.Address != common.HexToAddress(posmAddr) {
			t.Errorf("filter %d address = %s, want the posm %s", i, filter.Address, posmAddr)
		}
		if filter.Topic0 != abis.TransferTopic0() {
			t.Errorf("filter %d topic0 = %s, want the ERC-721 Transfer topic0", i, filter.Topic0)
		}
		if len(filter.Topic1) != 0 {
			t.Errorf("filter %d narrows topic1 to %v, which would scan only those tokens", i, filter.Topic1)
		}
	}
}

func TestTransferRun_PersistsEveryDecodedTransfer(t *testing.T) {
	f := newTransferFixture(t, nil)
	f.client.GetLogsFn = logsAt(
		transferFilteredLog(388720, posmDeployBlock+10, 2, zeroAddress, transferHolderAddr),
		transferFilteredLog(388721, posmDeployBlock+11, 5, transferHolderAddr, ownerA),
	)

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.TransfersDecoded != 2 {
		t.Errorf("TransfersDecoded = %d, want 2", summary.TransfersDecoded)
	}
	if summary.TransfersWritten != 2 {
		t.Errorf("TransfersWritten = %d, want 2", summary.TransfersWritten)
	}
	saved := f.repo.saved()
	if len(saved) != 2 {
		t.Fatalf("persisted %d rows, want 2", len(saved))
	}
	for _, row := range saved {
		if row.PositionManagerID != posmRowID {
			t.Errorf("row token %s has PositionManagerID %d, want %d", row.TokenID, row.PositionManagerID, posmRowID)
		}
		if row.BlockVersion != 0 {
			t.Errorf("row token %s has BlockVersion %d, want 0", row.TokenID, row.BlockVersion)
		}
	}
}

// The growth tripwire reads this counter as the table's growth; a backfill that
// wrote without it would grow the table while the rule stayed flat.
func TestTransferRun_RecordsTheRowsItQueuedAndLanded(t *testing.T) {
	f := newTransferFixture(t, nil)
	f.repo.SaveFn = func(transfers []*entity.UniswapV4PositionNFTTransfer) (int64, error) {
		return int64(len(transfers)) - 1, nil // one site already held a row
	}
	f.client.GetLogsFn = logsAt(
		transferFilteredLog(388720, posmDeployBlock+10, 2, zeroAddress, transferHolderAddr),
		transferFilteredLog(388721, posmDeployBlock+11, 5, transferHolderAddr, ownerA),
	)

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if f.telemetry.Attempted != 2 {
		t.Errorf("recorded %d attempted, want 2", f.telemetry.Attempted)
	}
	if f.telemetry.Written != 1 {
		t.Errorf("recorded %d written, want 1: the counter must report landed rows, not queued ones", f.telemetry.Written)
	}
}

func TestTransferRun_SplitsPersistenceIntoTransferBatchSizedTransactions(t *testing.T) {
	logs := make([]outbound.FilteredLog, 5)
	for i := range logs {
		logs[i] = transferFilteredLog(int64(1000+i), posmDeployBlock+int64(i), i, zeroAddress, transferHolderAddr)
	}
	f := newTransferFixture(t, func(d *TransferDeps) { d.Config.TransferBatch = 2 })
	f.client.GetLogsFn = logsAt(logs...)

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.Batches != 3 {
		t.Errorf("Batches = %d, want 3 (5 rows at 2 per transaction)", summary.Batches)
	}
	if len(f.repo.SavedBatches) != 3 {
		t.Errorf("persisted in %d transactions, want 3", len(f.repo.SavedBatches))
	}
}

// The resume point is a whole window, so a killed attempt redoes at most one.
func TestTransferRun_RecordsTheResumePointPastEachFinishedWindow(t *testing.T) {
	f := newTransferFixture(t, nil)
	f.client.GetLogsFn = logsAt()

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(f.progress.Saves) == 0 {
		t.Fatal("no progress was recorded")
	}
	last := f.progress.Saves[len(f.progress.Saves)-1]
	if last.NextBlock != testPinned+1 {
		t.Errorf("final NextBlock = %d, want %d (one past the pin)", last.NextBlock, testPinned+1)
	}
	if last.ChainID != testChainID || last.PositionManagerID != posmRowID ||
		last.PinnedBlock != testPinned || last.PinnedHash != pinHash {
		t.Errorf("progress %+v does not carry the scope it is true for", last)
	}
	for i, save := range f.progress.Saves {
		if save.NextBlock != f.client.Filters[i].ToBlock+1 {
			t.Errorf("progress %d records NextBlock %d for window ending %d", i, save.NextBlock, f.client.Filters[i].ToBlock)
		}
	}
}

func TestTransferRun_ResumesFromTheRecordedNextBlock(t *testing.T) {
	resumeAt := posmDeployBlock + 1_000_000
	f := newTransferFixture(t, nil)
	f.progress.Recorded = TransferProgress{
		ChainID: testChainID, PositionManagerID: posmRowID,
		PinnedBlock: testPinned, PinnedHash: pinHash, NextBlock: resumeAt,
	}
	f.progress.Found = true
	f.client.GetLogsFn = logsAt()

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.FromBlock != resumeAt {
		t.Errorf("FromBlock = %d, want the recorded resume point %d", summary.FromBlock, resumeAt)
	}
	if f.client.Filters[0].FromBlock != resumeAt {
		t.Errorf("first window starts at %d, want %d", f.client.Filters[0].FromBlock, resumeAt)
	}
	if f.client.HeadCalls != 0 {
		t.Errorf("read the chain head %d times while resuming: a resumed attempt must keep the recorded pin, not derive a fresh one", f.client.HeadCalls)
	}
}

func TestTransferRun_RefusesToResumeAPinThatWasReorged(t *testing.T) {
	f := newTransferFixture(t, nil)
	f.progress.Recorded = TransferProgress{
		ChainID: testChainID, PositionManagerID: posmRowID,
		PinnedBlock: testPinned, PinnedHash: forkHash, NextBlock: posmDeployBlock + 10,
	}
	f.progress.Found = true

	_, err := f.svc.Run(context.Background())
	if !errors.Is(err, ErrPinMoved) {
		t.Fatalf("Run error = %v, want ErrPinMoved", err)
	}
}

func TestTransferRun_PinsAfreshWhenTheRecordBelongsToAnotherChain(t *testing.T) {
	f := newTransferFixture(t, nil)
	f.progress.Recorded = TransferProgress{
		ChainID: 8453, PositionManagerID: posmRowID,
		PinnedBlock: 99, PinnedHash: forkHash, NextBlock: 500,
	}
	f.progress.Found = true
	f.client.GetLogsFn = logsAt()

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.FromBlock != posmDeployBlock {
		t.Errorf("FromBlock = %d, want the deploy block %d: another chain's record must not move this scan", summary.FromBlock, posmDeployBlock)
	}
}

func TestTransferRun_StopsWhenTheScanStartIsAboveThePin(t *testing.T) {
	f := newTransferFixture(t, func(d *TransferDeps) { d.PositionManager.DeployBlock = testHead + 1_000 })

	_, err := f.svc.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "nothing to scan") {
		t.Fatalf("Run error = %v, want it to report nothing to scan", err)
	}
}

func TestTransferRun_StopsOnAWriteFailureRatherThanLeavingAHole(t *testing.T) {
	f := newTransferFixture(t, nil)
	f.repo.SaveFn = func([]*entity.UniswapV4PositionNFTTransfer) (int64, error) {
		return 0, errors.New("deadlock detected")
	}
	f.client.GetLogsFn = logsAt(transferFilteredLog(388720, posmDeployBlock+10, 2, zeroAddress, transferHolderAddr))

	_, err := f.svc.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "deadlock detected") {
		t.Fatalf("Run error = %v, want the write failure to stop the run", err)
	}
}

func TestTransferRun_ReportsAPinThatMovedUnderTheScan(t *testing.T) {
	f := newTransferFixture(t, nil)
	f.client.GetLogsFn = func(filter outbound.LogFilter) ([]outbound.FilteredLog, error) {
		if filter.ToBlock == testPinned {
			f.client.HeaderByNumber[testPinned] = header(testPinned, forkHash)
		}
		return nil, nil
	}

	_, err := f.svc.Run(context.Background())
	if !errors.Is(err, ErrPinMoved) {
		t.Fatalf("Run error = %v, want ErrPinMoved once the pinned height named another hash", err)
	}
}

func TestNewTransferService_RefusesIncompleteDeps(t *testing.T) {
	tests := []struct {
		name    string
		mut     func(*TransferDeps)
		wantErr string
	}{
		{"no position manager address", func(d *TransferDeps) { d.PositionManager.Address = common.Address{} }, "non-zero address"},
		{"no position manager row id", func(d *TransferDeps) { d.PositionManager.ID = 0 }, "positive row id"},
		{"no deploy block", func(d *TransferDeps) { d.PositionManager.DeployBlock = 0 }, "the scan would start at genesis"},
		{"no log scan client", func(d *TransferDeps) { d.LogScan = nil }, "log scan client"},
		{"no repo", func(d *TransferDeps) { d.Repo = nil }, "repo"},
		{"no tx manager", func(d *TransferDeps) { d.TxManager = nil }, "txManager"},
		{"no progress store", func(d *TransferDeps) { d.Progress = nil }, "progress store"},
		{"no telemetry", func(d *TransferDeps) { d.Telemetry = nil }, "telemetry"},
		{"no logger", func(d *TransferDeps) { d.Logger = nil }, "logger"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deps := TransferDeps{
				PositionManager: testPositionManager(),
				LogScan:         newFakeLogScanClient(testHead, nil),
				Repo:            &fakeNFTTransferRepo{},
				TxManager:       &testutil.MockTxManager{},
				Progress:        &fakeTransferProgressStore{},
				Telemetry:       &recordedNFTTransferRows{},
				Logger:          testLogger(),
				Config:          Config{ChainID: testChainID},
			}
			tc.mut(&deps)
			_, err := NewTransferService(deps)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("NewTransferService error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

// A correcting uniswap_v4_position_manager version gives the chain a new surrogate
// id. Honouring a cursor written under the old one would leave every transfer
// below it unwritten under the new id, and SaveNFTTransfersIfAbsent keys on that
// id, so no rerun of this execution would look there again.
func TestTransferRun_IgnoresACursorWrittenUnderAnotherPositionManager(t *testing.T) {
	f := newTransferFixture(t, nil)
	f.progress.Recorded = TransferProgress{
		ChainID: testChainID, PositionManagerID: posmRowID + 1,
		PinnedBlock: testPinned, PinnedHash: pinHash, NextBlock: posmDeployBlock + 2_000_000,
	}
	f.progress.Found = true
	f.client.GetLogsFn = logsAt()

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary.FromBlock != posmDeployBlock {
		t.Errorf("FromBlock = %d, want the deploy block %d: a cursor from another PositionManager must not move this scan",
			summary.FromBlock, posmDeployBlock)
	}
}

// A cursor one past the pin means an earlier attempt finished the scan and only
// its closing pin check failed. Failing every remaining attempt would report a
// run that wrote every row as red, and send the operator to rescan from scratch.
func TestTransferRun_TreatsACursorPastThePinAsAFinishedScan(t *testing.T) {
	f := newTransferFixture(t, nil)
	f.progress.Recorded = TransferProgress{
		ChainID: testChainID, PositionManagerID: posmRowID,
		PinnedBlock: testPinned, PinnedHash: pinHash, NextBlock: testPinned + 1,
	}
	f.progress.Found = true

	summary, err := f.svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(f.client.Filters) != 0 {
		t.Errorf("scanned %d windows, want none: the scan was already complete", len(f.client.Filters))
	}
	if summary.FromBlock != testPinned+1 {
		t.Errorf("FromBlock = %d, want %d", summary.FromBlock, testPinned+1)
	}
}

// capturingHandler records the records a run logs, so the empty-history warning —
// which is the only signal a wrong PositionManager address produces — can be
// asserted rather than assumed.
type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) warned(substring string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, substring) {
			return true
		}
	}
	return false
}

const emptyHistoryWarning = "decoded no transfers at all"

// A scan of the WHOLE history that finds nothing is what a wrong PositionManager
// address looks like, so it must say so.
func TestTransferRun_WarnsWhenAWholeHistoryScanDecodesNothing(t *testing.T) {
	logs := &capturingHandler{}
	f := newTransferFixture(t, func(d *TransferDeps) { d.Logger = slog.New(logs) })
	f.client.GetLogsFn = logsAt()

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !logs.warned(emptyHistoryWarning) {
		t.Error("a full-history scan that decoded nothing logged no warning; a wrong PositionManager address would pass unnoticed")
	}
}

// A resumed attempt covers only a tail, and a quiet tail says nothing about the
// address — blaming the registry there sends the operator after a bug that is not
// there, on a run that just backfilled the whole chain.
func TestTransferRun_DoesNotWarnWhenAResumedAttemptScansAQuietTail(t *testing.T) {
	logs := &capturingHandler{}
	f := newTransferFixture(t, func(d *TransferDeps) { d.Logger = slog.New(logs) })
	f.progress.Recorded = TransferProgress{
		ChainID: testChainID, PositionManagerID: posmRowID,
		PinnedBlock: testPinned, PinnedHash: pinHash, NextBlock: posmDeployBlock + 1_000_000,
	}
	f.progress.Found = true
	f.client.GetLogsFn = logsAt()

	if _, err := f.svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if logs.warned(emptyHistoryWarning) {
		t.Error("a resumed attempt over a quiet tail blamed the PositionManager address")
	}
}
