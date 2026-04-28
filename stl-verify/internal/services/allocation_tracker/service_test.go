package allocation_tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
)

// ── Mocks ──

type testHandler struct {
	batches []*SnapshotBatch
	err     error
}

func (m *testHandler) HandleBatch(ctx context.Context, batch *SnapshotBatch) error {
	m.batches = append(m.batches, batch)
	return m.err
}

// ── NewService ──

func TestNewService_FillsDefaults(t *testing.T) {
	handler := &testHandler{}
	registry := NewSourceRegistry(ConfigDefaults().Logger)

	svc, err := NewService(
		Config{ChainID: 1},
		nil, nil, registry, nil, handler, nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc.config.MaxMessages != ConfigDefaults().MaxMessages {
		t.Errorf("MaxMessages not filled: got %d", svc.config.MaxMessages)
	}
	if svc.config.SweepEveryNBlocks != ConfigDefaults().SweepEveryNBlocks {
		t.Errorf("SweepEveryNBlocks not filled: got %d", svc.config.SweepEveryNBlocks)
	}
	if len(svc.entries) == 0 {
		t.Error("entries should default to DefaultTokenEntries for chain")
	}
}

func TestNewService_RequiresChainID(t *testing.T) {
	handler := &testHandler{}
	registry := NewSourceRegistry(ConfigDefaults().Logger)

	_, err := NewService(
		Config{},
		nil, nil, registry, nil, handler, nil,
	)
	if err == nil {
		t.Fatal("expected error when ChainID is 0")
	}
}

// ── processBlock ──

func TestProcessBlock_CacheMiss_ReturnsError(t *testing.T) {
	cache := testutil.NewMockBlockCache()
	svc := &Service{
		cache:  cache,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	// Don't store anything in cache — GetReceipts returns nil, nil.
	err := svc.processBlock(context.Background(), outbound.BlockEvent{
		ChainID: 1, BlockNumber: 99999, Version: 0,
	})
	if err == nil {
		t.Fatal("expected error for cache miss, got nil")
	}
}

func TestProcessBlock_PartialFetchFailure_ReturnsErrorAndDoesNotPersist(t *testing.T) {
	cache := testutil.NewMockBlockCache()

	proxy := common.HexToAddress("0xbbbb")
	contract1 := common.HexToAddress("0x1111")
	contract2 := common.HexToAddress("0x2222")

	receiptsJSON := mustMarshalReceipts(t, []TransactionReceipt{{
		Logs: []gethtypes.Log{
			makeTransferLog(contract1, common.HexToAddress("0xaaaa"), proxy, big.NewInt(1), 0),
			makeTransferLog(contract2, common.HexToAddress("0xcccc"), proxy, big.NewInt(2), 1),
		},
	}})
	cache.SetReceipts(1, 100, 0, receiptsJSON)

	handler := &testHandler{}
	registry := NewSourceRegistry(slog.New(slog.NewTextHandler(io.Discard, nil)))
	registry.Register(&mockSource{
		name:       "erc20",
		tokenTypes: map[string]bool{"erc20": true},
		result: func() *FetchResult {
			res := NewFetchResult()
			res.Balances[EntryKey{ContractAddress: contract1, WalletAddress: proxy}] = &PositionBalance{Balance: big.NewInt(100)}
			return res
		}(),
	})
	registry.Register(&mockSource{
		name:       "erc4626",
		tokenTypes: map[string]bool{"erc4626": true},
		err:        fmt.Errorf("rpc timeout"),
	})

	svc := &Service{
		cache:     cache,
		extractor: NewTransferExtractor([]ProxyConfig{{Address: proxy, Star: "spark", Chain: "mainnet"}}),
		registry:  registry,
		handler:   handler,
		entryLookup: BuildEntryLookup([]*TokenEntry{
			{ContractAddress: contract1, WalletAddress: proxy, TokenType: "erc20"},
			{ContractAddress: contract2, WalletAddress: proxy, TokenType: "erc4626"},
		}),
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	err := svc.processBlock(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 100, Version: 0, BlockTimestamp: 1700000000})
	if err == nil {
		t.Fatal("expected partial fetch failure to be returned")
	}
	if len(handler.batches) != 0 {
		t.Fatalf("HandleBatch should not be called on partial fetch failure, got %d calls", len(handler.batches))
	}
}

func TestProcessBlock_SweepFetchFailure_ReturnsError(t *testing.T) {
	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(1, 200, 0, mustMarshalReceipts(t, []TransactionReceipt{}))

	entries := []*TokenEntry{{
		ContractAddress: common.HexToAddress("0x1111"),
		WalletAddress:   common.HexToAddress("0xbbbb"),
		TokenType:       "erc20",
	}}
	registry := NewSourceRegistry(slog.New(slog.NewTextHandler(io.Discard, nil)))
	registry.Register(&mockSource{
		name:       "erc20",
		tokenTypes: map[string]bool{"erc20": true},
		err:        fmt.Errorf("alchemy rate limit"),
	})

	svc := &Service{
		cache:            cache,
		extractor:        NewTransferExtractor(nil),
		registry:         registry,
		entries:          entries,
		handler:          &testHandler{},
		logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		config:           Config{ChainID: 1, SweepEveryNBlocks: 1},
		blocksSinceSweep: 0,
	}

	err := svc.processBlock(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 200, Version: 0, BlockTimestamp: 1700000000})
	if err == nil {
		t.Fatal("expected sweep fetch failure to be returned")
	}
}

func TestProcessBlock_FailedSweepDoesNotResetCounter(t *testing.T) {
	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(1, 300, 0, mustMarshalReceipts(t, []TransactionReceipt{}))

	entries := []*TokenEntry{{
		ContractAddress: common.HexToAddress("0x1111"),
		WalletAddress:   common.HexToAddress("0xbbbb"),
		TokenType:       "erc20",
	}}
	badSource := &mockSource{
		name:       "erc20",
		tokenTypes: map[string]bool{"erc20": true},
		err:        fmt.Errorf("temporary rpc error"),
	}
	registry := NewSourceRegistry(slog.New(slog.NewTextHandler(io.Discard, nil)))
	registry.Register(badSource)

	svc := &Service{
		cache:            cache,
		extractor:        NewTransferExtractor(nil),
		registry:         registry,
		entries:          entries,
		handler:          &testHandler{},
		logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		config:           Config{ChainID: 1, SweepEveryNBlocks: 1},
		blocksSinceSweep: 0,
	}

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 300, Version: 0, BlockTimestamp: 1700000000}
	if err := svc.processBlock(context.Background(), event); err == nil {
		t.Fatal("expected first sweep attempt to fail")
	}
	if badSource.called != 1 {
		t.Fatalf("expected first call to attempt sweep once, got %d", badSource.called)
	}
	if svc.blocksSinceSweep != 1 {
		t.Fatalf("blocksSinceSweep after failed sweep = %d, want 1", svc.blocksSinceSweep)
	}

	if err := svc.processBlock(context.Background(), event); err == nil {
		t.Fatal("expected second sweep attempt to fail")
	}
	if badSource.called != 2 {
		t.Fatalf("expected retry to attempt sweep again, got %d calls", badSource.called)
	}
}

func TestProcessBlock_SweepHandlerFailure_ReturnsError(t *testing.T) {
	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(1, 400, 0, mustMarshalReceipts(t, []TransactionReceipt{}))

	entry := &TokenEntry{
		ContractAddress: common.HexToAddress("0x1111"),
		WalletAddress:   common.HexToAddress("0xbbbb"),
		TokenType:       "erc20",
	}
	result := NewFetchResult()
	result.Balances[entry.Key()] = &PositionBalance{Balance: big.NewInt(123)}

	registry := NewSourceRegistry(slog.New(slog.NewTextHandler(io.Discard, nil)))
	registry.Register(&mockSource{
		name:       "erc20",
		tokenTypes: map[string]bool{"erc20": true},
		result:     result,
	})

	handler := &testHandler{err: fmt.Errorf("db unavailable")}
	svc := &Service{
		cache:            cache,
		extractor:        NewTransferExtractor(nil),
		registry:         registry,
		entries:          []*TokenEntry{entry},
		handler:          handler,
		logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		config:           Config{ChainID: 1, SweepEveryNBlocks: 1},
		blocksSinceSweep: 0,
	}

	err := svc.processBlock(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 400, Version: 0, BlockTimestamp: 1700000000})
	if err == nil {
		t.Fatal("expected sweep handler failure to be returned")
	}
}

// ── matchTransfers ──

func TestMatchTransfers_MatchesKnownEntry(t *testing.T) {
	contract := common.HexToAddress("0xaaaa")
	wallet := common.HexToAddress("0xbbbb")

	entries := []*TokenEntry{
		{ContractAddress: contract, WalletAddress: wallet, Star: "spark", Chain: "mainnet", TokenType: "erc20"},
	}
	svc := &Service{
		entryLookup: BuildEntryLookup(entries),
	}

	transfers := []*TransferEvent{
		{TokenAddress: contract, ProxyAddress: wallet, Direction: DirectionIn},
	}

	matched := svc.matchTransfers(transfers)
	if len(matched) != 1 {
		t.Fatalf("expected 1 match, got %d", len(matched))
	}
	if matched[0].ContractAddress != contract {
		t.Error("matched wrong entry")
	}
}

func TestMatchTransfers_NoMatch(t *testing.T) {
	entries := []*TokenEntry{
		{
			ContractAddress: common.HexToAddress("0xaaaa"),
			WalletAddress:   common.HexToAddress("0xbbbb"),
			TokenType:       "erc20",
		},
	}
	svc := &Service{
		entryLookup: BuildEntryLookup(entries),
	}

	transfers := []*TransferEvent{
		{TokenAddress: common.HexToAddress("0xcccc"), ProxyAddress: common.HexToAddress("0xdddd")},
	}

	matched := svc.matchTransfers(transfers)
	if len(matched) != 0 {
		t.Errorf("expected 0 matches, got %d", len(matched))
	}
}

func TestMatchTransfers_Deduplicates(t *testing.T) {
	contract := common.HexToAddress("0xaaaa")
	wallet := common.HexToAddress("0xbbbb")

	entries := []*TokenEntry{
		{ContractAddress: contract, WalletAddress: wallet, TokenType: "erc20"},
	}
	svc := &Service{
		entryLookup: BuildEntryLookup(entries),
	}

	transfers := []*TransferEvent{
		{TokenAddress: contract, ProxyAddress: wallet, Direction: DirectionIn, TxHash: "0x1"},
		{TokenAddress: contract, ProxyAddress: wallet, Direction: DirectionOut, TxHash: "0x2"},
	}

	matched := svc.matchTransfers(transfers)
	if len(matched) != 1 {
		t.Errorf("duplicate transfers should produce 1 match, got %d", len(matched))
	}
}

func TestMatchTransfers_SameContractDifferentWallets(t *testing.T) {
	contract := common.HexToAddress("0xaaaa")
	wallet1 := common.HexToAddress("0xbbbb")
	wallet2 := common.HexToAddress("0xcccc")

	entries := []*TokenEntry{
		{ContractAddress: contract, WalletAddress: wallet1, Star: "spark", TokenType: "erc20"},
		{ContractAddress: contract, WalletAddress: wallet2, Star: "grove", TokenType: "erc20"},
	}
	svc := &Service{
		entryLookup: BuildEntryLookup(entries),
	}

	transfers := []*TransferEvent{
		{TokenAddress: contract, ProxyAddress: wallet1, Direction: DirectionIn},
		{TokenAddress: contract, ProxyAddress: wallet2, Direction: DirectionIn},
	}

	matched := svc.matchTransfers(transfers)
	if len(matched) != 2 {
		t.Fatalf("same contract with different wallets should match 2 entries, got %d", len(matched))
	}
}

// ── buildSnapshots ──

func TestBuildSnapshots_Basic(t *testing.T) {
	contract := common.HexToAddress("0xaaaa")
	wallet := common.HexToAddress("0xbbbb")

	entry := &TokenEntry{ContractAddress: contract, WalletAddress: wallet, Star: "spark", Chain: "mainnet"}

	balances := map[EntryKey]*PositionBalance{
		entry.Key(): {Balance: big.NewInt(1000000), ScaledBalance: big.NewInt(2000000)},
	}

	transfers := []*TransferEvent{
		{TokenAddress: contract, ProxyAddress: wallet, Amount: big.NewInt(500), Direction: DirectionIn, TxHash: "0xabc", LogIndex: 3},
	}

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100, Version: 0}

	svc := &Service{}
	snapshots := svc.buildSnapshots([]*TokenEntry{entry}, balances, transfers, event, time.Unix(1700000000, 0).UTC())

	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}

	s := snapshots[0]
	if s.Balance.Cmp(big.NewInt(1000000)) != 0 {
		t.Errorf("expected balance 1000000, got %s", s.Balance.String())
	}
	if s.ScaledBalance.Cmp(big.NewInt(2000000)) != 0 {
		t.Errorf("expected scaledBalance 2000000, got %s", s.ScaledBalance.String())
	}
	if s.ChainID != 1 {
		t.Errorf("expected chainID 1, got %d", s.ChainID)
	}
	if s.BlockNumber != 100 {
		t.Errorf("expected blockNumber 100, got %d", s.BlockNumber)
	}
	if s.TxHash != "0xabc" {
		t.Errorf("expected txHash 0xabc, got %s", s.TxHash)
	}
	if s.LogIndex != 3 {
		t.Errorf("expected logIndex 3, got %d", s.LogIndex)
	}
	if s.Direction != DirectionIn {
		t.Errorf("expected direction IN, got %s", s.Direction)
	}
}

func TestBuildSnapshots_SkipsMissingBalance(t *testing.T) {
	entry := &TokenEntry{
		ContractAddress: common.HexToAddress("0xaaaa"),
		WalletAddress:   common.HexToAddress("0xbbbb"),
	}

	balances := map[EntryKey]*PositionBalance{}
	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100}

	svc := &Service{}
	snapshots := svc.buildSnapshots([]*TokenEntry{entry}, balances, nil, event, time.Unix(1700000000, 0).UTC())
	if len(snapshots) != 0 {
		t.Errorf("expected 0 snapshots when balance missing, got %d", len(snapshots))
	}
}

func TestBuildSnapshots_NoTransferContext(t *testing.T) {
	contract := common.HexToAddress("0xaaaa")
	wallet := common.HexToAddress("0xbbbb")
	entry := &TokenEntry{ContractAddress: contract, WalletAddress: wallet}

	balances := map[EntryKey]*PositionBalance{
		entry.Key(): {Balance: big.NewInt(100), ScaledBalance: nil},
	}

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 50}
	svc := &Service{}
	snapshots := svc.buildSnapshots([]*TokenEntry{entry}, balances, nil, event, time.Unix(1700000000, 0).UTC())

	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}
	if snapshots[0].TxHash != "" {
		t.Errorf("expected empty txHash for sweep-style snapshot, got %s", snapshots[0].TxHash)
	}
}

func mustMarshalReceipts(t *testing.T, receipts []TransactionReceipt) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(receipts)
	if err != nil {
		t.Fatalf("marshal receipts: %v", err)
	}
	return data
}
