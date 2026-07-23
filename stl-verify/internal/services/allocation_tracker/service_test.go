package allocation_tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/ethereum/go-ethereum/common"
	gethtypes "github.com/ethereum/go-ethereum/core/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

func TestNewService_FillsConfigDefaults(t *testing.T) {
	handler := &testHandler{}
	registry := NewSourceRegistry(ConfigDefaults().Logger)
	entries := []*TokenEntry{{
		ContractAddress: common.HexToAddress("0x1111"),
		WalletAddress:   common.HexToAddress("0xaaaa"),
		Star:            "spark",
		Chain:           "mainnet",
		TokenType:       "erc20",
	}}
	proxies := []ProxyConfig{{
		Star:    "spark",
		Chain:   "mainnet",
		Address: common.HexToAddress("0xaaaa"),
	}}

	svc, err := NewService(
		Config{ChainID: 1},
		nil, nil, registry, entries, handler, proxies,
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
	if len(svc.entries) != 1 {
		t.Fatalf("entries length = %d, want 1", len(svc.entries))
	}
}

func TestNewService_RequiresEntriesAndProxies(t *testing.T) {
	handler := &testHandler{}
	registry := NewSourceRegistry(ConfigDefaults().Logger)
	entry := []*TokenEntry{{
		ContractAddress: common.HexToAddress("0x1111"),
		WalletAddress:   common.HexToAddress("0xaaaa"),
		Star:            "spark",
		Chain:           "mainnet",
		TokenType:       "erc20",
	}}
	proxy := []ProxyConfig{{
		Star:    "spark",
		Chain:   "mainnet",
		Address: common.HexToAddress("0xaaaa"),
	}}

	tests := []struct {
		name    string
		entries []*TokenEntry
		proxies []ProxyConfig
		wantErr string
	}{
		{
			name:    "missing entries",
			proxies: proxy,
			wantErr: "at least one token entry is required",
		},
		{
			name:    "missing proxies",
			entries: entry,
			wantErr: "at least one proxy is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewService(
				Config{ChainID: 1},
				nil, nil, registry, tt.entries, handler, tt.proxies,
			)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestNewService_RejectsChainScopedInputMismatch(t *testing.T) {
	handler := &testHandler{}
	registry := NewSourceRegistry(ConfigDefaults().Logger)

	_, err := NewService(
		Config{ChainID: 1},
		nil,
		nil,
		registry,
		[]*TokenEntry{{
			ContractAddress: common.HexToAddress("0x1111"),
			WalletAddress:   common.HexToAddress("0xaaaa"),
			Star:            "spark",
			Chain:           "base",
			TokenType:       "erc20",
		}},
		handler,
		[]ProxyConfig{{
			Star:    "spark",
			Chain:   "mainnet",
			Address: common.HexToAddress("0xaaaa"),
		}},
	)
	if err == nil || !strings.Contains(err.Error(), "want mainnet") {
		t.Fatalf("error = %v, want chain mismatch", err)
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

	err := svc.processBlock(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 100, Version: 0, BlockTimestamp: 1700000000, BlockHash: testBlockHash.Hex()})
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

	err := svc.processBlock(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 200, Version: 0, BlockTimestamp: 1700000000, BlockHash: testBlockHash.Hex()})
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

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 300, Version: 0, BlockTimestamp: 1700000000, BlockHash: testBlockHash.Hex()}
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

	err := svc.processBlock(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 400, Version: 0, BlockTimestamp: 1700000000, BlockHash: testBlockHash.Hex()})
	if err == nil {
		t.Fatal("expected sweep handler failure to be returned")
	}
}

// TestProcessBlock_MissingBlockHash_ReturnsError: an event with an empty
// BlockHash must fail loud before ever reaching a position source, instead of
// silently defaulting to the zero hash (common.HexToHash never errors).
func TestProcessBlock_MissingBlockHash_ReturnsError(t *testing.T) {
	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(1, 500, 0, mustMarshalReceipts(t, []TransactionReceipt{}))

	entries := []*TokenEntry{{
		ContractAddress: common.HexToAddress("0x1111"),
		WalletAddress:   common.HexToAddress("0xbbbb"),
		TokenType:       "erc20",
	}}
	source := &mockSource{
		name:       "erc20",
		tokenTypes: map[string]bool{"erc20": true},
	}
	registry := NewSourceRegistry(slog.New(slog.NewTextHandler(io.Discard, nil)))
	registry.Register(source)

	handler := &testHandler{}
	svc := &Service{
		cache:            cache,
		extractor:        NewTransferExtractor(nil),
		registry:         registry,
		entries:          entries,
		handler:          handler,
		logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		config:           Config{ChainID: 1, SweepEveryNBlocks: 1},
		blocksSinceSweep: 0,
	}

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 500, Version: 0, BlockTimestamp: 1700000000, BlockHash: ""}
	if err := svc.processBlock(context.Background(), event); err == nil {
		t.Fatal("expected non-nil error from processBlock when event.BlockHash is empty")
	}

	if source.called != 0 {
		t.Errorf("position source invoked %d times, want 0 (block must not be read)", source.called)
	}
	if len(handler.batches) != 0 {
		t.Errorf("HandleBatch called %d times, want 0 (block must not be persisted)", len(handler.batches))
	}
}

// runSweepWithBalance runs one sweep block where a single entry of the given
// token type resolves to the given balance, returning the snapshot the
// handler received.
func runSweepWithBalance(t *testing.T, tokenType, sourceName string, bal *PositionBalance) *PositionSnapshot {
	t.Helper()
	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(1, 500, 0, mustMarshalReceipts(t, []TransactionReceipt{}))

	entry := &TokenEntry{
		ContractAddress: common.HexToAddress("0x1111"),
		WalletAddress:   common.HexToAddress("0xbbbb"),
		TokenType:       tokenType,
	}
	result := NewFetchResult()
	result.Balances[entry.Key()] = bal

	registry := NewSourceRegistry(slog.New(slog.NewTextHandler(io.Discard, nil)))
	registry.Register(&mockSource{
		name:       sourceName,
		tokenTypes: map[string]bool{tokenType: true},
		result:     result,
	})

	handler := &testHandler{}
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

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 500, Version: 0, BlockTimestamp: 1700000000, BlockHash: testBlockHash.Hex()}
	if err := svc.processBlock(context.Background(), event); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if len(handler.batches) != 1 || len(handler.batches[0].Snapshots) != 1 {
		t.Fatalf("expected 1 batch with 1 snapshot, got %d batches", len(handler.batches))
	}
	return handler.batches[0].Snapshots[0]
}

func TestSweep_ThreadsUnderlyingValueOntoSnapshot(t *testing.T) {
	got := runSweepWithBalance(t, "erc4626", "erc4626", &PositionBalance{
		Balance:         big.NewInt(500),
		UnderlyingValue: big.NewInt(777),
	})
	if got.UnderlyingValue == nil || got.UnderlyingValue.Cmp(big.NewInt(777)) != 0 {
		t.Fatalf("UnderlyingValue = %v, want 777", got.UnderlyingValue)
	}
}

// TestSweep_ThreadsPoolPairOntoSnapshot: sweep is the only path uni_v3
// snapshots take in production (V3 pool contracts emit no ERC20 transfers to
// match), so the pool pair must survive the sweep copy or univ3RowMeta fails
// every sweep block.
func TestSweep_ThreadsPoolPairOntoSnapshot(t *testing.T) {
	poolToken0 := common.HexToAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a")
	poolToken1 := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

	got := runSweepWithBalance(t, "uni_v3_pool", "uni-v3", &PositionBalance{
		Balance:         big.NewInt(500),
		UnderlyingValue: big.NewInt(500),
		PoolToken0:      &poolToken0,
		PoolToken1:      &poolToken1,
	})
	if got.PoolToken0 == nil || *got.PoolToken0 != poolToken0 {
		t.Fatalf("PoolToken0 = %v, want %s", got.PoolToken0, poolToken0.Hex())
	}
	if got.PoolToken1 == nil || *got.PoolToken1 != poolToken1 {
		t.Fatalf("PoolToken1 = %v, want %s", got.PoolToken1, poolToken1.Hex())
	}
}

// TestSweep_ThreadsZeroExitRowOntoSnapshot: an explicit uni_v3 zero row (see
// computeEntryBalance) must survive the sweep copy intact, not be skipped as
// empty.
func TestSweep_ThreadsZeroExitRowOntoSnapshot(t *testing.T) {
	poolToken0 := common.HexToAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a")
	poolToken1 := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

	got := runSweepWithBalance(t, "uni_v3_pool", "uni-v3", &PositionBalance{
		Balance:         big.NewInt(0),
		UnderlyingValue: big.NewInt(0),
		PoolToken0:      &poolToken0,
		PoolToken1:      &poolToken1,
	})
	if got.Balance == nil || got.Balance.Sign() != 0 {
		t.Fatalf("Balance = %v, want explicit 0", got.Balance)
	}
	if got.UnderlyingValue == nil || got.UnderlyingValue.Sign() != 0 {
		t.Fatalf("UnderlyingValue = %v, want explicit 0", got.UnderlyingValue)
	}
	if got.PoolToken0 == nil || got.PoolToken1 == nil {
		t.Fatalf("pool pair should survive the sweep copy, got %v/%v", got.PoolToken0, got.PoolToken1)
	}
	if got.Direction != DirectionSweep {
		t.Fatalf("Direction = %q, want %q", got.Direction, DirectionSweep)
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

	poolToken0 := common.HexToAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a")
	poolToken1 := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	balances := map[EntryKey]*PositionBalance{
		entry.Key(): {
			Balance:         big.NewInt(1000000),
			ScaledBalance:   big.NewInt(2000000),
			UnderlyingValue: big.NewInt(42),
			PoolToken0:      &poolToken0,
			PoolToken1:      &poolToken1,
		},
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
	if s.UnderlyingValue == nil || s.UnderlyingValue.Cmp(big.NewInt(42)) != 0 {
		t.Errorf("expected underlyingValue 42, got %v", s.UnderlyingValue)
	}
	if s.PoolToken0 == nil || *s.PoolToken0 != poolToken0 {
		t.Errorf("expected poolToken0 %s, got %v", poolToken0.Hex(), s.PoolToken0)
	}
	if s.PoolToken1 == nil || *s.PoolToken1 != poolToken1 {
		t.Errorf("expected poolToken1 %s, got %v", poolToken1.Hex(), s.PoolToken1)
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

// ── per-block liveness/latency metrics ──

// TestProcessBlock_RecordsLivenessMetrics asserts every consumed block emits one
// blocks_processed_total sample and one processing_duration_seconds observation
// carrying {chain,status}, on both the success and error paths. These are the
// per-block liveness + latency signals the VectorAllocationTracker{Stalled,
// ErrorRatioHigh,BlockLatencyHigh} alerts key on, so the exact metric and label
// names must not drift from the alert expressions.
func TestProcessBlock_RecordsLivenessMetrics(t *testing.T) {
	tests := []struct {
		name       string
		buildSvc   func(t *testing.T, m *telemetry.Metrics) (*Service, outbound.BlockEvent)
		wantErr    bool
		wantStatus string
	}{
		{
			name:       "success path records success",
			buildSvc:   newCleanBlockSvc,
			wantErr:    false,
			wantStatus: outbound.StatusSuccess,
		},
		{
			name:       "error path records error",
			buildSvc:   newCacheMissSvc,
			wantErr:    true,
			wantStatus: outbound.StatusError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics, reader := newBlockMetrics(t)
			svc, event := tt.buildSvc(t, metrics)

			err := svc.processBlock(context.Background(), event)
			if tt.wantErr != (err != nil) {
				t.Fatalf("processBlock err = %v, wantErr = %v", err, tt.wantErr)
			}

			if got := singleStatusCounter(t, reader, "blocks_processed_total", tt.wantStatus); got != 1 {
				t.Errorf("blocks_processed_total{status=%q} = %d, want 1", tt.wantStatus, got)
			}
			if got := singleStatusHistogramCount(t, reader, "processing_duration_seconds", tt.wantStatus); got != 1 {
				t.Errorf("processing_duration_seconds{status=%q} count = %d, want 1", tt.wantStatus, got)
			}
		})
	}
}

// newBlockMetrics wires a real telemetry.Metrics to an in-memory reader via the
// global meter provider (telemetry.NewMetrics reads the global provider), so a
// processBlock run can be asserted against the exact series the Vector alerts
// query. Restores the previous global provider on cleanup.
func newBlockMetrics(t *testing.T) (*telemetry.Metrics, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = mp.Shutdown(context.Background())
	})

	m, err := telemetry.NewMetrics("prime-allocation-indexer", "mainnet")
	if err != nil {
		t.Fatalf("telemetry.NewMetrics: %v", err)
	}
	return m, reader
}

// newCleanBlockSvc builds a service whose processBlock consumes a transfer-free
// block and skips the sweep (SweepEveryNBlocks high), so it returns nil.
func newCleanBlockSvc(t *testing.T, m *telemetry.Metrics) (*Service, outbound.BlockEvent) {
	t.Helper()
	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(1, 500, 0, mustMarshalReceipts(t, []TransactionReceipt{}))
	svc := &Service{
		cache:       cache,
		metrics:     m,
		extractor:   NewTransferExtractor(nil),
		registry:    NewSourceRegistry(discardLogger()),
		entryLookup: map[EntryKey]*TokenEntry{},
		handler:     &testHandler{},
		logger:      discardLogger(),
		config:      Config{ChainID: 1, SweepEveryNBlocks: 100},
	}
	return svc, outbound.BlockEvent{ChainID: 1, BlockNumber: 500, Version: 0, BlockTimestamp: 1700000000, BlockHash: testBlockHash.Hex()}
}

// newCacheMissSvc builds a service whose processBlock errors on a cache miss
// (receipts not found) before any snapshot work.
func newCacheMissSvc(t *testing.T, m *telemetry.Metrics) (*Service, outbound.BlockEvent) {
	t.Helper()
	svc := &Service{
		cache:   testutil.NewMockBlockCache(),
		metrics: m,
		logger:  discardLogger(),
		config:  Config{ChainID: 1},
	}
	return svc, outbound.BlockEvent{ChainID: 1, BlockNumber: 99999, Version: 0}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// singleStatusCounter asserts the counter has exactly one datapoint, carrying
// chain="mainnet" and the given status, and returns its value.
func singleStatusCounter(t *testing.T, reader sdkmetric.Reader, name, status string) int64 {
	t.Helper()
	m := collectMetric(t, reader, name)
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s is %T, want Sum[int64]", name, m.Data)
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("%s has %d datapoints, want 1", name, len(sum.DataPoints))
	}
	assertChainStatus(t, sum.DataPoints[0].Attributes, status)
	return sum.DataPoints[0].Value
}

// singleStatusHistogramCount asserts the histogram has exactly one datapoint,
// carrying chain="mainnet" and the given status, and returns its observation
// count.
func singleStatusHistogramCount(t *testing.T, reader sdkmetric.Reader, name, status string) uint64 {
	t.Helper()
	m := collectMetric(t, reader, name)
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("%s is %T, want Histogram[float64]", name, m.Data)
	}
	if len(hist.DataPoints) != 1 {
		t.Fatalf("%s has %d datapoints, want 1", name, len(hist.DataPoints))
	}
	assertChainStatus(t, hist.DataPoints[0].Attributes, status)
	return hist.DataPoints[0].Count
}

func assertChainStatus(t *testing.T, attrs attribute.Set, status string) {
	t.Helper()
	if v, ok := attrs.Value("chain"); !ok || v.AsString() != "mainnet" {
		t.Errorf("chain attribute = %q (present=%v), want mainnet", v.AsString(), ok)
	}
	if v, ok := attrs.Value("status"); !ok || v.AsString() != status {
		t.Errorf("status attribute = %q (present=%v), want %s", v.AsString(), ok, status)
	}
}
