package allocation_tracker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/ethereum/go-ethereum/common"
)

// ── Mocks ──

type testHandler struct {
	snapshots []*PositionSnapshot
	err       error
}

func (m *testHandler) HandleSnapshots(ctx context.Context, snapshots []*PositionSnapshot) error {
	m.snapshots = append(m.snapshots, snapshots...)
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

// ── VEC-188: partial-fetch propagation ──

// failingSourceRegistry returns a SourceRegistry wired with a single
// mockSource that matches the "anything" token type and errors on
// FetchBalances. Shared by the two VEC-188 tests below.
func failingSourceRegistry(logger *slog.Logger) *SourceRegistry {
	registry := NewSourceRegistry(logger)
	registry.Register(&mockSource{
		name:       "always-fails",
		tokenTypes: map[string]bool{"anything": true},
		err:        errors.New("alchemy 429: compute units exceeded"),
	})
	return registry
}

// TestSweep_ReturnsErrorOnPartialFailure codifies the VEC-188 invariant for
// allocation_tracker's sweep path: a partial fetch must NACK the SQS message.
// Pre-VEC-188 this returned nil; the fix is the new `if err != nil` path
// in `Service.sweep`.
func TestSweep_ReturnsErrorOnPartialFailure(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	entry := &TokenEntry{
		ContractAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		WalletAddress:   common.HexToAddress("0x2222222222222222222222222222222222222222"),
		TokenType:       "anything",
	}
	svc, err := NewService(Config{
		ChainID:           1,
		SweepEveryNBlocks: 1000,
		Logger:            logger,
	}, nil, nil, failingSourceRegistry(logger), []*TokenEntry{entry}, &testHandler{}, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	err = svc.sweep(ctx, 100, 0, time.Unix(1, 0).UTC())
	if err == nil {
		t.Fatal("sweep must return an error on partial fetch failure; got nil")
	}
}

// TestProcessTransfers_ReturnsErrorOnPartialFetchFailure codifies the VEC-188
// invariant for allocation_tracker's live path. Pre-VEC-188 processBlock
// logged `Warn("partial balance fetch")` and returned nil; the fix is the
// new `if err != nil` path in `Service.processTransfers`. We exercise the
// helper directly so the test doesn't depend on cache+extractor plumbing
// that's orthogonal.
func TestProcessTransfers_ReturnsErrorOnPartialFetchFailure(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	entry := &TokenEntry{
		ContractAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		WalletAddress:   common.HexToAddress("0x2222222222222222222222222222222222222222"),
		TokenType:       "anything",
	}
	svc, err := NewService(Config{
		ChainID:           1,
		SweepEveryNBlocks: 1000,
		Logger:            logger,
	}, nil, nil, failingSourceRegistry(logger), []*TokenEntry{entry}, &testHandler{}, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100, Version: 0}
	transfers := []*TransferEvent{{
		TokenAddress: entry.ContractAddress,
		ProxyAddress: entry.WalletAddress,
		Direction:    DirectionIn,
	}}
	affected := []*TokenEntry{entry}

	err = svc.processTransfers(ctx, affected, transfers, event, time.Unix(1, 0).UTC())
	if err == nil {
		t.Fatal("processTransfers must return an error on partial fetch failure; got nil")
	}
}
