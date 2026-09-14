package allocation_tracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"slices"
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

func TestNewService_RejectsNegativeSweepCadence(t *testing.T) {
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

	_, err := NewService(
		Config{ChainID: 1, SweepEveryNBlocks: -1},
		nil, nil, registry, entries, &testHandler{}, proxies,
	)
	if err == nil || !strings.Contains(err.Error(), "must not be negative") {
		t.Fatalf("error = %v, want a negative-cadence rejection (it would sweep on every block)", err)
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

func TestStart_RefusesAVisibilityTimeoutAReceiveCanOutrun(t *testing.T) {
	consumer := &testutil.MockSQSConsumer{
		VisibilityTimeoutFn: func() time.Duration { return 30 * time.Second },
	}
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
		Config{ChainID: 1, Logger: quietLogger()},
		consumer, testutil.NewMockBlockCache(), NewSourceRegistry(quietLogger()), entries, &testHandler{}, proxies,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	err = svc.Start(context.Background())
	if err == nil {
		_ = svc.Stop()
		t.Fatal("Start accepted a 30s visibility timeout; a booted worker never crashloops on it, because " +
			"ProcessMessages revalidates on every poll and RunLoop only logs what it returns, so the pod reports " +
			"Ready and spins logging forever while the queue never drains")
	}
	if !strings.Contains(err.Error(), "visibility timeout") {
		t.Errorf("Start error = %q, want it to name the visibility timeout", err)
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

type trackerFixture struct {
	svc     *Service
	handler *testHandler
	source  *mockSource
}

func newTracker(t *testing.T, tokenType, sourceName string, bal *PositionBalance, sweepEveryN int) *trackerFixture {
	t.Helper()

	entry := &TokenEntry{
		ContractAddress: common.HexToAddress("0x1111"),
		WalletAddress:   common.HexToAddress("0xaaaa"),
		Star:            "spark",
		Chain:           "mainnet",
		TokenType:       tokenType,
	}
	result := NewFetchResult()
	result.Balances[entry.Key()] = bal

	source := &mockSource{
		name:       sourceName,
		tokenTypes: map[string]bool{tokenType: true},
		result:     result,
	}
	logger := quietLogger()
	registry := NewSourceRegistry(logger)
	registry.Register(source)

	receipts := mustMarshalReceipts(t, []TransactionReceipt{})
	cache := testutil.NewMockBlockCache()
	cache.GetReceiptsFn = func(context.Context, int64, int64, int) (json.RawMessage, error) {
		return receipts, nil
	}

	handler := &testHandler{}
	svc, err := NewService(
		Config{ChainID: 1, SweepEveryNBlocks: sweepEveryN, Logger: logger},
		nil,
		cache,
		registry,
		[]*TokenEntry{entry},
		handler,
		[]ProxyConfig{{Star: "spark", Chain: "mainnet", Address: common.HexToAddress("0xaaaa")}},
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return &trackerFixture{svc: svc, handler: handler, source: source}
}

func newCadenceFixture(t *testing.T, sweepEveryN int) *trackerFixture {
	t.Helper()
	return newTracker(t, "erc20", "erc20", &PositionBalance{Balance: big.NewInt(1), UnderlyingValue: big.NewInt(1)}, sweepEveryN)
}

func (f *trackerFixture) driveBacklog(first int64, count int) []int64 {
	var failed []int64
	for i := range count {
		event := outbound.BlockEvent{
			ChainID:        1,
			BlockNumber:    first + int64(i),
			BlockTimestamp: 1700000000,
			BlockHash:      testBlockHash.Hex(),
		}
		if err := f.svc.processBlock(context.Background(), event); err != nil {
			failed = append(failed, event.BlockNumber)
		}
	}
	return failed
}

func (f *trackerFixture) sweptBlocks() []int64 {
	var blocks []int64
	for _, batch := range f.handler.batches {
		for _, snapshot := range batch.Snapshots {
			if snapshot.Direction == DirectionSweep {
				blocks = append(blocks, snapshot.BlockNumber)
			}
		}
	}
	return blocks
}

func TestProcessBlock_SweepsOnEveryNthConsumedBlock(t *testing.T) {
	const first int64 = 50701400

	tests := []struct {
		name        string
		sweepEveryN int
		blocks      int
		want        []int64
	}{
		{
			name:        "75-block default",
			sweepEveryN: 75,
			blocks:      225,
			want:        []int64{first + 74, first + 149, first + 224},
		},
		{
			name:        "6000-block cadence",
			sweepEveryN: 6000,
			blocks:      12000,
			want:        []int64{first + 5999, first + 11999},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newCadenceFixture(t, tt.sweepEveryN)

			if failed := f.driveBacklog(first, tt.blocks); len(failed) > 0 {
				t.Fatalf("processBlock failed on blocks %v", failed)
			}

			if got := f.sweptBlocks(); !slices.Equal(got, tt.want) {
				t.Errorf("swept blocks = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProcessBlock_SweepFailureRetriesOnRedelivery(t *testing.T) {
	const first int64 = 50701400
	const sweepEveryN = 75

	f := newCadenceFixture(t, sweepEveryN)
	f.source.err = fmt.Errorf("temporary rpc error")
	f.source.errCalls = 1

	failed := f.driveBacklog(first, sweepEveryN)

	if want := []int64{first + 74}; !slices.Equal(failed, want) {
		t.Fatalf("failed blocks = %v, want %v", failed, want)
	}
	if failed := f.driveBacklog(first+74, 1); len(failed) != 0 {
		t.Fatalf("redelivery failed on blocks %v", failed)
	}
	if got, want := f.sweptBlocks(), []int64{first + 74}; !slices.Equal(got, want) {
		t.Errorf("swept blocks = %v, want %v", got, want)
	}

	if failed := f.driveBacklog(first+75, sweepEveryN); len(failed) != 0 {
		t.Fatalf("processBlock failed on blocks %v", failed)
	}
	if got, want := f.sweptBlocks(), []int64{first + 74, first + 149}; !slices.Equal(got, want) {
		t.Errorf("swept blocks = %v, want %v", got, want)
	}
}

// runSweepWithBalance runs one sweep block where a single entry of the given
// token type resolves to the given balance, returning the snapshot the
// handler received.
func runSweepWithBalance(t *testing.T, tokenType, sourceName string, bal *PositionBalance) *PositionSnapshot {
	t.Helper()

	f := newTracker(t, tokenType, sourceName, bal, 1)
	if failed := f.driveBacklog(500, 1); len(failed) > 0 {
		t.Fatalf("processBlock failed on blocks %v", failed)
	}
	if len(f.handler.batches) != 1 || len(f.handler.batches[0].Snapshots) != 1 {
		t.Fatalf("expected 1 batch with 1 snapshot, got %d batches", len(f.handler.batches))
	}
	return f.handler.batches[0].Snapshots[0]
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

// ── centrifuge share aliases ──

// Real mainnet addresses, because the shape that matters is real: grove holds
// JTRSY through a vault whose share() IS spark's own entry address.
var (
	groveJAAAVault  = common.HexToAddress("0x4880799ee5200fc58da299e965df644fbf46780b")
	groveJAAAShare  = common.HexToAddress("0x5a0F93D0AaE5A7Bb9Ca7a8E6A1e6b1E2b6C0Fb11")
	groveJTRSYVault = common.HexToAddress("0xfe6920eb6c421f1179ca8c8d4170530cdbdfd77a")
	sparkJTRSYShare = common.HexToAddress("0x8c213ee79581ff4984583c6a801e5263418c4b86")
	groveProxy      = common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")
	sparkProxy      = common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")

	centrifugeCounterparty = common.HexToAddress("0x9999999999999999999999999999999999999999")
	centrifugeTxHash       = common.HexToHash("0xda50e73f9d4722402ae4ec6e506c3726a78fc5f6146b4957bfadc2c1fffc8f8c")
)

const centrifugeFirstBlock int64 = 21000000

// centrifugeShape is one entry the fixture builds: the address the entry is keyed
// on, the token its share() names (itself for a direct share), and the holder.
type centrifugeShape struct {
	contract common.Address
	share    common.Address
	wallet   common.Address
}

func groveVaultShape(contract, share common.Address) centrifugeShape {
	return centrifugeShape{contract: contract, share: share, wallet: groveProxy}
}

func sparkDirectShareShape(share common.Address) centrifugeShape {
	return centrifugeShape{contract: share, share: share, wallet: sparkProxy}
}

// mockShareSource is a mockSource that also names its entries' share tokens, the
// shape ERC7540Source has for ERC-7540 vaults.
type mockShareSource struct {
	*mockSource
	shares    map[common.Address]common.Address
	sharesErr error

	resolveCalls int
	askedFor     []common.Address
}

func (m *mockShareSource) shareTokens(_ context.Context, entries []*TokenEntry, _ common.Hash) (map[common.Address]common.Address, error) {
	m.resolveCalls++
	for _, entry := range entries {
		m.askedFor = append(m.askedFor, entry.ContractAddress)
	}
	if m.sharesErr != nil {
		return nil, m.sharesErr
	}
	return m.shares, nil
}

type centrifugeFixture struct {
	svc     *Service
	cache   *testutil.MockBlockCache
	handler *testHandler
	source  *mockShareSource
	block   int64
	metrics sdkmetric.Reader
}

// newCentrifugeTracker wires a tracker over one centrifuge entry per shape, each
// holding 500 of the token that shape's share() names.
func newCentrifugeTracker(t *testing.T, shapes []centrifugeShape, sweepEveryN int) *centrifugeFixture {
	t.Helper()

	entries := make([]*TokenEntry, 0, len(shapes))
	shares := make(map[common.Address]common.Address, len(shapes))
	result := NewFetchResult()
	for _, shape := range shapes {
		entry := &TokenEntry{
			ContractAddress: shape.contract,
			WalletAddress:   shape.wallet,
			Star:            "grove",
			Chain:           "mainnet",
			Protocol:        "centrifuge",
			TokenType:       TokenTypeCentrifuge,
		}
		entries = append(entries, entry)
		shares[shape.contract] = shape.share
		result.Balances[entry.Key()] = &PositionBalance{
			Balance:       big.NewInt(500),
			ScaledBalance: big.NewInt(500),
			ShareToken:    &shape.share,
		}
	}

	source := &mockShareSource{
		mockSource: &mockSource{
			name:       "erc7540",
			tokenTypes: map[string]bool{TokenTypeCentrifuge: true},
			result:     result,
		},
		shares: shares,
	}
	return newCentrifugeTrackerWithSource(t, entries, source, sweepEveryN)
}

// newCentrifugeTrackerWithSource is the seam for a registry whose source cannot
// name a share, and for entries the default factory does not build.
func newCentrifugeTrackerWithSource(t *testing.T, entries []*TokenEntry, source *mockShareSource, sweepEveryN int) *centrifugeFixture {
	t.Helper()

	logger := quietLogger()
	registry := NewSourceRegistry(logger)
	if source != nil {
		registry.Register(source)
	} else {
		registry.Register(&mockSource{
			name:       "no-share-resolver",
			tokenTypes: map[string]bool{TokenTypeCentrifuge: true},
			result:     NewFetchResult(),
		})
	}

	proxies := make([]ProxyConfig, 0, 2)
	for _, wallet := range []common.Address{groveProxy, sparkProxy} {
		proxies = append(proxies, ProxyConfig{Star: "grove", Chain: "mainnet", Address: wallet})
	}

	cache := testutil.NewMockBlockCache()
	handler := &testHandler{}
	tel, reader := newRecordingTelemetry(t)
	svc, err := NewService(
		Config{ChainID: 1, SweepEveryNBlocks: sweepEveryN, Logger: logger, Telemetry: tel},
		nil,
		cache,
		registry,
		entries,
		handler,
		proxies,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return &centrifugeFixture{svc: svc, cache: cache, handler: handler, source: source, block: centrifugeFirstBlock, metrics: reader}
}

// consume drives the next block, whose single receipt carries the given logs.
func (f *centrifugeFixture) consume(t *testing.T, logs ...gethtypes.Log) error {
	t.Helper()
	f.block++
	f.cache.SetReceipts(1, f.block, 0, mustMarshalReceipts(t, []TransactionReceipt{{Logs: logs}}))
	return f.svc.processBlock(context.Background(), outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    f.block,
		Version:        0,
		BlockTimestamp: 1700000000,
		BlockHash:      testBlockHash.Hex(),
	})
}

// redeliver drives the block consume last processed again, as SQS does after a NACK.
func (f *centrifugeFixture) redeliver(t *testing.T, logs ...gethtypes.Log) error {
	t.Helper()
	f.block--
	return f.consume(t, logs...)
}

// seedRoute names an entry's emitter up front, the state a block that re-points a
// share starts from.
func (f *centrifugeFixture) seedRoute(t *testing.T, emitter common.Address, entry *TokenEntry) {
	t.Helper()
	routes, err := f.svc.nextTransferRoutes(context.Background(), []namedEmitter{{entry: entry, emitter: emitter}}, centrifugeFirstBlock)
	if err != nil {
		t.Fatalf("seed the route for %s: %v", entry.ContractAddress.Hex(), err)
	}
	f.svc.transferAliases = routes
}

// repointShare makes the next fetch report a different share for one entry, the
// shape a re-pointed vault (or a reverting share()) takes on the read path.
func (f *centrifugeFixture) repointShare(t *testing.T, contract, share common.Address) {
	t.Helper()
	for _, entry := range f.svc.entries {
		if entry.ContractAddress != contract {
			continue
		}
		bal, ok := f.source.result.Balances[entry.Key()]
		if !ok {
			t.Fatalf("no seeded balance for %s", contract.Hex())
		}
		bal.ShareToken = &share
		return
	}
	t.Fatalf("no entry keyed on %s", contract.Hex())
}

// snapshotFor returns the snapshot the handler received for one entry key.
func (f *centrifugeFixture) snapshotFor(contract, wallet common.Address) *PositionSnapshot {
	for _, batch := range f.handler.batches {
		for _, snap := range batch.Snapshots {
			if snap.Entry.ContractAddress == contract && snap.Entry.WalletAddress == wallet {
				return snap
			}
		}
	}
	return nil
}

// transferLog is a Transfer of the emitting token into a proxy, carrying a real
// transaction hash so an event row is distinguishable from a sweep row.
func transferLog(token common.Address, to common.Address, amount *big.Int, index uint) gethtypes.Log {
	log := makeTransferLog(token, centrifugeCounterparty, to, amount, index)
	log.TxHash = centrifugeTxHash
	return log
}

// TestProcessBlock_ShareTransferSnapshotsTheVaultEntry: the share token emits the
// Transfer, the entry is keyed on the vault, so without the alias the log matches
// nothing and the position only ever moves on the periodic sweep.
func TestProcessBlock_ShareTransferSnapshotsTheVaultEntry(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{groveVaultShape(groveJAAAVault, groveJAAAShare)}, 1000)

	if err := f.consume(t, transferLog(groveJAAAShare, groveProxy, big.NewInt(250), 7)); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	snap := f.snapshotFor(groveJAAAVault, groveProxy)
	if snap == nil {
		t.Fatal("no snapshot for the vault entry; the share transfer matched nothing")
	}
	if snap.TxHash != centrifugeTxHash.Hex() {
		t.Errorf("TxHash = %q, want %q", snap.TxHash, centrifugeTxHash.Hex())
	}
	if snap.TxAmount == nil || snap.TxAmount.Cmp(big.NewInt(250)) != 0 {
		t.Errorf("TxAmount = %v, want 250", snap.TxAmount)
	}
	if snap.Direction != DirectionIn {
		t.Errorf("Direction = %q, want %q", snap.Direction, DirectionIn)
	}
}

// TestProcessBlock_OneShareHeldTwoWaysKeepsBothPositions is the real mainnet
// shape: grove's JTRSY vault resolves to the very address spark's entry is keyed
// on, so an emitter-keyed alias would drop whichever entry lost the write.
func TestProcessBlock_OneShareHeldTwoWaysKeepsBothPositions(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{
		groveVaultShape(groveJTRSYVault, sparkJTRSYShare),
		sparkDirectShareShape(sparkJTRSYShare),
	}, 1000)

	err := f.consume(t,
		transferLog(sparkJTRSYShare, groveProxy, big.NewInt(250), 7),
		transferLog(sparkJTRSYShare, sparkProxy, big.NewInt(400), 8),
	)
	if err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	for _, tc := range []struct {
		name     string
		contract common.Address
		wallet   common.Address
		amount   int64
	}{
		{"grove holds it through the vault", groveJTRSYVault, groveProxy, 250},
		{"spark holds the share directly", sparkJTRSYShare, sparkProxy, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap := f.snapshotFor(tc.contract, tc.wallet)
			if snap == nil {
				t.Fatalf("no snapshot for %s/%s", tc.contract.Hex(), tc.wallet.Hex())
			}
			if snap.TxAmount == nil || snap.TxAmount.Cmp(big.NewInt(tc.amount)) != 0 {
				t.Errorf("TxAmount = %v, want %d", snap.TxAmount, tc.amount)
			}
		})
	}
}

// TestProcessBlock_ResolvesEachShareOnlyOnce: an entry left unaliased is resolved
// again on every block carrying any transfer, which is a share() multicall per
// block forever. The direct share is the risky half — it aliases to itself.
func TestProcessBlock_ResolvesEachShareOnlyOnce(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{
		groveVaultShape(groveJTRSYVault, sparkJTRSYShare),
		sparkDirectShareShape(sparkJTRSYShare),
	}, 1000)

	for range 3 {
		if err := f.consume(t, transferLog(sparkJTRSYShare, groveProxy, big.NewInt(1), 0)); err != nil {
			t.Fatalf("processBlock: %v", err)
		}
	}

	if f.source.resolveCalls != 1 {
		t.Errorf("share resolution ran %d times over 3 blocks, want 1", f.source.resolveCalls)
	}
}

// TestProcessBlock_ResolvesOnlyTheEntriesAwaitingAnAlias: a mixed chain is the
// normal case, and asking a resolver about an entry keyed on the token it holds
// would spend a multicall on an answer nothing reads.
func TestProcessBlock_ResolvesOnlyTheEntriesAwaitingAnAlias(t *testing.T) {
	plain := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	f := newCentrifugeTracker(t, []centrifugeShape{groveVaultShape(groveJAAAVault, groveJAAAShare)}, 1000)
	f.svc.entries = append(f.svc.entries, &TokenEntry{
		ContractAddress: plain,
		WalletAddress:   groveProxy,
		Star:            "grove",
		Chain:           "mainnet",
		TokenType:       "erc20",
	})

	if err := f.consume(t, transferLog(groveJAAAShare, groveProxy, big.NewInt(1), 0)); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if want := []common.Address{groveJAAAVault}; !slices.Equal(f.source.askedFor, want) {
		t.Errorf("resolver asked for %v, want only the centrifuge entry %v", f.source.askedFor, want)
	}
}

// TestProcessBlock_ShareResolutionFailure_ReturnsError: VEC-188 — an unresolved
// share leaves every share transfer unmatched, so the block must NACK rather than
// persist a block that silently saw no activity.
func TestProcessBlock_ShareResolutionFailure_ReturnsError(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{groveVaultShape(groveJAAAVault, groveJAAAShare)}, 1000)
	f.source.sharesErr = fmt.Errorf("rpc timeout")

	err := f.consume(t, transferLog(groveJAAAShare, groveProxy, big.NewInt(250), 7))
	if err == nil {
		t.Fatal("expected the share resolution failure to be returned")
	}
	if !strings.Contains(err.Error(), "resolve transfer aliases") {
		t.Errorf("error = %q, want it to name the alias resolution", err)
	}
	if len(f.handler.batches) != 0 {
		t.Errorf("HandleBatch called %d times, want 0", len(f.handler.batches))
	}
}

// TestProcessBlock_UnnameableShare_ReturnsError: an entry whose source cannot name
// its token would sit unaliased forever, so it fails the block instead of being
// skipped into permanent silence.
func TestProcessBlock_UnnameableShare_ReturnsError(t *testing.T) {
	entry := &TokenEntry{
		ContractAddress: groveJAAAVault,
		WalletAddress:   groveProxy,
		Star:            "grove",
		Chain:           "mainnet",
		Protocol:        "centrifuge",
		TokenType:       TokenTypeCentrifuge,
	}
	f := newCentrifugeTrackerWithSource(t, []*TokenEntry{entry}, nil, 1000)

	err := f.consume(t, transferLog(groveJAAAShare, groveProxy, big.NewInt(250), 7))
	if err == nil {
		t.Fatal("expected an entry with no share resolver to fail the block")
	}
	if !strings.Contains(err.Error(), "cannot name one") {
		t.Errorf("error = %q, want it to name the missing resolver", err)
	}
}

// TestProcessBlock_ShareResolvedForOnlySomeEntries_ReturnsError: a resolver that
// answers for one entry and omits another leaves the omitted one unmatched, which
// is the silent half-fix this path exists to prevent.
func TestProcessBlock_ShareResolvedForOnlySomeEntries_ReturnsError(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{
		groveVaultShape(groveJAAAVault, groveJAAAShare),
		groveVaultShape(groveJTRSYVault, sparkJTRSYShare),
	}, 1000)
	delete(f.source.shares, groveJTRSYVault)

	err := f.consume(t, transferLog(groveJAAAShare, groveProxy, big.NewInt(250), 7))
	if err == nil {
		t.Fatal("expected a partially answered resolution to fail the block")
	}
	if !strings.Contains(err.Error(), "no share token named") {
		t.Errorf("error = %q, want it to name the unresolved entry", err)
	}
}

// TestSweep_ReplacesTheAliasOfARepointedShare: the alias set is a function of the
// latest resolution, so a re-pointed share must both take effect and displace the
// old one — a transfer of the retired token is not this position's activity.
func TestSweep_ReplacesTheAliasOfARepointedShare(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{groveVaultShape(groveJAAAVault, groveJAAAShare)}, 1)
	stale := common.HexToAddress("0x0000000000000000000000000000000000005747")
	f.seedRoute(t, stale, f.svc.entries[0])

	if err := f.consume(t); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	vaultKey := EntryKey{ContractAddress: groveJAAAVault, WalletAddress: groveProxy}
	if got := f.svc.entryKeyFor(&TransferEvent{TokenAddress: groveJAAAShare, ProxyAddress: groveProxy}); got != vaultKey {
		t.Errorf("entryKeyFor(new share) = %v, want %v — the sweep did not adopt the alias", got, vaultKey)
	}
	staleKey := EntryKey{ContractAddress: stale, WalletAddress: groveProxy}
	if got := f.svc.entryKeyFor(&TransferEvent{TokenAddress: stale, ProxyAddress: groveProxy}); got != staleKey {
		t.Errorf("entryKeyFor(stale share) = %v, want the identity %v — the old alias was not dropped", got, staleKey)
	}
}

// TestSweep_RepointedShareRecordsMetric: pruneDisplacedRoutes records one
// allocation.share_repoints.total sample beside the Warn, so the follow-up
// alert has a series to read.
func TestSweep_RepointedShareRecordsMetric(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{groveVaultShape(groveJAAAVault, groveJAAAShare)}, 1)
	stale := common.HexToAddress("0x0000000000000000000000000000000000005747")
	f.seedRoute(t, stale, f.svc.entries[0])

	if err := f.consume(t); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	m := collectMetric(t, f.metrics, "allocation.share_repoints.total")
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("allocation.share_repoints.total is %T, want Sum[int64]", m.Data)
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("got %d data points, want 1", len(sum.DataPoints))
	}

	dp := sum.DataPoints[0]
	if dp.Value != 1 {
		t.Errorf("value = %d, want 1", dp.Value)
	}
	if entry, ok := dp.Attributes.Value("entry"); !ok || entry.AsString() != groveJAAAVault.Hex() {
		t.Errorf("entry attribute = %v, want %s", entry, groveJAAAVault.Hex())
	}
	if wallet, ok := dp.Attributes.Value("wallet"); !ok || wallet.AsString() != groveProxy.Hex() {
		t.Errorf("wallet attribute = %v, want %s", wallet, groveProxy.Hex())
	}
}

// TestProcessBlock_EventPathAdoptsARepointedShare: the event fetch resolves shares
// too, so a re-point seen there must not wait for the next sweep.
func TestProcessBlock_EventPathAdoptsARepointedShare(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{groveVaultShape(groveJAAAVault, groveJAAAShare)}, 1000)
	stale := common.HexToAddress("0x0000000000000000000000000000000000005747")
	f.seedRoute(t, stale, f.svc.entries[0])

	if err := f.consume(t, transferLog(stale, groveProxy, big.NewInt(250), 7)); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	snap := f.snapshotFor(groveJAAAVault, groveProxy)
	if snap == nil {
		t.Fatal("no snapshot for the vault entry; the re-point dropped the block's own transfer")
	}
	if snap.Direction != DirectionIn || snap.TxHash != centrifugeTxHash.Hex() ||
		snap.TxAmount == nil || snap.TxAmount.Cmp(big.NewInt(250)) != 0 {
		t.Errorf("snapshot = (%q, %q, %v), want the event row (in, %s, 250) — the route swapped before buildSnapshots read it",
			snap.Direction, snap.TxHash, snap.TxAmount, centrifugeTxHash.Hex())
	}

	vaultKey := EntryKey{ContractAddress: groveJAAAVault, WalletAddress: groveProxy}
	if got := f.svc.entryKeyFor(&TransferEvent{TokenAddress: groveJAAAShare, ProxyAddress: groveProxy}); got != vaultKey {
		t.Errorf("entryKeyFor(new share) = %v, want %v — the event path did not adopt the alias", got, vaultKey)
	}
}

// TestSweep_TwoEntriesSwappingSharesKeepBothRoutes: both entries must release
// before either claims, or the batch fails or passes on entry order — and a
// failing sweep never resets the counter, so the worker wedges.
func TestSweep_TwoEntriesSwappingSharesKeepBothRoutes(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{
		groveVaultShape(groveJAAAVault, groveJAAAShare),
		groveVaultShape(groveJTRSYVault, sparkJTRSYShare),
	}, 1)
	if err := f.consume(t); err != nil {
		t.Fatalf("first sweep: %v", err)
	}

	f.repointShare(t, groveJAAAVault, sparkJTRSYShare)
	f.repointShare(t, groveJTRSYVault, groveJAAAShare)

	if err := f.consume(t); err != nil {
		t.Fatalf("a swap between two entries must not fail: %v", err)
	}
	for _, tc := range []struct {
		emitter  common.Address
		contract common.Address
	}{
		{sparkJTRSYShare, groveJAAAVault},
		{groveJAAAShare, groveJTRSYVault},
	} {
		want := EntryKey{ContractAddress: tc.contract, WalletAddress: groveProxy}
		if got := f.svc.entryKeyFor(&TransferEvent{TokenAddress: tc.emitter, ProxyAddress: groveProxy}); got != want {
			t.Errorf("entryKeyFor(%s) = %v, want %v", tc.emitter.Hex(), got, want)
		}
	}
}

// TestSweep_ShareDowngradedToTheEntryItself_ReturnsError: share() reverting is how
// a direct share is detected, so a transient failure reads as one and would re-key
// the position onto its vault, where no price attaches.
func TestSweep_ShareDowngradedToTheEntryItself_ReturnsError(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{groveVaultShape(groveJAAAVault, groveJAAAShare)}, 1)
	if err := f.consume(t); err != nil {
		t.Fatalf("first sweep: %v", err)
	}
	batchesBefore := len(f.handler.batches)

	f.repointShare(t, groveJAAAVault, groveJAAAVault)

	err := f.consume(t)
	if err == nil {
		t.Fatal("a vault reporting itself as its own share must fail the block")
	}
	if !strings.Contains(err.Error(), "cannot become its own share") {
		t.Errorf("error = %q, want it to name the downgrade", err)
	}
	if len(f.handler.batches) != batchesBefore {
		t.Errorf("HandleBatch ran %d more times, want 0", len(f.handler.batches)-batchesBefore)
	}
}

// TestSweep_CentrifugeBalanceWithoutAShare_ReturnsError: a centrifuge source that
// stops naming the share would freeze the aliases at their last good value while
// every liveness signal stayed green.
func TestSweep_CentrifugeBalanceWithoutAShare_ReturnsError(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{groveVaultShape(groveJAAAVault, groveJAAAShare)}, 1)
	for _, bal := range f.source.result.Balances {
		bal.ShareToken = nil
	}

	err := f.consume(t)
	if err == nil {
		t.Fatal("expected a centrifuge balance with no share token to fail the block")
	}
	if !strings.Contains(err.Error(), "no share token") {
		t.Errorf("error = %q, want it to name the missing share token", err)
	}
}

// TestProcessBlock_HandlerFailureKeepsTheRoutesForTheRedelivery: the fetch of a
// block that re-points a share yields new routes, but they take effect only once
// the block has been persisted — SQS redelivers the same block, and it must match
// the same transfers it matched the first time.
func TestProcessBlock_HandlerFailureKeepsTheRoutesForTheRedelivery(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{groveVaultShape(groveJAAAVault, groveJAAAShare)}, 1000)
	stale := common.HexToAddress("0x0000000000000000000000000000000000005747")
	f.seedRoute(t, stale, f.svc.entries[0])
	staleTransfer := transferLog(stale, groveProxy, big.NewInt(250), 7)

	f.handler.err = errors.New("db down")
	if err := f.consume(t, staleTransfer); err == nil {
		t.Fatal("a failing handler must fail the block")
	}
	f.handler.batches = nil
	f.handler.err = nil

	if err := f.redeliver(t, staleTransfer); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	snap := f.snapshotFor(groveJAAAVault, groveProxy)
	if snap == nil {
		t.Fatal("the redelivered block matched nothing: the routes advanced past a block that never persisted")
	}
	if snap.TxHash != centrifugeTxHash.Hex() || snap.TxAmount == nil || snap.TxAmount.Cmp(big.NewInt(250)) != 0 {
		t.Errorf("snapshot = (%q, %v), want the event row (%s, 250)", snap.TxHash, snap.TxAmount, centrifugeTxHash.Hex())
	}

	// Persisted now, so the re-point applies: the new share's transfers match next.
	f.handler.batches = nil
	if err := f.consume(t, transferLog(groveJAAAShare, groveProxy, big.NewInt(9), 1)); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	if snap := f.snapshotFor(groveJAAAVault, groveProxy); snap == nil || snap.TxAmount == nil || snap.TxAmount.Cmp(big.NewInt(9)) != 0 {
		t.Errorf("snapshot after the persisted re-point = %v, want the new share's transfer of 9", snap)
	}
}

// TestProcessBlock_EventPath_CentrifugeBalanceWithoutAShare_ReturnsError is the
// event-path twin of the sweep test: the guard sits on the fetch every path shares,
// and the block's own transfer must not be persisted past it.
func TestProcessBlock_EventPath_CentrifugeBalanceWithoutAShare_ReturnsError(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{groveVaultShape(groveJAAAVault, groveJAAAShare)}, 1000)
	if err := f.consume(t, transferLog(groveJAAAShare, groveProxy, big.NewInt(1), 0)); err != nil {
		t.Fatalf("first block: %v", err)
	}
	batchesBefore := len(f.handler.batches)
	for _, bal := range f.source.result.Balances {
		bal.ShareToken = nil
	}

	err := f.consume(t, transferLog(groveJAAAShare, groveProxy, big.NewInt(250), 7))
	if err == nil {
		t.Fatal("expected a centrifuge balance with no share token to fail the block")
	}
	if !strings.Contains(err.Error(), "read the share tokens of block") {
		t.Errorf("error = %q, want the event path's wrapper", err)
	}
	if len(f.handler.batches) != batchesBefore {
		t.Errorf("HandleBatch ran %d more times, want 0", len(f.handler.batches)-batchesBefore)
	}
}

// TestProcessBlock_EventPath_ShareDowngradedToTheEntryItself_ReturnsError is the
// event-path twin of the sweep test for the ratchet.
func TestProcessBlock_EventPath_ShareDowngradedToTheEntryItself_ReturnsError(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{groveVaultShape(groveJAAAVault, groveJAAAShare)}, 1000)
	if err := f.consume(t, transferLog(groveJAAAShare, groveProxy, big.NewInt(1), 0)); err != nil {
		t.Fatalf("first block: %v", err)
	}
	batchesBefore := len(f.handler.batches)
	f.repointShare(t, groveJAAAVault, groveJAAAVault)

	err := f.consume(t, transferLog(groveJAAAShare, groveProxy, big.NewInt(250), 7))
	if err == nil {
		t.Fatal("a vault reporting itself as its own share must fail the block")
	}
	if !strings.Contains(err.Error(), "route transfers for block") || !strings.Contains(err.Error(), "cannot become its own share") {
		t.Errorf("error = %q, want the event path's wrapper around the downgrade", err)
	}
	if len(f.handler.batches) != batchesBefore {
		t.Errorf("HandleBatch ran %d more times, want 0", len(f.handler.batches)-batchesBefore)
	}
}

func TestProcessBlock_RejectsTwoEntriesClaimingOneShare(t *testing.T) {
	f := newCentrifugeTracker(t, []centrifugeShape{
		groveVaultShape(groveJAAAVault, groveJAAAShare),
		groveVaultShape(groveJTRSYVault, groveJAAAShare),
	}, 1000)

	err := f.consume(t, transferLog(groveJAAAShare, groveProxy, big.NewInt(250), 7))

	if err == nil {
		t.Fatal("two vaults fronting one share for one wallet must fail; tracking both double counts")
	}
	if !strings.Contains(err.Error(), "double count") {
		t.Errorf("error = %q, want it to name the double count", err)
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

	counterparty := common.HexToAddress("0xcccc")
	transfers := []*TransferEvent{
		{TokenAddress: contract, ProxyAddress: wallet, From: counterparty, To: wallet, Amount: big.NewInt(500), Direction: DirectionIn, TxHash: "0xabc", LogIndex: 3},
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
	if s.From == nil || *s.From != counterparty {
		t.Errorf("expected from %s, got %v", counterparty.Hex(), s.From)
	}
	if s.To == nil || *s.To != wallet {
		t.Errorf("expected to %s (the proxy), got %v", wallet.Hex(), s.To)
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
	if snapshots[0].From != nil || snapshots[0].To != nil {
		t.Errorf("expected nil transfer parties without a transfer, got from=%v to=%v",
			snapshots[0].From, snapshots[0].To)
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

// TestProcessBlock_RecordsLivenessMetrics asserts every consumed block advances
// one of the seeded blocks_processed_total series and emits one
// processing_duration_seconds observation carrying {chain,status}, on both the
// success and error paths. These are the
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

			if got := seededStatusCounter(t, reader, "blocks_processed_total", tt.wantStatus); got != 1 {
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
	// telemetry.SetMeterProvider, not otel's: the blocks_processed_total seed is
	// registered with OnMeterProviderReady, and only this entry point runs it.
	telemetry.SetMeterProvider(mp)
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

// seededStatusCounter asserts the counter carries exactly the two series
// telemetry.NewMetrics seeds at 0, and that only wantStatus advanced. The
// second half is the point: it proves the recorded sample landed ON the seeded
// series rather than orphaning it into a parallel one, which would leave the
// seeded series flat at 0 and defeat the alert it exists for.
func seededStatusCounter(t *testing.T, reader sdkmetric.Reader, name, wantStatus string) int64 {
	t.Helper()
	m := collectMetric(t, reader, name)
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("%s is %T, want Sum[int64]", name, m.Data)
	}
	if len(sum.DataPoints) != 2 {
		t.Fatalf("%s has %d datapoints, want 2 (the seeded success and error series)", name, len(sum.DataPoints))
	}

	var got int64
	var seen []string
	for _, dp := range sum.DataPoints {
		status, _ := dp.Attributes.Value("status")
		seen = append(seen, status.AsString())
		assertChainStatus(t, dp.Attributes, status.AsString())
		if status.AsString() == wantStatus {
			got = dp.Value
			continue
		}
		if dp.Value != 0 {
			t.Errorf("%s{status=%q} = %d, want 0", name, status.AsString(), dp.Value)
		}
	}
	if !slices.Contains(seen, wantStatus) {
		t.Fatalf("%s has series %v, none of them status=%q", name, seen, wantStatus)
	}
	return got
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
