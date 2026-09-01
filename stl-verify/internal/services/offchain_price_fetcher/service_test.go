package offchain_price_fetcher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// =============================================================================
// Test Helpers
// =============================================================================

//go:fix inline

// =============================================================================
// Mock PriceProvider
// =============================================================================

type mockPriceProvider struct {
	name               string
	supportsHistorical bool

	currentPricesFunc   func(ctx context.Context, assetIDs []string) ([]outbound.PriceData, error)
	historicalDataFunc  func(ctx context.Context, assetID string, from, to time.Time) (*outbound.HistoricalData, error)
	getCurrentPriceErr  error
	getHistoricalErr    error
	currentPrices       []outbound.PriceData
	historicalData      map[string]*outbound.HistoricalData
	getCurrentCallCount atomic.Int32
	getHistoricalCalls  []string
	mu                  sync.Mutex
}

func newMockProvider(name string, supportsHistorical bool) *mockPriceProvider {
	return &mockPriceProvider{
		name:               name,
		supportsHistorical: supportsHistorical,
		historicalData:     make(map[string]*outbound.HistoricalData),
	}
}

func (m *mockPriceProvider) Name() string {
	return m.name
}

func (m *mockPriceProvider) SupportsHistorical() bool {
	return m.supportsHistorical
}

func (m *mockPriceProvider) GetCurrentPrices(ctx context.Context, assetIDs []string) ([]outbound.PriceData, error) {
	m.getCurrentCallCount.Add(1)

	if m.currentPricesFunc != nil {
		return m.currentPricesFunc(ctx, assetIDs)
	}

	if m.getCurrentPriceErr != nil {
		return nil, m.getCurrentPriceErr
	}
	return m.currentPrices, nil
}

func (m *mockPriceProvider) GetHistoricalData(ctx context.Context, assetID string, from, to time.Time) (*outbound.HistoricalData, error) {
	m.mu.Lock()
	m.getHistoricalCalls = append(m.getHistoricalCalls, assetID)
	m.mu.Unlock()

	if m.historicalDataFunc != nil {
		return m.historicalDataFunc(ctx, assetID, from, to)
	}

	if m.getHistoricalErr != nil {
		return nil, m.getHistoricalErr
	}

	if data, ok := m.historicalData[assetID]; ok {
		return data, nil
	}

	return &outbound.HistoricalData{SourceAssetID: assetID}, nil
}

func (m *mockPriceProvider) GetHistoricalCalls() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.getHistoricalCalls))
	copy(result, m.getHistoricalCalls)
	return result
}

// =============================================================================
// Mock PriceRepository
// =============================================================================

type mockPriceRepository struct {
	source                 *entity.PriceSource
	enabledAssets          []*entity.PriceAsset
	assetsByIDs            []*entity.PriceAsset
	getSourceErr           error
	getEnabledAssetsErr    error
	getAssetsByIDsErr      error
	upsertPricesErr        error
	upsertPricesCalls      [][]*entity.TokenPrice
	upsertAssetPricesErr   error
	upsertAssetPricesCalls [][]*entity.AssetPrice
	getSourceCallCount     atomic.Int32
	getEnabledAssetsCount  atomic.Int32
	getAssetsByIDsCount    atomic.Int32
	upsertPricesCount      atomic.Int32
	mu                     sync.Mutex
}

func newMockRepository() *mockPriceRepository {
	return &mockPriceRepository{
		source: &entity.PriceSource{
			ID:   1,
			Name: "coingecko",
		},
	}
}

func (m *mockPriceRepository) GetSourceByName(ctx context.Context, name string) (*entity.PriceSource, error) {
	m.getSourceCallCount.Add(1)
	if m.getSourceErr != nil {
		return nil, m.getSourceErr
	}
	return m.source, nil
}

func (m *mockPriceRepository) GetEnabledAssets(ctx context.Context, sourceID int64) ([]*entity.PriceAsset, error) {
	m.getEnabledAssetsCount.Add(1)
	if m.getEnabledAssetsErr != nil {
		return nil, m.getEnabledAssetsErr
	}
	return m.enabledAssets, nil
}

func (m *mockPriceRepository) GetAssetsBySourceAssetIDs(ctx context.Context, sourceID int64, sourceAssetIDs []string) ([]*entity.PriceAsset, error) {
	m.getAssetsByIDsCount.Add(1)
	if m.getAssetsByIDsErr != nil {
		return nil, m.getAssetsByIDsErr
	}
	return m.assetsByIDs, nil
}

func (m *mockPriceRepository) UpsertPrices(ctx context.Context, prices []*entity.TokenPrice) error {
	m.upsertPricesCount.Add(1)
	m.mu.Lock()
	m.upsertPricesCalls = append(m.upsertPricesCalls, prices)
	m.mu.Unlock()
	if m.upsertPricesErr != nil {
		return m.upsertPricesErr
	}
	return nil
}

func (m *mockPriceRepository) UpsertAssetPrices(ctx context.Context, prices []*entity.AssetPrice) error {
	m.mu.Lock()
	m.upsertAssetPricesCalls = append(m.upsertAssetPricesCalls, prices)
	m.mu.Unlock()
	if m.upsertAssetPricesErr != nil {
		return m.upsertAssetPricesErr
	}
	return nil
}

// GetUpsertedAssetPrices flattens every UpsertAssetPrices call, because callers
// assert on what landed, not on call boundaries.
func (m *mockPriceRepository) GetUpsertedAssetPrices() []*entity.AssetPrice {
	m.mu.Lock()
	defer m.mu.Unlock()
	var all []*entity.AssetPrice
	for _, call := range m.upsertAssetPricesCalls {
		all = append(all, call...)
	}
	return all
}

func (m *mockPriceRepository) GetUpsertPricesCalls() [][]*entity.TokenPrice {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.upsertPricesCalls
}

// =============================================================================
// Test Fixtures
// =============================================================================

// pastHour is a range that is always non-empty. Tests that do not care about the
// window must not build one from two `time.Now()` calls: FetchHistoricalData
// rejects from == to, so those tests pass or fail on whether the clock ticked
// between the two calls.
func pastHour() (from, to time.Time) {
	now := time.Now()
	return now.Add(-time.Hour), now
}

func createAsset(id int64, sourceAssetID, symbol string, tokenID *int64) *entity.PriceAsset {
	return &entity.PriceAsset{
		ID:            id,
		SourceID:      1,
		SourceAssetID: sourceAssetID,
		TokenID:       tokenID,
		Name:          symbol,
		Symbol:        symbol,
		Enabled:       true,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
}

func createPriceData(assetID string, price float64, ts time.Time) outbound.PriceData {
	return outbound.PriceData{
		SourceAssetID: assetID,
		PriceUSD:      price,
		MarketCapUSD:  new(price * 1000000),
		Timestamp:     ts,
	}
}

func createHistoricalData(assetID string, prices []outbound.PricePoint, volumes []outbound.VolumePoint, marketCaps []outbound.MarketCapPoint) *outbound.HistoricalData {
	return &outbound.HistoricalData{
		SourceAssetID: assetID,
		Prices:        prices,
		Volumes:       volumes,
		MarketCaps:    marketCaps,
	}
}

// singlePricePoint is the minimum payload that satisfies the "asset returned no
// data at all" guard, for tests whose subject is something other than the data
// (chunk arithmetic, concurrency) and which would otherwise trip it incidentally.
func singlePricePoint(assetID string, ts time.Time) *outbound.HistoricalData {
	return createHistoricalData(assetID, []outbound.PricePoint{{Timestamp: ts, PriceUSD: 100}}, nil, nil)
}

// =============================================================================
// Tests: NewService
// =============================================================================

func TestNewService_Success(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	svc, err := NewService(ServiceConfig{
		ChainID: 1,
		Logger:  testutil.DiscardLogger(),
	}, provider, repo)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc == nil {
		t.Fatal("expected service to be created")
	}
	if svc.concurrency != 5 {
		t.Errorf("expected default concurrency=5, got %d", svc.concurrency)
	}
}

func TestNewService_NilProvider(t *testing.T) {
	repo := newMockRepository()

	_, err := NewService(ServiceConfig{ChainID: 1}, nil, repo)

	if err == nil {
		t.Fatal("expected error for nil provider")
	}
	if err.Error() != "provider cannot be nil" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNewService_NilRepo(t *testing.T) {
	provider := newMockProvider("coingecko", true)

	_, err := NewService(ServiceConfig{ChainID: 1}, provider, nil)

	if err == nil {
		t.Fatal("expected error for nil repo")
	}
	if err.Error() != "repo cannot be nil" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNewService_InvalidChainID(t *testing.T) {
	tests := []struct {
		name    string
		chainID int
	}{
		{"zero", 0},
		{"negative", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := newMockProvider("coingecko", true)
			repo := newMockRepository()

			_, err := NewService(ServiceConfig{ChainID: tt.chainID}, provider, repo)

			if err == nil {
				t.Fatal("expected error for invalid chainID")
			}
		})
	}
}

func TestNewService_DefaultLogger(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	svc, err := NewService(ServiceConfig{
		ChainID: 1,
		Logger:  nil, // Should use default
	}, provider, repo)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc.logger == nil {
		t.Error("expected logger to be set")
	}
}

func TestNewService_CustomConcurrency(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	svc, err := NewService(ServiceConfig{
		ChainID:     1,
		Concurrency: 10,
		Logger:      testutil.DiscardLogger(),
	}, provider, repo)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc.concurrency != 10 {
		t.Errorf("expected concurrency=10, got %d", svc.concurrency)
	}
}

// =============================================================================
// Tests: FetchCurrentPrices
// =============================================================================

func TestFetchCurrentPrices_Success(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	ts := time.Now().Truncate(time.Second)
	provider.currentPrices = []outbound.PriceData{
		createPriceData("weth", 2500.0, ts),
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchCurrentPrices(context.Background(), nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.upsertPricesCount.Load() != 1 {
		t.Errorf("expected 1 upsert call, got %d", repo.upsertPricesCount.Load())
	}

	calls := repo.GetUpsertPricesCalls()
	if len(calls) != 1 || len(calls[0]) != 1 {
		t.Fatalf("expected 1 price to be upserted")
	}
	if calls[0][0].PriceUSD != 2500.0 {
		t.Errorf("expected price=2500.0, got %f", calls[0][0].PriceUSD)
	}
}

func TestFetchCurrentPrices_WithSpecificAssetIDs(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.assetsByIDs = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	ts := time.Now().Truncate(time.Second)
	provider.currentPrices = []outbound.PriceData{
		createPriceData("weth", 2500.0, ts),
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchCurrentPrices(context.Background(), []string{"weth"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.getAssetsByIDsCount.Load() != 1 {
		t.Error("expected GetAssetsBySourceAssetIDs to be called")
	}
	if repo.getEnabledAssetsCount.Load() != 0 {
		t.Error("GetEnabledAssets should not be called when specific IDs provided")
	}
}

func TestFetchCurrentPrices_NoAssets(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()
	repo.enabledAssets = []*entity.PriceAsset{} // Empty

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchCurrentPrices(context.Background(), nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if provider.getCurrentCallCount.Load() != 0 {
		t.Error("provider should not be called when no assets")
	}
}

func TestFetchCurrentPrices_ResolveAssetsFails(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()
	repo.getSourceErr = errors.New("database error")

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchCurrentPrices(context.Background(), nil)

	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, repo.getSourceErr) {
		t.Errorf("expected wrapped database error, got: %v", err)
	}
}

func TestFetchCurrentPrices_ProviderFails(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}
	provider.getCurrentPriceErr = errors.New("API error")

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchCurrentPrices(context.Background(), nil)

	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, provider.getCurrentPriceErr) {
		t.Errorf("expected wrapped API error, got: %v", err)
	}
}

func TestFetchCurrentPrices_UnknownAssetInPriceData(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	ts := time.Now().Truncate(time.Second)
	provider.currentPrices = []outbound.PriceData{
		createPriceData("unknown-asset", 100.0, ts), // Asset not in our list
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchCurrentPrices(context.Background(), nil)

	if err == nil {
		t.Fatal("expected error for unknown asset")
	}
}

func TestFetchCurrentPrices_AssetWithoutTokenID(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	// One asset with tokenID, one without
	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
		createAsset(2, "unmapped", "UNM", nil), // No token mapping
	}

	ts := time.Now().Truncate(time.Second)
	provider.currentPrices = []outbound.PriceData{
		createPriceData("weth", 2500.0, ts),
		createPriceData("unmapped", 1.0, ts),
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchCurrentPrices(context.Background(), nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Each asset routes to its own store: the mapped one to the token table,
	// the token-less one to the asset table.
	calls := repo.GetUpsertPricesCalls()
	if len(calls) != 1 || len(calls[0]) != 1 {
		t.Fatalf("expected 1 token-keyed price")
	}
	if calls[0][0].TokenID != 100 {
		t.Error("expected the mapped token in the token store")
	}
	assetPrices := repo.GetUpsertedAssetPrices()
	if len(assetPrices) != 1 {
		t.Fatalf("expected 1 asset-keyed price, got %d", len(assetPrices))
	}
	if assetPrices[0].AssetID != 2 {
		t.Errorf("expected asset_id 2 in the asset store, got %d", assetPrices[0].AssetID)
	}
}

func TestFetchCurrentPrices_AllAssetsUnmapped(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "unmapped", "UNM", nil), // No token mapping
	}

	ts := time.Now().Truncate(time.Second)
	provider.currentPrices = []outbound.PriceData{
		createPriceData("unmapped", 1.0, ts),
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchCurrentPrices(context.Background(), nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assetPrices := repo.GetUpsertedAssetPrices()
	if len(assetPrices) != 1 {
		t.Fatalf("expected the token-less asset's price in the asset store, got %d", len(assetPrices))
	}
	for _, call := range repo.GetUpsertPricesCalls() {
		if len(call) != 0 {
			t.Error("expected no token-keyed prices for a token-less asset")
		}
	}
}

func TestFetchCurrentPrices_UpsertFails(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}
	repo.upsertPricesErr = errors.New("database write error")

	ts := time.Now().Truncate(time.Second)
	provider.currentPrices = []outbound.PriceData{
		createPriceData("weth", 2500.0, ts),
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchCurrentPrices(context.Background(), nil)

	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, repo.upsertPricesErr) {
		t.Errorf("expected wrapped database error, got: %v", err)
	}
}

func TestFetchCurrentPrices_InvalidPriceData(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	// Negative price should fail entity validation
	provider.currentPrices = []outbound.PriceData{
		{
			SourceAssetID: "weth",
			PriceUSD:      -100.0, // Invalid
			Timestamp:     time.Now(),
		},
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchCurrentPrices(context.Background(), nil)

	if err == nil {
		t.Fatal("expected error for invalid price")
	}
}

func TestFetchCurrentPrices_ZeroTimestamp(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	// Zero timestamp should fail entity validation
	provider.currentPrices = []outbound.PriceData{
		{
			SourceAssetID: "weth",
			PriceUSD:      2500.0,
			Timestamp:     time.Time{}, // Zero
		},
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchCurrentPrices(context.Background(), nil)

	if err == nil {
		t.Fatal("expected error for zero timestamp")
	}
}

func TestFetchCurrentPrices_ContextCancelled(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	provider.currentPricesFunc = func(ctx context.Context, assetIDs []string) ([]outbound.PriceData, error) {
		return nil, ctx.Err()
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := svc.FetchCurrentPrices(ctx, nil)

	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

// =============================================================================
// Tests: FetchHistoricalData
// =============================================================================

func TestFetchHistoricalData_Success(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	ts := time.Now().Truncate(time.Hour)
	provider.historicalData["weth"] = createHistoricalData("weth",
		[]outbound.PricePoint{{Timestamp: ts, PriceUSD: 2500.0}},
		[]outbound.VolumePoint{{Timestamp: ts, VolumeUSD: 1000000.0}},
		[]outbound.MarketCapPoint{{Timestamp: ts, MarketCapUSD: 5000000000.0}},
	)

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	from := time.Now().AddDate(0, 0, -7)
	to := time.Now()

	err := svc.FetchHistoricalData(context.Background(), nil, from, to)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.upsertPricesCount.Load() != 1 {
		t.Errorf("expected 1 upsert prices call, got %d", repo.upsertPricesCount.Load())
	}

	// Volume should be merged into the price entity
	calls := repo.GetUpsertPricesCalls()
	if len(calls) > 0 && len(calls[0]) > 0 {
		if calls[0][0].VolumeUSD == nil {
			t.Error("expected volume to be set on price entity")
		} else if *calls[0][0].VolumeUSD != 1000000.0 {
			t.Errorf("expected volume=1000000.0, got %f", *calls[0][0].VolumeUSD)
		}
	}
}

func TestFetchHistoricalData_ProviderDoesNotSupportHistorical(t *testing.T) {
	provider := newMockProvider("limited-provider", false) // Does not support historical
	repo := newMockRepository()

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	from, to := pastHour()
	err := svc.FetchHistoricalData(context.Background(), nil, from, to)

	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "provider limited-provider does not support historical data" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestFetchHistoricalData_NoAssets(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()
	repo.enabledAssets = []*entity.PriceAsset{} // Empty

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	from, to := pastHour()
	err := svc.FetchHistoricalData(context.Background(), nil, from, to)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	calls := provider.GetHistoricalCalls()
	if len(calls) != 0 {
		t.Error("provider should not be called when no assets")
	}
}

func TestFetchHistoricalData_ResolveAssetsFails(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()
	repo.getSourceErr = errors.New("database error")

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	from, to := pastHour()
	err := svc.FetchHistoricalData(context.Background(), nil, from, to)

	if err == nil {
		t.Fatal("expected error")
	}
	// Without naming the injected failure this passes on any error, including the
	// range guard firing before resolveAssets is ever reached.
	if !strings.Contains(err.Error(), "database error") {
		t.Errorf("error should carry the injected resolve failure, got: %v", err)
	}
}

func TestFetchHistoricalData_AssetWithoutTokenID(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	from := time.Now().AddDate(0, 0, -1)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(9, "ripple", "XRP", nil), // No token mapping
	}
	provider.historicalData["ripple"] = singlePricePoint("ripple", from)

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, from, time.Now())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls := provider.GetHistoricalCalls(); len(calls) == 0 {
		t.Fatal("expected the token-less asset to be fetched, not skipped")
	}
	assetPrices := repo.GetUpsertedAssetPrices()
	if len(assetPrices) != 1 {
		t.Fatalf("expected 1 asset-keyed price, got %d", len(assetPrices))
	}
	if assetPrices[0].AssetID != 9 {
		t.Errorf("expected asset_id 9, got %d", assetPrices[0].AssetID)
	}
}

func TestFetchHistoricalData_MultipleAssets(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID1 := int64(100)
	tokenID2 := int64(101)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID1),
		createAsset(2, "usdc", "USDC", &tokenID2),
	}

	ts := time.Now().Truncate(time.Hour)
	provider.historicalData["weth"] = createHistoricalData("weth",
		[]outbound.PricePoint{{Timestamp: ts, PriceUSD: 2500.0}},
		[]outbound.VolumePoint{{Timestamp: ts, VolumeUSD: 1000000.0}},
		nil,
	)
	provider.historicalData["usdc"] = createHistoricalData("usdc",
		[]outbound.PricePoint{{Timestamp: ts, PriceUSD: 1.0}},
		[]outbound.VolumePoint{{Timestamp: ts, VolumeUSD: 500000.0}},
		nil,
	)

	svc, _ := NewService(ServiceConfig{ChainID: 1, Concurrency: 2, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -7), time.Now())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := provider.GetHistoricalCalls()
	if len(calls) != 2 {
		t.Errorf("expected 2 provider calls, got %d", len(calls))
	}
}

func TestFetchHistoricalData_PartialFailure(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID1 := int64(100)
	tokenID2 := int64(101)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID1),
		createAsset(2, "failing-asset", "FAIL", &tokenID2),
	}

	ts := time.Now().Truncate(time.Hour)
	provider.historicalData["weth"] = createHistoricalData("weth",
		[]outbound.PricePoint{{Timestamp: ts, PriceUSD: 2500.0}},
		nil, nil,
	)

	provider.historicalDataFunc = func(ctx context.Context, assetID string, from, to time.Time) (*outbound.HistoricalData, error) {
		if assetID == "failing-asset" {
			return nil, errors.New("API error for this asset")
		}
		return provider.historicalData[assetID], nil
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Concurrency: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -7), time.Now())

	if err == nil {
		t.Fatal("expected error for partial failure")
	}
}

func TestFetchHistoricalData_ChunkingOver30Days(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	callCount := 0
	provider.historicalDataFunc = func(ctx context.Context, assetID string, from, to time.Time) (*outbound.HistoricalData, error) {
		callCount++
		ts := from.Add(time.Hour)
		return createHistoricalData(assetID,
			[]outbound.PricePoint{{Timestamp: ts, PriceUSD: 2500.0}},
			nil, nil,
		), nil
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	// 45 days should result in 2 chunks (30 + 15)
	from := time.Now().AddDate(0, 0, -45)
	to := time.Now()

	err := svc.FetchHistoricalData(context.Background(), nil, from, to)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if callCount != 2 {
		t.Errorf("expected 2 chunk calls for 45 days, got %d", callCount)
	}
}

func TestFetchHistoricalData_LessThan30Days(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	callCount := 0
	provider.historicalDataFunc = func(ctx context.Context, assetID string, from, to time.Time) (*outbound.HistoricalData, error) {
		callCount++
		ts := from.Add(time.Hour)
		return createHistoricalData(assetID,
			[]outbound.PricePoint{{Timestamp: ts, PriceUSD: 2500.0}},
			nil, nil,
		), nil
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	// 20 days should be 1 chunk (less than 30-day chunk size)
	from := time.Now().AddDate(0, 0, -20)
	to := time.Now()

	err := svc.FetchHistoricalData(context.Background(), nil, from, to)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected 1 chunk call for 20 days, got %d", callCount)
	}
}

func TestFetchHistoricalData_UpsertPricesFails(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}
	repo.upsertPricesErr = errors.New("database error")

	ts := time.Now().Truncate(time.Hour)
	provider.historicalData["weth"] = createHistoricalData("weth",
		[]outbound.PricePoint{{Timestamp: ts, PriceUSD: 2500.0}},
		nil, nil,
	)

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -1), time.Now())

	if err == nil {
		t.Fatal("expected error")
	}
}

// Every rejection path of BackfillChunk, and the sentinel that lets a caller with
// a retry budget tell "this will never succeed" from "try again".
func TestBackfillChunk_RejectsRequestsThatCannotSucceed(t *testing.T) {
	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tokenID := int64(100)

	tests := []struct {
		name               string
		supportsHistorical bool
		asset              string
		tokenID            *int64
		from, to           time.Time
		wantErrContains    string
	}{
		{
			name:               "provider cannot serve history at all",
			supportsHistorical: false,
			asset:              "weth",
			tokenID:            &tokenID,
			from:               from,
			to:                 from.Add(24 * time.Hour),
			wantErrContains:    "does not support historical data",
		},
		{
			name:               "range spans no time",
			supportsHistorical: true,
			asset:              "weth",
			tokenID:            &tokenID,
			from:               from,
			to:                 from,
			wantErrContains:    "must be before",
		},
		{
			name:               "range is inverted",
			supportsHistorical: true,
			asset:              "weth",
			tokenID:            &tokenID,
			from:               from.Add(24 * time.Hour),
			to:                 from,
			wantErrContains:    "must be before",
		},
		{
			name:               "window past the hourly ceiling would silently return daily",
			supportsHistorical: true,
			asset:              "weth",
			tokenID:            &tokenID,
			from:               from,
			to:                 from.Add(MaxHourlyWindow + time.Hour),
			wantErrContains:    "ceiling for hourly data",
		},
		{
			name:               "asset is not registered for this source",
			supportsHistorical: true,
			asset:              "not-a-coin",
			tokenID:            &tokenID,
			from:               from,
			to:                 from.Add(24 * time.Hour),
			wantErrContains:    "unknown source asset IDs",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := newMockProvider("coingecko", tc.supportsHistorical)
			provider.historicalDataFunc = func(_ context.Context, id string, f, _ time.Time) (*outbound.HistoricalData, error) {
				return singlePricePoint(id, f), nil
			}
			repo := newMockRepository()
			repo.assetsByIDs = []*entity.PriceAsset{createAsset(1, "weth", "WETH", tc.tokenID)}

			svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

			stored, err := svc.BackfillChunk(context.Background(), tc.asset, tc.from, tc.to)

			if err == nil {
				t.Fatalf("expected an error, got stored=%d", stored)
			}
			if !strings.Contains(err.Error(), tc.wantErrContains) {
				t.Errorf("error = %q, want it to contain %q", err, tc.wantErrContains)
			}
			// Every one of these is deterministic, so a caller with a retry budget
			// must be able to recognise it and stop.
			if !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("error should wrap ErrInvalidRequest so retrying callers fail fast: %v", err)
			}
			if stored != 0 {
				t.Errorf("stored = %d on a rejected request, want 0", stored)
			}
		})
	}
}

// The ceiling is a maximum, not a forbidden value: exactly MaxHourlyWindow still
// returns hourly data, and rejecting it would refuse a valid request. Without
// this case, flipping the guard from `>` to `>=` passes the whole suite.
func TestBackfillChunk_AcceptsAWindowExactlyAtTheHourlyCeiling(t *testing.T) {
	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tokenID := int64(100)

	provider := newMockProvider("coingecko", true)
	provider.historicalDataFunc = func(_ context.Context, id string, f, _ time.Time) (*outbound.HistoricalData, error) {
		return singlePricePoint(id, f), nil
	}
	repo := newMockRepository()
	repo.assetsByIDs = []*entity.PriceAsset{createAsset(1, "weth", "WETH", &tokenID)}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	stored, err := svc.BackfillChunk(context.Background(), "weth", from, from.Add(MaxHourlyWindow))

	if err != nil {
		t.Fatalf("a window of exactly MaxHourlyWindow must be accepted: %v", err)
	}
	if stored != 1 {
		t.Errorf("stored = %d, want 1", stored)
	}
}

// Transient faults must stay retryable. ErrInvalidRequest makes a Temporal
// activity non-retryable, so tagging a provider outage or a database blip with it
// would turn one bad minute into a permanently failed backfill. Without this,
// wrapping either error in ErrInvalidRequest passes the suite.
func TestBackfillChunk_TokenlessAssetStoresToAssetPrices(t *testing.T) {
	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	provider := newMockProvider("coingecko", true)
	provider.historicalDataFunc = func(_ context.Context, id string, f, _ time.Time) (*outbound.HistoricalData, error) {
		return singlePricePoint(id, f), nil
	}
	repo := newMockRepository()
	repo.assetsByIDs = []*entity.PriceAsset{createAsset(3, "ripple", "XRP", nil)}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	stored, err := svc.BackfillChunk(context.Background(), "ripple", from, from.Add(24*time.Hour))

	if err != nil {
		t.Fatalf("a token-less asset must be backfillable: %v", err)
	}
	if stored != 1 {
		t.Errorf("stored = %d, want 1", stored)
	}
	assetPrices := repo.GetUpsertedAssetPrices()
	if len(assetPrices) != 1 || assetPrices[0].AssetID != 3 {
		t.Fatalf("expected 1 asset-keyed price for asset_id 3, got %v", assetPrices)
	}
}

func TestBackfillChunk_KeepsTransientFailuresRetryable(t *testing.T) {
	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tokenID := int64(100)

	tests := []struct {
		name    string
		failure func(*mockPriceProvider, *mockPriceRepository)
	}{
		{
			name: "provider is unreachable",
			failure: func(p *mockPriceProvider, _ *mockPriceRepository) {
				// Replaces the stub rather than setting getHistoricalErr, which
				// the mock only consults when no func is installed.
				p.historicalDataFunc = func(context.Context, string, time.Time, time.Time) (*outbound.HistoricalData, error) {
					return nil, errors.New("connection reset by peer")
				}
			},
		},
		{
			name: "the upsert fails",
			failure: func(_ *mockPriceProvider, r *mockPriceRepository) {
				r.upsertPricesErr = errors.New("deadlock detected")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := newMockProvider("coingecko", true)
			provider.historicalDataFunc = func(_ context.Context, id string, f, _ time.Time) (*outbound.HistoricalData, error) {
				return singlePricePoint(id, f), nil
			}
			repo := newMockRepository()
			repo.assetsByIDs = []*entity.PriceAsset{createAsset(1, "weth", "WETH", &tokenID)}
			tc.failure(provider, repo)

			svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

			stored, err := svc.BackfillChunk(context.Background(), "weth", from, from.Add(24*time.Hour))

			if err == nil {
				t.Fatal("expected the failure to propagate")
			}
			if errors.Is(err, ErrInvalidRequest) {
				t.Errorf("a transient failure must NOT wrap ErrInvalidRequest, or Temporal "+
					"gives up after one attempt: %v", err)
			}
			if stored != 0 {
				t.Errorf("stored = %d on a failed chunk, want 0", stored)
			}
		})
	}
}

// A request the provider itself refused (401, 403, 404) cannot succeed on retry,
// so it has to reach the caller as ErrInvalidRequest. Otherwise a revoked API key
// costs the full retry budget on every chunk before surfacing.
func TestBackfillChunk_TreatsAProviderRejectionAsNonRetryable(t *testing.T) {
	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tokenID := int64(100)

	provider := newMockProvider("coingecko", true)
	provider.getHistoricalErr = fmt.Errorf("API error (HTTP 401): invalid api key: %w", outbound.ErrRequestRejected)
	repo := newMockRepository()
	repo.assetsByIDs = []*entity.PriceAsset{createAsset(1, "weth", "WETH", &tokenID)}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	_, err := svc.BackfillChunk(context.Background(), "weth", from, from.Add(24*time.Hour))

	if err == nil {
		t.Fatal("expected the rejection to propagate")
	}
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("a provider rejection must wrap ErrInvalidRequest so the first attempt is the last: %v", err)
	}
}

func TestBackfillChunk_StoresAndCountsAServedWindow(t *testing.T) {
	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tokenID := int64(100)

	provider := newMockProvider("coingecko", true)
	provider.historicalDataFunc = func(_ context.Context, id string, f, _ time.Time) (*outbound.HistoricalData, error) {
		return createHistoricalData(id, []outbound.PricePoint{
			{Timestamp: f, PriceUSD: 100},
			{Timestamp: f.Add(time.Hour), PriceUSD: 101},
		}, nil, nil), nil
	}
	repo := newMockRepository()
	repo.assetsByIDs = []*entity.PriceAsset{createAsset(1, "weth", "WETH", &tokenID)}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	stored, err := svc.BackfillChunk(context.Background(), "weth", from, from.Add(24*time.Hour))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stored != 2 {
		t.Errorf("stored = %d, want 2", stored)
	}
	if repo.upsertPricesCount.Load() != 1 {
		t.Errorf("UpsertPrices called %d times, want 1", repo.upsertPricesCount.Load())
	}
}

// An empty window is not an error here: only the orchestrator sees every chunk,
// so only it can tell a coverage boundary from a real hole.
func TestBackfillChunk_ReportsAnEmptyWindowWithoutError(t *testing.T) {
	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	tokenID := int64(100)

	provider := newMockProvider("coingecko", true)
	provider.historicalDataFunc = func(_ context.Context, id string, _, _ time.Time) (*outbound.HistoricalData, error) {
		return createHistoricalData(id, nil, nil, nil), nil
	}
	repo := newMockRepository()
	repo.assetsByIDs = []*entity.PriceAsset{createAsset(1, "weth", "WETH", &tokenID)}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	stored, err := svc.BackfillChunk(context.Background(), "weth", from, from.Add(24*time.Hour))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stored != 0 {
		t.Errorf("stored = %d, want 0", stored)
	}
	if repo.upsertPricesCount.Load() != 0 {
		t.Error("should not upsert an empty window")
	}
}

// A hand-triggered backfill names its assets explicitly, so a mistyped ID must
// fail loudly. It resolves to zero rows, which would otherwise be indistinguishable
// from a clean run that had nothing to do.
func TestFetchHistoricalData_ErrorsOnUnknownRequestedAssetID(t *testing.T) {
	tests := []struct {
		name      string
		requested []string
		wantErr   bool
	}{
		{name: "all requested IDs known", requested: []string{"weth"}, wantErr: false},
		{name: "one unknown ID among known", requested: []string{"weth", "not-a-coin"}, wantErr: true},
		{name: "every requested ID unknown", requested: []string{"not-a-coin"}, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			provider := newMockProvider("coingecko", true)
			repo := newMockRepository()

			tokenID := int64(100)
			known := createAsset(1, "weth", "WETH", &tokenID)
			// The mock ignores the requested IDs and returns this set, mirroring
			// the real query returning only rows that actually exist.
			repo.enabledAssets = []*entity.PriceAsset{known}
			repo.assetsByIDs = []*entity.PriceAsset{known}

			provider.historicalDataFunc = func(_ context.Context, assetID string, from, _ time.Time) (*outbound.HistoricalData, error) {
				return singlePricePoint(assetID, from), nil
			}

			svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

			err := svc.FetchHistoricalData(context.Background(), tc.requested, time.Now().AddDate(0, 0, -1), time.Now())

			if !tc.wantErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}

			if err == nil {
				t.Fatal("expected an error for an unregistered source asset ID")
			}
			// Asserting only err != nil would survive the regression this guard
			// exists to prevent: moving the check after fetch+upsert still errors,
			// but leaves a partial write behind first.
			if !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("error must wrap ErrInvalidRequest so the caller fails fast, got: %v", err)
			}
			if !strings.Contains(err.Error(), "not-a-coin") {
				t.Errorf("error should name the unresolved ID, got: %v", err)
			}
			if calls := provider.GetHistoricalCalls(); len(calls) != 0 {
				t.Errorf("provider was called %d times before the ID check; nothing may be fetched or written", len(calls))
			}
			if n := repo.upsertPricesCount.Load(); n != 0 {
				t.Errorf("repository upserted %d times for a rejected request, want 0", n)
			}
		})
	}
}

// An asset that yields nothing across the whole range is a failure, not an empty
// result: CoinGecko answers an unknown asset ID or an out-of-entitlement window
// with HTTP 200 and empty arrays, so reporting success here would silently claim
// a backfill that wrote no rows.
func TestFetchHistoricalData_ErrorsWhenAssetReturnsNoDataAtAll(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	provider.historicalData["weth"] = createHistoricalData("weth", nil, nil, nil)

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -1), time.Now())

	if err == nil {
		t.Fatal("expected an error when the provider returns no data points for the entire range")
	}
	if repo.upsertPricesCount.Load() != 0 {
		t.Error("should not upsert when no prices")
	}
}

// A single empty chunk is legitimate — an asset listed part-way through the range
// has no data before its listing date — so it must warn rather than fail, as long
// as some other chunk delivered data.
func TestFetchHistoricalData_ToleratesEmptyChunkWhenOtherChunksHaveData(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	// 90 days spans three 30-day chunks; only the last one returns data.
	call := 0
	provider.historicalDataFunc = func(_ context.Context, assetID string, from, _ time.Time) (*outbound.HistoricalData, error) {
		call++
		if call < 3 {
			return createHistoricalData(assetID, nil, nil, nil), nil
		}
		return singlePricePoint(assetID, from), nil
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -90), time.Now())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.upsertPricesCount.Load() == 0 {
		t.Error("expected the non-empty chunks to be upserted")
	}
}

func TestFetchHistoricalData_MarketCapMatching(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	ts := time.Now().Truncate(time.Hour)
	provider.historicalData["weth"] = createHistoricalData("weth",
		[]outbound.PricePoint{{Timestamp: ts, PriceUSD: 2500.0}},
		nil,
		[]outbound.MarketCapPoint{{Timestamp: ts, MarketCapUSD: 5000000000.0}},
	)

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -1), time.Now())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := repo.GetUpsertPricesCalls()
	if len(calls) == 0 || len(calls[0]) == 0 {
		t.Fatal("expected prices to be stored")
	}

	// Market cap should be matched
	if calls[0][0].MarketCapUSD == nil {
		t.Error("expected market cap to be set")
	} else if *calls[0][0].MarketCapUSD != 5000000000.0 {
		t.Errorf("expected market cap=5000000000, got %f", *calls[0][0].MarketCapUSD)
	}
}

func TestFetchHistoricalData_InvalidHistoricalPriceData(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	// Negative price
	provider.historicalData["weth"] = createHistoricalData("weth",
		[]outbound.PricePoint{{Timestamp: time.Now(), PriceUSD: -100.0}},
		nil, nil,
	)

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -1), time.Now())

	if err == nil {
		t.Fatal("expected error for invalid price data")
	}
}

func TestFetchHistoricalData_UnknownAssetInHistoricalData(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	// Provider returns data for wrong asset ID
	provider.historicalDataFunc = func(ctx context.Context, assetID string, from, to time.Time) (*outbound.HistoricalData, error) {
		return createHistoricalData("wrong-asset-id",
			[]outbound.PricePoint{{Timestamp: time.Now(), PriceUSD: 100.0}},
			nil, nil,
		), nil
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -1), time.Now())

	if err == nil {
		t.Fatal("expected error for unknown asset in historical data")
	}
}

func TestFetchHistoricalData_ConcurrencyLimit(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	// Create many assets
	var assets []*entity.PriceAsset
	for i := 1; i <= 10; i++ {
		tokenID := int64(100 + i)
		assets = append(assets, createAsset(int64(i), fmt.Sprintf("asset%02d", i), fmt.Sprintf("A%02d", i), &tokenID))
	}
	repo.enabledAssets = assets

	var maxConcurrent atomic.Int32
	var currentConcurrent atomic.Int32

	provider.historicalDataFunc = func(ctx context.Context, assetID string, from, to time.Time) (*outbound.HistoricalData, error) {
		current := currentConcurrent.Add(1)
		defer currentConcurrent.Add(-1)

		// Track max
		for {
			max := maxConcurrent.Load()
			if current <= max || maxConcurrent.CompareAndSwap(max, current) {
				break
			}
		}

		time.Sleep(10 * time.Millisecond) // Simulate work
		return singlePricePoint(assetID, from), nil
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Concurrency: 3, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -1), time.Now())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Max concurrent should not exceed 3
	if maxConcurrent.Load() > 3 {
		t.Errorf("concurrency exceeded limit: max was %d, expected <= 3", maxConcurrent.Load())
	}
}

// =============================================================================
// Tests: Helper Functions
// =============================================================================

func TestExtractSourceAssetIDs(t *testing.T) {
	assets := []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", nil),
		createAsset(2, "usdc", "USDC", nil),
		createAsset(3, "dai", "DAI", nil),
	}

	ids := extractSourceAssetIDs(assets)

	if len(ids) != 3 {
		t.Fatalf("expected 3 IDs, got %d", len(ids))
	}
	if ids[0] != "weth" || ids[1] != "usdc" || ids[2] != "dai" {
		t.Errorf("unexpected IDs: %v", ids)
	}
}

func TestExtractSourceAssetIDs_Empty(t *testing.T) {
	ids := extractSourceAssetIDs([]*entity.PriceAsset{})

	if len(ids) != 0 {
		t.Errorf("expected empty slice, got %v", ids)
	}
}

func TestBuildAssetMap(t *testing.T) {
	tokenID := int64(100)
	assets := []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
		createAsset(2, "usdc", "USDC", &tokenID),
	}

	m := buildAssetMap(assets)

	if len(m) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(m))
	}
	if m["weth"].Symbol != "WETH" {
		t.Error("weth not found or wrong")
	}
	if m["usdc"].Symbol != "USDC" {
		t.Error("usdc not found or wrong")
	}
}

func TestBuildAssetMap_Empty(t *testing.T) {
	m := buildAssetMap([]*entity.PriceAsset{})

	if len(m) != 0 {
		t.Errorf("expected empty map, got %v", m)
	}
}

// =============================================================================
// Tests: Conversion Functions (Direct)
// =============================================================================

func TestConvertHistoricalPrices_NilTokenID(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	// Asset without token_id routes to the asset-keyed slice
	asset := createAsset(7, "ripple", "XRP", nil)
	assetMap := map[string]*entity.PriceAsset{"ripple": asset}

	data := &outbound.HistoricalData{
		SourceAssetID: "ripple",
		Prices:        []outbound.PricePoint{{Timestamp: time.Now(), PriceUSD: 100.0}},
	}

	tokenPrices, assetPrices, err := svc.convertHistoricalPrices(data, assetMap)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tokenPrices != nil {
		t.Errorf("expected no token-keyed prices for asset without token_id, got %d", len(tokenPrices))
	}
	if len(assetPrices) != 1 {
		t.Fatalf("expected 1 asset-keyed price, got %d", len(assetPrices))
	}
	if assetPrices[0].AssetID != 7 {
		t.Errorf("expected asset_id 7, got %d", assetPrices[0].AssetID)
	}
}

// =============================================================================
// Tests: Edge Cases
// =============================================================================

func TestFetchCurrentPrices_LargeNumberOfPrices(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	// Create 100 assets
	var assets []*entity.PriceAsset
	var prices []outbound.PriceData
	ts := time.Now().Truncate(time.Second)

	for i := 1; i <= 100; i++ {
		tokenID := int64(100 + i)
		assetID := fmt.Sprintf("asset%02d", i)
		assets = append(assets, createAsset(int64(i), assetID, "SYM", &tokenID))
		prices = append(prices, createPriceData(assetID, float64(i)*100, ts))
	}
	repo.enabledAssets = assets
	provider.currentPrices = prices

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchCurrentPrices(context.Background(), nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := repo.GetUpsertPricesCalls()
	if len(calls) != 1 || len(calls[0]) != 100 {
		t.Errorf("expected 100 prices to be upserted")
	}
}

func TestFetchHistoricalData_VeryShortTimeRange(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	callCount := 0
	provider.historicalDataFunc = func(ctx context.Context, assetID string, from, to time.Time) (*outbound.HistoricalData, error) {
		callCount++
		return singlePricePoint(assetID, from), nil
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	// Just 1 hour
	from := time.Now().Add(-time.Hour)
	to := time.Now()

	err := svc.FetchHistoricalData(context.Background(), nil, from, to)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected 1 call for short range, got %d", callCount)
	}
}

// An empty or inverted range must error, not succeed quietly: it produces zero
// chunks, which would otherwise skip the coverage check and report a clean run
// that fetched nothing.
func TestFetchHistoricalData_RejectsEmptyOrInvertedRange(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	callCount := 0
	provider.historicalDataFunc = func(ctx context.Context, assetID string, from, to time.Time) (*outbound.HistoricalData, error) {
		callCount++
		return createHistoricalData(assetID, nil, nil, nil), nil
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	now := time.Now()
	for _, tc := range []struct {
		name     string
		from, to time.Time
	}{
		{name: "from equals to", from: now, to: now},
		{name: "from after to", from: now, to: now.AddDate(0, 0, -1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := svc.FetchHistoricalData(context.Background(), nil, tc.from, tc.to); err == nil {
				t.Fatal("expected an error for a range that spans no time")
			}
			if callCount != 0 {
				t.Errorf("provider was called %d times for a range spanning no time, want 0", callCount)
			}
		})
	}
}

func TestFetchHistoricalData_AllAssetsFail(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID1 := int64(100)
	tokenID2 := int64(101)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "asset1", "A1", &tokenID1),
		createAsset(2, "asset2", "A2", &tokenID2),
	}

	provider.getHistoricalErr = errors.New("API down")

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -1), time.Now())

	if err == nil {
		t.Fatal("expected error when all assets fail")
	}
}

func TestFetchHistoricalData_VolumesMergedIntoPrices(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	ts := time.Now().Truncate(time.Hour)
	provider.historicalData["weth"] = createHistoricalData("weth",
		[]outbound.PricePoint{{Timestamp: ts, PriceUSD: 2500.0}},
		[]outbound.VolumePoint{{Timestamp: ts, VolumeUSD: 1234567.89}},
		[]outbound.MarketCapPoint{{Timestamp: ts, MarketCapUSD: 300000000000.0}},
	)

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -1), time.Now())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	calls := repo.GetUpsertPricesCalls()
	if len(calls) != 1 || len(calls[0]) != 1 {
		t.Fatal("expected exactly 1 price to be upserted")
	}

	price := calls[0][0]
	if price.PriceUSD != 2500.0 {
		t.Errorf("PriceUSD = %f, want 2500.0", price.PriceUSD)
	}
	if price.VolumeUSD == nil {
		t.Fatal("VolumeUSD should not be nil")
	}
	if *price.VolumeUSD != 1234567.89 {
		t.Errorf("VolumeUSD = %f, want 1234567.89", *price.VolumeUSD)
	}
	if price.MarketCapUSD == nil {
		t.Fatal("MarketCapUSD should not be nil")
	}
	if *price.MarketCapUSD != 300000000000.0 {
		t.Errorf("MarketCapUSD = %f, want 300000000000.0", *price.MarketCapUSD)
	}
}
