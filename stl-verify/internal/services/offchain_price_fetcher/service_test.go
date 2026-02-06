package offchain_price_fetcher

import (
	"context"
	"errors"
	"fmt"
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

func ptr[T any](v T) *T {
	return &v
}

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
	source                *entity.PriceSource
	enabledAssets         []*entity.PriceAsset
	assetsByIDs           []*entity.PriceAsset
	getSourceErr          error
	getEnabledAssetsErr   error
	getAssetsByIDsErr     error
	upsertPricesErr       error
	upsertVolumesErr      error
	upsertPricesCalls     [][]*entity.TokenPrice
	upsertVolumesCalls    [][]*entity.TokenVolume
	getSourceCallCount    atomic.Int32
	getEnabledAssetsCount atomic.Int32
	getAssetsByIDsCount   atomic.Int32
	upsertPricesCount     atomic.Int32
	upsertVolumesCount    atomic.Int32
	mu                    sync.Mutex
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

func (m *mockPriceRepository) GetLatestPrice(ctx context.Context, tokenID int64) (*entity.TokenPrice, error) {
	return nil, nil
}

func (m *mockPriceRepository) UpsertVolumes(ctx context.Context, volumes []*entity.TokenVolume) error {
	m.upsertVolumesCount.Add(1)
	m.mu.Lock()
	m.upsertVolumesCalls = append(m.upsertVolumesCalls, volumes)
	m.mu.Unlock()
	if m.upsertVolumesErr != nil {
		return m.upsertVolumesErr
	}
	return nil
}

func (m *mockPriceRepository) GetUpsertPricesCalls() [][]*entity.TokenPrice {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.upsertPricesCalls
}

func (m *mockPriceRepository) GetUpsertVolumesCalls() [][]*entity.TokenVolume {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.upsertVolumesCalls
}

// =============================================================================
// Test Fixtures
// =============================================================================

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
		MarketCapUSD:  ptr(price * 1000000),
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

	// Only the mapped asset should be stored
	calls := repo.GetUpsertPricesCalls()
	if len(calls) != 1 || len(calls[0]) != 1 {
		t.Fatalf("expected 1 price (only mapped asset)")
	}
	if calls[0][0].TokenID != 100 {
		t.Error("expected only the mapped token to be stored")
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

	// Should succeed but with warning (no prices to store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.upsertPricesCount.Load() != 0 {
		t.Error("should not call upsert when no prices to store")
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
	if repo.upsertVolumesCount.Load() != 1 {
		t.Errorf("expected 1 upsert volumes call, got %d", repo.upsertVolumesCount.Load())
	}
}

func TestFetchHistoricalData_ProviderDoesNotSupportHistorical(t *testing.T) {
	provider := newMockProvider("limited-provider", false) // Does not support historical
	repo := newMockRepository()

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now(), time.Now())

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

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now(), time.Now())

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

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now(), time.Now())

	if err == nil {
		t.Fatal("expected error")
	}
}

func TestFetchHistoricalData_AssetWithoutTokenID(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "unmapped", "UNM", nil), // No token mapping
	}

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -1), time.Now())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Provider should not be called for unmapped assets
	calls := provider.GetHistoricalCalls()
	if len(calls) != 0 {
		t.Error("should skip assets without token_id")
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
	// Should indicate which assets failed
	if !errors.Is(err, err) { // Just check it's an error
		t.Logf("error message: %v", err)
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

func TestFetchHistoricalData_UpsertVolumesFails(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}
	repo.upsertVolumesErr = errors.New("database error")

	ts := time.Now().Truncate(time.Hour)
	provider.historicalData["weth"] = createHistoricalData("weth",
		[]outbound.PricePoint{{Timestamp: ts, PriceUSD: 2500.0}},
		[]outbound.VolumePoint{{Timestamp: ts, VolumeUSD: 1000000.0}},
		nil,
	)

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -1), time.Now())

	if err == nil {
		t.Fatal("expected error")
	}
}

func TestFetchHistoricalData_EmptyPricesAndVolumes(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	// Empty data
	provider.historicalData["weth"] = createHistoricalData("weth", nil, nil, nil)

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -1), time.Now())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// No upserts should happen
	if repo.upsertPricesCount.Load() != 0 {
		t.Error("should not upsert when no prices")
	}
	if repo.upsertVolumesCount.Load() != 0 {
		t.Error("should not upsert when no volumes")
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

func TestFetchHistoricalData_InvalidVolumeData(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	tokenID := int64(100)
	repo.enabledAssets = []*entity.PriceAsset{
		createAsset(1, "weth", "WETH", &tokenID),
	}

	// Negative volume
	provider.historicalData["weth"] = createHistoricalData("weth",
		nil,
		[]outbound.VolumePoint{{Timestamp: time.Now(), VolumeUSD: -1000.0}},
		nil,
	)

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	err := svc.FetchHistoricalData(context.Background(), nil, time.Now().AddDate(0, 0, -1), time.Now())

	if err == nil {
		t.Fatal("expected error for invalid volume data")
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
		return createHistoricalData(assetID, nil, nil, nil), nil
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

	// Asset without token_id
	asset := createAsset(1, "unmapped", "UNM", nil)
	assetMap := map[string]*entity.PriceAsset{"unmapped": asset}

	data := &outbound.HistoricalData{
		SourceAssetID: "unmapped",
		Prices:        []outbound.PricePoint{{Timestamp: time.Now(), PriceUSD: 100.0}},
	}

	prices, err := svc.convertHistoricalPrices(data, assetMap)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if prices != nil {
		t.Error("expected nil prices for asset without token_id")
	}
}

func TestConvertHistoricalVolumes_NilTokenID(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	// Asset without token_id
	asset := createAsset(1, "unmapped", "UNM", nil)
	assetMap := map[string]*entity.PriceAsset{"unmapped": asset}

	data := &outbound.HistoricalData{
		SourceAssetID: "unmapped",
		Volumes:       []outbound.VolumePoint{{Timestamp: time.Now(), VolumeUSD: 1000.0}},
	}

	volumes, err := svc.convertHistoricalVolumes(data, assetMap)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if volumes != nil {
		t.Error("expected nil volumes for asset without token_id")
	}
}

func TestConvertHistoricalVolumes_UnknownAsset(t *testing.T) {
	provider := newMockProvider("coingecko", true)
	repo := newMockRepository()

	svc, _ := NewService(ServiceConfig{ChainID: 1, Logger: testutil.DiscardLogger()}, provider, repo)

	// Empty asset map - asset not found
	assetMap := map[string]*entity.PriceAsset{}

	data := &outbound.HistoricalData{
		SourceAssetID: "unknown",
		Volumes:       []outbound.VolumePoint{{Timestamp: time.Now(), VolumeUSD: 1000.0}},
	}

	_, err := svc.convertHistoricalVolumes(data, assetMap)

	if err == nil {
		t.Fatal("expected error for unknown asset")
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
		return createHistoricalData(assetID, nil, nil, nil), nil
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

func TestFetchHistoricalData_FromEqualsTo(t *testing.T) {
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
	err := svc.FetchHistoricalData(context.Background(), nil, now, now)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// from.Before(to) is false, so no chunks
	if callCount != 0 {
		t.Errorf("expected 0 calls when from=to, got %d", callCount)
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
