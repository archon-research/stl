// Package offchain_price_fetcher provides a source-agnostic service for fetching and storing token prices.
package offchain_price_fetcher

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// HistoricalChunkWidth is the widest window that still returns hourly data.
//
// CoinGecko picks resolution from the width of the requested range and gives no
// signal when it downgrades: measured live, a 30-day request returns 721 hourly
// points while a multi-year request over the same span returns daily points. So
// this is a correctness bound, not a tuning knob — exceeding it silently loses
// 96% of the resolution. Exported because callers that chunk a range themselves
// (the Temporal backfill workflow) must use the same bound.
const HistoricalChunkWidth = 30 * 24 * time.Hour

// ServiceConfig holds configuration for the price fetcher service.
type ServiceConfig struct {
	// ChainID is the blockchain chain ID (e.g., 1 for Ethereum mainnet).
	ChainID int

	// Concurrency is the maximum number of assets to fetch in parallel.
	// The rate limiter in the provider ensures we stay within API limits.
	// Defaults to 5 if not set.
	Concurrency int

	// Logger is the structured logger for the service.
	Logger *slog.Logger
}

// Service orchestrates fetching prices from providers and storing them in the repository.
type Service struct {
	config      ServiceConfig
	provider    outbound.PriceProvider
	repo        outbound.PriceRepository
	logger      *slog.Logger
	concurrency int
}

// NewService creates a new price fetcher service.
func NewService(config ServiceConfig, provider outbound.PriceProvider, repo outbound.PriceRepository) (*Service, error) {
	if provider == nil {
		return nil, fmt.Errorf("provider cannot be nil")
	}
	if repo == nil {
		return nil, fmt.Errorf("repo cannot be nil")
	}
	if config.ChainID <= 0 {
		return nil, fmt.Errorf("chainID must be positive, got %d", config.ChainID)
	}

	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	concurrency := config.Concurrency
	if concurrency <= 0 {
		concurrency = 5
	}

	return &Service{
		config:      config,
		provider:    provider,
		repo:        repo,
		logger:      logger.With("component", "price-fetcher", "provider", provider.Name()),
		concurrency: concurrency,
	}, nil
}

// FetchCurrentPrices fetches and stores current prices for the specified assets.
// If assetIDs is empty, fetches prices for all enabled assets for the provider.
func (s *Service) FetchCurrentPrices(ctx context.Context, assetIDs []string) error {
	assets, err := s.resolveAssets(ctx, assetIDs)
	if err != nil {
		return fmt.Errorf("resolving assets: %w", err)
	}

	if len(assets) == 0 {
		s.logger.Info("no assets to fetch prices for")
		return nil
	}

	sourceAssetIDs := extractSourceAssetIDs(assets)
	s.logger.Info("fetching current prices", "assetCount", len(sourceAssetIDs))

	prices, err := s.provider.GetCurrentPrices(ctx, sourceAssetIDs)
	if err != nil {
		return fmt.Errorf("fetching current prices: %w", err)
	}

	tokenPrices, err := s.convertToTokenPrices(prices, assets)
	if err != nil {
		return fmt.Errorf("converting prices: %w", err)
	}
	if len(tokenPrices) == 0 {
		s.logger.Warn("no prices to store")
		return nil
	}

	if err := s.repo.UpsertPrices(ctx, tokenPrices); err != nil {
		return fmt.Errorf("storing prices: %w", err)
	}

	s.logger.Info("stored current prices", "count", len(tokenPrices))
	return nil
}

// FetchHistoricalData fetches and stores historical price and volume data for the specified assets.
// Fetches data in 30-day chunks to preserve hourly granularity from CoinGecko.
// Uses concurrent workers (controlled by ServiceConfig.Concurrency) while respecting rate limits.
func (s *Service) FetchHistoricalData(ctx context.Context, assetIDs []string, from, to time.Time) error {
	if !s.provider.SupportsHistorical() {
		return fmt.Errorf("provider %s does not support historical data", s.provider.Name())
	}

	assets, err := s.resolveAssets(ctx, assetIDs)
	if err != nil {
		return fmt.Errorf("resolving assets: %w", err)
	}

	// A caller that named assets explicitly (a backfill triggered by hand) must
	// not get a silent no-op from a mistyped ID: an unmatched ID resolves to zero
	// rows, which would otherwise look like a clean run that stored nothing.
	if err := assertRequestedAssetsResolved(assetIDs, assets); err != nil {
		return err
	}

	if len(assets) == 0 {
		s.logger.Info("no assets to fetch historical data for")
		return nil
	}

	assetMap := buildAssetMap(assets)

	s.logger.Info("fetching historical data",
		"assetCount", len(assets),
		"concurrency", s.concurrency,
		"from", from.Format(time.DateOnly),
		"to", to.Format(time.DateOnly),
	)

	// Use a semaphore pattern for bounded concurrency
	sem := make(chan struct{}, s.concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failedAssets []string

	for _, asset := range assets {
		sem <- struct{}{} // Acquire semaphore
		wg.Add(1)

		go func(asset *entity.PriceAsset) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore

			if err := s.fetchHistoricalDataForAsset(ctx, asset, assetMap, from, to); err != nil {
				s.logger.Error("failed to fetch historical data for asset",
					"asset", asset.SourceAssetID,
					"error", err,
				)
				mu.Lock()
				failedAssets = append(failedAssets, asset.SourceAssetID)
				mu.Unlock()
			}
		}(asset)
	}

	wg.Wait()

	if len(failedAssets) > 0 {
		return fmt.Errorf("failed to fetch %d/%d assets: %v", len(failedAssets), len(assets), failedAssets)
	}
	return nil
}

// BackfillChunk fetches one window for a single asset and reports how many price
// points were stored.
//
// It exists for orchestrators that chunk a long range themselves so each chunk
// can be retried and resumed independently. Unlike FetchHistoricalData it does
// not treat an empty window as an error: only the orchestrator sees every chunk,
// so only it can distinguish "this asset has no data at all" — a failure, usually
// a wrong ID or a range outside the provider's entitlement — from "this chunk
// predates the asset's listing date", which is legitimate.
func (s *Service) BackfillChunk(ctx context.Context, assetID string, from, to time.Time) (int, error) {
	if !s.provider.SupportsHistorical() {
		return 0, fmt.Errorf("provider %s does not support historical data", s.provider.Name())
	}
	if !from.Before(to) {
		return 0, fmt.Errorf("from (%s) must be before to (%s)",
			from.Format(time.RFC3339), to.Format(time.RFC3339))
	}
	if to.Sub(from) > HistoricalChunkWidth {
		return 0, fmt.Errorf("window %s to %s is %s wide, over the %s limit that still returns hourly data",
			from.Format(time.DateOnly), to.Format(time.DateOnly), to.Sub(from), HistoricalChunkWidth)
	}

	assets, err := s.resolveAssets(ctx, []string{assetID})
	if err != nil {
		return 0, fmt.Errorf("resolving asset %s: %w", assetID, err)
	}
	if err := assertRequestedAssetsResolved([]string{assetID}, assets); err != nil {
		return 0, err
	}

	asset := assets[0]
	// A caller naming one asset explicitly gets an error rather than the silent
	// skip FetchHistoricalData uses when sweeping every enabled asset: an
	// unmapped token_id here means the request cannot be satisfied at all.
	if asset.TokenID == nil {
		return 0, fmt.Errorf("asset %s has no token_id, so its prices have nowhere to go in offchain_token_price", assetID)
	}

	return s.fetchAndStoreChunk(ctx, asset, buildAssetMap(assets), from, to)
}

func (s *Service) fetchHistoricalDataForAsset(ctx context.Context, asset *entity.PriceAsset, assetMap map[string]*entity.PriceAsset, from, to time.Time) error {
	if asset.TokenID == nil {
		s.logger.Debug("skipping asset without token_id", "asset", asset.SourceAssetID)
		return nil
	}

	s.logger.Info("fetching historical data for asset",
		"asset", asset.SourceAssetID,
		"symbol", asset.Symbol,
	)

	chunkStart := from

	var chunks, stored int
	for chunkStart.Before(to) {
		chunkEnd := chunkStart.Add(HistoricalChunkWidth)
		if chunkEnd.After(to) {
			chunkEnd = to
		}

		n, err := s.fetchAndStoreChunk(ctx, asset, assetMap, chunkStart, chunkEnd)
		if err != nil {
			return fmt.Errorf("fetching chunk %s to %s: %w",
				chunkStart.Format(time.DateOnly),
				chunkEnd.Format(time.DateOnly),
				err,
			)
		}
		chunks++
		stored += n

		chunkStart = chunkEnd
	}

	// CoinGecko answers a range it cannot serve with HTTP 200 and empty arrays
	// rather than an error — an unknown asset ID, or a window older than the
	// plan's historical entitlement. Storing nothing across every chunk of a
	// non-empty range is therefore a failure, not an empty result, and must not
	// be reported as a successful backfill. Individual empty chunks stay a
	// warning: an asset listed part-way through the range legitimately has none.
	if chunks > 0 && stored == 0 {
		return fmt.Errorf("asset %s returned no data points across %d chunks covering %s to %s: "+
			"check the asset ID and that the range is within the provider's historical entitlement",
			asset.SourceAssetID, chunks,
			from.Format(time.DateOnly), to.Format(time.DateOnly))
	}

	return nil
}

// fetchAndStoreChunk returns the number of price points stored so the caller can
// tell an empty range from a filled one.
func (s *Service) fetchAndStoreChunk(ctx context.Context, asset *entity.PriceAsset, assetMap map[string]*entity.PriceAsset, from, to time.Time) (int, error) {
	s.logger.Debug("fetching chunk",
		"asset", asset.SourceAssetID,
		"from", from.Format(time.DateOnly),
		"to", to.Format(time.DateOnly),
	)

	data, err := s.provider.GetHistoricalData(ctx, asset.SourceAssetID, from, to)
	if err != nil {
		return 0, fmt.Errorf("fetching historical data: %w", err)
	}

	prices, err := s.convertHistoricalPrices(data, assetMap)
	if err != nil {
		return 0, fmt.Errorf("converting historical prices: %w", err)
	}
	if len(prices) == 0 {
		s.logger.Warn("provider returned no price points for chunk",
			"asset", asset.SourceAssetID,
			"from", from.Format(time.DateOnly),
			"to", to.Format(time.DateOnly),
		)
		return 0, nil
	}

	if err := s.repo.UpsertPrices(ctx, prices); err != nil {
		return 0, fmt.Errorf("storing prices: %w", err)
	}
	s.logger.Debug("stored prices", "count", len(prices))

	return len(prices), nil
}

// assertRequestedAssetsResolved reports the explicitly-requested source asset IDs
// that matched no row in offchain_price_asset. It is a no-op when assetIDs is
// empty, because that means "every enabled asset" rather than a specific list.
func assertRequestedAssetsResolved(assetIDs []string, resolved []*entity.PriceAsset) error {
	if len(assetIDs) == 0 {
		return nil
	}

	found := make(map[string]struct{}, len(resolved))
	for _, a := range resolved {
		found[a.SourceAssetID] = struct{}{}
	}

	var missing []string
	for _, id := range assetIDs {
		if _, ok := found[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("unknown source asset IDs %v: they are not registered in offchain_price_asset for this source", missing)
	}
	return nil
}

func (s *Service) resolveAssets(ctx context.Context, assetIDs []string) ([]*entity.PriceAsset, error) {
	source, err := s.repo.GetSourceByName(ctx, s.provider.Name())
	if err != nil {
		return nil, fmt.Errorf("getting source: %w", err)
	}

	if len(assetIDs) == 0 {
		return s.repo.GetEnabledAssets(ctx, source.ID)
	}

	return s.repo.GetAssetsBySourceAssetIDs(ctx, source.ID, assetIDs)
}

func (s *Service) convertToTokenPrices(prices []outbound.PriceData, assets []*entity.PriceAsset) ([]*entity.TokenPrice, error) {
	assetMap := buildAssetMap(assets)
	result := make([]*entity.TokenPrice, 0, len(prices))

	for _, p := range prices {
		asset, ok := assetMap[p.SourceAssetID]
		if !ok {
			return nil, fmt.Errorf("price for unknown asset: %s", p.SourceAssetID)
		}
		if asset.TokenID == nil {
			s.logger.Debug("skipping asset without token_id", "asset", p.SourceAssetID)
			continue
		}

		tp, err := entity.NewTokenPrice(
			*asset.TokenID,
			int16(asset.SourceID),
			p.PriceUSD,
			p.MarketCapUSD,
			nil,
			p.Timestamp,
		)
		if err != nil {
			return nil, fmt.Errorf("invalid price data for asset %s: %w", p.SourceAssetID, err)
		}
		result = append(result, tp)
	}

	return result, nil
}

func (s *Service) convertHistoricalPrices(data *outbound.HistoricalData, assetMap map[string]*entity.PriceAsset) ([]*entity.TokenPrice, error) {
	asset, ok := assetMap[data.SourceAssetID]
	if !ok {
		return nil, fmt.Errorf("historical data for unknown asset: %s", data.SourceAssetID)
	}
	if asset.TokenID == nil {
		return nil, nil
	}

	// Build maps of timestamps to market caps and volumes for efficient lookup
	marketCapMap := make(map[int64]float64, len(data.MarketCaps))
	for _, mc := range data.MarketCaps {
		marketCapMap[mc.Timestamp.Unix()] = mc.MarketCapUSD
	}

	volumeMap := make(map[int64]float64, len(data.Volumes))
	for _, v := range data.Volumes {
		volumeMap[v.Timestamp.Unix()] = v.VolumeUSD
	}

	result := make([]*entity.TokenPrice, 0, len(data.Prices))
	for _, p := range data.Prices {
		var marketCap *float64
		if mc, ok := marketCapMap[p.Timestamp.Unix()]; ok {
			marketCap = &mc
		}

		var volume *float64
		if v, ok := volumeMap[p.Timestamp.Unix()]; ok {
			volume = &v
		}

		tp, err := entity.NewTokenPrice(
			*asset.TokenID,
			int16(asset.SourceID),
			p.PriceUSD,
			marketCap,
			volume,
			p.Timestamp,
		)
		if err != nil {
			return nil, fmt.Errorf("invalid historical price data for asset %s: %w", data.SourceAssetID, err)
		}
		result = append(result, tp)
	}

	return result, nil
}

func extractSourceAssetIDs(assets []*entity.PriceAsset) []string {
	ids := make([]string, len(assets))
	for i, a := range assets {
		ids[i] = a.SourceAssetID
	}
	return ids
}

func buildAssetMap(assets []*entity.PriceAsset) map[string]*entity.PriceAsset {
	m := make(map[string]*entity.PriceAsset, len(assets))
	for _, a := range assets {
		m[a.SourceAssetID] = a
	}
	return m
}
