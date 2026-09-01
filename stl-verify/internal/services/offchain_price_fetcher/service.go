// Package offchain_price_fetcher provides a source-agnostic service for fetching and storing token prices.
package offchain_price_fetcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ErrInvalidRequest marks a request that will fail identically no matter how many
// times it is retried: a mistyped asset ID, an inverted or over-wide window. A
// caller with a retry budget (a Temporal activity) matches
// on it to fail fast, so an operator sees "you typed the ID wrong" immediately
// rather than after a retry budget has been spent on a fixed answer.
var ErrInvalidRequest = errors.New("invalid request")

// MaxHourlyWindow is the widest range CoinGecko still answers at hourly
// resolution. Past it the API silently drops to daily and gives no signal.
//
// Measured against the live Pro API on 2026-08-05 for `bitcoin` from 2020-01-01,
// which puts the boundary exactly at 90 days:
//
//	30d -> 721 pts @ 60min      89d -> 2135 pts @ 60min
//	60d -> 1441 pts @ 60min     90d -> 2159 pts @ 60min
//	91d -> 92 pts @ 1440min     100d -> 101 pts @ 1440min
//
// This is a correctness bound: exceeding it costs 96% of the resolution with no
// error to notice. Re-measure before changing it — it is an undocumented,
// unversioned property of a third-party API.
const MaxHourlyWindow = 90 * 24 * time.Hour

// HistoricalChunkWidth is the window size this package requests when walking a
// long range. Unlike MaxHourlyWindow it is a *choice*, not a limit: a third of
// the hourly ceiling, traded for finer retry granularity and a smaller working
// set per request. Callers that chunk a range themselves (the Temporal backfill
// workflow) use it so every path requests the same shape.
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

	tokenPrices, assetPrices, err := s.convertCurrentPrices(prices, assets)
	if err != nil {
		return fmt.Errorf("converting prices: %w", err)
	}
	if len(tokenPrices)+len(assetPrices) == 0 {
		s.logger.Warn("no prices to store")
		return nil
	}

	if err := s.storePrices(ctx, tokenPrices, assetPrices); err != nil {
		return err
	}

	s.logger.Info("stored current prices", "tokenKeyed", len(tokenPrices), "assetKeyed", len(assetPrices))
	return nil
}

// storePrices writes each kind to its own table, sequentially. A failure between
// the two writes propagates and fails the whole run; both upserts are idempotent
// (ON CONFLICT DO NOTHING under a build-aware version trigger), so the retry
// re-covers the half that already landed without duplicating it.
func (s *Service) storePrices(ctx context.Context, tokenPrices []*entity.TokenPrice, assetPrices []*entity.AssetPrice) error {
	if err := s.repo.UpsertPrices(ctx, tokenPrices); err != nil {
		return fmt.Errorf("storing token prices: %w", err)
	}
	if err := s.repo.UpsertAssetPrices(ctx, assetPrices); err != nil {
		return fmt.Errorf("storing asset prices: %w", err)
	}
	return nil
}

// FetchHistoricalData fetches and stores historical price and volume data for the specified assets.
// Fetches data in 30-day chunks to preserve hourly granularity from CoinGecko.
// Uses concurrent workers (controlled by ServiceConfig.Concurrency) while respecting rate limits.
func (s *Service) FetchHistoricalData(ctx context.Context, assetIDs []string, from, to time.Time) error {
	if !s.provider.SupportsHistorical() {
		return fmt.Errorf("provider %s does not support historical data", s.provider.Name())
	}
	// Without this an inverted range produces zero chunks, which skips the
	// coverage check below and returns a clean success having fetched nothing.
	if !from.Before(to) {
		return fmt.Errorf("from (%s) must be before to (%s)",
			from.Format(time.RFC3339), to.Format(time.RFC3339))
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
// points the provider returned.
//
// That is deliberately "returned", not "written": the repository upserts with
// ON CONFLICT DO NOTHING and reports no row count, so re-running a filled range
// yields the same non-zero number having inserted nothing. The count answers
// "did the provider serve this window", which is what the coverage checks need.
//
// It exists for orchestrators that chunk a long range themselves so each chunk
// can be retried and resumed independently. Unlike FetchHistoricalData it does
// not treat an empty window as an error: only the orchestrator sees every chunk,
// so only it can distinguish "this asset has no data at all" — a failure, usually
// a wrong ID or a range outside the provider's entitlement — from "this chunk
// predates the asset's listing date", which is legitimate.
func (s *Service) BackfillChunk(ctx context.Context, assetID string, from, to time.Time) (int, error) {
	if !s.provider.SupportsHistorical() {
		return 0, fmt.Errorf("provider %s does not support historical data: %w", s.provider.Name(), ErrInvalidRequest)
	}
	if !from.Before(to) {
		return 0, fmt.Errorf("from (%s) must be before to (%s): %w",
			from.Format(time.RFC3339), to.Format(time.RFC3339), ErrInvalidRequest)
	}
	// Bounded by the API's real hourly ceiling, not by the narrower chunk size this
	// package happens to request: a caller asking for anything up to 90 days still
	// gets hourly data, and rejecting that would be refusing a valid request.
	if to.Sub(from) > MaxHourlyWindow {
		return 0, fmt.Errorf("window %s to %s is %s wide, past the %s ceiling for hourly data (it would silently return daily): %w",
			from.Format(time.DateOnly), to.Format(time.DateOnly), to.Sub(from), MaxHourlyWindow, ErrInvalidRequest)
	}

	assets, err := s.resolveAssets(ctx, []string{assetID})
	if err != nil {
		return 0, fmt.Errorf("resolving asset %s: %w", assetID, err)
	}
	if err := assertRequestedAssetsResolved([]string{assetID}, assets); err != nil {
		return 0, err
	}

	return s.fetchAndStoreChunk(ctx, assets[0], buildAssetMap(assets), from, to)
}

func (s *Service) fetchHistoricalDataForAsset(ctx context.Context, asset *entity.PriceAsset, assetMap map[string]*entity.PriceAsset, from, to time.Time) error {
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

// fetchAndStoreChunk returns how many points the provider returned, so the caller
// can tell a served window from an empty one. Not a row count — see BackfillChunk.
func (s *Service) fetchAndStoreChunk(ctx context.Context, asset *entity.PriceAsset, assetMap map[string]*entity.PriceAsset, from, to time.Time) (int, error) {
	s.logger.Debug("fetching chunk",
		"asset", asset.SourceAssetID,
		"from", from.Format(time.DateOnly),
		"to", to.Format(time.DateOnly),
	)

	data, err := s.provider.GetHistoricalData(ctx, asset.SourceAssetID, from, to)
	if err != nil {
		return 0, fmt.Errorf("fetching historical data: %w", classifyProviderError(err))
	}

	tokenPrices, assetPrices, err := s.convertHistoricalPrices(data, assetMap)
	if err != nil {
		return 0, fmt.Errorf("converting historical prices: %w", err)
	}
	total := len(tokenPrices) + len(assetPrices)
	if total == 0 {
		s.logger.Warn("provider returned no price points for chunk",
			"asset", asset.SourceAssetID,
			"from", from.Format(time.DateOnly),
			"to", to.Format(time.DateOnly),
		)
		return 0, nil
	}

	if err := s.storePrices(ctx, tokenPrices, assetPrices); err != nil {
		return 0, err
	}
	s.logger.Debug("stored prices", "count", total)

	return total, nil
}

// classifyProviderError re-labels a request the provider refused outright as
// ErrInvalidRequest, so a caller with a retry budget stops on the first attempt.
//
// Without this the only fast-fail path is our own pre-flight validation, and an
// upstream verdict that cannot change — a revoked API key, a plan that does not
// cover the range, a coin ID the provider does not know — costs the full retry
// budget per chunk before surfacing, which reads like a flaky upstream rather
// than the configuration error it is.
func classifyProviderError(err error) error {
	if errors.Is(err, outbound.ErrRequestRejected) {
		return fmt.Errorf("%w: %w", err, ErrInvalidRequest)
	}
	return err
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
		return fmt.Errorf("unknown source asset IDs %v: they are not registered in offchain_price_asset for this source: %w", missing, ErrInvalidRequest)
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

// convertCurrentPrices routes each point by the asset's identity: token-keyed
// assets to TokenPrice (offchain_token_price), assets with no token row to
// AssetPrice (offchain_asset_price).
func (s *Service) convertCurrentPrices(prices []outbound.PriceData, assets []*entity.PriceAsset) ([]*entity.TokenPrice, []*entity.AssetPrice, error) {
	assetMap := buildAssetMap(assets)
	tokenPrices := make([]*entity.TokenPrice, 0, len(prices))
	var assetPrices []*entity.AssetPrice

	for _, p := range prices {
		asset, ok := assetMap[p.SourceAssetID]
		if !ok {
			return nil, nil, fmt.Errorf("price for unknown asset: %s", p.SourceAssetID)
		}

		if asset.TokenID == nil {
			ap, err := entity.NewAssetPrice(asset.ID, int16(asset.SourceID), p.PriceUSD, p.MarketCapUSD, nil, p.Timestamp)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid price data for asset %s: %w", p.SourceAssetID, err)
			}
			assetPrices = append(assetPrices, ap)
			continue
		}

		tp, err := entity.NewTokenPrice(*asset.TokenID, int16(asset.SourceID), p.PriceUSD, p.MarketCapUSD, nil, p.Timestamp)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid price data for asset %s: %w", p.SourceAssetID, err)
		}
		tokenPrices = append(tokenPrices, tp)
	}

	return tokenPrices, assetPrices, nil
}

// convertHistoricalPrices routes one asset's points by its identity — see
// convertCurrentPrices. One chunk covers one asset, so exactly one of the two
// returned slices is populated.
func (s *Service) convertHistoricalPrices(data *outbound.HistoricalData, assetMap map[string]*entity.PriceAsset) ([]*entity.TokenPrice, []*entity.AssetPrice, error) {
	asset, ok := assetMap[data.SourceAssetID]
	if !ok {
		return nil, nil, fmt.Errorf("historical data for unknown asset: %s", data.SourceAssetID)
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

	var tokenPrices []*entity.TokenPrice
	var assetPrices []*entity.AssetPrice
	for _, p := range data.Prices {
		var marketCap *float64
		if mc, ok := marketCapMap[p.Timestamp.Unix()]; ok {
			marketCap = &mc
		}

		var volume *float64
		if v, ok := volumeMap[p.Timestamp.Unix()]; ok {
			volume = &v
		}

		if asset.TokenID == nil {
			ap, err := entity.NewAssetPrice(asset.ID, int16(asset.SourceID), p.PriceUSD, marketCap, volume, p.Timestamp)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid historical price data for asset %s: %w", data.SourceAssetID, err)
			}
			assetPrices = append(assetPrices, ap)
			continue
		}

		tp, err := entity.NewTokenPrice(*asset.TokenID, int16(asset.SourceID), p.PriceUSD, marketCap, volume, p.Timestamp)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid historical price data for asset %s: %w", data.SourceAssetID, err)
		}
		tokenPrices = append(tokenPrices, tp)
	}

	return tokenPrices, assetPrices, nil
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
