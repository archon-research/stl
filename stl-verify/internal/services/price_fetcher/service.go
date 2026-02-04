// Package price_fetcher provides a source-agnostic service for fetching and storing token prices.
package price_fetcher

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ServiceConfig holds configuration for the price fetcher service.
type ServiceConfig struct {
	// ChainID is the blockchain chain ID (e.g., 1 for Ethereum mainnet).
	ChainID int

	// Logger is the structured logger for the service.
	Logger *slog.Logger
}

// Service orchestrates fetching prices from providers and storing them in the repository.
type Service struct {
	config   ServiceConfig
	provider outbound.PriceProvider
	repo     outbound.PriceRepository
	logger   *slog.Logger
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

	return &Service{
		config:   config,
		provider: provider,
		repo:     repo,
		logger:   logger.With("component", "price-fetcher", "provider", provider.Name()),
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

	tokenPrices := s.convertToTokenPrices(prices, assets)
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
func (s *Service) FetchHistoricalData(ctx context.Context, assetIDs []string, from, to time.Time) error {
	if !s.provider.SupportsHistorical() {
		return fmt.Errorf("provider %s does not support historical data", s.provider.Name())
	}

	assets, err := s.resolveAssets(ctx, assetIDs)
	if err != nil {
		return fmt.Errorf("resolving assets: %w", err)
	}

	if len(assets) == 0 {
		s.logger.Info("no assets to fetch historical data for")
		return nil
	}

	assetMap := buildAssetMap(assets)

	s.logger.Info("fetching historical data",
		"assetCount", len(assets),
		"from", from.Format(time.DateOnly),
		"to", to.Format(time.DateOnly),
	)

	for _, asset := range assets {
		if err := s.fetchHistoricalDataForAsset(ctx, asset, assetMap, from, to); err != nil {
			s.logger.Error("failed to fetch historical data for asset",
				"asset", asset.SourceAssetID,
				"error", err,
			)
			// Continue with other assets even if one fails
		}
	}

	return nil
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

	// Fetch in 30-day chunks to preserve hourly granularity
	chunkDuration := 30 * 24 * time.Hour
	chunkStart := from

	for chunkStart.Before(to) {
		chunkEnd := chunkStart.Add(chunkDuration)
		if chunkEnd.After(to) {
			chunkEnd = to
		}

		if err := s.fetchAndStoreChunk(ctx, asset, assetMap, chunkStart, chunkEnd); err != nil {
			return fmt.Errorf("fetching chunk %s to %s: %w",
				chunkStart.Format(time.DateOnly),
				chunkEnd.Format(time.DateOnly),
				err,
			)
		}

		chunkStart = chunkEnd
	}

	return nil
}

func (s *Service) fetchAndStoreChunk(ctx context.Context, asset *entity.PriceAsset, assetMap map[string]*entity.PriceAsset, from, to time.Time) error {
	s.logger.Debug("fetching chunk",
		"asset", asset.SourceAssetID,
		"from", from.Format(time.DateOnly),
		"to", to.Format(time.DateOnly),
	)

	data, err := s.provider.GetHistoricalData(ctx, asset.SourceAssetID, from, to)
	if err != nil {
		return fmt.Errorf("fetching historical data: %w", err)
	}

	prices := s.convertHistoricalPrices(data, assetMap)
	if len(prices) > 0 {
		if err := s.repo.UpsertPrices(ctx, prices); err != nil {
			return fmt.Errorf("storing prices: %w", err)
		}
		s.logger.Debug("stored prices", "count", len(prices))
	}

	volumes := s.convertHistoricalVolumes(data, assetMap)
	if len(volumes) > 0 {
		if err := s.repo.UpsertVolumes(ctx, volumes); err != nil {
			return fmt.Errorf("storing volumes: %w", err)
		}
		s.logger.Debug("stored volumes", "count", len(volumes))
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

func (s *Service) convertToTokenPrices(prices []outbound.PriceData, assets []*entity.PriceAsset) []*entity.TokenPrice {
	assetMap := buildAssetMap(assets)
	result := make([]*entity.TokenPrice, 0, len(prices))

	for _, p := range prices {
		asset, ok := assetMap[p.SourceAssetID]
		if !ok {
			s.logger.Warn("price for unknown asset", "asset", p.SourceAssetID)
			continue
		}
		if asset.TokenID == nil {
			s.logger.Debug("skipping asset without token_id", "asset", p.SourceAssetID)
			continue
		}

		result = append(result, &entity.TokenPrice{
			TokenID:       *asset.TokenID,
			ChainID:       s.config.ChainID,
			Source:        s.provider.Name(),
			SourceAssetID: p.SourceAssetID,
			PriceUSD:      p.PriceUSD,
			MarketCapUSD:  p.MarketCapUSD,
			Timestamp:     p.Timestamp,
			CreatedAt:     time.Now(),
		})
	}

	return result
}

func (s *Service) convertHistoricalPrices(data *outbound.HistoricalData, assetMap map[string]*entity.PriceAsset) []*entity.TokenPrice {
	asset, ok := assetMap[data.SourceAssetID]
	if !ok || asset.TokenID == nil {
		return nil
	}

	// Build a map of timestamps to market caps for efficient lookup
	marketCapMap := make(map[int64]float64, len(data.MarketCaps))
	for _, mc := range data.MarketCaps {
		marketCapMap[mc.Timestamp.Unix()] = mc.MarketCapUSD
	}

	result := make([]*entity.TokenPrice, 0, len(data.Prices))
	for _, p := range data.Prices {
		var marketCap *float64
		if mc, ok := marketCapMap[p.Timestamp.Unix()]; ok {
			marketCap = &mc
		}

		result = append(result, &entity.TokenPrice{
			TokenID:       *asset.TokenID,
			ChainID:       s.config.ChainID,
			Source:        s.provider.Name(),
			SourceAssetID: data.SourceAssetID,
			PriceUSD:      p.PriceUSD,
			MarketCapUSD:  marketCap,
			Timestamp:     p.Timestamp,
			CreatedAt:     time.Now(),
		})
	}

	return result
}

func (s *Service) convertHistoricalVolumes(data *outbound.HistoricalData, assetMap map[string]*entity.PriceAsset) []*entity.TokenVolume {
	asset, ok := assetMap[data.SourceAssetID]
	if !ok || asset.TokenID == nil {
		return nil
	}

	result := make([]*entity.TokenVolume, 0, len(data.Volumes))
	for _, v := range data.Volumes {
		result = append(result, &entity.TokenVolume{
			TokenID:       *asset.TokenID,
			ChainID:       s.config.ChainID,
			Source:        s.provider.Name(),
			SourceAssetID: data.SourceAssetID,
			VolumeUSD:     v.VolumeUSD,
			Timestamp:     v.Timestamp,
			CreatedAt:     time.Now(),
		})
	}

	return result
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
