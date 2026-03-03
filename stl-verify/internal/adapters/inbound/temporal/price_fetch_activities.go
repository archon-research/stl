// Package temporal provides Temporal workflow and activity definitions for scheduled jobs.
package temporal

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/activity"
)

// PriceFetcher defines the interface for fetching prices.
// This is satisfied by offchain_price_fetcher.Service.
type PriceFetcher interface {
	FetchCurrentPrices(ctx context.Context, assetIDs []string) error
}

// PriceFetchActivities holds dependencies for price fetch Temporal activities.
type PriceFetchActivities struct {
	priceFetcher PriceFetcher
}

// NewPriceFetchActivities creates a new PriceFetchActivities instance with the given dependencies.
func NewPriceFetchActivities(priceFetcher PriceFetcher) (*PriceFetchActivities, error) {
	if priceFetcher == nil {
		return nil, fmt.Errorf("priceFetcher cannot be nil")
	}
	return &PriceFetchActivities{priceFetcher: priceFetcher}, nil
}

// FetchCurrentPricesInput is the input for the FetchCurrentPrices activity.
type FetchCurrentPricesInput struct {
	AssetIDs []string
}

// FetchCurrentPricesOutput is the output of the FetchCurrentPrices activity.
type FetchCurrentPricesOutput struct {
	Success bool
}

// FetchCurrentPrices fetches and stores current prices for the given assets.
func (a *PriceFetchActivities) FetchCurrentPrices(ctx context.Context, input FetchCurrentPricesInput) (*FetchCurrentPricesOutput, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("fetching current prices", "assetCount", len(input.AssetIDs))

	if err := a.priceFetcher.FetchCurrentPrices(ctx, input.AssetIDs); err != nil {
		return nil, fmt.Errorf("fetching current prices: %w", err)
	}

	logger.Info("successfully fetched current prices")
	return &FetchCurrentPricesOutput{Success: true}, nil
}
