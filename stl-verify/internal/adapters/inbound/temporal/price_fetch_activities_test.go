package temporal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

type mockPriceFetcher struct {
	fetchFn func(ctx context.Context, assetIDs []string) error
}

func (m *mockPriceFetcher) FetchCurrentPrices(ctx context.Context, assetIDs []string) error {
	return m.fetchFn(ctx, assetIDs)
}

func TestNewPriceFetchActivities(t *testing.T) {
	tests := []struct {
		name         string
		priceFetcher PriceFetcher
		wantErr      bool
		errContains  string
	}{
		{
			name:         "valid price fetcher",
			priceFetcher: &mockPriceFetcher{},
		},
		{
			name:        "nil price fetcher",
			wantErr:     true,
			errContains: "priceFetcher cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			activities, err := NewPriceFetchActivities(tt.priceFetcher)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				assert.Nil(t, activities)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, activities)
		})
	}
}

func TestFetchCurrentPrices(t *testing.T) {
	tests := []struct {
		name        string
		input       FetchCurrentPricesInput
		fetchErr    error
		wantErr     bool
		errContains string
	}{
		{
			name:  "successful fetch with asset IDs",
			input: FetchCurrentPricesInput{AssetIDs: []string{"bitcoin", "ethereum"}},
		},
		{
			name:  "successful fetch with empty asset IDs",
			input: FetchCurrentPricesInput{},
		},
		{
			name:        "fetch returns error",
			input:       FetchCurrentPricesInput{AssetIDs: []string{"bitcoin"}},
			fetchErr:    errors.New("API rate limit exceeded"),
			wantErr:     true,
			errContains: "fetching current prices",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			suite := &testsuite.WorkflowTestSuite{}
			activityEnv := suite.NewTestActivityEnvironment()

			var calledWith []string
			activities, err := NewPriceFetchActivities(&mockPriceFetcher{
				fetchFn: func(_ context.Context, assetIDs []string) error {
					calledWith = assetIDs
					return tt.fetchErr
				},
			})
			require.NoError(t, err)

			activityEnv.RegisterActivity(activities.FetchCurrentPrices)
			result, err := activityEnv.ExecuteActivity(activities.FetchCurrentPrices, tt.input)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}

			require.NoError(t, err)
			var output FetchCurrentPricesOutput
			require.NoError(t, result.Get(&output))
			assert.True(t, output.Success)
			assert.Equal(t, tt.input.AssetIDs, calledWith)
		})
	}
}
