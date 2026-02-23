package temporal

import (
	"context"
	"errors"
	"strings"
	"testing"

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
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
				if activities != nil {
					t.Errorf("expected nil activities, got %v", activities)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if activities == nil {
				t.Fatal("expected non-nil activities")
			}
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
			if err != nil {
				t.Fatalf("unexpected error creating activities: %v", err)
			}

			activityEnv.RegisterActivity(activities.FetchCurrentPrices)
			result, err := activityEnv.ExecuteActivity(activities.FetchCurrentPrices, tt.input)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var output FetchCurrentPricesOutput
			if err := result.Get(&output); err != nil {
				t.Fatalf("failed to get output: %v", err)
			}
			if !output.Success {
				t.Error("expected output.Success to be true")
			}
			if len(tt.input.AssetIDs) != len(calledWith) {
				t.Fatalf("expected %d asset IDs, got %d", len(tt.input.AssetIDs), len(calledWith))
			}
			for i, id := range tt.input.AssetIDs {
				if calledWith[i] != id {
					t.Errorf("asset ID[%d]: got %q, want %q", i, calledWith[i], id)
				}
			}
		})
	}
}
