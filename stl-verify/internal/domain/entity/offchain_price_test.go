package entity

import (
	"testing"
	"time"
)

func TestNewAssetPrice(t *testing.T) {
	ts := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		assetID   int64
		sourceID  int16
		priceUSD  float64
		timestamp time.Time
		wantErr   bool
	}{
		{name: "valid", assetID: 1, sourceID: 1, priceUSD: 2.71, timestamp: ts},
		{name: "zero price is valid", assetID: 1, sourceID: 1, priceUSD: 0, timestamp: ts},
		{name: "non-positive assetID", assetID: 0, sourceID: 1, priceUSD: 1, timestamp: ts, wantErr: true},
		{name: "non-positive sourceID", assetID: 1, sourceID: 0, priceUSD: 1, timestamp: ts, wantErr: true},
		{name: "negative price", assetID: 1, sourceID: 1, priceUSD: -1, timestamp: ts, wantErr: true},
		{name: "zero timestamp", assetID: 1, sourceID: 1, priceUSD: 1, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ap, err := NewAssetPrice(tc.assetID, tc.sourceID, tc.priceUSD, nil, nil, tc.timestamp)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ap.AssetID != tc.assetID || ap.SourceID != tc.sourceID || ap.PriceUSD != tc.priceUSD {
				t.Errorf("fields not carried through: %+v", ap)
			}
		})
	}
}
