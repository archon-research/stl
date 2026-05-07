package entity

import (
	"strings"
	"testing"
	"time"
)

func TestNewOnchainTokenPrice(t *testing.T) {
	validTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		tokenID      int64
		oracleID     int16
		blockNumber  int64
		blockVersion int16
		timestamp    time.Time
		priceUSD     float64
		wantErr      bool
		errContains  string
	}{
		{
			name:         "valid price",
			tokenID:      1,
			oracleID:     1,
			blockNumber:  100,
			blockVersion: 0,
			timestamp:    validTime,
			priceUSD:     1234.56,
		},
		{
			name:         "zero tokenID",
			tokenID:      0,
			oracleID:     1,
			blockNumber:  100,
			blockVersion: 0,
			timestamp:    validTime,
			priceUSD:     1.0,
			wantErr:      true,
			errContains:  "tokenID must be positive",
		},
		{
			name:         "negative tokenID",
			tokenID:      -1,
			oracleID:     1,
			blockNumber:  100,
			blockVersion: 0,
			timestamp:    validTime,
			priceUSD:     1.0,
			wantErr:      true,
			errContains:  "tokenID must be positive",
		},
		{
			name:         "zero oracleID",
			tokenID:      1,
			oracleID:     0,
			blockNumber:  100,
			blockVersion: 0,
			timestamp:    validTime,
			priceUSD:     1.0,
			wantErr:      true,
			errContains:  "oracleID",
		},
		{
			name:         "zero blockNumber",
			tokenID:      1,
			oracleID:     1,
			blockNumber:  0,
			blockVersion: 0,
			timestamp:    validTime,
			priceUSD:     1.0,
			wantErr:      true,
			errContains:  "blockNumber must be positive",
		},
		{
			name:         "negative blockVersion",
			tokenID:      1,
			oracleID:     1,
			blockNumber:  100,
			blockVersion: -1,
			timestamp:    validTime,
			priceUSD:     1.0,
			wantErr:      true,
			errContains:  "blockVersion must be non-negative",
		},
		{
			name:         "zero timestamp",
			tokenID:      1,
			oracleID:     1,
			blockNumber:  100,
			blockVersion: 0,
			timestamp:    time.Time{},
			priceUSD:     1.0,
			wantErr:      true,
			errContains:  "timestamp must not be zero",
		},
		{
			name:         "negative priceUSD",
			tokenID:      1,
			oracleID:     1,
			blockNumber:  100,
			blockVersion: 0,
			timestamp:    validTime,
			priceUSD:     -1.0,
			wantErr:      true,
			errContains:  "priceUSD must be non-negative",
		},
		{
			name:         "zero priceUSD is valid",
			tokenID:      1,
			oracleID:     1,
			blockNumber:  100,
			blockVersion: 0,
			timestamp:    validTime,
			priceUSD:     0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewOnchainTokenPrice(tt.tokenID, tt.oracleID, tt.blockNumber, tt.blockVersion, tt.timestamp, tt.priceUSD)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.TokenID != tt.tokenID {
				t.Errorf("TokenID = %d, want %d", got.TokenID, tt.tokenID)
			}
			if got.PriceUSD != tt.priceUSD {
				t.Errorf("PriceUSD = %f, want %f", got.PriceUSD, tt.priceUSD)
			}
		})
	}
}
