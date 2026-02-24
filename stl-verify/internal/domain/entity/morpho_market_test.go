package entity

import (
	"math/big"
	"testing"
)

func TestNewMorphoMarket(t *testing.T) {
	validMarketID := make([]byte, 32)
	validAddr := make([]byte, 20)

	tests := []struct {
		name        string
		protocolID  int64
		marketID    []byte
		loanToken   int64
		collToken   int64
		oracle      []byte
		irm         []byte
		lltv        *big.Int
		block       int64
		wantErr     bool
		errContains string
	}{
		{
			name:       "valid market",
			protocolID: 1, marketID: validMarketID, loanToken: 1, collToken: 2,
			oracle: validAddr, irm: validAddr, lltv: big.NewInt(860000000000000000), block: 18883124,
		},
		{
			name:       "zero protocol ID",
			protocolID: 0, marketID: validMarketID, loanToken: 1, collToken: 2,
			oracle: validAddr, irm: validAddr, lltv: big.NewInt(0), block: 1,
			wantErr: true, errContains: "protocolID must be positive",
		},
		{
			name:       "short market ID",
			protocolID: 1, marketID: make([]byte, 16), loanToken: 1, collToken: 2,
			oracle: validAddr, irm: validAddr, lltv: big.NewInt(0), block: 1,
			wantErr: true, errContains: "marketID must be 32 bytes",
		},
		{
			name:       "zero loan token",
			protocolID: 1, marketID: validMarketID, loanToken: 0, collToken: 2,
			oracle: validAddr, irm: validAddr, lltv: big.NewInt(0), block: 1,
			wantErr: true, errContains: "loanTokenID must be positive",
		},
		{
			name:       "zero collateral token",
			protocolID: 1, marketID: validMarketID, loanToken: 1, collToken: 0,
			oracle: validAddr, irm: validAddr, lltv: big.NewInt(0), block: 1,
			wantErr: true, errContains: "collateralTokenID must be positive",
		},
		{
			name:       "short oracle address",
			protocolID: 1, marketID: validMarketID, loanToken: 1, collToken: 2,
			oracle: make([]byte, 10), irm: validAddr, lltv: big.NewInt(0), block: 1,
			wantErr: true, errContains: "oracleAddress must be 20 bytes",
		},
		{
			name:       "short irm address",
			protocolID: 1, marketID: validMarketID, loanToken: 1, collToken: 2,
			oracle: validAddr, irm: make([]byte, 10), lltv: big.NewInt(0), block: 1,
			wantErr: true, errContains: "irmAddress must be 20 bytes",
		},
		{
			name:       "nil lltv",
			protocolID: 1, marketID: validMarketID, loanToken: 1, collToken: 2,
			oracle: validAddr, irm: validAddr, lltv: nil, block: 1,
			wantErr: true, errContains: "lltv must not be nil",
		},
		{
			name:       "negative lltv",
			protocolID: 1, marketID: validMarketID, loanToken: 1, collToken: 2,
			oracle: validAddr, irm: validAddr, lltv: big.NewInt(-1), block: 1,
			wantErr: true, errContains: "lltv must be non-negative",
		},
		{
			name:       "zero block",
			protocolID: 1, marketID: validMarketID, loanToken: 1, collToken: 2,
			oracle: validAddr, irm: validAddr, lltv: big.NewInt(0), block: 0,
			wantErr: true, errContains: "createdAtBlock must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMorphoMarket(tt.protocolID, tt.marketID, tt.loanToken, tt.collToken, tt.oracle, tt.irm, tt.lltv, tt.block)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" && !containsStr(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.ProtocolID != tt.protocolID {
				t.Errorf("ProtocolID = %d, want %d", got.ProtocolID, tt.protocolID)
			}
		})
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
