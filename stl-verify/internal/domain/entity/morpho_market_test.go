package entity

import (
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestNewMorphoMarket(t *testing.T) {
	validMarketID := common.BytesToHash([]byte("test-market-id-32-bytes-long!!!!"))
	validOracle := common.HexToAddress("0x1111111111111111111111111111111111111111")
	validIrm := common.HexToAddress("0x2222222222222222222222222222222222222222")

	tests := []struct {
		name        string
		chainID     int64
		protocolID  int64
		marketID    common.Hash
		loanToken   int64
		collToken   int64
		oracle      common.Address
		irm         common.Address
		lltv        *big.Int
		block       int64
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid market",
			chainID: 1, protocolID: 1, marketID: validMarketID, loanToken: 1, collToken: 2,
			oracle: validOracle, irm: validIrm, lltv: big.NewInt(860000000000000000), block: 18883124,
		},
		{
			name:    "zero chain ID",
			chainID: 0, protocolID: 1, marketID: validMarketID, loanToken: 1, collToken: 2,
			oracle: validOracle, irm: validIrm, lltv: big.NewInt(0), block: 1,
			wantErr: true, errContains: "chainID must be positive",
		},
		{
			name:    "zero protocol ID",
			chainID: 1, protocolID: 0, marketID: validMarketID, loanToken: 1, collToken: 2,
			oracle: validOracle, irm: validIrm, lltv: big.NewInt(0), block: 1,
			wantErr: true, errContains: "protocolID must be positive",
		},
		{
			name:    "empty market ID",
			chainID: 1, protocolID: 1, marketID: common.Hash{}, loanToken: 1, collToken: 2,
			oracle: validOracle, irm: validIrm, lltv: big.NewInt(0), block: 1,
			wantErr: true, errContains: "marketID must not be empty",
		},
		{
			name:    "zero loan token",
			chainID: 1, protocolID: 1, marketID: validMarketID, loanToken: 0, collToken: 2,
			oracle: validOracle, irm: validIrm, lltv: big.NewInt(0), block: 1,
			wantErr: true, errContains: "loanTokenID must be positive",
		},
		{
			name:    "zero collateral token",
			chainID: 1, protocolID: 1, marketID: validMarketID, loanToken: 1, collToken: 0,
			oracle: validOracle, irm: validIrm, lltv: big.NewInt(0), block: 1,
			wantErr: true, errContains: "collateralTokenID must be positive",
		},
		{
			name:    "nil lltv",
			chainID: 1, protocolID: 1, marketID: validMarketID, loanToken: 1, collToken: 2,
			oracle: validOracle, irm: validIrm, lltv: nil, block: 1,
			wantErr: true, errContains: "lltv must not be nil",
		},
		{
			name:    "negative lltv",
			chainID: 1, protocolID: 1, marketID: validMarketID, loanToken: 1, collToken: 2,
			oracle: validOracle, irm: validIrm, lltv: big.NewInt(-1), block: 1,
			wantErr: true, errContains: "lltv must be non-negative",
		},
		{
			name:    "zero block",
			chainID: 1, protocolID: 1, marketID: validMarketID, loanToken: 1, collToken: 2,
			oracle: validOracle, irm: validIrm, lltv: big.NewInt(0), block: 0,
			wantErr: true, errContains: "createdAtBlock must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewMorphoMarket(tt.chainID, tt.protocolID, tt.marketID, tt.loanToken, tt.collToken, tt.oracle, tt.irm, tt.lltv, tt.block)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
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
