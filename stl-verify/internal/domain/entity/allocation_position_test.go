package entity

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func validPosition() *AllocationPosition {
	return &AllocationPosition{
		ChainID:        1,
		TokenAddress:   common.HexToAddress("0x38464507e02c983f20428a6e8566693fe9e422a9"),
		TokenSymbol:    "sparkPrimeUSDC1",
		TokenDecimals:  6,
		PrimeID:        1,
		ProxyAddress:   common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e"),
		Balance:        big.NewInt(100),
		Direction:      "sweep",
		BlockNumber:    100,
		CreatedAtBlock: 50,
		CreatedAt:      time.Unix(1750000000, 0).UTC(),
	}
}

func TestAllocationPosition_Validate(t *testing.T) {
	proxy := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e") // matches validPosition
	counterparty := common.HexToAddress("0x9999999999999999999999999999999999999999")
	mintBurn := common.Address{}

	cases := []struct {
		name    string
		mut     func(*AllocationPosition)
		wantErr bool
	}{
		{"ok", func(*AllocationPosition) {}, false},
		{"missing created_at", func(p *AllocationPosition) { p.CreatedAt = time.Time{} }, true},
		{"missing chain", func(p *AllocationPosition) { p.ChainID = 0 }, true},
		{"missing token address", func(p *AllocationPosition) { p.TokenAddress = common.Address{} }, true},
		{"missing proxy address", func(p *AllocationPosition) { p.ProxyAddress = common.Address{} }, true},
		{"missing balance", func(p *AllocationPosition) { p.Balance = nil }, true},
		{"missing direction", func(p *AllocationPosition) { p.Direction = "" }, true},
		{"bad direction", func(p *AllocationPosition) { p.Direction = "sideways" }, true},
		{"missing prime", func(p *AllocationPosition) { p.PrimeID = 0 }, true},
		{"missing block number", func(p *AllocationPosition) { p.BlockNumber = 0 }, true},
		{"zero created_at_block", func(p *AllocationPosition) { p.CreatedAtBlock = 0 }, true},
		{"negative created_at_block", func(p *AllocationPosition) { p.CreatedAtBlock = -1 }, true},
		{"transfer without either party", func(p *AllocationPosition) { p.Direction = "in" }, true},
		{"transfer with only one party", func(p *AllocationPosition) {
			p.Direction = "in"
			p.FromAddress = &counterparty
		}, true},
		{"inbound transfer", func(p *AllocationPosition) {
			p.Direction = "in"
			p.FromAddress = &counterparty
			p.ToAddress = &proxy
		}, false},
		{"outbound transfer", func(p *AllocationPosition) {
			p.Direction = "out"
			p.FromAddress = &proxy
			p.ToAddress = &counterparty
		}, false},
		// direction is redundant with the two addresses, so it can contradict them.
		{"inbound but the proxy is the sender", func(p *AllocationPosition) {
			p.Direction = "in"
			p.FromAddress = &proxy
			p.ToAddress = &counterparty
		}, true},
		{"outbound but the proxy is the recipient", func(p *AllocationPosition) {
			p.Direction = "out"
			p.FromAddress = &counterparty
			p.ToAddress = &proxy
		}, true},
		{"zero address is a mint, not a missing value", func(p *AllocationPosition) {
			p.Direction = "in"
			p.FromAddress = &mintBurn
			p.ToAddress = &proxy
		}, false},
		{"zero address is a burn, not a missing value", func(p *AllocationPosition) {
			p.Direction = "out"
			p.FromAddress = &proxy
			p.ToAddress = &mintBurn
		}, false},
		{"sweep carries neither party", func(p *AllocationPosition) { p.Direction = "sweep" }, false},
		{"sweep carrying a sender", func(p *AllocationPosition) {
			p.Direction = "sweep"
			p.FromAddress = &counterparty
		}, true},
		{"sweep carrying a recipient", func(p *AllocationPosition) {
			p.Direction = "sweep"
			p.ToAddress = &proxy
		}, true},
		{"sweep carrying both parties", func(p *AllocationPosition) {
			p.Direction = "sweep"
			p.FromAddress = &counterparty
			p.ToAddress = &proxy
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := validPosition()
			tc.mut(p)
			err := p.Validate()
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestAllocationPositionValidate_Underlying(t *testing.T) {
	usdc := common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
	tests := []struct {
		name       string
		underlying *UnderlyingValuation
		wantErr    bool
	}{
		{"nil valuation is valid", nil, false},
		{"complete valuation is valid", &UnderlyingValuation{Value: big.NewInt(1), AssetAddress: usdc, AssetSymbol: "USDC", AssetDecimals: 6}, false},
		{"zero value is valid", &UnderlyingValuation{Value: big.NewInt(0), AssetAddress: usdc, AssetSymbol: "USDC", AssetDecimals: 6}, false},
		{"missing value errors", &UnderlyingValuation{AssetAddress: usdc, AssetSymbol: "USDC", AssetDecimals: 6}, true},
		{"zero asset address errors", &UnderlyingValuation{Value: big.NewInt(1), AssetSymbol: "USDC", AssetDecimals: 6}, true},
		{"negative decimals errors", &UnderlyingValuation{Value: big.NewInt(1), AssetAddress: usdc, AssetSymbol: "USDC", AssetDecimals: -1}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := validPosition()
			p.Underlying = tc.underlying
			err := p.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
