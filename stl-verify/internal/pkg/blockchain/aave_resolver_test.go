package blockchain

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestAaveResolver_ResolveOracleAddresses(t *testing.T) {
	providerAddr := common.HexToAddress("0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e")
	expectedOracle := common.HexToAddress("0x54586bE62E3c3580375aE3723C145253060Ca0C2")

	providerABI, err := abis.GetPoolAddressesProviderABI()
	if err != nil {
		t.Fatalf("loading ABI: %v", err)
	}

	tests := []struct {
		name        string
		executeFn   func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error)
		wantAddr    common.Address
		wantErr     bool
		errContains string
	}{
		{
			name: "success",
			executeFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				if len(calls) != 1 {
					t.Fatalf("expected 1 call, got %d", len(calls))
				}
				if calls[0].Target != providerAddr {
					t.Errorf("expected target %s, got %s", providerAddr, calls[0].Target)
				}
				returnData, _ := providerABI.Methods["getPriceOracle"].Outputs.Pack(expectedOracle)
				return []outbound.Result{{Success: true, ReturnData: returnData}}, nil
			},
			wantAddr: expectedOracle,
		},
		{
			name: "multicall error",
			executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				return nil, context.DeadlineExceeded
			},
			wantErr:     true,
			errContains: "resolving Aave oracle",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := &testutil.MockMulticaller{ExecuteFn: tt.executeFn}
			resolver, err := NewAaveResolver(mc, providerAddr)
			if err != nil {
				t.Fatalf("NewAaveResolver: %v", err)
			}

			addrs, err := resolver.ResolveOracleAddresses(context.Background(), 100)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(addrs) != 1 {
				t.Fatalf("expected 1 address, got %d", len(addrs))
			}
			if addrs[0] != tt.wantAddr {
				t.Errorf("expected %s, got %s", tt.wantAddr, addrs[0])
			}
		})
	}
}
