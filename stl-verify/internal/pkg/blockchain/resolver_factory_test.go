package blockchain

import (
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestNewOracleResolver(t *testing.T) {
	mc := testutil.NewMockMulticaller()

	tests := []struct {
		name        string
		metadata    map[string]any
		wantNil     bool
		wantErr     bool
		wantType    string
		errContains string
	}{
		{
			name: "sparklend resolver",
			metadata: map[string]any{
				"oracle_resolver_type":    "sparklend",
				"pool_addresses_provider": "0x02C3eA4e34C0cBd694D2adFa2c690EECbC1793eE",
			},
			wantType: "*blockchain.SparkLendResolver",
		},
		{
			name: "aave resolver",
			metadata: map[string]any{
				"oracle_resolver_type":    "aave",
				"pool_addresses_provider": "0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e",
			},
			wantType: "*blockchain.AaveResolver",
		},
		{
			name:        "no resolver type returns error",
			metadata:    map[string]any{},
			wantErr:     true,
			errContains: "no oracle_resolver_type",
		},
		{
			name: "unknown resolver type returns error",
			metadata: map[string]any{
				"oracle_resolver_type": "compound",
			},
			wantErr:     true,
			errContains: "unsupported oracle_resolver_type",
		},
		{
			name: "sparklend missing provider address",
			metadata: map[string]any{
				"oracle_resolver_type": "sparklend",
			},
			wantErr:     true,
			errContains: "missing pool_addresses_provider",
		},
		{
			name: "aave missing provider address",
			metadata: map[string]any{
				"oracle_resolver_type": "aave",
			},
			wantErr:     true,
			errContains: "missing pool_addresses_provider",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protocol := &entity.Protocol{
				ID:             1,
				ChainID:        1,
				Address:        make([]byte, 20),
				Name:           "TestProtocol",
				ProtocolType:   "lending",
				CreatedAtBlock: 100,
				Metadata:       tt.metadata,
			}

			resolver, err := NewOracleResolver(protocol, mc)

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

			if tt.wantNil {
				if resolver != nil {
					t.Errorf("expected nil resolver, got %T", resolver)
				}
				return
			}

			if resolver == nil {
				t.Fatal("expected non-nil resolver")
			}
		})
	}
}
