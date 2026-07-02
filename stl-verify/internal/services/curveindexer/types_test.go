package curveindexer

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// TestStateSnapshot_ValidateRejectsCrossClassConfig verifies that a snapshot
// whose config pointer does not match Pool.Kind fails validation, so a handler
// bug never writes a mismatched config row.
func TestStateSnapshot_ValidateRejectsCrossClassConfig(t *testing.T) {
	addr := common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022")

	cases := []struct {
		name string
		snap StateSnapshot
	}{
		{
			name: "stableswap_state_with_cryptoswap_config",
			snap: StateSnapshot{
				Pool:             RegisteredPool{Address: addr, Kind: KindStableswapNG},
				Stableswap:       &entity.CurveStableswapState{},
				CryptoswapConfig: &entity.CurveCryptoswapConfig{},
			},
		},
		{
			name: "cryptoswap_state_with_stableswap_config",
			snap: StateSnapshot{
				Pool:             RegisteredPool{Address: addr, Kind: KindCryptoswap},
				Cryptoswap:       &entity.CurveCryptoswapState{},
				StableswapConfig: &entity.CurveStableswapConfig{},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.snap.Validate(); err == nil {
				t.Error("Validate() = nil, want error for cross-class config")
			}
		})
	}
}

// TestStateSnapshot_ValidateAcceptsMatchingConfig verifies a class-matched config
// passes validation.
func TestStateSnapshot_ValidateAcceptsMatchingConfig(t *testing.T) {
	addr := common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022")
	snap := StateSnapshot{
		Pool:             RegisteredPool{Address: addr, Kind: KindStableswapNG},
		Stableswap:       &entity.CurveStableswapState{},
		StableswapConfig: &entity.CurveStableswapConfig{},
	}
	if err := snap.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil for matching config", err)
	}
}
