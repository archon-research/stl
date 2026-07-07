package blockchain

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func erc4626ABI(t *testing.T) *abi.ABI {
	t.Helper()
	parsed, err := abis.GetERC4626ABI()
	if err != nil {
		t.Fatalf("loading ERC4626 ABI: %v", err)
	}
	return parsed
}

func TestFetchERC4626SharePrices(t *testing.T) {
	shareABI := erc4626ABI(t)
	fABI := feedABI(t)
	blockNum := int64(22_000_000)

	fsusds := common.HexToAddress("0x2BBE31d63E6813E3AC858C04dae43FB2a72B0D11")
	sUSDSFeed := common.HexToAddress("0xfF30586cD0F29eD462364C7e81375FC0C71219b1")

	vault := ERC4626VaultConfig{
		TokenID:            10,
		VaultAddress:       fsusds,
		ShareDecimals:      18,
		UnderlyingFeed:     sUSDSFeed,
		UnderlyingDecimals: 18,
		FeedDecimals:       8,
	}

	// convertToAssets(1e18) = 1.05 * 1e18 → ratio 1.05; sUSDS/USD = 1.0 (8 decimals) → $1.05.
	ratio105 := new(big.Int).Add(testutil.E18(1), new(big.Int).Div(testutil.E18(1), big.NewInt(20)))

	tests := []struct {
		name        string
		vaults      []ERC4626VaultConfig
		mock        *mockMulticaller
		wantErr     bool
		errContains string
		wantResults []FeedPriceResult
	}{
		{
			name:   "happy path - ratio 1.05 times sUSDS 1.0",
			vaults: []ERC4626VaultConfig{vault},
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					if len(calls) != 2 {
						t.Fatalf("expected 2 calls (convertToAssets + latestRoundData), got %d", len(calls))
					}
					if calls[0].Target != fsusds {
						t.Errorf("call[0] target = %s, want vault %s", calls[0].Target, fsusds)
					}
					if calls[1].Target != sUSDSFeed {
						t.Errorf("call[1] target = %s, want feed %s", calls[1].Target, sUSDSFeed)
					}
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackConvertToAssets(t, ratio105)},
						{Success: true, ReturnData: packRoundData(t, big.NewInt(100_000_000), big.NewInt(1000))},
					}, nil
				},
			},
			wantResults: []FeedPriceResult{
				{TokenID: 10, Price: 1.05, Success: true},
			},
		},
		{
			name:   "underlying above peg propagates to share price",
			vaults: []ERC4626VaultConfig{vault},
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					// ratio 1.0, sUSDS/USD = 1.01 → $1.01
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackConvertToAssets(t, testutil.E18(1))},
						{Success: true, ReturnData: packRoundData(t, big.NewInt(101_000_000), big.NewInt(1000))},
					}, nil
				},
			},
			wantResults: []FeedPriceResult{
				{TokenID: 10, Price: 1.01, Success: true},
			},
		},
		{
			name:   "sole vault convertToAssets reverts - all failed, returns error",
			vaults: []ERC4626VaultConfig{vault},
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: false},
						{Success: true, ReturnData: packRoundData(t, big.NewInt(100_000_000), big.NewInt(1000))},
					}, nil
				},
			},
			wantErr: true, errContains: "all 1 erc4626 vaults failed",
		},
		{
			name:   "sole underlying feed reverts - all failed, returns error",
			vaults: []ERC4626VaultConfig{vault},
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackConvertToAssets(t, ratio105)},
						{Success: false},
					}, nil
				},
			},
			wantErr: true, errContains: "all 1 erc4626 vaults failed",
		},
		{
			name:   "sole underlying feed non-positive answer - all failed, returns error",
			vaults: []ERC4626VaultConfig{vault},
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackConvertToAssets(t, ratio105)},
						{Success: true, ReturnData: packRoundData(t, big.NewInt(0), big.NewInt(1000))},
					}, nil
				},
			},
			wantErr: true, errContains: "all 1 erc4626 vaults failed",
		},
		{
			name:   "sole vault convertToAssets returns zero assets - all failed, returns error",
			vaults: []ERC4626VaultConfig{vault},
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackConvertToAssets(t, big.NewInt(0))},
						{Success: true, ReturnData: packRoundData(t, big.NewInt(100_000_000), big.NewInt(1000))},
					}, nil
				},
			},
			wantErr: true, errContains: "all 1 erc4626 vaults failed",
		},
		{
			name:   "one of two vaults fails - partial success returns no error",
			vaults: []ERC4626VaultConfig{vault, {TokenID: 11, VaultAddress: fsusds, ShareDecimals: 18, UnderlyingFeed: sUSDSFeed, UnderlyingDecimals: 18, FeedDecimals: 8}},
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackConvertToAssets(t, ratio105)},
						{Success: true, ReturnData: packRoundData(t, big.NewInt(100_000_000), big.NewInt(1000))},
						{Success: false}, // second vault convertToAssets reverts
						{Success: true, ReturnData: packRoundData(t, big.NewInt(100_000_000), big.NewInt(1000))},
					}, nil
				},
			},
			wantResults: []FeedPriceResult{
				{TokenID: 10, Price: 1.05, Success: true},
				{TokenID: 11, Success: false},
			},
		},
		{
			name:   "multicall error returns error",
			vaults: []ERC4626VaultConfig{vault},
			mock: &mockMulticaller{executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				return nil, errors.New("RPC down")
			}},
			wantErr: true, errContains: "executing multicall at block",
		},
		{
			name:   "convertToAssets unpack error returns error",
			vaults: []ERC4626VaultConfig{vault},
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: []byte{0xde, 0xad}},
						{Success: true, ReturnData: packRoundData(t, big.NewInt(100_000_000), big.NewInt(1000))},
					}, nil
				},
			},
			wantErr: true, errContains: "unpacking convertToAssets",
		},
		{
			name:   "result count mismatch returns error",
			vaults: []ERC4626VaultConfig{vault},
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{{Success: true, ReturnData: testutil.PackConvertToAssets(t, ratio105)}}, nil
				},
			},
			wantErr: true, errContains: "expected 2 multicall results",
		},
		{
			name:   "empty vaults returns nil",
			vaults: nil,
			mock: &mockMulticaller{executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				t.Fatal("Execute should not be called")
				return nil, nil
			}},
			wantResults: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := FetchERC4626SharePrices(
				context.Background(),
				tt.mock,
				shareABI,
				fABI,
				tt.vaults,
				blockNum,
				oracleTestBlockHash,
				testutil.DiscardLogger(),
			)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errContains)
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error = %q, want it to contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(results) != len(tt.wantResults) {
				t.Fatalf("results len = %d, want %d", len(results), len(tt.wantResults))
			}
			for i, want := range tt.wantResults {
				got := results[i]
				if got.TokenID != want.TokenID {
					t.Errorf("result[%d].TokenID = %d, want %d", i, got.TokenID, want.TokenID)
				}
				if got.Success != want.Success {
					t.Errorf("result[%d].Success = %v, want %v", i, got.Success, want.Success)
				}
				if got.Success && got.Price != want.Price {
					t.Errorf("result[%d].Price = %v, want %v", i, got.Price, want.Price)
				}
			}
		})
	}
}

func TestFetchERC4626SharePrices_PinsToBlockHash(t *testing.T) {
	shareABI := erc4626ABI(t)
	fABI := feedABI(t)

	vault := ERC4626VaultConfig{
		TokenID:            10,
		VaultAddress:       common.HexToAddress("0x2BBE31d63E6813E3AC858C04dae43FB2a72B0D11"),
		ShareDecimals:      18,
		UnderlyingFeed:     common.HexToAddress("0xfF30586cD0F29eD462364C7e81375FC0C71219b1"),
		UnderlyingDecimals: 18,
		FeedDecimals:       8,
	}

	// convertToAssets (share ratio) and latestRoundData are per-block state, so a
	// reorg must not answer eth_call-by-number from the wrong fork (VEC-471).
	mock := &mockMulticaller{
		executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			t.Fatal("FetchERC4626SharePrices must call ExecuteAtHash for a non-zero block hash, not Execute")
			return nil, nil
		},
		executeAtHashFn: func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
			if blockHash != oracleTestBlockHash {
				t.Errorf("blockHash = %s, want %s", blockHash, oracleTestBlockHash)
			}
			return []outbound.Result{
				{Success: true, ReturnData: testutil.PackConvertToAssets(t, testutil.E18(1))},
				{Success: true, ReturnData: packRoundData(t, big.NewInt(100_000_000), big.NewInt(1000))},
			}, nil
		},
	}

	results, err := FetchERC4626SharePrices(
		context.Background(), mock, shareABI, fABI,
		[]ERC4626VaultConfig{vault}, 22_000_000, oracleTestBlockHash, testutil.DiscardLogger(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 || !results[0].Success {
		t.Fatalf("expected 1 successful result, got %+v", results)
	}
}
