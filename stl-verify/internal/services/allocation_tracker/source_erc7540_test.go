package allocation_tracker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func newTestERC7540Source(t *testing.T, mc outbound.Multicaller) *ERC7540Source {
	t.Helper()
	src, err := NewERC7540Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return src
}

func packShareOutput(t *testing.T, src *ERC7540Source, share common.Address) []byte {
	t.Helper()
	data, err := src.vaultABI.Methods["share"].Outputs.Pack(share)
	if err != nil {
		t.Fatalf("pack share output: %v", err)
	}
	return data
}

func packBalanceOutput(t *testing.T, src *ERC7540Source, balance *big.Int) []byte {
	t.Helper()
	data, err := src.vaultABI.Methods["balanceOf"].Outputs.Pack(balance)
	if err != nil {
		t.Fatalf("pack balanceOf output: %v", err)
	}
	return data
}

func TestERC7540Source_Supports(t *testing.T) {
	src := newTestERC7540Source(t, nil)

	if got := src.Name(); got != "erc7540" {
		t.Fatalf("Name() = %q, want %q", got, "erc7540")
	}

	tests := []struct {
		name      string
		tokenType string
		protocol  string
		want      bool
	}{
		{"centrifuge token type", "centrifuge", "centrifuge", true},
		{"centrifuge with other protocol", "centrifuge", "grove", true},
		{"centrifuge feeder token type", "centrifuge_feeder", "centrifuge", false},
		{"plain erc20 token type", "erc20", "centrifuge", false},
		{"erc4626 token type", "erc4626", "sky", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := src.Supports(tc.tokenType, tc.protocol); got != tc.want {
				t.Fatalf("Supports(%q, %q) = %v, want %v", tc.tokenType, tc.protocol, got, tc.want)
			}
		})
	}
}

func TestERC7540Source_FetchBalances_ResolvesShareAndStoresBalance(t *testing.T) {
	vault := common.HexToAddress("0xfe6920ebdcf2cb12c0a7a2cd49b6a51d622e1ce1")
	share := common.HexToAddress("0x8c213ee79581ff4984583c6a801e5263418c4b86")
	wallet := common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")
	expectedShares := new(big.Int).Mul(big.NewInt(54321), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))

	mc := testutil.NewMockMulticaller()
	src := newTestERC7540Source(t, mc)

	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		if blockHash != testBlockHash {
			t.Fatalf("blockHash = %v, want %v (state read must be pinned to the block hash, not the number, so a reorg can't return the wrong fork's state)", blockHash, testBlockHash)
		}
		switch mc.CallCount {
		case 1: // share() resolution round
			if len(calls) != 1 {
				t.Fatalf("expected 1 share call, got %d", len(calls))
			}
			if calls[0].Target != vault {
				t.Fatalf("share call target = %s, want vault %s", calls[0].Target.Hex(), vault.Hex())
			}
			return []outbound.Result{{Success: true, ReturnData: packShareOutput(t, src, share)}}, nil
		case 2: // balanceOf round against the resolved share token
			if len(calls) != 1 {
				t.Fatalf("expected 1 balanceOf call, got %d", len(calls))
			}
			if calls[0].Target != share {
				t.Fatalf("balanceOf call target = %s, want share %s", calls[0].Target.Hex(), share.Hex())
			}
			return []outbound.Result{{Success: true, ReturnData: packBalanceOutput(t, src, expectedShares)}}, nil
		default:
			t.Fatalf("unexpected multicall round %d", mc.CallCount)
			return nil, nil
		}
	}

	entries := []*TokenEntry{{
		ContractAddress: vault,
		WalletAddress:   wallet,
		Star:            "grove",
		Chain:           "mainnet",
		Protocol:        "centrifuge",
		TokenType:       "centrifuge",
	}}

	results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
	if err != nil {
		t.Fatalf("FetchBalances failed: %v", err)
	}
	if mc.CallCount != 2 {
		t.Fatalf("expected exactly two multicall rounds, got %d", mc.CallCount)
	}

	got := results.Balances[entries[0].Key()]
	if got == nil {
		t.Fatal("expected result for entry")
	}
	if got.Balance.Cmp(expectedShares) != 0 {
		t.Fatalf("balance = %s, want %s", got.Balance, expectedShares)
	}
	if got.ScaledBalance == nil || got.ScaledBalance.Cmp(expectedShares) != 0 {
		t.Fatalf("scaled balance = %v, want %s", got.ScaledBalance, expectedShares)
	}
	if len(results.Supplies) != 0 {
		t.Fatalf("erc7540 source should not emit supply rows, got %d", len(results.Supplies))
	}
}

func TestERC7540Source_FetchBalances_ZeroBalance(t *testing.T) {
	vault := common.HexToAddress("0xaaaa")
	share := common.HexToAddress("0xcccc")

	mc := testutil.NewMockMulticaller()
	src := newTestERC7540Source(t, mc)

	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		if mc.CallCount == 1 {
			return []outbound.Result{{Success: true, ReturnData: packShareOutput(t, src, share)}}, nil
		}
		return []outbound.Result{{Success: true, ReturnData: packBalanceOutput(t, src, big.NewInt(0))}}, nil
	}

	entries := []*TokenEntry{{
		ContractAddress: vault,
		WalletAddress:   common.HexToAddress("0xbbbb"),
		TokenType:       "centrifuge",
	}}

	results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
	if err != nil {
		t.Fatalf("FetchBalances failed: %v", err)
	}
	got := results.Balances[entries[0].Key()]
	if got == nil {
		t.Fatal("expected zero-balance result for entry")
	}
	if got.Balance.Sign() != 0 || got.ScaledBalance.Sign() != 0 {
		t.Fatalf("balance = %s / %s, want 0 / 0", got.Balance, got.ScaledBalance)
	}
}

func TestERC7540Source_FetchBalances_SharedVaultResolvedOnce(t *testing.T) {
	vault := common.HexToAddress("0xaaaa")
	share := common.HexToAddress("0xcccc")
	walletA := common.HexToAddress("0x1111")
	walletB := common.HexToAddress("0x2222")

	mc := testutil.NewMockMulticaller()
	src := newTestERC7540Source(t, mc)

	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		if mc.CallCount == 1 {
			if len(calls) != 1 {
				t.Fatalf("expected 1 share call for shared vault, got %d", len(calls))
			}
			return []outbound.Result{{Success: true, ReturnData: packShareOutput(t, src, share)}}, nil
		}
		if len(calls) != 2 {
			t.Fatalf("expected 2 balanceOf calls, got %d", len(calls))
		}
		return []outbound.Result{
			{Success: true, ReturnData: packBalanceOutput(t, src, big.NewInt(100))},
			{Success: true, ReturnData: packBalanceOutput(t, src, big.NewInt(200))},
		}, nil
	}

	entries := []*TokenEntry{
		{ContractAddress: vault, WalletAddress: walletA, TokenType: "centrifuge"},
		{ContractAddress: vault, WalletAddress: walletB, TokenType: "centrifuge"},
	}

	results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
	if err != nil {
		t.Fatalf("FetchBalances failed: %v", err)
	}
	if got := results.Balances[entries[0].Key()]; got == nil || got.Balance.Cmp(big.NewInt(100)) != 0 {
		t.Fatalf("walletA balance = %v, want 100", got)
	}
	if got := results.Balances[entries[1].Key()]; got == nil || got.Balance.Cmp(big.NewInt(200)) != 0 {
		t.Fatalf("walletB balance = %v, want 200", got)
	}
}

func TestERC7540Source_FetchBalances_DuplicateShareForWalletFails(t *testing.T) {
	vaultA := common.HexToAddress("0xaaaa")
	vaultB := common.HexToAddress("0xdddd")
	share := common.HexToAddress("0xcccc")
	wallet := common.HexToAddress("0xbbbb")

	mc := testutil.NewMockMulticaller()
	src := newTestERC7540Source(t, mc)

	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		shareData := packShareOutput(t, src, share)
		return []outbound.Result{
			{Success: true, ReturnData: shareData},
			{Success: true, ReturnData: shareData},
		}, nil
	}

	entries := []*TokenEntry{
		{ContractAddress: vaultA, WalletAddress: wallet, TokenType: "centrifuge"},
		{ContractAddress: vaultB, WalletAddress: wallet, TokenType: "centrifuge"},
	}

	results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
	if err == nil {
		t.Fatal("expected double-count error for two vaults resolving to the same share")
	}
	if !strings.Contains(err.Error(), "double count") {
		t.Fatalf("error = %v, want double-count message", err)
	}
	if results != nil {
		t.Fatal("expected nil results on duplicate share")
	}
	if mc.CallCount != 1 {
		t.Fatalf("expected no balanceOf round after duplicate detection, got %d rounds", mc.CallCount)
	}
}

func TestERC7540Source_FetchBalances_SameShareDifferentWalletsSucceeds(t *testing.T) {
	vaultA := common.HexToAddress("0xaaaa")
	vaultB := common.HexToAddress("0xdddd")
	share := common.HexToAddress("0xcccc")
	walletA := common.HexToAddress("0x1111")
	walletB := common.HexToAddress("0x2222")

	mc := testutil.NewMockMulticaller()
	src := newTestERC7540Source(t, mc)

	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		if mc.CallCount == 1 {
			shareData := packShareOutput(t, src, share)
			return []outbound.Result{
				{Success: true, ReturnData: shareData},
				{Success: true, ReturnData: shareData},
			}, nil
		}
		return []outbound.Result{
			{Success: true, ReturnData: packBalanceOutput(t, src, big.NewInt(100))},
			{Success: true, ReturnData: packBalanceOutput(t, src, big.NewInt(200))},
		}, nil
	}

	entries := []*TokenEntry{
		{ContractAddress: vaultA, WalletAddress: walletA, TokenType: "centrifuge"},
		{ContractAddress: vaultB, WalletAddress: walletB, TokenType: "centrifuge"},
	}

	results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
	if err != nil {
		t.Fatalf("same share for different wallets must not be a double count: %v", err)
	}
	if got := results.Balances[entries[0].Key()]; got == nil || got.Balance.Cmp(big.NewInt(100)) != 0 {
		t.Fatalf("walletA balance = %v, want 100", got)
	}
	if got := results.Balances[entries[1].Key()]; got == nil || got.Balance.Cmp(big.NewInt(200)) != 0 {
		t.Fatalf("walletB balance = %v, want 200", got)
	}
}

func TestERC7540Source_FetchBalances_FailureModes(t *testing.T) {
	share := common.HexToAddress("0xcccc")

	tests := []struct {
		name string
		// results/error returned per multicall round; a one-element slice means
		// the failure happens in round 1 and round 2 is never reached.
		rounds         func(src *ERC7540Source) []func() ([]outbound.Result, error)
		expectedRounds int
	}{
		{
			name: "share call reverted",
			rounds: func(src *ERC7540Source) []func() ([]outbound.Result, error) {
				return []func() ([]outbound.Result, error){
					func() ([]outbound.Result, error) { return []outbound.Result{{Success: false}}, nil },
				}
			},
			expectedRounds: 1,
		},
		{
			name: "transport error in share round",
			rounds: func(src *ERC7540Source) []func() ([]outbound.Result, error) {
				return []func() ([]outbound.Result, error){
					func() ([]outbound.Result, error) { return nil, errors.New("rpc down") },
				}
			},
			expectedRounds: 1,
		},
		{
			name: "short result slice in share round",
			rounds: func(src *ERC7540Source) []func() ([]outbound.Result, error) {
				return []func() ([]outbound.Result, error){
					func() ([]outbound.Result, error) { return []outbound.Result{}, nil },
				}
			},
			expectedRounds: 1,
		},
		{
			name: "undecodable share return data",
			rounds: func(src *ERC7540Source) []func() ([]outbound.Result, error) {
				return []func() ([]outbound.Result, error){
					func() ([]outbound.Result, error) {
						return []outbound.Result{{Success: true, ReturnData: []byte{0x01, 0x02}}}, nil
					},
				}
			},
			expectedRounds: 1,
		},
		{
			name: "zero share address",
			rounds: func(src *ERC7540Source) []func() ([]outbound.Result, error) {
				return []func() ([]outbound.Result, error){
					func() ([]outbound.Result, error) {
						return []outbound.Result{{Success: true, ReturnData: packShareOutput(t, src, common.Address{})}}, nil
					},
				}
			},
			expectedRounds: 1,
		},
		{
			name: "balanceOf call reverted",
			rounds: func(src *ERC7540Source) []func() ([]outbound.Result, error) {
				return []func() ([]outbound.Result, error){
					func() ([]outbound.Result, error) {
						return []outbound.Result{{Success: true, ReturnData: packShareOutput(t, src, share)}}, nil
					},
					func() ([]outbound.Result, error) { return []outbound.Result{{Success: false}}, nil },
				}
			},
			expectedRounds: 2,
		},
		{
			name: "transport error in balanceOf round",
			rounds: func(src *ERC7540Source) []func() ([]outbound.Result, error) {
				return []func() ([]outbound.Result, error){
					func() ([]outbound.Result, error) {
						return []outbound.Result{{Success: true, ReturnData: packShareOutput(t, src, share)}}, nil
					},
					func() ([]outbound.Result, error) { return nil, errors.New("rpc down") },
				}
			},
			expectedRounds: 2,
		},
		{
			name: "short result slice in balanceOf round",
			rounds: func(src *ERC7540Source) []func() ([]outbound.Result, error) {
				return []func() ([]outbound.Result, error){
					func() ([]outbound.Result, error) {
						return []outbound.Result{{Success: true, ReturnData: packShareOutput(t, src, share)}}, nil
					},
					func() ([]outbound.Result, error) { return []outbound.Result{}, nil },
				}
			},
			expectedRounds: 2,
		},
		{
			name: "undecodable balanceOf return data",
			rounds: func(src *ERC7540Source) []func() ([]outbound.Result, error) {
				return []func() ([]outbound.Result, error){
					func() ([]outbound.Result, error) {
						return []outbound.Result{{Success: true, ReturnData: packShareOutput(t, src, share)}}, nil
					},
					func() ([]outbound.Result, error) {
						return []outbound.Result{{Success: true, ReturnData: []byte{0x01, 0x02}}}, nil
					},
				}
			},
			expectedRounds: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mc := testutil.NewMockMulticaller()
			src := newTestERC7540Source(t, mc)
			rounds := tc.rounds(src)

			mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
				if mc.CallCount > len(rounds) {
					t.Fatalf("unexpected multicall round %d", mc.CallCount)
				}
				return rounds[mc.CallCount-1]()
			}

			entries := []*TokenEntry{{
				ContractAddress: common.HexToAddress("0xaaaa"),
				WalletAddress:   common.HexToAddress("0xbbbb"),
				TokenType:       "centrifuge",
			}}

			results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
			if err == nil {
				t.Fatal("expected error")
			}
			if results != nil {
				t.Fatal("expected nil results on failure")
			}
			if mc.CallCount != tc.expectedRounds {
				t.Fatalf("multicall rounds = %d, want %d", mc.CallCount, tc.expectedRounds)
			}
		})
	}
}

// TestERC7540Source_FetchBalances_PinsToBlockHash asserts the share-resolution
// and balance reads are pinned to blockHash (VEC-471).
func TestERC7540Source_FetchBalances_PinsToBlockHash(t *testing.T) {
	share := common.HexToAddress("0xcccc")

	mc := testutil.NewMockMulticaller()
	src := newTestERC7540Source(t, mc)

	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		if blockHash != testBlockHash {
			t.Fatalf("blockHash = %v, want %v", blockHash, testBlockHash)
		}
		if mc.CallCount == 1 {
			return []outbound.Result{{Success: true, ReturnData: packShareOutput(t, src, share)}}, nil
		}
		return []outbound.Result{{Success: true, ReturnData: packBalanceOutput(t, src, big.NewInt(7))}}, nil
	}

	entries := []*TokenEntry{{
		ContractAddress: common.HexToAddress("0xaaaa"),
		WalletAddress:   common.HexToAddress("0xbbbb"),
		TokenType:       "centrifuge",
	}}

	results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
	if err != nil {
		t.Fatalf("FetchBalances failed: %v", err)
	}
	if got := results.Balances[entries[0].Key()]; got == nil || got.Balance.Cmp(big.NewInt(7)) != 0 {
		t.Fatalf("balance = %v, want 7", got)
	}
}

func TestERC7540Source_FetchBalances_EmptyEntries(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	src := newTestERC7540Source(t, mc)

	results, err := src.FetchBalances(context.Background(), nil, testBlockHash)
	if err != nil {
		t.Fatalf("FetchBalances failed: %v", err)
	}
	if len(results.Balances) != 0 || len(results.Supplies) != 0 {
		t.Fatal("expected empty result for empty entries")
	}
	if mc.CallCount != 0 {
		t.Fatalf("expected no multicalls for empty entries, got %d", mc.CallCount)
	}
}
