package maple_indexer

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// mustNewBlockchainService builds a BlockchainService for tests, failing fast if
// construction (ABI loading, validation) errors so downstream assertions never
// run against a nil service.
func mustNewBlockchainService(t *testing.T, mc outbound.Multicaller) *BlockchainService {
	t.Helper()
	bs, err := NewBlockchainService(mc, nil)
	if err != nil {
		t.Fatalf("NewBlockchainService failed: %v", err)
	}
	return bs
}

func TestNewBlockchainService_RejectsNilMulticaller(t *testing.T) {
	if _, err := NewBlockchainService(nil, nil); err == nil {
		t.Fatal("expected error for nil multicaller")
	}
}

func TestNewBlockchainService_PrePacksNoArgViews(t *testing.T) {
	bs, err := NewBlockchainService(&multicallStub{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(bs.totalAssetsData) == 0 || len(bs.totalSupplyData) == 0 {
		t.Fatal("pre-packed call data is empty")
	}
	if bytesEqual(bs.totalAssetsData, bs.totalSupplyData) {
		t.Fatal("totalAssets == totalSupply selector")
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestFetchVaultState_DecodesAllFields(t *testing.T) {
	mc := &multicallStub{
		Responses: [][]byte{
			encodeUint256(big.NewInt(1_000_000_000_000)), // totalAssets
			encodeUint256(big.NewInt(900_000_000_000)),   // totalSupply
			encodeUint256(big.NewInt(1_111_111)),         // convertToAssets(1e6)
		},
	}
	bs, err := NewBlockchainService(mc, nil)
	if err != nil {
		t.Fatal(err)
	}
	vault := common.HexToAddress(syrupUSDCAddr)
	state, err := bs.FetchVaultState(context.Background(), vault, 6, big.NewInt(18_500_000))
	if err != nil {
		t.Fatalf("FetchVaultState: %v", err)
	}
	if state.TotalAssets.Cmp(big.NewInt(1_000_000_000_000)) != 0 {
		t.Fatalf("TotalAssets=%s", state.TotalAssets)
	}
	if state.TotalSupply.Cmp(big.NewInt(900_000_000_000)) != 0 {
		t.Fatalf("TotalSupply=%s", state.TotalSupply)
	}
	if state.SharePrice.Cmp(big.NewInt(1_111_111)) != 0 {
		t.Fatalf("SharePrice=%s", state.SharePrice)
	}
	// Confirm we sent exactly one multicall with 3 calls.
	if len(mc.Calls) != 1 {
		t.Fatalf("expected 1 multicall batch, got %d", len(mc.Calls))
	}
	if len(mc.Calls[0]) != 3 {
		t.Fatalf("expected 3 calls, got %d", len(mc.Calls[0]))
	}
	// All calls must target the vault.
	for i, c := range mc.Calls[0] {
		if c.Target != vault {
			t.Fatalf("call %d target=%s, want %s", i, c.Target.Hex(), vault.Hex())
		}
	}
}

// TestFetchVaultState_PacksShareUnitFromDecimals proves the convertToAssets
// argument scales with the vault's decimals: a 6-decimal vault asks for
// convertToAssets(1e6), an 18-decimal vault for convertToAssets(1e18). A bug
// here would mis-scale the share price by 10^(18-6) for a BSC-style vault.
func TestFetchVaultState_PacksShareUnitFromDecimals(t *testing.T) {
	cases := []struct {
		name     string
		decimals uint8
		want     *big.Int
	}{
		{"six decimals", 6, big.NewInt(1_000_000)},
		{"eighteen decimals", 18, new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mc := &multicallStub{
				Responses: [][]byte{
					encodeUint256(big.NewInt(1)), // totalAssets
					encodeUint256(big.NewInt(1)), // totalSupply
					encodeUint256(big.NewInt(1)), // convertToAssets
				},
			}
			bs := mustNewBlockchainService(t, mc)
			vault := common.HexToAddress(syrupUSDCAddr)
			if _, err := bs.FetchVaultState(context.Background(), vault, tc.decimals, big.NewInt(1)); err != nil {
				t.Fatalf("FetchVaultState: %v", err)
			}
			// The convertToAssets call is the 3rd in the batch.
			convData := mc.Calls[0][2].CallData
			args, err := bs.viewABI.Methods["convertToAssets"].Inputs.Unpack(convData[4:])
			if err != nil {
				t.Fatalf("unpacking convertToAssets input: %v", err)
			}
			got, ok := args[0].(*big.Int)
			if !ok {
				t.Fatalf("convertToAssets arg type %T", args[0])
			}
			if got.Cmp(tc.want) != 0 {
				t.Fatalf("share unit = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestFetchVaultState_RejectsZeroDecimals confirms we never fall back to a
// default share unit: a zero decimals is a hard error, not a silent 1e6.
func TestFetchVaultState_RejectsZeroDecimals(t *testing.T) {
	bs := mustNewBlockchainService(t, &multicallStub{})
	_, err := bs.FetchVaultState(context.Background(), common.HexToAddress(syrupUSDCAddr), 0, big.NewInt(1))
	if err == nil {
		t.Fatal("expected error for zero decimals")
	}
	if len(bs.multicaller.(*multicallStub).Calls) != 0 {
		t.Fatal("multicaller invoked despite zero decimals — should fail before any RPC")
	}
}

// shortResultMulticaller returns no results regardless of how many calls were
// requested, exercising the result-count-mismatch guard.
type shortResultMulticaller struct{}

func (s *shortResultMulticaller) Execute(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	return nil, nil
}

func (s *shortResultMulticaller) Address() common.Address { return common.Address{} }

func TestFetchVaultState_FailsOnResultCountMismatch(t *testing.T) {
	bs := mustNewBlockchainService(t, &shortResultMulticaller{})
	_, err := bs.FetchVaultState(context.Background(), common.HexToAddress(syrupUSDCAddr), 6, big.NewInt(1))
	if err == nil {
		t.Fatal("expected error when multicall returns fewer results than calls")
	}
}

func TestFetchVaultState_PropagatesMulticallError(t *testing.T) {
	wantErr := errors.New("network down")
	mc := &multicallStub{Err: wantErr}
	bs := mustNewBlockchainService(t, mc)
	_, err := bs.FetchVaultState(context.Background(), common.HexToAddress(syrupUSDCAddr), 6, big.NewInt(1))
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped multicall error, got %v", err)
	}
}

func TestFetchVaultState_FailsOnRevertedCall(t *testing.T) {
	bs := mustNewBlockchainService(t, &multicallStub{})
	mc := &reverteringMulticaller{}
	bs.multicaller = mc
	_, err := bs.FetchVaultState(context.Background(), common.HexToAddress(syrupUSDCAddr), 6, big.NewInt(1))
	if err == nil {
		t.Fatal("expected error on reverted multicall result")
	}
}

// reverteringMulticaller returns successful=false for every call.
type reverteringMulticaller struct{}

func (r *reverteringMulticaller) Execute(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	out := make([]outbound.Result, len(calls))
	for i := range out {
		out[i] = outbound.Result{Success: false}
	}
	return out, nil
}

func (r *reverteringMulticaller) Address() common.Address { return common.Address{} }

func TestFetchUserPositions_EmptyUsers_ShortCircuits(t *testing.T) {
	mc := &multicallStub{}
	bs := mustNewBlockchainService(t, mc)
	out, err := bs.FetchUserPositions(context.Background(), common.HexToAddress(syrupUSDCAddr), nil, big.NewInt(1))
	if err != nil {
		t.Fatal(err)
	}
	if out == nil {
		t.Fatal("nil map — should be empty map")
	}
	if len(out) != 0 {
		t.Fatalf("got %d entries", len(out))
	}
	if len(mc.Calls) != 0 {
		t.Fatal("multicaller was invoked despite empty users — should short-circuit")
	}
}

func TestFetchUserPositions_TwoBatches(t *testing.T) {
	mc := &multicallStub{
		Responses: [][]byte{
			// Batch 1 — balanceOf
			encodeUint256(big.NewInt(500)),
			encodeUint256(big.NewInt(750)),
			// Batch 2 — convertToAssets
			encodeUint256(big.NewInt(550)),
			encodeUint256(big.NewInt(825)),
		},
	}
	bs := mustNewBlockchainService(t, mc)
	vault := common.HexToAddress(syrupUSDCAddr)
	u1 := common.HexToAddress(userA)
	u2 := common.HexToAddress(userB)

	out, err := bs.FetchUserPositions(context.Background(), vault, []common.Address{u1, u2}, big.NewInt(18_500_000))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 positions, got %d", len(out))
	}
	if out[u1].Shares.Cmp(big.NewInt(500)) != 0 {
		t.Fatalf("u1 shares=%s", out[u1].Shares)
	}
	if out[u1].Assets.Cmp(big.NewInt(550)) != 0 {
		t.Fatalf("u1 assets=%s", out[u1].Assets)
	}
	if out[u2].Shares.Cmp(big.NewInt(750)) != 0 {
		t.Fatalf("u2 shares=%s", out[u2].Shares)
	}
	if out[u2].Assets.Cmp(big.NewInt(825)) != 0 {
		t.Fatalf("u2 assets=%s", out[u2].Assets)
	}
	// Two multicalls — one for balanceOf, one for convertToAssets.
	if len(mc.Calls) != 2 {
		t.Fatalf("expected 2 multicall batches, got %d", len(mc.Calls))
	}
}

func TestFetchUserPositions_PropagatesBalanceOfError(t *testing.T) {
	wantErr := errors.New("rpc 502")
	mc := &multicallStub{Err: wantErr}
	bs := mustNewBlockchainService(t, mc)
	_, err := bs.FetchUserPositions(context.Background(),
		common.HexToAddress(syrupUSDCAddr),
		[]common.Address{common.HexToAddress(userA)},
		big.NewInt(1))
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped error, got %v", err)
	}
}

func TestFetchUserPositions_PassesBlockNumberThrough(t *testing.T) {
	mc := &multicallStub{
		Responses: [][]byte{
			encodeUint256(big.NewInt(1)),
			encodeUint256(big.NewInt(2)),
		},
	}
	bs := mustNewBlockchainService(t, mc)
	block := big.NewInt(18_700_000)
	if _, err := bs.FetchUserPositions(context.Background(),
		common.HexToAddress(syrupUSDCAddr),
		[]common.Address{common.HexToAddress(userA)},
		block); err != nil {
		t.Fatal(err)
	}
	if len(mc.BlockNumbers) != 2 {
		t.Fatalf("expected 2 calls (batched), got %d", len(mc.BlockNumbers))
	}
	for i, b := range mc.BlockNumbers {
		if b.Cmp(block) != 0 {
			t.Fatalf("call %d block=%s, want %s", i, b, block)
		}
	}
}
