package main

import (
	"context"
	"errors"
	"log/slog"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func newTestResolver(t *testing.T, mc outbound.Multicaller) *erc4626ArchiveResolver {
	t.Helper()
	r, err := newERC4626ArchiveResolver()
	if err != nil {
		t.Fatalf("newERC4626ArchiveResolver: %v", err)
	}
	r.multicallers[1] = mc // pre-populated so resolve() never dials a real archive RPC
	return r
}

func packConvertToAssets(t *testing.T, assets *big.Int) []byte {
	t.Helper()
	erc4626ABI, err := abis.GetERC4626ABI()
	if err != nil {
		t.Fatalf("GetERC4626ABI: %v", err)
	}
	data, err := erc4626ABI.Methods["convertToAssets"].Outputs.Pack(assets)
	if err != nil {
		t.Fatalf("packing convertToAssets output: %v", err)
	}
	return data
}

func TestERC4626ArchiveResolver_BatchesOneMulticallPerBlock(t *testing.T) {
	rowA := erc4626Candidate() // block_number 0
	rowA.blockNumber = 100
	rowB := erc4626Candidate()
	rowB.blockNumber = 100 // same block as rowA: must share one multicall
	rowC := erc4626Candidate()
	rowC.blockNumber = 200 // different block: its own multicall

	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i := range calls {
			results[i] = outbound.Result{Success: true, ReturnData: packConvertToAssets(t, blockNumber)}
		}
		return results, nil
	}

	r := newTestResolver(t, mock)
	got, err := r.resolve(context.Background(), []candidateRow{rowA, rowB, rowC}, slog.Default())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if len(mock.Invocations) != 2 {
		t.Fatalf("multicall invocations = %d, want 2 (one per distinct block)", len(mock.Invocations))
	}
	for _, inv := range mock.Invocations {
		if inv.BlockNumber.Int64() == 100 && len(inv.Calls) != 2 {
			t.Errorf("block 100 multicall carried %d calls, want 2 (rowA + rowB batched)", len(inv.Calls))
		}
	}
	if got[0].Cmp(big.NewInt(100)) != 0 || got[1].Cmp(big.NewInt(100)) != 0 {
		t.Errorf("rows sharing block 100 = %s, %s, want both 100", got[0], got[1])
	}
	if got[2].Cmp(big.NewInt(200)) != 0 {
		t.Errorf("row at block 200 = %s, want 200", got[2])
	}
}

func TestERC4626ArchiveResolver_RevertedCallIsOmittedNotErrored(t *testing.T) {
	row := erc4626Candidate()
	row.blockNumber = 100

	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{Success: false}}, nil // vault call reverted
	}

	r := newTestResolver(t, mock)
	got, err := r.resolve(context.Background(), []candidateRow{row}, slog.Default())
	if err != nil {
		t.Fatalf("a reverted per-row call must not fail the whole resolve: %v", err)
	}
	if _, ok := got[0]; ok {
		t.Error("reverted row must be absent from the result map so the caller falls back to price-ratio")
	}
}

func TestERC4626ArchiveResolver_TransportErrorPropagates(t *testing.T) {
	row := erc4626Candidate()
	row.blockNumber = 100

	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("connection reset")
	}

	r := newTestResolver(t, mock)
	_, err := r.resolve(context.Background(), []candidateRow{row}, slog.Default())
	if err == nil {
		t.Fatal("a transport-level failure must abort resolve, not be swallowed")
	}
}

// TestERC4626ArchiveResolver_DecodeErrorIsOmittedNotErrored guards B4's
// "adjacent" finding: Success:true with empty or malformed returndata (an
// uninitialised proxy, a non-4626 contract at that address) is a per-vault
// historical fact, exactly like a revert -- it must not abort the whole run.
func TestERC4626ArchiveResolver_DecodeErrorIsOmittedNotErrored(t *testing.T) {
	row := erc4626Candidate()
	row.blockNumber = 100

	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{Success: true, ReturnData: []byte{0x01, 0x02}}}, nil // malformed
	}

	r := newTestResolver(t, mock)
	got, err := r.resolve(context.Background(), []candidateRow{row}, slog.Default())
	if err != nil {
		t.Fatalf("a per-vault decode failure must not fail the whole resolve: %v", err)
	}
	if _, ok := got[0]; ok {
		t.Error("undecodable row must be absent from the result map so the caller falls back to price-ratio")
	}
}

// TestERC4626ArchiveResolver_ZeroAssetsForNonzeroBalanceIsOmittedNotTrusted
// guards B4: a live erc4626 vault does not plausibly convert a nonzero share
// balance to zero assets, and that is exactly what 32 zero-byte returndata
// from a paused vault or uninitialised proxy decodes to -- so it must not be
// trusted as an observation.
func TestERC4626ArchiveResolver_ZeroAssetsForNonzeroBalanceIsOmittedNotTrusted(t *testing.T) {
	row := erc4626Candidate()
	row.blockNumber = 100
	row.balance = big.NewInt(1_000_000) // nonzero shares

	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{Success: true, ReturnData: packConvertToAssets(t, big.NewInt(0))}}, nil
	}

	r := newTestResolver(t, mock)
	got, err := r.resolve(context.Background(), []candidateRow{row}, slog.Default())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if _, ok := got[0]; ok {
		t.Error("an implausible zero for a nonzero balance must fall back to price-ratio, not be trusted")
	}
}

// TestERC4626ArchiveResolver_ZeroAssetsForZeroBalanceIsTrusted is the B4
// counterpart: zero shares converting to zero assets is a real, emptied
// position, not an implausibility signal, and must be trusted.
func TestERC4626ArchiveResolver_ZeroAssetsForZeroBalanceIsTrusted(t *testing.T) {
	row := erc4626Candidate()
	row.blockNumber = 100
	row.balance = big.NewInt(0)

	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{Success: true, ReturnData: packConvertToAssets(t, big.NewInt(0))}}, nil
	}

	r := newTestResolver(t, mock)
	got, err := r.resolve(context.Background(), []candidateRow{row}, slog.Default())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	assets, ok := got[0]
	if !ok || assets.Sign() != 0 {
		t.Errorf("got[0] = %v, ok=%v, want a trusted explicit 0 for a genuinely empty position", assets, ok)
	}
}

// TestERC4626ArchiveResolver_MixedOutcomeBatch covers a batch sharing one
// block where some rows succeed and others revert, guarding that a failure on
// one call in the multicall never contaminates a sibling call's own result.
func TestERC4626ArchiveResolver_MixedOutcomeBatch(t *testing.T) {
	rowOK := erc4626Candidate()
	rowOK.blockNumber = 100
	rowReverted := erc4626Candidate()
	rowReverted.blockNumber = 100
	rowReverted.tokenAddress = common.HexToAddress("0x2222222222222222222222222222222222222222")

	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{
			{Success: true, ReturnData: packConvertToAssets(t, big.NewInt(500))},
			{Success: false},
		}, nil
	}

	r := newTestResolver(t, mock)
	got, err := r.resolve(context.Background(), []candidateRow{rowOK, rowReverted}, slog.Default())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if assets, ok := got[0]; !ok || assets.Cmp(big.NewInt(500)) != 0 {
		t.Errorf("got[0] = %v, ok=%v, want the successful sibling call's own result 500", assets, ok)
	}
	if _, ok := got[1]; ok {
		t.Error("got[1] must be absent: this row's own call reverted, regardless of its sibling's success")
	}
}

func TestERC4626ArchiveResolver_SkipsNonERC4626Rows(t *testing.T) {
	direct := baseCandidate()
	direct.isReceiptToken = false

	underlying := testUnderlying
	aToken := baseCandidate()
	aToken.isReceiptToken = true
	aToken.underlyingIsOneToOne = true
	aToken.underlyingAddress = &underlying

	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		t.Fatal("no multicall should be issued when no row is erc4626-like")
		return nil, nil
	}

	r := newTestResolver(t, mock)
	got, err := r.resolve(context.Background(), []candidateRow{direct, aToken}, slog.Default())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d archive results, want 0", len(got))
	}
}

func TestERC4626ArchiveResolver_UsesTheRowsOwnVaultAddressAsTarget(t *testing.T) {
	row := erc4626Candidate()
	row.blockNumber = 100
	row.tokenAddress = common.HexToAddress("0x9999999999999999999999999999999999999999")

	var gotTarget common.Address
	mock := testutil.NewMockMulticaller()
	mock.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		gotTarget = calls[0].Target
		return []outbound.Result{{Success: true, ReturnData: packConvertToAssets(t, big.NewInt(1))}}, nil
	}

	r := newTestResolver(t, mock)
	if _, err := r.resolve(context.Background(), []candidateRow{row}, slog.Default()); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if gotTarget != row.tokenAddress {
		t.Errorf("multicall target = %s, want the row's own vault address %s", gotTarget.Hex(), row.tokenAddress.Hex())
	}
}
