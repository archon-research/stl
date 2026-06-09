package maple_indexer

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// mustNewBlockchainService builds a BlockchainService for tests, failing fast if
// construction (ABI loading, validation) errors so downstream assertions never
// run against a nil service. chunkSize 0 exercises the <=0 fallback to the
// default multicall chunk size.
func mustNewBlockchainService(t *testing.T, mc outbound.Multicaller) *BlockchainService {
	t.Helper()
	return mustNewBlockchainServiceChunk(t, mc, 0)
}

// mustNewBlockchainServiceChunk builds a BlockchainService with an explicit
// multicall chunk size for chunking-specific tests.
func mustNewBlockchainServiceChunk(t *testing.T, mc outbound.Multicaller, chunkSize int) *BlockchainService {
	t.Helper()
	bs, err := NewBlockchainService(mc, nil, chunkSize)
	if err != nil {
		t.Fatalf("NewBlockchainService failed: %v", err)
	}
	return bs
}

func TestNewBlockchainService_RejectsNilMulticaller(t *testing.T) {
	if _, err := NewBlockchainService(nil, nil, DefaultMulticallChunkSize); err == nil {
		t.Fatal("expected error for nil multicaller")
	}
}

func TestNewBlockchainService_PrePacksNoArgViews(t *testing.T) {
	bs, err := NewBlockchainService(&multicallStub{}, nil, DefaultMulticallChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(bs.totalAssetsData) == 0 || len(bs.totalSupplyData) == 0 {
		t.Fatal("pre-packed call data is empty")
	}
	if bytes.Equal(bs.totalAssetsData, bs.totalSupplyData) {
		t.Fatal("totalAssets == totalSupply selector")
	}
}

func TestFetchVaultState_DecodesAllFields(t *testing.T) {
	mc := &multicallStub{
		Responses: [][]byte{
			encodeUint256(big.NewInt(1_000_000_000_000)), // totalAssets
			encodeUint256(big.NewInt(900_000_000_000)),   // totalSupply
			encodeUint256(big.NewInt(1_111_111)),         // convertToAssets(1e6)
		},
	}
	bs, err := NewBlockchainService(mc, nil, DefaultMulticallChunkSize)
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

// makeAddrs builds n distinct, deterministic addresses by encoding the index
// into the trailing bytes — enough to key a position map without collisions.
func makeAddrs(n int) []common.Address {
	addrs := make([]common.Address, n)
	for i := range addrs {
		var a common.Address
		a[18] = byte(i >> 8)
		a[19] = byte(i)
		addrs[i] = a
	}
	return addrs
}

// TestFetchUserPositions_ChunksBothPasses proves the balanceOf and
// convertToAssets passes are each split into ceil(n/chunkSize) multicalls and
// that the stitched result map is complete and correctly ordered.
func TestFetchUserPositions_ChunksBothPasses(t *testing.T) {
	const chunkSize = 100
	const n = 250
	users := makeAddrs(n)

	responses := make([][]byte, 0, 2*n)
	// balanceOf pass: balance = i+1 (all non-zero).
	for i := range n {
		responses = append(responses, encodeUint256(big.NewInt(int64(i+1))))
	}
	// convertToAssets pass: assets = (i+1)*2.
	for i := range n {
		responses = append(responses, encodeUint256(big.NewInt(int64((i+1)*2))))
	}

	mc := &multicallStub{Responses: responses}
	bs := mustNewBlockchainServiceChunk(t, mc, chunkSize)

	out, err := bs.FetchUserPositions(context.Background(), common.HexToAddress(syrupUSDCAddr), users, big.NewInt(18_500_000))
	if err != nil {
		t.Fatalf("FetchUserPositions: %v", err)
	}
	if len(out) != n {
		t.Fatalf("got %d positions, want %d", len(out), n)
	}
	// ceil(250/100) = 3 balanceOf batches + 3 convertToAssets batches = 6.
	if len(mc.Calls) != 6 {
		t.Fatalf("got %d multicall batches, want 6", len(mc.Calls))
	}
	// Each chunk except the last is full size; the last is the remainder.
	wantSizes := []int{100, 100, 50, 100, 100, 50}
	for i, calls := range mc.Calls {
		if len(calls) != wantSizes[i] {
			t.Fatalf("batch %d size=%d, want %d", i, len(calls), wantSizes[i])
		}
	}
	for i, u := range users {
		pos, ok := out[u]
		if !ok {
			t.Fatalf("user %d (%s) missing from result", i, u.Hex())
		}
		if pos.Shares.Cmp(big.NewInt(int64(i+1))) != 0 {
			t.Fatalf("user %d shares=%s, want %d", i, pos.Shares, i+1)
		}
		if pos.Assets.Cmp(big.NewInt(int64((i+1)*2))) != 0 {
			t.Fatalf("user %d assets=%s, want %d", i, pos.Assets, (i+1)*2)
		}
	}
}

// TestFetchUserPositions_ZeroBalanceShortCircuitAcrossChunks confirms zero-share
// users are resolved locally (Assets=0, no convertToAssets call) even when the
// non-zero accounts span a chunk boundary.
func TestFetchUserPositions_ZeroBalanceShortCircuitAcrossChunks(t *testing.T) {
	const chunkSize = 2
	users := makeAddrs(5)
	// balances: non-zero at indices 0, 2, 4 → 3 convertToAssets calls spanning
	// two asset chunks (2 + 1).
	balances := []int64{1, 0, 2, 0, 3}

	responses := make([][]byte, 0, len(balances)+3)
	for _, b := range balances {
		responses = append(responses, encodeUint256(big.NewInt(b)))
	}
	// convertToAssets for the 3 non-zero balances, in order: assets = shares*10.
	for _, b := range balances {
		if b != 0 {
			responses = append(responses, encodeUint256(big.NewInt(b*10)))
		}
	}

	mc := &multicallStub{Responses: responses}
	bs := mustNewBlockchainServiceChunk(t, mc, chunkSize)

	out, err := bs.FetchUserPositions(context.Background(), common.HexToAddress(syrupUSDCAddr), users, big.NewInt(1))
	if err != nil {
		t.Fatalf("FetchUserPositions: %v", err)
	}
	if len(out) != 5 {
		t.Fatalf("got %d positions, want 5", len(out))
	}
	// balanceOf: ceil(5/2)=3 batches. convertToAssets: ceil(3/2)=2 batches.
	if len(mc.Calls) != 5 {
		t.Fatalf("got %d multicall batches, want 5", len(mc.Calls))
	}
	for i, b := range balances {
		pos := out[users[i]]
		if pos == nil {
			t.Fatalf("user %d missing", i)
		}
		if pos.Shares.Cmp(big.NewInt(b)) != 0 {
			t.Fatalf("user %d shares=%s, want %d", i, pos.Shares, b)
		}
		wantAssets := int64(0)
		if b != 0 {
			wantAssets = b * 10
		}
		if pos.Assets.Cmp(big.NewInt(wantAssets)) != 0 {
			t.Fatalf("user %d assets=%s, want %d", i, pos.Assets, wantAssets)
		}
	}
}

// TestFetchUserPositions_FailsHardOnChunkError confirms a mid-pass chunk failure
// aborts the whole call with no partial map.
func TestFetchUserPositions_FailsHardOnChunkError(t *testing.T) {
	const chunkSize = 2
	users := makeAddrs(5)
	responses := make([][]byte, 0, 5)
	for i := range 5 {
		responses = append(responses, encodeUint256(big.NewInt(int64(i+1))))
	}
	// Fail on the 2nd balanceOf chunk.
	mc := &multicallStub{Responses: responses, FailAtCall: 2}
	bs := mustNewBlockchainServiceChunk(t, mc, chunkSize)

	out, err := bs.FetchUserPositions(context.Background(), common.HexToAddress(syrupUSDCAddr), users, big.NewInt(1))
	if err == nil {
		t.Fatal("expected error on chunk failure")
	}
	if out != nil {
		t.Fatalf("expected nil map on failure, got %d entries", len(out))
	}
}

// TestConfigApplyDefaults_ChunkSize covers both branches of the chunk-size
// default: a non-positive value falls back to the default, a positive value is
// left untouched.
func TestConfigApplyDefaults_ChunkSize(t *testing.T) {
	var zero Config
	zero.ApplyDefaults()
	if zero.MulticallChunkSize != DefaultMulticallChunkSize {
		t.Fatalf("zero chunk size = %d, want default %d", zero.MulticallChunkSize, DefaultMulticallChunkSize)
	}

	custom := Config{MulticallChunkSize: 42}
	custom.ApplyDefaults()
	if custom.MulticallChunkSize != 42 {
		t.Fatalf("custom chunk size = %d, want 42 (left untouched)", custom.MulticallChunkSize)
	}
}

// shortChunkMulticaller returns one fewer result than requested on every
// Execute, exercising the per-chunk length guard in executeChunked — a mismatch
// the call-site aggregate check could miss if a later chunk over-returned.
type shortChunkMulticaller struct{}

func (s *shortChunkMulticaller) Execute(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	out := make([]outbound.Result, 0, len(calls))
	for i := 1; i < len(calls); i++ {
		out = append(out, outbound.Result{Success: true, ReturnData: encodeUint256(big.NewInt(1))})
	}
	return out, nil
}

func (s *shortChunkMulticaller) Address() common.Address { return common.Address{} }

// TestFetchUserPositions_FailsOnChunkLengthMismatch confirms a chunk returning
// the wrong number of results aborts the call rather than silently shifting
// later users' positions.
func TestFetchUserPositions_FailsOnChunkLengthMismatch(t *testing.T) {
	bs := mustNewBlockchainServiceChunk(t, &shortChunkMulticaller{}, 2)
	out, err := bs.FetchUserPositions(context.Background(), common.HexToAddress(syrupUSDCAddr), makeAddrs(5), big.NewInt(1))
	if err == nil {
		t.Fatal("expected error when a chunk returns a mismatched result count")
	}
	if out != nil {
		t.Fatalf("expected nil map on failure, got %d entries", len(out))
	}
}

// TestNewBlockchainService_ChunkSizeFallback proves a non-positive chunk size
// falls back to the default (100) rather than producing zero-size chunks.
func TestNewBlockchainService_ChunkSizeFallback(t *testing.T) {
	if got := ConfigDefaults().MulticallChunkSize; got != DefaultMulticallChunkSize {
		t.Fatalf("ConfigDefaults().MulticallChunkSize=%d, want %d", got, DefaultMulticallChunkSize)
	}

	const n = 150
	users := makeAddrs(n)
	responses := make([][]byte, 0, 2*n)
	for i := range n {
		responses = append(responses, encodeUint256(big.NewInt(int64(i+1))))
	}
	for i := range n {
		responses = append(responses, encodeUint256(big.NewInt(int64(i+1))))
	}

	mc := &multicallStub{Responses: responses}
	bs := mustNewBlockchainServiceChunk(t, mc, 0) // 0 → fall back to 100

	if _, err := bs.FetchUserPositions(context.Background(), common.HexToAddress(syrupUSDCAddr), users, big.NewInt(1)); err != nil {
		t.Fatalf("FetchUserPositions: %v", err)
	}
	// ceil(150/100)=2 balanceOf batches + 2 convertToAssets batches = 4.
	if len(mc.Calls) != 4 {
		t.Fatalf("got %d multicall batches, want 4 (chunk size fell back to 100)", len(mc.Calls))
	}
}
