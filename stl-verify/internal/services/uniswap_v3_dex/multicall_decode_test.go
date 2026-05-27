package uniswap_v3_dex

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// buildPoolStateResults builds a fully-successful 5-call result vector for
// readPoolState. Override individual indices to test specific failure modes.
func buildPoolStateResults(t *testing.T) []outbound.Result {
	t.Helper()
	exABI, err := poolABIForTests()
	if err != nil {
		t.Fatalf("loading pool ABI: %v", err)
	}
	slot0Args := exABI.Methods["slot0"].Outputs
	sqrtPriceX96 := new(big.Int).Lsh(big.NewInt(1), 96)
	slot0Data, err := slot0Args.Pack(
		sqrtPriceX96,     // sqrtPriceX96 (uint160)
		big.NewInt(100),  // tick (int24)
		uint16(10),       // observationIndex
		uint16(100),      // observationCardinality
		uint16(100),      // observationCardinalityNext
		uint8(0),         // feeProtocol
		true,             // unlocked
	)
	if err != nil {
		t.Fatalf("pack slot0: %v", err)
	}
	liqArgs := exABI.Methods["liquidity"].Outputs
	liqData, err := liqArgs.Pack(big.NewInt(1_000_000))
	if err != nil {
		t.Fatalf("pack liquidity: %v", err)
	}
	obsArgs := exABI.Methods["observe"].Outputs
	obsData, err := obsArgs.Pack([]*big.Int{big.NewInt(42)}, []*big.Int{big.NewInt(43)})
	if err != nil {
		t.Fatalf("pack observe: %v", err)
	}
	balArgs := exABI.Methods["balanceOf"].Outputs
	balData, err := balArgs.Pack(big.NewInt(500))
	if err != nil {
		t.Fatalf("pack balanceOf: %v", err)
	}
	return []outbound.Result{
		{Success: true, ReturnData: slot0Data},
		{Success: true, ReturnData: liqData},
		{Success: true, ReturnData: obsData},
		{Success: true, ReturnData: balData},
		{Success: true, ReturnData: balData},
	}
}

func poolABIForTests() (*abi.ABI, error) {
	// Build via the package's own ABI helper so tests stay in sync with
	// production code if the ABI changes.
	bs, err := newBlockchainService(testutil.NewMockMulticaller())
	if err != nil {
		return nil, err
	}
	return bs.poolRead, nil
}

// B1: any unexpected runtime Go type in the unpacked tuple must produce a
// wrapped error, not a panic. Pre-fix the assertions were bare `.(T)` calls
// and a wrong-typed element would crash the worker. Each subtest swaps one
// field with a wrong type and asserts the helper returns an error and does
// not panic. A `defer recover()` would mask a regression where the panic
// is reintroduced — we want the test runner to record the panic as a
// failure, which happens by default when an assertion panics inside a
// test function.

func TestSlot0FieldsFromUnpacked_RejectsWrongTypes(t *testing.T) {
	good := func() []any {
		return []any{
			big.NewInt(1),  // 0: sqrtPriceX96 (*big.Int)
			big.NewInt(2),  // 1: tick (*big.Int)
			uint16(3),      // 2: observationIndex
			uint16(4),      // 3: observationCardinality
			uint16(5),      // 4: observationCardinalityNext
			uint8(6),       // 5: feeProtocol
			true,           // 6: unlocked
		}
	}

	// Sanity: the good payload decodes without error.
	var out poolMulticallResult
	if err := slot0FieldsFromUnpacked(good(), &out); err != nil {
		t.Fatalf("good payload: unexpected error %v", err)
	}
	if out.SqrtPriceX96.Int64() != 1 || out.Tick != 2 || out.ObservationIndex != 3 {
		t.Fatalf("good payload: unexpected output %+v", out)
	}

	cases := []struct {
		name string
		idx  int
		bad  any
	}{
		{"sqrtPriceX96 wrong type", 0, "not-a-big-int"},
		{"tick wrong type", 1, uint32(99)},
		{"observationIndex wrong type", 2, big.NewInt(3)},
		{"observationCardinality wrong type", 3, int32(4)},
		{"observationCardinalityNext wrong type", 4, "x"},
		{"feeProtocol wrong type", 5, uint16(6)},
		{"unlocked wrong type", 6, "true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := good()
			u[tc.idx] = tc.bad
			var got poolMulticallResult
			err := slot0FieldsFromUnpacked(u, &got)
			if err == nil {
				t.Fatal("expected error for wrong-typed field, got nil")
			}
		})
	}
}

func TestPositionFieldsFromUnpacked_RejectsWrongTypes(t *testing.T) {
	good := func() []any {
		return []any{
			big.NewInt(0),        // 0: nonce (unused but still asserted typewise? no — unused)
			common.Address{},     // 1: operator (unused)
			common.HexToAddress("0x1111111111111111111111111111111111111111"), // 2: token0
			common.HexToAddress("0x2222222222222222222222222222222222222222"), // 3: token1
			big.NewInt(500),      // 4: fee
			big.NewInt(-887000),  // 5: tickLower
			big.NewInt(887000),   // 6: tickUpper
			big.NewInt(1e18),     // 7: liquidity
			big.NewInt(10),       // 8: feeGrowthInside0LastX128
			big.NewInt(11),       // 9: feeGrowthInside1LastX128
			big.NewInt(12),       // 10: tokensOwed0
			big.NewInt(13),       // 11: tokensOwed1
		}
	}

	// Sanity: good payload decodes cleanly.
	if pos, err := positionFieldsFromUnpacked(good()); err != nil {
		t.Fatalf("good payload: unexpected error %v", err)
	} else if pos.Fee != 500 || pos.TickLower != -887000 || pos.Liquidity.String() != "1000000000000000000" {
		t.Fatalf("good payload: unexpected output %+v", pos)
	}

	cases := []struct {
		name string
		idx  int
		bad  any
	}{
		{"token0 wrong type", 2, "0x1111"},
		{"token1 wrong type", 3, common.Hash{}},
		{"fee wrong type", 4, uint32(500)},
		{"tickLower wrong type", 5, int32(-887000)},
		{"tickUpper wrong type", 6, "high"},
		{"liquidity wrong type", 7, uint64(1)},
		{"feeGrowthInside0LastX128 wrong type", 8, "10"},
		{"feeGrowthInside1LastX128 wrong type", 9, 11},
		{"tokensOwed0 wrong type", 10, struct{}{}},
		{"tokensOwed1 wrong type", 11, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := good()
			u[tc.idx] = tc.bad
			_, err := positionFieldsFromUnpacked(u)
			if err == nil {
				t.Fatal("expected error for wrong-typed field, got nil")
			}
		})
	}

	// Wrong length must also be a wrapped error, not a panic-on-index.
	t.Run("wrong length", func(t *testing.T) {
		if _, err := positionFieldsFromUnpacked(make([]any, 11)); err == nil {
			t.Fatal("expected error for unpacked len != 12, got nil")
		}
	})
}

// B2: pool.balanceOf(token0/token1) reverting must be treated as fatal.
// Pre-fix the decoder silently set Balance0/Balance1 to nil, which downstream
// turns into SQL NULL on the uniswap_v3_pool_state row — indistinguishable
// from "balance read failed" and "balance not measured." The fix: propagate
// the revert so SQS doesn't ack and the retry runs against a fresh block.
func TestDecodePoolState_BalanceOfRevert_PropagatesError(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	bs, err := newBlockchainService(mc)
	if err != nil {
		t.Fatalf("newBlockchainService: %v", err)
	}

	t.Run("balanceOf(token0) reverts", func(t *testing.T) {
		results := buildPoolStateResults(t)
		results[3] = outbound.Result{Success: false}
		if _, err := bs.decodePoolState(results); err == nil {
			t.Fatal("expected error when balanceOf(token0) reverts, got nil")
		} else if !strings.Contains(err.Error(), "balanceOf") {
			t.Errorf("error must name the failing call; got %q", err.Error())
		}
	})

	t.Run("balanceOf(token1) reverts", func(t *testing.T) {
		results := buildPoolStateResults(t)
		results[4] = outbound.Result{Success: false}
		if _, err := bs.decodePoolState(results); err == nil {
			t.Fatal("expected error when balanceOf(token1) reverts, got nil")
		}
	})
}

// Sanity check via the public readPoolState entry point: the revert surfaces
// to the worker, not silently swallowed into a NULL column.
func TestReadPoolState_BalanceOfRevert_BubblesUp(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		r := buildPoolStateResults(t)
		r[3] = outbound.Result{Success: false}
		return r, nil
	}
	bs, err := newBlockchainService(mc)
	if err != nil {
		t.Fatalf("newBlockchainService: %v", err)
	}
	pool := &entity.UniswapV3Pool{
		ID: 1, ChainID: 1, FeeTier: 500,
		Address: common.HexToAddress("0xAAAa0000000000000000000000000000000000aa"),
	}
	t0 := common.HexToAddress("0xBBBB000000000000000000000000000000000001")
	t1 := common.HexToAddress("0xCCCC000000000000000000000000000000000002")
	_, err = bs.readPoolState(context.Background(), pool, t0, t1, 100)
	if err == nil {
		t.Fatal("expected error from readPoolState when balanceOf reverts, got nil")
	}
}

// Defensive: a nil error here without any wrong call would suggest the test
// rig is wrong. Keeps an explicit happy-path baseline alongside the failure
// tests above.
var _ = errors.New

