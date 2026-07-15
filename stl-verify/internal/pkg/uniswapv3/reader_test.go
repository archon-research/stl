package uniswapv3

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Tick math tests
// ---------------------------------------------------------------------------

func TestGetSqrtRatioAtTick_Zero(t *testing.T) {
	// tick 0 → sqrt(1.0001^0) = 1.0 → 1 * 2^96
	result := GetSqrtRatioAtTick(0)
	if result.Cmp(q96) != 0 {
		t.Errorf("GetSqrtRatioAtTick(0) = %s, want %s (2^96)", result, q96)
	}
}

func TestGetSqrtRatioAtTick_Symmetric(t *testing.T) {
	// sqrt(1.0001^tick) * sqrt(1.0001^-tick) should ≈ (2^96)^2
	pos := GetSqrtRatioAtTick(100)
	neg := GetSqrtRatioAtTick(-100)

	product := new(big.Int).Mul(pos, neg)
	q96Squared := new(big.Int).Mul(q96, q96)

	// Allow 0.01% tolerance for float imprecision.
	diff := new(big.Int).Sub(product, q96Squared)
	diff.Abs(diff)

	tolerance := new(big.Int).Div(q96Squared, big.NewInt(10000))
	if diff.Cmp(tolerance) > 0 {
		t.Errorf("tick symmetry violated: pos*neg = %s, want ≈ %s", product, q96Squared)
	}
}

func TestComputePositionAmounts_InRange(t *testing.T) {
	// Stablecoin pair: ticks -1 to 1, price at tick 0 (1:1).
	sqrtPriceX96 := GetSqrtRatioAtTick(0)
	liquidity := big.NewInt(250012499687515624)

	amounts := ComputePositionAmounts(sqrtPriceX96, -1, 1, liquidity)

	if amounts.Amount0.Sign() <= 0 {
		t.Errorf("amount0 should be positive in-range, got %s", amounts.Amount0)
	}
	if amounts.Amount1.Sign() <= 0 {
		t.Errorf("amount1 should be positive in-range, got %s", amounts.Amount1)
	}
}

func TestComputePositionAmounts_BelowRange(t *testing.T) {
	// Price below range — should be all token0, zero token1.
	sqrtPriceX96 := GetSqrtRatioAtTick(-10)
	liquidity := big.NewInt(1000000)

	amounts := ComputePositionAmounts(sqrtPriceX96, -1, 1, liquidity)

	if amounts.Amount0.Sign() <= 0 {
		t.Errorf("amount0 should be positive below range, got %s", amounts.Amount0)
	}
	if amounts.Amount1.Sign() != 0 {
		t.Errorf("amount1 should be zero below range, got %s", amounts.Amount1)
	}
}

func TestComputePositionAmounts_AboveRange(t *testing.T) {
	// Price above range — should be all token1, zero token0.
	sqrtPriceX96 := GetSqrtRatioAtTick(10)
	liquidity := big.NewInt(1000000)

	amounts := ComputePositionAmounts(sqrtPriceX96, -1, 1, liquidity)

	if amounts.Amount0.Sign() != 0 {
		t.Errorf("amount0 should be zero above range, got %s", amounts.Amount0)
	}
	if amounts.Amount1.Sign() <= 0 {
		t.Errorf("amount1 should be positive above range, got %s", amounts.Amount1)
	}
}

func TestComputePositionAmounts_ZeroLiquidity(t *testing.T) {
	sqrtPriceX96 := GetSqrtRatioAtTick(0)
	liquidity := big.NewInt(0)

	amounts := ComputePositionAmounts(sqrtPriceX96, -1, 1, liquidity)

	if amounts.Amount0.Sign() != 0 {
		t.Errorf("amount0 should be zero for zero liquidity, got %s", amounts.Amount0)
	}
	if amounts.Amount1.Sign() != 0 {
		t.Errorf("amount1 should be zero for zero liquidity, got %s", amounts.Amount1)
	}
}

func TestGetAmount0ForLiquidity_OrderIndependent(t *testing.T) {
	a := GetSqrtRatioAtTick(-1)
	b := GetSqrtRatioAtTick(1)
	liq := big.NewInt(1000000)

	result1 := getAmount0ForLiquidity(a, b, liq)
	result2 := getAmount0ForLiquidity(b, a, liq)

	if result1.Cmp(result2) != 0 {
		t.Errorf("getAmount0ForLiquidity should be order-independent: %s vs %s", result1, result2)
	}
}

func TestGetAmount1ForLiquidity_OrderIndependent(t *testing.T) {
	a := GetSqrtRatioAtTick(-1)
	b := GetSqrtRatioAtTick(1)
	liq := big.NewInt(1000000)

	result1 := getAmount1ForLiquidity(a, b, liq)
	result2 := getAmount1ForLiquidity(b, a, liq)

	if result1.Cmp(result2) != 0 {
		t.Errorf("getAmount1ForLiquidity should be order-independent: %s vs %s", result1, result2)
	}
}

// ---------------------------------------------------------------------------
// Reader fail-loud tests; rationale on the Reader doc comment.
// ---------------------------------------------------------------------------

var (
	readerNFTManager = common.HexToAddress("0xC36442b4a4522E871399CD717aBDD847Ab11FE88")
	readerWallet     = common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")
	readerPool       = common.HexToAddress("0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d")
	readerToken0     = common.HexToAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a")
	readerToken1     = common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	readerBlockHash  = common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
)

// readerFixture answers the reader's multicall rounds for one wallet holding
// one position in one pool, with per-method overrides to make a single
// sub-call fail or return undecodable bytes.
type readerFixture struct {
	t               *testing.T
	nftABI          abi.ABI
	poolABI         abi.ABI
	fail            map[string]bool // method name → Success:false
	garbage         map[string]bool // method name → Success:true, undecodable data
	balanceOfReturn *big.Int        // nil → 1
}

func newReaderFixture(t *testing.T) *readerFixture {
	t.Helper()
	nftABI, err := abi.JSON(strings.NewReader(nftManagerABIJSON))
	if err != nil {
		t.Fatalf("parse nft ABI: %v", err)
	}
	poolABI, err := abi.JSON(strings.NewReader(poolABIJSON))
	if err != nil {
		t.Fatalf("parse pool ABI: %v", err)
	}
	return &readerFixture{
		t:       t,
		nftABI:  nftABI,
		poolABI: poolABI,
		fail:    make(map[string]bool),
		garbage: make(map[string]bool),
	}
}

func (f *readerFixture) multicaller() *testutil.MockMulticaller {
	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, call := range calls {
			results[i] = f.answer(call)
		}
		return results, nil
	}
	return mc
}

func (f *readerFixture) answer(call outbound.Call) outbound.Result {
	f.t.Helper()
	method, ok := f.methodBySelector(call.CallData[:4])
	if !ok {
		f.t.Fatalf("readerFixture: unexpected selector %x", call.CallData[:4])
	}
	if f.fail[method.Name] {
		return outbound.Result{Success: false}
	}
	if f.garbage[method.Name] {
		return outbound.Result{Success: true, ReturnData: []byte{0xde, 0xad}}
	}
	switch method.Name {
	case "balanceOf":
		if f.balanceOfReturn != nil {
			return f.pack(method, f.balanceOfReturn)
		}
		return f.pack(method, big.NewInt(1))
	case "tokenOfOwnerByIndex":
		return f.pack(method, big.NewInt(7))
	case "positions":
		return f.pack(method,
			big.NewInt(0), common.Address{}, readerToken0, readerToken1, big.NewInt(100),
			big.NewInt(-887220), big.NewInt(887220), big.NewInt(1_000_000),
			big.NewInt(0), big.NewInt(0), big.NewInt(0), big.NewInt(0))
	case "slot0":
		return f.pack(method,
			new(big.Int).Lsh(big.NewInt(1), 96), big.NewInt(0),
			uint16(0), uint16(1), uint16(1), uint8(0), true)
	case "token0":
		return f.pack(method, readerToken0)
	case "token1":
		return f.pack(method, readerToken1)
	case "fee":
		return f.pack(method, big.NewInt(100))
	default:
		f.t.Fatalf("readerFixture: unhandled method %s", method.Name)
		return outbound.Result{}
	}
}

func (f *readerFixture) methodBySelector(selector []byte) (abi.Method, bool) {
	for _, m := range f.nftABI.Methods {
		if string(m.ID) == string(selector) {
			return m, true
		}
	}
	for _, m := range f.poolABI.Methods {
		if string(m.ID) == string(selector) {
			return m, true
		}
	}
	return abi.Method{}, false
}

func (f *readerFixture) pack(method abi.Method, values ...any) outbound.Result {
	f.t.Helper()
	data, err := method.Outputs.Pack(values...)
	if err != nil {
		f.t.Fatalf("pack %s outputs: %v", method.Name, err)
	}
	return outbound.Result{Success: true, ReturnData: data}
}

func (f *readerFixture) reader() *Reader {
	f.t.Helper()
	reader, err := NewReader(f.multicaller())
	if err != nil {
		f.t.Fatalf("NewReader: %v", err)
	}
	return reader
}

func TestReader_GetPositions_SubCallFailure_Error(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		garbage bool
	}{
		{"failed balanceOf", "balanceOf", false},
		{"undecodable balanceOf", "balanceOf", true},
		{"failed tokenOfOwnerByIndex", "tokenOfOwnerByIndex", false},
		{"undecodable tokenOfOwnerByIndex", "tokenOfOwnerByIndex", true},
		{"failed positions", "positions", false},
		{"undecodable positions", "positions", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newReaderFixture(t)
			if tc.garbage {
				f.garbage[tc.method] = true
			} else {
				f.fail[tc.method] = true
			}

			_, err := f.reader().GetPositions(
				context.Background(), []common.Address{readerWallet}, readerNFTManager, readerBlockHash)
			if err == nil {
				t.Fatalf("expected error when %s, got nil", tc.name)
			}
			if !strings.Contains(err.Error(), tc.method) {
				t.Errorf("error %q should mention the failed call %s", err, tc.method)
			}
		})
	}
}

// TestReader_GetPositions_NFTCountOverflow_Error: a balanceOf return outside
// int64 must error rather than silently truncate to a wrong (possibly zero)
// NFT count, which would report the wallet as holding no positions.
func TestReader_GetPositions_NFTCountOverflow_Error(t *testing.T) {
	f := newReaderFixture(t)
	f.balanceOfReturn = new(big.Int).Lsh(big.NewInt(1), 70)

	_, err := f.reader().GetPositions(
		context.Background(), []common.Address{readerWallet}, readerNFTManager, readerBlockHash)
	if err == nil {
		t.Fatal("expected error for a balanceOf count outside int64")
	}
	if !strings.Contains(err.Error(), "balanceOf") {
		t.Errorf("error %q should mention balanceOf", err)
	}
}

// TestReader_ResultCountMismatch_Error: a multicall answering with fewer
// results than calls must error; indexing into the short batch would either
// panic or silently misattribute results across wallets/pools.
func TestReader_ResultCountMismatch_Error(t *testing.T) {
	truncatingMulticaller := func(f *readerFixture) *testutil.MockMulticaller {
		mc := testutil.NewMockMulticaller()
		mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
			results := make([]outbound.Result, 0, len(calls)-1)
			for _, call := range calls[:len(calls)-1] {
				results = append(results, f.answer(call))
			}
			return results, nil
		}
		return mc
	}

	tests := []struct {
		name string
		call func(r *Reader) error
	}{
		{"GetPositions", func(r *Reader) error {
			_, err := r.GetPositions(
				context.Background(), []common.Address{readerWallet}, readerNFTManager, readerBlockHash)
			return err
		}},
		{"GetPoolStates", func(r *Reader) error {
			_, err := r.GetPoolStates(context.Background(), []common.Address{readerPool}, readerBlockHash)
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newReaderFixture(t)
			reader, err := NewReader(truncatingMulticaller(f))
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			if err := tc.call(reader); err == nil {
				t.Fatal("expected error when the multicall returns fewer results than calls")
			}
		})
	}
}

func TestReader_GetPoolStates_SubCallFailure_Error(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		garbage bool
	}{
		{"failed slot0", "slot0", false},
		{"undecodable slot0", "slot0", true},
		{"failed token0", "token0", false},
		{"undecodable token0", "token0", true},
		{"failed token1", "token1", false},
		{"undecodable token1", "token1", true},
		{"failed fee", "fee", false},
		{"undecodable fee", "fee", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newReaderFixture(t)
			if tc.garbage {
				f.garbage[tc.method] = true
			} else {
				f.fail[tc.method] = true
			}

			_, err := f.reader().GetPoolStates(context.Background(), []common.Address{readerPool}, readerBlockHash)
			if err == nil {
				t.Fatalf("expected error when %s, got nil", tc.name)
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(readerPool.Hex())) {
				t.Errorf("error %q should mention the pool address", err)
			}
			// The error must name exactly the failed sub-call, not the whole
			// method list, or the message cannot direct debugging.
			for _, method := range []string{"slot0", "token0", "token1", "fee"} {
				if method == tc.method {
					if !strings.Contains(err.Error(), method) {
						t.Errorf("error %q should mention the failed call %s", err, method)
					}
					continue
				}
				if strings.Contains(err.Error(), method) {
					t.Errorf("error %q should not mention %s (only %s failed)", err, method, tc.method)
				}
			}
		})
	}
}
