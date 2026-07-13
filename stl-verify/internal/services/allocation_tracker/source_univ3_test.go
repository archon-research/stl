package allocation_tracker

import (
	"context"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/uniswapv3"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Fake V3 chain: serves the reader's four multicall rounds (balanceOf,
// tokenOfOwnerByIndex, positions, slot0/token0/token1) from in-memory fixtures
// so source tests can drive exact position/pool states through the real
// decoding path.
// ---------------------------------------------------------------------------

// Mirrors the reader's on-chain interface; kept in the test so a drift in the
// production ABI surfaces as a selector mismatch here.
const testV3NFTManagerABI = `[
	{"name":"balanceOf","type":"function","inputs":[{"name":"owner","type":"address"}],"outputs":[{"name":"","type":"uint256"}]},
	{"name":"tokenOfOwnerByIndex","type":"function","inputs":[{"name":"owner","type":"address"},{"name":"index","type":"uint256"}],"outputs":[{"name":"","type":"uint256"}]},
	{"name":"positions","type":"function","inputs":[{"name":"tokenId","type":"uint256"}],"outputs":[
		{"name":"nonce","type":"uint96"},
		{"name":"operator","type":"address"},
		{"name":"token0","type":"address"},
		{"name":"token1","type":"address"},
		{"name":"fee","type":"uint24"},
		{"name":"tickLower","type":"int24"},
		{"name":"tickUpper","type":"int24"},
		{"name":"liquidity","type":"uint128"},
		{"name":"feeGrowthInside0LastX128","type":"uint256"},
		{"name":"feeGrowthInside1LastX128","type":"uint256"},
		{"name":"tokensOwed0","type":"uint128"},
		{"name":"tokensOwed1","type":"uint128"}
	]}
]`

const testV3PoolABI = `[
	{"name":"slot0","type":"function","inputs":[],"outputs":[
		{"name":"sqrtPriceX96","type":"uint160"},
		{"name":"tick","type":"int24"},
		{"name":"observationIndex","type":"uint16"},
		{"name":"observationCardinality","type":"uint16"},
		{"name":"observationCardinalityNext","type":"uint16"},
		{"name":"feeProtocol","type":"uint8"},
		{"name":"unlocked","type":"bool"}
	]},
	{"name":"token0","type":"function","inputs":[],"outputs":[{"name":"","type":"address"}]},
	{"name":"token1","type":"function","inputs":[],"outputs":[{"name":"","type":"address"}]}
]`

type fakeV3Position struct {
	tokenID   *big.Int
	token0    common.Address
	token1    common.Address
	tickLower int
	tickUpper int
	liquidity *big.Int
}

type fakeV3Pool struct {
	sqrtPriceX96 *big.Int
	tick         int
	token0       common.Address
	token1       common.Address
}

type fakeV3Chain struct {
	t           *testing.T
	nftABI      abi.ABI
	poolABI     abi.ABI
	byWallet    map[common.Address][]*fakeV3Position
	byTokenID   map[string]*fakeV3Position
	pools       map[common.Address]*fakeV3Pool
	nextTokenID int64
}

func newFakeV3Chain(t *testing.T) *fakeV3Chain {
	t.Helper()
	nftABI, err := abi.JSON(strings.NewReader(testV3NFTManagerABI))
	if err != nil {
		t.Fatalf("parse nft ABI: %v", err)
	}
	poolABI, err := abi.JSON(strings.NewReader(testV3PoolABI))
	if err != nil {
		t.Fatalf("parse pool ABI: %v", err)
	}
	return &fakeV3Chain{
		t:           t,
		nftABI:      nftABI,
		poolABI:     poolABI,
		byWallet:    make(map[common.Address][]*fakeV3Position),
		byTokenID:   make(map[string]*fakeV3Position),
		pools:       make(map[common.Address]*fakeV3Pool),
		nextTokenID: 1,
	}
}

func (c *fakeV3Chain) addPosition(wallet common.Address, p fakeV3Position) {
	p.tokenID = big.NewInt(c.nextTokenID)
	c.nextTokenID++
	pos := &p
	c.byWallet[wallet] = append(c.byWallet[wallet], pos)
	c.byTokenID[pos.tokenID.String()] = pos
}

func (c *fakeV3Chain) setPool(addr common.Address, pool fakeV3Pool) {
	c.pools[addr] = &pool
}

func (c *fakeV3Chain) multicaller() *testutil.MockMulticaller {
	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, call := range calls {
			results[i] = c.answer(call)
		}
		return results, nil
	}
	return mc
}

func (c *fakeV3Chain) answer(call outbound.Call) outbound.Result {
	c.t.Helper()
	selector := string(call.CallData[:4])
	switch selector {
	case string(c.nftABI.Methods["balanceOf"].ID):
		in := c.unpackInputs(c.nftABI.Methods["balanceOf"], call.CallData)
		wallet := in[0].(common.Address)
		return c.packOutputs(c.nftABI.Methods["balanceOf"], big.NewInt(int64(len(c.byWallet[wallet]))))
	case string(c.nftABI.Methods["tokenOfOwnerByIndex"].ID):
		in := c.unpackInputs(c.nftABI.Methods["tokenOfOwnerByIndex"], call.CallData)
		wallet := in[0].(common.Address)
		index := in[1].(*big.Int).Int64()
		positions := c.byWallet[wallet]
		if index >= int64(len(positions)) {
			return outbound.Result{Success: false}
		}
		return c.packOutputs(c.nftABI.Methods["tokenOfOwnerByIndex"], positions[index].tokenID)
	case string(c.nftABI.Methods["positions"].ID):
		in := c.unpackInputs(c.nftABI.Methods["positions"], call.CallData)
		pos, ok := c.byTokenID[in[0].(*big.Int).String()]
		if !ok {
			return outbound.Result{Success: false}
		}
		return c.packOutputs(c.nftABI.Methods["positions"],
			big.NewInt(0), common.Address{}, pos.token0, pos.token1, big.NewInt(100),
			big.NewInt(int64(pos.tickLower)), big.NewInt(int64(pos.tickUpper)), pos.liquidity,
			big.NewInt(0), big.NewInt(0), big.NewInt(0), big.NewInt(0))
	case string(c.poolABI.Methods["slot0"].ID):
		pool, ok := c.pools[call.Target]
		if !ok {
			return outbound.Result{Success: false}
		}
		return c.packOutputs(c.poolABI.Methods["slot0"],
			pool.sqrtPriceX96, big.NewInt(int64(pool.tick)),
			uint16(0), uint16(1), uint16(1), uint8(0), true)
	case string(c.poolABI.Methods["token0"].ID):
		pool, ok := c.pools[call.Target]
		if !ok {
			return outbound.Result{Success: false}
		}
		return c.packOutputs(c.poolABI.Methods["token0"], pool.token0)
	case string(c.poolABI.Methods["token1"].ID):
		pool, ok := c.pools[call.Target]
		if !ok {
			return outbound.Result{Success: false}
		}
		return c.packOutputs(c.poolABI.Methods["token1"], pool.token1)
	default:
		c.t.Fatalf("fakeV3Chain: unexpected selector %x", call.CallData[:4])
		return outbound.Result{}
	}
}

func (c *fakeV3Chain) unpackInputs(method abi.Method, callData []byte) []any {
	c.t.Helper()
	in, err := method.Inputs.Unpack(callData[4:])
	if err != nil {
		c.t.Fatalf("unpack %s inputs: %v", method.Name, err)
	}
	return in
}

func (c *fakeV3Chain) packOutputs(method abi.Method, values ...any) outbound.Result {
	c.t.Helper()
	data, err := method.Outputs.Pack(values...)
	if err != nil {
		c.t.Fatalf("pack %s outputs: %v", method.Name, err)
	}
	return outbound.Result{Success: true, ReturnData: data}
}

// ---------------------------------------------------------------------------
// Shared fixtures: the real grove AUSD/USDC pool constellation.
// ---------------------------------------------------------------------------

var (
	univ3PoolAddr   = common.HexToAddress("0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d")
	univ3AUSD       = common.HexToAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a") // token0
	univ3USDC       = common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48") // token1
	univ3Wallet     = common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")
	univ3FullRangeL = -887220
	univ3FullRangeU = 887220
)

func univ3Entry(asset *common.Address) *TokenEntry {
	return &TokenEntry{
		ContractAddress: univ3PoolAddr,
		WalletAddress:   univ3Wallet,
		AssetAddress:    asset,
		Star:            "grove",
		Chain:           "mainnet",
		TokenType:       "uni_v3_pool",
	}
}

// newAUSDUSDCChain returns a fake chain with the AUSD/USDC pool at the given
// sqrt price and one wallet position per (tickLower, tickUpper, liquidity).
func newAUSDUSDCChain(t *testing.T, sqrtPriceX96 *big.Int, positions ...fakeV3Position) *fakeV3Chain {
	t.Helper()
	chain := newFakeV3Chain(t)
	chain.setPool(univ3PoolAddr, fakeV3Pool{
		sqrtPriceX96: sqrtPriceX96,
		token0:       univ3AUSD,
		token1:       univ3USDC,
	})
	for _, p := range positions {
		p.token0 = univ3AUSD
		p.token1 = univ3USDC
		chain.addPosition(univ3Wallet, p)
	}
	return chain
}

func fetchUniV3Balance(t *testing.T, chain *fakeV3Chain, entry *TokenEntry) *PositionBalance {
	t.Helper()
	src, err := NewUniV3Source(chain.multicaller(), slog.Default())
	if err != nil {
		t.Fatalf("NewUniV3Source: %v", err)
	}
	result, err := src.FetchBalances(context.Background(), []*TokenEntry{entry}, testBlockHash)
	if err != nil {
		t.Fatalf("FetchBalances: %v", err)
	}
	bal, ok := result.Balances[entry.Key()]
	if !ok {
		t.Fatalf("no balance entry for %s/%s", entry.ContractAddress.Hex(), entry.WalletAddress.Hex())
	}
	return bal
}

// sqrtPriceX96 = 2^97 encodes a spot price of exactly 4 token1-raw per
// token0-raw ((2^97 / 2^96)^2), so the expected conversions below are exact
// integer arithmetic applied by hand, independent of the production helpers.
func univ3TestPrice4() *big.Int { return new(big.Int).Lsh(big.NewInt(1), 97) }

// TestUniV3Source_FetchBalances_FullValueInHintAsset is the core fix: an
// in-range two-sided position must be valued with BOTH sides, denominated in
// the hint asset via the pool's own spot price. The old code dropped the
// non-hint side entirely.
func TestUniV3Source_FetchBalances_FullValueInHintAsset(t *testing.T) {
	liquidity := big.NewInt(1_000_000_000_000_000_000)
	amounts := uniswapv3.ComputePositionAmounts(univ3TestPrice4(), univ3FullRangeL, univ3FullRangeU, liquidity)
	if amounts.Amount0.Sign() <= 0 || amounts.Amount1.Sign() <= 0 {
		t.Fatalf("fixture must be two-sided, got amount0=%s amount1=%s", amounts.Amount0, amounts.Amount1)
	}

	tests := []struct {
		name string
		hint common.Address
		want *big.Int
	}{
		{
			name: "hint is token1: amount1 + amount0 x price",
			hint: univ3USDC,
			want: new(big.Int).Add(amounts.Amount1, new(big.Int).Mul(amounts.Amount0, big.NewInt(4))),
		},
		{
			name: "hint is token0: amount0 + amount1 / price",
			hint: univ3AUSD,
			want: new(big.Int).Add(amounts.Amount0, new(big.Int).Div(amounts.Amount1, big.NewInt(4))),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chain := newAUSDUSDCChain(t, univ3TestPrice4(),
				fakeV3Position{tickLower: univ3FullRangeL, tickUpper: univ3FullRangeU, liquidity: liquidity})
			bal := fetchUniV3Balance(t, chain, univ3Entry(&tc.hint))
			if bal.Balance.Cmp(tc.want) != 0 {
				t.Errorf("Balance = %s, want %s", bal.Balance, tc.want)
			}
			if bal.UnderlyingValue == nil || bal.UnderlyingValue.Cmp(tc.want) != 0 {
				t.Errorf("UnderlyingValue = %v, want %s", bal.UnderlyingValue, tc.want)
			}
		})
	}
}

// TestUniV3Source_FetchBalances_OneSidedOutOfRange: out-of-range positions
// hold a single token; the value must be that side alone, converted when it is
// not the hint side.
func TestUniV3Source_FetchBalances_OneSidedOutOfRange(t *testing.T) {
	liquidity := big.NewInt(1_000_000_000_000)

	tests := []struct {
		name                 string
		tickLower, tickUpper int
		wantSide             func(a uniswapv3.PositionAmounts) *big.Int
	}{
		{
			// Price (2^97, price 4, tick ~13863) is below the range: all
			// token0, converted into the token1 hint at x4.
			name:      "below range all token0 converted into hint",
			tickLower: 20000, tickUpper: 30000,
			wantSide: func(a uniswapv3.PositionAmounts) *big.Int {
				return new(big.Int).Mul(a.Amount0, big.NewInt(4))
			},
		},
		{
			// Price above the range: all token1, already in hint units.
			name:      "above range all token1 passes through",
			tickLower: -30000, tickUpper: -20000,
			wantSide: func(a uniswapv3.PositionAmounts) *big.Int {
				return new(big.Int).Set(a.Amount1)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			amounts := uniswapv3.ComputePositionAmounts(univ3TestPrice4(), tc.tickLower, tc.tickUpper, liquidity)
			if amounts.Amount0.Sign() != 0 && amounts.Amount1.Sign() != 0 {
				t.Fatalf("fixture must be one-sided, got amount0=%s amount1=%s", amounts.Amount0, amounts.Amount1)
			}
			chain := newAUSDUSDCChain(t, univ3TestPrice4(),
				fakeV3Position{tickLower: tc.tickLower, tickUpper: tc.tickUpper, liquidity: liquidity})
			bal := fetchUniV3Balance(t, chain, univ3Entry(&univ3USDC))
			want := tc.wantSide(amounts)
			if bal.Balance.Cmp(want) != 0 {
				t.Errorf("Balance = %s, want %s", bal.Balance, want)
			}
		})
	}
}

func TestUniV3Source_FetchBalances_SumsMultiplePositions(t *testing.T) {
	liqA := big.NewInt(1_000_000_000_000_000_000)
	liqB := big.NewInt(3_000_000_000_000_000)
	a := uniswapv3.ComputePositionAmounts(univ3TestPrice4(), univ3FullRangeL, univ3FullRangeU, liqA)
	b := uniswapv3.ComputePositionAmounts(univ3TestPrice4(), -30000, -20000, liqB)

	totalAmount0 := new(big.Int).Add(a.Amount0, b.Amount0)
	totalAmount1 := new(big.Int).Add(a.Amount1, b.Amount1)
	want := new(big.Int).Add(totalAmount1, new(big.Int).Mul(totalAmount0, big.NewInt(4)))

	chain := newAUSDUSDCChain(t, univ3TestPrice4(),
		fakeV3Position{tickLower: univ3FullRangeL, tickUpper: univ3FullRangeU, liquidity: liqA},
		fakeV3Position{tickLower: -30000, tickUpper: -20000, liquidity: liqB})
	bal := fetchUniV3Balance(t, chain, univ3Entry(&univ3USDC))
	if bal.Balance.Cmp(want) != 0 {
		t.Errorf("Balance = %s, want %s", bal.Balance, want)
	}
}

// TestUniV3Source_FetchBalances_CarriesPoolPair: the pool's pair is carried so
// the handler can compose a truthful pool-row symbol.
func TestUniV3Source_FetchBalances_CarriesPoolPair(t *testing.T) {
	chain := newAUSDUSDCChain(t, univ3TestPrice4(),
		fakeV3Position{tickLower: univ3FullRangeL, tickUpper: univ3FullRangeU, liquidity: big.NewInt(1_000_000)})
	bal := fetchUniV3Balance(t, chain, univ3Entry(&univ3USDC))
	if bal.PoolToken0 == nil || *bal.PoolToken0 != univ3AUSD {
		t.Errorf("PoolToken0 = %v, want %s", bal.PoolToken0, univ3AUSD.Hex())
	}
	if bal.PoolToken1 == nil || *bal.PoolToken1 != univ3USDC {
		t.Errorf("PoolToken1 = %v, want %s", bal.PoolToken1, univ3USDC.Hex())
	}
}

// TestUniV3Source_FetchBalances_ScaledBalanceNil: a V3 position has no share
// count, and the old amount0+amount1 sum mixed units, so ScaledBalance stays
// nil by design.
func TestUniV3Source_FetchBalances_ScaledBalanceNil(t *testing.T) {
	chain := newAUSDUSDCChain(t, univ3TestPrice4(),
		fakeV3Position{tickLower: univ3FullRangeL, tickUpper: univ3FullRangeU, liquidity: big.NewInt(1_000_000)})
	bal := fetchUniV3Balance(t, chain, univ3Entry(&univ3USDC))
	if bal.ScaledBalance != nil {
		t.Errorf("ScaledBalance = %s, want nil", bal.ScaledBalance)
	}
}

// TestUniV3Source_FetchBalances_HintMatchesNeither_Error: adding amount0 and
// amount1 across different tokens is meaningless, so a hint that matches
// neither pool side must fail the whole fetch, not fall back to a sum.
func TestUniV3Source_FetchBalances_HintMatchesNeither_Error(t *testing.T) {
	other := common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7")
	chain := newAUSDUSDCChain(t, univ3TestPrice4(),
		fakeV3Position{tickLower: univ3FullRangeL, tickUpper: univ3FullRangeU, liquidity: big.NewInt(1_000_000)})
	src, err := NewUniV3Source(chain.multicaller(), slog.Default())
	if err != nil {
		t.Fatalf("NewUniV3Source: %v", err)
	}

	_, err = src.FetchBalances(context.Background(), []*TokenEntry{univ3Entry(&other)}, testBlockHash)
	if err == nil {
		t.Fatal("expected error when the hint asset matches neither pool token")
	}
	for _, fragment := range []string{
		strings.ToLower(univ3PoolAddr.Hex()),
		strings.ToLower(other.Hex()),
	} {
		if !strings.Contains(strings.ToLower(err.Error()), fragment) {
			t.Errorf("error %q should mention %s", err, fragment)
		}
	}
}

// TestUniV3Source_FetchBalances_NoAssetAddress_Error: without a hint asset the
// position value has no denomination, so the entry is a config error surfaced
// immediately (not a silent mixed-unit sum).
func TestUniV3Source_FetchBalances_NoAssetAddress_Error(t *testing.T) {
	src, err := NewUniV3Source(testutil.NewMockMulticaller(), slog.Default())
	if err != nil {
		t.Fatalf("NewUniV3Source: %v", err)
	}

	_, err = src.FetchBalances(context.Background(), []*TokenEntry{univ3Entry(nil)}, testBlockHash)
	if err == nil {
		t.Fatal("expected error for uni_v3 entry without asset address")
	}
	if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(univ3PoolAddr.Hex())) {
		t.Errorf("error %q should mention the pool address", err)
	}
}

// TestUniV3Source_FetchBalances_MissingPoolState_Error: the pool-state read is
// issued for every entry's pool, so an absent state is a failed AllowFailure
// sub-call (or a non-pool address), never a structural absence. Skipping would
// freeze the position at its previous row while the wallet holds live
// liquidity; the block must fail so SQS retries.
func TestUniV3Source_FetchBalances_MissingPoolState_Error(t *testing.T) {
	// The wallet has a live position, but the pool is never registered, so its
	// slot0/token0/token1 sub-calls fail.
	chain := newFakeV3Chain(t)
	chain.addPosition(univ3Wallet, fakeV3Position{
		token0:    univ3AUSD,
		token1:    univ3USDC,
		tickLower: univ3FullRangeL,
		tickUpper: univ3FullRangeU,
		liquidity: big.NewInt(1_000_000),
	})
	src, err := NewUniV3Source(chain.multicaller(), slog.Default())
	if err != nil {
		t.Fatalf("NewUniV3Source: %v", err)
	}

	_, err = src.FetchBalances(context.Background(), []*TokenEntry{univ3Entry(&univ3USDC)}, testBlockHash)
	if err == nil {
		t.Fatal("expected error when the pool state read failed")
	}
	if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(univ3PoolAddr.Hex())) {
		t.Errorf("error %q should mention the pool address", err)
	}
}

// TestUniV3Source_FetchBalances_ExcludesForeignPairPositions: positions the
// wallet holds in a different pair must not leak into the entry's value.
func TestUniV3Source_FetchBalances_ExcludesForeignPairPositions(t *testing.T) {
	liquidity := big.NewInt(1_000_000_000_000_000_000)
	amounts := uniswapv3.ComputePositionAmounts(univ3TestPrice4(), univ3FullRangeL, univ3FullRangeU, liquidity)
	want := new(big.Int).Add(amounts.Amount1, new(big.Int).Mul(amounts.Amount0, big.NewInt(4)))

	chain := newAUSDUSDCChain(t, univ3TestPrice4(),
		fakeV3Position{tickLower: univ3FullRangeL, tickUpper: univ3FullRangeU, liquidity: liquidity})
	// Same wallet, different pair: must be excluded from the AUSD/USDC value.
	chain.addPosition(univ3Wallet, fakeV3Position{
		token0:    common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
		token1:    univ3USDC,
		tickLower: univ3FullRangeL,
		tickUpper: univ3FullRangeU,
		liquidity: big.NewInt(9_000_000_000_000_000_000),
	})

	bal := fetchUniV3Balance(t, chain, univ3Entry(&univ3USDC))
	if bal.Balance.Cmp(want) != 0 {
		t.Errorf("Balance = %s, want %s (matching position only)", bal.Balance, want)
	}
}

func TestUniV3Source_FetchBalances_ZeroLiquidity_NoEntry(t *testing.T) {
	chain := newAUSDUSDCChain(t, univ3TestPrice4(),
		fakeV3Position{tickLower: univ3FullRangeL, tickUpper: univ3FullRangeU, liquidity: big.NewInt(0)})
	src, err := NewUniV3Source(chain.multicaller(), slog.Default())
	if err != nil {
		t.Fatalf("NewUniV3Source: %v", err)
	}

	result, err := src.FetchBalances(context.Background(), []*TokenEntry{univ3Entry(&univ3USDC)}, testBlockHash)
	if err != nil {
		t.Fatalf("FetchBalances: %v", err)
	}
	if len(result.Balances) != 0 {
		t.Errorf("expected no balance entries for a zero-liquidity position, got %d", len(result.Balances))
	}
}

// ---------------------------------------------------------------------------
// UniV3Source adapter tests
// ---------------------------------------------------------------------------

func TestUniV3Source_Name(t *testing.T) {
	src, err := NewUniV3Source(testutil.NewMockMulticaller(), slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if src.Name() != "uni-v3" {
		t.Errorf("Name() = %q, want %q", src.Name(), "uni-v3")
	}
}

func TestUniV3Source_Supports(t *testing.T) {
	src, err := NewUniV3Source(testutil.NewMockMulticaller(), slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tests := []struct {
		tokenType string
		protocol  string
		want      bool
	}{
		{"uni_v3_pool", "uniswap", true},
		{"uni_v3_lp", "uniswap", true},
		{"uni_v3_pool", "", true},
		{"uni_v3_lp", "", true},
		{"erc20", "uniswap", false},
		{"erc4626", "", false},
		{"curve", "", false},
	}

	for _, tt := range tests {
		got := src.Supports(tt.tokenType, tt.protocol)
		if got != tt.want {
			t.Errorf("Supports(%q, %q) = %v, want %v", tt.tokenType, tt.protocol, got, tt.want)
		}
	}
}

func TestUniV3Source_FetchBalances_EmptyEntries(t *testing.T) {
	src, err := NewUniV3Source(testutil.NewMockMulticaller(), slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := src.FetchBalances(context.Background(), nil, testBlockHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Balances) != 0 {
		t.Errorf("expected empty result, got %d", len(result.Balances))
	}
}

// TestUniV3Source_FetchBalances_UnknownChain_Error: a chain missing from the
// PositionManagers registry is a config error of the same class as a missing
// hint asset; skipping it would silently drop the position on every block.
func TestUniV3Source_FetchBalances_UnknownChain_Error(t *testing.T) {
	src, err := NewUniV3Source(testutil.NewMockMulticaller(), slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	entries := []*TokenEntry{
		{
			ContractAddress: common.HexToAddress("0x1111"),
			WalletAddress:   common.HexToAddress("0x2222"),
			AssetAddress:    &univ3USDC,
			Star:            "spark",
			Chain:           "unknown-chain",
			TokenType:       "uni_v3_pool",
		},
	}

	_, err = src.FetchBalances(context.Background(), entries, testBlockHash)
	if err == nil {
		t.Fatal("expected error for chain without a NonfungiblePositionManager")
	}
	if !strings.Contains(err.Error(), "unknown-chain") {
		t.Errorf("error %q should mention the chain", err)
	}
}

// TestUniV3Source_FetchBalances_PinsToBlockHash asserts the NFT-position and
// pool-state reads are pinned to the block hash, not the block number: after a
// reorg an archive node answers eth_call-by-number with the new canonical
// state, which can silently disagree with the reorged (older-version) data
// this fetch is being made for.
func TestUniV3Source_FetchBalances_PinsToBlockHash(t *testing.T) {
	// An empty-but-successful result batch sized to the call count lets the test
	// assert the reader pins state reads to the block hash rather than the block
	// number, without modelling exact multicall shapes.
	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return make([]outbound.Result, len(calls)), nil
	}
	src, err := NewUniV3Source(mc, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	entries := []*TokenEntry{
		{
			ContractAddress: common.HexToAddress("0x1111"),
			WalletAddress:   common.HexToAddress("0x2222"),
			AssetAddress:    &univ3USDC,
			Star:            "spark",
			Chain:           "mainnet",
			TokenType:       "uni_v3_pool",
		},
	}

	if _, err := src.FetchBalances(context.Background(), entries, testBlockHash); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mc.Invocations) == 0 {
		t.Fatal("expected at least one multicall")
	}
	last := mc.Invocations[len(mc.Invocations)-1]
	if !last.ViaHash {
		t.Fatal("multicaller invoked via the number path, want the hash-pinned path")
	}
	if last.BlockHash != testBlockHash {
		t.Fatalf("multicall block hash = %s, want %s", last.BlockHash, testBlockHash)
	}
}
