package allocation_tracker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
	"github.com/archon-research/stl/stl-verify/internal/pkg/uniswapv3"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ---------------------------------------------------------------------------
// Fake UniswapV4PositionValuationReader: an in-memory double for the Postgres
// reads UniV4Source drives. Present-in-map is "found"; absent is the
// not-yet-indexed / not-a-holder case each method documents as nil.
// ---------------------------------------------------------------------------

type fakeV4Reader struct {
	t *testing.T

	pools    map[int64][]outbound.UniswapV4PoolRow
	poolsErr error

	held    map[string][]*big.Int
	heldErr error

	positions   map[string]outbound.UniswapV4PositionSnapshot
	positionErr error

	poolStates   map[int64]*big.Int
	poolStateErr error
}

func newFakeV4Reader(t *testing.T) *fakeV4Reader {
	t.Helper()
	return &fakeV4Reader{
		t:          t,
		pools:      make(map[int64][]outbound.UniswapV4PoolRow),
		held:       make(map[string][]*big.Int),
		positions:  make(map[string]outbound.UniswapV4PositionSnapshot),
		poolStates: make(map[int64]*big.Int),
	}
}

// newFakeV4ReaderWithPool is the common case: a reader with one registered
// pool, so fetchChainBalances resolves a PositionManager and doesn't take the
// no-pools shortcut.
func newFakeV4ReaderWithPool(t *testing.T, pool outbound.UniswapV4PoolRow) *fakeV4Reader {
	t.Helper()
	r := newFakeV4Reader(t)
	r.setPool(v4ChainID, pool)
	return r
}

func v4DefaultPool() outbound.UniswapV4PoolRow {
	return outbound.UniswapV4PoolRow{ID: 10, PositionManager: v4PositionManagerAddr, Currency0: v4CurrencyA, Currency1: v4HintAsset}
}

func v4HeldKey(chainID int64, wallet common.Address) string {
	return fmt.Sprintf("%d|%s", chainID, wallet.Hex())
}

func v4PositionKey(chainID int64, positionManager common.Address, tokenID *big.Int) string {
	return fmt.Sprintf("%d|%s|%s", chainID, positionManager.Hex(), tokenID.String())
}

func (f *fakeV4Reader) setPool(chainID int64, pool outbound.UniswapV4PoolRow) {
	f.pools[chainID] = append(f.pools[chainID], pool)
}

func (f *fakeV4Reader) setHeld(chainID int64, wallet common.Address, tokenIDs ...*big.Int) {
	f.held[v4HeldKey(chainID, wallet)] = tokenIDs
}

func (f *fakeV4Reader) setPosition(chainID int64, positionManager common.Address, tokenID *big.Int, p outbound.UniswapV4PositionSnapshot) {
	f.positions[v4PositionKey(chainID, positionManager, tokenID)] = p
}

func (f *fakeV4Reader) LoadPools(ctx context.Context, chainID int64) ([]outbound.UniswapV4PoolRow, error) {
	if f.poolsErr != nil {
		return nil, f.poolsErr
	}
	return f.pools[chainID], nil
}

func (f *fakeV4Reader) HeldTokenIDsAtBlock(ctx context.Context, chainID int64, wallet common.Address, blockNumber int64) ([]*big.Int, error) {
	if f.heldErr != nil {
		return nil, f.heldErr
	}
	return f.held[v4HeldKey(chainID, wallet)], nil
}

func (f *fakeV4Reader) PositionForTokenAtBlock(
	ctx context.Context, chainID int64, positionManager common.Address, tokenID *big.Int, blockNumber int64,
) (*outbound.UniswapV4PositionSnapshot, error) {
	if f.positionErr != nil {
		return nil, f.positionErr
	}
	p, ok := f.positions[v4PositionKey(chainID, positionManager, tokenID)]
	if !ok {
		return nil, nil
	}
	return &p, nil
}

func (f *fakeV4Reader) PoolStateAtBlock(ctx context.Context, poolID int64, blockNumber int64) (*big.Int, error) {
	if f.poolStateErr != nil {
		return nil, f.poolStateErr
	}
	return f.poolStates[poolID], nil
}

// ---------------------------------------------------------------------------
// Shared fixtures.
// ---------------------------------------------------------------------------

const v4ChainID = int64(1)

var (
	v4PositionManagerAddr = common.HexToAddress("0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e")
	v4Wallet              = common.HexToAddress("0x1601843c5E9bC251A3272907010AFa41Fa18347E")
	// v4HintAsset is the common quote leg across every pool in these fixtures,
	// mirroring how Spark's PYUSD/USDS, USDT/USDS and RLUSD/USDS V4 pools all
	// settle in USDS.
	v4HintAsset  = common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	v4CurrencyA  = common.HexToAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a")
	v4CurrencyB  = common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7")
	v4BlockHash  = common.HexToHash("0x4444444444444444444444444444444444444444444444444444444444444a")
	v4BlockNum   = int64(20_000_000)
	v4TokenIDOne = big.NewInt(1)
	v4TokenIDTwo = big.NewInt(2)
	v4FullRangeL = univ3FullRangeL
	v4FullRangeU = univ3FullRangeU
)

func v4Entry(tokenType string, asset *common.Address) *TokenEntry {
	return &TokenEntry{
		ContractAddress: v4PositionManagerAddr,
		WalletAddress:   v4Wallet,
		AssetAddress:    asset,
		Star:            "spark",
		Chain:           "mainnet",
		TokenType:       tokenType,
	}
}

// newV4BlockState returns a BlockStateRepository seeded so hash resolves to
// number.
func newV4BlockState(t *testing.T, hash common.Hash, number int64) outbound.BlockHashResolver {
	t.Helper()
	repo := memory.NewBlockStateRepository()
	if _, err := repo.SaveBlock(context.Background(), outbound.BlockState{
		Number:         number,
		Hash:           hash.Hex(),
		BlockTimestamp: 1_700_000_000,
	}); err != nil {
		t.Fatalf("seed block state: %v", err)
	}
	return repo
}

func fetchUniV4Balance(t *testing.T, reader outbound.UniswapV4PositionValuationReader, entry *TokenEntry) *PositionBalance {
	t.Helper()
	src := NewUniV4Source(reader, newV4BlockState(t, v4BlockHash, v4BlockNum), quietLogger())
	result, err := src.FetchBalances(context.Background(), []*TokenEntry{entry}, v4BlockHash)
	if err != nil {
		t.Fatalf("FetchBalances: %v", err)
	}
	bal, ok := result.Balances[entry.Key()]
	if !ok {
		t.Fatalf("no balance recorded for entry %+v", entry.Key())
	}
	return bal
}

// erroringBlockState wraps the in-memory fake to force GetBlockByHash to
// fail, exercising the branch a healthy block_states read never takes.
type erroringBlockState struct{}

func (erroringBlockState) GetBlockByHash(ctx context.Context, hash string) (*outbound.BlockState, error) {
	return nil, errors.New("db unavailable")
}

// recordingV4Reader wraps fakeV4Reader to capture the blockNumber every call
// resolved to, proving FetchBalances used the hash-resolved number rather than
// some other value.
type recordingV4Reader struct {
	*fakeV4Reader
	gotBlockNumbers []int64
}

func (r *recordingV4Reader) HeldTokenIDsAtBlock(ctx context.Context, chainID int64, wallet common.Address, blockNumber int64) ([]*big.Int, error) {
	r.gotBlockNumbers = append(r.gotBlockNumbers, blockNumber)
	return r.fakeV4Reader.HeldTokenIDsAtBlock(ctx, chainID, wallet, blockNumber)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestUniV4Source_Name(t *testing.T) {
	src := NewUniV4Source(nil, nil, quietLogger())
	if got := src.Name(); got != "uni-v4" {
		t.Errorf("Name() = %q, want %q", got, "uni-v4")
	}
}

func TestUniV4Source_FetchBalances_BlockStateLookupError(t *testing.T) {
	src := NewUniV4Source(newFakeV4Reader(t), erroringBlockState{}, quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{v4Entry("uni_v4_lp", &v4HintAsset)}, v4BlockHash)
	if err == nil {
		t.Fatal("expected the block_states lookup error to propagate")
	}
}

func TestUniV4Source_Supports(t *testing.T) {
	src := NewUniV4Source(nil, nil, quietLogger())
	tests := []struct {
		tokenType string
		want      bool
	}{
		{"uni_v4_pool", true},
		{"uni_v4_lp", true},
		{"uni_v3_pool", false},
		{"erc20", false},
	}
	for _, tt := range tests {
		if got := src.Supports(tt.tokenType, "uniswap"); got != tt.want {
			t.Errorf("Supports(%q) = %v, want %v", tt.tokenType, got, tt.want)
		}
	}
}

func TestUniV4Source_FetchBalances_EmptyEntries(t *testing.T) {
	src := NewUniV4Source(nil, nil, quietLogger())
	result, err := src.FetchBalances(context.Background(), nil, v4BlockHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Balances) != 0 {
		t.Errorf("expected empty balances, got %d", len(result.Balances))
	}
}

func TestUniV4Source_FetchBalances_NoAssetAddress_Error(t *testing.T) {
	src := NewUniV4Source(newFakeV4Reader(t), newV4BlockState(t, v4BlockHash, v4BlockNum), quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{v4Entry("uni_v4_lp", nil)}, v4BlockHash)
	if err == nil {
		t.Fatal("expected error for missing asset address")
	}
}

func TestUniV4Source_FetchBalances_UnresolvedBlockHash_Error(t *testing.T) {
	reader := newFakeV4Reader(t)
	src := NewUniV4Source(reader, memory.NewBlockStateRepository(), quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{v4Entry("uni_v4_lp", &v4HintAsset)}, v4BlockHash)
	if err == nil {
		t.Fatal("expected error when the block hash is not in block_states")
	}
}

func TestUniV4Source_FetchBalances_UnknownChain_Error(t *testing.T) {
	entry := v4Entry("uni_v4_lp", &v4HintAsset)
	entry.Chain = "not-a-real-chain"
	src := NewUniV4Source(newFakeV4Reader(t), newV4BlockState(t, v4BlockHash, v4BlockNum), quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{entry}, v4BlockHash)
	if err == nil {
		t.Fatal("expected error for unrecognised chain")
	}
}

func TestUniV4Source_FetchBalances_NoRegisteredPools_WritesZeroRow(t *testing.T) {
	bal := fetchUniV4Balance(t, newFakeV4Reader(t), v4Entry("uni_v4_lp", &v4HintAsset))
	if bal.Balance == nil || bal.Balance.Sign() != 0 {
		t.Errorf("Balance = %v, want explicit zero", bal.Balance)
	}
	if bal.UnderlyingValue == nil || bal.UnderlyingValue.Sign() != 0 {
		t.Errorf("UnderlyingValue = %v, want explicit zero", bal.UnderlyingValue)
	}
}

func TestUniV4Source_FetchBalances_NoHeldTokens_WritesZeroRow(t *testing.T) {
	reader := newFakeV4ReaderWithPool(t, v4DefaultPool())
	bal := fetchUniV4Balance(t, reader, v4Entry("uni_v4_lp", &v4HintAsset))
	if bal.Balance == nil || bal.Balance.Sign() != 0 {
		t.Errorf("Balance = %v, want explicit zero", bal.Balance)
	}
	if bal.UnderlyingValue == nil || bal.UnderlyingValue.Sign() != 0 {
		t.Errorf("UnderlyingValue = %v, want explicit zero", bal.UnderlyingValue)
	}
}

// TestUniV4Source_FetchBalances_ValuesInHintAsset covers both sides of the
// spot conversion, mirroring UniV3Source's equivalent full-value test: the
// same uniswapv3.ComputePositionAmounts + ValueInTokenN math is reused
// unchanged, only the data source (Postgres reads, not a live positions()
// call) differs.
func TestUniV4Source_FetchBalances_ValuesInHintAsset(t *testing.T) {
	liquidity := big.NewInt(1_000_000_000_000_000_000)
	sqrtPriceX96 := univ3TestPrice4() // price = 4: currencyB per currencyA
	amounts := uniswapv3.ComputePositionAmounts(sqrtPriceX96, v4FullRangeL, v4FullRangeU, liquidity)
	if amounts.Amount0.Sign() <= 0 || amounts.Amount1.Sign() <= 0 {
		t.Fatalf("fixture must be two-sided, got amount0=%s amount1=%s", amounts.Amount0, amounts.Amount1)
	}

	tests := []struct {
		name string
		hint common.Address
		want *big.Int
	}{
		{
			name: "hint is currency1: amount1 + amount0 x price",
			hint: v4CurrencyB,
			want: new(big.Int).Add(amounts.Amount1, new(big.Int).Mul(amounts.Amount0, big.NewInt(4))),
		},
		{
			name: "hint is currency0: amount0 + amount1 / price",
			hint: v4CurrencyA,
			want: new(big.Int).Add(amounts.Amount0, new(big.Int).Div(amounts.Amount1, big.NewInt(4))),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := newFakeV4ReaderWithPool(t, outbound.UniswapV4PoolRow{
				ID: 10, PositionManager: v4PositionManagerAddr, Currency0: v4CurrencyA, Currency1: v4CurrencyB,
			})
			reader.setHeld(v4ChainID, v4Wallet, v4TokenIDOne)
			reader.setPosition(v4ChainID, v4PositionManagerAddr, v4TokenIDOne, outbound.UniswapV4PositionSnapshot{
				PoolID: 10, TickLower: v4FullRangeL, TickUpper: v4FullRangeU, Liquidity: liquidity,
			})
			reader.poolStates[10] = sqrtPriceX96

			bal := fetchUniV4Balance(t, reader, v4Entry("uni_v4_lp", &tc.hint))
			if bal.Balance.Cmp(tc.want) != 0 {
				t.Errorf("Balance = %s, want %s", bal.Balance, tc.want)
			}
			if bal.UnderlyingValue == nil || bal.UnderlyingValue.Cmp(tc.want) != 0 {
				t.Errorf("UnderlyingValue = %v, want %s", bal.UnderlyingValue, tc.want)
			}
			if bal.PoolToken0 != nil || bal.PoolToken1 != nil {
				t.Errorf("PoolToken0/1 must stay nil for an aggregated V4 entry, got %v/%v", bal.PoolToken0, bal.PoolToken1)
			}
		})
	}
}

// TestUniV4Source_FetchBalances_SumsAcrossPools is the headline VEC-829 case:
// one entry aggregates positions from pools with different pairs (the posm
// token id names no pool), summed in the shared hint asset — including the
// case where the hint asset sits on different sides (currency0 vs currency1)
// of the two pools, as Spark's real PYUSD/USDS vs USDT/USDS pools do.
func TestUniV4Source_FetchBalances_SumsAcrossPools(t *testing.T) {
	liqA := big.NewInt(1_000_000_000_000_000_000)
	liqB := big.NewInt(2_000_000_000_000_000)
	sqrtPriceX96 := univ3TestPrice4()

	amountsA := uniswapv3.ComputePositionAmounts(sqrtPriceX96, v4FullRangeL, v4FullRangeU, liqA)
	amountsB := uniswapv3.ComputePositionAmounts(sqrtPriceX96, v4FullRangeL, v4FullRangeU, liqB)

	tests := []struct {
		name      string
		poolB     outbound.UniswapV4PoolRow
		wantBSide *big.Int // pool B's contribution, already in hint units
	}{
		{
			name:      "hint is currency1 in both pools",
			poolB:     outbound.UniswapV4PoolRow{ID: 20, PositionManager: v4PositionManagerAddr, Currency0: v4CurrencyB, Currency1: v4HintAsset},
			wantBSide: new(big.Int).Add(amountsB.Amount1, new(big.Int).Mul(amountsB.Amount0, big.NewInt(4))),
		},
		{
			name:      "hint is currency1 in pool A but currency0 in pool B",
			poolB:     outbound.UniswapV4PoolRow{ID: 20, PositionManager: v4PositionManagerAddr, Currency0: v4HintAsset, Currency1: v4CurrencyB},
			wantBSide: new(big.Int).Add(amountsB.Amount0, new(big.Int).Div(amountsB.Amount1, big.NewInt(4))),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want := new(big.Int).Add(amountsA.Amount1, new(big.Int).Mul(amountsA.Amount0, big.NewInt(4)))
			want.Add(want, tc.wantBSide)

			reader := newFakeV4ReaderWithPool(t, outbound.UniswapV4PoolRow{
				ID: 10, PositionManager: v4PositionManagerAddr, Currency0: v4CurrencyA, Currency1: v4HintAsset,
			})
			reader.setPool(v4ChainID, tc.poolB)
			reader.setHeld(v4ChainID, v4Wallet, v4TokenIDOne, v4TokenIDTwo)
			reader.setPosition(v4ChainID, v4PositionManagerAddr, v4TokenIDOne, outbound.UniswapV4PositionSnapshot{
				PoolID: 10, TickLower: v4FullRangeL, TickUpper: v4FullRangeU, Liquidity: liqA,
			})
			reader.setPosition(v4ChainID, v4PositionManagerAddr, v4TokenIDTwo, outbound.UniswapV4PositionSnapshot{
				PoolID: 20, TickLower: v4FullRangeL, TickUpper: v4FullRangeU, Liquidity: liqB,
			})
			reader.poolStates[10] = sqrtPriceX96
			reader.poolStates[20] = sqrtPriceX96

			bal := fetchUniV4Balance(t, reader, v4Entry("uni_v4_lp", &v4HintAsset))
			if bal.Balance.Cmp(want) != 0 {
				t.Errorf("Balance = %s, want %s (sum across both pools)", bal.Balance, want)
			}
		})
	}
}

func TestUniV4Source_FetchBalances_ZeroLiquidityPosition_ContributesZero(t *testing.T) {
	reader := newFakeV4ReaderWithPool(t, v4DefaultPool())
	reader.setHeld(v4ChainID, v4Wallet, v4TokenIDOne)
	reader.setPosition(v4ChainID, v4PositionManagerAddr, v4TokenIDOne, outbound.UniswapV4PositionSnapshot{
		PoolID: 10, TickLower: v4FullRangeL, TickUpper: v4FullRangeU, Liquidity: big.NewInt(0),
	})
	// Deliberately no pool state seeded: a zero-liquidity position must never
	// reach PoolStateAtBlock, or this would fail as a missing-state error.

	bal := fetchUniV4Balance(t, reader, v4Entry("uni_v4_lp", &v4HintAsset))
	if bal.Balance.Sign() != 0 {
		t.Errorf("Balance = %s, want zero for a fully-withdrawn position", bal.Balance)
	}
}

func TestUniV4Source_FetchBalances_MissingIndexedPosition_Error(t *testing.T) {
	reader := newFakeV4ReaderWithPool(t, v4DefaultPool())
	reader.setHeld(v4ChainID, v4Wallet, v4TokenIDOne)
	// No position registered: the transfer log says wallet holds the token,
	// but uniswap_v4_position has no row for it.
	src := NewUniV4Source(reader, newV4BlockState(t, v4BlockHash, v4BlockNum), quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{v4Entry("uni_v4_lp", &v4HintAsset)}, v4BlockHash)
	if err == nil {
		t.Fatal("expected error for a held token with no indexed position")
	}
}

func TestUniV4Source_FetchBalances_PoolNotInRegistry_Error(t *testing.T) {
	reader := newFakeV4ReaderWithPool(t, v4DefaultPool())
	reader.setHeld(v4ChainID, v4Wallet, v4TokenIDOne)
	reader.setPosition(v4ChainID, v4PositionManagerAddr, v4TokenIDOne, outbound.UniswapV4PositionSnapshot{
		PoolID: 999, TickLower: v4FullRangeL, TickUpper: v4FullRangeU, Liquidity: big.NewInt(1),
	})
	// The position resolves to pool 999, absent from the registered (pool 10) set.
	src := NewUniV4Source(reader, newV4BlockState(t, v4BlockHash, v4BlockNum), quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{v4Entry("uni_v4_lp", &v4HintAsset)}, v4BlockHash)
	if err == nil {
		t.Fatal("expected error when the position's pool is absent from the chain's pool registry")
	}
}

func TestUniV4Source_FetchBalances_MissingPoolState_Error(t *testing.T) {
	reader := newFakeV4ReaderWithPool(t, v4DefaultPool())
	reader.setHeld(v4ChainID, v4Wallet, v4TokenIDOne)
	reader.setPosition(v4ChainID, v4PositionManagerAddr, v4TokenIDOne, outbound.UniswapV4PositionSnapshot{
		PoolID: 10, TickLower: v4FullRangeL, TickUpper: v4FullRangeU, Liquidity: big.NewInt(1_000_000),
	})
	// No pool state seeded for pool 10.
	src := NewUniV4Source(reader, newV4BlockState(t, v4BlockHash, v4BlockNum), quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{v4Entry("uni_v4_lp", &v4HintAsset)}, v4BlockHash)
	if err == nil {
		t.Fatal("expected error when the pool has no state snapshot")
	}
}

func TestUniV4Source_FetchBalances_HintMatchesNeither_Error(t *testing.T) {
	reader := newFakeV4ReaderWithPool(t, outbound.UniswapV4PoolRow{
		ID: 10, PositionManager: v4PositionManagerAddr, Currency0: v4CurrencyA, Currency1: v4CurrencyB,
	})
	reader.setHeld(v4ChainID, v4Wallet, v4TokenIDOne)
	reader.setPosition(v4ChainID, v4PositionManagerAddr, v4TokenIDOne, outbound.UniswapV4PositionSnapshot{
		PoolID: 10, TickLower: v4FullRangeL, TickUpper: v4FullRangeU, Liquidity: big.NewInt(1_000_000),
	})
	reader.poolStates[10] = univ3TestPrice4()

	src := NewUniV4Source(reader, newV4BlockState(t, v4BlockHash, v4BlockNum), quietLogger())
	// Neither currency0 (A) nor currency1 (B) matches the hint asset.
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{v4Entry("uni_v4_lp", &v4HintAsset)}, v4BlockHash)
	if err == nil {
		t.Fatal("expected error when the hint asset matches neither side of the pool")
	}
}

func TestUniV4Source_FetchBalances_LoadPoolsError(t *testing.T) {
	reader := newFakeV4Reader(t)
	reader.poolsErr = errors.New("db unavailable")
	src := NewUniV4Source(reader, newV4BlockState(t, v4BlockHash, v4BlockNum), quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{v4Entry("uni_v4_lp", &v4HintAsset)}, v4BlockHash)
	if err == nil {
		t.Fatal("expected the LoadPools error to propagate")
	}
}

func TestUniV4Source_FetchBalances_HeldTokenIDsError(t *testing.T) {
	reader := newFakeV4ReaderWithPool(t, v4DefaultPool())
	reader.heldErr = errors.New("db unavailable")
	src := NewUniV4Source(reader, newV4BlockState(t, v4BlockHash, v4BlockNum), quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{v4Entry("uni_v4_lp", &v4HintAsset)}, v4BlockHash)
	if err == nil {
		t.Fatal("expected the held-token-ids error to propagate")
	}
}

func TestUniV4Source_FetchBalances_PositionLookupError(t *testing.T) {
	reader := newFakeV4ReaderWithPool(t, v4DefaultPool())
	reader.setHeld(v4ChainID, v4Wallet, v4TokenIDOne)
	reader.positionErr = errors.New("db unavailable")
	src := NewUniV4Source(reader, newV4BlockState(t, v4BlockHash, v4BlockNum), quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{v4Entry("uni_v4_lp", &v4HintAsset)}, v4BlockHash)
	if err == nil {
		t.Fatal("expected the position lookup error to propagate")
	}
}

func TestUniV4Source_FetchBalances_PoolStateLookupError(t *testing.T) {
	reader := newFakeV4ReaderWithPool(t, v4DefaultPool())
	reader.setHeld(v4ChainID, v4Wallet, v4TokenIDOne)
	reader.setPosition(v4ChainID, v4PositionManagerAddr, v4TokenIDOne, outbound.UniswapV4PositionSnapshot{
		PoolID: 10, TickLower: v4FullRangeL, TickUpper: v4FullRangeU, Liquidity: big.NewInt(1_000_000),
	})
	reader.poolStateErr = errors.New("db unavailable")
	src := NewUniV4Source(reader, newV4BlockState(t, v4BlockHash, v4BlockNum), quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{v4Entry("uni_v4_lp", &v4HintAsset)}, v4BlockHash)
	if err == nil {
		t.Fatal("expected the pool-state lookup error to propagate")
	}
}

// TestUniV4Source_FetchBalances_PinsBlockNumberFromHash proves the number
// resolved from block_states — not some other value — is what reaches the
// Postgres reads, by recording every blockNumber the reader is called with
// against two seeded (hash, number) pairs.
func TestUniV4Source_FetchBalances_PinsBlockNumberFromHash(t *testing.T) {
	reader := &recordingV4Reader{fakeV4Reader: newFakeV4ReaderWithPool(t, v4DefaultPool())}
	other := common.HexToHash("0x9999999999999999999999999999999999999999999999999999999999999a")
	blockState := memory.NewBlockStateRepository()
	if _, err := blockState.SaveBlock(context.Background(), outbound.BlockState{
		Number: 111, Hash: v4BlockHash.Hex(), BlockTimestamp: 1_700_000_000,
	}); err != nil {
		t.Fatalf("seed block state: %v", err)
	}
	if _, err := blockState.SaveBlock(context.Background(), outbound.BlockState{
		Number: 222, Hash: other.Hex(), BlockTimestamp: 1_700_000_001,
	}); err != nil {
		t.Fatalf("seed block state: %v", err)
	}

	src := NewUniV4Source(reader, blockState, quietLogger())
	if _, err := src.FetchBalances(context.Background(), []*TokenEntry{v4Entry("uni_v4_lp", &v4HintAsset)}, v4BlockHash); err != nil {
		t.Fatalf("FetchBalances: %v", err)
	}

	if len(reader.gotBlockNumbers) != 1 || reader.gotBlockNumbers[0] != 111 {
		t.Errorf("resolved block numbers = %v, want [111] (v4BlockHash's number, not other's 222)", reader.gotBlockNumbers)
	}
}
