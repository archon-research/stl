package oracle_backfill

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	curvePoolAddr     = common.HexToAddress("0xE79c1C7E24755574438A26D5e062AD2626c04662")
	curveUSDCFeedAddr = common.HexToAddress("0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6")
	curveAUSDFeedAddr = common.HexToAddress("0xB00341502DfEA6Ced8A5786b4059d29dA5E4D1FD")
)

const curveLPTokenID = 42

// curveLPNGRepoSetup returns a mockRepo preconfigured for a single curve_lp_ng
// oracle (curve_ausdusdc_lp, oracle ID 7) pricing LP token 42 against its two
// coin USD feeds (USDC at 8 decimals, AUSD at 18 decimals).
func curveLPNGRepoSetup() *mockRepo {
	return &mockRepo{
		getEnabledOraclesByChainFn: func(_ context.Context, _ int64) ([]*entity.Oracle, error) {
			return []*entity.Oracle{{
				ID: 7, Name: "curve_ausdusdc_lp", Enabled: true,
				OracleType: entity.OracleTypeCurveLPNG, Address: curvePoolAddr,
				DeploymentBlock: 100, PriceDecimals: 8,
			}}, nil
		},
		getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
			return []*entity.OracleAsset{
				{ID: 1, OracleID: 7, TokenID: curveLPTokenID, Enabled: true, FeedAddress: curveUSDCFeedAddr, FeedDecimals: 8, QuoteCurrency: "USD"},
				{ID: 2, OracleID: 7, TokenID: curveLPTokenID, Enabled: true, FeedAddress: curveAUSDFeedAddr, FeedDecimals: 18, QuoteCurrency: "USD"},
			}, nil
		},
		getTokenInfosFn: func(_ context.Context, _ int64) (map[int64]outbound.TokenInfo, error) {
			return map[int64]outbound.TokenInfo{curveLPTokenID: {Address: curvePoolAddr.Bytes(), Decimals: 18}}, nil
		},
	}
}

// curveHeaderFor returns the header the mock header fetcher serves for a
// block. Tests derive the expected pin hash from it via Header.Hash(), the
// same derivation the backfill uses on real Erigon headers.
func curveHeaderFor(blockNum int64) *ethtypes.Header {
	return &ethtypes.Header{Time: uint64(1700000000 + blockNum)}
}

func curveHeaderFetcher() *mockHeaderFetcher {
	return &mockHeaderFetcher{
		headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
			return curveHeaderFor(number.Int64()), nil
		},
	}
}

func packCurveVirtualPrice(t *testing.T, price *big.Int) []byte {
	t.Helper()
	poolABI, err := abis.GetCurveNGPoolABI()
	if err != nil {
		t.Fatalf("loading Curve NG pool ABI: %v", err)
	}
	data, err := poolABI.Methods["get_virtual_price"].Outputs.Pack(price)
	if err != nil {
		t.Fatalf("packing get_virtual_price return: %v", err)
	}
	return data
}

// curvePricingResults answers one pricing batch: get_virtual_price plus the
// two coin feed latestRoundData answers (USDC 8 decimals, AUSD 18 decimals).
func curvePricingResults(t *testing.T, virtualPrice, usdcAnswer, ausdAnswer *big.Int) []outbound.Result {
	t.Helper()
	return []outbound.Result{
		{Success: true, ReturnData: packCurveVirtualPrice(t, virtualPrice)},
		{Success: true, ReturnData: testutil.PackLatestRoundData(t, big.NewInt(1), usdcAnswer, big.NewInt(1000), big.NewInt(1000), big.NewInt(1))},
		{Success: true, ReturnData: testutil.PackLatestRoundData(t, big.NewInt(1), ausdAnswer, big.NewInt(1000), big.NewInt(1000), big.NewInt(1))},
	}
}

// curveHashRecorder resolves the block hashes ExecuteAtHash receives back to
// block numbers via the canonical mock-header hashes, recording which blocks
// received a hash-pinned pricing read. An unknown hash fails the lookup, so
// any pricing read not pinned to a real header hash surfaces as a test
// failure.
type curveHashRecorder struct {
	mu          sync.Mutex
	hashToBlock map[common.Hash]int64
	readBlocks  []int64
}

func newCurveHashRecorder(fromBlock, toBlock int64) *curveHashRecorder {
	hashToBlock := make(map[common.Hash]int64)
	for bn := fromBlock; bn <= toBlock; bn++ {
		hashToBlock[curveHeaderFor(bn).Hash()] = bn
	}
	return &curveHashRecorder{hashToBlock: hashToBlock}
}

func (r *curveHashRecorder) resolve(blockHash common.Hash) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	bn, ok := r.hashToBlock[blockHash]
	if !ok {
		return 0, errors.New("pricing read pinned to a hash that matches no canonical header")
	}
	r.readBlocks = append(r.readBlocks, bn)
	return bn, nil
}

func (r *curveHashRecorder) reads() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]int64, len(r.readBlocks))
	copy(cp, r.readBlocks)
	return cp
}

// curveBackfillFactory wires the backfill's two multicaller uses: the first
// instance serves the one-time feed decimals validation (number-pinned, 8/18
// matching the seeded config), every later instance serves hash-pinned
// pricing batches from resultsByBlock. Results are pre-packed on the test
// goroutine because the pricing callback runs on backfill worker goroutines,
// where t.Fatalf-based packers must not be called. ExecuteFn stays nil on
// pricing instances so a number-pinned pricing read fails loudly instead of
// silently unpinning.
func curveBackfillFactory(
	t *testing.T,
	recorder *curveHashRecorder,
	resultsByBlock map[int64][]outbound.Result,
) MulticallFactory {
	t.Helper()
	var mu sync.Mutex
	first := true
	return func(_ entity.OracleType) (outbound.Multicaller, error) {
		mu.Lock()
		isFirst := first
		first = false
		mu.Unlock()

		if isFirst {
			return &testutil.MockMulticaller{
				ExecuteFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					if len(calls) != 2 {
						t.Errorf("expected 2 decimals() validation calls, got %d", len(calls))
					}
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackDecimals(t, 8)},
						{Success: true, ReturnData: testutil.PackDecimals(t, 18)},
					}, nil
				},
			}, nil
		}
		return &testutil.MockMulticaller{
			ExecuteAtHashFn: func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
				if len(calls) != 3 {
					t.Errorf("expected 3 pricing calls (get_virtual_price + 2 feeds), got %d", len(calls))
				}
				bn, err := recorder.resolve(blockHash)
				if err != nil {
					return nil, err
				}
				results, ok := resultsByBlock[bn]
				if !ok {
					return nil, fmt.Errorf("no pricing results prepared for block %d", bn)
				}
				return results, nil
			},
		}, nil
	}
}

// ---------------------------------------------------------------------------
// TestRun_CurveLPNGOracle: end-to-end curve LP backfill — hash-pinned pricing
// against each block's canonical header, correct price math, and change-only
// persistence.
// ---------------------------------------------------------------------------

func TestRun_CurveLPNGOracle(t *testing.T) {
	// virtual_price 1.25 on blocks 100-101, 1.50 on block 102 (dyadic values,
	// so the expected products are exact in float64).
	// USDC 0.75 (8 dec), AUSD 1.5 (18 dec) → min 0.75.
	// Block 100: 1.25 * 0.75 = $0.9375 (stored)
	// Block 101: same → NOT stored
	// Block 102: 1.50 * 0.75 = $1.125 (stored)
	usdc, ausd := big.NewInt(75_000_000), big.NewInt(1_500_000_000_000_000_000)
	pricingByBlock := map[int64][]outbound.Result{
		100: curvePricingResults(t, big.NewInt(1_250_000_000_000_000_000), usdc, ausd),
		101: curvePricingResults(t, big.NewInt(1_250_000_000_000_000_000), usdc, ausd),
		102: curvePricingResults(t, big.NewInt(1_500_000_000_000_000_000), usdc, ausd),
	}

	repo := curveLPNGRepoSetup()
	recorder := newCurveHashRecorder(100, 102)
	mcFactory := curveBackfillFactory(t, recorder, pricingByBlock)

	svc, err := NewService(
		Config{ChainID: 1, Concurrency: 1, BatchSize: 100, Logger: testutil.DiscardLogger()},
		curveHeaderFetcher(),
		mcFactory,
		repo,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if err := svc.Run(context.Background(), 100, 102); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reads := recorder.reads()
	if len(reads) != 3 {
		t.Errorf("hash-pinned pricing reads = %d (%v), want 3 (one per block)", len(reads), reads)
	}

	upserted := repo.getUpserted()
	if len(upserted) != 2 {
		t.Fatalf("upserted count = %d, want 2 (blocks 100 and 102)", len(upserted))
	}

	byBlock := make(map[int64]*entity.OnchainTokenPrice)
	for _, p := range upserted {
		if p.TokenID != curveLPTokenID {
			t.Errorf("TokenID = %d, want %d", p.TokenID, curveLPTokenID)
		}
		if p.OracleID != 7 {
			t.Errorf("OracleID = %d, want 7", p.OracleID)
		}
		if p.BlockVersion != 0 {
			t.Errorf("BlockVersion = %d, want 0 (historical backfill)", p.BlockVersion)
		}
		if want := time.Unix(int64(curveHeaderFor(p.BlockNumber).Time), 0).UTC(); !p.Timestamp.Equal(want) {
			t.Errorf("block %d Timestamp = %v, want %v", p.BlockNumber, p.Timestamp, want)
		}
		byBlock[p.BlockNumber] = p
	}
	if p, ok := byBlock[100]; !ok || p.PriceUSD != 0.9375 {
		t.Errorf("block 100 price = %+v, want 0.9375", byBlock[100])
	}
	if p, ok := byBlock[102]; !ok || p.PriceUSD != 1.125 {
		t.Errorf("block 102 price = %+v, want 1.125", byBlock[102])
	}
	if _, ok := byBlock[101]; ok {
		t.Error("block 101 should be skipped (unchanged price)")
	}
}

// ---------------------------------------------------------------------------
// TestRun_CurveLPNGOracle_PoolCallFailureSkipsBlock: FetchCurveLPNGPrices
// hard-errors on a failed get_virtual_price sub-call (no partial success for
// a single-token unit), so the block is counted failed and stores nothing
// while the rest of the range proceeds — the backfill's standard per-block
// failure semantics.
// ---------------------------------------------------------------------------

func TestRun_CurveLPNGOracle_PoolCallFailureSkipsBlock(t *testing.T) {
	usdc, ausd := big.NewInt(75_000_000), big.NewInt(1_500_000_000_000_000_000)
	pricingByBlock := map[int64][]outbound.Result{
		100: curvePricingResults(t, big.NewInt(1_250_000_000_000_000_000), usdc, ausd),
		101: {{Success: false}, {Success: false}, {Success: false}},
		102: curvePricingResults(t, big.NewInt(1_300_000_000_000_000_000), usdc, ausd),
	}

	repo := curveLPNGRepoSetup()
	recorder := newCurveHashRecorder(100, 102)
	mcFactory := curveBackfillFactory(t, recorder, pricingByBlock)

	svc, err := NewService(
		Config{ChainID: 1, Concurrency: 1, BatchSize: 100, Logger: testutil.DiscardLogger()},
		curveHeaderFetcher(),
		mcFactory,
		repo,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if err := svc.Run(context.Background(), 100, 102); err != nil {
		t.Fatalf("Run: %v", err)
	}

	upserted := repo.getUpserted()
	if len(upserted) != 2 {
		t.Fatalf("upserted count = %d, want 2 (blocks 100 and 102)", len(upserted))
	}
	for _, p := range upserted {
		if p.BlockNumber == 101 {
			t.Error("found price at block 101 (pool call failed, block must store nothing)")
		}
	}
}

// ---------------------------------------------------------------------------
// TestRun_CurveLPNGOracle_HeaderFetchFailureSkipsBlock: curve pricing needs
// the block's real hash before the multicall, so a failed header fetch fails
// the block outright — it must never downgrade to an unpinned read.
// ---------------------------------------------------------------------------

func TestRun_CurveLPNGOracle_HeaderFetchFailureSkipsBlock(t *testing.T) {
	usdc, ausd := big.NewInt(75_000_000), big.NewInt(1_500_000_000_000_000_000)
	pricingByBlock := map[int64][]outbound.Result{
		100: curvePricingResults(t, big.NewInt(1_250_000_000_000_000_000), usdc, ausd),
		102: curvePricingResults(t, big.NewInt(1_300_000_000_000_000_000), usdc, ausd),
	}

	repo := curveLPNGRepoSetup()
	recorder := newCurveHashRecorder(100, 102)
	mcFactory := curveBackfillFactory(t, recorder, pricingByBlock)

	headerFetcher := &mockHeaderFetcher{
		headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
			if number.Int64() == 101 {
				return nil, errors.New("header not found")
			}
			return curveHeaderFor(number.Int64()), nil
		},
	}

	svc, err := NewService(
		Config{ChainID: 1, Concurrency: 1, BatchSize: 100, Logger: testutil.DiscardLogger()},
		headerFetcher,
		mcFactory,
		repo,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if err := svc.Run(context.Background(), 100, 102); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reads := recorder.reads()
	for _, bn := range reads {
		if bn == 101 {
			t.Error("block 101 was priced despite its header fetch failing (must fail before the multicall)")
		}
	}
	if len(reads) != 2 {
		t.Errorf("hash-pinned pricing reads = %d (%v), want 2 (blocks 100 and 102)", len(reads), reads)
	}

	upserted := repo.getUpserted()
	if len(upserted) != 2 {
		t.Fatalf("upserted count = %d, want 2 (blocks 100 and 102)", len(upserted))
	}
	for _, p := range upserted {
		if p.BlockNumber == 101 {
			t.Error("found price at block 101 (header fetch failed, block must store nothing)")
		}
	}
}

// ---------------------------------------------------------------------------
// TestRun_CurveLPNGOracle_ClampsToDeploymentBlock: a range extending below the
// curve oracle's deployment block is clamped, so no pre-deployment block gets
// a pricing read (get_virtual_price on empty code would hard-fail each one).
// ---------------------------------------------------------------------------

func TestRun_CurveLPNGOracle_ClampsToDeploymentBlock(t *testing.T) {
	usdc, ausd := big.NewInt(75_000_000), big.NewInt(1_500_000_000_000_000_000)
	pricingByBlock := map[int64][]outbound.Result{
		100: curvePricingResults(t, big.NewInt(1_250_000_000_000_000_000), usdc, ausd),
		101: curvePricingResults(t, big.NewInt(1_300_000_000_000_000_000), usdc, ausd),
		102: curvePricingResults(t, big.NewInt(1_500_000_000_000_000_000), usdc, ausd),
	}

	repo := curveLPNGRepoSetup() // oracle DeploymentBlock = 100
	recorder := newCurveHashRecorder(50, 102)
	mcFactory := curveBackfillFactory(t, recorder, pricingByBlock)

	svc, err := NewService(
		Config{ChainID: 1, Concurrency: 1, BatchSize: 100, Logger: testutil.DiscardLogger()},
		curveHeaderFetcher(),
		mcFactory,
		repo,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if err := svc.Run(context.Background(), 50, 102); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reads := recorder.reads()
	if len(reads) != 3 {
		t.Errorf("hash-pinned pricing reads = %d (%v), want 3 (blocks 100-102 only)", len(reads), reads)
	}
	for _, bn := range reads {
		if bn < 100 {
			t.Errorf("block %d received a pricing read before the deployment block 100", bn)
		}
	}

	upserted := repo.getUpserted()
	if len(upserted) != 3 {
		t.Errorf("upserted count = %d, want 3 (distinct prices on blocks 100-102)", len(upserted))
	}
	for _, p := range upserted {
		if p.BlockNumber < 100 {
			t.Errorf("found price at block %d, before deployment block 100", p.BlockNumber)
		}
	}
}

// ---------------------------------------------------------------------------
// TestRun_CurveLPNGFeedDecimalsValidation: on-chain coin feed decimals
// contradicting the registry halt the backfill before any mis-scaled
// historical price is written (a mismatch would mis-scale the min() input by
// orders of magnitude).
// ---------------------------------------------------------------------------

func TestRun_CurveLPNGFeedDecimalsValidation(t *testing.T) {
	repo := curveLPNGRepoSetup()

	// decimals() returns 8 for the AUSD feed on-chain, contradicting the seeded 18.
	mcFactory := func(_ entity.OracleType) (outbound.Multicaller, error) {
		return &testutil.MockMulticaller{
			ExecuteFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				results := make([]outbound.Result, len(calls))
				for i := range calls {
					results[i] = outbound.Result{Success: true, ReturnData: testutil.PackDecimals(t, 8)}
				}
				return results, nil
			},
		}, nil
	}

	svc, err := NewService(
		Config{ChainID: 1, Concurrency: 1, BatchSize: 10, Logger: testutil.DiscardLogger()},
		&mockHeaderFetcher{},
		mcFactory,
		repo,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	err = svc.Run(context.Background(), 100, 102)
	if err == nil {
		t.Fatal("expected decimals mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "feed decimals") {
		t.Errorf("error = %q, expected it to contain 'feed decimals'", err)
	}

	if upserted := repo.getUpserted(); len(upserted) != 0 {
		t.Errorf("upserted = %d, want 0 (backfill should have halted)", len(upserted))
	}
}
