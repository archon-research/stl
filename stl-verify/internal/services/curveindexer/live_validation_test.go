//go:build livevalidation

package curveindexer

import (
	"context"
	"log/slog"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// liveCurveExpectation is what each seeded pool must actually return. The
// curation itself comes from the database, so a column that disagrees with the
// contract surfaces as a revert; this table catches the other direction, where a
// getter the pool does have is curated FALSE and its column silently goes NULL.
type liveCurveExpectation struct {
	name            string
	wantPriceOracle bool
	wantStoredRates bool
	wantFutureFee   bool
	wantOffpeg      bool
}

var liveCurveExpectations = map[common.Address]liveCurveExpectation{
	common.HexToAddress("0xDC24316b9AE028F1497c275EB9192a3Ea0f67022"): {
		name: "stETH classic (pre-NG)", wantFutureFee: true,
	},
	common.HexToAddress("0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7"): {
		name: "3pool (pre-NG, 3 coins)", wantFutureFee: true,
	},
	common.HexToAddress("0x21E27a5E5513D6e65C4f830167390997aA84843a"): {
		name: "stETH-ng (original NG)", wantPriceOracle: true, wantStoredRates: true, wantFutureFee: true,
	},
	common.HexToAddress("0x7F86Bf177Dd4F3494b841a37e810A34dD56c829B"): {
		name: "TricryptoUSDC (cryptoswap)",
	},
	common.HexToAddress("0x00836fe54625be242bcfa286207795405ca4fd10"): {
		name: "sUSDS/USDT (ARCT-384)", wantStoredRates: true, wantOffpeg: true,
	},
	common.HexToAddress("0xa632d59b9b804a956bfaa9b48af3a1b74808fc1f"): {
		name: "PYUSD/USDS (ARCT-384)", wantStoredRates: true, wantOffpeg: true,
	},
	common.HexToAddress("0xe79c1c7e24755574438a26d5e062ad2626c04662"): {
		name: "USDC/AUSD (ARCT-384)", wantStoredRates: true, wantOffpeg: true,
	},
	common.HexToAddress("0x4f493b7de8aac7d55f71853688b1f7c8f0243c85"): {
		name: "USDC/USDT (ARCT-384)", wantStoredRates: true, wantOffpeg: true,
	},
	common.HexToAddress("0xdb74dfdd3bb46be8ce6c33dc9d82777bcfc3ded5"): {
		name: "WETH/weETH (ARCT-384)", wantStoredRates: true, wantOffpeg: true,
	},
}

// TestLiveCurveSnapshot runs the real snapshot path against mainnet for every
// seeded Curve pool, with the capability columns read from a freshly migrated
// database rather than restated here. It is the only check that the curation
// matches the deployed contracts: a pool curated as exposing a getter it does
// not have reverts, and a reverted snapshot read stops every block on the chain,
// so what this catches is a stalled indexer rather than a missing value.
//
//	ALCHEMY_API_KEY=… go test -tags=livevalidation -run TestLiveCurveSnapshot ./internal/services/curveindexer/
func TestLiveCurveSnapshot(t *testing.T) {
	ctx := context.Background()
	ec, err := ethclient.DialContext(ctx, testutil.AlchemyMainnetURL(t))
	if err != nil {
		t.Fatalf("dialing mainnet: %v", err)
	}
	defer ec.Close()

	bn, hash := settledBlock(t, ctx, ec)
	mc, err := multicall.NewClient(ec, blockchain.Multicall3)
	if err != nil {
		t.Fatalf("creating multicall client: %v", err)
	}
	handlers := liveHandlers(t)

	for _, pool := range loadSeededCurvePools(t, ctx) {
		want, known := liveCurveExpectations[pool.Address]
		if !known {
			t.Errorf("pool %s is seeded but has no expectation here; add it", pool.Address)
			continue
		}
		t.Run(want.name, func(t *testing.T) {
			assertLivePool(t, ctx, handlers, mc, pool, bn, hash, want)
		})
	}
}

// assertLivePool snapshots one pool through its class handler and checks that
// the fields the pool actually has are the fields that came back. The two
// classes return different typed states, so each branch asserts its own shape.
func assertLivePool(
	t *testing.T,
	ctx context.Context,
	h *liveHandlers2,
	mc outbound.Multicaller,
	pool RegisteredPool,
	bn int64,
	hash common.Hash,
	want liveCurveExpectation,
) {
	t.Helper()
	ts := time.Now().UTC()

	switch pool.Kind {
	case KindStableswapPreNG, KindStableswapNG:
		st, cfg, err := h.stable.SnapshotState(ctx, mc, pool, bn, 0, hash, ts)
		if err != nil {
			t.Fatalf("SnapshotState: %v (a revert here means the curated capability columns disagree with the deployed contract)", err)
		}
		assertPositive(t, "virtual_price", st.VirtualPrice)
		assertPositive(t, "calc_token_amount", st.CalcTokenAmount)
		assertLivePresence(t, "price_oracle", st.PriceOracle != nil, want.wantPriceOracle)
		assertLivePresence(t, "stored_rates", len(st.StoredRates) == pool.NCoins, want.wantStoredRates)
		assertLivePresence(t, "future_fee", cfg.FutureFee != nil, want.wantFutureFee)
		assertLivePresence(t, "offpeg_fee_multiplier", cfg.OffpegFeeMultiplier != nil, want.wantOffpeg)
	case KindCryptoswap:
		st, _, err := h.crypto.SnapshotState(ctx, mc, pool, bn, 0, hash, ts)
		if err != nil {
			t.Fatalf("SnapshotState: %v (a revert here means the curated capability columns disagree with the deployed contract)", err)
		}
		assertPositive(t, "virtual_price", st.VirtualPrice)
		assertPositive(t, "calc_token_amount", st.CalcTokenAmount)
	default:
		t.Fatalf("unknown pool_kind %q", pool.Kind)
	}
}

type liveHandlers2 struct {
	stable *StableswapHandler
	crypto *CryptoswapHandler
}

func liveHandlers(t *testing.T) *liveHandlers2 {
	t.Helper()
	stableABI, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading stableswap ABI: %v", err)
	}
	cryptoABI, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading cryptoswap ABI: %v", err)
	}
	return &liveHandlers2{stable: NewStableswapHandler(stableABI), crypto: NewCryptoswapHandler(cryptoABI)}
}

// loadSeededCurvePools migrates a throwaway database and reads the registry back
// through LoadPools, so the capability columns under test are the ones the
// migrations actually wrote.
func loadSeededCurvePools(t *testing.T, ctx context.Context) []RegisteredPool {
	t.Helper()
	dbPool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	repo, err := postgres.NewCurveRepository(dbPool, slog.New(slog.NewTextHandler(os.Stderr, nil)), buildregistry.BuildID(1))
	if err != nil {
		t.Fatalf("creating curve repository: %v", err)
	}
	rows, err := repo.LoadPools(ctx, 1)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}
	if len(rows) != len(liveCurveExpectations) {
		t.Fatalf("LoadPools returned %d pools, want %d (a seed migration did not apply, or a pool was added without an expectation here)",
			len(rows), len(liveCurveExpectations))
	}
	return IndexPoolsByAddress(rows)
}

// settledBlock steps back from the head so every node agrees on the hash the
// multicall pins to.
func settledBlock(t *testing.T, ctx context.Context, ec *ethclient.Client) (int64, common.Hash) {
	t.Helper()
	head, err := ec.HeaderByNumber(ctx, nil)
	if err != nil {
		t.Fatalf("fetching head: %v", err)
	}
	hdr, err := ec.HeaderByNumber(ctx, new(big.Int).Sub(head.Number, big.NewInt(5)))
	if err != nil {
		t.Fatalf("fetching header: %v", err)
	}
	return hdr.Number.Int64(), hdr.Hash()
}

func assertPositive(t *testing.T, field string, v *big.Int) {
	t.Helper()
	if v == nil || v.Sign() <= 0 {
		t.Errorf("%s = %v, want a positive value", field, v)
	}
}

func assertLivePresence(t *testing.T, field string, got, want bool) {
	t.Helper()
	if got != want {
		t.Errorf("%s present = %v, want %v", field, got, want)
	}
}
