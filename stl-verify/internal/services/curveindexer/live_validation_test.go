//go:build livevalidation

package curveindexer

import (
	"context"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
)

// alchemyURL builds the real mainnet endpoint this harness dials, from the same
// ALCHEMY_API_KEY the workers use. Real network access is the point of this
// build-tagged test; it is never compiled into normal `go test`/CI runs.
func alchemyURL(t *testing.T) string {
	t.Helper()
	key := os.Getenv("ALCHEMY_API_KEY")
	if key == "" {
		t.Fatal("ALCHEMY_API_KEY must be set to run TestLiveCurveSnapshot")
	}
	return "https://eth-mainnet.g.alchemy.com/v2/" + key
}

// liveCurvePool mirrors one seeded curve_pool row: the curated capability
// columns as the migrations set them, plus what the pool must return.
type liveCurvePool struct {
	name         string
	address      string
	kind         PoolKind
	coinDecimals []int
	lpToken      string // empty when the pool is its own LP token

	hasAPrecise            bool
	hasNoArgOracleGetters  bool
	calcTokenAmountDyn     bool
	hasFutureFee           bool
	hasOffpegFeeMultiplier bool

	wantPriceOracle bool
	wantStoredRates bool
}

// liveCurvePools is the seeded mainnet registry with the curation the migrations
// apply. Keep it in step with 20260521_110000, 20260831_110000 and 20260831_120000:
// a row here that disagrees with the DB is the bug this test exists to catch.
var liveCurvePools = []liveCurvePool{
	{
		name: "stETH classic (pre-NG)", address: "0xDC24316b9AE028F1497c275EB9192a3Ea0f67022",
		kind: KindStableswapPreNG, coinDecimals: []int{18, 18},
		lpToken:     "0x06325440D014e39736583c165C2963BA99fAf14E",
		hasAPrecise: true, hasFutureFee: true,
	},
	{
		name: "stETH-ng (original NG)", address: "0x21E27a5E5513D6e65C4f830167390997aA84843a",
		kind: KindStableswapNG, coinDecimals: []int{18, 18},
		hasAPrecise: true, hasNoArgOracleGetters: true, hasFutureFee: true,
		wantPriceOracle: true, wantStoredRates: true,
	},
	{
		name: "sUSDS/USDT (ARCT-384)", address: "0x00836fe54625be242bcfa286207795405ca4fd10",
		kind: KindStableswapNG, coinDecimals: []int{18, 6},
		hasAPrecise: true, calcTokenAmountDyn: true, hasOffpegFeeMultiplier: true,
		wantStoredRates: true,
	},
	{
		name: "PYUSD/USDS (ARCT-384)", address: "0xa632d59b9b804a956bfaa9b48af3a1b74808fc1f",
		kind: KindStableswapNG, coinDecimals: []int{6, 18},
		hasAPrecise: true, calcTokenAmountDyn: true, hasOffpegFeeMultiplier: true,
		wantStoredRates: true,
	},
	{
		name: "USDC/AUSD (ARCT-384)", address: "0xe79c1c7e24755574438a26d5e062ad2626c04662",
		kind: KindStableswapNG, coinDecimals: []int{6, 6},
		hasAPrecise: true, calcTokenAmountDyn: true, hasOffpegFeeMultiplier: true,
		wantStoredRates: true,
	},
	{
		name: "USDC/USDT (ARCT-384)", address: "0x4f493b7de8aac7d55f71853688b1f7c8f0243c85",
		kind: KindStableswapNG, coinDecimals: []int{6, 6},
		hasAPrecise: true, calcTokenAmountDyn: true, hasOffpegFeeMultiplier: true,
		wantStoredRates: true,
	},
	{
		name: "WETH/weETH (ARCT-384)", address: "0xdb74dfdd3bb46be8ce6c33dc9d82777bcfc3ded5",
		kind: KindStableswapNG, coinDecimals: []int{18, 18},
		hasAPrecise: true, calcTokenAmountDyn: true, hasOffpegFeeMultiplier: true,
		wantStoredRates: true,
	},
}

func (p liveCurvePool) registered(id int64) RegisteredPool {
	dyn := p.calcTokenAmountDyn
	pool := RegisteredPool{
		ID:                      id,
		Address:                 common.HexToAddress(p.address),
		Kind:                    p.kind,
		NCoins:                  len(p.coinDecimals),
		CoinDecimals:            p.coinDecimals,
		HasAPrecise:             p.hasAPrecise,
		HasNoArgOracleGetters:   p.hasNoArgOracleGetters,
		CalcTokenAmountDynArray: &dyn,
		HasFutureFee:            p.hasFutureFee,
		HasOffpegFeeMultiplier:  p.hasOffpegFeeMultiplier,
	}
	if p.lpToken != "" {
		lp := common.HexToAddress(p.lpToken)
		pool.LpTokenAddress = &lp
	}
	return pool
}

// TestLiveCurveSnapshot runs the real snapshot path against mainnet for every
// seeded Curve pool. It is the only check that the curated capability columns
// match the deployed contracts: a pool curated as exposing a getter it does not
// have reverts, and a reverted snapshot read stops every block on the chain, so
// the failure mode this catches is a stalled indexer rather than a missing value.
//
//	ALCHEMY_API_KEY=… go test -tags=livevalidation -run TestLiveCurveSnapshot ./internal/services/curveindexer/
func TestLiveCurveSnapshot(t *testing.T) {
	ctx := context.Background()
	ec, err := ethclient.DialContext(ctx, alchemyURL(t))
	if err != nil {
		t.Fatalf("dialing mainnet: %v", err)
	}
	defer ec.Close()

	head, err := ec.HeaderByNumber(ctx, nil)
	if err != nil {
		t.Fatalf("fetching head: %v", err)
	}
	// Step back so every node agrees on the hash the multicall pins to.
	hdr, err := ec.HeaderByNumber(ctx, new(big.Int).Sub(head.Number, big.NewInt(5)))
	if err != nil {
		t.Fatalf("fetching header: %v", err)
	}

	mc, err := multicall.NewClient(ec, blockchain.Multicall3)
	if err != nil {
		t.Fatalf("creating multicall client: %v", err)
	}
	stableABI, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading stableswap ABI: %v", err)
	}
	h := NewStableswapHandler(stableABI)

	for i, p := range liveCurvePools {
		t.Run(p.name, func(t *testing.T) {
			st, cfg, err := h.SnapshotState(
				ctx, mc, p.registered(int64(i+1)), hdr.Number.Int64(), 0, hdr.Hash(), time.Now().UTC(),
			)
			if err != nil {
				t.Fatalf("SnapshotState: %v (a revert here means the curated capability columns disagree with the deployed contract)", err)
			}
			if st.VirtualPrice == nil || st.VirtualPrice.Sign() <= 0 {
				t.Errorf("virtual_price = %v, want a positive value", st.VirtualPrice)
			}
			if st.CalcTokenAmount == nil || st.CalcTokenAmount.Sign() <= 0 {
				t.Errorf("calc_token_amount = %v, want a positive value (the curated argument shape answered)", st.CalcTokenAmount)
			}
			assertLivePresence(t, "price_oracle", st.PriceOracle != nil, p.wantPriceOracle)
			assertLivePresence(t, "stored_rates", len(st.StoredRates) == len(p.coinDecimals), p.wantStoredRates)
			assertLivePresence(t, "future_fee", cfg.FutureFee != nil, p.hasFutureFee)
			assertLivePresence(t, "offpeg_fee_multiplier", cfg.OffpegFeeMultiplier != nil, p.hasOffpegFeeMultiplier)
		})
	}
}

func assertLivePresence(t *testing.T, field string, got, want bool) {
	t.Helper()
	if got != want {
		t.Errorf("%s present = %v, want %v", field, got, want)
	}
}
