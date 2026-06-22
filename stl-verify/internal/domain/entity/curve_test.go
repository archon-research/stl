package entity

import (
	"math/big"
	"testing"
	"time"
)

func TestNewCurveStableswapState_RejectsNilRequired(t *testing.T) {
	_, err := NewCurveStableswapState(CurveStableswapStateParams{
		CurvePoolID: 1, BlockNumber: 100, Timestamp: time.Unix(1, 0).UTC(),
		Balances: []*big.Int{big.NewInt(1)}, VirtualPrice: nil, // missing
		TotalSupply: big.NewInt(1), A: big.NewInt(900), Fee: big.NewInt(1),
		SpotDy: []*big.Int{big.NewInt(1)},
	})
	if err == nil {
		t.Fatal("expected error for nil VirtualPrice")
	}
}

func TestNewCurveStableswapState_Valid(t *testing.T) {
	st, err := NewCurveStableswapState(CurveStableswapStateParams{
		CurvePoolID: 1, BlockNumber: 100, BlockVersion: 0, Timestamp: time.Unix(1, 0).UTC(),
		Balances: []*big.Int{big.NewInt(1), big.NewInt(2)}, VirtualPrice: big.NewInt(1),
		TotalSupply: big.NewInt(3), A: big.NewInt(900), Fee: big.NewInt(1000000),
		SpotDy: []*big.Int{big.NewInt(1), big.NewInt(1)},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.CurvePoolID != 1 || len(st.Balances) != 2 {
		t.Fatalf("unexpected state: %+v", st)
	}
}

func TestNewCurveCryptoswapState_RejectsNilRequired(t *testing.T) {
	_, err := NewCurveCryptoswapState(CurveCryptoswapStateParams{
		CurvePoolID: 1, BlockNumber: 100, Timestamp: time.Unix(1, 0).UTC(),
		Balances: []*big.Int{big.NewInt(1)}, VirtualPrice: nil, // missing
		TotalSupply: big.NewInt(1), A: big.NewInt(900), Gamma: big.NewInt(500), Fee: big.NewInt(1),
		PriceScale: []*big.Int{big.NewInt(1)}, PriceOracle: []*big.Int{big.NewInt(1)},
		LastPrices: []*big.Int{big.NewInt(1)}, SpotDy: []*big.Int{big.NewInt(1)},
	})
	if err == nil {
		t.Fatal("expected error for nil VirtualPrice")
	}
}

func TestNewCurveCryptoswapState_Valid(t *testing.T) {
	st, err := NewCurveCryptoswapState(CurveCryptoswapStateParams{
		CurvePoolID: 1, BlockNumber: 100, BlockVersion: 0, Timestamp: time.Unix(1, 0).UTC(),
		Balances: []*big.Int{big.NewInt(1), big.NewInt(2)}, VirtualPrice: big.NewInt(1),
		TotalSupply: big.NewInt(3), A: big.NewInt(900), Gamma: big.NewInt(500), Fee: big.NewInt(1000000),
		PriceScale: []*big.Int{big.NewInt(1), big.NewInt(1)}, PriceOracle: []*big.Int{big.NewInt(1), big.NewInt(1)},
		LastPrices: []*big.Int{big.NewInt(1), big.NewInt(1)}, SpotDy: []*big.Int{big.NewInt(1), big.NewInt(1)},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.CurvePoolID != 1 || len(st.Balances) != 2 {
		t.Fatalf("unexpected state: %+v", st)
	}
}
