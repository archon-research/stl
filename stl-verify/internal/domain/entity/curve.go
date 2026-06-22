package entity

import (
	"fmt"
	"math/big"
	"time"
)

type CurveStableswapStateParams struct {
	CurvePoolID  int64
	BlockNumber  int64
	BlockVersion int
	Timestamp    time.Time
	Balances     []*big.Int
	VirtualPrice *big.Int
	TotalSupply  *big.Int
	A            *big.Int
	Fee          *big.Int
	SpotDy       []*big.Int
	LastPrice    *big.Int // NG only; nil for pre-NG
	PriceOracle  *big.Int // NG only; nil for pre-NG
}

type CurveStableswapState struct {
	CurvePoolID  int64
	BlockNumber  int64
	BlockVersion int
	Timestamp    time.Time
	Balances     []*big.Int
	VirtualPrice *big.Int
	TotalSupply  *big.Int
	A            *big.Int
	Fee          *big.Int
	SpotDy       []*big.Int
	LastPrice    *big.Int
	PriceOracle  *big.Int
}

func NewCurveStableswapState(p CurveStableswapStateParams) (*CurveStableswapState, error) {
	for name, v := range map[string]*big.Int{
		"virtual_price": p.VirtualPrice, "total_supply": p.TotalSupply, "a": p.A, "fee": p.Fee,
	} {
		if v == nil {
			return nil, fmt.Errorf("curve stableswap state: %s must not be nil", name)
		}
	}
	if len(p.Balances) == 0 {
		return nil, fmt.Errorf("curve stableswap state: balances must not be empty")
	}
	return &CurveStableswapState{
		CurvePoolID: p.CurvePoolID, BlockNumber: p.BlockNumber, BlockVersion: p.BlockVersion,
		Timestamp: p.Timestamp, Balances: p.Balances, VirtualPrice: p.VirtualPrice,
		TotalSupply: p.TotalSupply, A: p.A, Fee: p.Fee, SpotDy: p.SpotDy,
		LastPrice: p.LastPrice, PriceOracle: p.PriceOracle,
	}, nil
}

type CurveCryptoswapStateParams struct {
	CurvePoolID  int64
	BlockNumber  int64
	BlockVersion int
	Timestamp    time.Time
	Balances     []*big.Int
	VirtualPrice *big.Int
	TotalSupply  *big.Int
	A            *big.Int
	Gamma        *big.Int
	Fee          *big.Int
	D            *big.Int // nullable
	XcpProfit    *big.Int // nullable
	PriceScale   []*big.Int
	PriceOracle  []*big.Int
	LastPrices   []*big.Int
	SpotDy       []*big.Int
}

type CurveCryptoswapState struct {
	CurvePoolID  int64
	BlockNumber  int64
	BlockVersion int
	Timestamp    time.Time
	Balances     []*big.Int
	VirtualPrice *big.Int
	TotalSupply  *big.Int
	A            *big.Int
	Gamma        *big.Int
	Fee          *big.Int
	D            *big.Int
	XcpProfit    *big.Int
	PriceScale   []*big.Int
	PriceOracle  []*big.Int
	LastPrices   []*big.Int
	SpotDy       []*big.Int
}

func NewCurveCryptoswapState(p CurveCryptoswapStateParams) (*CurveCryptoswapState, error) {
	for name, v := range map[string]*big.Int{
		"virtual_price": p.VirtualPrice, "total_supply": p.TotalSupply, "a": p.A, "gamma": p.Gamma, "fee": p.Fee,
	} {
		if v == nil {
			return nil, fmt.Errorf("curve cryptoswap state: %s must not be nil", name)
		}
	}
	if len(p.Balances) == 0 {
		return nil, fmt.Errorf("curve cryptoswap state: balances must not be empty")
	}
	return &CurveCryptoswapState{
		CurvePoolID: p.CurvePoolID, BlockNumber: p.BlockNumber, BlockVersion: p.BlockVersion,
		Timestamp: p.Timestamp, Balances: p.Balances, VirtualPrice: p.VirtualPrice,
		TotalSupply: p.TotalSupply, A: p.A, Gamma: p.Gamma, Fee: p.Fee, D: p.D,
		XcpProfit: p.XcpProfit, PriceScale: p.PriceScale, PriceOracle: p.PriceOracle,
		LastPrices: p.LastPrices, SpotDy: p.SpotDy,
	}, nil
}
