package entity

import (
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
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
	// Extended nullable fields.
	APrecise            *big.Int
	AdminBalances       []*big.Int
	StoredRates         []*big.Int // NG only; nil for pre-NG
	EmaPrice            *big.Int   // NG only
	GetP                *big.Int   // NG only
	CalcTokenAmount     *big.Int
	CalcWithdrawOneCoin []*big.Int
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
	// Extended nullable fields.
	APrecise            *big.Int
	AdminBalances       []*big.Int
	StoredRates         []*big.Int
	EmaPrice            *big.Int
	GetP                *big.Int
	CalcTokenAmount     *big.Int
	CalcWithdrawOneCoin []*big.Int
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
		APrecise: p.APrecise, AdminBalances: p.AdminBalances, StoredRates: p.StoredRates,
		EmaPrice: p.EmaPrice, GetP: p.GetP, CalcTokenAmount: p.CalcTokenAmount,
		CalcWithdrawOneCoin: p.CalcWithdrawOneCoin,
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
	// Extended nullable fields.
	AdminBalances       []*big.Int
	LpPrice             *big.Int
	XcpProfitA          *big.Int
	LastPricesTimestamp *int64
	GetDx               []*big.Int
	CalcTokenAmount     *big.Int
	CalcWithdrawOneCoin []*big.Int
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
	// Extended nullable fields.
	AdminBalances       []*big.Int
	LpPrice             *big.Int
	XcpProfitA          *big.Int
	LastPricesTimestamp *int64
	GetDx               []*big.Int
	CalcTokenAmount     *big.Int
	CalcWithdrawOneCoin []*big.Int
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
		AdminBalances: p.AdminBalances, LpPrice: p.LpPrice, XcpProfitA: p.XcpProfitA,
		LastPricesTimestamp: p.LastPricesTimestamp, GetDx: p.GetDx,
		CalcTokenAmount: p.CalcTokenAmount, CalcWithdrawOneCoin: p.CalcWithdrawOneCoin,
	}, nil
}

type CurveStableswapConfigParams struct {
	CurvePoolID    int64
	BlockNumber    int64
	BlockVersion   int
	Timestamp      time.Time
	InitialA       *big.Int
	InitialATime   int64
	FutureA        *big.Int
	FutureATime    int64
	AdminFee       *big.Int
	FutureFee      *big.Int
	FutureAdminFee *big.Int // nullable: pre-NG only
	MaExpTime      *int64   // nullable: NG only
	OracleMethod   *big.Int // nullable: NG only
}

type CurveStableswapConfig struct {
	CurvePoolID    int64
	BlockNumber    int64
	BlockVersion   int
	Timestamp      time.Time
	InitialA       *big.Int
	InitialATime   int64
	FutureA        *big.Int
	FutureATime    int64
	AdminFee       *big.Int
	FutureFee      *big.Int
	FutureAdminFee *big.Int
	MaExpTime      *int64
	OracleMethod   *big.Int
}

func NewCurveStableswapConfig(p CurveStableswapConfigParams) (*CurveStableswapConfig, error) {
	if p.Timestamp.IsZero() {
		return nil, fmt.Errorf("curve stableswap config: timestamp must not be zero")
	}
	for name, v := range map[string]*big.Int{
		"initial_a": p.InitialA, "future_a": p.FutureA,
		"admin_fee": p.AdminFee, "future_fee": p.FutureFee,
	} {
		if v == nil {
			return nil, fmt.Errorf("curve stableswap config: %s must not be nil", name)
		}
	}
	return &CurveStableswapConfig{
		CurvePoolID:    p.CurvePoolID,
		BlockNumber:    p.BlockNumber,
		BlockVersion:   p.BlockVersion,
		Timestamp:      p.Timestamp,
		InitialA:       p.InitialA,
		InitialATime:   p.InitialATime,
		FutureA:        p.FutureA,
		FutureATime:    p.FutureATime,
		AdminFee:       p.AdminFee,
		FutureFee:      p.FutureFee,
		FutureAdminFee: p.FutureAdminFee,
		MaExpTime:      p.MaExpTime,
		OracleMethod:   p.OracleMethod,
	}, nil
}

type CurveCryptoswapConfigParams struct {
	CurvePoolID        int64
	BlockNumber        int64
	BlockVersion       int
	Timestamp          time.Time
	InitialAGamma      *big.Int
	FutureAGamma       *big.Int
	InitialAGammaTime  int64
	FutureAGammaTime   int64
	MidFee             *big.Int
	OutFee             *big.Int
	FeeGamma           *big.Int
	AllowedExtraProfit *big.Int
	AdjustmentStep     *big.Int
	MaTime             *big.Int
	AdminFee           *big.Int
}

type CurveCryptoswapConfig struct {
	CurvePoolID        int64
	BlockNumber        int64
	BlockVersion       int
	Timestamp          time.Time
	InitialAGamma      *big.Int
	FutureAGamma       *big.Int
	InitialAGammaTime  int64
	FutureAGammaTime   int64
	MidFee             *big.Int
	OutFee             *big.Int
	FeeGamma           *big.Int
	AllowedExtraProfit *big.Int
	AdjustmentStep     *big.Int
	MaTime             *big.Int
	AdminFee           *big.Int
}

func NewCurveCryptoswapConfig(p CurveCryptoswapConfigParams) (*CurveCryptoswapConfig, error) {
	if p.Timestamp.IsZero() {
		return nil, fmt.Errorf("curve cryptoswap config: timestamp must not be zero")
	}
	for name, v := range map[string]*big.Int{
		"initial_a_gamma": p.InitialAGamma, "future_a_gamma": p.FutureAGamma,
		"mid_fee": p.MidFee, "out_fee": p.OutFee, "fee_gamma": p.FeeGamma,
		"allowed_extra_profit": p.AllowedExtraProfit, "adjustment_step": p.AdjustmentStep,
		"ma_time": p.MaTime, "admin_fee": p.AdminFee,
	} {
		if v == nil {
			return nil, fmt.Errorf("curve cryptoswap config: %s must not be nil", name)
		}
	}
	return &CurveCryptoswapConfig{
		CurvePoolID:        p.CurvePoolID,
		BlockNumber:        p.BlockNumber,
		BlockVersion:       p.BlockVersion,
		Timestamp:          p.Timestamp,
		InitialAGamma:      p.InitialAGamma,
		FutureAGamma:       p.FutureAGamma,
		InitialAGammaTime:  p.InitialAGammaTime,
		FutureAGammaTime:   p.FutureAGammaTime,
		MidFee:             p.MidFee,
		OutFee:             p.OutFee,
		FeeGamma:           p.FeeGamma,
		AllowedExtraProfit: p.AllowedExtraProfit,
		AdjustmentStep:     p.AdjustmentStep,
		MaTime:             p.MaTime,
		AdminFee:           p.AdminFee,
	}, nil
}

var curveParameterEventNames = map[string]struct{}{
	"ramp_a": {}, "stop_ramp_a": {}, "ramp_a_gamma": {},
	"new_fee": {}, "commit_new_fee": {}, "apply_new_fee": {},
	"new_parameters": {}, "commit_new_parameters": {},
	"claim_admin_fee": {}, "new_admin": {}, "commit_new_admin": {},
}

type CurveParameterEventParams struct {
	CurvePoolID  int64
	BlockNumber  int64
	BlockVersion int
	Timestamp    time.Time
	TxHash       common.Hash
	LogIndex     int
	EventName    string
	Params       json.RawMessage
}

type CurveParameterEvent struct {
	CurvePoolID  int64
	BlockNumber  int64
	BlockVersion int
	Timestamp    time.Time
	TxHash       common.Hash
	LogIndex     int
	EventName    string
	Params       json.RawMessage
}

func NewCurveParameterEvent(p CurveParameterEventParams) (*CurveParameterEvent, error) {
	if p.Timestamp.IsZero() {
		return nil, fmt.Errorf("curve parameter event: timestamp must not be zero")
	}
	if _, ok := curveParameterEventNames[p.EventName]; !ok {
		return nil, fmt.Errorf("curve parameter event: event_name %q is not allowed", p.EventName)
	}
	if len(p.Params) == 0 {
		return nil, fmt.Errorf("curve parameter event: params must not be empty")
	}
	if !json.Valid(p.Params) {
		return nil, fmt.Errorf("curve parameter event: params must be valid JSON")
	}
	return &CurveParameterEvent{
		CurvePoolID:  p.CurvePoolID,
		BlockNumber:  p.BlockNumber,
		BlockVersion: p.BlockVersion,
		Timestamp:    p.Timestamp,
		TxHash:       p.TxHash,
		LogIndex:     p.LogIndex,
		EventName:    p.EventName,
		Params:       p.Params,
	}, nil
}

var curveLpTokenEventNames = map[string]struct{}{
	"transfer": {}, "approval": {},
}

type CurveLpTokenEventParams struct {
	CurvePoolID  int64
	BlockNumber  int64
	BlockVersion int
	Timestamp    time.Time
	TxHash       common.Hash
	LogIndex     int
	EventName    string
	From         common.Address
	To           common.Address
	Value        *big.Int
}

type CurveLpTokenEvent struct {
	CurvePoolID  int64
	BlockNumber  int64
	BlockVersion int
	Timestamp    time.Time
	TxHash       common.Hash
	LogIndex     int
	EventName    string
	From         common.Address
	To           common.Address
	Value        *big.Int
}

func NewCurveLpTokenEvent(p CurveLpTokenEventParams) (*CurveLpTokenEvent, error) {
	if p.Timestamp.IsZero() {
		return nil, fmt.Errorf("curve lp token event: timestamp must not be zero")
	}
	if _, ok := curveLpTokenEventNames[p.EventName]; !ok {
		return nil, fmt.Errorf("curve lp token event: event_name %q is not allowed", p.EventName)
	}
	if p.Value == nil {
		return nil, fmt.Errorf("curve lp token event: value must not be nil")
	}
	return &CurveLpTokenEvent{
		CurvePoolID:  p.CurvePoolID,
		BlockNumber:  p.BlockNumber,
		BlockVersion: p.BlockVersion,
		Timestamp:    p.Timestamp,
		TxHash:       p.TxHash,
		LogIndex:     p.LogIndex,
		EventName:    p.EventName,
		From:         p.From,
		To:           p.To,
		Value:        p.Value,
	}, nil
}
