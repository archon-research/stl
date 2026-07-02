package entity

import (
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func TestNewCurveStableswapState_RejectsNilRequired(t *testing.T) {
	_, err := NewCurveStableswapState(CurveStableswapStateParams{
		CurvePoolID: 1, BlockNumber: 100, Timestamp: time.Unix(1, 0).UTC(),
		Balances: []*big.Int{big.NewInt(1)}, VirtualPrice: nil, // missing
		TotalSupply: big.NewInt(1), Amp: big.NewInt(900), Fee: big.NewInt(1),
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
		TotalSupply: big.NewInt(3), Amp: big.NewInt(900), Fee: big.NewInt(1000000),
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
		TotalSupply: big.NewInt(1), Amp: big.NewInt(900), Gamma: big.NewInt(500), Fee: big.NewInt(1),
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
		TotalSupply: big.NewInt(3), Amp: big.NewInt(900), Gamma: big.NewInt(500), Fee: big.NewInt(1000000),
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

// ---------------------------------------------------------------------------
// CurveStableswapState extended fields
// ---------------------------------------------------------------------------

func TestNewCurveStableswapState_NullableFieldsPassThrough(t *testing.T) {
	aPrecise := big.NewInt(9000)
	adminBals := []*big.Int{big.NewInt(10), big.NewInt(20)}
	storedRates := []*big.Int{big.NewInt(1e18), big.NewInt(1e18)}
	emaPrice := big.NewInt(999)
	getP := big.NewInt(888)
	calcTokenAmount := big.NewInt(777)
	calcWithdraw := []*big.Int{big.NewInt(100), big.NewInt(200)}

	st, err := NewCurveStableswapState(CurveStableswapStateParams{
		CurvePoolID: 2, BlockNumber: 200, BlockVersion: 0, Timestamp: time.Unix(2, 0).UTC(),
		Balances: []*big.Int{big.NewInt(1), big.NewInt(2)}, VirtualPrice: big.NewInt(1),
		TotalSupply: big.NewInt(3), Amp: big.NewInt(900), Fee: big.NewInt(1000000),
		SpotDy:              []*big.Int{big.NewInt(1)},
		APrecise:            aPrecise,
		AdminBalances:       adminBals,
		StoredRates:         storedRates,
		EmaPrice:            emaPrice,
		GetP:                getP,
		CalcTokenAmount:     calcTokenAmount,
		CalcWithdrawOneCoin: calcWithdraw,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.APrecise != aPrecise {
		t.Errorf("APrecise not set")
	}
	if len(st.AdminBalances) != 2 {
		t.Errorf("AdminBalances not set")
	}
	if len(st.StoredRates) != 2 {
		t.Errorf("StoredRates not set")
	}
	if st.EmaPrice != emaPrice {
		t.Errorf("EmaPrice not set")
	}
	if st.GetP != getP {
		t.Errorf("GetP not set")
	}
	if st.CalcTokenAmount != calcTokenAmount {
		t.Errorf("CalcTokenAmount not set")
	}
	if len(st.CalcWithdrawOneCoin) != 2 {
		t.Errorf("CalcWithdrawOneCoin not set")
	}
}

func TestNewCurveStableswapState_NullableFieldsNilByDefault(t *testing.T) {
	st, err := NewCurveStableswapState(CurveStableswapStateParams{
		CurvePoolID: 3, BlockNumber: 300, BlockVersion: 0, Timestamp: time.Unix(3, 0).UTC(),
		Balances: []*big.Int{big.NewInt(1)}, VirtualPrice: big.NewInt(1),
		TotalSupply: big.NewInt(1), Amp: big.NewInt(900), Fee: big.NewInt(1),
		SpotDy: []*big.Int{big.NewInt(1)},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.APrecise != nil || st.AdminBalances != nil || st.StoredRates != nil ||
		st.EmaPrice != nil || st.GetP != nil || st.CalcTokenAmount != nil || st.CalcWithdrawOneCoin != nil {
		t.Errorf("expected all nullable extended fields to be nil when not provided")
	}
}

// ---------------------------------------------------------------------------
// CurveCryptoswapState extended fields
// ---------------------------------------------------------------------------

func TestNewCurveCryptoswapState_NullableFieldsPassThrough(t *testing.T) {
	adminBals := []*big.Int{big.NewInt(10), big.NewInt(20), big.NewInt(30)}
	lpPrice := big.NewInt(1111)
	xcpProfitA := big.NewInt(2222)
	ts := int64(1700000000)
	getDx := []*big.Int{big.NewInt(50), big.NewInt(60)}
	calcTokenAmount := big.NewInt(333)
	calcWithdraw := []*big.Int{big.NewInt(44), big.NewInt(55)}

	st, err := NewCurveCryptoswapState(CurveCryptoswapStateParams{
		CurvePoolID: 4, BlockNumber: 400, BlockVersion: 0, Timestamp: time.Unix(4, 0).UTC(),
		Balances: []*big.Int{big.NewInt(1), big.NewInt(2)}, VirtualPrice: big.NewInt(1),
		TotalSupply: big.NewInt(1), Amp: big.NewInt(900), Gamma: big.NewInt(500), Fee: big.NewInt(1),
		PriceScale: []*big.Int{big.NewInt(1)}, PriceOracle: []*big.Int{big.NewInt(1)},
		LastPrices: []*big.Int{big.NewInt(1)}, SpotDy: []*big.Int{big.NewInt(1)},
		AdminBalances:       adminBals,
		LpPrice:             lpPrice,
		XcpProfitA:          xcpProfitA,
		LastPricesTimestamp: &ts,
		GetDx:               getDx,
		CalcTokenAmount:     calcTokenAmount,
		CalcWithdrawOneCoin: calcWithdraw,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(st.AdminBalances) != 3 {
		t.Errorf("AdminBalances not set")
	}
	if st.LpPrice != lpPrice {
		t.Errorf("LpPrice not set")
	}
	if st.XcpProfitA != xcpProfitA {
		t.Errorf("XcpProfitA not set")
	}
	if st.LastPricesTimestamp == nil || *st.LastPricesTimestamp != ts {
		t.Errorf("LastPricesTimestamp not set correctly")
	}
	if len(st.GetDx) != 2 {
		t.Errorf("GetDx not set")
	}
	if st.CalcTokenAmount != calcTokenAmount {
		t.Errorf("CalcTokenAmount not set")
	}
	if len(st.CalcWithdrawOneCoin) != 2 {
		t.Errorf("CalcWithdrawOneCoin not set")
	}
}

func TestNewCurveCryptoswapState_NullableFieldsNilByDefault(t *testing.T) {
	st, err := NewCurveCryptoswapState(CurveCryptoswapStateParams{
		CurvePoolID: 5, BlockNumber: 500, BlockVersion: 0, Timestamp: time.Unix(5, 0).UTC(),
		Balances: []*big.Int{big.NewInt(1)}, VirtualPrice: big.NewInt(1),
		TotalSupply: big.NewInt(1), Amp: big.NewInt(900), Gamma: big.NewInt(500), Fee: big.NewInt(1),
		PriceScale: []*big.Int{big.NewInt(1)}, PriceOracle: []*big.Int{big.NewInt(1)},
		LastPrices: []*big.Int{big.NewInt(1)}, SpotDy: []*big.Int{big.NewInt(1)},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.AdminBalances != nil || st.LpPrice != nil || st.XcpProfitA != nil ||
		st.LastPricesTimestamp != nil || st.GetDx != nil || st.CalcTokenAmount != nil || st.CalcWithdrawOneCoin != nil {
		t.Errorf("expected all nullable extended fields to be nil when not provided")
	}
}

// ---------------------------------------------------------------------------
// CurveStableswapConfig
// ---------------------------------------------------------------------------

func stableswapConfigValidParams() CurveStableswapConfigParams {
	return CurveStableswapConfigParams{
		CurvePoolID:    1,
		BlockNumber:    100,
		BlockVersion:   0,
		Timestamp:      time.Unix(1000, 0).UTC(),
		InitialA:       big.NewInt(200),
		InitialATime:   1700000000,
		FutureA:        big.NewInt(400),
		FutureATime:    1800000000,
		AdminFee:       big.NewInt(5000000000),
		FutureFee:      big.NewInt(4000000),
		FutureAdminFee: nil,
		MaExpTime:      nil,
		OracleMethod:   nil,
	}
}

func TestNewCurveStableswapConfig(t *testing.T) {
	maExpTime := int64(600)
	oracleMethod := big.NewInt(1)
	futureAdminFee := big.NewInt(5000000000)

	tests := []struct {
		name        string
		modify      func(*CurveStableswapConfigParams)
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid minimal (pre-NG, all nullable nil)",
			modify:  nil,
			wantErr: false,
		},
		{
			name: "valid with all nullable fields set (NG)",
			modify: func(p *CurveStableswapConfigParams) {
				p.FutureAdminFee = futureAdminFee
				p.MaExpTime = &maExpTime
				p.OracleMethod = oracleMethod
			},
			wantErr: false,
		},
		{
			name:        "nil InitialA rejected",
			modify:      func(p *CurveStableswapConfigParams) { p.InitialA = nil },
			wantErr:     true,
			errContains: "initial_a",
		},
		{
			name:        "nil FutureA rejected",
			modify:      func(p *CurveStableswapConfigParams) { p.FutureA = nil },
			wantErr:     true,
			errContains: "future_a",
		},
		{
			name:        "nil AdminFee rejected",
			modify:      func(p *CurveStableswapConfigParams) { p.AdminFee = nil },
			wantErr:     true,
			errContains: "admin_fee",
		},
		{
			name:        "nil FutureFee rejected",
			modify:      func(p *CurveStableswapConfigParams) { p.FutureFee = nil },
			wantErr:     true,
			errContains: "future_fee",
		},
		{
			name:        "zero Timestamp rejected",
			modify:      func(p *CurveStableswapConfigParams) { p.Timestamp = time.Time{} },
			wantErr:     true,
			errContains: "timestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := stableswapConfigValidParams()
			if tt.modify != nil {
				tt.modify(&p)
			}
			got, err := NewCurveStableswapConfig(p)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil {
				t.Fatal("expected non-nil result")
			}
			if got.CurvePoolID != p.CurvePoolID || got.BlockNumber != p.BlockNumber {
				t.Errorf("fields not copied: %+v", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CurveCryptoswapConfig
// ---------------------------------------------------------------------------

func cryptoswapConfigValidParams() CurveCryptoswapConfigParams {
	return CurveCryptoswapConfigParams{
		CurvePoolID:        1,
		BlockNumber:        100,
		BlockVersion:       0,
		Timestamp:          time.Unix(1000, 0).UTC(),
		InitialAGamma:      big.NewInt(2),
		FutureAGamma:       big.NewInt(4),
		InitialAGammaTime:  1700000000,
		FutureAGammaTime:   1800000000,
		MidFee:             big.NewInt(3000000),
		OutFee:             big.NewInt(45000000),
		FeeGamma:           big.NewInt(230000000000000),
		AllowedExtraProfit: big.NewInt(2000000000000),
		AdjustmentStep:     big.NewInt(490000000000000),
		MaTime:             big.NewInt(866),
		AdminFee:           big.NewInt(5000000000),
	}
}

func TestNewCurveCryptoswapConfig(t *testing.T) {
	tests := []struct {
		name        string
		modify      func(*CurveCryptoswapConfigParams)
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid",
			modify:  nil,
			wantErr: false,
		},
		{
			name:        "nil InitialAGamma rejected",
			modify:      func(p *CurveCryptoswapConfigParams) { p.InitialAGamma = nil },
			wantErr:     true,
			errContains: "initial_a_gamma",
		},
		{
			name:        "nil FutureAGamma rejected",
			modify:      func(p *CurveCryptoswapConfigParams) { p.FutureAGamma = nil },
			wantErr:     true,
			errContains: "future_a_gamma",
		},
		{
			name:        "nil MidFee rejected",
			modify:      func(p *CurveCryptoswapConfigParams) { p.MidFee = nil },
			wantErr:     true,
			errContains: "mid_fee",
		},
		{
			name:        "nil OutFee rejected",
			modify:      func(p *CurveCryptoswapConfigParams) { p.OutFee = nil },
			wantErr:     true,
			errContains: "out_fee",
		},
		{
			name:        "nil FeeGamma rejected",
			modify:      func(p *CurveCryptoswapConfigParams) { p.FeeGamma = nil },
			wantErr:     true,
			errContains: "fee_gamma",
		},
		{
			name:        "nil AllowedExtraProfit rejected",
			modify:      func(p *CurveCryptoswapConfigParams) { p.AllowedExtraProfit = nil },
			wantErr:     true,
			errContains: "allowed_extra_profit",
		},
		{
			name:        "nil AdjustmentStep rejected",
			modify:      func(p *CurveCryptoswapConfigParams) { p.AdjustmentStep = nil },
			wantErr:     true,
			errContains: "adjustment_step",
		},
		{
			name:        "nil MaTime rejected",
			modify:      func(p *CurveCryptoswapConfigParams) { p.MaTime = nil },
			wantErr:     true,
			errContains: "ma_time",
		},
		{
			name:        "nil AdminFee rejected",
			modify:      func(p *CurveCryptoswapConfigParams) { p.AdminFee = nil },
			wantErr:     true,
			errContains: "admin_fee",
		},
		{
			name:        "zero Timestamp rejected",
			modify:      func(p *CurveCryptoswapConfigParams) { p.Timestamp = time.Time{} },
			wantErr:     true,
			errContains: "timestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := cryptoswapConfigValidParams()
			if tt.modify != nil {
				tt.modify(&p)
			}
			got, err := NewCurveCryptoswapConfig(p)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil {
				t.Fatal("expected non-nil result")
			}
			if got.CurvePoolID != p.CurvePoolID || got.BlockNumber != p.BlockNumber {
				t.Errorf("fields not copied: %+v", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CurveParameterEvent
// ---------------------------------------------------------------------------

func paramEventValidParams() CurveParameterEventParams {
	return CurveParameterEventParams{
		CurvePoolID:  1,
		BlockNumber:  100,
		BlockVersion: 0,
		Timestamp:    time.Unix(1000, 0).UTC(),
		TxHash:       common.HexToHash("0xabc"),
		LogIndex:     0,
		EventName:    "ramp_a",
		Params:       json.RawMessage(`{"old_a":200,"new_a":400}`),
	}
}

func TestNewCurveParameterEvent(t *testing.T) {
	validEventNames := []string{
		"ramp_a", "stop_ramp_a", "ramp_a_gamma",
		"new_fee", "commit_new_fee", "apply_new_fee",
		"new_parameters", "commit_new_parameters",
		"claim_admin_fee", "new_admin", "commit_new_admin",
	}

	tests := []struct {
		name        string
		modify      func(*CurveParameterEventParams)
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid",
			modify:  nil,
			wantErr: false,
		},
		{
			name:        "zero Timestamp rejected",
			modify:      func(p *CurveParameterEventParams) { p.Timestamp = time.Time{} },
			wantErr:     true,
			errContains: "timestamp",
		},
		{
			name:        "empty EventName rejected",
			modify:      func(p *CurveParameterEventParams) { p.EventName = "" },
			wantErr:     true,
			errContains: "event_name",
		},
		{
			name:        "unknown EventName rejected",
			modify:      func(p *CurveParameterEventParams) { p.EventName = "unknown_event" },
			wantErr:     true,
			errContains: "event_name",
		},
		{
			name:        "nil Params rejected",
			modify:      func(p *CurveParameterEventParams) { p.Params = nil },
			wantErr:     true,
			errContains: "params",
		},
		{
			name:        "empty Params rejected",
			modify:      func(p *CurveParameterEventParams) { p.Params = json.RawMessage{} },
			wantErr:     true,
			errContains: "params",
		},
		{
			name:        "invalid JSON Params rejected",
			modify:      func(p *CurveParameterEventParams) { p.Params = json.RawMessage(`not json`) },
			wantErr:     true,
			errContains: "params",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := paramEventValidParams()
			if tt.modify != nil {
				tt.modify(&p)
			}
			got, err := NewCurveParameterEvent(p)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil {
				t.Fatal("expected non-nil result")
			}
			if got.CurvePoolID != p.CurvePoolID || got.EventName != p.EventName {
				t.Errorf("fields not copied: %+v", got)
			}
		})
	}

	// Verify every allowed event_name is accepted.
	for _, name := range validEventNames {
		t.Run("valid event_name "+name, func(t *testing.T) {
			p := paramEventValidParams()
			p.EventName = name
			_, err := NewCurveParameterEvent(p)
			if err != nil {
				t.Errorf("valid event_name %q rejected: %v", name, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CurveLpTokenEvent
// ---------------------------------------------------------------------------

func lpTokenEventValidParams() CurveLpTokenEventParams {
	return CurveLpTokenEventParams{
		CurvePoolID:  1,
		BlockNumber:  100,
		BlockVersion: 0,
		Timestamp:    time.Unix(1000, 0).UTC(),
		TxHash:       common.HexToHash("0xdef"),
		LogIndex:     0,
		EventName:    "transfer",
		From:         common.HexToAddress("0x1111111111111111111111111111111111111111"),
		To:           common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Value:        big.NewInt(1e18),
	}
}

func TestNewCurveLpTokenEvent(t *testing.T) {
	tests := []struct {
		name        string
		modify      func(*CurveLpTokenEventParams)
		wantErr     bool
		errContains string
	}{
		{
			name:    "valid transfer",
			modify:  nil,
			wantErr: false,
		},
		{
			name:    "valid approval",
			modify:  func(p *CurveLpTokenEventParams) { p.EventName = "approval" },
			wantErr: false,
		},
		{
			name:        "zero Timestamp rejected",
			modify:      func(p *CurveLpTokenEventParams) { p.Timestamp = time.Time{} },
			wantErr:     true,
			errContains: "timestamp",
		},
		{
			name:        "empty EventName rejected",
			modify:      func(p *CurveLpTokenEventParams) { p.EventName = "" },
			wantErr:     true,
			errContains: "event_name",
		},
		{
			name:        "unknown EventName rejected",
			modify:      func(p *CurveLpTokenEventParams) { p.EventName = "mint" },
			wantErr:     true,
			errContains: "event_name",
		},
		{
			name:        "nil Value rejected",
			modify:      func(p *CurveLpTokenEventParams) { p.Value = nil },
			wantErr:     true,
			errContains: "value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := lpTokenEventValidParams()
			if tt.modify != nil {
				tt.modify(&p)
			}
			got, err := NewCurveLpTokenEvent(p)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil {
				t.Fatal("expected non-nil result")
			}
			if got.CurvePoolID != p.CurvePoolID || got.EventName != p.EventName {
				t.Errorf("fields not copied: %+v", got)
			}
			if got.From != p.From || got.To != p.To {
				t.Errorf("addresses not copied: %+v", got)
			}
			if got.Value.Cmp(p.Value) != 0 {
				t.Errorf("value not copied")
			}
		})
	}
}
