package main

import (
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/services/curveindexer"
)

func curatedCurvePool(kind curveindexer.PoolKind) curveindexer.RegisteredPool {
	dyn := false
	return curveindexer.RegisteredPool{
		Address:                 common.HexToAddress("0x21E27a5E5513D6e65C4f830167390997aA84843a"),
		Kind:                    kind,
		NCoins:                  2,
		CalcTokenAmountDynArray: &dyn,
		HasFutureFee:            true,
	}
}

type curationCase struct {
	name        string
	pool        func() curveindexer.RegisteredPool
	wantErr     string
	wantNoError bool
}

func curationCases() []curationCase {
	return []curationCase{
		{
			name:        "fully curated stableswap pool passes",
			pool:        func() curveindexer.RegisteredPool { return curatedCurvePool(curveindexer.KindStableswapNG) },
			wantNoError: true,
		},
		{
			name: "cryptoswap pool needs no fee flag",
			pool: func() curveindexer.RegisteredPool {
				p := curatedCurvePool(curveindexer.KindCryptoswap)
				p.HasFutureFee = false
				return p
			},
			wantNoError: true,
		},
		{
			name: "offpeg alone satisfies the fee schedule",
			pool: func() curveindexer.RegisteredPool {
				p := curatedCurvePool(curveindexer.KindStableswapNG)
				p.HasFutureFee = false
				p.HasOffpegFeeMultiplier = true
				return p
			},
			wantNoError: true,
		},
		{
			name: "unprobed calc_token_amount is rejected",
			pool: func() curveindexer.RegisteredPool {
				p := curatedCurvePool(curveindexer.KindStableswapNG)
				p.CalcTokenAmountDynArray = nil
				return p
			},
			wantErr: "calc_token_amount_dyn_array",
		},
		{
			name: "stableswap pool with neither fee flag is rejected",
			pool: func() curveindexer.RegisteredPool {
				p := curatedCurvePool(curveindexer.KindStableswapPreNG)
				p.HasFutureFee = false
				return p
			},
			wantErr: "has_future_fee/has_offpeg_fee_multiplier",
		},
	}
}

func TestValidateCurveCuration(t *testing.T) {
	for _, tc := range curationCases() {
		t.Run(tc.name, func(t *testing.T) {
			err := validateCurveCuration([]curveindexer.RegisteredPool{tc.pool()})
			if tc.wantNoError {
				if err != nil {
					t.Fatalf("validateCurveCuration: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error %q does not name %q", err.Error(), tc.wantErr)
			}
		})
	}
}
