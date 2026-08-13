package maple

import (
	"testing"
	"time"
)

// TestStateConstructors_NormalizeSyncedAt pins the snapshot timestamp
// convention (UTC, second precision) at every construction site: exact
// synced_at equality is the dedup key in the maple_* processing-version
// triggers and part of every state primary key, so a non-UTC or sub-second
// timestamp must never reach the database.
func TestStateConstructors_NormalizeSyncedAt(t *testing.T) {
	in := time.Date(2026, 6, 10, 12, 30, 45, 999999999, time.FixedZone("UTC+2", 2*3600))
	want := in.UTC().Truncate(time.Second)

	constructors := map[string]func() (time.Time, error){
		"PoolState": func() (time.Time, error) {
			v := validPoolState()
			s, err := NewPoolState(PoolStateParams{
				PoolID:             v.PoolID,
				SyncedAt:           in,
				TVL:                v.TVL,
				LiquidAssets:       v.LiquidAssets,
				CollateralValueUSD: v.CollateralValueUSD,
				PrincipalOut:       v.PrincipalOut,
				MonthlyAPY:         v.MonthlyAPY,
				SpotAPY:            v.SpotAPY,
			})
			if err != nil {
				return time.Time{}, err
			}
			return s.SyncedAt, nil
		},
		"LoanState": func() (time.Time, error) {
			v := validLoanState()
			s, err := NewLoanState(v.LoanID, in, v.State, v.PrincipalOwed, v.AcmRatio)
			if err != nil {
				return time.Time{}, err
			}
			return s.SyncedAt, nil
		},
		"LoanCollateral": func() (time.Time, error) {
			v := validLoanCollateral()
			c, err := NewLoanCollateral(LoanCollateralParams{
				LoanID:           v.LoanID,
				SyncedAt:         in,
				AssetSymbol:      v.AssetSymbol,
				AssetAmount:      v.AssetAmount,
				AssetDecimals:    v.AssetDecimals,
				AssetValueUSD:    v.AssetValueUSD,
				State:            v.State,
				Custodian:        v.Custodian,
				LiquidationLevel: v.LiquidationLevel,
			})
			if err != nil {
				return time.Time{}, err
			}
			return c.SyncedAt, nil
		},
		"SkyStrategyState": func() (time.Time, error) {
			v := validSkyStrategyState()
			s, err := NewSkyStrategyState(SkyStrategyStateParams{
				SkyStrategyID:      v.SkyStrategyID,
				SyncedAt:           in,
				State:              v.State,
				CurrentlyDeployed:  v.CurrentlyDeployed,
				DepositedAssets:    v.DepositedAssets,
				WithdrawnAssets:    v.WithdrawnAssets,
				StrategyFeeRate:    v.StrategyFeeRate,
				TotalFeesCollected: v.TotalFeesCollected,
			})
			if err != nil {
				return time.Time{}, err
			}
			return s.SyncedAt, nil
		},
		"SyrupGlobalState": func() (time.Time, error) {
			v := validSyrupGlobalState()
			s, err := NewSyrupGlobalState(v.ChainID, in, v.TVL, v.APY, v.CollateralAPY, v.PoolAPY, v.DripsYieldBoost)
			if err != nil {
				return time.Time{}, err
			}
			return s.SyncedAt, nil
		},
	}

	for name, construct := range constructors {
		t.Run(name, func(t *testing.T) {
			got, err := construct()
			if err != nil {
				t.Fatalf("constructor: %v", err)
			}
			if !got.Equal(want) {
				t.Errorf("SyncedAt = %v, want %v", got, want)
			}
			if got.Location() != time.UTC {
				t.Errorf("SyncedAt location = %v, want UTC", got.Location())
			}
			if got.Nanosecond() != 0 {
				t.Errorf("SyncedAt nanoseconds = %d, want 0", got.Nanosecond())
			}
		})
	}
}
