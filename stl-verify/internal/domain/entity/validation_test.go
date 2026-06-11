package entity

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
		"MaplePoolState": func() (time.Time, error) {
			v := validMaplePoolState()
			s, err := NewMaplePoolState(v.MaplePoolID, in, v.TVL, v.LiquidAssets, v.CollateralValueUSD, v.PrincipalOut, v.MonthlyAPY, v.SpotAPY)
			if err != nil {
				return time.Time{}, err
			}
			return s.SyncedAt, nil
		},
		"MapleLoanState": func() (time.Time, error) {
			v := validMapleLoanState()
			s, err := NewMapleLoanState(v.MapleLoanID, in, v.State, v.PrincipalOwed, v.AcmRatio)
			if err != nil {
				return time.Time{}, err
			}
			return s.SyncedAt, nil
		},
		"MapleLoanCollateral": func() (time.Time, error) {
			v := validMapleLoanCollateral()
			c, err := NewMapleLoanCollateral(v.MapleLoanID, in, v.AssetSymbol, v.AssetAmount, v.AssetDecimals, v.AssetValueUSD, v.State, v.Custodian, v.LiquidationLevel)
			if err != nil {
				return time.Time{}, err
			}
			return c.SyncedAt, nil
		},
		"MapleSkyStrategyState": func() (time.Time, error) {
			v := validMapleSkyStrategyState()
			s, err := NewMapleSkyStrategyState(v.MapleSkyStrategyID, in, v.State, v.CurrentlyDeployed, v.DepositedAssets, v.WithdrawnAssets, v.StrategyFeeRate, v.TotalFeesCollected)
			if err != nil {
				return time.Time{}, err
			}
			return s.SyncedAt, nil
		},
		"MapleSyrupGlobalState": func() (time.Time, error) {
			v := validMapleSyrupGlobalState()
			s, err := NewMapleSyrupGlobalState(v.ChainID, in, v.TVL, v.APY, v.CollateralAPY, v.PoolAPY, v.DripsYieldBoost)
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
