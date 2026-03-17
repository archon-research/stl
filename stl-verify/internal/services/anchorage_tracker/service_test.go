package anchorage_tracker

import (
	"testing"
	"time"
)

func TestToSnapshots_FlattensCollateralAssets(t *testing.T) {
	packages := []Package{
		{
			PackageID:      "pkg-1",
			PledgorID:      "pledgor-1",
			SecuredPartyID: "sp-1",
			Active:         true,
			State:          "HEALTHY",
			CurrentLTV:     "0.68",
			ExposureValue:  "50000000",
			PackageValue:   "73000000",
			LTVTimestamp:   "2026-03-16T20:34:11Z",
			MarginCall:     MarginConfig{LTV: "0.8"},
			Critical:       MarginConfig{LTV: "0.9"},
			MarginReturn:   MarginReturnConfig{LTV: "0.6"},
			CollateralAssets: []CollateralAsset{
				{
					Asset:         AssetInfo{AssetType: "BTC", Type: "AnchorageCustody"},
					Price:         "74073.59",
					Quantity:      "988.33",
					WeightedValue: "73000000",
				},
			},
		},
	}

	now := time.Now().UTC()
	snapshots, err := toSnapshots(packages, 1, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}

	snap := snapshots[0]
	if snap.PrimeID != 1 {
		t.Errorf("expected prime_id=1, got %d", snap.PrimeID)
	}
	if snap.PackageID != "pkg-1" {
		t.Errorf("expected package_id=pkg-1, got %s", snap.PackageID)
	}
	if snap.AssetType != "BTC" {
		t.Errorf("expected asset_type=BTC, got %s", snap.AssetType)
	}
	if snap.CustodyType != "AnchorageCustody" {
		t.Errorf("expected custody_type=AnchorageCustody, got %s", snap.CustodyType)
	}
	if snap.MarginCallLTV != "0.8" {
		t.Errorf("expected margin_call_ltv=0.8, got %s", snap.MarginCallLTV)
	}
	if snap.CriticalLTV != "0.9" {
		t.Errorf("expected critical_ltv=0.9, got %s", snap.CriticalLTV)
	}
	if snap.MarginReturnLTV != "0.6" {
		t.Errorf("expected margin_return_ltv=0.6, got %s", snap.MarginReturnLTV)
	}
	if !snap.SnapshotTime.Equal(now) {
		t.Errorf("expected snapshot_time=%v, got %v", now, snap.SnapshotTime)
	}
}

func TestToSnapshots_MultipleAssetsPerPackage(t *testing.T) {
	packages := []Package{
		{
			PackageID:      "pkg-multi",
			PledgorID:      "p1",
			SecuredPartyID: "sp1",
			Active:         true,
			State:          "HEALTHY",
			CurrentLTV:     "0.5",
			ExposureValue:  "100000000",
			PackageValue:   "200000000",
			LTVTimestamp:   "2026-03-16T12:00:00Z",
			MarginCall:     MarginConfig{LTV: "0.8"},
			Critical:       MarginConfig{LTV: "0.9"},
			MarginReturn:   MarginReturnConfig{LTV: "0.6"},
			CollateralAssets: []CollateralAsset{
				{Asset: AssetInfo{AssetType: "BTC", Type: "AnchorageCustody"}, Price: "70000", Quantity: "1000", WeightedValue: "70000000"},
				{Asset: AssetInfo{AssetType: "ETH", Type: "AnchorageCustody"}, Price: "3000", Quantity: "43333", WeightedValue: "130000000"},
			},
		},
	}

	snapshots, err := toSnapshots(packages, 1, time.Now().UTC())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(snapshots) != 2 {
		t.Fatalf("expected 2 snapshots (one per asset), got %d", len(snapshots))
	}

	if snapshots[0].AssetType != "BTC" {
		t.Errorf("expected first asset=BTC, got %s", snapshots[0].AssetType)
	}
	if snapshots[1].AssetType != "ETH" {
		t.Errorf("expected second asset=ETH, got %s", snapshots[1].AssetType)
	}

	if snapshots[0].PackageID != snapshots[1].PackageID {
		t.Error("both snapshots should share the same package_id")
	}
}

func TestToSnapshots_EmptyCollateral(t *testing.T) {
	packages := []Package{
		{
			PackageID:        "pkg-empty",
			PledgorID:        "p1",
			SecuredPartyID:   "sp1",
			Active:           false,
			State:            "CLOSED",
			CurrentLTV:       "0",
			ExposureValue:    "0",
			PackageValue:     "0",
			LTVTimestamp:     "2026-01-01T00:00:00Z",
			MarginCall:       MarginConfig{LTV: "0.8"},
			Critical:         MarginConfig{LTV: "0.9"},
			MarginReturn:     MarginReturnConfig{LTV: "0.6"},
			CollateralAssets: []CollateralAsset{},
		},
	}

	snapshots, err := toSnapshots(packages, 1, time.Now().UTC())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot for empty collateral package, got %d", len(snapshots))
	}

	if snapshots[0].AssetType != "" {
		t.Errorf("expected empty asset_type, got %s", snapshots[0].AssetType)
	}
}

func TestToSnapshots_BadTimestamp(t *testing.T) {
	packages := []Package{
		{
			PackageID:    "pkg-bad",
			LTVTimestamp: "not-a-timestamp",
		},
	}

	_, err := toSnapshots(packages, 1, time.Now().UTC())
	if err == nil {
		t.Fatal("expected error for bad timestamp")
	}
}
