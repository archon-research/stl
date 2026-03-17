package anchorage_tracker

import (
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
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
					Weight:        "1",
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

	expectedLTVTimestamp, _ := time.Parse(time.RFC3339, "2026-03-16T20:34:11Z")

	want := entity.AnchoragePackageSnapshot{
		PrimeID:            1,
		PackageID:          "pkg-1",
		PledgorID:          "pledgor-1",
		SecuredPartyID:     "sp-1",
		Active:             true,
		State:              "HEALTHY",
		CurrentLTV:         "0.68",
		ExposureValue:      "50000000",
		PackageValue:       "73000000",
		MarginCallLTV:      "0.8",
		CriticalLTV:        "0.9",
		MarginReturnLTV:    "0.6",
		AssetType:          "BTC",
		CustodyType:        "AnchorageCustody",
		AssetPrice:         "74073.59",
		AssetQuantity:      "988.33",
		AssetWeightedValue: "73000000",
		LTVTimestamp:       expectedLTVTimestamp,
		SnapshotTime:       now,
	}

	got := snapshots[0]

	assertField(t, "PrimeID", got.PrimeID, want.PrimeID)
	assertField(t, "PackageID", got.PackageID, want.PackageID)
	assertField(t, "PledgorID", got.PledgorID, want.PledgorID)
	assertField(t, "SecuredPartyID", got.SecuredPartyID, want.SecuredPartyID)
	assertField(t, "Active", got.Active, want.Active)
	assertField(t, "State", got.State, want.State)
	assertField(t, "CurrentLTV", got.CurrentLTV, want.CurrentLTV)
	assertField(t, "ExposureValue", got.ExposureValue, want.ExposureValue)
	assertField(t, "PackageValue", got.PackageValue, want.PackageValue)
	assertField(t, "MarginCallLTV", got.MarginCallLTV, want.MarginCallLTV)
	assertField(t, "CriticalLTV", got.CriticalLTV, want.CriticalLTV)
	assertField(t, "MarginReturnLTV", got.MarginReturnLTV, want.MarginReturnLTV)
	assertField(t, "AssetType", got.AssetType, want.AssetType)
	assertField(t, "CustodyType", got.CustodyType, want.CustodyType)
	assertField(t, "AssetPrice", got.AssetPrice, want.AssetPrice)
	assertField(t, "AssetQuantity", got.AssetQuantity, want.AssetQuantity)
	assertField(t, "AssetWeightedValue", got.AssetWeightedValue, want.AssetWeightedValue)

	if !got.LTVTimestamp.Equal(want.LTVTimestamp) {
		t.Errorf("LTVTimestamp: got %v, want %v", got.LTVTimestamp, want.LTVTimestamp)
	}
	if !got.SnapshotTime.Equal(want.SnapshotTime) {
		t.Errorf("SnapshotTime: got %v, want %v", got.SnapshotTime, want.SnapshotTime)
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

	// Both rows share the same package-level fields.
	assertField(t, "[0].PackageID", snapshots[0].PackageID, "pkg-multi")
	assertField(t, "[1].PackageID", snapshots[1].PackageID, "pkg-multi")
	assertField(t, "[0].AssetType", snapshots[0].AssetType, "BTC")
	assertField(t, "[1].AssetType", snapshots[1].AssetType, "ETH")
	assertField(t, "[0].AssetPrice", snapshots[0].AssetPrice, "70000")
	assertField(t, "[1].AssetPrice", snapshots[1].AssetPrice, "3000")
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

	assertField(t, "Active", snapshots[0].Active, false)
	assertField(t, "State", snapshots[0].State, "CLOSED")
	assertField(t, "AssetType", snapshots[0].AssetType, "")
	assertField(t, "CustodyType", snapshots[0].CustodyType, "")
	assertField(t, "AssetPrice", snapshots[0].AssetPrice, "")
	assertField(t, "AssetQuantity", snapshots[0].AssetQuantity, "")
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

func assertField[T comparable](t *testing.T, name string, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %v, want %v", name, got, want)
	}
}
