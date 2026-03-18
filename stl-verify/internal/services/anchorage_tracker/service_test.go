package anchorage_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

type mockClient struct {
	packages   []Package
	operations []Operation
	fetchErr   error
	callCount  atomic.Int32
}

func (m *mockClient) FetchPackages(_ context.Context) ([]Package, error) {
	m.callCount.Add(1)
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	return m.packages, nil
}

func (m *mockClient) ForEachOperationsPage(_ context.Context, _ string, fn func([]Operation) error) error {
	if m.fetchErr != nil {
		return m.fetchErr
	}
	if len(m.operations) > 0 {
		return fn(m.operations)
	}
	return nil
}

type mockSnapshotRepo struct {
	snapshots []entity.AnchoragePackageSnapshot
	saveErr   error
}

func (m *mockSnapshotRepo) SaveSnapshots(_ context.Context, s []entity.AnchoragePackageSnapshot) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	m.snapshots = append(m.snapshots, s...)
	return nil
}

type mockOperationRepo struct {
	operations []entity.AnchorageOperation
	cursor     string
	saveErr    error
}

func (m *mockOperationRepo) SaveOperations(_ context.Context, ops []entity.AnchorageOperation) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	m.operations = append(m.operations, ops...)
	return nil
}

func (m *mockOperationRepo) GetLastCursor(_ context.Context, _ int64) (string, error) {
	return m.cursor, nil
}

// ---------------------------------------------------------------------------
// toSnapshots tests
// ---------------------------------------------------------------------------

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

	if len(snapshots) != 0 {
		t.Fatalf("expected 0 snapshots for empty collateral package, got %d", len(snapshots))
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

// ---------------------------------------------------------------------------
// Service tests (using mocks)
// ---------------------------------------------------------------------------

func newTestPackage() Package {
	return Package{
		PackageID:      "pkg-1",
		PledgorID:      "p1",
		SecuredPartyID: "sp1",
		Active:         true,
		State:          "HEALTHY",
		CurrentLTV:     "0.5",
		ExposureValue:  "50000000",
		PackageValue:   "100000000",
		LTVTimestamp:   "2026-03-16T20:34:11Z",
		MarginCall:     MarginConfig{LTV: "0.8"},
		Critical:       MarginConfig{LTV: "0.9"},
		MarginReturn:   MarginReturnConfig{LTV: "0.6"},
		CollateralAssets: []CollateralAsset{
			{Asset: AssetInfo{AssetType: "BTC", Type: "AnchorageCustody"}, Price: "100000", Quantity: "1000", WeightedValue: "100000000"},
		},
	}
}

func newTestOperation() Operation {
	return Operation{
		ID:        "op-1",
		Action:    "INITIAL_DEPOSIT",
		Type:      "COLLATERAL_PACKAGE",
		TypeID:    "pkg-1",
		Asset:     AssetInfo{AssetType: "BTC", Type: "ANCHORAGECUSTODY"},
		Quantity:  "1000",
		Notes:     "test",
		CreatedAt: "2025-12-19T12:00:00.000000Z",
		UpdatedAt: "2025-12-19T12:00:00.000000Z",
	}
}

func TestService_StartAndStop(t *testing.T) {
	client := &mockClient{
		packages:   []Package{newTestPackage()},
		operations: []Operation{newTestOperation()},
	}
	snapRepo := &mockSnapshotRepo{}
	opRepo := &mockOperationRepo{}

	svc := NewService(client, snapRepo, opRepo, 1, 100*time.Millisecond, slog.Default())

	ctx := t.Context()

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Initial sync should have stored data.
	if len(snapRepo.snapshots) == 0 {
		t.Error("expected snapshots after Start")
	}
	if len(opRepo.operations) == 0 {
		t.Error("expected operations after Start")
	}

	// Wait for at least one tick.
	time.Sleep(200 * time.Millisecond)

	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Should have more snapshots from the tick.
	if len(snapRepo.snapshots) < 2 {
		t.Errorf("expected at least 2 snapshots (initial + tick), got %d", len(snapRepo.snapshots))
	}
}

func TestService_StartFailsOnBadPollInterval(t *testing.T) {
	svc := NewService(&mockClient{}, &mockSnapshotRepo{}, &mockOperationRepo{}, 1, 0, slog.Default())

	err := svc.Start(context.Background())
	if err == nil {
		t.Fatal("expected error for zero poll interval")
	}
}

func TestService_StartFailsOnAPIError(t *testing.T) {
	client := &mockClient{fetchErr: fmt.Errorf("api down")}
	svc := NewService(client, &mockSnapshotRepo{}, &mockOperationRepo{}, 1, time.Minute, slog.Default())

	err := svc.Start(context.Background())
	if err == nil {
		t.Fatal("expected error when initial poll fails")
	}
}

func TestService_BackfillOperations(t *testing.T) {
	client := &mockClient{
		operations: []Operation{newTestOperation()},
	}
	opRepo := &mockOperationRepo{}

	svc := NewService(client, &mockSnapshotRepo{}, opRepo, 1, time.Minute, slog.Default())

	n, err := svc.BackfillOperations(context.Background())
	if err != nil {
		t.Fatalf("BackfillOperations failed: %v", err)
	}

	if n != 1 {
		t.Errorf("expected 1 operation backfilled, got %d", n)
	}
	if len(opRepo.operations) != 1 {
		t.Errorf("expected 1 operation in repo, got %d", len(opRepo.operations))
	}
	assertField(t, "OperationID", opRepo.operations[0].OperationID, "op-1")
	assertField(t, "OperationType", opRepo.operations[0].OperationType, "COLLATERAL_PACKAGE")
}

func TestService_RetryOnTransientError(t *testing.T) {
	callCount := atomic.Int32{}
	snapRepo := &mockSnapshotRepo{}
	opRepo := &mockOperationRepo{}

	svc := NewService(&mockClient{}, snapRepo, opRepo, 1, time.Minute, slog.Default())

	// Use runWithRetry directly — fails twice, succeeds on third.
	ctx := context.Background()
	svc.runWithRetry(ctx, "test", func() error {
		n := callCount.Add(1)
		if n <= 2 {
			return fmt.Errorf("transient error")
		}
		return nil
	})

	if callCount.Load() != 3 {
		t.Errorf("expected 3 attempts (2 failures + 1 success), got %d", callCount.Load())
	}
}

func TestService_RetryExhausted(t *testing.T) {
	callCount := atomic.Int32{}
	svc := NewService(&mockClient{}, &mockSnapshotRepo{}, &mockOperationRepo{}, 1, time.Minute, slog.Default())

	ctx := context.Background()
	svc.runWithRetry(ctx, "test", func() error {
		callCount.Add(1)
		return fmt.Errorf("permanent error")
	})

	// maxRetries is 3, so 4 total attempts (0, 1, 2, 3).
	if callCount.Load() != 4 {
		t.Errorf("expected 4 attempts, got %d", callCount.Load())
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func assertField[T comparable](t *testing.T, name string, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %v, want %v", name, got, want)
	}
}
