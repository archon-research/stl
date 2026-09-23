package anchorage_tracker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

type mockClient struct {
	packages []Package
	// operationPages is served one page per callback, mirroring the client
	// following page.next.
	operationPages [][]Operation
	fetchErr       error
}

func (m *mockClient) FetchPackages(_ context.Context) ([]Package, error) {
	if m.fetchErr != nil {
		return nil, m.fetchErr
	}
	return m.packages, nil
}

func (m *mockClient) ForEachOperationsPage(_ context.Context, fn func([]Operation) error) error {
	if m.fetchErr != nil {
		return m.fetchErr
	}
	for _, page := range m.operationPages {
		if err := fn(page); err != nil {
			return err
		}
	}
	return nil
}

type mockSnapshotRepo struct {
	mu        sync.Mutex
	snapshots []entity.AnchoragePackageSnapshot
	saveErr   error
}

func (m *mockSnapshotRepo) SaveSnapshots(_ context.Context, s []entity.AnchoragePackageSnapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return m.saveErr
	}
	m.snapshots = append(m.snapshots, s...)
	return nil
}

func (m *mockSnapshotRepo) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.snapshots)
}

type mockOperationRepo struct {
	mu         sync.Mutex
	operations []entity.AnchorageOperation
	// known seeds KnownOperationIDs with ids "already in the database".
	known    []string
	saveErr  error
	knownErr error
	// saveCalls counts SaveOperations invocations that reached the store.
	saveCalls int
}

func (m *mockOperationRepo) SaveOperations(_ context.Context, ops []entity.AnchorageOperation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return m.saveErr
	}
	m.saveCalls++
	m.operations = append(m.operations, ops...)
	return nil
}

// KnownOperationIDs returns a fresh map each call so the service's own
// bookkeeping cannot leak back into the mock between runs.
func (m *mockOperationRepo) KnownOperationIDs(_ context.Context, _ int64) (map[string]struct{}, error) {
	if m.knownErr != nil {
		return nil, m.knownErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	known := make(map[string]struct{}, len(m.known)+len(m.operations))
	for _, id := range m.known {
		known[id] = struct{}{}
	}
	for _, op := range m.operations {
		known[op.OperationID] = struct{}{}
	}
	return known, nil
}

func (m *mockOperationRepo) GetPrimeIDByName(_ context.Context, name string) (int64, error) {
	if name == "" {
		return 0, fmt.Errorf("prime name is required")
	}
	return 1, nil
}

func (m *mockOperationRepo) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.operations)
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
// Service tests
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
	}
}

func TestService_Run(t *testing.T) {
	client := &mockClient{
		packages:       []Package{newTestPackage()},
		operationPages: [][]Operation{{newTestOperation()}},
	}
	snapRepo := &mockSnapshotRepo{}
	opRepo := &mockOperationRepo{}

	svc := NewService(client, snapRepo, opRepo, 1, nil)

	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if snapRepo.count() != 1 {
		t.Errorf("expected 1 snapshot, got %d", snapRepo.count())
	}
	if opRepo.count() != 1 {
		t.Errorf("expected 1 operation, got %d", opRepo.count())
	}
}

func TestService_RunSkipsInactivePackages(t *testing.T) {
	// Inactive package shaped like the real Anchorage response: empty
	// ltvTimestamp / currentLtv. Without filtering, this would fail the
	// whole batch on time.Parse and drop the active package too.
	inactive := Package{
		PackageID:    "pkg-inactive",
		Active:       false,
		State:        "",
		LTVTimestamp: "",
		CurrentLTV:   "",
	}
	client := &mockClient{
		packages:       []Package{inactive, newTestPackage()},
		operationPages: [][]Operation{{newTestOperation()}},
	}
	snapRepo := &mockSnapshotRepo{}
	opRepo := &mockOperationRepo{}

	svc := NewService(client, snapRepo, opRepo, 1, nil)

	if err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if snapRepo.count() != 1 {
		t.Errorf("expected 1 snapshot (active only), got %d", snapRepo.count())
	}
}

func TestService_RunFailsOnAPIError(t *testing.T) {
	client := &mockClient{fetchErr: fmt.Errorf("api down")}
	svc := NewService(client, &mockSnapshotRepo{}, &mockOperationRepo{}, 1, nil)

	err := svc.Run(context.Background())
	if err == nil {
		t.Fatal("expected error when API fails")
	}
}

func TestService_BackfillOperations(t *testing.T) {
	client := &mockClient{
		operationPages: [][]Operation{{newTestOperation()}},
	}
	opRepo := &mockOperationRepo{}

	svc := NewService(client, &mockSnapshotRepo{}, opRepo, 1, nil)

	n, err := svc.BackfillOperations(context.Background())
	if err != nil {
		t.Fatalf("BackfillOperations failed: %v", err)
	}

	if n != 1 {
		t.Errorf("expected 1 operation backfilled, got %d", n)
	}
	if opRepo.count() != 1 {
		t.Errorf("expected 1 operation in repo, got %d", opRepo.count())
	}

	opRepo.mu.Lock()
	assertField(t, "OperationID", opRepo.operations[0].OperationID, "op-1")
	assertField(t, "OperationType", opRepo.operations[0].OperationType, "COLLATERAL_PACKAGE")
	opRepo.mu.Unlock()
}

// The sync re-reads the whole feed every run (no afterId cursor, VEC-826), so
// everything already stored must be filtered out before SaveOperations and
// only genuinely new ids may reach the store.
func TestService_SyncOperationsStoresOnlyUnknownIDs(t *testing.T) {
	old := newTestOperation()
	fresh := newTestOperation()
	fresh.ID = "op-2"
	fresh.Action = "TOP_UP"
	fresh.CreatedAt = "2026-04-08T02:30:00.000000Z"

	client := &mockClient{
		operationPages: [][]Operation{{old, fresh}},
	}
	opRepo := &mockOperationRepo{known: []string{old.ID}}

	svc := NewService(client, &mockSnapshotRepo{}, opRepo, 1, nil)

	n, err := svc.syncOperations(context.Background())
	if err != nil {
		t.Fatalf("syncOperations failed: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 new operation stored, got %d", n)
	}
	if opRepo.count() != 1 {
		t.Fatalf("expected 1 operation in repo, got %d", opRepo.count())
	}
	opRepo.mu.Lock()
	assertField(t, "OperationID", opRepo.operations[0].OperationID, "op-2")
	opRepo.mu.Unlock()
}

// A page made only of known ids must not produce an empty SaveOperations
// call, and an id repeated across pages is stored once.
func TestService_SyncOperationsSkipsKnownPagesAndDuplicates(t *testing.T) {
	known := newTestOperation()
	fresh := newTestOperation()
	fresh.ID = "op-2"

	client := &mockClient{
		operationPages: [][]Operation{{known}, {fresh}, {fresh}},
	}
	opRepo := &mockOperationRepo{known: []string{known.ID}}

	svc := NewService(client, &mockSnapshotRepo{}, opRepo, 1, nil)

	n, err := svc.syncOperations(context.Background())
	if err != nil {
		t.Fatalf("syncOperations failed: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 stored, got %d", n)
	}
	opRepo.mu.Lock()
	defer opRepo.mu.Unlock()
	if opRepo.saveCalls != 1 {
		t.Errorf("expected exactly 1 SaveOperations call, got %d", opRepo.saveCalls)
	}
	if len(opRepo.operations) != 1 || opRepo.operations[0].OperationID != "op-2" {
		t.Errorf("expected only op-2 stored, got %+v", opRepo.operations)
	}
}

// Two consecutive runs over an unchanged feed store nothing on the second
// run: the ids written by run one are known to run two.
func TestService_SyncOperationsIsIdempotentAcrossRuns(t *testing.T) {
	client := &mockClient{
		operationPages: [][]Operation{{newTestOperation()}},
	}
	opRepo := &mockOperationRepo{}
	svc := NewService(client, &mockSnapshotRepo{}, opRepo, 1, nil)

	for run := 1; run <= 2; run++ {
		if _, err := svc.syncOperations(context.Background()); err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
	}
	if opRepo.count() != 1 {
		t.Errorf("expected 1 operation after two runs, got %d", opRepo.count())
	}
}

func TestService_SyncOperationsFailsWhenKnownIDsUnavailable(t *testing.T) {
	client := &mockClient{
		operationPages: [][]Operation{{newTestOperation()}},
	}
	opRepo := &mockOperationRepo{knownErr: fmt.Errorf("db down")}
	svc := NewService(client, &mockSnapshotRepo{}, opRepo, 1, nil)

	if _, err := svc.syncOperations(context.Background()); err == nil {
		t.Fatal("expected error when known ids cannot be listed")
	}
	if opRepo.count() != 0 {
		t.Errorf("expected nothing stored, got %d", opRepo.count())
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
