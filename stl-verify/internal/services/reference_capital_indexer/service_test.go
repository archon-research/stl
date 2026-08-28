package reference_capital_indexer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var (
	errProvider  = errors.New("provider boom")
	errRepo      = errors.New("repo boom")
	syncedAt     = time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)
	trackedStars = []string{"grove", "spark"}
)

type mockPrimeRepo struct {
	primes []entity.Prime
	err    error
}

func (m *mockPrimeRepo) GetPrimeIDByName(_ context.Context, name string) (int64, error) {
	for _, p := range m.primes {
		if p.Name == name {
			return p.ID, nil
		}
	}
	return 0, nil
}

func (m *mockPrimeRepo) ListPrimes(_ context.Context) ([]entity.Prime, error) {
	return m.primes, m.err
}

type mockCapitalRepo struct {
	snapshots []entity.PrimeCapitalStackSnapshot
	err       error
}

func (m *mockCapitalRepo) SavePrimeCapitalSnapshots(_ context.Context, _ pgx.Tx, snapshots []entity.PrimeCapitalStackSnapshot) error {
	if m.err != nil {
		return m.err
	}
	m.snapshots = append(m.snapshots, snapshots...)
	return nil
}

// fakeTxManager calls fn with a nil pgx.Tx; sufficient since the mock repos
// above ignore the tx argument.
type fakeTxManager struct{}

func (m *fakeTxManager) WithTransaction(_ context.Context, fn func(pgx.Tx) error) error {
	return fn(nil)
}

type mockRiskProvider struct {
	rows      []outbound.RiskCapitalPrimeSnapshot
	err       error
	requested []string
}

func (m *mockRiskProvider) FetchPrimeSnapshots(
	_ context.Context,
	stars []string,
) ([]outbound.RiskCapitalPrimeSnapshot, error) {
	m.requested = stars
	return m.rows, m.err
}

func upstreamRow(star string) outbound.RiskCapitalPrimeSnapshot {
	ratio := "0.3705"
	return outbound.RiskCapitalPrimeSnapshot{
		Star:                       star,
		Exposure:                   "2098090654.81",
		RequiredRiskCapital:        "17837860.43",
		TotalRiskCapital:           "48142491.08",
		JuniorRiskCapital:          "48142491.08",
		SeniorRiskCapital:          "0",
		InternalJuniorRiskCapital:  "48142491.08",
		ExternalJuniorRiskCapital:  "0",
		TokenizedJuniorRiskCapital: "0",
		InternalSeniorRiskCapital:  "0",
		ExternalSeniorRiskCapital:  "0",
		EncumbranceRatio:           &ratio,
		ExposureShare:              "0.0084",
		EPIUtilization:             "0",
		SPJUtilization:             "0",
	}
}

type mockSheetRepo struct {
	sheets []entity.PrimeBalanceSheetSnapshot
	err    error
}

func (m *mockSheetRepo) SaveBalanceSheetSnapshots(
	_ context.Context,
	snapshots []entity.PrimeBalanceSheetSnapshot,
) error {
	if m.err != nil {
		return m.err
	}
	m.sheets = append(m.sheets, snapshots...)
	return nil
}

type mockSheetProvider struct {
	days          []outbound.BalanceSheetDay
	err           error
	requestedDays int
}

func (m *mockSheetProvider) FetchHistory(
	_ context.Context,
	_ []string,
	daysAgo int,
) ([]outbound.BalanceSheetDay, error) {
	m.requestedDays = daysAgo
	if m.err != nil {
		return nil, m.err
	}
	return m.days, nil
}

// mockAllocationProvider fabricates one breakdown row per requested star unless
// given explicit rows, so tests not about allocations pass the breakdown
// coverage guard without stating fixtures.
type mockAllocationProvider struct {
	rows      []outbound.RiskCapitalAllocationRow
	err       error
	requested []string
}

func (m *mockAllocationProvider) FetchPrimeAllocations(
	_ context.Context,
	stars []string,
) ([]outbound.RiskCapitalAllocationRow, error) {
	m.requested = stars
	if m.err != nil {
		return nil, m.err
	}
	if m.rows != nil {
		return m.rows, nil
	}
	rows := make([]outbound.RiskCapitalAllocationRow, 0, len(stars))
	for _, star := range stars {
		rows = append(rows, allocationRow(star))
	}
	return rows, nil
}

func allocationRow(star string) outbound.RiskCapitalAllocationRow {
	chainID := int64(1)
	name := "Spark USDS"
	loanAddress := "0xdc035d45d973e3ec169d2276ddab16f1e407384f"
	loanSymbol := "USDS"
	return outbound.RiskCapitalAllocationRow{
		Star:                star,
		Protocol:            "sparklend",
		Network:             "ethereum",
		ChainID:             &chainID,
		Symbol:              "spUSDS",
		Name:                &name,
		TokenAddress:        "0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359",
		LoanTokenAddress:    &loanAddress,
		LoanTokenSymbol:     &loanSymbol,
		Exposure:            "782710914.129541047405509005",
		RequiredRiskCapital: "23308466.81",
		CRR:                 "0.0447",
	}
}

type mockAllocationRepo struct {
	allocations []entity.PrimeCapitalStackAllocation
	err         error
}

func (m *mockAllocationRepo) SaveCapitalStackAllocations(
	_ context.Context,
	_ pgx.Tx,
	allocations []entity.PrimeCapitalStackAllocation,
) error {
	if m.err != nil {
		return m.err
	}
	m.allocations = append(m.allocations, allocations...)
	return nil
}

// mockPositionProvider fabricates one position per requested star unless given
// explicit rows, mirroring mockAllocationProvider.
type mockPositionProvider struct {
	rows      []outbound.ReferencePositionRow
	err       error
	requested []string
}

func (m *mockPositionProvider) FetchPositions(
	_ context.Context,
	stars []string,
) ([]outbound.ReferencePositionRow, error) {
	m.requested = stars
	if m.err != nil {
		return nil, m.err
	}
	if m.rows != nil {
		return m.rows, nil
	}
	rows := make([]outbound.ReferencePositionRow, 0, len(stars))
	for _, star := range stars {
		rows = append(rows, positionRow(star))
	}
	return rows, nil
}

func positionRow(star string) outbound.ReferencePositionRow {
	chainID := int64(1)
	allocated := "0.631323107861320473"
	return outbound.ReferencePositionRow{
		Star:            star,
		Protocol:        "sparklend",
		Network:         "ethereum",
		ChainID:         &chainID,
		TokenSymbol:     "spUSDS",
		TokenAddress:    "0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359",
		Assets:          "0.825893123256664748",
		AllocatedAssets: &allocated,
	}
}

type mockPositionRepo struct {
	positions []entity.PrimeReferencePosition
	err       error
}

func (m *mockPositionRepo) SaveReferencePositions(
	_ context.Context,
	_ pgx.Tx,
	positions []entity.PrimeReferencePosition,
) error {
	if m.err != nil {
		return m.err
	}
	m.positions = append(m.positions, positions...)
	return nil
}

func defaultDeps(primes *mockPrimeRepo, capital *mockCapitalRepo, provider *mockRiskProvider) Deps {
	return Deps{
		PrimeRepo:          primes,
		CapitalRepo:        capital,
		RiskProvider:       provider,
		AllocationProvider: &mockAllocationProvider{},
		AllocationRepo:     &mockAllocationRepo{},
		SheetRepo:          &mockSheetRepo{},
		SheetProvider:      &mockSheetProvider{},
		PositionProvider:   &mockPositionProvider{},
		PositionRepo:       &mockPositionRepo{},
		TxManager:          &fakeTxManager{},
	}
}

func newService(primes *mockPrimeRepo, capital *mockCapitalRepo, provider *mockRiskProvider) *Service {
	return newServiceWithDeps(defaultDeps(primes, capital, provider))
}

func newServiceWithSheets(
	primes *mockPrimeRepo,
	capital *mockCapitalRepo,
	provider *mockRiskProvider,
	sheets *mockSheetRepo,
	sheetProvider *mockSheetProvider,
) *Service {
	deps := defaultDeps(primes, capital, provider)
	deps.SheetRepo = sheets
	deps.SheetProvider = sheetProvider
	return newServiceWithDeps(deps)
}

func newServiceWithDeps(deps Deps) *Service {
	service, err := NewService(deps, trackedStars, 7, func() time.Time { return syncedAt }, nil, nil)
	if err != nil {
		panic(err)
	}
	return service
}

func TestRunPersistsASnapshotPerUpstreamPrime(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}, {ID: 2, Name: "grove"}}}
	capital := &mockCapitalRepo{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark"), upstreamRow("grove")}}

	if err := newService(primes, capital, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	if got := len(capital.snapshots); got != 2 {
		t.Fatalf("saved %d snapshots, want 2", got)
	}
	if got := capital.snapshots[0].PrimeID; got != 1 {
		t.Errorf("PrimeID = %d, want 1", got)
	}
	if got := capital.snapshots[1].PrimeID; got != 2 {
		t.Errorf("PrimeID = %d, want 2", got)
	}
}

func TestRunStampsEveryRowOfACycleWithOneSyncedAtAndBuildID(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}, {ID: 2, Name: "grove"}}}
	capital := &mockCapitalRepo{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark"), upstreamRow("grove")}}

	if err := newService(primes, capital, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	for i, s := range capital.snapshots {
		if !s.SyncedAt.Equal(syncedAt) {
			t.Errorf("snapshot %d SyncedAt = %v, want %v", i, s.SyncedAt, syncedAt)
		}
		if s.BuildID != 7 {
			t.Errorf("snapshot %d BuildID = %d, want 7", i, s.BuildID)
		}
	}
}

func TestRunCarriesEveryUpstreamFigureOntoTheSnapshot(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	capital := &mockCapitalRepo{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	if err := newService(primes, capital, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	got := capital.snapshots[0]
	for _, tc := range []struct{ field, got, want string }{
		{"ExposureUSD", got.ExposureUSD, "2098090654.81"},
		{"RequiredRiskCapitalUSD", got.RequiredRiskCapitalUSD, "17837860.43"},
		{"TotalRiskCapitalUSD", got.TotalRiskCapitalUSD, "48142491.08"},
		{"JuniorRiskCapitalUSD", got.JuniorRiskCapitalUSD, "48142491.08"},
		{"SeniorRiskCapitalUSD", got.SeniorRiskCapitalUSD, "0"},
		{"ExposureShare", got.ExposureShare, "0.0084"},
		{"Source", got.Source, entity.ReferenceDataSource},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.field, tc.got, tc.want)
		}
	}
	if got.EncumbranceRatio == nil || *got.EncumbranceRatio != "0.3705" {
		t.Errorf("EncumbranceRatio = %v, want 0.3705", got.EncumbranceRatio)
	}
}

func TestRunKeepsAnAbsentEncumbranceRatioNullRatherThanZero(t *testing.T) {
	row := upstreamRow("spark")
	row.EncumbranceRatio = nil
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	capital := &mockCapitalRepo{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{row}}

	if err := newService(primes, capital, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	if capital.snapshots[0].EncumbranceRatio != nil {
		t.Errorf("EncumbranceRatio = %v, want nil", *capital.snapshots[0].EncumbranceRatio)
	}
}

func TestRunMatchesPrimeNamesCaseInsensitively(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "Spark"}}}
	capital := &mockCapitalRepo{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	if err := newService(primes, capital, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	if len(capital.snapshots) != 1 {
		t.Fatalf("saved %d snapshots, want 1", len(capital.snapshots))
	}
}

// A star the registry does not know must fail the cycle rather than be skipped:
// a skipped prime leaves a hole indistinguishable from one the monitor stopped
// covering.
func TestRunFailsOnAPrimeTheRegistryDoesNotKnow(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	capital := &mockCapitalRepo{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark"), upstreamRow("newstar")}}

	err := newService(primes, capital, provider).Run(context.Background())

	if err == nil {
		t.Fatal("Run() = nil, want an error naming the unknown prime")
	}
	if len(capital.snapshots) != 0 {
		t.Errorf("saved %d snapshots, want 0 — a failed cycle must persist nothing", len(capital.snapshots))
	}
}

// The monitor reports primes STL does not track (obex, osero today). Asking
// only for the tracked ones keeps their snapshots out of the table entirely,
// rather than relying on the prime table to exclude them — it still carries a
// row for a prime STL has stopped tracking.
func TestRunAsksTheMonitorOnlyForTheTrackedPrimes(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}, {ID: 2, Name: "grove"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark"), upstreamRow("grove")}}

	if err := newService(primes, &mockCapitalRepo{}, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	if len(provider.requested) != 2 || provider.requested[0] != "grove" || provider.requested[1] != "spark" {
		t.Errorf("requested = %v, want [grove spark]", provider.requested)
	}
}

func TestRunRecordsTheTrackedPrimesTheMonitorDoesCover(t *testing.T) {
	// A tracked prime the monitor omits is a coverage fact, not a failure, so
	// the rest of the cycle is still recorded.
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}, {ID: 2, Name: "grove"}}}
	capital := &mockCapitalRepo{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	if err := newService(primes, capital, provider).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	if len(capital.snapshots) != 1 || capital.snapshots[0].PrimeID != 1 {
		t.Errorf("snapshots = %+v, want one for spark", capital.snapshots)
	}
}

func TestRunFailsWhenNoPrimesAreTracked(t *testing.T) {
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}
	service, err := NewService(
		defaultDeps(&mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}, &mockCapitalRepo{}, provider),
		nil,
		7,
		func() time.Time { return syncedAt },
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("NewService() = %v", err)
	}

	if err := service.Run(context.Background()); err == nil {
		t.Fatal("Run() = nil, want an error")
	}
}

// A forgotten Deps field must die at wiring time, not nil-deref mid-cycle
// after rows were already persisted.
func TestNewServiceRejectsAMissingPort(t *testing.T) {
	deps := defaultDeps(&mockPrimeRepo{}, &mockCapitalRepo{}, &mockRiskProvider{})
	deps.PositionRepo = nil

	_, err := NewService(deps, trackedStars, 7, nil, nil, nil)

	if err == nil {
		t.Fatal("NewService() = nil, want an error naming the missing port")
	}
	if !strings.Contains(err.Error(), "PositionRepo") {
		t.Errorf("error = %v, want it to name PositionRepo", err)
	}
}

func TestRunFailsWhenTheMonitorReportsNoPrimes(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	capital := &mockCapitalRepo{}

	err := newService(primes, capital, &mockRiskProvider{}).Run(context.Background())

	if err == nil {
		t.Fatal("Run() = nil, want an error — an empty feed is a broken feed, not a no-op")
	}
}

func TestRunFailsWhenNoPrimesExistToAttributeSnapshotsTo(t *testing.T) {
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	err := newService(&mockPrimeRepo{}, &mockCapitalRepo{}, provider).Run(context.Background())

	if err == nil {
		t.Fatal("Run() = nil, want an error")
	}
}

func TestRunPropagatesAProviderFailure(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}

	err := newService(primes, &mockCapitalRepo{}, &mockRiskProvider{err: errProvider}).Run(context.Background())

	if !errors.Is(err, errProvider) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, errProvider)
	}
}

func TestRunPropagatesAPersistenceFailure(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	err := newService(primes, &mockCapitalRepo{err: errRepo}, provider).Run(context.Background())

	if !errors.Is(err, errRepo) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, errRepo)
	}
}

func TestRunPropagatesAPrimeListingFailure(t *testing.T) {
	primes := &mockPrimeRepo{err: errRepo}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	err := newService(primes, &mockCapitalRepo{}, provider).Run(context.Background())

	if !errors.Is(err, errRepo) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, errRepo)
	}
}

func TestRunAdvancesTheBalanceSheetEachCycle(t *testing.T) {
	// The monitor publishes no assets figure, so without this the collateral
	// series stops the day the one-shot backfill ran.
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	sheets := &mockSheetRepo{}
	sheetProvider := &mockSheetProvider{days: []outbound.BalanceSheetDay{{
		Star: "spark", Date: "2026-08-19", TreasuryBalance: "1", Assets: "3291806969.21",
		AllocatedAssets: "2", IdleAssets: "3", Debt: "4", BackstopCapital: "5",
	}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	err := newServiceWithSheets(primes, &mockCapitalRepo{}, provider, sheets, sheetProvider).
		Run(context.Background())

	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if len(sheets.sheets) != 1 {
		t.Fatalf("saved %d balance sheets, want 1", len(sheets.sheets))
	}
	if got := sheets.sheets[0].AssetsUSD; got != "3291806969.21" {
		t.Errorf("AssetsUSD = %q, want the upstream figure", got)
	}
	if got := sheets.sheets[0].Source; got != entity.ReferenceDataSource {
		t.Errorf("Source = %q, want %q", got, entity.ReferenceDataSource)
	}
}

func TestRunAsksOnlyForEnoughDaysToCloseTheGap(t *testing.T) {
	// The backfill seeded the year; a history-sized window every cycle would
	// re-fetch all of it each time.
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	sheetProvider := &mockSheetProvider{}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	err := newServiceWithSheets(primes, &mockCapitalRepo{}, provider, &mockSheetRepo{}, sheetProvider).
		Run(context.Background())

	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if sheetProvider.requestedDays != balanceSheetLookbackDays {
		t.Errorf("requested %d days, want %d", sheetProvider.requestedDays, balanceSheetLookbackDays)
	}
}

func TestRunTreatsNoNewCompletedDayAsSuccess(t *testing.T) {
	// The provider withholds the in-progress day, so a cycle running before
	// upstream publishes yesterday legitimately finds nothing to add. The repo
	// is rigged to fail, which proves it is never reached.
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	err := newServiceWithSheets(
		primes, &mockCapitalRepo{}, provider, &mockSheetRepo{err: errRepo}, &mockSheetProvider{},
	).Run(context.Background())

	if err != nil {
		t.Fatalf("Run() = %v, want nil — an empty window must not fail the cycle", err)
	}
}

func TestRunPropagatesABalanceSheetFailure(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}

	err := newServiceWithSheets(
		primes, &mockCapitalRepo{}, provider, &mockSheetRepo{}, &mockSheetProvider{err: errProvider},
	).Run(context.Background())

	if !errors.Is(err, errProvider) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, errProvider)
	}
}

func TestRunPersistsTheBreakdownWithTheCyclesSyncedAt(t *testing.T) {
	// The breakdown must join its prime-level totals exactly, which only holds
	// if both tables carry the same cycle timestamp.
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}
	allocations := &mockAllocationRepo{}
	deps := defaultDeps(primes, &mockCapitalRepo{}, provider)
	deps.AllocationRepo = allocations

	if err := newServiceWithDeps(deps).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if len(allocations.allocations) != 1 {
		t.Fatalf("saved %d allocations, want 1", len(allocations.allocations))
	}
	got := allocations.allocations[0]
	if !got.SyncedAt.Equal(syncedAt) {
		t.Errorf("SyncedAt = %v, want the cycle's %v", got.SyncedAt, syncedAt)
	}
	if got.PrimeID != 1 {
		t.Errorf("PrimeID = %d, want 1", got.PrimeID)
	}
	if got.BuildID != 7 {
		t.Errorf("BuildID = %d, want 7", got.BuildID)
	}
	if got.Source != entity.ReferenceDataSource {
		t.Errorf("Source = %q, want %q", got.Source, entity.ReferenceDataSource)
	}
}

func TestRunCarriesEveryBreakdownFigureOntoTheAllocation(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}
	allocations := &mockAllocationRepo{}
	deps := defaultDeps(primes, &mockCapitalRepo{}, provider)
	deps.AllocationRepo = allocations

	if err := newServiceWithDeps(deps).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	got := allocations.allocations[0]
	for _, tc := range []struct{ field, got, want string }{
		{"ProtocolName", got.ProtocolName, "sparklend"},
		{"Network", got.Network, "ethereum"},
		{"Symbol", got.Symbol, "spUSDS"},
		{"TokenAddress", got.TokenAddress, "0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359"},
		{"ExposureUSD", got.ExposureUSD, "782710914.129541047405509005"},
		{"RequiredRiskCapitalUSD", got.RequiredRiskCapitalUSD, "23308466.81"},
		{"CRR", got.CRR, "0.0447"},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %q, want %q", tc.field, tc.got, tc.want)
		}
	}
	if got.ChainID == nil || *got.ChainID != 1 {
		t.Errorf("ChainID = %v, want 1", got.ChainID)
	}
	if got.LoanTokenSymbol == nil || *got.LoanTokenSymbol != "USDS" {
		t.Errorf("LoanTokenSymbol = %v, want USDS", got.LoanTokenSymbol)
	}
}

func TestRunAsksForBreakdownsOnlyForTheCoveredStars(t *testing.T) {
	// grove is tracked but uncovered this cycle; the breakdown route answers an
	// unknown star with a 500, so it must never be asked for one.
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}
	allocationProvider := &mockAllocationProvider{}
	positionProvider := &mockPositionProvider{}
	deps := defaultDeps(primes, &mockCapitalRepo{}, provider)
	deps.AllocationProvider = allocationProvider
	deps.PositionProvider = positionProvider

	if err := newServiceWithDeps(deps).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if len(allocationProvider.requested) != 1 || allocationProvider.requested[0] != "spark" {
		t.Errorf("allocation stars = %v, want [spark]", allocationProvider.requested)
	}
	if len(positionProvider.requested) != 1 || positionProvider.requested[0] != "spark" {
		t.Errorf("position stars = %v, want [spark]", positionProvider.requested)
	}
}

func TestRunFailsWhenACoveredStarHasExposureButNoBreakdown(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}
	deps := defaultDeps(primes, &mockCapitalRepo{}, provider)
	deps.AllocationProvider = &mockAllocationProvider{rows: []outbound.RiskCapitalAllocationRow{}}

	err := newServiceWithDeps(deps).Run(context.Background())

	if err == nil {
		t.Fatal("Run() = nil, want an error — real exposure beside an empty breakdown is upstream disagreeing with itself")
	}
}

func TestRunAcceptsAnEmptyBreakdownForAZeroExposureStar(t *testing.T) {
	row := upstreamRow("spark")
	row.Exposure = "0"
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{row}}
	deps := defaultDeps(primes, &mockCapitalRepo{}, provider)
	deps.AllocationProvider = &mockAllocationProvider{rows: []outbound.RiskCapitalAllocationRow{}}

	if err := newServiceWithDeps(deps).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil — a prime holding nothing has nothing to break down", err)
	}
}

func TestRunPersistsThePositionsWithTheCyclesSyncedAt(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}
	positions := &mockPositionRepo{}
	deps := defaultDeps(primes, &mockCapitalRepo{}, provider)
	deps.PositionRepo = positions

	if err := newServiceWithDeps(deps).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if len(positions.positions) != 1 {
		t.Fatalf("saved %d positions, want 1", len(positions.positions))
	}
	got := positions.positions[0]
	if !got.SyncedAt.Equal(syncedAt) {
		t.Errorf("SyncedAt = %v, want the cycle's %v", got.SyncedAt, syncedAt)
	}
	if got.AssetsUSD != "0.825893123256664748" {
		t.Errorf("AssetsUSD = %q, want the upstream figure", got.AssetsUSD)
	}
	if got.Source != entity.ReferenceDataSource {
		t.Errorf("Source = %q, want %q", got.Source, entity.ReferenceDataSource)
	}
	if got.IdleAssetsUSD != nil {
		t.Errorf("IdleAssetsUSD = %v, want nil — an omitted figure must not become zero", *got.IdleAssetsUSD)
	}
}

func TestRunFailsOnABreakdownForAPrimeTheRegistryDoesNotKnow(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}
	deps := defaultDeps(primes, &mockCapitalRepo{}, provider)
	deps.AllocationProvider = &mockAllocationProvider{rows: []outbound.RiskCapitalAllocationRow{allocationRow("newstar")}}

	err := newServiceWithDeps(deps).Run(context.Background())

	if err == nil {
		t.Fatal("Run() = nil, want an error naming the unknown prime")
	}
}

func TestRunPropagatesAnAllocationProviderFailure(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}
	deps := defaultDeps(primes, &mockCapitalRepo{}, provider)
	deps.AllocationProvider = &mockAllocationProvider{err: errProvider}

	if err := newServiceWithDeps(deps).Run(context.Background()); !errors.Is(err, errProvider) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, errProvider)
	}
}

func TestRunPropagatesAnAllocationPersistenceFailure(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}
	deps := defaultDeps(primes, &mockCapitalRepo{}, provider)
	deps.AllocationRepo = &mockAllocationRepo{err: errRepo}

	if err := newServiceWithDeps(deps).Run(context.Background()); !errors.Is(err, errRepo) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, errRepo)
	}
}

func TestRunPropagatesAPositionProviderFailure(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}
	deps := defaultDeps(primes, &mockCapitalRepo{}, provider)
	deps.PositionProvider = &mockPositionProvider{err: errProvider}

	if err := newServiceWithDeps(deps).Run(context.Background()); !errors.Is(err, errProvider) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, errProvider)
	}
}

func TestRunPropagatesAPositionPersistenceFailure(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}
	deps := defaultDeps(primes, &mockCapitalRepo{}, provider)
	deps.PositionRepo = &mockPositionRepo{err: errRepo}

	if err := newServiceWithDeps(deps).Run(context.Background()); !errors.Is(err, errRepo) {
		t.Fatalf("Run() = %v, want it to wrap %v", err, errRepo)
	}
}

func TestRunFailsOnAPositionForAPrimeTheRegistryDoesNotKnow(t *testing.T) {
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{upstreamRow("spark")}}
	deps := defaultDeps(primes, &mockCapitalRepo{}, provider)
	deps.PositionProvider = &mockPositionProvider{rows: []outbound.ReferencePositionRow{positionRow("newstar")}}

	err := newServiceWithDeps(deps).Run(context.Background())

	if err == nil {
		t.Fatal("Run() = nil, want an error naming the unknown prime")
	}
}

func TestRunFailsOnAnUnparseableExposure(t *testing.T) {
	row := upstreamRow("spark")
	row.Exposure = "n/a"
	primes := &mockPrimeRepo{primes: []entity.Prime{{ID: 1, Name: "spark"}}}
	provider := &mockRiskProvider{rows: []outbound.RiskCapitalPrimeSnapshot{row}}
	deps := defaultDeps(primes, &mockCapitalRepo{}, provider)
	deps.AllocationProvider = &mockAllocationProvider{rows: []outbound.RiskCapitalAllocationRow{}}

	err := newServiceWithDeps(deps).Run(context.Background())

	if err == nil {
		t.Fatal("Run() = nil, want an error — an unparseable exposure must fail, not panic")
	}
}
