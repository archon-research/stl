package maple_graphql_indexer

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

type mockClient struct {
	GetPoolsFn         func(ctx context.Context) ([]outbound.MaplePool, error)
	GetActiveLoansFn   func(ctx context.Context) ([]outbound.MapleActiveLoan, error)
	GetSkyStrategiesFn func(ctx context.Context) ([]outbound.MapleSkyStrategy, error)
	GetSyrupGlobalsFn  func(ctx context.Context) (*outbound.MapleSyrupGlobals, error)

	activeLoansCalled bool
	strategiesCalled  bool
	globalsCalled     bool
}

func (m *mockClient) GetPools(ctx context.Context) ([]outbound.MaplePool, error) {
	return m.GetPoolsFn(ctx)
}

func (m *mockClient) GetActiveLoans(ctx context.Context) ([]outbound.MapleActiveLoan, error) {
	m.activeLoansCalled = true
	return m.GetActiveLoansFn(ctx)
}

func (m *mockClient) GetSkyStrategies(ctx context.Context) ([]outbound.MapleSkyStrategy, error) {
	m.strategiesCalled = true
	return m.GetSkyStrategiesFn(ctx)
}

func (m *mockClient) GetSyrupGlobals(ctx context.Context) (*outbound.MapleSyrupGlobals, error) {
	m.globalsCalled = true
	return m.GetSyrupGlobalsFn(ctx)
}

type mockRepo struct {
	GetMapleProtocolIDFn       func(ctx context.Context, chainID int64) (int64, error)
	GetOrCreateBorrowerUsersFn func(ctx context.Context, tx pgx.Tx, chainID int64, borrowers []common.Address) (map[common.Address]int64, error)
	UpsertPoolsFn              func(ctx context.Context, tx pgx.Tx, pools []*entity.MaplePool) (map[string]int64, error)
	SavePoolStatesFn           func(ctx context.Context, tx pgx.Tx, states []*entity.MaplePoolState) error
	UpsertLoansFn              func(ctx context.Context, tx pgx.Tx, loans []*entity.MapleLoan) (map[string]int64, error)
	SaveLoanStatesFn           func(ctx context.Context, tx pgx.Tx, states []*entity.MapleLoanState) error
	SaveLoanCollateralsFn      func(ctx context.Context, tx pgx.Tx, collaterals []*entity.MapleLoanCollateral) error
	UpsertSkyStrategiesFn      func(ctx context.Context, tx pgx.Tx, strategies []*entity.MapleSkyStrategy) (map[string]int64, error)
	SaveSkyStrategyStatesFn    func(ctx context.Context, tx pgx.Tx, states []*entity.MapleSkyStrategyState) error
	SaveSyrupGlobalStateFn     func(ctx context.Context, tx pgx.Tx, state *entity.MapleSyrupGlobalState) error

	// Captured arguments for assertions.
	borrowerCalls   [][]common.Address
	savedPoolStates []*entity.MaplePoolState
	savedLoanStates []*entity.MapleLoanState
	savedCollats    []*entity.MapleLoanCollateral
	savedStrategies []*entity.MapleSkyStrategyState
	savedGlobals    []*entity.MapleSyrupGlobalState
}

// newMockRepo returns a repo whose registry upserts assign sequential ids
// keyed by address and whose saves capture their inputs.
func newMockRepo() *mockRepo {
	r := &mockRepo{}
	r.GetMapleProtocolIDFn = func(context.Context, int64) (int64, error) { return 7, nil }
	r.GetOrCreateBorrowerUsersFn = func(_ context.Context, _ pgx.Tx, _ int64, borrowers []common.Address) (map[common.Address]int64, error) {
		r.borrowerCalls = append(r.borrowerCalls, borrowers)
		ids := make(map[common.Address]int64, len(borrowers))
		for i, b := range borrowers {
			ids[b] = int64(100 + i)
		}
		return ids, nil
	}
	r.UpsertPoolsFn = func(_ context.Context, _ pgx.Tx, pools []*entity.MaplePool) (map[string]int64, error) {
		ids := make(map[string]int64, len(pools))
		for i, p := range pools {
			ids["0x"+common.Bytes2Hex(p.Address)] = int64(10 + i)
		}
		return ids, nil
	}
	r.SavePoolStatesFn = func(_ context.Context, _ pgx.Tx, states []*entity.MaplePoolState) error {
		r.savedPoolStates = append(r.savedPoolStates, states...)
		return nil
	}
	r.UpsertLoansFn = func(_ context.Context, _ pgx.Tx, loans []*entity.MapleLoan) (map[string]int64, error) {
		ids := make(map[string]int64, len(loans))
		for i, l := range loans {
			ids["0x"+common.Bytes2Hex(l.LoanAddress)] = int64(20 + i)
		}
		return ids, nil
	}
	r.SaveLoanStatesFn = func(_ context.Context, _ pgx.Tx, states []*entity.MapleLoanState) error {
		r.savedLoanStates = append(r.savedLoanStates, states...)
		return nil
	}
	r.SaveLoanCollateralsFn = func(_ context.Context, _ pgx.Tx, collaterals []*entity.MapleLoanCollateral) error {
		r.savedCollats = append(r.savedCollats, collaterals...)
		return nil
	}
	r.UpsertSkyStrategiesFn = func(_ context.Context, _ pgx.Tx, strategies []*entity.MapleSkyStrategy) (map[string]int64, error) {
		ids := make(map[string]int64, len(strategies))
		for i, s := range strategies {
			ids["0x"+common.Bytes2Hex(s.StrategyAddress)] = int64(30 + i)
		}
		return ids, nil
	}
	r.SaveSkyStrategyStatesFn = func(_ context.Context, _ pgx.Tx, states []*entity.MapleSkyStrategyState) error {
		r.savedStrategies = append(r.savedStrategies, states...)
		return nil
	}
	r.SaveSyrupGlobalStateFn = func(_ context.Context, _ pgx.Tx, state *entity.MapleSyrupGlobalState) error {
		r.savedGlobals = append(r.savedGlobals, state)
		return nil
	}
	return r
}

func (m *mockRepo) GetMapleProtocolID(ctx context.Context, chainID int64) (int64, error) {
	return m.GetMapleProtocolIDFn(ctx, chainID)
}

func (m *mockRepo) GetOrCreateBorrowerUsers(ctx context.Context, tx pgx.Tx, chainID int64, borrowers []common.Address) (map[common.Address]int64, error) {
	return m.GetOrCreateBorrowerUsersFn(ctx, tx, chainID, borrowers)
}

func (m *mockRepo) UpsertPools(ctx context.Context, tx pgx.Tx, pools []*entity.MaplePool) (map[string]int64, error) {
	return m.UpsertPoolsFn(ctx, tx, pools)
}

func (m *mockRepo) SavePoolStates(ctx context.Context, tx pgx.Tx, states []*entity.MaplePoolState) error {
	return m.SavePoolStatesFn(ctx, tx, states)
}

func (m *mockRepo) UpsertLoans(ctx context.Context, tx pgx.Tx, loans []*entity.MapleLoan) (map[string]int64, error) {
	return m.UpsertLoansFn(ctx, tx, loans)
}

func (m *mockRepo) SaveLoanStates(ctx context.Context, tx pgx.Tx, states []*entity.MapleLoanState) error {
	return m.SaveLoanStatesFn(ctx, tx, states)
}

func (m *mockRepo) SaveLoanCollaterals(ctx context.Context, tx pgx.Tx, collaterals []*entity.MapleLoanCollateral) error {
	return m.SaveLoanCollateralsFn(ctx, tx, collaterals)
}

func (m *mockRepo) UpsertSkyStrategies(ctx context.Context, tx pgx.Tx, strategies []*entity.MapleSkyStrategy) (map[string]int64, error) {
	return m.UpsertSkyStrategiesFn(ctx, tx, strategies)
}

func (m *mockRepo) SaveSkyStrategyStates(ctx context.Context, tx pgx.Tx, states []*entity.MapleSkyStrategyState) error {
	return m.SaveSkyStrategyStatesFn(ctx, tx, states)
}

func (m *mockRepo) SaveSyrupGlobalState(ctx context.Context, tx pgx.Tx, state *entity.MapleSyrupGlobalState) error {
	return m.SaveSyrupGlobalStateFn(ctx, tx, state)
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

func addr(b byte) common.Address {
	var a common.Address
	for i := range a {
		a[i] = b
	}
	return a
}

func fixturePools() []outbound.MaplePool {
	return []outbound.MaplePool{
		{
			Address: addr(0x10), Name: "Syrup USDC", AssetAddress: addr(0xee),
			AssetSymbol: "USDC", AssetDecimals: 6, IsSyrup: true,
			TVL: big.NewInt(1000), LiquidAssets: big.NewInt(400),
			CollateralUSD: big.NewInt(500), PrincipalOut: big.NewInt(600),
			MonthlyAPY: big.NewInt(123), SpotAPY: big.NewInt(456),
		},
		{
			Address: addr(0x11), Name: "Empty Pool", AssetAddress: addr(0xef),
			AssetSymbol: "USDT", AssetDecimals: 6, IsSyrup: false,
			TVL: big.NewInt(0), LiquidAssets: big.NewInt(0),
			CollateralUSD: big.NewInt(0), PrincipalOut: big.NewInt(0),
		},
	}
}

func fixtureLoans() []outbound.MapleActiveLoan {
	return []outbound.MapleActiveLoan{
		{
			LoanID: addr(0x20), Borrower: addr(0xa0), State: "Active",
			PrincipalOwed: big.NewInt(100), AcmRatio: big.NewInt(1445731),
			Collateral: &outbound.MapleLoanCollateral{
				Asset: "BTC", AssetAmount: big.NewInt(5), AssetValueUSD: big.NewInt(6357500000),
				Decimals: 8, State: "Deposited", Custodian: "ANCHORAGE", LiquidationLevel: big.NewInt(1020000),
			},
			LoanMeta:    &outbound.MapleLoanMeta{Type: "amm", DexName: "Uniswap"},
			PoolAddress: addr(0x10),
		},
		{
			// Uncollateralized loan: nil collateral, nil meta, nil acmRatio.
			LoanID: addr(0x21), Borrower: addr(0xa1), State: "Active",
			PrincipalOwed: big.NewInt(200), AcmRatio: nil,
			PoolAddress: addr(0x10),
		},
		{
			// Same borrower as the first loan — must be deduplicated.
			LoanID: addr(0x22), Borrower: addr(0xa0), State: "Active",
			PrincipalOwed: big.NewInt(300), AcmRatio: big.NewInt(2000000),
			Collateral: &outbound.MapleLoanCollateral{
				Asset: "SOL", AssetAmount: big.NewInt(9), AssetValueUSD: big.NewInt(100), Decimals: 9,
			},
			PoolAddress: addr(0x11),
		},
	}
}

func fixtureStrategies() []outbound.MapleSkyStrategy {
	return []outbound.MapleSkyStrategy{
		{
			Address: addr(0x30), PoolAddress: addr(0x10), State: "Active", Version: 100,
			CurrentlyDeployed: big.NewInt(0), DepositedAssets: big.NewInt(100), WithdrawnAssets: big.NewInt(50),
			StrategyFeeRate: big.NewInt(100000), TotalFeesCollected: big.NewInt(1),
		},
	}
}

func fixtureGlobals() *outbound.MapleSyrupGlobals {
	return &outbound.MapleSyrupGlobals{
		TVL: big.NewInt(3563135115920200), APY: big.NewInt(1),
		CollateralAPY: big.NewInt(2), PoolAPY: big.NewInt(3), DripsYieldBoost: big.NewInt(0),
	}
}

func happyClient() *mockClient {
	return &mockClient{
		GetPoolsFn:         func(context.Context) ([]outbound.MaplePool, error) { return fixturePools(), nil },
		GetActiveLoansFn:   func(context.Context) ([]outbound.MapleActiveLoan, error) { return fixtureLoans(), nil },
		GetSkyStrategiesFn: func(context.Context) ([]outbound.MapleSkyStrategy, error) { return fixtureStrategies(), nil },
		GetSyrupGlobalsFn:  func(context.Context) (*outbound.MapleSyrupGlobals, error) { return fixtureGlobals(), nil },
	}
}

func newTestService(t *testing.T, client *mockClient, repo *mockRepo) *Service {
	t.Helper()
	service, err := NewService(ServiceConfig{ChainID: 1}, client, repo, &testutil.MockTxManager{}, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	// Fixed clock with sub-second precision to verify truncation.
	service.now = func() time.Time {
		return time.Date(2026, 6, 10, 10, 0, 0, 123456789, time.UTC)
	}
	return service
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

func TestNewService_Validation(t *testing.T) {
	client := happyClient()
	repo := newMockRepo()
	txm := &testutil.MockTxManager{}

	tests := []struct {
		name    string
		run     func() (*Service, error)
		wantErr string
	}{
		{
			name:    "nil client",
			run:     func() (*Service, error) { return NewService(ServiceConfig{ChainID: 1}, nil, repo, txm, nil) },
			wantErr: "client cannot be nil",
		},
		{
			name:    "nil repo",
			run:     func() (*Service, error) { return NewService(ServiceConfig{ChainID: 1}, client, nil, txm, nil) },
			wantErr: "repo cannot be nil",
		},
		{
			name:    "nil tx manager",
			run:     func() (*Service, error) { return NewService(ServiceConfig{ChainID: 1}, client, repo, nil, nil) },
			wantErr: "txManager cannot be nil",
		},
		{
			name:    "zero chain id",
			run:     func() (*Service, error) { return NewService(ServiceConfig{ChainID: 0}, client, repo, txm, nil) },
			wantErr: "chainID must be 1",
		},
		{
			name:    "non-mainnet chain id",
			run:     func() (*Service, error) { return NewService(ServiceConfig{ChainID: 137}, client, repo, txm, nil) },
			wantErr: "chainID must be 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.run()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}

	service, err := NewService(ServiceConfig{ChainID: 1}, client, repo, txm, nil)
	if err != nil {
		t.Fatalf("valid config: %v", err)
	}
	if service.logger == nil {
		t.Error("logger should default to slog.Default()")
	}
}

// ---------------------------------------------------------------------------
// Sync happy path
// ---------------------------------------------------------------------------

func TestSync_HappyPath(t *testing.T) {
	client := happyClient()
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	if err := service.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	wantSyncedAt := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC) // truncated to second

	if len(repo.savedPoolStates) != 2 {
		t.Fatalf("pool states = %d, want 2", len(repo.savedPoolStates))
	}
	// Utilization: 600/(400+600) = 0.6 for pool 1; empty pool guards to 0.
	if repo.savedPoolStates[0].Utilization != 0.6 {
		t.Errorf("utilization = %f, want 0.6", repo.savedPoolStates[0].Utilization)
	}
	if repo.savedPoolStates[1].Utilization != 0 {
		t.Errorf("empty pool utilization = %f, want 0", repo.savedPoolStates[1].Utilization)
	}

	if len(repo.savedLoanStates) != 3 {
		t.Fatalf("loan states = %d, want 3", len(repo.savedLoanStates))
	}
	// Nil collateral is skipped: 2 of 3 loans have collateral.
	if len(repo.savedCollats) != 2 {
		t.Errorf("collaterals = %d, want 2 (nil collateral skipped)", len(repo.savedCollats))
	}
	// Each state/collateral row must pair with the id mapped from ITS loan
	// address (mock assigns 20, 21, 22 in fixture order), not just have the
	// right counts.
	wantPrincipalByLoanID := map[int64]int64{20: 100, 21: 200, 22: 300}
	for _, s := range repo.savedLoanStates {
		want, ok := wantPrincipalByLoanID[s.MapleLoanID]
		if !ok {
			t.Errorf("unexpected loan id %d", s.MapleLoanID)
			continue
		}
		if s.PrincipalOwed.Int64() != want {
			t.Errorf("loan %d principal = %s, want %d", s.MapleLoanID, s.PrincipalOwed, want)
		}
		// Only the uncollateralized loan (0x21 -> id 21) has nil AcmRatio.
		if (s.AcmRatio == nil) != (s.MapleLoanID == 21) {
			t.Errorf("loan %d AcmRatio nil-ness wrong: %v", s.MapleLoanID, s.AcmRatio)
		}
	}
	wantCollateralByLoanID := map[int64]string{20: "BTC", 22: "SOL"}
	for _, c := range repo.savedCollats {
		if want := wantCollateralByLoanID[c.MapleLoanID]; c.AssetSymbol != want {
			t.Errorf("collateral for loan %d = %q, want %q", c.MapleLoanID, c.AssetSymbol, want)
		}
	}
	// Pool states pair with their pool ids (10, 11 in fixture order).
	if repo.savedPoolStates[0].MaplePoolID != 10 || repo.savedPoolStates[0].TVL.Int64() != 1000 {
		t.Errorf("pool state 0 = id %d tvl %s, want id 10 tvl 1000",
			repo.savedPoolStates[0].MaplePoolID, repo.savedPoolStates[0].TVL)
	}
	if repo.savedPoolStates[1].MaplePoolID != 11 || repo.savedPoolStates[1].TVL.Int64() != 0 {
		t.Errorf("pool state 1 = id %d tvl %s, want id 11 tvl 0",
			repo.savedPoolStates[1].MaplePoolID, repo.savedPoolStates[1].TVL)
	}

	// Borrower resolution: one call, two distinct borrowers (0xa0 deduped).
	if len(repo.borrowerCalls) != 1 {
		t.Fatalf("borrower calls = %d, want 1", len(repo.borrowerCalls))
	}
	if got := repo.borrowerCalls[0]; len(got) != 2 || got[0] != addr(0xa0) || got[1] != addr(0xa1) {
		t.Errorf("borrowers = %v, want [0xa0..., 0xa1...]", got)
	}

	if len(repo.savedStrategies) != 1 {
		t.Fatalf("strategy states = %d, want 1", len(repo.savedStrategies))
	}
	if len(repo.savedGlobals) != 1 {
		t.Fatalf("global states = %d, want 1", len(repo.savedGlobals))
	}

	// synced_at must be identical (and second-truncated) across every row of
	// the cycle.
	for _, s := range repo.savedPoolStates {
		if !s.SyncedAt.Equal(wantSyncedAt) {
			t.Errorf("pool state syncedAt = %v, want %v", s.SyncedAt, wantSyncedAt)
		}
	}
	for _, s := range repo.savedLoanStates {
		if !s.SyncedAt.Equal(wantSyncedAt) {
			t.Errorf("loan state syncedAt = %v, want %v", s.SyncedAt, wantSyncedAt)
		}
	}
	for _, c := range repo.savedCollats {
		if !c.SyncedAt.Equal(wantSyncedAt) {
			t.Errorf("collateral syncedAt = %v, want %v", c.SyncedAt, wantSyncedAt)
		}
	}
	for _, s := range repo.savedStrategies {
		if !s.SyncedAt.Equal(wantSyncedAt) {
			t.Errorf("strategy state syncedAt = %v, want %v", s.SyncedAt, wantSyncedAt)
		}
	}
	if !repo.savedGlobals[0].SyncedAt.Equal(wantSyncedAt) {
		t.Errorf("globals syncedAt = %v, want %v", repo.savedGlobals[0].SyncedAt, wantSyncedAt)
	}
}

// ---------------------------------------------------------------------------
// Phase failure isolation
// ---------------------------------------------------------------------------

func TestSync_PoolsFailSkipsDependentPhases(t *testing.T) {
	client := happyClient()
	client.GetPoolsFn = func(context.Context) ([]outbound.MaplePool, error) {
		return nil, errors.New("pools exploded")
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "pools exploded") {
		t.Errorf("error %q should contain the pool failure", err.Error())
	}
	if !strings.Contains(err.Error(), "skipping loans") || !strings.Contains(err.Error(), "skipping sky strategies") {
		t.Errorf("error %q should mark loans and strategies as skipped", err.Error())
	}
	if client.activeLoansCalled || client.strategiesCalled {
		t.Error("loans/strategies must not be fetched when pools fail")
	}
	// Globals are independent and must still run.
	if !client.globalsCalled {
		t.Error("globals must still run when pools fail")
	}
	if len(repo.savedGlobals) != 1 {
		t.Errorf("global states = %d, want 1", len(repo.savedGlobals))
	}
}

func TestSync_LoansFailAlone(t *testing.T) {
	client := happyClient()
	client.GetActiveLoansFn = func(context.Context) ([]outbound.MapleActiveLoan, error) {
		return nil, errors.New("loans exploded")
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "loans exploded") {
		t.Errorf("error %q should contain the loan failure", err.Error())
	}
	// Other phases all ran.
	if len(repo.savedPoolStates) != 2 || len(repo.savedStrategies) != 1 || len(repo.savedGlobals) != 1 {
		t.Errorf("other phases must run: pools=%d strategies=%d globals=%d",
			len(repo.savedPoolStates), len(repo.savedStrategies), len(repo.savedGlobals))
	}
}

func TestSync_StrategiesFailAlone(t *testing.T) {
	client := happyClient()
	client.GetSkyStrategiesFn = func(context.Context) ([]outbound.MapleSkyStrategy, error) {
		return nil, errors.New("strategies exploded")
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "strategies exploded") {
		t.Errorf("error %q should contain the strategy failure", err.Error())
	}
	if len(repo.savedPoolStates) != 2 || len(repo.savedLoanStates) != 3 || len(repo.savedGlobals) != 1 {
		t.Errorf("other phases must run: pools=%d loans=%d globals=%d",
			len(repo.savedPoolStates), len(repo.savedLoanStates), len(repo.savedGlobals))
	}
}

func TestSync_GlobalsFailAlone(t *testing.T) {
	client := happyClient()
	client.GetSyrupGlobalsFn = func(context.Context) (*outbound.MapleSyrupGlobals, error) {
		return nil, errors.New("globals exploded")
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "globals exploded") {
		t.Errorf("error %q should contain the globals failure", err.Error())
	}
	if len(repo.savedPoolStates) != 2 || len(repo.savedLoanStates) != 3 || len(repo.savedStrategies) != 1 {
		t.Errorf("other phases must run: pools=%d loans=%d strategies=%d",
			len(repo.savedPoolStates), len(repo.savedLoanStates), len(repo.savedStrategies))
	}
}

// ---------------------------------------------------------------------------
// Edge cases and error paths
// ---------------------------------------------------------------------------

func TestSync_LoanReferencingUnknownPool(t *testing.T) {
	client := happyClient()
	client.GetActiveLoansFn = func(context.Context) ([]outbound.MapleActiveLoan, error) {
		loans := fixtureLoans()
		loans[1].PoolAddress = addr(0x99) // not in the pool registry
		return loans, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "references unknown pool") {
		t.Errorf("error %q should name the unknown pool", err.Error())
	}
	// The whole loan snapshot is all-or-nothing.
	if len(repo.savedLoanStates) != 0 {
		t.Errorf("loan states = %d, want 0", len(repo.savedLoanStates))
	}
}

func TestSync_StrategyReferencingUnknownPool(t *testing.T) {
	client := happyClient()
	client.GetSkyStrategiesFn = func(context.Context) ([]outbound.MapleSkyStrategy, error) {
		strategies := fixtureStrategies()
		strategies[0].PoolAddress = addr(0x99)
		return strategies, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "references unknown pool") {
		t.Errorf("error %q should name the unknown pool", err.Error())
	}
}

func TestSync_ProtocolLookupFails(t *testing.T) {
	client := happyClient()
	repo := newMockRepo()
	repo.GetMapleProtocolIDFn = func(context.Context, int64) (int64, error) {
		return 0, errors.New("protocol missing")
	}
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "protocol missing") {
		t.Errorf("error %q should contain the protocol failure", err.Error())
	}
	if len(repo.savedPoolStates) != 0 {
		t.Errorf("pool states = %d, want 0", len(repo.savedPoolStates))
	}
}

func TestSync_EmptyLoansIsNotAnError(t *testing.T) {
	client := happyClient()
	client.GetActiveLoansFn = func(context.Context) ([]outbound.MapleActiveLoan, error) {
		return nil, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	if err := service.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if len(repo.borrowerCalls) != 0 {
		t.Error("no borrower resolution expected for empty loans")
	}
}

func TestSync_EmptyStrategiesIsNotAnError(t *testing.T) {
	client := happyClient()
	client.GetSkyStrategiesFn = func(context.Context) ([]outbound.MapleSkyStrategy, error) {
		return nil, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	if err := service.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if len(repo.savedStrategies) != 0 {
		t.Errorf("strategy states = %d, want 0", len(repo.savedStrategies))
	}
}

func TestSync_BorrowerMissingFromUpsertResult(t *testing.T) {
	client := happyClient()
	repo := newMockRepo()
	repo.GetOrCreateBorrowerUsersFn = func(context.Context, pgx.Tx, int64, []common.Address) (map[common.Address]int64, error) {
		return map[common.Address]int64{}, nil
	}
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "missing from upsert result") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_LoanMissingFromUpsertResult(t *testing.T) {
	client := happyClient()
	repo := newMockRepo()
	repo.UpsertLoansFn = func(context.Context, pgx.Tx, []*entity.MapleLoan) (map[string]int64, error) {
		return map[string]int64{}, nil
	}
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "missing from upsert result") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_PoolMissingFromUpsertResult(t *testing.T) {
	client := happyClient()
	repo := newMockRepo()
	repo.UpsertPoolsFn = func(context.Context, pgx.Tx, []*entity.MaplePool) (map[string]int64, error) {
		return map[string]int64{}, nil
	}
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "missing from upsert result") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_StrategyMissingFromUpsertResult(t *testing.T) {
	client := happyClient()
	repo := newMockRepo()
	repo.UpsertSkyStrategiesFn = func(context.Context, pgx.Tx, []*entity.MapleSkyStrategy) (map[string]int64, error) {
		return map[string]int64{}, nil
	}
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "missing from upsert result") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_InvalidPoolEntityFailsPhase(t *testing.T) {
	client := happyClient()
	client.GetPoolsFn = func(context.Context) ([]outbound.MaplePool, error) {
		pools := fixturePools()
		pools[0].AssetSymbol = "" // entity validation rejects this
		return pools, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "assetSymbol must not be empty") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_InvalidLoanStateFailsPhase(t *testing.T) {
	client := happyClient()
	client.GetActiveLoansFn = func(context.Context) ([]outbound.MapleActiveLoan, error) {
		loans := fixtureLoans()
		loans[0].PrincipalOwed = nil // entity validation rejects this
		return loans, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "principalOwed must not be nil") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_SaveErrorsPropagate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(r *mockRepo)
	}{
		{
			name: "pool states save fails",
			mutate: func(r *mockRepo) {
				r.SavePoolStatesFn = func(context.Context, pgx.Tx, []*entity.MaplePoolState) error {
					return errors.New("save failed")
				}
			},
		},
		{
			name: "loan states save fails",
			mutate: func(r *mockRepo) {
				r.SaveLoanStatesFn = func(context.Context, pgx.Tx, []*entity.MapleLoanState) error {
					return errors.New("save failed")
				}
			},
		},
		{
			name: "loan collaterals save fails",
			mutate: func(r *mockRepo) {
				r.SaveLoanCollateralsFn = func(context.Context, pgx.Tx, []*entity.MapleLoanCollateral) error {
					return errors.New("save failed")
				}
			},
		},
		{
			name: "strategy states save fails",
			mutate: func(r *mockRepo) {
				r.SaveSkyStrategyStatesFn = func(context.Context, pgx.Tx, []*entity.MapleSkyStrategyState) error {
					return errors.New("save failed")
				}
			},
		},
		{
			name: "globals save fails",
			mutate: func(r *mockRepo) {
				r.SaveSyrupGlobalStateFn = func(context.Context, pgx.Tx, *entity.MapleSyrupGlobalState) error {
					return errors.New("save failed")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newMockRepo()
			tt.mutate(repo)
			service := newTestService(t, happyClient(), repo)

			err := service.Sync(context.Background())
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "save failed") {
				t.Errorf("error = %q", err.Error())
			}
		})
	}
}

func TestSync_UpsertErrorsPropagate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(r *mockRepo)
	}{
		{
			name: "pool upsert fails",
			mutate: func(r *mockRepo) {
				r.UpsertPoolsFn = func(context.Context, pgx.Tx, []*entity.MaplePool) (map[string]int64, error) {
					return nil, errors.New("upsert failed")
				}
			},
		},
		{
			name: "borrower upsert fails",
			mutate: func(r *mockRepo) {
				r.GetOrCreateBorrowerUsersFn = func(context.Context, pgx.Tx, int64, []common.Address) (map[common.Address]int64, error) {
					return nil, errors.New("upsert failed")
				}
			},
		},
		{
			name: "loan upsert fails",
			mutate: func(r *mockRepo) {
				r.UpsertLoansFn = func(context.Context, pgx.Tx, []*entity.MapleLoan) (map[string]int64, error) {
					return nil, errors.New("upsert failed")
				}
			},
		},
		{
			name: "strategy upsert fails",
			mutate: func(r *mockRepo) {
				r.UpsertSkyStrategiesFn = func(context.Context, pgx.Tx, []*entity.MapleSkyStrategy) (map[string]int64, error) {
					return nil, errors.New("upsert failed")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newMockRepo()
			tt.mutate(repo)
			service := newTestService(t, happyClient(), repo)

			err := service.Sync(context.Background())
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "upsert failed") {
				t.Errorf("error = %q", err.Error())
			}
		})
	}
}

func TestSync_ZeroPoolsIsAnError(t *testing.T) {
	// {"data": null} decodes into an empty pool slice; a green run writing
	// zero rows forever must not happen.
	client := happyClient()
	client.GetPoolsFn = func(context.Context) ([]outbound.MaplePool, error) { return nil, nil }
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "0 pools") {
		t.Errorf("error = %q", err.Error())
	}
	// Loans/strategies skipped, globals independent.
	if client.activeLoansCalled || client.strategiesCalled {
		t.Error("dependent phases must be skipped on empty pools")
	}
	if len(repo.savedGlobals) != 1 {
		t.Errorf("global states = %d, want 1", len(repo.savedGlobals))
	}
}

func TestSync_DuplicateIDsFail(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(c *mockClient)
	}{
		{
			name: "duplicate pool",
			mutate: func(c *mockClient) {
				c.GetPoolsFn = func(context.Context) ([]outbound.MaplePool, error) {
					pools := fixturePools()
					pools[1].Address = pools[0].Address
					return pools, nil
				}
			},
		},
		{
			name: "duplicate loan",
			mutate: func(c *mockClient) {
				c.GetActiveLoansFn = func(context.Context) ([]outbound.MapleActiveLoan, error) {
					loans := fixtureLoans()
					loans[2].LoanID = loans[0].LoanID
					return loans, nil
				}
			},
		},
		{
			name: "duplicate strategy",
			mutate: func(c *mockClient) {
				c.GetSkyStrategiesFn = func(context.Context) ([]outbound.MapleSkyStrategy, error) {
					strategies := append(fixtureStrategies(), fixtureStrategies()...)
					return strategies, nil
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := happyClient()
			tt.mutate(client)
			repo := newMockRepo()
			service := newTestService(t, client, repo)

			err := service.Sync(context.Background())
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "duplicate") {
				t.Errorf("error = %q", err.Error())
			}
		})
	}
}

func TestSync_DecimalsOverflowFails(t *testing.T) {
	// An API bug returning decimals > MaxInt16 must not silently wrap into a
	// plausible small value.
	client := happyClient()
	client.GetPoolsFn = func(context.Context) ([]outbound.MaplePool, error) {
		pools := fixturePools()
		pools[0].AssetDecimals = 65542
		return pools, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "out of int16 range") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_CollateralDecimalsOverflowFails(t *testing.T) {
	client := happyClient()
	client.GetActiveLoansFn = func(context.Context) ([]outbound.MapleActiveLoan, error) {
		loans := fixtureLoans()
		loans[0].Collateral.Decimals = 1 << 20
		return loans, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "out of int16 range") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_InvalidPoolStateFailsPhase(t *testing.T) {
	client := happyClient()
	client.GetPoolsFn = func(context.Context) ([]outbound.MaplePool, error) {
		pools := fixturePools()
		pools[0].TVL = big.NewInt(-1) // state entity validation rejects this
		return pools, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "tvl must be non-negative") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_InvalidLoanEntityFailsPhase(t *testing.T) {
	client := happyClient()
	repo := newMockRepo()
	// A zero borrower user id resolves from the map but fails loan entity
	// validation.
	repo.GetOrCreateBorrowerUsersFn = func(_ context.Context, _ pgx.Tx, _ int64, borrowers []common.Address) (map[common.Address]int64, error) {
		ids := make(map[common.Address]int64, len(borrowers))
		for _, b := range borrowers {
			ids[b] = 0
		}
		return ids, nil
	}
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "borrowerUserID must be positive") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_InvalidCollateralFailsPhase(t *testing.T) {
	client := happyClient()
	client.GetActiveLoansFn = func(context.Context) ([]outbound.MapleActiveLoan, error) {
		loans := fixtureLoans()
		loans[0].Collateral.Asset = "" // entity validation rejects this
		return loans, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "assetSymbol must not be empty") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_InvalidStrategyFailsPhase(t *testing.T) {
	client := happyClient()
	client.GetSkyStrategiesFn = func(context.Context) ([]outbound.MapleSkyStrategy, error) {
		strategies := fixtureStrategies()
		strategies[0].Version = -1 // entity validation rejects this
		return strategies, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "version must be non-negative") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_InvalidStrategyStateFailsPhase(t *testing.T) {
	client := happyClient()
	client.GetSkyStrategiesFn = func(context.Context) ([]outbound.MapleSkyStrategy, error) {
		strategies := fixtureStrategies()
		strategies[0].CurrentlyDeployed = nil // state entity validation rejects this
		return strategies, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "currentlyDeployed must not be nil") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_InvalidGlobalsFailsPhase(t *testing.T) {
	client := happyClient()
	client.GetSyrupGlobalsFn = func(context.Context) (*outbound.MapleSyrupGlobals, error) {
		globals := fixtureGlobals()
		globals.TVL = nil
		return globals, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "tvl must not be nil") {
		t.Errorf("error = %q", err.Error())
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func TestToInt16(t *testing.T) {
	tests := []struct {
		name    string
		in      int
		want    int16
		wantErr bool
	}{
		{name: "zero", in: 0, want: 0},
		{name: "typical decimals", in: 18, want: 18},
		{name: "max int16", in: 32767, want: 32767},
		{name: "overflow", in: 32768, wantErr: true},
		{name: "wraps to plausible value", in: 65542, wantErr: true},
		{name: "negative", in: -1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toInt16(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("toInt16(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestToEntityLoanMeta(t *testing.T) {
	if toEntityLoanMeta(nil) != nil {
		t.Error("nil meta should map to nil")
	}

	got := toEntityLoanMeta(&outbound.MapleLoanMeta{
		Type: "amm", AssetSymbol: "WETH", DexName: "Uniswap",
		Location: "base", WalletAddress: "0xabc", WalletType: "EVM",
	})
	want := &entity.MapleLoanMeta{
		Type: "amm", AssetSymbol: "WETH", Dex: "Uniswap",
		Location: "base", WalletAddress: "0xabc", WalletType: "EVM",
	}
	if *got != *want {
		t.Errorf("toEntityLoanMeta = %+v, want %+v", got, want)
	}
}
