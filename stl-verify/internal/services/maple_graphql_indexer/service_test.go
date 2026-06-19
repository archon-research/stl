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
	"github.com/archon-research/stl/stl-verify/internal/domain/entity/maple"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

type mockClient struct {
	GetPoolsFn                func(ctx context.Context) ([]outbound.MaplePool, error)
	GetActiveLoansFn          func(ctx context.Context) ([]outbound.MapleActiveLoan, error)
	GetActiveFixedTermLoansFn func(ctx context.Context) ([]outbound.MapleFixedTermLoan, error)
	GetSkyStrategiesFn        func(ctx context.Context) ([]outbound.MapleSkyStrategy, error)
	GetSyrupGlobalsFn         func(ctx context.Context) (*outbound.MapleSyrupGlobals, error)

	activeLoansCalled bool
	ftlLoansCalled    bool
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

func (m *mockClient) GetActiveFixedTermLoans(ctx context.Context) ([]outbound.MapleFixedTermLoan, error) {
	m.ftlLoansCalled = true
	return m.GetActiveFixedTermLoansFn(ctx)
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
	GetMapleProtocolIDFn    func(ctx context.Context, chainID int64) (int64, error)
	UpsertPoolsFn           func(ctx context.Context, tx pgx.Tx, pools []*maple.Pool) (map[common.Address]int64, error)
	SavePoolStatesFn        func(ctx context.Context, tx pgx.Tx, states []*maple.PoolState) error
	UpsertLoansFn           func(ctx context.Context, tx pgx.Tx, loans []*maple.Loan) (map[common.Address]int64, error)
	SaveLoanStatesFn        func(ctx context.Context, tx pgx.Tx, states []*maple.LoanState) error
	SaveLoanCollateralsFn   func(ctx context.Context, tx pgx.Tx, collaterals []*maple.LoanCollateral) error
	UpsertFixedTermLoansFn  func(ctx context.Context, tx pgx.Tx, loans []*maple.FTLLoan) (map[common.Address]int64, error)
	SaveFTLLoanStatesFn     func(ctx context.Context, tx pgx.Tx, states []*maple.FTLLoanState) error
	UpsertSkyStrategiesFn   func(ctx context.Context, tx pgx.Tx, strategies []*maple.SkyStrategy) (map[common.Address]int64, error)
	SaveSkyStrategyStatesFn func(ctx context.Context, tx pgx.Tx, states []*maple.SkyStrategyState) error
	SaveSyrupGlobalStateFn  func(ctx context.Context, tx pgx.Tx, state *maple.SyrupGlobalState) error

	// Token and user registries live behind their own ports; the maple repo no
	// longer resolves them. newMockRepo wires these to capture and id behavior.
	tokenRepo *testutil.MockTokenRepository
	userRepo  *testutil.MockUserRepository

	// Captured arguments for assertions.
	borrowerCalls    [][]entity.User
	assetTokenCalls  [][]outbound.TokenInput
	upsertedPools    []*maple.Pool
	upsertedLoans    []*maple.Loan
	upsertedFTLLoans []*maple.FTLLoan
	savedPoolStates  []*maple.PoolState
	savedLoanStates  []*maple.LoanState
	savedFTLStates   []*maple.FTLLoanState
	savedCollats     []*maple.LoanCollateral
	savedStrategies  []*maple.SkyStrategyState
	savedGlobals     []*maple.SyrupGlobalState
}

// newMockRepo returns a repo whose registry upserts assign sequential ids
// keyed by address and whose saves capture their inputs.
func newMockRepo() *mockRepo {
	r := &mockRepo{}
	r.GetMapleProtocolIDFn = func(context.Context, int64) (int64, error) { return 7, nil }
	r.userRepo = &testutil.MockUserRepository{
		GetOrCreateUsersFn: func(_ context.Context, _ pgx.Tx, users []entity.User) (map[common.Address]int64, error) {
			r.borrowerCalls = append(r.borrowerCalls, users)
			ids := make(map[common.Address]int64, len(users))
			for i, u := range users {
				ids[u.Address] = int64(100 + i)
			}
			return ids, nil
		},
	}
	r.tokenRepo = &testutil.MockTokenRepository{
		GetOrCreateTokensFn: func(_ context.Context, _ pgx.Tx, tokens []outbound.TokenInput) (map[common.Address]int64, error) {
			r.assetTokenCalls = append(r.assetTokenCalls, tokens)
			ids := make(map[common.Address]int64, len(tokens))
			for i, t := range tokens {
				ids[t.Address] = int64(500 + i)
			}
			return ids, nil
		},
	}
	r.UpsertPoolsFn = func(_ context.Context, _ pgx.Tx, pools []*maple.Pool) (map[common.Address]int64, error) {
		r.upsertedPools = append(r.upsertedPools, pools...)
		ids := make(map[common.Address]int64, len(pools))
		for i, p := range pools {
			ids[common.BytesToAddress(p.Address)] = int64(10 + i)
		}
		return ids, nil
	}
	r.SavePoolStatesFn = func(_ context.Context, _ pgx.Tx, states []*maple.PoolState) error {
		r.savedPoolStates = append(r.savedPoolStates, states...)
		return nil
	}
	r.UpsertLoansFn = func(_ context.Context, _ pgx.Tx, loans []*maple.Loan) (map[common.Address]int64, error) {
		r.upsertedLoans = append(r.upsertedLoans, loans...)
		ids := make(map[common.Address]int64, len(loans))
		for i, l := range loans {
			ids[common.BytesToAddress(l.LoanAddress)] = int64(20 + i)
		}
		return ids, nil
	}
	r.SaveLoanStatesFn = func(_ context.Context, _ pgx.Tx, states []*maple.LoanState) error {
		r.savedLoanStates = append(r.savedLoanStates, states...)
		return nil
	}
	r.SaveLoanCollateralsFn = func(_ context.Context, _ pgx.Tx, collaterals []*maple.LoanCollateral) error {
		r.savedCollats = append(r.savedCollats, collaterals...)
		return nil
	}
	r.UpsertFixedTermLoansFn = func(_ context.Context, _ pgx.Tx, loans []*maple.FTLLoan) (map[common.Address]int64, error) {
		r.upsertedFTLLoans = append(r.upsertedFTLLoans, loans...)
		ids := make(map[common.Address]int64, len(loans))
		for i, l := range loans {
			ids[common.BytesToAddress(l.LoanAddress)] = int64(40 + i)
		}
		return ids, nil
	}
	r.SaveFTLLoanStatesFn = func(_ context.Context, _ pgx.Tx, states []*maple.FTLLoanState) error {
		r.savedFTLStates = append(r.savedFTLStates, states...)
		return nil
	}
	r.UpsertSkyStrategiesFn = func(_ context.Context, _ pgx.Tx, strategies []*maple.SkyStrategy) (map[common.Address]int64, error) {
		ids := make(map[common.Address]int64, len(strategies))
		for i, s := range strategies {
			ids[common.BytesToAddress(s.StrategyAddress)] = int64(30 + i)
		}
		return ids, nil
	}
	r.SaveSkyStrategyStatesFn = func(_ context.Context, _ pgx.Tx, states []*maple.SkyStrategyState) error {
		r.savedStrategies = append(r.savedStrategies, states...)
		return nil
	}
	r.SaveSyrupGlobalStateFn = func(_ context.Context, _ pgx.Tx, state *maple.SyrupGlobalState) error {
		r.savedGlobals = append(r.savedGlobals, state)
		return nil
	}
	return r
}

func (m *mockRepo) GetMapleProtocolID(ctx context.Context, chainID int64) (int64, error) {
	return m.GetMapleProtocolIDFn(ctx, chainID)
}

func (m *mockRepo) UpsertPools(ctx context.Context, tx pgx.Tx, pools []*maple.Pool) (map[common.Address]int64, error) {
	return m.UpsertPoolsFn(ctx, tx, pools)
}

func (m *mockRepo) SavePoolStates(ctx context.Context, tx pgx.Tx, states []*maple.PoolState) error {
	return m.SavePoolStatesFn(ctx, tx, states)
}

func (m *mockRepo) UpsertLoans(ctx context.Context, tx pgx.Tx, loans []*maple.Loan) (map[common.Address]int64, error) {
	return m.UpsertLoansFn(ctx, tx, loans)
}

func (m *mockRepo) SaveLoanStates(ctx context.Context, tx pgx.Tx, states []*maple.LoanState) error {
	return m.SaveLoanStatesFn(ctx, tx, states)
}

func (m *mockRepo) SaveLoanCollaterals(ctx context.Context, tx pgx.Tx, collaterals []*maple.LoanCollateral) error {
	return m.SaveLoanCollateralsFn(ctx, tx, collaterals)
}

func (m *mockRepo) UpsertFixedTermLoans(ctx context.Context, tx pgx.Tx, loans []*maple.FTLLoan) (map[common.Address]int64, error) {
	return m.UpsertFixedTermLoansFn(ctx, tx, loans)
}

func (m *mockRepo) SaveFixedTermLoanStates(ctx context.Context, tx pgx.Tx, states []*maple.FTLLoanState) error {
	return m.SaveFTLLoanStatesFn(ctx, tx, states)
}

func (m *mockRepo) UpsertSkyStrategies(ctx context.Context, tx pgx.Tx, strategies []*maple.SkyStrategy) (map[common.Address]int64, error) {
	return m.UpsertSkyStrategiesFn(ctx, tx, strategies)
}

func (m *mockRepo) SaveSkyStrategyStates(ctx context.Context, tx pgx.Tx, states []*maple.SkyStrategyState) error {
	return m.SaveSkyStrategyStatesFn(ctx, tx, states)
}

func (m *mockRepo) SaveSyrupGlobalState(ctx context.Context, tx pgx.Tx, state *maple.SyrupGlobalState) error {
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

// fixtureFTLLoans returns two live fixed-term loans sharing one borrower (to
// exercise dedup) and one funded in the same asset it is collateralized in.
func fixtureFTLLoans() []outbound.MapleFixedTermLoan {
	wbtc := outbound.MapleAssetToken{Address: addr(0xc0), Symbol: "WBTC", Decimals: 8}
	usdc := outbound.MapleAssetToken{Address: addr(0xf0), Symbol: "USDC", Decimals: 6}
	return []outbound.MapleFixedTermLoan{
		{
			LoanID: addr(0x70), Borrower: addr(0xb0), PoolAddress: addr(0x10),
			Collateral: wbtc, Funds: usdc, State: "Active", StateDetail: "ActiveInArrears",
			PrincipalOwed: big.NewInt(10000000000000), InterestRate: big.NewInt(182000), InterestPaid: big.NewInt(5000),
			PaymentsRemaining: 6, PaymentIntervalDays: 30, TermDays: 180,
			MaturityDate: 1662393177, NextPaymentDue: 1659801177,
			CollateralAmount: big.NewInt(21510), CollateralRequired: big.NewInt(20000), CollateralRatio: big.NewInt(1500000),
			DrawdownAmount: big.NewInt(16917002739727), ClaimableAmount: big.NewInt(0),
			AcmRatio: big.NewInt(1445731), IsImpaired: false,
		},
		{
			// Pre-funding loan: same borrower (deduped), zero amounts/dates,
			// null acmRatio and empty stateDetail.
			LoanID: addr(0x71), Borrower: addr(0xb0), PoolAddress: addr(0x11),
			Collateral: usdc, Funds: usdc, State: "WaitingForAcceptance", StateDetail: "",
			PrincipalOwed: big.NewInt(0), InterestRate: big.NewInt(0), InterestPaid: big.NewInt(0),
			PaymentsRemaining: 0, PaymentIntervalDays: 0, TermDays: 0,
			MaturityDate: 0, NextPaymentDue: 0,
			CollateralAmount: big.NewInt(0), CollateralRequired: big.NewInt(0), CollateralRatio: big.NewInt(0),
			DrawdownAmount: big.NewInt(0), ClaimableAmount: big.NewInt(0),
			AcmRatio: nil, IsImpaired: false,
		},
	}
}

func happyClient() *mockClient {
	return &mockClient{
		GetPoolsFn:       func(context.Context) ([]outbound.MaplePool, error) { return fixturePools(), nil },
		GetActiveLoansFn: func(context.Context) ([]outbound.MapleActiveLoan, error) { return fixtureLoans(), nil },
		// The FTL book is dormant by default; FTL-specific tests opt in to
		// fixtureFTLLoans. This keeps the shared token/borrower mock-call
		// assertions of the OTL tests unaffected.
		GetActiveFixedTermLoansFn: func(context.Context) ([]outbound.MapleFixedTermLoan, error) { return nil, nil },
		GetSkyStrategiesFn:        func(context.Context) ([]outbound.MapleSkyStrategy, error) { return fixtureStrategies(), nil },
		GetSyrupGlobalsFn:         func(context.Context) (*outbound.MapleSyrupGlobals, error) { return fixtureGlobals(), nil },
	}
}

func newTestService(t *testing.T, client *mockClient, repo *mockRepo) *Service {
	t.Helper()
	service, err := NewService(ServiceConfig{ChainID: 1}, client, repo, repo.tokenRepo, repo.userRepo, &testutil.MockTxManager{}, nil)
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
	tokenRepo := repo.tokenRepo
	userRepo := repo.userRepo
	txm := &testutil.MockTxManager{}

	tests := []struct {
		name    string
		run     func() (*Service, error)
		wantErr string
	}{
		{
			name: "nil client",
			run: func() (*Service, error) {
				return NewService(ServiceConfig{ChainID: 1}, nil, repo, tokenRepo, userRepo, txm, nil)
			},
			wantErr: "client cannot be nil",
		},
		{
			name: "nil repo",
			run: func() (*Service, error) {
				return NewService(ServiceConfig{ChainID: 1}, client, nil, tokenRepo, userRepo, txm, nil)
			},
			wantErr: "repo cannot be nil",
		},
		{
			name: "nil token repo",
			run: func() (*Service, error) {
				return NewService(ServiceConfig{ChainID: 1}, client, repo, nil, userRepo, txm, nil)
			},
			wantErr: "tokenRepo cannot be nil",
		},
		{
			name: "nil user repo",
			run: func() (*Service, error) {
				return NewService(ServiceConfig{ChainID: 1}, client, repo, tokenRepo, nil, txm, nil)
			},
			wantErr: "userRepo cannot be nil",
		},
		{
			name: "nil tx manager",
			run: func() (*Service, error) {
				return NewService(ServiceConfig{ChainID: 1}, client, repo, tokenRepo, userRepo, nil, nil)
			},
			wantErr: "txManager cannot be nil",
		},
		{
			name: "zero chain id",
			run: func() (*Service, error) {
				return NewService(ServiceConfig{ChainID: 0}, client, repo, tokenRepo, userRepo, txm, nil)
			},
			wantErr: "chainID must be 1",
		},
		{
			name: "non-mainnet chain id",
			run: func() (*Service, error) {
				return NewService(ServiceConfig{ChainID: 137}, client, repo, tokenRepo, userRepo, txm, nil)
			},
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

	service, err := NewService(ServiceConfig{ChainID: 1}, client, repo, tokenRepo, userRepo, txm, nil)
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
		want, ok := wantPrincipalByLoanID[s.LoanID]
		if !ok {
			t.Errorf("unexpected loan id %d", s.LoanID)
			continue
		}
		if s.PrincipalOwed.Int64() != want {
			t.Errorf("loan %d principal = %s, want %d", s.LoanID, s.PrincipalOwed, want)
		}
		// Only the uncollateralized loan (0x21 -> id 21) has nil AcmRatio.
		if (s.AcmRatio == nil) != (s.LoanID == 21) {
			t.Errorf("loan %d AcmRatio nil-ness wrong: %v", s.LoanID, s.AcmRatio)
		}
	}
	wantCollateralByLoanID := map[int64]string{20: "BTC", 22: "SOL"}
	for _, c := range repo.savedCollats {
		if want := wantCollateralByLoanID[c.LoanID]; c.AssetSymbol != want {
			t.Errorf("collateral for loan %d = %q, want %q", c.LoanID, c.AssetSymbol, want)
		}
	}
	// Pool states pair with their pool ids (10, 11 in fixture order).
	if repo.savedPoolStates[0].PoolID != 10 || repo.savedPoolStates[0].TVL.Int64() != 1000 {
		t.Errorf("pool state 0 = id %d tvl %s, want id 10 tvl 1000",
			repo.savedPoolStates[0].PoolID, repo.savedPoolStates[0].TVL)
	}
	if repo.savedPoolStates[1].PoolID != 11 || repo.savedPoolStates[1].TVL.Int64() != 0 {
		t.Errorf("pool state 1 = id %d tvl %s, want id 11 tvl 0",
			repo.savedPoolStates[1].PoolID, repo.savedPoolStates[1].TVL)
	}

	// Loan meta maps field-for-field into the registry entity; the loan
	// without API meta keeps a nil meta.
	if len(repo.upsertedLoans) != 3 {
		t.Fatalf("upserted loans = %d, want 3", len(repo.upsertedLoans))
	}
	metaByAddr := map[common.Address]*maple.LoanMeta{}
	for _, l := range repo.upsertedLoans {
		metaByAddr[common.BytesToAddress(l.LoanAddress)] = l.LoanMeta
	}
	wantMeta := maple.LoanMeta{Type: "amm", DexName: "Uniswap"}
	if got := metaByAddr[addr(0x20)]; got == nil || *got != wantMeta {
		t.Errorf("loan 0x20 meta = %+v, want %+v", got, wantMeta)
	}
	if got := metaByAddr[addr(0x21)]; got != nil {
		t.Errorf("loan 0x21 meta = %+v, want nil", got)
	}

	// Asset token resolution: one call with the two distinct pool assets, and
	// each pool entity carries the token id mapped from ITS asset address
	// (mock assigns 500, 501 in fixture order).
	if len(repo.assetTokenCalls) != 1 {
		t.Fatalf("asset token calls = %d, want 1", len(repo.assetTokenCalls))
	}
	gotAssets := repo.assetTokenCalls[0]
	if len(gotAssets) != 2 || gotAssets[0].Address != addr(0xee) || gotAssets[1].Address != addr(0xef) {
		t.Errorf("assets = %v, want [0xee..., 0xef...]", gotAssets)
	}
	if gotAssets[0].Symbol != "USDC" || gotAssets[0].Decimals != 6 {
		t.Errorf("asset 0 = %+v, want USDC/6", gotAssets[0])
	}
	// GraphQL data has no block context: the block must be nil so the shared
	// upsert preserves any existing on-chain block.
	if gotAssets[0].CreatedAtBlock != nil || gotAssets[1].CreatedAtBlock != nil {
		t.Errorf("asset CreatedAtBlock = %v/%v, want nil/nil", gotAssets[0].CreatedAtBlock, gotAssets[1].CreatedAtBlock)
	}
	if len(repo.upsertedPools) != 2 {
		t.Fatalf("upserted pools = %d, want 2", len(repo.upsertedPools))
	}
	if repo.upsertedPools[0].AssetTokenID != 500 || repo.upsertedPools[1].AssetTokenID != 501 {
		t.Errorf("pool asset token ids = %d/%d, want 500/501",
			repo.upsertedPools[0].AssetTokenID, repo.upsertedPools[1].AssetTokenID)
	}

	// Borrower resolution: one call, two distinct borrowers (0xa0 deduped).
	if len(repo.borrowerCalls) != 1 {
		t.Fatalf("borrower calls = %d, want 1", len(repo.borrowerCalls))
	}
	if got := repo.borrowerCalls[0]; len(got) != 2 || got[0].Address != addr(0xa0) || got[1].Address != addr(0xa1) {
		t.Errorf("borrowers = %v, want [0xa0..., 0xa1...]", got)
	}
	// Borrowers carry no block context: FirstSeenBlock must be nil.
	if got := repo.borrowerCalls[0]; got[0].FirstSeenBlock != nil || got[1].FirstSeenBlock != nil {
		t.Errorf("borrower FirstSeenBlock = %v/%v, want nil/nil", got[0].FirstSeenBlock, got[1].FirstSeenBlock)
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

func TestSyncAt_NormalizesTimestamp(t *testing.T) {
	// SyncAt's contract: any zoned, sub-second timestamp is normalized to
	// UTC second precision before it becomes the cycle's synced_at.
	repo := newMockRepo()
	service := newTestService(t, happyClient(), repo)

	in := time.Date(2026, 6, 10, 12, 30, 45, 999999999, time.FixedZone("UTC+2", 2*3600))
	want := time.Date(2026, 6, 10, 10, 30, 45, 0, time.UTC)

	if err := service.SyncAt(context.Background(), in); err != nil {
		t.Fatalf("SyncAt: %v", err)
	}
	if len(repo.savedPoolStates) == 0 {
		t.Fatal("no pool states saved")
	}
	for _, s := range repo.savedPoolStates {
		if !s.SyncedAt.Equal(want) {
			t.Errorf("pool state syncedAt = %v, want %v", s.SyncedAt, want)
		}
	}
	if !repo.savedGlobals[0].SyncedAt.Equal(want) {
		t.Errorf("globals syncedAt = %v, want %v", repo.savedGlobals[0].SyncedAt, want)
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

func TestSync_TwoIndependentPhaseFailuresJoinBoth(t *testing.T) {
	client := happyClient()
	client.GetActiveLoansFn = func(context.Context) ([]outbound.MapleActiveLoan, error) {
		return nil, errors.New("loans exploded")
	}
	client.GetSyrupGlobalsFn = func(context.Context) (*outbound.MapleSyrupGlobals, error) {
		return nil, errors.New("globals exploded")
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "loans exploded") || !strings.Contains(err.Error(), "globals exploded") {
		t.Errorf("error %q should carry both independent phase failures", err.Error())
	}
	if len(repo.savedPoolStates) != 2 || len(repo.savedStrategies) != 1 {
		t.Errorf("healthy phases must run: pools=%d strategies=%d",
			len(repo.savedPoolStates), len(repo.savedStrategies))
	}
}

func TestSync_CtxCancelledDuringPoolsAbortsCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := happyClient()
	client.GetPoolsFn = func(ctx context.Context) ([]outbound.MaplePool, error) {
		cancel()
		return nil, ctx.Err()
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v should be context.Canceled", err)
	}
	if !strings.Contains(err.Error(), "aborting sync cycle after pools phase") {
		t.Errorf("error %q should mark the cycle as aborted after pools", err.Error())
	}
	if client.activeLoansCalled || client.strategiesCalled || client.globalsCalled {
		t.Error("no later phase may run against a cancelled context")
	}
}

func TestSync_CtxCancelledDuringLoansAbortsCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := happyClient()
	client.GetActiveLoansFn = func(ctx context.Context) ([]outbound.MapleActiveLoan, error) {
		cancel()
		return nil, ctx.Err()
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v should be context.Canceled", err)
	}
	if !strings.Contains(err.Error(), "aborting sync cycle after loans phase") {
		t.Errorf("error %q should mark the cycle as aborted after loans", err.Error())
	}
	if client.strategiesCalled || client.globalsCalled {
		t.Error("strategies/globals must not run against a cancelled context")
	}
	if len(repo.savedPoolStates) != 2 {
		t.Errorf("pool states = %d, want 2 (pools phase completed before cancellation)", len(repo.savedPoolStates))
	}
}

func TestSync_CtxCancelledDuringStrategiesAbortsCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := happyClient()
	client.GetSkyStrategiesFn = func(ctx context.Context) ([]outbound.MapleSkyStrategy, error) {
		cancel()
		return nil, ctx.Err()
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v should be context.Canceled", err)
	}
	if !strings.Contains(err.Error(), "aborting sync cycle after sky strategies phase") {
		t.Errorf("error %q should mark the cycle as aborted after sky strategies", err.Error())
	}
	if client.globalsCalled {
		t.Error("globals must not run against a cancelled context")
	}
	if len(repo.savedPoolStates) != 2 || len(repo.savedLoanStates) != 3 {
		t.Errorf("earlier phases must have completed: pools=%d loans=%d",
			len(repo.savedPoolStates), len(repo.savedLoanStates))
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
	repo.userRepo.GetOrCreateUsersFn = func(context.Context, pgx.Tx, []entity.User) (map[common.Address]int64, error) {
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
	repo.UpsertLoansFn = func(context.Context, pgx.Tx, []*maple.Loan) (map[common.Address]int64, error) {
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

func TestSync_PoolMissingFromUpsertResult(t *testing.T) {
	client := happyClient()
	repo := newMockRepo()
	repo.UpsertPoolsFn = func(context.Context, pgx.Tx, []*maple.Pool) (map[common.Address]int64, error) {
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

func TestSync_StrategyMissingFromUpsertResult(t *testing.T) {
	client := happyClient()
	repo := newMockRepo()
	repo.UpsertSkyStrategiesFn = func(context.Context, pgx.Tx, []*maple.SkyStrategy) (map[common.Address]int64, error) {
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

func TestSync_EmptyAssetSymbolFailsPhase(t *testing.T) {
	client := happyClient()
	client.GetPoolsFn = func(context.Context) ([]outbound.MaplePool, error) {
		pools := fixturePools()
		pools[0].AssetSymbol = ""
		return pools, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "symbol must not be empty") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_SharedAssetIsDeduplicated(t *testing.T) {
	// Two pools denominated in the same asset must resolve one token, and
	// both pool entities must reference it.
	client := happyClient()
	client.GetPoolsFn = func(context.Context) ([]outbound.MaplePool, error) {
		pools := fixturePools()
		pools[1].AssetAddress = pools[0].AssetAddress
		pools[1].AssetSymbol = pools[0].AssetSymbol
		pools[1].AssetDecimals = pools[0].AssetDecimals
		return pools, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	if err := service.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if len(repo.assetTokenCalls) != 1 || len(repo.assetTokenCalls[0]) != 1 {
		t.Fatalf("asset token calls = %v, want one call with one asset", repo.assetTokenCalls)
	}
	if len(repo.upsertedPools) != 2 {
		t.Fatalf("upserted pools = %d, want 2", len(repo.upsertedPools))
	}
	if repo.upsertedPools[0].AssetTokenID != 500 || repo.upsertedPools[1].AssetTokenID != 500 {
		t.Errorf("pool asset token ids = %d/%d, want 500/500",
			repo.upsertedPools[0].AssetTokenID, repo.upsertedPools[1].AssetTokenID)
	}
}

func TestSync_ConflictingAssetMetadataFails(t *testing.T) {
	// The same asset address reported with different symbol or decimals is an
	// API inconsistency the token table cannot represent; first-write-wins
	// would silently persist one of the two.
	client := happyClient()
	client.GetPoolsFn = func(context.Context) ([]outbound.MaplePool, error) {
		pools := fixturePools()
		pools[1].AssetAddress = pools[0].AssetAddress
		pools[1].AssetSymbol = "USDT" // differs from pool 0's USDC
		return pools, nil
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "conflicting metadata") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_InvalidPoolEntityFailsPhase(t *testing.T) {
	// A zero token id resolves from the map but fails pool entity validation.
	client := happyClient()
	repo := newMockRepo()
	repo.tokenRepo.GetOrCreateTokensFn = func(_ context.Context, _ pgx.Tx, tokens []outbound.TokenInput) (map[common.Address]int64, error) {
		ids := make(map[common.Address]int64, len(tokens))
		for _, t := range tokens {
			ids[t.Address] = 0
		}
		return ids, nil
	}
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "assetTokenID must be positive") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSync_AssetTokenMissingFromUpsertResult(t *testing.T) {
	client := happyClient()
	repo := newMockRepo()
	repo.tokenRepo.GetOrCreateTokensFn = func(context.Context, pgx.Tx, []outbound.TokenInput) (map[common.Address]int64, error) {
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
				r.SavePoolStatesFn = func(context.Context, pgx.Tx, []*maple.PoolState) error {
					return errors.New("save failed")
				}
			},
		},
		{
			name: "loan states save fails",
			mutate: func(r *mockRepo) {
				r.SaveLoanStatesFn = func(context.Context, pgx.Tx, []*maple.LoanState) error {
					return errors.New("save failed")
				}
			},
		},
		{
			name: "loan collaterals save fails",
			mutate: func(r *mockRepo) {
				r.SaveLoanCollateralsFn = func(context.Context, pgx.Tx, []*maple.LoanCollateral) error {
					return errors.New("save failed")
				}
			},
		},
		{
			name: "strategy states save fails",
			mutate: func(r *mockRepo) {
				r.SaveSkyStrategyStatesFn = func(context.Context, pgx.Tx, []*maple.SkyStrategyState) error {
					return errors.New("save failed")
				}
			},
		},
		{
			name: "globals save fails",
			mutate: func(r *mockRepo) {
				r.SaveSyrupGlobalStateFn = func(context.Context, pgx.Tx, *maple.SyrupGlobalState) error {
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
				r.UpsertPoolsFn = func(context.Context, pgx.Tx, []*maple.Pool) (map[common.Address]int64, error) {
					return nil, errors.New("upsert failed")
				}
			},
		},
		{
			name: "asset token upsert fails",
			mutate: func(r *mockRepo) {
				r.tokenRepo.GetOrCreateTokensFn = func(context.Context, pgx.Tx, []outbound.TokenInput) (map[common.Address]int64, error) {
					return nil, errors.New("upsert failed")
				}
			},
		},
		{
			name: "borrower upsert fails",
			mutate: func(r *mockRepo) {
				r.userRepo.GetOrCreateUsersFn = func(context.Context, pgx.Tx, []entity.User) (map[common.Address]int64, error) {
					return nil, errors.New("upsert failed")
				}
			},
		},
		{
			name: "loan upsert fails",
			mutate: func(r *mockRepo) {
				r.UpsertLoansFn = func(context.Context, pgx.Tx, []*maple.Loan) (map[common.Address]int64, error) {
					return nil, errors.New("upsert failed")
				}
			},
		},
		{
			name: "strategy upsert fails",
			mutate: func(r *mockRepo) {
				r.UpsertSkyStrategiesFn = func(context.Context, pgx.Tx, []*maple.SkyStrategy) (map[common.Address]int64, error) {
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
	// plausible small value (65542 would wrap to 6).
	for _, tc := range []struct {
		name     string
		decimals int
		wantErr  bool
	}{
		{name: "wraps to plausible value", decimals: 65542, wantErr: true},
		{name: "one past max int16", decimals: 32768, wantErr: true},
		{name: "negative", decimals: -1, wantErr: true},
		{name: "max int16 is valid", decimals: 32767, wantErr: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := happyClient()
			client.GetPoolsFn = func(context.Context) ([]outbound.MaplePool, error) {
				pools := fixturePools()
				pools[0].AssetDecimals = tc.decimals
				return pools, nil
			}
			repo := newMockRepo()
			service := newTestService(t, client, repo)

			err := service.Sync(context.Background())
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "out of int16 range") {
				t.Errorf("error = %q", err.Error())
			}
		})
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
	repo.userRepo.GetOrCreateUsersFn = func(_ context.Context, _ pgx.Tx, users []entity.User) (map[common.Address]int64, error) {
		ids := make(map[common.Address]int64, len(users))
		for _, u := range users {
			ids[u.Address] = 0
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
// Fixed-term loans phase
// ---------------------------------------------------------------------------

// ftlHappyClient is happyClient with a live (non-dormant) FTL book.
func ftlHappyClient() *mockClient {
	c := happyClient()
	c.GetActiveFixedTermLoansFn = func(context.Context) ([]outbound.MapleFixedTermLoan, error) {
		return fixtureFTLLoans(), nil
	}
	return c
}

func TestSync_FixedTermLoansHappyPath(t *testing.T) {
	client := ftlHappyClient()
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	if err := service.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	if len(repo.upsertedFTLLoans) != 2 {
		t.Fatalf("upserted ftl loans = %d, want 2", len(repo.upsertedFTLLoans))
	}
	if len(repo.savedFTLStates) != 2 {
		t.Fatalf("ftl states = %d, want 2", len(repo.savedFTLStates))
	}

	// Token resolution: the FTL phase resolves WBTC (0xc0) and USDC (0xf0); the
	// second loan reuses USDC for both collateral and funds, so only two distinct
	// tokens are requested. The FTL call is the second GetOrCreateTokens call
	// (the pools phase made the first).
	if len(repo.assetTokenCalls) != 2 {
		t.Fatalf("asset token calls = %d, want 2 (pools + ftl)", len(repo.assetTokenCalls))
	}
	ftlAssets := repo.assetTokenCalls[1]
	if len(ftlAssets) != 2 || ftlAssets[0].Address != addr(0xc0) || ftlAssets[1].Address != addr(0xf0) {
		t.Errorf("ftl assets = %v, want [0xc0..., 0xf0...]", ftlAssets)
	}
	if ftlAssets[0].Symbol != "WBTC" || ftlAssets[0].Decimals != 8 || ftlAssets[0].CreatedAtBlock != nil {
		t.Errorf("ftl collateral asset = %+v, want WBTC/8 with nil block", ftlAssets[0])
	}

	// Borrower resolution: both loans share borrower 0xb0, deduped to one. The
	// FTL call is the second GetOrCreateUsers call (loans phase made the first).
	if len(repo.borrowerCalls) != 2 {
		t.Fatalf("borrower calls = %d, want 2 (loans + ftl)", len(repo.borrowerCalls))
	}
	if ftlBorrowers := repo.borrowerCalls[1]; len(ftlBorrowers) != 1 || ftlBorrowers[0].Address != addr(0xb0) {
		t.Errorf("ftl borrowers = %v, want [0xb0...]", repo.borrowerCalls[1])
	}

	// Each registry entity carries the ids mapped from ITS pool/borrower/tokens.
	entByAddr := map[common.Address]*maple.FTLLoan{}
	for _, l := range repo.upsertedFTLLoans {
		entByAddr[common.BytesToAddress(l.LoanAddress)] = l
	}
	first := entByAddr[addr(0x70)]
	if first == nil || first.CollateralTokenID != 500 || first.FundsTokenID != 501 || first.BorrowerUserID != 100 {
		t.Errorf("loan 0x70 entity = %+v, want collateral 500 funds 501 borrower 100", first)
	}
	second := entByAddr[addr(0x71)]
	// fundsAsset == collateralAsset (USDC) for the second loan: same token id is valid.
	if second == nil || second.CollateralTokenID != 501 || second.FundsTokenID != 501 {
		t.Errorf("loan 0x71 entity = %+v, want collateral 501 funds 501", second)
	}

	// State rows: epoch 0 maps to nil dates, null acmRatio/empty stateDetail stay
	// null; non-zero epochs convert to UTC instants.
	stateByID := map[int64]*maple.FTLLoanState{}
	for _, s := range repo.savedFTLStates {
		stateByID[s.LoanID] = s
	}
	funded := stateByID[40] // loan 0x70 -> id 40
	if funded == nil || funded.MaturityDate == nil || !funded.MaturityDate.Equal(time.Unix(1662393177, 0).UTC()) {
		t.Errorf("funded loan maturity = %v, want %v", funded.MaturityDate, time.Unix(1662393177, 0).UTC())
	}
	if funded.AcmRatio == nil || funded.AcmRatio.Int64() != 1445731 || funded.StateDetail != "ActiveInArrears" {
		t.Errorf("funded loan acm/detail = %v/%q", funded.AcmRatio, funded.StateDetail)
	}
	pending := stateByID[41] // loan 0x71 -> id 41
	if pending == nil || pending.MaturityDate != nil || pending.NextPaymentDue != nil {
		t.Errorf("pending loan dates = %v/%v, want nil/nil", pending.MaturityDate, pending.NextPaymentDue)
	}
	if pending.AcmRatio != nil || pending.StateDetail != "" {
		t.Errorf("pending loan acm/detail = %v/%q, want nil/empty", pending.AcmRatio, pending.StateDetail)
	}

	// synced_at is shared (and second-truncated) with the rest of the cycle.
	wantSyncedAt := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	for _, s := range repo.savedFTLStates {
		if !s.SyncedAt.Equal(wantSyncedAt) {
			t.Errorf("ftl state syncedAt = %v, want %v", s.SyncedAt, wantSyncedAt)
		}
	}
}

func TestSync_FixedTermLoansEmptyIsInfoNotError(t *testing.T) {
	// The dormant FTL book (empty) must succeed without resolving any borrowers
	// or tokens for the FTL phase.
	client := happyClient() // FTL empty by default
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	if err := service.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if !client.ftlLoansCalled {
		t.Error("FTL fetch must still run")
	}
	if len(repo.upsertedFTLLoans) != 0 || len(repo.savedFTLStates) != 0 {
		t.Errorf("ftl writes = %d/%d, want 0/0 for the dormant book", len(repo.upsertedFTLLoans), len(repo.savedFTLStates))
	}
	// Only the pools and loans phases resolve tokens/borrowers; the empty FTL
	// phase adds no calls.
	if len(repo.assetTokenCalls) != 1 || len(repo.borrowerCalls) != 1 {
		t.Errorf("token/borrower calls = %d/%d, want 1/1 (FTL empty adds none)", len(repo.assetTokenCalls), len(repo.borrowerCalls))
	}
}

func TestSync_FixedTermLoanReferencingUnknownPool(t *testing.T) {
	client := ftlHappyClient()
	client.GetActiveFixedTermLoansFn = func(context.Context) ([]outbound.MapleFixedTermLoan, error) {
		loans := fixtureFTLLoans()
		loans[0].PoolAddress = addr(0x99) // not in the pool registry
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
	if len(repo.savedFTLStates) != 0 {
		t.Errorf("ftl states = %d, want 0 (snapshot is all-or-nothing)", len(repo.savedFTLStates))
	}
}

func TestSync_FixedTermLoansFailAlone(t *testing.T) {
	client := ftlHappyClient()
	client.GetActiveFixedTermLoansFn = func(context.Context) ([]outbound.MapleFixedTermLoan, error) {
		return nil, errors.New("ftl exploded")
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "ftl exploded") {
		t.Errorf("error %q should contain the ftl failure", err.Error())
	}
	// Every other phase still ran.
	if len(repo.savedPoolStates) != 2 || len(repo.savedLoanStates) != 3 ||
		len(repo.savedStrategies) != 1 || len(repo.savedGlobals) != 1 {
		t.Errorf("other phases must run: pools=%d loans=%d strategies=%d globals=%d",
			len(repo.savedPoolStates), len(repo.savedLoanStates), len(repo.savedStrategies), len(repo.savedGlobals))
	}
}

func TestSync_FixedTermLoansSkippedWhenPoolsFail(t *testing.T) {
	client := ftlHappyClient()
	client.GetPoolsFn = func(context.Context) ([]outbound.MaplePool, error) {
		return nil, errors.New("pools exploded")
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "skipping fixed-term loans") {
		t.Errorf("error %q should mark fixed-term loans as skipped", err.Error())
	}
	if client.ftlLoansCalled {
		t.Error("FTL must not be fetched when pools fail")
	}
}

func TestSync_CtxCancelledDuringFTLAbortsCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := ftlHappyClient()
	client.GetActiveFixedTermLoansFn = func(ctx context.Context) ([]outbound.MapleFixedTermLoan, error) {
		cancel()
		return nil, ctx.Err()
	}
	repo := newMockRepo()
	service := newTestService(t, client, repo)

	err := service.Sync(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v should be context.Canceled", err)
	}
	if !strings.Contains(err.Error(), "aborting sync cycle after fixed-term loans phase") {
		t.Errorf("error %q should mark the cycle as aborted after fixed-term loans", err.Error())
	}
	if client.strategiesCalled || client.globalsCalled {
		t.Error("strategies/globals must not run against a cancelled context")
	}
}

func TestSync_FixedTermLoanErrorsPropagate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(r *mockRepo)
	}{
		{
			name: "ftl upsert fails",
			mutate: func(r *mockRepo) {
				r.UpsertFixedTermLoansFn = func(context.Context, pgx.Tx, []*maple.FTLLoan) (map[common.Address]int64, error) {
					return nil, errors.New("ftl upsert failed")
				}
			},
		},
		{
			name: "ftl state save fails",
			mutate: func(r *mockRepo) {
				r.SaveFTLLoanStatesFn = func(context.Context, pgx.Tx, []*maple.FTLLoanState) error {
					return errors.New("ftl save failed")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newMockRepo()
			tt.mutate(repo)
			service := newTestService(t, ftlHappyClient(), repo)

			err := service.Sync(context.Background())
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "failed") {
				t.Errorf("error = %q", err.Error())
			}
		})
	}
}

func TestSync_FixedTermLoanResolutionFailures(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(r *mockRepo)
		wantErr string
	}{
		{
			name: "asset token missing from upsert result",
			mutate: func(r *mockRepo) {
				r.tokenRepo.GetOrCreateTokensFn = func(context.Context, pgx.Tx, []outbound.TokenInput) (map[common.Address]int64, error) {
					return map[common.Address]int64{}, nil
				}
			},
			wantErr: "missing from upsert result",
		},
		{
			name: "borrower missing from upsert result",
			mutate: func(r *mockRepo) {
				r.userRepo.GetOrCreateUsersFn = func(context.Context, pgx.Tx, []entity.User) (map[common.Address]int64, error) {
					return map[common.Address]int64{}, nil
				}
			},
			wantErr: "missing from upsert result",
		},
		{
			name: "zero token id fails entity validation",
			mutate: func(r *mockRepo) {
				r.tokenRepo.GetOrCreateTokensFn = func(_ context.Context, _ pgx.Tx, tokens []outbound.TokenInput) (map[common.Address]int64, error) {
					ids := make(map[common.Address]int64, len(tokens))
					for _, tok := range tokens {
						ids[tok.Address] = 0
					}
					return ids, nil
				}
			},
			wantErr: "must be positive",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newMockRepo()
			tt.mutate(repo)
			service := newTestService(t, ftlHappyClient(), repo)

			err := service.Sync(context.Background())
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestSync_FixedTermLoanInputValidationFailures(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(loans []outbound.MapleFixedTermLoan)
		wantErr string
	}{
		{
			name:    "duplicate loan id",
			mutate:  func(loans []outbound.MapleFixedTermLoan) { loans[1].LoanID = loans[0].LoanID },
			wantErr: "duplicate",
		},
		{
			name:    "empty asset symbol",
			mutate:  func(loans []outbound.MapleFixedTermLoan) { loans[0].Collateral.Symbol = "" },
			wantErr: "symbol must not be empty",
		},
		{
			name:    "decimals overflow",
			mutate:  func(loans []outbound.MapleFixedTermLoan) { loans[0].Collateral.Decimals = 65542 },
			wantErr: "out of int16 range",
		},
		{
			name: "conflicting asset metadata",
			mutate: func(loans []outbound.MapleFixedTermLoan) {
				// Same address as loan0's funds (USDC) but a different symbol.
				loans[1].Funds = outbound.MapleAssetToken{Address: addr(0xf0), Symbol: "USDT", Decimals: 6}
			},
			wantErr: "conflicting metadata",
		},
		{
			name:    "invalid loan state value",
			mutate:  func(loans []outbound.MapleFixedTermLoan) { loans[0].PrincipalOwed = nil },
			wantErr: "principalOwed must not be nil",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := ftlHappyClient()
			client.GetActiveFixedTermLoansFn = func(context.Context) ([]outbound.MapleFixedTermLoan, error) {
				loans := fixtureFTLLoans()
				tt.mutate(loans)
				return loans, nil
			}
			repo := newMockRepo()
			service := newTestService(t, client, repo)

			err := service.Sync(context.Background())
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}
