//go:build integration

package maple_graphql_indexer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/maple"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const (
	itPoolSyrup    = "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"
	itPoolPlain    = "0xc39a5a616f0ad1ff45077fa2de3f79ab8eb8b8b9"
	itUSDC         = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
	itLoanInternal = "0x0009bff1fcb8c767e5894164124d3e42aaca0542"
	itLoanExternal = "0x02cfceb3665d055953561f69c6b1cc475ab080d5"
	itLoanBare     = "0x29356f80d6016583c03991cda7dd42259517c005"
	itBorrowerA    = "0xfba4bc924ba50c3b3dd0c1aa6d2f499b4fa55c81"
	itBorrowerB    = "0xb32dd55d4ff63e39c304b00e069fbaefe885f0fb"
	itStrategy     = "0x859c9980931fa0a63765fd8ef2e29918af5b038c"
	itWBTC         = "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
	itFTLFunded    = "0x01e4533e02cb4da3578e8b329dcec2f7e53cdda3"
	itFTLPending   = "0x023db56966858d139fe6406ae927275490715a3a"
)

// newMapleAPIFixture serves realistic GraphQL responses mirroring live API
// shapes (string integers, JSON-number liquidationLevel/version, null
// acmRatio on the bare loan, loanMeta with null fields, null tvl and
// collateralValue on the plain pool).
func newMapleAPIFixture(t *testing.T) *httptest.Server {
	t.Helper()

	poolsJSON := `[
		{"id": "` + itPoolSyrup + `", "name": "Syrup USDC", "monthlyApy": "46314953537216910976747498327", "spotApy": "0",
		 "assets": "400", "collateralValue": "500", "principalOut": "600", "tvl": "1000",
		 "asset": {"id": "` + itUSDC + `", "symbol": "USDC", "decimals": 6},
		 "syrupRouter": {"id": "0x1234567890123456789012345678901234567890"}},
		{"id": "` + itPoolPlain + `", "name": "High Yield Secured Lending", "monthlyApy": null, "spotApy": null,
		 "assets": "0", "collateralValue": null, "principalOut": "0", "tvl": null,
		 "asset": {"id": "` + itUSDC + `", "symbol": "USDC", "decimals": 6},
		 "syrupRouter": null}
	]`

	loansJSON := `[
		{"id": "` + itLoanInternal + `", "borrower": {"id": "` + itBorrowerA + `"}, "state": "Active",
		 "principalOwed": "10000000000000", "acmRatio": "1000000",
		 "collateral": {"asset": "USDC", "assetAmount": "10000000000000", "assetValueUsd": "100000000",
		                "decimals": 6, "state": "Deposited", "custodian": null, "liquidationLevel": 900000},
		 "loanMeta": {"type": "amm", "assetSymbol": null, "dexName": "Uniswap", "location": null,
		              "walletAddress": "0x2570fAF7C8A0da87d3F123B35cC722EC3fCC3e08", "walletType": "EVM"},
		 "fundingPool": {"id": "` + itPoolSyrup + `"}},
		{"id": "` + itLoanExternal + `", "borrower": {"id": "` + itBorrowerB + `"}, "state": "Active",
		 "principalOwed": "7000000", "acmRatio": "1953569",
		 "collateral": {"asset": "SOL", "assetAmount": "215100000", "assetValueUsd": "6357500000",
		                "decimals": 9, "state": "Deposited", "custodian": "ANCHORAGE", "liquidationLevel": 1020000},
		 "loanMeta": null,
		 "fundingPool": {"id": "` + itPoolPlain + `"}},
		{"id": "` + itLoanBare + `", "borrower": {"id": "` + itBorrowerA + `"}, "state": "Active",
		 "principalOwed": "5932464850000", "acmRatio": null,
		 "collateral": null, "loanMeta": null,
		 "fundingPool": {"id": "` + itPoolPlain + `"}}
	]`

	ftlLoansJSON := `[
		{"id": "` + itFTLFunded + `", "borrower": {"id": "` + itBorrowerA + `"}, "fundingPool": {"id": "` + itPoolSyrup + `"},
		 "collateralAsset": {"id": "` + itWBTC + `", "symbol": "WBTC", "decimals": 8},
		 "liquidityAsset": {"id": "` + itUSDC + `", "symbol": "USDC", "decimals": 6},
		 "state": "Active", "stateDetail": "ActiveInArrears",
		 "principalOwed": "10000000000000", "interestRate": "182000", "interestPaid": "659786301366",
		 "paymentsRemaining": "6", "paymentIntervalDays": "30", "termDays": "180",
		 "maturityDate": "1662393177", "nextPaymentDue": "1659801177",
		 "collateralAmount": "21510", "collateralRequired": "20000", "collateralRatio": "1500000",
		 "drawdownAmount": "16917002739727", "claimableAmount": "0",
		 "acmRatio": "1445731", "isImpaired": true},
		{"id": "` + itFTLPending + `", "borrower": {"id": "` + itBorrowerB + `"}, "fundingPool": {"id": "` + itPoolPlain + `"},
		 "collateralAsset": {"id": "` + itUSDC + `", "symbol": "USDC", "decimals": 6},
		 "liquidityAsset": {"id": "` + itUSDC + `", "symbol": "USDC", "decimals": 6},
		 "state": "WaitingForAcceptance", "stateDetail": null,
		 "principalOwed": "0", "interestRate": "0", "interestPaid": "0",
		 "paymentsRemaining": "0", "paymentIntervalDays": "0", "termDays": "0",
		 "maturityDate": "0", "nextPaymentDue": "0",
		 "collateralAmount": "0", "collateralRequired": "0", "collateralRatio": "0",
		 "drawdownAmount": "0", "claimableAmount": "0",
		 "acmRatio": null, "isImpaired": false}
	]`

	strategiesJSON := `[
		{"id": "` + itStrategy + `", "state": "Active", "currentlyDeployed": "0",
		 "depositedAssets": "9464548714891221", "withdrawnAssets": "9474661204598509",
		 "strategyFeeRate": "100000", "totalFeesCollected": "1121557832133", "version": 100,
		 "pool": {"id": "` + itPoolSyrup + `", "name": "Syrup USDC"}}
	]`

	globalsJSON := `{"apy": "46314950526928033107296807949", "collateralApy": "11044228807145689488478201423",
	                 "poolApy": "35270730693993489373296467374", "dripsYieldBoost": "0", "tvl": "3563135115920200"}`

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decoding request: %v", err)
		}

		// Only the first page carries data; subsequent pages are empty.
		skip := 0
		if v, ok := req.Variables["skip"].(float64); ok {
			skip = int(v)
		}
		dataFor := func(field, payload string) string {
			if skip > 0 {
				payload = "[]"
			}
			return `{"data": {"` + field + `": ` + payload + `}}`
		}

		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(req.Query, "poolV2S"):
			_, _ = w.Write([]byte(dataFor("poolV2S", poolsJSON)))
		case strings.Contains(req.Query, "openTermLoans"):
			_, _ = w.Write([]byte(dataFor("openTermLoans", loansJSON)))
		case strings.Contains(req.Query, "GetFixedTermLoans"):
			_, _ = w.Write([]byte(dataFor("loans", ftlLoansJSON)))
		case strings.Contains(req.Query, "skyStrategies"):
			_, _ = w.Write([]byte(dataFor("skyStrategies", strategiesJSON)))
		case strings.Contains(req.Query, "syrupGlobals"):
			_, _ = w.Write([]byte(`{"data": {"syrupGlobals": ` + globalsJSON + `}}`))
		default:
			t.Errorf("unexpected query: %s", req.Query)
		}
	}))
}

func newIntegrationService(t *testing.T, pool *pgxpool.Pool, endpoint string) *Service {
	t.Helper()

	client, err := maple.NewClient(maple.Config{Endpoint: endpoint})
	if err != nil {
		t.Fatalf("maple.NewClient: %v", err)
	}
	repo, err := postgres.NewMapleGraphQLRepository(pool, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewMapleGraphQLRepository: %v", err)
	}
	tokenRepo, err := postgres.NewTokenRepository(pool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}
	userRepo, err := postgres.NewUserRepository(pool, nil, 0)
	if err != nil {
		t.Fatalf("NewUserRepository: %v", err)
	}
	txManager, err := postgres.NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	service, err := NewService(ServiceConfig{ChainID: 1}, client, repo, tokenRepo, userRepo, txManager, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	// Fixed clock so a second Sync produces the same synced_at and exercises
	// same-build idempotency end to end.
	service.now = func() time.Time {
		return time.Date(2026, 6, 10, 10, 0, 0, 500000000, time.UTC)
	}
	return service
}

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil {
		t.Fatalf("counting %s: %v", table, err)
	}
	return count
}

func TestSyncIntegration_FullCycle(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	server := newMapleAPIFixture(t)
	defer server.Close()

	service := newIntegrationService(t, pool, server.URL)

	if err := service.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	wantCounts := map[string]int{
		"maple_pool":               2,
		"maple_pool_state":         2,
		"maple_loan":               3,
		"maple_loan_state":         3,
		"maple_loan_collateral":    2, // bare loan has null collateral
		"maple_ftl_loan":           2,
		"maple_ftl_loan_state":     2,
		"maple_sky_strategy":       1,
		"maple_sky_strategy_state": 1,
		"maple_syrup_global_state": 1,
	}
	for table, want := range wantCounts {
		if got := countRows(t, ctx, pool, table); got != want {
			t.Errorf("%s rows = %d, want %d", table, got, want)
		}
	}

	// Two distinct borrowers across three loans.
	if got := countRows(t, ctx, pool, `"user"`); got != 2 {
		t.Errorf("user rows = %d, want 2", got)
	}

	// Both pools share USDC: one token row upserted (NULL created_at_block —
	// GraphQL has no block context), both pools FK it.
	var tokenCount, poolsOnToken int
	var tokenSymbol string
	var tokenCAB *int64
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM token WHERE chain_id = 1 AND address = decode($1, 'hex')`,
		strings.TrimPrefix(itUSDC, "0x")).Scan(&tokenCount); err != nil {
		t.Fatalf("counting usdc token: %v", err)
	}
	if tokenCount != 1 {
		t.Errorf("usdc token rows = %d, want 1 (shared asset must dedupe)", tokenCount)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(p.id), MIN(t.symbol), MIN(t.created_at_block)
		FROM maple_pool p JOIN token t ON t.id = p.asset_token_id
		WHERE t.address = decode($1, 'hex')`,
		strings.TrimPrefix(itUSDC, "0x")).Scan(&poolsOnToken, &tokenSymbol, &tokenCAB); err != nil {
		t.Fatalf("joining pools to token: %v", err)
	}
	if poolsOnToken != 2 || tokenSymbol != "USDC" {
		t.Errorf("pools on usdc token = %d/%s, want 2/USDC", poolsOnToken, tokenSymbol)
	}
	if tokenCAB != nil {
		t.Errorf("token created_at_block = %v, want NULL", *tokenCAB)
	}

	// is_internal derives from loanMeta type.
	var internalCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM maple_loan WHERE is_internal`).Scan(&internalCount); err != nil {
		t.Fatalf("counting internal loans: %v", err)
	}
	if internalCount != 1 {
		t.Errorf("internal loans = %d, want 1", internalCount)
	}

	// The bare loan persisted a NULL acm_ratio.
	var nullACMCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM maple_loan_state WHERE acm_ratio IS NULL`).Scan(&nullACMCount); err != nil {
		t.Fatalf("counting null acm: %v", err)
	}
	if nullACMCount != 1 {
		t.Errorf("null acm_ratio rows = %d, want 1", nullACMCount)
	}

	// The plain pool's schema-nullable tvl/collateralValue persisted as NULL
	// end-to-end (client -> service -> repo).
	var nullTVLCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM maple_pool_state WHERE tvl IS NULL AND collateral_value_usd IS NULL`,
	).Scan(&nullTVLCount); err != nil {
		t.Fatalf("counting null tvl pool states: %v", err)
	}
	if nullTVLCount != 1 {
		t.Errorf("null tvl/collateral_value_usd pool state rows = %d, want 1", nullTVLCount)
	}

	// State and collateral rows pair with the RIGHT loan: join back through
	// loan_address rather than trusting counts.
	var principal string
	if err := pool.QueryRow(ctx, `
		SELECT s.principal_owed::text
		FROM maple_loan_state s JOIN maple_loan l ON l.id = s.maple_loan_id
		WHERE l.loan_address = decode($1, 'hex')`,
		strings.TrimPrefix(itLoanInternal, "0x")).Scan(&principal); err != nil {
		t.Fatalf("joining loan state: %v", err)
	}
	if principal != "10000000000000" {
		t.Errorf("internal loan principal = %s, want 10000000000000", principal)
	}
	var collateralAsset string
	if err := pool.QueryRow(ctx, `
		SELECT c.asset_symbol
		FROM maple_loan_collateral c JOIN maple_loan l ON l.id = c.maple_loan_id
		WHERE l.loan_address = decode($1, 'hex')`,
		strings.TrimPrefix(itLoanExternal, "0x")).Scan(&collateralAsset); err != nil {
		t.Fatalf("joining collateral: %v", err)
	}
	if collateralAsset != "SOL" {
		t.Errorf("external loan collateral = %s, want SOL", collateralAsset)
	}

	// Fixed-term loans: both FK the token registry. The funded loan's WBTC
	// collateral became a token row; both loans' USDC reuses the shared row.
	var ftlOnWBTC int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM maple_ftl_loan f JOIN token t ON t.id = f.collateral_token_id
		WHERE t.address = decode($1, 'hex')`,
		strings.TrimPrefix(itWBTC, "0x")).Scan(&ftlOnWBTC); err != nil {
		t.Fatalf("joining ftl loan to wbtc token: %v", err)
	}
	if ftlOnWBTC != 1 {
		t.Errorf("ftl loans collateralized in WBTC = %d, want 1", ftlOnWBTC)
	}

	// The pending FTL loan persisted null stateDetail/acmRatio and NULL epoch
	// dates (sentinel 0), while reporting zero principal (a valid value).
	var stateDetail, acmRatio *string
	var maturityDate, nextPaymentDue *time.Time
	var principalOwed string
	if err := pool.QueryRow(ctx, `
		SELECT s.state_detail, s.acm_ratio::text, s.maturity_date, s.next_payment_due, s.principal_owed::text
		FROM maple_ftl_loan_state s JOIN maple_ftl_loan f ON f.id = s.maple_ftl_loan_id
		WHERE f.loan_address = decode($1, 'hex')`,
		strings.TrimPrefix(itFTLPending, "0x")).Scan(&stateDetail, &acmRatio, &maturityDate, &nextPaymentDue, &principalOwed); err != nil {
		t.Fatalf("joining pending ftl loan state: %v", err)
	}
	if stateDetail != nil || acmRatio != nil {
		t.Errorf("pending ftl state_detail/acm_ratio = %v/%v, want NULL/NULL", stateDetail, acmRatio)
	}
	if maturityDate != nil || nextPaymentDue != nil {
		t.Errorf("pending ftl dates = %v/%v, want NULL/NULL", maturityDate, nextPaymentDue)
	}
	if principalOwed != "0" {
		t.Errorf("pending ftl principal_owed = %s, want 0", principalOwed)
	}

	// The funded FTL loan's epoch dates converted to the expected UTC instants.
	var maturity time.Time
	if err := pool.QueryRow(ctx, `
		SELECT s.maturity_date
		FROM maple_ftl_loan_state s JOIN maple_ftl_loan f ON f.id = s.maple_ftl_loan_id
		WHERE f.loan_address = decode($1, 'hex')`,
		strings.TrimPrefix(itFTLFunded, "0x")).Scan(&maturity); err != nil {
		t.Fatalf("querying funded ftl maturity: %v", err)
	}
	if !maturity.Equal(time.Unix(1662393177, 0).UTC()) {
		t.Errorf("funded ftl maturity = %v, want %v", maturity, time.Unix(1662393177, 0).UTC())
	}

	// Every snapshot row of the cycle shares one synced_at.
	var distinctSyncedAt int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(DISTINCT synced_at) FROM (
			SELECT synced_at FROM maple_pool_state
			UNION ALL SELECT synced_at FROM maple_loan_state
			UNION ALL SELECT synced_at FROM maple_loan_collateral
			UNION ALL SELECT synced_at FROM maple_ftl_loan_state
			UNION ALL SELECT synced_at FROM maple_sky_strategy_state
			UNION ALL SELECT synced_at FROM maple_syrup_global_state
		) all_rows`).Scan(&distinctSyncedAt); err != nil {
		t.Fatalf("counting distinct synced_at: %v", err)
	}
	if distinctSyncedAt != 1 {
		t.Errorf("distinct synced_at = %d, want 1", distinctSyncedAt)
	}

	// Second run with the same build and synced_at must be a no-op
	// (trigger reuses the processing_version, conflicts dedupe).
	if err := service.Sync(ctx); err != nil {
		t.Fatalf("second Sync: %v", err)
	}
	for table, want := range wantCounts {
		if got := countRows(t, ctx, pool, table); got != want {
			t.Errorf("%s rows after retry = %d, want %d (idempotency)", table, got, want)
		}
	}
	var maxVersion int
	if err := pool.QueryRow(ctx, `SELECT MAX(processing_version) FROM maple_loan_state`).Scan(&maxVersion); err != nil {
		t.Fatalf("max processing_version: %v", err)
	}
	if maxVersion != 0 {
		t.Errorf("max processing_version = %d, want 0 (same build retry must not bump)", maxVersion)
	}
}

func TestSyncIntegration_PoolsPhaseFailsOthersIsolated(t *testing.T) {
	// Only the pools query breaks (null top-level collection). Phases run in
	// their own transactions, so the run must fail yet the independent globals
	// phase still commits its row, and the pool-dependent loans/strategies
	// phases (skipped because pools failed) leave their tables empty. A
	// regression to one transaction per cycle would wipe the globals row too.
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	globalsJSON := `{"apy": "46314950526928033107296807949", "collateralApy": "11044228807145689488478201423",
	                 "poolApy": "35270730693993489373296467374", "dripsYieldBoost": "0", "tvl": "3563135115920200"}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decoding request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(req.Query, "poolV2S"):
			// Null top-level collection: a hard error in the pools phase.
			_, _ = w.Write([]byte(`{"data": {"poolV2S": null}}`))
		case strings.Contains(req.Query, "syrupGlobals"):
			_, _ = w.Write([]byte(`{"data": {"syrupGlobals": ` + globalsJSON + `}}`))
		default:
			t.Errorf("unexpected query (loans/strategies should be skipped): %s", req.Query)
		}
	}))
	defer server.Close()

	service := newIntegrationService(t, pool, server.URL)

	if err := service.Sync(ctx); err == nil {
		t.Fatal("expected error from the failed pools phase, got nil")
	}

	// The independent globals phase committed its row.
	if got := countRows(t, ctx, pool, "maple_syrup_global_state"); got != 1 {
		t.Errorf("maple_syrup_global_state rows = %d, want 1 (globals phase is independent)", got)
	}
	// Pools failed and its dependents were skipped: all empty.
	for _, table := range []string{
		"maple_pool", "maple_pool_state", "maple_loan", "maple_loan_state",
		"maple_loan_collateral", "maple_ftl_loan", "maple_ftl_loan_state",
		"maple_sky_strategy", "maple_sky_strategy_state",
	} {
		if got := countRows(t, ctx, pool, table); got != 0 {
			t.Errorf("%s rows = %d, want 0", table, got)
		}
	}
}

func TestSyncIntegration_GraphQLErrorMarksRunFailed(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errors": [{"message": "INTROSPECTION_DISABLED"}]}`))
	}))
	defer server.Close()

	service := newIntegrationService(t, pool, server.URL)

	err := service.Sync(ctx)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "INTROSPECTION_DISABLED") {
		t.Errorf("error = %q", err.Error())
	}
	// Nothing persisted.
	for _, table := range []string{"maple_pool", "maple_pool_state", "maple_loan", "maple_syrup_global_state"} {
		if got := countRows(t, ctx, pool, table); got != 0 {
			t.Errorf("%s rows = %d, want 0", table, got)
		}
	}
}
