//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const onchainPriceDBName = "test_onchain_price"

var onchainPricePool *pgxpool.Pool

func init() {
	useFileDatabase(onchainPriceDBName, &onchainPricePool)
}

// retiredSourceFixture is one oracle pricing one token, enabled from enabledFrom and
// retired from retiredFrom, plus the repository under test.
type retiredSourceFixture struct {
	repo     *OnchainPriceRepository
	oracleID int64
	tokenID  int64
}

// newRetiredSourceFixture seeds a source that was enabled on enabledFrom and retired on
// retiredFrom, the VEC-549 shape: the toggle that used to destroy its own history.
func newRetiredSourceFixture(t *testing.T, ctx context.Context, oracleName, tokenAddr, enabledFrom, retiredFrom string) retiredSourceFixture {
	t.Helper()

	_, err := onchainPricePool.Exec(ctx, `TRUNCATE oracle, token, oracle_asset CASCADE`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}

	oracleID := testutil.SeedOracle(t, ctx, onchainPricePool, oracleName, oracleName, 1, "0x8105f69D9C41644c6A0803fDA7D03Aa70996cFD9")
	tokenID := testutil.SeedToken(t, ctx, onchainPricePool, 1, tokenAddr, "TKN", 18)
	testutil.SeedOracleAssetEffectiveFrom(t, ctx, onchainPricePool, oracleID, tokenID, enabledFrom)
	testutil.SetOracleAssetEnabled(t, ctx, onchainPricePool, oracleID, tokenID, false, retiredFrom, "VEC-597 test: source retired")

	repo, err := NewOnchainPriceRepository(onchainPricePool, nil, 0, 0)
	if err != nil {
		t.Fatalf("NewOnchainPriceRepository: %v", err)
	}
	return retiredSourceFixture{repo: repo, oracleID: oracleID, tokenID: tokenID}
}

func mustDate(t *testing.T, value string) time.Time {
	t.Helper()
	d, err := time.Parse(time.DateOnly, value)
	if err != nil {
		t.Fatalf("parse date %q: %v", value, err)
	}
	return d
}

// TestGetEnabledAssetsResolvesTheVersionEffectiveAtTheRecordedDate is the reader half of
// VEC-597: which assets an oracle prices depends on the run's recorded effective_at, not
// on when the query happens to run.
func TestGetEnabledAssetsResolvesTheVersionEffectiveAtTheRecordedDate(t *testing.T) {
	ctx := context.Background()
	f := newRetiredSourceFixture(t, ctx, "vec597-assets", "0x1111111111111111111111111111111111111111", "2026-01-01", "2026-08-20")

	for _, tc := range []struct {
		name        string
		effectiveAt string
		wantAssets  int
	}{
		{"before the source was registered", "2025-12-31", 0},
		{"while enabled", "2026-06-01", 1},
		{"on the retirement date", "2026-08-20", 0},
		{"after the retirement", "2026-12-31", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assets, err := f.repo.GetEnabledAssets(ctx, f.oracleID, mustDate(t, tc.effectiveAt))
			if err != nil {
				t.Fatalf("GetEnabledAssets(%s): %v", tc.effectiveAt, err)
			}
			if len(assets) != tc.wantAssets {
				t.Fatalf("GetEnabledAssets(%s) returned %d assets, want %d", tc.effectiveAt, len(assets), tc.wantAssets)
			}
			if tc.wantAssets > 0 && assets[0].TokenID != f.tokenID {
				t.Errorf("asset token_id = %d, want %d", assets[0].TokenID, f.tokenID)
			}
		})
	}
}

// TestGetTokenInfosResolvesTheVersionEffectiveAtTheRecordedDate keeps the second read of
// the same mapping consistent with the first: a unit built from assets and token infos
// that disagreed would price a token it never resolved an address for.
func TestGetTokenInfosResolvesTheVersionEffectiveAtTheRecordedDate(t *testing.T) {
	ctx := context.Background()
	f := newRetiredSourceFixture(t, ctx, "vec597-infos", "0x2222222222222222222222222222222222222222", "2026-01-01", "2026-08-20")

	whileEnabled, err := f.repo.GetTokenInfos(ctx, f.oracleID, mustDate(t, "2026-06-01"))
	if err != nil {
		t.Fatalf("GetTokenInfos while enabled: %v", err)
	}
	if _, ok := whileEnabled[f.tokenID]; !ok {
		t.Errorf("token %d missing from token infos as of 2026-06-01", f.tokenID)
	}

	afterRetirement, err := f.repo.GetTokenInfos(ctx, f.oracleID, mustDate(t, "2026-12-31"))
	if err != nil {
		t.Fatalf("GetTokenInfos after retirement: %v", err)
	}
	if _, ok := afterRetirement[f.tokenID]; ok {
		t.Errorf("token %d still in token infos as of 2026-12-31, after its source was retired", f.tokenID)
	}
}

// TestCopyOracleAssetsCopiesTheVersionEffectiveAtTheRecordedDate covers the writer path:
// seeding a newly discovered oracle must copy the mappings that were effective at the
// run's effective_at, and must not resurrect a retired one.
func TestCopyOracleAssetsCopiesTheVersionEffectiveAtTheRecordedDate(t *testing.T) {
	ctx := context.Background()
	f := newRetiredSourceFixture(t, ctx, "vec597-copy", "0x3333333333333333333333333333333333333333", "2026-01-01", "2026-08-20")
	targetID := testutil.SeedOracle(t, ctx, onchainPricePool, "vec597-copy-target", "target", 1, "0x9999999999999999999999999999999999999999")

	if err := f.repo.CopyOracleAssets(ctx, f.oracleID, targetID, mustDate(t, "2026-12-31")); err != nil {
		t.Fatalf("CopyOracleAssets after retirement: %v", err)
	}
	assets, err := f.repo.GetEnabledAssets(ctx, targetID, mustDate(t, "2026-12-31"))
	if err != nil {
		t.Fatalf("GetEnabledAssets on the target: %v", err)
	}
	if len(assets) != 0 {
		t.Errorf("copied %d assets from a fully retired oracle, want 0", len(assets))
	}

	if err := f.repo.CopyOracleAssets(ctx, f.oracleID, targetID, mustDate(t, "2026-06-01")); err != nil {
		t.Fatalf("CopyOracleAssets while enabled: %v", err)
	}
	assets, err = f.repo.GetEnabledAssets(ctx, targetID, mustDate(t, "2026-06-01"))
	if err != nil {
		t.Fatalf("GetEnabledAssets on the target: %v", err)
	}
	if len(assets) != 1 {
		t.Fatalf("copied %d assets that were effective on 2026-06-01, want 1", len(assets))
	}
	if assets[0].TokenID != f.tokenID {
		t.Errorf("copied asset token_id = %d, want %d", assets[0].TokenID, f.tokenID)
	}
}
