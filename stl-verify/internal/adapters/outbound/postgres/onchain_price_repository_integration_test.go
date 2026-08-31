//go:build integration

package postgres

import (
	"context"
	"strings"
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

type retiredSourceFixture struct {
	repo     *OnchainPriceRepository
	oracleID int64
	tokenID  int64
}

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

func TestGetEnabledAssetsResolvesTheVersionEffectiveAtTheRecordedInstant(t *testing.T) {
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

func TestGetTokenInfosResolvesTheVersionEffectiveAtTheRecordedInstant(t *testing.T) {
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

// The arbiter silently skips a source key the target already holds, so a target carrying a
// DIFFERENT mapping for that key would otherwise leave a partial copy reported as success.
func TestCopyOracleAssetsRejectsATargetHoldingAConflictingMapping(t *testing.T) {
	ctx := context.Background()
	f := newRetiredSourceFixture(t, ctx, "vec597-copy-conflict", "0x4444444444444444444444444444444444444444", "2026-01-01", "2026-08-20")
	targetID := testutil.SeedOracle(t, ctx, onchainPricePool, "vec597-conflict-target", "target", 1, "0x8888888888888888888888888888888888888888")

	// Same natural key as the source's, different feed metadata.
	if _, err := onchainPricePool.Exec(ctx, `
		INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_decimals, quote_currency, valid_from, change_reason)
		VALUES ($1, $2, true, 18, 'ETH', $3, 'pre-existing conflicting mapping')`,
		targetID, f.tokenID, testutil.MustUTCInstant(t, "2026-01-01")); err != nil {
		t.Fatalf("seed the conflicting target mapping: %v", err)
	}

	err := f.repo.CopyOracleAssets(ctx, f.oracleID, targetID, mustDate(t, "2026-06-01"))
	if err == nil {
		t.Fatal("CopyOracleAssets reported success onto a target already holding a different mapping for the same key")
	}
	if !strings.Contains(err.Error(), "absent or differ on the target") {
		t.Errorf("error = %v, want it to name the unmapped source keys", err)
	}
}

func TestCopyOracleAssetsCopiesTheVersionEffectiveAtTheRecordedInstant(t *testing.T) {
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
