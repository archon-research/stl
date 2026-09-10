//go:build integration

package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
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

	repo, err := NewOnchainPriceRepository(onchainPricePool, nil, 0, 0, 0)
	if err != nil {
		t.Fatalf("NewOnchainPriceRepository: %v", err)
	}
	return retiredSourceFixture{repo: repo, oracleID: oracleID, tokenID: tokenID}
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
			assets, err := f.repo.GetEnabledAssets(ctx, f.oracleID, testutil.MustUTCInstant(t, tc.effectiveAt))
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

	whileEnabled, err := f.repo.GetTokenInfos(ctx, f.oracleID, testutil.MustUTCInstant(t, "2026-06-01"))
	if err != nil {
		t.Fatalf("GetTokenInfos while enabled: %v", err)
	}
	if _, ok := whileEnabled[f.tokenID]; !ok {
		t.Errorf("token %d missing from token infos as of 2026-06-01", f.tokenID)
	}

	afterRetirement, err := f.repo.GetTokenInfos(ctx, f.oracleID, testutil.MustUTCInstant(t, "2026-12-31"))
	if err != nil {
		t.Fatalf("GetTokenInfos after retirement: %v", err)
	}
	if _, ok := afterRetirement[f.tokenID]; ok {
		t.Errorf("token %d still in token infos as of 2026-12-31, after its source was retired", f.tokenID)
	}
}

// The arbiter silently skips a source key the target already holds, so a target carrying a
// DIFFERENT mapping for that key would otherwise leave a partial copy reported as success.
//
// The source carries a SECOND, non-conflicting mapping so the rejected copy has something to
// leave behind: the arbiter skips only the conflicting key, so that second row inserts cleanly
// and the verification then fails on the first. Before the copy became transactional the row
// stayed committed, registering the target as partially mapped behind the caller's error.
func TestCopyOracleAssetsRejectsATargetHoldingAConflictingMapping(t *testing.T) {
	ctx := context.Background()
	f := newRetiredSourceFixture(t, ctx, "vec597-copy-conflict", "0x4444444444444444444444444444444444444444", "2026-01-01", "2026-08-20")
	targetID := testutil.SeedOracle(t, ctx, onchainPricePool, "vec597-conflict-target", "target", 1, "0x8888888888888888888888888888888888888888")

	cleanTokenID := testutil.SeedToken(t, ctx, onchainPricePool, 1, "0x5555555555555555555555555555555555555555", "CLEAN", 18)
	testutil.SeedOracleAssetEffectiveFrom(t, ctx, onchainPricePool, f.oracleID, cleanTokenID, "2026-01-01")

	// Same natural key as the source's, different feed metadata.
	if _, err := onchainPricePool.Exec(ctx, `
		INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_decimals, quote_currency, valid_from, change_reason)
		VALUES ($1, $2, true, 18, 'ETH', $3, 'pre-existing conflicting mapping')`,
		targetID, f.tokenID, testutil.MustUTCInstant(t, "2026-01-01")); err != nil {
		t.Fatalf("seed the conflicting target mapping: %v", err)
	}

	err := f.repo.CopyOracleAssets(ctx, f.oracleID, targetID, testutil.MustUTCInstant(t, "2026-06-01"))
	if err == nil {
		t.Fatal("CopyOracleAssets reported success onto a target already holding a different mapping for the same key")
	}
	if !strings.Contains(err.Error(), "absent or differ on the target") {
		t.Errorf("error = %v, want it to name the unmapped source keys", err)
	}

	// Counts the raw table, not a pinned read: a row left behind at any valid_from is a partial
	// copy, and the point is that the failed call wrote nothing at all.
	var leftBehind int
	if err := onchainPricePool.QueryRow(ctx, `
		SELECT count(*) FROM oracle_asset WHERE oracle_id = $1 AND token_id = $2`,
		targetID, cleanTokenID).Scan(&leftBehind); err != nil {
		t.Fatalf("counting the target's rows for the non-conflicting token: %v", err)
	}
	if leftBehind != 0 {
		t.Errorf("the rejected copy left %d row(s) for the non-conflicting token on the target, want 0: the copy is not atomic", leftBehind)
	}
}

func TestCopyOracleAssetsCopiesTheVersionEffectiveAtTheRecordedInstant(t *testing.T) {
	ctx := context.Background()
	f := newRetiredSourceFixture(t, ctx, "vec597-copy", "0x3333333333333333333333333333333333333333", "2026-01-01", "2026-08-20")
	targetID := testutil.SeedOracle(t, ctx, onchainPricePool, "vec597-copy-target", "target", 1, "0x9999999999999999999999999999999999999999")

	if err := f.repo.CopyOracleAssets(ctx, f.oracleID, targetID, testutil.MustUTCInstant(t, "2026-12-31")); err != nil {
		t.Fatalf("CopyOracleAssets after retirement: %v", err)
	}
	assets, err := f.repo.GetEnabledAssets(ctx, targetID, testutil.MustUTCInstant(t, "2026-12-31"))
	if err != nil {
		t.Fatalf("GetEnabledAssets on the target: %v", err)
	}
	if len(assets) != 0 {
		t.Errorf("copied %d assets from a fully retired oracle, want 0", len(assets))
	}

	if err := f.repo.CopyOracleAssets(ctx, f.oracleID, targetID, testutil.MustUTCInstant(t, "2026-06-01")); err != nil {
		t.Fatalf("CopyOracleAssets while enabled: %v", err)
	}
	assets, err = f.repo.GetEnabledAssets(ctx, targetID, testutil.MustUTCInstant(t, "2026-06-01"))
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

func newOnchainPriceRunRepo(t *testing.T, ctx context.Context) (*OnchainPriceRepository, buildregistry.RunID) {
	t.Helper()
	if _, err := onchainPricePool.Exec(ctx, `TRUNCATE oracle, protocol_oracle CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	buildID, runID := testutil.OpenTestRun(t, ctx, onchainPricePool)
	repo, err := NewOnchainPriceRepository(onchainPricePool, nil, buildID, runID, 0)
	if err != nil {
		t.Fatalf("NewOnchainPriceRepository: %v", err)
	}
	return repo, runID
}

func TestInsertOracle_WrittenRowCarriesTheRunID(t *testing.T) {
	ctx := context.Background()
	repo, runID := newOnchainPriceRunRepo(t, ctx)

	oracle, err := repo.InsertOracle(ctx, &entity.Oracle{
		Name: "run-id-oracle", DisplayName: "Run ID Oracle", ChainID: 1,
		Address:    common.HexToAddress("0x8105f69D9C41644c6A0803fDA7D03Aa70996cFD9"),
		OracleType: entity.OracleTypeAave, DeploymentBlock: 100, Enabled: true, PriceDecimals: 8,
	})
	if err != nil {
		t.Fatalf("InsertOracle: %v", err)
	}

	var gotRunID *int64
	if err := onchainPricePool.QueryRow(ctx, `SELECT run_id FROM oracle WHERE id = $1`, oracle.ID).Scan(&gotRunID); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	testutil.RequireRunID(t, gotRunID, runID)
}

func TestInsertProtocolOracleBinding_WrittenRowCarriesTheRunID(t *testing.T) {
	ctx := context.Background()
	repo, runID := newOnchainPriceRunRepo(t, ctx)
	protocolID := testutil.SeedProtocol(t, ctx, onchainPricePool, 1, "0xC13e21B648A5Ee794902342038FF3aDAB66BE987", "SparkLend", "lending", 100, "")
	oracleID := testutil.SeedOracle(t, ctx, onchainPricePool, "run-id-oracle", "Run ID Oracle", 1, "0x8105f69D9C41644c6A0803fDA7D03Aa70996cFD9")

	binding, err := repo.InsertProtocolOracleBinding(ctx, &entity.ProtocolOracle{ProtocolID: protocolID, OracleID: oracleID, FromBlock: 100})
	if err != nil {
		t.Fatalf("InsertProtocolOracleBinding: %v", err)
	}

	var gotRunID *int64
	if err := onchainPricePool.QueryRow(ctx, `SELECT run_id FROM protocol_oracle WHERE id = $1`, binding.ID).Scan(&gotRunID); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	testutil.RequireRunID(t, gotRunID, runID)
}
