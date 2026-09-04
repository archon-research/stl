//go:build integration

package buildregistry_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func registerTestBuild(t *testing.T, ctx context.Context, pool *pgxpool.Pool) *buildregistry.Registry {
	t.Helper()
	reg, err := buildregistry.NewWithIdentity(ctx, pool, testutil.TestIdentity("run-test"))
	if err != nil {
		t.Fatalf("NewWithIdentity: %v", err)
	}
	return reg
}

func TestOpenRun_RecordsTheRunAgainstTheBuild(t *testing.T) {
	ctx := context.Background()
	pool := setupDB(t)
	reg := registerTestBuild(t, ctx, pool)
	effectiveAt := testutil.MustUTCInstant(t, "2026-06-01")

	runID, err := reg.OpenRun(ctx, effectiveAt, nil)
	if err != nil {
		t.Fatalf("OpenRun: %v", err)
	}

	var buildID int
	var snapshotIsValid bool
	var recordedEffectiveAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT build_id, reference_snapshot::pg_snapshot IS NOT NULL, reference_effective_at
		FROM writer_run WHERE id = $1`, int64(runID)).Scan(&buildID, &snapshotIsValid, &recordedEffectiveAt); err != nil {
		t.Fatalf("read writer_run %d: %v", runID, err)
	}
	if buildID != int(reg.BuildID()) {
		t.Errorf("writer_run.build_id = %d, want %d", buildID, reg.BuildID())
	}
	if !snapshotIsValid {
		t.Error("reference_snapshot does not parse as a pg_snapshot")
	}
	if !recordedEffectiveAt.Equal(effectiveAt) {
		t.Errorf("reference_effective_at = %s, want %s", recordedEffectiveAt, effectiveAt)
	}
}

func TestOpenRun_EveryProcessStartIsANewRun(t *testing.T) {
	ctx := context.Background()
	pool := setupDB(t)
	reg := registerTestBuild(t, ctx, pool)
	now := time.Now().UTC()

	first, err := reg.OpenRun(ctx, now, nil)
	if err != nil {
		t.Fatalf("first OpenRun: %v", err)
	}
	second, err := reg.OpenRun(ctx, now, nil)
	if err != nil {
		t.Fatalf("second OpenRun: %v", err)
	}
	if first == second {
		t.Errorf("two process starts share run %d", first)
	}
}

func TestOpenRun_RejectsAZeroReferenceEffectiveAt(t *testing.T) {
	ctx := context.Background()
	pool := setupDB(t)
	reg := registerTestBuild(t, ctx, pool)

	if _, err := reg.OpenRun(ctx, time.Time{}, nil); err == nil || !strings.Contains(err.Error(), "referenceEffectiveAt") {
		t.Fatalf("OpenRun() error = %v, want a referenceEffectiveAt error", err)
	}
}

func TestOpenRun_AFailedLoadLeavesNoRun(t *testing.T) {
	ctx := context.Background()
	pool := setupDB(t)
	reg := registerTestBuild(t, ctx, pool)
	loadErr := errors.New("reference table unreadable")

	_, err := reg.OpenRun(ctx, time.Now().UTC(), func(pgx.Tx, buildregistry.RunID) error { return loadErr })
	if !errors.Is(err, loadErr) {
		t.Fatalf("OpenRun() error = %v, want one wrapping the load error", err)
	}

	var runs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM writer_run WHERE build_id = $1`, int(reg.BuildID())).Scan(&runs); err != nil {
		t.Fatalf("count runs: %v", err)
	}
	if runs != 0 {
		t.Errorf("%d writer_run row(s) committed for a process whose reference load failed, want 0", runs)
	}
}

func TestOpenRun_TheLoadReceivesTheRunItRunsUnder(t *testing.T) {
	ctx := context.Background()
	pool := setupDB(t)
	reg := registerTestBuild(t, ctx, pool)

	var seen buildregistry.RunID
	got, err := reg.OpenRun(ctx, time.Now().UTC(), func(tx pgx.Tx, runID buildregistry.RunID) error {
		seen = runID
		var visible bool
		return tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM writer_run WHERE id = $1)`, int64(runID)).Scan(&visible)
	})
	if err != nil {
		t.Fatalf("OpenRun: %v", err)
	}
	if seen != got {
		t.Errorf("load saw run %d, OpenRun returned %d", seen, got)
	}
}

// The reproducibility claim of ADR-0006 §2: with the snapshot and the effective instant both
// recorded, the reference rows a run saw are recoverable even after a version with an earlier
// valid_from is appended — the case a valid_from-only read gets wrong.
func TestOpenRun_ReferenceDataAsOfTheRunSurvivesALaterAppend(t *testing.T) {
	ctx := context.Background()
	pool := setupDB(t)
	reg := registerTestBuild(t, ctx, pool)

	oracleID := testutil.SeedOracle(t, ctx, pool, "as-of-run", "As Of Run", 1, "0x1111111111111111111111111111111111111111")
	tokenID := testutil.SeedToken(t, ctx, pool, 1, "0x2222222222222222222222222222222222222222", "AOR", 18)
	testutil.SeedOracleAssetEffectiveFrom(t, ctx, pool, oracleID, tokenID, "2026-01-01")
	effectiveAt := testutil.MustUTCInstant(t, "2026-06-01")

	const asOf = `
		SELECT enabled FROM oracle_asset
		WHERE oracle_id = $1 AND token_id = $2 AND valid_from <= $3
		ORDER BY valid_from DESC, processing_version DESC
		LIMIT 1`

	var enabledAtLoad bool
	runID, err := reg.OpenRun(ctx, effectiveAt, func(tx pgx.Tx, _ buildregistry.RunID) error {
		return tx.QueryRow(ctx, asOf, oracleID, tokenID, effectiveAt).Scan(&enabledAtLoad)
	})
	if err != nil {
		t.Fatalf("OpenRun: %v", err)
	}
	if !enabledAtLoad {
		t.Fatal("the run loaded the asset as disabled; the seed did not establish the state this test needs")
	}

	// A version appended after the run, effective before the run's instant.
	testutil.SetOracleAssetEnabled(t, ctx, pool, oracleID, tokenID, false, "2026-03-01", "retired after the run")

	var naive bool
	if err := pool.QueryRow(ctx, asOf, oracleID, tokenID, effectiveAt).Scan(&naive); err != nil {
		t.Fatalf("valid_from-only read: %v", err)
	}
	if naive {
		t.Fatal("the valid_from-only read still returns the enabled version; the append did not land where this test needs it")
	}

	var asOfRun bool
	if err := pool.QueryRow(ctx, `
		SELECT a.enabled FROM oracle_asset a, writer_run r
		WHERE r.id = $3
		  AND a.oracle_id = $1 AND a.token_id = $2
		  AND a.valid_from <= r.reference_effective_at
		  AND pg_visible_in_snapshot(a.xmin::text::xid8, r.reference_snapshot::pg_snapshot)
		ORDER BY a.valid_from DESC, a.processing_version DESC
		LIMIT 1`, oracleID, tokenID, int64(runID)).Scan(&asOfRun); err != nil {
		t.Fatalf("as-of-run read: %v", err)
	}
	if !asOfRun {
		t.Error("reference data as of the run resolves the version appended after it, not the one the run loaded")
	}
}
