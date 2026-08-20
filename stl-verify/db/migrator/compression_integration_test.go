//go:build integration

package migrator_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Two days one chunk apart, so a write into a compressed chunk can be compared against the same write
// into a row-store one. Well clear of the 2035 range the plan fixtures seed.
const (
	compressedFixtureDay = "2036-03-01"
	rowStoreFixtureDay   = "2036-03-06"
)

// Four block numbers per timestamp: the production sort order breaks ties on synced_at by
// block_number, so a rewrite that reordered within a timestamp would go unnoticed without them.
const (
	fixtureTimestampsPerDay = 5
	fixtureBlocksPerStamp   = 4
	fixtureRowsPerDay       = fixtureTimestampsPerDay * fixtureBlocksPerStamp
)

// The lowest block number the seed writes, so a key every fixture day carries, and one no fixture row
// uses, for the write that lands on a key the columnstore has never seen.
const (
	firstFixtureBlock    = 1_000_001
	unseededFixtureBlock = 9_000_000
)

// Compressing a chunk must not change what reading it returns. The rows are read through the
// production sort order, the chunk is rewritten into the columnstore, and the read is repeated: a row
// the rewrite dropped, duplicated, or reordered shows up as a difference between the two sequences.
func TestCompressedChunkReadPathReturnsTheSameRows(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	fixture := seedCompressionFixture(t, ctx, pool)

	before := fixture.readDay(t, ctx, pool, compressedFixtureDay)
	if len(before) != fixtureRowsPerDay {
		t.Fatalf("fixture day %s holds %d rows, want %d — the comparison below is only as strong as what "+
			"it reads", compressedFixtureDay, len(before), fixtureRowsPerDay)
	}

	fixture.compressDay(t, ctx, pool, compressedFixtureDay)

	after := fixture.readDay(t, ctx, pool, compressedFixtureDay)
	if !slices.Equal(before, after) {
		t.Fatalf("compressing the %s chunk changed its read path\nbefore:\n%s\nafter:\n%s",
			compressedFixtureDay, strings.Join(before, "\n"), strings.Join(after, "\n"))
	}
}

// Both per-row trigger statements have to keep pruning to one chunk once that chunk is in the
// columnstore, where the covering index no longer exists and the lookup is served by the sparse
// min/max index on the compress_hyper_* twin instead. Same assertions as the row-store fixture, plus
// the twin the plan must name exactly once: a second twin, or a second row-store chunk, is the
// fan-out the force_custom_plan setting exists to prevent.
func TestProcessingVersionTriggerLookupsPruneToOneCompressedChunk(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	seedProcessingVersionPlanRows(t, ctx, pool, 2_000)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire conn: %v", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SET plan_cache_mode = "+planCacheModeValue); err != nil {
		t.Fatalf("set plan_cache_mode: %v", err)
	}
	defer resetSessionState(t, ctx, conn, "RESET plan_cache_mode")

	for _, tc := range processingVersionIndexCases() {
		compressChunk(t, ctx, pool, probeChunk(t, ctx, conn, tc))

		for _, lookup := range tc.triggerLookups() {
			t.Run(tc.tableName+"/"+lookup.label, func(t *testing.T) {
				plan := explainWarmedLookup(t, ctx, conn, lookup)
				assertLookupPrunedToOneChunk(t, plan, tc.tableName+" "+lookup.label)
				if twins := plan.compressedChunkNames(); len(twins) != 1 {
					t.Fatalf("%s %s lookup read %d columnstore twins, want exactly 1: the probe key's chunk "+
						"is compressed, so its twin is where the lookup reads the batches from\nplan:\n%s",
						tc.tableName, lookup.label, len(twins), plan.raw)
				}
			})
		}
	}
}

// Reprocessing a key whose row is already compressed fails, and the failure is TimescaleDB's, not the
// constraint's: the columnstore checks the incoming tuple against the compressed batches before
// ModifyTable fires any BEFORE INSERT trigger, so it compares the processing_version the writer sent
// (the column default, 0) instead of the one the trigger is about to assign. The version the trigger
// would have written is free — TestCompressedChunkStoresAKeyWhoseVersionIsSuppliedUpFront inserts it
// directly and succeeds — so this is a defect in the versioning path on compressed chunks, pinned here
// as current behaviour rather than as intent (VEC-594). A new build reprocessing anything older than
// the compression policy interval hits it.
func TestReprocessingACompressedKeyIsRejectedBeforeTheVersionTriggerRuns(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	fixture := seedCompressionFixture(t, ctx, pool)
	fixture.compressDay(t, ctx, pool, compressedFixtureDay)

	_, err := fixture.reprocess(ctx, pool, compressedFixtureDay, firstFixtureBlock, probeBuildID)
	if !isUniqueViolation(err) {
		t.Fatalf("reprocessing a compressed key returned %v, want a unique-constraint violation", err)
	}
	if versions := fixture.versionsAt(t, ctx, pool, compressedFixtureDay, firstFixtureBlock); !slices.Equal(versions, []int32{0}) {
		t.Fatalf("rejected reprocessing left versions %v at the key, want only the original [0]", versions)
	}
}

// The control for the rejection above: same fixture, same write, the other day — the one whose chunk was
// left in the row store. It assigns the next version, which is what the versioning path is for. Without
// this, that test would pass just as well against a trigger that had stopped versioning altogether.
func TestReprocessingARowStoreKeyAssignsTheNextVersion(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	fixture := seedCompressionFixture(t, ctx, pool)
	fixture.compressDay(t, ctx, pool, compressedFixtureDay)

	version, err := fixture.reprocess(ctx, pool, rowStoreFixtureDay, firstFixtureBlock, probeBuildID)
	if err != nil {
		t.Fatalf("reprocessing a row-store key: %v", err)
	}
	if version != 1 {
		t.Fatalf("reprocessing a row-store key assigned version %d, want 1", version)
	}
}

// A key no compressed batch holds is versioned as usual inside a compressed chunk: the rejection above
// is scoped to keys the columnstore already carries, not to compressed chunks as such, so an indexer
// writing fresh keys into an aged chunk keeps working. The second build is what shows the trigger ran
// at all — a first write landing on version 0 is indistinguishable from the column default.
func TestWritingANewKeyIntoACompressedChunkIsVersionedNormally(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	fixture := seedCompressionFixture(t, ctx, pool)
	fixture.compressDay(t, ctx, pool, compressedFixtureDay)

	for _, build := range []struct {
		id          int
		wantVersion int32
	}{
		{id: probeBuildID, wantVersion: 0},
		{id: probeBuildID + 1, wantVersion: 1},
	} {
		version, err := fixture.reprocess(ctx, pool, compressedFixtureDay, unseededFixtureBlock, build.id)
		if err != nil {
			t.Fatalf("build %d writing a new key into a compressed chunk: %v", build.id, err)
		}
		if version != build.wantVersion {
			t.Fatalf("build %d writing a new key into a compressed chunk assigned version %d, want %d",
				build.id, version, build.wantVersion)
		}
	}
}

// The storage layer stores the very row the rejected reprocessing would have written: supplied up
// front, version 1 at a compressed key is accepted. So no constraint is genuinely violated there, and
// the rejection is the pre-trigger comparison alone.
func TestCompressedChunkStoresAKeyWhoseVersionIsSuppliedUpFront(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()
	fixture := seedCompressionFixture(t, ctx, pool)
	fixture.compressDay(t, ctx, pool, compressedFixtureDay)

	if err := fixture.insertVersionDirectly(ctx, pool, compressedFixtureDay, firstFixtureBlock, 1); err != nil {
		t.Fatalf("storing version 1 at a compressed key: %v", err)
	}
	if versions := fixture.versionsAt(t, ctx, pool, compressedFixtureDay, firstFixtureBlock); !slices.Equal(versions, []int32{0, 1}) {
		t.Fatalf("versions at the key are %v, want [0 1]", versions)
	}
}

// The chunk the probe key lands in, read off the plan that prunes to it: plan-time chunk exclusion
// already names that chunk, which saves repeating each table's dimension arithmetic to find it. The key
// is inlined rather than run through the prepared lookups so this does not take a PREPARE name the
// measured lookups need on the same session.
func probeChunk(t *testing.T, ctx context.Context, conn *pgxpool.Conn, tc processingVersionIndexCase) string {
	t.Helper()

	plan := explainJSON(t, ctx, conn, "EXPLAIN (FORMAT JSON) "+tc.literalKeyLookup())
	chunks := plan.chunkNames()
	if len(chunks) != 1 {
		t.Fatalf("locating the probe key's chunk in %s named %d chunks, want exactly 1\nplan:\n%s",
			tc.tableName, len(chunks), plan.raw)
	}

	var chunk string
	if err := conn.QueryRow(ctx, `
		SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks
		WHERE chunk_name = $1
	`, chunks[0]).Scan(&chunk); err != nil {
		t.Fatalf("resolve chunk %s of %s: %v", chunks[0], tc.tableName, err)
	}
	return chunk
}

func (c processingVersionIndexCase) literalKeyLookup() string {
	preds := make([]string, len(c.keyColumns))
	for i, col := range c.keyColumns {
		preds[i] = fmt.Sprintf("%s = %s", col.name, col.literal)
	}
	return fmt.Sprintf("SELECT processing_version FROM %s WHERE %s", c.tableName, strings.Join(preds, " AND "))
}

// Rewrites one chunk into the columnstore, and fails the test unless it took. compress_chunk() in the
// test's own session is what makes this deterministic: a foreground call is unaffected by the policy
// jobs setupMigratedPostgres switches off, so nothing here waits on the TimescaleDB job scheduler and
// nothing re-enables a job it disabled.
func compressChunk(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chunk string) {
	t.Helper()

	if _, err := pool.Exec(ctx, "SELECT compress_chunk($1::regclass)", chunk); err != nil {
		t.Fatalf("compress %s: %v", chunk, err)
	}
	var compressed bool
	if err := pool.QueryRow(ctx, `
		SELECT is_compressed FROM timescaledb_information.chunks
		WHERE format('%I.%I', chunk_schema, chunk_name) = $1
	`, chunk).Scan(&compressed); err != nil {
		t.Fatalf("read the compression state of %s: %v", chunk, err)
	}
	// Every assertion about compressed behaviour is vacuous if the chunk under it is still row-store.
	if !compressed {
		t.Fatalf("%s is still a row-store chunk after compress_chunk", chunk)
	}
}

// One prime of its own and two days of prime_debt rows a chunk apart, written through the real trigger.
// A prime per test keeps each fixture on keys no sibling touches, and prime_debt is the table these
// writes go through because its columnstore segmentby is the leading column of its processing_version
// key; its trigger has the same shape as the other 35.
type compressionFixture struct {
	primeID int64
}

func seedCompressionFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) compressionFixture {
	t.Helper()

	fixture := compressionFixture{primeID: upsertFixturePrime(t, ctx, pool)}
	for _, day := range []string{compressedFixtureDay, rowStoreFixtureDay} {
		if _, err := pool.Exec(ctx, fixtureSeedStatement, fixture.primeID, day,
			fixtureTimestampsPerDay, fixtureBlocksPerStamp); err != nil {
			t.Fatalf("seed %s rows for %s: %v", manyChunkTable, day, err)
		}
	}
	return fixture
}

// Distinct synced_at values per day and several block numbers per value, so the day's rows exercise
// both levels of the production sort order.
const fixtureSeedStatement = `
	INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, build_id)
	SELECT $1, 'compression-fixture', b, 1000000 + b, 0,
	       $2::timestamptz + (s * interval '1 minute'), 0
	FROM generate_series(1, $3::int) AS s, generate_series(1, $4::int) AS b`

// The day's rows in the order the `(prime_id, synced_at DESC, block_number DESC, block_version DESC)`
// index serves them, rendered so two reads compare as sequences.
func (f compressionFixture) readDay(t *testing.T, ctx context.Context, pool *pgxpool.Pool, day string) []string {
	t.Helper()

	rows, err := pool.Query(ctx, `
		SELECT block_number, block_version, synced_at, processing_version, debt_wad::text
		FROM prime_debt
		WHERE prime_id = $1 AND synced_at >= $2::timestamptz AND synced_at < $2::timestamptz + interval '1 day'
		ORDER BY synced_at DESC, block_number DESC, block_version DESC
	`, f.primeID, day)
	if err != nil {
		t.Fatalf("read %s rows for %s: %v", manyChunkTable, day, err)
	}
	defer rows.Close()

	var rendered []string
	for rows.Next() {
		var blockNumber int64
		var blockVersion, processingVersion int32
		var syncedAt time.Time
		var debtWad string
		if err := rows.Scan(&blockNumber, &blockVersion, &syncedAt, &processingVersion, &debtWad); err != nil {
			t.Fatalf("scan %s row: %v", manyChunkTable, err)
		}
		rendered = append(rendered, fmt.Sprintf("%d/%d/%s/%d/%s",
			blockNumber, blockVersion, syncedAt.UTC().Format(time.RFC3339Nano), processingVersion, debtWad))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read %s rows for %s: %v", manyChunkTable, day, err)
	}
	return rendered
}

func (f compressionFixture) compressDay(t *testing.T, ctx context.Context, pool *pgxpool.Pool, day string) {
	t.Helper()
	compressChunk(t, ctx, pool, f.chunkOfDay(t, ctx, pool, day))
}

// The chunk holding a fixture day, named by the rows in it: a row's tableoid is the chunk it landed in,
// which spares this the chunk-interval arithmetic that finding it from the day alone would take.
func (f compressionFixture) chunkOfDay(t *testing.T, ctx context.Context, pool *pgxpool.Pool, day string) string {
	t.Helper()

	var chunk string
	if err := pool.QueryRow(ctx, `
		SELECT DISTINCT tableoid::regclass::text
		FROM prime_debt
		WHERE prime_id = $1 AND synced_at >= $2::timestamptz AND synced_at < $2::timestamptz + interval '1 day'
	`, f.primeID, day).Scan(&chunk); err != nil {
		t.Fatalf("locate the chunk holding %s: %v", day, err)
	}
	return chunk
}

// A write on an existing key by a build that has not written it, which is what a reprocessing run
// issues: the trigger owns processing_version, so the caller sends none.
func (f compressionFixture) reprocess(ctx context.Context, pool *pgxpool.Pool, day string, blockNumber int64, buildID int) (int32, error) {
	var version int32
	err := pool.QueryRow(ctx, `
		INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, build_id)
		VALUES ($1, 'compression-reprocess', 1, $2, 0, $3::timestamptz + interval '1 minute', $4)
		RETURNING processing_version
	`, f.primeID, blockNumber, day, buildID).Scan(&version)
	return version, err
}

// The same row the trigger would have written, with the version supplied instead of assigned.
// session_replication_role keeps the trigger from overwriting it, exactly as the plan fixtures do.
func (f compressionFixture) insertVersionDirectly(ctx context.Context, pool *pgxpool.Pool, day string, blockNumber int64, version int32) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after a successful commit is a no-op

	if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role = 'replica'"); err != nil {
		return fmt.Errorf("disable triggers: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, processing_version, build_id)
		VALUES ($1, 'compression-direct', 1, $2, 0, $3::timestamptz + interval '1 minute', $4, $5)
	`, f.primeID, blockNumber, day, version, probeBuildID); err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	return tx.Commit(ctx)
}

func (f compressionFixture) versionsAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, day string, blockNumber int64) []int32 {
	t.Helper()

	rows, err := pool.Query(ctx, `
		SELECT processing_version FROM prime_debt
		WHERE prime_id = $1 AND block_number = $2 AND block_version = 0
		  AND synced_at = $3::timestamptz + interval '1 minute'
		ORDER BY processing_version
	`, f.primeID, blockNumber, day)
	if err != nil {
		t.Fatalf("read versions at the key: %v", err)
	}
	defer rows.Close()

	var versions []int32
	for rows.Next() {
		var version int32
		if err := rows.Scan(&version); err != nil {
			t.Fatalf("scan version: %v", err)
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read versions at the key: %v", err)
	}
	return versions
}

// SQLSTATE 23505.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
