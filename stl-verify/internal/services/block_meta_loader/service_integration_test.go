//go:build integration

package block_meta_loader

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// No block_meta fixture here, deliberately: the schema PR's migration is in db/migrations, so
// testutil.SetupTestDB's template carries the real table, its version-tuple PK and the CHECKs. A
// fixture shaped like the query it tests cannot detect a disagreement with production.

// newLocalStackReader builds the real S3 reader adapter pointed at the shared
// LocalStack, so the test exercises the adapter's .gz auto-decompression path.
func newLocalStackReader(t *testing.T, ctx context.Context, logger *slog.Logger) *s3adapter.Reader {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(sharedLocalStackCfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	return s3adapter.NewReaderWithOptions(awsCfg, logger, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(sharedLocalStackCfg.Endpoint)
		o.UsePathStyle = true
	})
}

// uploadBlock uploads a gzipped block JSON carrying the given hex timestamp to the
// key the loader will read for (blockNum, version).
// hexSeconds parses an on-chain hex timestamp to its epoch-second value.
func hexSeconds(t *testing.T, hexTimestamp string) int64 {
	t.Helper()
	sec, err := strconv.ParseInt(strings.TrimPrefix(hexTimestamp, "0x"), 16, 64)
	if err != nil {
		t.Fatalf("parse hex timestamp %q: %v", hexTimestamp, err)
	}
	return sec
}

// TestRunIntegration_FillsBlockMetaFromS3 exercises the full loader: it seeds
// observation rows across both chain-resolution arms (protocol_event resolves
// chain natively; borrower resolves it via protocol.chain_id), archives the
// referenced block headers in S3, runs the loader, and asserts block_meta is
// filled with the authoritative header timestamps. It also asserts resumability
// (a block already in block_meta is not re-fetched from S3) and that a rerun is a
// no-op.
// loaderFixture is the seeded world every scenario below shares: one chain-1 deployment with
// blocks referenced through each arm, one block already loaded, one block on another chain, and
// one block at two reorg versions. Each test gets its own database and bucket, and asserts one
// thing — a chain-filter regression used to fail as "expected 5 rows upserted" with five
// candidate causes.
type loaderFixture struct {
	pool    *pgxpool.Pool
	svc     *Service
	runID   buildregistry.RunID
	chainID int64
}

// Hex header times for the blocks the fixture uploads, and the instant block 300 is pre-seeded at.
const (
	b100Hex    = "0x67c00000" // referenced by protocol_event: the native chain_id arm
	b200Hex    = "0x67c00e10" // referenced by borrower: the protocol.chain_id join arm
	b500Hex    = "0x67c01c20" // referenced by prime_debt, which is NOT an arm: the control below
	b600v0Hex  = "0x67c02710"
	b600v1Hex  = "0x67c03a98"
	b300Seeded = int64(1_700_000_000)
	fixtureChn = int64(1)
)

func newLoaderFixture(t *testing.T, ctx context.Context) loaderFixture {
	t.Helper()
	pool, _, dbCleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(dbCleanup)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	bucket := testutil.S3TestBucketName(t, "blockmeta-")
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	// The initial-schema migration seeds a SparkLend protocol on chain 1; reuse it for the
	// FK-bearing observation rows.
	var protocolID, userID, tokenID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM protocol WHERE chain_id = $1 ORDER BY id LIMIT 1`, fixtureChn).Scan(&protocolID); err != nil {
		t.Fatalf("load seeded protocol id: %v", err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO "user" (chain_id, address) VALUES ($1, '\xabc0'::bytea) RETURNING id`, fixtureChn).Scan(&userID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO token (chain_id, address, symbol, decimals) VALUES ($1, '\xdef0'::bytea, 'TKN', 18) RETURNING id`, fixtureChn).Scan(&tokenID); err != nil {
		t.Fatalf("seed token: %v", err)
	}

	event := func(chain, proto, block int64, version int, tx, addr string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO protocol_event
				(chain_id, protocol_id, block_number, block_version, tx_hash, log_index, contract_address, event_name, event_data)
			VALUES ($1, $2, $3, $4, decode($5,'hex'), 0, decode($6,'hex'), 'Borrow', '{}'::jsonb)`,
			chain, proto, block, version, tx, addr); err != nil {
			t.Fatalf("seed protocol_event block %d/%d: %v", block, version, err)
		}
	}

	event(fixtureChn, protocolID, 100, 0, "01", "02")
	testutil.UploadBlockHeader(t, ctx, s3Client, bucket, 100, 0, b100Hex)

	if _, err := pool.Exec(ctx, `
		INSERT INTO borrower (user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		VALUES ($1, $2, $3, 200, 0, 1, 1, 'Borrow', '\x03'::bytea)`, userID, protocolID, tokenID); err != nil {
		t.Fatalf("seed borrower block 200: %v", err)
	}
	testutil.UploadBlockHeader(t, ctx, s3Client, bucket, 200, 0, b200Hex)

	// Block 300 is referenced but already loaded, at two processing_versions. Its object is
	// deliberately absent: a loader that re-fetched it would fail hard on the missing key.
	event(fixtureChn, protocolID, 300, 0, "04", "05")
	if _, err := pool.Exec(ctx,
		`INSERT INTO block_meta (chain_id, block_number, block_version, processing_version, block_timestamp)
		 VALUES ($1, 300, 0, 0, to_timestamp($2)), ($1, 300, 0, 1, to_timestamp($2 + 7))`,
		fixtureChn, b300Seeded); err != nil {
		t.Fatalf("pre-seed block_meta block 300: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at)
		VALUES ((SELECT id FROM prime WHERE name = 'spark'), 'ETH-A', 0, 500, 0, now())`); err != nil {
		t.Fatalf("seed prime_debt block 500: %v", err)
	}
	testutil.UploadBlockHeader(t, ctx, s3Client, bucket, 500, 0, b500Hex)

	// Block 400 is referenced only on Base. Its object is deliberately absent, so a broken chain
	// filter fails hard rather than passing silently.
	var baseProtocolID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type) VALUES (8453, '\xbee5'::bytea, 'test-base', 'lending') RETURNING id`).Scan(&baseProtocolID); err != nil {
		t.Fatalf("seed base protocol: %v", err)
	}
	event(8453, baseProtocolID, 400, 0, "06", "07")

	// Block 600 at two reorg versions, which with BatchSize 2 straddles a batch boundary.
	event(fixtureChn, protocolID, 600, 0, "08", "02")
	event(fixtureChn, protocolID, 600, 1, "09", "02")
	testutil.UploadBlockHeader(t, ctx, s3Client, bucket, 600, 0, b600v0Hex)
	testutil.UploadBlockHeader(t, ctx, s3Client, bucket, 600, 1, b600v1Hex)

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := postgres.NewBlockMetaRepository(pool, logger, buildID, runID)
	if err != nil {
		t.Fatalf("NewBlockMetaRepository: %v", err)
	}
	// BatchSize 2 forces several batches over the pending set, so the keyset cursor advances
	// across boundaries rather than filling everything at once.
	svc, err := New(Config{ChainID: fixtureChn, Bucket: bucket, BatchSize: 2}, repo, newLocalStackReader(t, ctx, logger), logger)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return loaderFixture{pool: pool, svc: svc, runID: runID, chainID: fixtureChn}
}

// Each referencing arm reaches S3 and lands its header time.
func TestRunIntegration_FillsEachArm(t *testing.T) {
	ctx := context.Background()
	f := newLoaderFixture(t, ctx)

	if _, err := f.svc.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertBlockTimestamp(t, ctx, f.pool, f.chainID, 100, hexSeconds(t, b100Hex)) // native chain_id
	assertBlockTimestamp(t, ctx, f.pool, f.chainID, 200, hexSeconds(t, b200Hex)) // protocol join
	// Control: prime_debt references block 500 and is not an arm, because it renames synced_at to
	// block_timestamp by transform and so declares no block_meta fill. A table nobody declared a need
	// for must not pull its blocks into the loader's work.
	var unreferenced int
	if err := f.pool.QueryRow(ctx,
		`SELECT count(*) FROM block_meta WHERE chain_id = $1 AND block_number = 500`, f.chainID).Scan(&unreferenced); err != nil {
		t.Fatal(err)
	}
	if unreferenced != 0 {
		t.Errorf("block 500 was loaded, but only prime_debt references it and it declares no block_meta fill")
	}
}

// Two reorg versions of one block are distinct rows, filled across a batch boundary.
func TestRunIntegration_FillsBothReorgVersions(t *testing.T) {
	ctx := context.Background()
	f := newLoaderFixture(t, ctx)

	if _, err := f.svc.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertBlockTimestamp(t, ctx, f.pool, f.chainID, 600, hexSeconds(t, b600v0Hex))

	var ts600v1 time.Time
	if err := f.pool.QueryRow(ctx,
		`SELECT block_timestamp FROM block_meta WHERE chain_id = $1 AND block_number = 600 AND block_version = 1`,
		f.chainID).Scan(&ts600v1); err != nil {
		t.Fatalf("query block 600 v1: %v", err)
	}
	if ts600v1.Unix() != hexSeconds(t, b600v1Hex) {
		t.Errorf("block 600 v1 timestamp = %d, want %d", ts600v1.Unix(), hexSeconds(t, b600v1Hex))
	}
}

// A block referenced only on another chain is excluded, not fetched.
func TestRunIntegration_ExcludesAnotherChainsBlocks(t *testing.T) {
	ctx := context.Background()
	f := newLoaderFixture(t, ctx)

	if _, err := f.svc.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	var rows int
	if err := f.pool.QueryRow(ctx, `SELECT COUNT(*) FROM block_meta WHERE block_number = 400`).Scan(&rows); err != nil {
		t.Fatalf("count block 400 rows: %v", err)
	}
	if rows != 0 {
		t.Errorf("chain-8453 block 400 leaked into block_meta (%d rows); the chain filter is broken", rows)
	}
}

// An already-loaded block is neither re-fetched nor rewritten, on either processing_version — the
// correction axis belongs to the operator.
func TestRunIntegration_LeavesAlreadyLoadedBlocksAlone(t *testing.T) {
	ctx := context.Background()
	f := newLoaderFixture(t, ctx)

	if _, err := f.svc.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertBlockTimestamp(t, ctx, f.pool, f.chainID, 300, b300Seeded)

	var seeded int
	if err := f.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM block_meta
		 WHERE chain_id = $1 AND block_number = 300 AND run_id IS NULL
		   AND extract(epoch FROM block_timestamp) = $2 + 7 * processing_version`,
		f.chainID, b300Seeded).Scan(&seeded); err != nil {
		t.Fatalf("count seeded block 300 rows: %v", err)
	}
	if seeded != 2 {
		t.Errorf("expected both seeded block 300 rows untouched, got %d matching", seeded)
	}
}

// Every row the loader writes is processing_version 0 and carries the run that wrote it.
func TestRunIntegration_StampsTheWriterRun(t *testing.T) {
	ctx := context.Background()
	f := newLoaderFixture(t, ctx)

	upserted, err := f.svc.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Four, not five: prime_debt's block 500 is no longer enumerated (it is not an arm).
	if upserted != 4 {
		t.Fatalf("upserted %d rows, want 4", upserted)
	}
	var loaded int
	if err := f.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM block_meta WHERE processing_version = 0 AND run_id = $1 AND block_number <> 300`,
		int64(f.runID)).Scan(&loaded); err != nil {
		t.Fatalf("count loader-stamped rows: %v", err)
	}
	if loaded != 4 {
		t.Errorf("%d rows stamped with run_id %d at processing_version 0, want 4", loaded, f.runID)
	}
}

// A second run has nothing to do, and writes nothing.
func TestRunIntegration_RerunIsANoOp(t *testing.T) {
	ctx := context.Background()
	f := newLoaderFixture(t, ctx)

	if _, err := f.svc.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	before := countBlockMeta(t, ctx, f.pool)

	upserted, err := f.svc.Run(ctx)
	if err != nil {
		t.Fatalf("rerun Run: %v", err)
	}
	if upserted != 0 {
		t.Errorf("rerun upserted %d rows, want 0", upserted)
	}
	if got := countBlockMeta(t, ctx, f.pool); got != before {
		t.Errorf("block_meta holds %d rows after the rerun, want %d unchanged", got, before)
	}
}

func assertBlockTimestamp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, chainID, blockNumber, wantUnix int64) {
	t.Helper()
	var ts time.Time
	err := pool.QueryRow(ctx,
		`SELECT block_timestamp FROM block_meta WHERE chain_id = $1 AND block_number = $2 AND block_version = 0`,
		chainID, blockNumber,
	).Scan(&ts)
	if err != nil {
		t.Fatalf("query block_meta for block %d: %v", blockNumber, err)
	}
	if ts.Unix() != wantUnix {
		t.Errorf("block %d: block_timestamp = %d (%s), want %d", blockNumber, ts.Unix(), ts.UTC(), wantUnix)
	}
}

func countBlockMeta(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM block_meta`).Scan(&n); err != nil {
		t.Fatalf("count block_meta: %v", err)
	}
	return n
}
