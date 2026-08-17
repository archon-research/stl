//go:build integration

package block_meta_loader

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// createBlockMetaTable creates the block_meta dimension in the test schema. The
// authoritative DDL ships in migration 20260818_120000_create_block_meta_dimension.sql
// (schema PR #695), which is not on this branch, so the loader's target table is
// created inline here to keep the test self-contained. The PK matches the upsert's
// ON CONFLICT target exactly.
func createBlockMetaTable(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS block_meta (
		chain_id        integer     NOT NULL,
		block_number    bigint      NOT NULL,
		block_version   integer     NOT NULL DEFAULT 0,
		block_timestamp timestamptz NOT NULL,
		created_at      timestamptz NOT NULL DEFAULT now(),
		CONSTRAINT block_meta_pkey PRIMARY KEY (chain_id, block_number, block_version)
	)`); err != nil {
		t.Fatalf("create block_meta table: %v", err)
	}
}

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
func uploadBlock(t *testing.T, ctx context.Context, client *s3.Client, bucket string, blockNum int64, version int, hexTimestamp string) {
	t.Helper()
	key := s3key.Build(blockNum, version, s3key.Block)
	blockJSON := fmt.Sprintf(`{"timestamp":%q}`, hexTimestamp)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write([]byte(blockJSON)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf.Bytes()),
	}); err != nil {
		t.Fatalf("put block %s: %v", key, err)
	}
}

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
func TestRunIntegration_FillsBlockMetaFromS3(t *testing.T) {
	ctx := context.Background()

	pool, _, dbCleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer dbCleanup()
	createBlockMetaTable(t, ctx, pool)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	bucket := testutil.S3TestBucketName(t, "blockmeta-")
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	const chainID = int64(1)

	// The initial-schema migration seeds a SparkLend protocol on chain 1; reuse it
	// for the FK-bearing observation rows.
	var protocolID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM protocol WHERE chain_id = $1 ORDER BY id LIMIT 1`, chainID).Scan(&protocolID); err != nil {
		t.Fatalf("load seeded protocol id: %v", err)
	}

	// A user and token for the borrower (protocol-join) arm.
	var userID, tokenID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO "user" (chain_id, address) VALUES ($1, '\xabc0'::bytea) RETURNING id`, chainID,
	).Scan(&userID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals) VALUES ($1, '\xdef0'::bytea, 'TKN', 18) RETURNING id`, chainID,
	).Scan(&tokenID); err != nil {
		t.Fatalf("seed token: %v", err)
	}

	// Block 100 referenced by a protocol_event (native chain_id arm).
	const b100Hex = "0x67c00000"
	if _, err := pool.Exec(ctx, `
		INSERT INTO protocol_event
			(chain_id, protocol_id, block_number, block_version, tx_hash, log_index, contract_address, event_name, event_data)
		VALUES ($1, $2, 100, 0, '\x01'::bytea, 0, '\x02'::bytea, 'Borrow', '{}'::jsonb)`,
		chainID, protocolID); err != nil {
		t.Fatalf("seed protocol_event block 100: %v", err)
	}
	uploadBlock(t, ctx, s3Client, bucket, 100, 0, b100Hex)

	// Block 200 referenced by a borrower row (protocol.chain_id join arm).
	const b200Hex = "0x67c00e10"
	if _, err := pool.Exec(ctx, `
		INSERT INTO borrower
			(user_id, protocol_id, token_id, block_number, block_version, amount, change, event_type, tx_hash)
		VALUES ($1, $2, $3, 200, 0, 1, 1, 'Borrow', '\x03'::bytea)`,
		userID, protocolID, tokenID); err != nil {
		t.Fatalf("seed borrower block 200: %v", err)
	}
	uploadBlock(t, ctx, s3Client, bucket, 200, 0, b200Hex)

	// Block 300 is referenced by a protocol_event but already present in block_meta.
	// Its S3 object is deliberately NOT uploaded: a correctly resumable loader must
	// skip it. If it tried to fetch, blockTimestamp would fail hard on the missing
	// object and Run would error.
	const b300Seeded = int64(1_700_000_000)
	if _, err := pool.Exec(ctx, `
		INSERT INTO protocol_event
			(chain_id, protocol_id, block_number, block_version, tx_hash, log_index, contract_address, event_name, event_data)
		VALUES ($1, $2, 300, 0, '\x04'::bytea, 0, '\x05'::bytea, 'Borrow', '{}'::jsonb)`,
		chainID, protocolID); err != nil {
		t.Fatalf("seed protocol_event block 300: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO block_meta (chain_id, block_number, block_version, block_timestamp) VALUES ($1, 300, 0, to_timestamp($2))`,
		chainID, b300Seeded); err != nil {
		t.Fatalf("pre-seed block_meta block 300: %v", err)
	}

	svc, err := New(Config{ChainID: chainID, Bucket: bucket, BatchSize: 100}, pool, newLocalStackReader(t, ctx, logger), logger)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// First run: fills 100 and 200 (300 already present, not re-fetched).
	upserted, err := svc.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if upserted != 2 {
		t.Errorf("expected 2 rows upserted, got %d", upserted)
	}

	assertBlockTimestamp(t, ctx, pool, chainID, 100, hexSeconds(t, b100Hex))
	assertBlockTimestamp(t, ctx, pool, chainID, 200, hexSeconds(t, b200Hex))
	assertBlockTimestamp(t, ctx, pool, chainID, 300, b300Seeded) // untouched

	if got := countBlockMeta(t, ctx, pool); got != 3 {
		t.Errorf("expected 3 block_meta rows after first run, got %d", got)
	}

	// Rerun: every referenced block is now present, so it is a no-op.
	upserted2, err := svc.Run(ctx)
	if err != nil {
		t.Fatalf("rerun Run: %v", err)
	}
	if upserted2 != 0 {
		t.Errorf("expected rerun to upsert 0 rows, got %d", upserted2)
	}
	if got := countBlockMeta(t, ctx, pool); got != 3 {
		t.Errorf("expected 3 block_meta rows after rerun, got %d", got)
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
