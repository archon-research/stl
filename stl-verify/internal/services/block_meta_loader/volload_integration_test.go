//go:build integration

package block_meta_loader

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Volume validation against real Postgres and real S3: enough referenced blocks, spread over enough
// chunks, that batching, the windowed enumeration, bounded concurrency and the anti-join all do
// real work rather than degenerate to one batch.
func TestVolume_RealDataEndToEnd(t *testing.T) {
	const blocks = 2000
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	s3Client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	bucket := testutil.S3TestBucketName(t, "volload-")
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	var protocolID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM protocol WHERE chain_id = 1 ORDER BY id LIMIT 1`).Scan(&protocolID); err != nil {
		t.Fatalf("seeded protocol: %v", err)
	}
	// Spread the referencing rows across days so protocol_event has many chunks, which is what the
	// windowed enumeration has to walk.
	if _, err := pool.Exec(ctx, `
		INSERT INTO protocol_event
			(chain_id, protocol_id, block_number, block_version, tx_hash, log_index, contract_address,
			 event_name, event_data, created_at)
		SELECT 1, $1, 1000000 + g, 0, decode(lpad(to_hex(g),8,'0'),'hex'), 0, '\x02'::bytea,
		       'Borrow', '{}'::jsonb, TIMESTAMPTZ '2026-01-01' + (g / 50) * interval '1 day'
		  FROM generate_series(0, $2 - 1) g`, protocolID, blocks); err != nil {
		t.Fatalf("seed referenced blocks: %v", err)
	}

	// Real gzipped objects in real S3, one per block.
	for i := range blocks {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(fmt.Appendf(nil, `{"timestamp":"0x%x"}`, 0x67c00000+i)); err != nil {
			t.Fatalf("gzip: %v", err)
		}
		if err := gz.Close(); err != nil {
			t.Fatalf("gzip close: %v", err)
		}
		if _, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(s3key.Build(int64(1000000+i), 0, s3key.Block)),
			Body:   bytes.NewReader(buf.Bytes()),
		}); err != nil {
			t.Fatalf("put block %d: %v", 1000000+i, err)
		}
	}

	var chunks int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'protocol_event'`).Scan(&chunks); err != nil {
		t.Fatalf("count chunks: %v", err)
	}
	t.Logf("seeded %d referenced blocks over %d protocol_event chunks", blocks, chunks)

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := postgres.NewBlockMetaRepository(pool, logger, buildID, runID)
	if err != nil {
		t.Fatalf("repository: %v", err)
	}
	svc, err := New(Config{ChainID: 1, Bucket: bucket, BatchSize: 100, Concurrency: 10},
		repo, newLocalStackReader(t, ctx, logger), logger)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	start := time.Now()
	loaded, err := svc.Run(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	t.Logf("loaded %d rows in %s (%d chunks, batch 100, concurrency 10)", loaded, elapsed.Round(time.Millisecond), chunks)

	if loaded != blocks {
		t.Errorf("loaded %d rows, want %d", loaded, blocks)
	}

	// Every row correct, not just counted: right count, right provenance, timestamps matching the
	// objects, no duplicates, and the whole contiguous range present.
	var rows, distinct, wrongProv, wrongTS int64
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(DISTINCT (block_number, block_version)),
		       count(*) FILTER (WHERE build_id <> $1 OR run_id <> $2),
		       count(*) FILTER (WHERE extract(epoch FROM block_timestamp)::bigint
		                              <> 1740636160 + (block_number - 1000000))
		  FROM block_meta WHERE chain_id = 1`,
		int64(buildID), int64(runID)).Scan(&rows, &distinct, &wrongProv, &wrongTS); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if rows != int64(blocks) || distinct != int64(blocks) {
		t.Errorf("block_meta holds %d rows (%d distinct), want %d of each", rows, distinct, blocks)
	}
	if wrongProv != 0 {
		t.Errorf("%d row(s) carry the wrong build or run", wrongProv)
	}
	if wrongTS != 0 {
		t.Errorf("%d row(s) carry a timestamp that is not the object's", wrongTS)
	}

	// A rerun over the same real data is a no-op, and leaves the work list empty.
	again, err := svc.Run(ctx)
	if err != nil {
		t.Fatalf("rerun: %v", err)
	}
	if again != 0 {
		t.Errorf("rerun loaded %d rows, want 0", again)
	}
	var leftover int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM block_meta_worklist WHERE chain_id = 1`).Scan(&leftover); err != nil {
		t.Fatalf("count work list: %v", err)
	}
	if leftover != 0 {
		t.Errorf("%d work-list rows survive a completed rerun", leftover)
	}
}
