package testutil

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
)

// UploadBlockHeader writes a gzipped block object carrying only the header timestamp, which is all the
// block_meta loader reads from the raw archive.
func UploadBlockHeader(t *testing.T, ctx context.Context, client *s3.Client, bucket string, blockNum int64, version int, hexTimestamp string) {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(fmt.Appendf(nil, `{"timestamp":%q}`, hexTimestamp)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(s3key.Build(blockNum, version, s3key.Block)),
		Body:   bytes.NewReader(buf.Bytes()),
	}); err != nil {
		t.Fatalf("put block %d/%d: %v", blockNum, version, err)
	}
}

// SeedReferencedBlocks inserts chain-1 protocol_event rows at the given blocks, so the block_meta work
// list has blocks to enumerate that block_meta lacks.
func SeedReferencedBlocks(t *testing.T, ctx context.Context, pool *pgxpool.Pool, blocks ...int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO chain (chain_id, name) VALUES (1, 'ethereum') ON CONFLICT DO NOTHING;
		INSERT INTO protocol (chain_id, address, name) VALUES (1, '\x7001', 'itest') ON CONFLICT DO NOTHING;`); err != nil {
		t.Fatalf("seed chain/protocol: %v", err)
	}
	for _, b := range blocks {
		if _, err := pool.Exec(ctx, `
			INSERT INTO protocol_event
				(chain_id, protocol_id, block_number, block_version, tx_hash, log_index, contract_address, event_name, event_data)
			VALUES (1, (SELECT id FROM protocol WHERE address='\x7001'), $1, 0, '\x09'::bytea, 0, '\x02'::bytea, 'Borrow', '{}'::jsonb)
			ON CONFLICT DO NOTHING`, b); err != nil {
			t.Fatalf("seed referenced block %d: %v", b, err)
		}
	}
}
