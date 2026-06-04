//go:build integration

package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/klauspost/compress/zstd"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestCallArchiverAgainstLocalStack exercises the full S3 path: PUT, key
// shape, content-encoding, zstd round-trip and idempotency under IfNoneMatch.
func TestCallArchiverAgainstLocalStack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	_, lsCfg := testutil.StartLocalStack(t, ctx, "s3")
	client := testutil.NewS3Client(t, ctx, lsCfg)

	const bucket = "raw-sc-calls-test"
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	archiver, err := newCallArchiverWithClient(client, bucket, slog.Default())
	if err != nil {
		t.Fatalf("newCallArchiverWithClient: %v", err)
	}
	t.Cleanup(func() { _ = archiver.Close() })

	batch := outbound.CallBatch{
		ChainID:      1,
		BlockNumber:  21500042,
		BlockVersion: 0,
		BuildID:      99,
		Source:       "oracle-price",
		Multicaller:  common.HexToAddress("0xca11bde05779b6216e94d2f0a4d57f1ddee6e6"),
		Timestamp:    time.Date(2026, 6, 4, 10, 30, 45, 0, time.UTC),
		Records: []outbound.CallRecord{
			{
				ContractAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"),
				Method:          "latestRoundData",
				CallData:        []byte{0xfe, 0xaf, 0x96, 0x8c},
				Response:        []byte{0xde, 0xad, 0xbe, 0xef},
				Success:         true,
			},
		},
	}

	if err := archiver.ArchiveBatch(ctx, batch); err != nil {
		t.Fatalf("ArchiveBatch: %v", err)
	}

	wantKey := "raw-sc-calls/chain_id=1/block=21500000-21500999/bv=0/build_id=99/oracle-price_latestRoundData_20260604T103045Z.jsonl.zst"

	head, err := client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(wantKey),
	})
	if err != nil {
		t.Fatalf("HeadObject %s: %v", wantKey, err)
	}
	if head.ContentType == nil || *head.ContentType != "application/x-ndjson" {
		t.Errorf("ContentType = %v, want application/x-ndjson", head.ContentType)
	}
	if head.ContentEncoding == nil || *head.ContentEncoding != "zstd" {
		t.Errorf("ContentEncoding = %v, want zstd", head.ContentEncoding)
	}

	get, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(wantKey),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer get.Body.Close()

	raw, err := io.ReadAll(get.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	dec, err := zstd.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer dec.Close()
	jsonl, err := io.ReadAll(dec)
	if err != nil {
		t.Fatalf("zstd read: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(jsonl), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("jsonl lines = %d, want 1", len(lines))
	}
	var line map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &line); err != nil {
		t.Fatalf("decode jsonl: %v", err)
	}
	if line["call_data"] != "0xfeaf968c" || line["response"] != "0xdeadbeef" {
		t.Errorf("payload mismatch: %+v", line)
	}

	// Idempotency: re-archiving the same batch must not error (412 is benign).
	if err := archiver.ArchiveBatch(ctx, batch); err != nil {
		t.Fatalf("re-ArchiveBatch (idempotency): %v", err)
	}
}
