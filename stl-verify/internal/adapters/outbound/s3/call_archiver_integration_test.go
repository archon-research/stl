//go:build integration

package s3_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/klauspost/compress/zstd"
)

const archiveTestBucket = "raw-sc-calls-test"

// newArchiverOnLocalStack provisions a LocalStack S3 bucket and returns a
// CallArchiver writing to it plus the S3 client for assertions.
func newArchiverOnLocalStack(t *testing.T, ctx context.Context) (*s3adapter.CallArchiver, *awss3.Client) {
	t.Helper()

	_, lsCfg := testutil.StartLocalStack(t, ctx, "s3")
	s3Client := testutil.NewS3Client(t, ctx, lsCfg)
	if _, err := s3Client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(archiveTestBucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(lsCfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	endpointFn := func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(lsCfg.Endpoint)
		o.UsePathStyle = true
	}

	logger := slog.Default()
	writer := s3adapter.NewWriterWithOptions(awsCfg, logger, endpointFn)
	archiver, err := s3adapter.NewCallArchiver(writer, archiveTestBucket, logger)
	if err != nil {
		t.Fatalf("NewCallArchiver: %v", err)
	}
	return archiver, s3Client
}

// sampleBatch returns a two-call batch with one success and one failure, used by
// the archiver integration tests.
func sampleBatch() outbound.CallBatchRecord {
	return outbound.CallBatchRecord{
		ChainID:      1,
		BlockNumber:  21500042,
		BlockVersion: 0,
		BuildID:      47,
		Source:       "oracle-price",
		Multicaller:  "0xcA11bde05977b3631167028862bE2a173976CA11",
		Timestamp:    time.Date(2026, 6, 8, 10, 30, 45, 0, time.UTC),
		Calls: []outbound.CallEntry{
			{
				ContractAddress: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
				Selector:        "0xfeaf968c",
				CallData:        []byte{0xfe, 0xaf, 0x96, 0x8c, 0xaa},
				Success:         true,
				Response:        []byte{0x00, 0x01, 0x02},
			},
			{
				ContractAddress: "0x1F98431c8aD98523631AE4a59f267346ea31F984",
				Selector:        "0x1816d0dd",
				CallData:        []byte{0x18, 0x16, 0x0d, 0xdd},
				Success:         false,
				Response:        []byte{0xde, 0xad},
			},
		},
	}
}

func TestCallArchiverWritesRetrievableObject(t *testing.T) {
	ctx := context.Background()
	archiver, s3Client := newArchiverOnLocalStack(t, ctx)
	batch := sampleBatch()

	if err := archiver.Archive(ctx, batch); err != nil {
		t.Fatalf("Archive: %v", err)
	}

	// Discover the object via ListObjectsV2 — the key embeds an opaque batch
	// hash so the test stays decoupled from that hash function.
	list, err := s3Client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String(archiveTestBucket),
		Prefix: aws.String("raw-sc-calls/chain_id=1/block=21500000-21500999/21500042_0_oracle-price_"),
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Contents) != 1 {
		t.Fatalf("listed %d objects, want exactly 1 per batch", len(list.Contents))
	}
	key := aws.ToString(list.Contents[0].Key)
	if !strings.HasSuffix(key, ".jsonl.zst") {
		t.Fatalf("key %q missing .jsonl.zst suffix", key)
	}

	get, err := s3Client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(archiveTestBucket), Key: aws.String(key)})
	if err != nil {
		t.Fatalf("GetObject %s: %v", key, err)
	}
	defer get.Body.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(get.Body); err != nil {
		t.Fatalf("read object: %v", err)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer dec.Close()
	raw, err := dec.DecodeAll(buf.Bytes(), nil)
	if err != nil {
		t.Fatalf("zstd decode: %v", err)
	}

	// Both calls' payload fields appear in the multi-line JSONL.
	if !bytes.Contains(raw, []byte(`"call_data":"0xfeaf968caa"`)) {
		t.Fatalf("archived object missing first call_data; got: %s", raw)
	}
	if !bytes.Contains(raw, []byte(`"response":"0x000102"`)) {
		t.Fatalf("archived object missing first response; got: %s", raw)
	}
	if !bytes.Contains(raw, []byte(`"call_data":"0x18160ddd"`)) {
		t.Fatalf("archived object missing second call_data; got: %s", raw)
	}
	if !bytes.Contains(raw, []byte(`"response":"0xdead"`)) {
		t.Fatalf("archived object missing second response; got: %s", raw)
	}
	if got, want := bytes.Count(raw, []byte{'\n'}), len(batch.Calls); got != want {
		t.Fatalf("got %d JSONL newlines, want %d (one per call)", got, want)
	}
}

// TestCallArchiverWritesIdempotent verifies that archiving the same batch twice
// is a no-op: the second PUT must not error and must not create a second object.
func TestCallArchiverWritesIdempotent(t *testing.T) {
	ctx := context.Background()
	archiver, s3Client := newArchiverOnLocalStack(t, ctx)
	batch := sampleBatch()

	if err := archiver.Archive(ctx, batch); err != nil {
		t.Fatalf("first Archive: %v", err)
	}
	if err := archiver.Archive(ctx, batch); err != nil {
		t.Fatalf("second Archive (idempotent) failed: %v", err)
	}

	list, err := s3Client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String(archiveTestBucket),
		Prefix: aws.String("raw-sc-calls/chain_id=1/"),
	})
	if err != nil {
		t.Fatalf("list after re-archive: %v", err)
	}
	if len(list.Contents) != 1 {
		t.Fatalf("after idempotent re-archive listed %d objects, want exactly 1", len(list.Contents))
	}
}
