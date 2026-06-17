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

// chainPrefix lists every archive object written for chainID=1, used to assert
// object counts independent of the per-batch hash suffix.
const chainPrefix = "raw-sc-calls/chain_id=1/"

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

// successCall and failureCall are the two call fixtures the archiver tests draw
// from; the JSON substrings each one must produce live alongside them so the
// table rows can assert on content without re-deriving the hex encoding.
var (
	successCall = outbound.CallEntry{
		ContractAddress: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
		Selector:        "0xfeaf968c",
		CallData:        []byte{0xfe, 0xaf, 0x96, 0x8c, 0xaa},
		Success:         true,
		Response:        []byte{0x00, 0x01, 0x02},
	}
	failureCall = outbound.CallEntry{
		ContractAddress: "0x1F98431c8aD98523631AE4a59f267346ea31F984",
		Selector:        "0x1816d0dd",
		CallData:        []byte{0x18, 0x16, 0x0d, 0xdd},
		Success:         false,
		Response:        []byte{0xde, 0xad},
	}

	successCallSubstrings = []string{`"call_data":"0xfeaf968caa"`, `"response":"0x000102"`}
	failureCallSubstrings = []string{`"call_data":"0x18160ddd"`, `"response":"0xdead"`}
)

// batchWith returns the standard batch metadata stamped with the given calls,
// keeping the table rows free of repeated boilerplate.
func batchWith(calls ...outbound.CallEntry) outbound.CallBatchRecord {
	return outbound.CallBatchRecord{
		ChainID:      1,
		BlockNumber:  21500042,
		BlockVersion: 0,
		BuildID:      47,
		Source:       "oracle-price",
		Multicaller:  "0xcA11bde05977b3631167028862bE2a173976CA11",
		Timestamp:    time.Date(2026, 6, 8, 10, 30, 45, 0, time.UTC),
		Calls:        calls,
	}
}

// countObjects returns how many archive objects exist for chainID=1.
func countObjects(t *testing.T, ctx context.Context, s3Client *awss3.Client) int {
	t.Helper()
	list, err := s3Client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String(archiveTestBucket),
		Prefix: aws.String(chainPrefix),
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	return len(list.Contents)
}

// fetchAndDecode reads the single archive object under chainPrefix and returns
// its decompressed JSONL bytes. It fails the test unless exactly one exists.
func fetchAndDecode(t *testing.T, ctx context.Context, s3Client *awss3.Client) []byte {
	t.Helper()
	list, err := s3Client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String(archiveTestBucket),
		Prefix: aws.String(chainPrefix),
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
	return raw
}

// TestCallArchiverWrites covers the empty (0), single (1), and multi (2) call
// batch cases: an empty batch writes nothing, while a non-empty batch writes one
// object with one JSONL line per call carrying that call's payload.
func TestCallArchiverWrites(t *testing.T) {
	tests := []struct {
		name           string
		calls          []outbound.CallEntry
		wantObjects    int
		wantSubstrings []string
	}{
		{
			name:        "empty batch writes nothing",
			calls:       nil,
			wantObjects: 0,
		},
		{
			name:           "single call",
			calls:          []outbound.CallEntry{successCall},
			wantObjects:    1,
			wantSubstrings: successCallSubstrings,
		},
		{
			name:           "two calls",
			calls:          []outbound.CallEntry{successCall, failureCall},
			wantObjects:    1,
			wantSubstrings: append(append([]string{}, successCallSubstrings...), failureCallSubstrings...),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			archiver, s3Client := newArchiverOnLocalStack(t, ctx)

			if err := archiver.Archive(ctx, batchWith(tt.calls...)); err != nil {
				t.Fatalf("Archive: %v", err)
			}

			if got := countObjects(t, ctx, s3Client); got != tt.wantObjects {
				t.Fatalf("wrote %d objects, want %d", got, tt.wantObjects)
			}
			if tt.wantObjects == 0 {
				return
			}

			raw := fetchAndDecode(t, ctx, s3Client)
			for _, want := range tt.wantSubstrings {
				if !bytes.Contains(raw, []byte(want)) {
					t.Fatalf("archived object missing %q; got: %s", want, raw)
				}
			}
			if got, want := bytes.Count(raw, []byte{'\n'}), len(tt.calls); got != want {
				t.Fatalf("got %d JSONL newlines, want %d (one per call)", got, want)
			}
		})
	}
}

// TestCallArchiverWritesIdempotent verifies that archiving the same batch twice
// is a no-op: the second PUT must not error and must not create a second object.
func TestCallArchiverWritesIdempotent(t *testing.T) {
	ctx := context.Background()
	archiver, s3Client := newArchiverOnLocalStack(t, ctx)
	batch := batchWith(successCall, failureCall)

	if err := archiver.Archive(ctx, batch); err != nil {
		t.Fatalf("first Archive: %v", err)
	}
	if err := archiver.Archive(ctx, batch); err != nil {
		t.Fatalf("second Archive (idempotent) failed: %v", err)
	}

	if got := countObjects(t, ctx, s3Client); got != 1 {
		t.Fatalf("after idempotent re-archive listed %d objects, want exactly 1", got)
	}
}
