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

func TestCallArchiverWritesRetrievableObject(t *testing.T) {
	ctx := context.Background()
	_, lsCfg := testutil.StartLocalStack(t, ctx, "s3")

	const bucket = "raw-sc-calls-test"
	s3Client := testutil.NewS3Client(t, ctx, lsCfg)
	if _, err := s3Client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
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
	archiver := s3adapter.NewCallArchiver(writer, bucket, logger)

	batch := outbound.CallBatchRecord{
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

	if err := archiver.Archive(ctx, batch); err != nil {
		t.Fatalf("Archive: %v", err)
	}

	// Discover the object via ListObjectsV2 — the key embeds an opaque batch
	// hash so the test stays decoupled from that hash function.
	list, err := s3Client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
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

	get, err := s3Client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
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

	// Idempotency: a second archive of the same batch must not error and must
	// not produce a second object.
	if err := archiver.Archive(ctx, batch); err != nil {
		t.Fatalf("second Archive (idempotent) failed: %v", err)
	}
	list2, err := s3Client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("raw-sc-calls/chain_id=1/"),
	})
	if err != nil {
		t.Fatalf("list after re-archive: %v", err)
	}
	if len(list2.Contents) != 1 {
		t.Fatalf("after idempotent re-archive listed %d objects, want exactly 1", len(list2.Contents))
	}
}
