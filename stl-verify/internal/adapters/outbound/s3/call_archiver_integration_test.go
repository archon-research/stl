//go:build integration

package s3_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rawsckey"
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
	reader := s3adapter.NewReaderWithOptions(awsCfg, logger, endpointFn)
	archiver := s3adapter.NewCallArchiver(writer, bucket, logger)

	rec := outbound.CallRecord{
		ChainID:         1,
		BlockNumber:     21500042,
		BlockVersion:    0,
		BuildID:         47,
		Source:          "oracle-price",
		Multicaller:     "0xcA11bde05977b3631167028862bE2a173976CA11",
		Timestamp:       time.Date(2026, 6, 8, 10, 30, 45, 0, time.UTC),
		ContractAddress: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
		Selector:        "0xfeaf968c",
		CallData:        []byte{0xfe, 0xaf, 0x96, 0x8c, 0xaa},
		Success:         true,
		Response:        []byte{0x00, 0x01, 0x02},
	}

	if err := archiver.Archive(ctx, rec); err != nil {
		t.Fatalf("Archive: %v", err)
	}

	key := rawsckey.Build(rec.ChainID, rec.BlockNumber, rec.BlockVersion, rec.Source, rec.CallData)
	rc, err := reader.StreamFile(ctx, bucket, key)
	if err != nil {
		t.Fatalf("StreamFile %s: %v", key, err)
	}
	defer rc.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
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
	if !bytes.Contains(raw, []byte(`"call_data":"0xfeaf968caa"`)) {
		t.Fatalf("archived object missing call_data; got: %s", raw)
	}
	if !bytes.Contains(raw, []byte(`"response":"0x000102"`)) {
		t.Fatalf("archived object missing response; got: %s", raw)
	}

	// Idempotency: a second archive of the same record must not error.
	if err := archiver.Archive(ctx, rec); err != nil {
		t.Fatalf("second Archive (idempotent) failed: %v", err)
	}
}
