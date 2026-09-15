//go:build integration

package s3_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Every read method answers a key that is not there the same way, because callers tell
// an absent object from a broken one by that sentinel alone: block-meta-loader counts a
// miss on it and fails the run on anything else. A method that omits the mapping turns
// ordinary archive lag into a failed run.
func TestReaderReportsMissingObjectsConsistently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	bucket := testutil.S3TestBucketName(t, "missing-object-")
	client := testutil.NewS3Client(t, ctx, sharedLocalStackCfg)
	testutil.EnsureBucket(t, ctx, client, bucket)

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(sharedLocalStackCfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("load aws config: %v", err)
	}
	reader := s3adapter.NewReaderWithOptions(awsCfg, nil, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(sharedLocalStackCfg.Endpoint)
		o.UsePathStyle = true
	})

	// The bucket exists and is readable, so only the key's absence can fail these.
	const absentKey = "chain_id=1/partition=00000000/1_0_block.json.gz"

	t.Run("StreamFile", func(t *testing.T) {
		rc, err := reader.StreamFile(ctx, bucket, absentKey)
		if err == nil {
			rc.Close()
			t.Fatal("reading an absent key succeeded")
		}
		if !errors.Is(err, outbound.ErrObjectNotFound) {
			t.Errorf("absent key reported as %v, which callers cannot tell from a transport failure", err)
		}
	})

	t.Run("ReadRange", func(t *testing.T) {
		if _, err := reader.ReadRange(ctx, bucket, absentKey, 0, 16); !errors.Is(err, outbound.ErrObjectNotFound) {
			t.Errorf("absent key reported as %v, which callers cannot tell from a transport failure", err)
		}
	})
}
