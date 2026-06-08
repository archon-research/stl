// Package archivingwire wires the raw SC call archiver from environment config.
package archivingwire

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/pkg/awsconfig"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const (
	// EnvFlag gates archiving globally for a worker.
	EnvFlag = "ARCHIVE_SC_CALLS"
	// EnvBucket is the per-chain S3 bucket for archived calls.
	EnvBucket = "RAW_SC_BUCKET"
	// EnvEndpoint optionally overrides the S3 endpoint (LocalStack).
	EnvEndpoint = "AWS_S3_ENDPOINT"
)

// Wrap decorates a multicaller with archiving. Identity when archiving is off.
type Wrap func(outbound.Multicaller) outbound.Multicaller

// Enabled reports whether ARCHIVE_SC_CALLS=true.
func Enabled() bool { return env.Get(EnvFlag, "") == "true" }

// NewS3WrapFromEnv builds the archiving wrap from env config. The returned
// drain func blocks until all in-flight archive writes finish; call it during
// graceful shutdown. All decorators produced by the wrap share one WaitGroup,
// so a single drain() covers them all.
func NewS3WrapFromEnv(ctx context.Context, logger *slog.Logger, chainID, buildID int64, source string) (Wrap, func(), error) {
	if logger == nil {
		logger = slog.Default()
	}

	bucket := env.Get(EnvBucket, "")
	if bucket == "" {
		return nil, nil, fmt.Errorf("%s is required when %s=true", EnvBucket, EnvFlag)
	}

	awsCfg, err := awsconfig.Load(ctx, awsconfig.Options{StaticCredentialsFromEnv: true})
	if err != nil {
		return nil, nil, fmt.Errorf("loading AWS config: %w", err)
	}

	var writer outbound.S3Writer
	if endpoint := env.Get(EnvEndpoint, ""); endpoint != "" {
		writer = s3adapter.NewWriterWithOptions(awsCfg, logger, func(o *awss3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	} else {
		writer = s3adapter.NewWriter(awsCfg, logger)
	}

	archiver := s3adapter.NewCallArchiver(writer, bucket, logger)

	var wg sync.WaitGroup
	wrap := func(inner outbound.Multicaller) outbound.Multicaller {
		return archiving.NewMulticaller(inner, archiver, archiving.Config{
			Source:  source,
			ChainID: chainID,
			BuildID: buildID,
			Wait:    &wg,
			Logger:  logger,
		})
	}
	drain := func() { wg.Wait() }

	return wrap, drain, nil
}
