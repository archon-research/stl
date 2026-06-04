// Package archivingwire builds an S3-backed archiving.Multicaller wrap from
// environment variables. It lives in its own package so the core archiving
// decorator stays free of any adapter imports.
package archivingwire

import (
	"context"
	"fmt"
	"log/slog"

	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/pkg/awsconfig"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// EnvFlag is the env var that gates SC-call archiving for every service.
const EnvFlag = "ARCHIVE_SC_CALLS"

// EnvBucket is the env var holding the S3 bucket name. Required when EnvFlag
// is "true".
const EnvBucket = "RAW_SC_BUCKET"

// Wrap decorates a single outbound.Multicaller.
type Wrap func(outbound.Multicaller) outbound.Multicaller

// Enabled reports whether ARCHIVE_SC_CALLS=true in the environment.
func Enabled() bool {
	return env.Get(EnvFlag, "") == "true"
}

// NewS3WrapFromEnv builds an S3-backed CallArchiver from RAW_SC_BUCKET and
// returns a wrap function that callers apply to each Multicaller they
// construct. It is the standard wiring entry point for every cmd/* binary
// that uses the archiving decorator.
//
// Resolution: RAW_SC_BUCKET (required), AWS_ENDPOINT_URL (optional, for
// LocalStack and friends), AWS_REGION (via awsconfig.Load default).
//
// Returns an error only on configuration problems (missing bucket, bad AWS
// config). Callers should only invoke it after Enabled() returns true.
func NewS3WrapFromEnv(ctx context.Context, logger *slog.Logger, chainID int64, buildID int, source string) (Wrap, error) {
	bucket := env.Get(EnvBucket, "")
	if bucket == "" {
		return nil, fmt.Errorf("%s must be set when %s=true", EnvBucket, EnvFlag)
	}
	awsCfg, err := awsconfig.Load(ctx, awsconfig.Options{
		Endpoint:                 env.Get("AWS_ENDPOINT_URL", ""),
		StaticCredentialsFromEnv: true,
	})
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	archiver, err := s3adapter.NewCallArchiver(awsCfg, bucket, logger)
	if err != nil {
		return nil, fmt.Errorf("building S3 call archiver: %w", err)
	}
	logger.Info("raw SC-call archiving enabled",
		"bucket", bucket, "source", source, "chain_id", chainID, "build_id", buildID)
	return func(mc outbound.Multicaller) outbound.Multicaller {
		return archiving.New(mc, archiver, chainID, buildID, source)
	}, nil
}
