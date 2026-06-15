// Package archivingwire wires the raw SC call archiver from environment config.
package archivingwire

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
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

// Enabled reports whether ARCHIVE_SC_CALLS is set to a truthy value. It accepts
// anything strconv.ParseBool does (1, t, T, TRUE, true, True, ...) so a common
// value like "1" enables archiving instead of being silently ignored.
func Enabled() bool {
	enabled, err := strconv.ParseBool(env.Get(EnvFlag, ""))
	return err == nil && enabled
}

// identityWrap returns its argument unchanged; used when archiving is disabled
// so callers can apply the returned Wrap unconditionally.
func identityWrap(inner outbound.Multicaller) outbound.Multicaller { return inner }

// Bootstrap returns the archiving Wrap and a drain func for a worker entrypoint.
// When ARCHIVE_SC_CALLS is unset the Wrap is the identity and drain is a no-op,
// so callers wire it unconditionally:
//
//	wrap, drain, err := archivingwire.Bootstrap(ctx, logger, chainID, buildID, "source")
//	if err != nil { return err }
//	defer drain()
//	mc = wrap(mc)
//
// This keeps the enable/build/log/drain wiring in one place instead of repeating
// it across every cmd binary.
func Bootstrap(ctx context.Context, logger *slog.Logger, chainID, buildID int64, source string) (Wrap, func(), error) {
	if logger == nil {
		logger = slog.Default()
	}
	if !Enabled() {
		// Log the resolved state so a mistyped flag (e.g. ARCHIVE_SC_CALLS=yes) is
		// visible at startup rather than silently leaving archiving off. Warn loudly
		// when the value is non-empty but unparseable, since that signals intent to
		// enable archiving that we are not honouring.
		if raw := env.Get(EnvFlag, ""); raw != "" {
			if _, err := strconv.ParseBool(raw); err != nil {
				logger.Warn("ARCHIVE_SC_CALLS set to an unrecognised value; archiving stays off", EnvFlag, raw)
			}
		}
		logger.Info("raw SC call archiving disabled")
		return identityWrap, func() {}, nil
	}
	wrap, drain, err := NewS3WrapFromEnv(ctx, logger, chainID, buildID, source)
	if err != nil {
		return nil, nil, fmt.Errorf("wiring SC call archiver: %w", err)
	}
	logger.Info("raw SC call archiving enabled", "bucket", env.Get(EnvBucket, ""))
	return wrap, drain, nil
}

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

	chainName, err := entity.ChainName(chainID)
	if err != nil {
		return nil, nil, fmt.Errorf("resolving chain name for archiving metrics: %w", err)
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

	archiver, err := s3adapter.NewCallArchiver(writer, bucket, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("creating call archiver: %w", err)
	}

	wg := &archiving.WriteGroup{}
	wrap := func(inner outbound.Multicaller) outbound.Multicaller {
		return archiving.NewMulticaller(inner, archiver, archiving.Config{
			Source:  source,
			ChainID: chainID,
			Chain:   chainName,
			BuildID: buildID,
			Wait:    wg,
			Logger:  logger,
		})
	}
	drain := func() { wg.Wait() }

	return wrap, drain, nil
}
