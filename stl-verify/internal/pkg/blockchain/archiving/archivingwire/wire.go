// Package archivingwire wires the raw SC call archiver from environment config.
package archivingwire

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

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

// DrainTimeout bounds the wait for in-flight archive writes at shutdown. It
// covers archiving.ArchiveTimeout so a write the drain kills is one that
// outlived its own bound, never a healthy slow PUT; a write killed here is
// unrecoverable, since its SQS message is already deleted. The drain is
// deferred, so lifecycle.ShutdownTailBudget must hold it plus the OTEL flush.
const DrainTimeout = 35 * time.Second

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

// Bootstrap returns the archiving Wrap, a reusable wait and the process-exit
// drain for a worker entrypoint. When ARCHIVE_SC_CALLS is unset the Wrap is the
// identity and both funcs are no-ops, so callers wire them unconditionally:
//
//	wrap, wait, drain, err := archivingwire.Bootstrap(ctx, logger, chainID, buildID, "source")
//	if err != nil { return err }
//	defer drain()
//	mc = wrap(mc)
//
// wait blocks on the writes in flight and leaves archiving usable afterwards, so
// a unit of work that ends (a Temporal activity) can wait out its own writes.
// drain shuts archiving for good and belongs at the process's exit, once.
//
// This keeps the enable/build/log/drain wiring in one place instead of repeating
// it across every cmd binary.
func Bootstrap(ctx context.Context, logger *slog.Logger, chainID, buildID int64, source string) (Wrap, func(), func(), error) {
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
		return identityWrap, func() {}, func() {}, nil
	}
	wrap, wait, drain, err := NewS3WrapFromEnv(ctx, logger, chainID, buildID, source)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("wiring SC call archiver: %w", err)
	}
	logger.Info("raw SC call archiving enabled", "bucket", env.Get(EnvBucket, ""))
	return wrap, wait, drain, nil
}

// NewS3WrapFromEnv builds the archiving wrap from env config, plus the two
// shutdown handles that go with it. Both block until the writes in flight
// finish or DrainTimeout expires: wait leaves archiving usable afterwards,
// drain refuses every later write and counts the ones it kills as lost. All
// decorators produced by the wrap share one drain gate, so one pair covers them.
func NewS3WrapFromEnv(ctx context.Context, logger *slog.Logger, chainID, buildID int64, source string) (Wrap, func(), func(), error) {
	if logger == nil {
		logger = slog.Default()
	}

	bucket := env.Get(EnvBucket, "")
	if bucket == "" {
		return nil, nil, nil, fmt.Errorf("%s is required when %s=true", EnvBucket, EnvFlag)
	}

	chainName, err := entity.ChainName(chainID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("resolving chain name for archiving metrics: %w", err)
	}

	awsCfg, err := awsconfig.Load(ctx, awsconfig.Options{StaticCredentialsFromEnv: true})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("loading AWS config: %w", err)
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

	archiver, err := s3adapter.NewCallArchiver(writer, bucket, chainName, logger, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("creating call archiver: %w", err)
	}

	gate := archiving.NewDrainGate(logger)
	wrap := func(inner outbound.Multicaller) outbound.Multicaller {
		return archiving.NewMulticaller(inner, archiver, archiving.Config{
			Source:  source,
			ChainID: chainID,
			Chain:   chainName,
			BuildID: buildID,
			Gate:    gate,
			Logger:  logger,
		})
	}
	writes := archiving.NewWriteCounter(nil, chainName, source, logger)
	return wrap, newWait(gate, logger, DrainTimeout), newDrain(gate, logger, writes, DrainTimeout), nil
}

// newWait waits out the writes in flight without closing the gate, so the next
// unit of work on the same process still archives. The writes it leaves running
// keep going and still record their own outcome.
func newWait(gate *archiving.DrainGate, logger *slog.Logger, budget time.Duration) func() {
	return func() {
		if finished, outstanding := gate.WaitBounded(budget); !finished {
			logger.Warn("raw SC call archive writes outlasted the wait budget; leaving them running",
				"budget", budget,
				"outstanding", outstanding)
		}
	}
}

// A non-positive budget would abandon every write in flight on every shutdown,
// including ones already about to land, so it floors to DrainTimeout the way
// sqsutil's drain floors to its own default.
func newDrain(gate *archiving.DrainGate, logger *slog.Logger, writes *archiving.WriteCounter, budget time.Duration) func() {
	if budget <= 0 {
		budget = DrainTimeout
	}
	return func() {
		finished, lost := gate.Drain(budget)
		if finished {
			return
		}
		// The gate stopped these batches from claiming an outcome, so this is the
		// only status they get; their queue message is already deleted.
		writes.Record(archiving.WriteStatusLost, int64(lost))
		logger.Warn("raw SC call archive drain budget expired; abandoning in-flight writes",
			"budget", budget,
			"lost", lost)
	}
}
