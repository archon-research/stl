// Command block-meta-loader is an on-demand Temporal worker that fills the
// block_meta dimension for ONE chain from that chain's S3 raw-block archive (the
// authoritative block-header timestamp). One deployment serves one chain, on that
// chain's task queue.
//
// It is a worker rather than a Job because the run must be started by an explicit
// operator action, never by a deploy or an ArgoCD sync: the deployment idles on
// its task queue with no schedule, and an operator starts a run from the Temporal
// UI. The workflow id guards concurrency, cancel is a button, and a timed-out or
// cancelled attempt resumes from the committed work list rather than
// re-enumerating the chain.
//
//	temporal workflow start \
//	  --task-queue block-meta-loader \
//	  --type BlockMetaLoad \
//	  --workflow-id block-meta-loader-ethereum-2026-09-14 \
//	  --input '{}'
//
// Env: DATABASE_URL (write role), CHAIN_ID, S3_BUCKET (that chain's raw-block
// bucket), DEPLOY_ENV. Optional: AWS_REGION, AWS_S3_ENDPOINT (LocalStack),
// BATCH_SIZE, LOG_LEVEL.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/awsconfig"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockmetacfg"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	err := run(ctx)
	cancel()
	if code := exitCode(err); code != 0 {
		slog.Error("block-meta-loader exited with error", "error", err)
		os.Exit(code)
	}
}

// exitCode decides what a finished run is worth to the supervisor. It is a function rather than an
// inline branch in main so the decision is testable without exiting the test binary.
//
// A SIGTERM arrives as a cancelled context and is how a drain or a deliberate stop reaches this
// process, so it exits clean: counted as a failure it would consume a restart budget, and two node
// drains in a long run would leave one real attempt.
func exitCode(err error) int {
	if err == nil || errors.Is(err, context.Canceled) {
		return 0
	}
	return 1
}

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.Populate(&GitCommit, &GitBranch, &BuildTime)
}

// workflowTypeName is what an operator types into the Temporal UI's "Workflow
// Type" field, so it is registered explicitly rather than derived from the Go
// function name — a rename must not invalidate the runbook or muscle memory.
// queueBaseName is this component's deployed name.
const queueBaseName = "block-meta-loader"

const workflowTypeName = "BlockMetaLoad"

func run(ctx context.Context) error {
	taskQueue, err := chainutil.TaskQueueName(queueBaseName)
	if err != nil {
		return fmt.Errorf("resolving the task queue: %w", err)
	}

	cfg, err := blockmetacfg.Load()
	if err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	return temporal.RunWorker(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.WorkerConfig{
		Name:         taskQueue,
		OpenDatabase: postgres.PoolOpener(postgres.DefaultDBConfig(cfg.DSN)),
		Register: func(ctx context.Context, deps temporal.Dependencies, r worker.Registry) error {
			return register(ctx, cfg, deps, r)
		},
	})
}

func register(ctx context.Context, cfg blockmetacfg.Config, deps temporal.Dependencies, r worker.Registry) error {
	reader, err := newS3Reader(ctx, deps.Logger)
	if err != nil {
		return err
	}

	// s3:ListBucket and s3:GetObject come from an EKS Pod Identity association granted in the infra
	// repo. Proven here, a missing grant is a pod that will not start rather than a run an operator
	// started and has to come back to.
	if err := s3adapter.NewArchiveReader(reader, cfg.Bucket).Ping(ctx); err != nil {
		return fmt.Errorf("the raw archive %s is unusable: %w", cfg.Bucket, err)
	}

	activities := &loadActivities{cfg: cfg, pool: deps.Pool, reader: reader, logger: deps.Logger}

	r.RegisterWorkflowWithOptions(loadWorkflow, workflow.RegisterOptions{Name: workflowTypeName})
	activities.register(r)

	deps.Logger.Info("block-meta-loader configured",
		"chainID", cfg.ChainID, "bucket", cfg.Bucket, "environment", cfg.DeployEnv)
	return nil
}

// newS3Reader builds the raw-block archive reader. The endpoint override is
// s3adapter's, so it honours AWS_S3_ENDPOINT like every other worker: the dev
// overlay and the kind targets set that, and a local variable of our own would
// resolve real S3 in kind and fail on credentials.
func newS3Reader(ctx context.Context, logger *slog.Logger) (*s3adapter.Reader, error) {
	awsCfg, err := awsconfig.Load(ctx, awsconfig.Options{})
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	return s3adapter.NewReaderFromEnv(awsCfg, logger), nil
}
