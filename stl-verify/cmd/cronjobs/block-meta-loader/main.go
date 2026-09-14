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
// bucket), DEPLOY_ENV. Optional: AWS_REGION (default eu-west-1), AWS_ENDPOINT_URL
// (LocalStack), BATCH_SIZE, LOG_LEVEL.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	awssdkconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	err := run(ctx)
	cancel()
	if err != nil {
		slog.Error("block-meta-loader exited with error", "error", err)
		os.Exit(1)
	}
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
const workflowTypeName = "BlockMetaLoad"

func run(ctx context.Context) error {
	taskQueue, err := taskQueueName()
	if err != nil {
		return fmt.Errorf("resolving the task queue: %w", err)
	}

	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	return temporal.RunWorker(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.WorkerConfig{
		Name:         taskQueue,
		OpenDatabase: postgres.PoolOpener(postgres.DefaultDBConfig(cfg.dsn)),
		Register: func(ctx context.Context, deps temporal.Dependencies, r worker.Registry) error {
			return register(ctx, cfg, deps, r)
		},
	})
}

func register(ctx context.Context, cfg config, deps temporal.Dependencies, r worker.Registry) error {
	reader, err := newS3Reader(ctx, deps.Logger)
	if err != nil {
		return err
	}

	activities := &loadActivities{cfg: cfg, pool: deps.Pool, reader: reader, logger: deps.Logger}

	r.RegisterWorkflowWithOptions(loadWorkflow, workflow.RegisterOptions{Name: workflowTypeName})
	activities.register(r)

	deps.Logger.Info("block-meta-loader configured",
		"chainID", cfg.chainID, "bucket", cfg.bucket, "environment", cfg.deployEnv)
	return nil
}

// newS3Reader builds the raw-block archive reader from the environment. A custom
// endpoint (LocalStack) needs path-style addressing; virtual-hosted URLs won't
// resolve against it.
func newS3Reader(ctx context.Context, logger *slog.Logger) (*s3adapter.Reader, error) {
	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		awsRegion = "eu-west-1"
	}
	awsOpts := []func(*awssdkconfig.LoadOptions) error{awssdkconfig.WithRegion(awsRegion)}
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint != "" {
		awsOpts = append(awsOpts, awssdkconfig.WithBaseEndpoint(endpoint))
		logger.Info("using custom AWS endpoint", "url", endpoint)
	}
	awsCfg, err := awssdkconfig.LoadDefaultConfig(ctx, awsOpts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	if endpoint == "" {
		return s3adapter.NewReader(awsCfg, logger), nil
	}
	return s3adapter.NewReaderWithOptions(awsCfg, logger, func(o *s3.Options) {
		o.UsePathStyle = true
	}), nil
}
