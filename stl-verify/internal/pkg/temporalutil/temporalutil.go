// Package temporalutil provides shared infrastructure for Temporal cronjob workers.
//
// To create a new cronjob, define a CronjobConfig and call RunCronjob:
//
//	func main() {
//	    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
//	    defer cancel()
//
//	    if err := temporalutil.RunCronjob(ctx, meta, temporalutil.CronjobConfig{
//	        Name:            "my-cronjob",
//	        TaskQueue:       "my-queue",
//	        ScheduleID:      "my-schedule",
//	        IntervalDefault: "5m",
//	        Workflow:        myWorkflow,
//	        Setup:           createActivities,
//	    }); err != nil {
//	        slog.Error("fatal", "error", err)
//	        os.Exit(1)
//	    }
//	}
package temporalutil

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

// BuildMeta holds build-time metadata injected via ldflags.
type BuildMeta struct {
	Commit    string
	Branch    string
	BuildTime string
}

// Dependencies are the shared resources available to every cronjob's Setup function.
type Dependencies struct {
	Pool    *pgxpool.Pool
	Logger  *slog.Logger
	ChainID int
}

// CronjobConfig defines everything needed to run a Temporal cronjob worker.
type CronjobConfig struct {
	// Name identifies this cronjob in logs (e.g. "offchain-price-indexer").
	Name string

	// TaskQueue is the Temporal task queue this worker polls.
	TaskQueue string

	// Schedule configuration.
	ScheduleID      string // defaults to Name if empty
	IntervalEnv     string // env var name for the interval (optional)
	IntervalDefault string // default interval (e.g. "5m", "1h")
	WorkflowID      string // defaults to "scheduled-"+Name if empty

	// Workflow is the workflow function to register and schedule.
	Workflow     any
	WorkflowArgs []any // args passed to the workflow when the schedule triggers

	// Setup creates the activities struct for this cronjob.
	// Return a single activities struct; it will be registered on the worker.
	Setup func(ctx context.Context, deps Dependencies) (activities any, err error)
}

// RunCronjob runs a Temporal cronjob worker end-to-end: sets up logging,
// connects to the database and Temporal, registers the workflow/activities,
// ensures the schedule exists, and runs the worker until ctx is cancelled.
func RunCronjob(ctx context.Context, meta BuildMeta, cfg CronjobConfig) error {
	if cfg.ScheduleID == "" {
		cfg.ScheduleID = cfg.Name
	}
	if cfg.WorkflowID == "" {
		cfg.WorkflowID = "scheduled-" + cfg.Name
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	logger.Info("starting "+cfg.Name+" worker",
		"commit", meta.Commit,
		"branch", meta.Branch,
		"buildTime", meta.BuildTime,
	)

	chainID, err := getChainID()
	if err != nil {
		return err
	}

	pool, err := openDatabase(ctx)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer pool.Close()

	temporalClient, err := createClient()
	if err != nil {
		return fmt.Errorf("creating temporal client: %w", err)
	}
	defer temporalClient.Close()

	if err := waitForServer(ctx, temporalClient, logger); err != nil {
		return fmt.Errorf("waiting for Temporal: %w", err)
	}

	activities, err := cfg.Setup(ctx, Dependencies{
		Pool:    pool,
		Logger:  logger,
		ChainID: chainID,
	})
	if err != nil {
		return fmt.Errorf("setting up %s: %w", cfg.Name, err)
	}

	w := worker.New(temporalClient, cfg.TaskQueue, worker.Options{})
	w.RegisterWorkflow(cfg.Workflow)
	w.RegisterActivity(activities)

	if err := ensureSchedule(ctx, temporalClient, logger, cfg); err != nil {
		return fmt.Errorf("ensuring schedule: %w", err)
	}

	logger.Info("starting worker", "taskQueue", cfg.TaskQueue)

	if err := w.Run(interruptFromContext(ctx)); err != nil {
		return fmt.Errorf("running worker: %w", err)
	}

	logger.Info("worker stopped")
	return nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func createClient() (client.Client, error) {
	hostPort := env.Get("TEMPORAL_HOST_PORT", "localhost:7233")
	namespace := env.Get("TEMPORAL_NAMESPACE", "sentinel")

	return client.Dial(client.Options{
		HostPort:  hostPort,
		Namespace: namespace,
	})
}

func waitForServer(ctx context.Context, c client.Client, logger *slog.Logger) error {
	for {
		_, err := c.WorkflowService().GetSystemInfo(ctx, &workflowservicepb.GetSystemInfoRequest{})
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if st, ok := grpcstatus.FromError(err); ok {
			switch st.Code() {
			case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
				// Transient — keep retrying.
			default:
				return fmt.Errorf("unexpected Temporal error: %w", err)
			}
		}
		logger.Info("waiting for Temporal to become ready", "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func ensureSchedule(ctx context.Context, c client.Client, logger *slog.Logger, cfg CronjobConfig) error {
	interval := env.Get(cfg.IntervalEnv, cfg.IntervalDefault)

	intervalDuration, err := time.ParseDuration(interval)
	if err != nil {
		return fmt.Errorf("parsing %s %q: %w", cfg.IntervalEnv, interval, err)
	}

	// Attempt to create the schedule directly. Temporal returns AlreadyExists if it is
	// already present (normal after any restart). Skipping a Describe-first check avoids
	// a race window and also sidesteps the case where Describe returns an internal error
	// due to a stuck workflow task left over from a previous run.
	//
	// Note: changes to the interval env var will NOT take effect until the existing
	// schedule is deleted from Temporal and the worker is restarted.
	// Use the Temporal UI or CLI to delete a schedule.
	_, err = c.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID: cfg.ScheduleID,
		Spec: client.ScheduleSpec{
			Intervals: []client.ScheduleIntervalSpec{
				{Every: intervalDuration},
			},
		},
		Action: &client.ScheduleWorkflowAction{
			Workflow:  cfg.Workflow,
			Args:      cfg.WorkflowArgs,
			ID:        cfg.WorkflowID,
			TaskQueue: cfg.TaskQueue,
		},
	})
	if err != nil {
		// The Temporal SDK wraps the gRPC AlreadyExists error inconsistently across
		// SDK versions, so we check both the gRPC status code and the error message.
		if grpcstatus.Code(err) == codes.AlreadyExists || strings.Contains(err.Error(), "already registered") {
			logger.Info("schedule already exists", "scheduleID", cfg.ScheduleID)
			return nil
		}
		return fmt.Errorf("creating schedule %q: %w", cfg.ScheduleID, err)
	}

	logger.Info("schedule created", "scheduleID", cfg.ScheduleID, "interval", intervalDuration)
	return nil
}

func openDatabase(ctx context.Context) (*pgxpool.Pool, error) {
	postgresURL := env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")
	return postgres.OpenPool(ctx, postgres.DefaultDBConfig(postgresURL))
}

func getChainID() (int, error) {
	chainIDStr := env.Get("CHAIN_ID", "1")
	chainID, err := strconv.Atoi(chainIDStr)
	if err != nil {
		return 0, fmt.Errorf("CHAIN_ID must be a valid integer: %w", err)
	}
	return chainID, nil
}

func interruptFromContext(ctx context.Context) <-chan any {
	ch := make(chan any)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch
}
