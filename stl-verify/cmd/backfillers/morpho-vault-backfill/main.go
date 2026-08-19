// Package main implements an on-demand Temporal worker that backfills Morpho
// vaults from the archived Ethereum receipts in S3.
//
// A run has two phases. Discovery scans the range's receipt files for Morpho
// Blue events and VaultV2 AccrueInterest events, probes the candidate addresses
// on-chain via multicall (MORPHO() must return the Morpho Blue singleton) and
// stores the confirmed vaults in morpho_vault. Replay then re-walks the same
// range, one S3 partition at a time in ascending block order, driving every
// persisted VaultV2 vault's structured events (adapter / cap / fee) through the
// handler path the live worker uses.
//
// It carries no schedule. The worker idles on its task queue until someone
// starts a run and supplies the range, either from the Temporal UI ("Start
// Workflow", Workflow Type "MorphoVaultBackfill") or via `temporal workflow
// start`. That is the whole reason it is not a cronjob: a backfill's window is
// an argument, and cronjobWorkflow accepts none.
//
// A run keeps no progress state of its own. Every completed partition is in the
// workflow's event history, so a retry resumes there; everything a run writes is
// an idempotent append, so redoing one costs wall clock, not correctness.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/awsconfig"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving/archivingwire"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpchttp"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
)

const (
	jobName = "morpho-vault-backfill"

	// workflowTypeName is what an operator types into the Temporal UI's "Workflow
	// Type" field, so it is registered explicitly rather than derived from the Go
	// method name — a rename must not invalidate the runbook or muscle memory.
	workflowTypeName = "MorphoVaultBackfill"

	// progressQueryName is queryable mid-run from the UI's Query tab, which is the
	// only way to see how far a long backfill has got without reading raw history.
	progressQueryName = "progress"

	defaultDatabaseURL = "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable"

	// rpcConcurrency is the historical RPC budget for this pipeline, deliberately
	// decoupled from config.goroutines (which sizes the S3 reader pool).
	rpcConcurrency = 10
)

var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() { buildinfo.PopulateFromVCS(&GitCommit, &BuildTime) }

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	return temporal.RunWorker(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.WorkerConfig{
		Name:         jobName,
		OpenDatabase: postgres.PoolOpener(postgres.DefaultDBConfig(env.Get("DATABASE_URL", defaultDatabaseURL))),
		Register:     register,
	})
}

func register(ctx context.Context, deps temporal.Dependencies, r worker.Registry) error {
	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("loading configuration: %w", err)
	}

	activities, err := newBackfillActivities(ctx, deps, cfg)
	if err != nil {
		return fmt.Errorf("wiring the backfill activities: %w", err)
	}

	workflows := &backfillWorkflows{chainID: cfg.chainID}
	r.RegisterWorkflowWithOptions(workflows.Backfill, workflow.RegisterOptions{Name: workflowTypeName})
	r.RegisterActivity(activities)
	return nil
}

func newBackfillActivities(ctx context.Context, deps temporal.Dependencies, cfg config) (*backfillActivities, error) {
	buildReg, err := buildregistry.New(ctx, deps.Pool)
	if err != nil {
		return nil, fmt.Errorf("registering build: %w", err)
	}

	s3Reader, err := newS3Reader(ctx, deps.Logger, cfg)
	if err != nil {
		return nil, err
	}

	ethClient, err := dialChain(ctx, cfg)
	if err != nil {
		return nil, err
	}

	multicaller, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return nil, fmt.Errorf("creating multicall client: %w", err)
	}

	// Optional raw SC call archiving (VEC-81). Off unless ARCHIVE_SC_CALLS=true.
	archiveWrap, archiveDrain, err := archivingwire.Bootstrap(ctx, deps.Logger, cfg.chainID, int64(buildReg.BuildID()), "morpho-vault")
	if err != nil {
		return nil, err
	}
	multicaller = archiveWrap(multicaller)

	prober, err := newVaultProber(deps.Logger, multicaller)
	if err != nil {
		return nil, err
	}

	extractor, err := morpho_indexer.NewEventExtractor()
	if err != nil {
		return nil, fmt.Errorf("creating event extractor: %w", err)
	}

	return &backfillActivities{
		cfg:          cfg,
		logger:       deps.Logger,
		pool:         deps.Pool,
		buildID:      buildReg.BuildID(),
		s3Reader:     s3Reader,
		extractor:    extractor,
		prober:       prober,
		ethClient:    ethClient,
		multicaller:  multicaller,
		archiveDrain: archiveDrain,
	}, nil
}

// newS3Reader sizes its connection pool off the scan's worker count, which is
// what saturates it; the default transport's 2 idle conns per host would
// serialise them. AWS_S3_ENDPOINT is honoured alongside that so the worker can
// be pointed at LocalStack in kind and in the integration tests.
func newS3Reader(ctx context.Context, logger *slog.Logger, cfg config) (*s3adapter.Reader, error) {
	awsCfg, err := awsconfig.Load(ctx, awsconfig.Options{StaticCredentialsFromEnv: true})
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	conns := cfg.goroutines + 64
	httpClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          conns,
			MaxIdleConnsPerHost:   conns,
			MaxConnsPerHost:       conns,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
	options := append([]func(*s3.Options){func(o *s3.Options) { o.HTTPClient = httpClient }},
		s3adapter.EndpointOptionsFromEnv()...)
	return s3adapter.NewReaderWithOptions(awsCfg, logger, options...), nil
}

// dialChain connects to the node and refuses a chain that disagrees with
// CHAIN_ID: every block number in a run's range is meaningless on another chain,
// and the mismatch would surface as missing S3 keys rather than as itself.
func dialChain(ctx context.Context, cfg config) (*ethclient.Client, error) {
	// Retry 429/5xx/network errors via rpchttp so transient RPC failures don't
	// fail a partition that would have succeeded.
	rpcClient, err := rpc.DialOptions(ctx, cfg.rpcURL, rpc.WithHTTPClient(rpchttp.NewBackfillerClient(rpcConcurrency)))
	if err != nil {
		return nil, fmt.Errorf("connecting to RPC: %w", err)
	}

	ethClient := ethclient.NewClient(rpcClient)
	rpcChainID, err := ethClient.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching RPC chain ID: %w", err)
	}
	if rpcChainID.Int64() != cfg.chainID {
		return nil, fmt.Errorf("RPC chain ID mismatch: RPC reports %d, config says %d", rpcChainID.Int64(), cfg.chainID)
	}
	return ethClient, nil
}

func newVaultProber(logger *slog.Logger, multicaller outbound.Multicaller) (*vaultProber, error) {
	sharedProber, err := morpho_indexer.NewVaultProber()
	if err != nil {
		return nil, fmt.Errorf("creating vault prober: %w", err)
	}
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return nil, fmt.Errorf("loading ERC20 ABI: %w", err)
	}
	return &vaultProber{
		multicaller:  multicaller,
		sharedProber: sharedProber,
		erc20ABI:     erc20ABI,
		logger:       logger,
	}, nil
}
