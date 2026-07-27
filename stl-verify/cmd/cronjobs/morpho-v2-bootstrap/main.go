// Package main implements the morpho-v2-bootstrap Temporal cronjob: a one-shot,
// operator-triggered repair for Morpho VaultV2 vaults that were discovered
// before VaultV2 discovery became atomic (VEC-218).
//
// # What it heals
//
// Those vaults have a morpho_vault row but no morpho_adapter, adapter_state,
// vault_cap or vault_fee rows. Live indexing will never fix them on its own: the
// worker short-circuits on IsKnownVault before it would enumerate a vault's
// adapters, and the AddAdapter / cap / fee events that would have built those
// rows are historical, so they never arrive on SNS/SQS again. One run enumerates
// every V2 vault's current adapter set at a pinned finalized block, snapshots
// each adapter's realAssets(), and replays the full VaultV2 governance-event
// history through the live handler path.
//
// # How to trigger it (the button)
//
// The schedule is created PAUSED and with no interval, so deploying this worker
// never starts a run. To run it:
//
//	Temporal UI → Schedules → morpho-v2-bootstrap → Trigger
//
// That fires exactly one workflow. The schedule stays paused afterwards. The run
// takes hours (a full mainnet log sweep), which is why the activity timeouts
// below are far larger than the shared cronjob defaults.
//
// # Idempotency
//
// Every write goes through the same idempotent repository methods live indexing
// uses (converging GetOrCreateAdapter, ON CONFLICT DO NOTHING snapshots), so
// clicking Trigger again is safe — it redoes the work and converges to the same
// state. Any failure fails the whole run and shows red in the Temporal UI; the
// fix is to re-trigger once the cause is addressed.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpchttp"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_v2_bootstrap"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil {
		slog.Error("morpho-v2-bootstrap exited with error", "error", err)
		os.Exit(1)
	}
}

// Build metadata, populated from VCS in init() (GitBranch is set at link time).
var (
	GitCommit string
	GitBranch string
	BuildTime string
)

func init() {
	buildinfo.PopulateFromVCS(&GitCommit, &BuildTime)
}

func run(ctx context.Context) error {
	// Require DATABASE_URL rather than default to localhost: a deployed worker
	// that silently connected to a local (empty) database would look healthy
	// while healing nothing.
	dbURL, err := env.Require("DATABASE_URL")
	if err != nil {
		return fmt.Errorf("startup configuration: %w", err)
	}

	return temporal.RunCronjob(ctx, temporal.BuildMeta{
		Commit: GitCommit, Branch: GitBranch, BuildTime: BuildTime,
	}, temporal.CronjobConfig{
		Name:             "morpho-v2-bootstrap",
		ManualOnly:       true,
		ActivityTimeouts: bootstrapActivityTimeouts,
		OpenDatabase:     postgres.PoolOpener(postgres.DefaultDBConfig(dbURL)),
		Setup:            setupRunner,
	})
}

// bootstrapActivityTimeouts sizes one run against a full mainnet sweep: ~2M
// blocks of eth_getLogs plus a per-vault adapter enumeration. The shared 10m
// default would kill it mid-sweep. MaximumAttempts is 1 because a failed run is
// something an operator should look at before re-triggering — an automatic retry
// would just burn another multi-hour attempt against the same cause.
var bootstrapActivityTimeouts = temporal.ActivityTimeouts{
	StartToClose:    12 * time.Hour,
	ScheduleToClose: 24 * time.Hour,
	MaximumAttempts: 1,
}

func setupRunner(ctx context.Context, deps temporal.Dependencies) (temporal.Runner, error) {
	chainID, err := chainutil.RequireChainID()
	if err != nil {
		return nil, err
	}

	sweepConfig, err := parseSweepConfig(os.Getenv)
	if err != nil {
		return nil, err
	}
	sweepConfig.ChainID = int64(chainID)
	sweepConfig.Logger = deps.Logger

	rpcURL, err := resolveRPCURL(os.Getenv)
	if err != nil {
		return nil, err
	}
	// The sweep issues long, wide eth_getLogs requests; the default 60s client
	// budget would abort them before the node finished collecting results.
	ethClient, err := rpchttp.DialEthereum(ctx, rpcURL, rpchttp.WithClientTimeout(5*time.Minute))
	if err != nil {
		return nil, fmt.Errorf("connecting to RPC: %w", err)
	}

	replayService, err := buildReplayService(ctx, deps, int64(chainID), ethClient)
	if err != nil {
		return nil, err
	}

	service, err := morpho_v2_bootstrap.NewService(sweepConfig, ethClient, replayService)
	if err != nil {
		return nil, fmt.Errorf("creating morpho v2 bootstrap service: %w", err)
	}
	return temporal.RunnerFunc(service.Run), nil
}

// buildReplayService wires the morpho-indexer service in its replay
// configuration — the same one the morpho-vault-indexer backfiller uses. The
// bootstrap drives the real handlers through it rather than reimplementing them.
func buildReplayService(ctx context.Context, deps temporal.Dependencies, chainID int64, ethClient *ethclient.Client) (*morpho_indexer.Service, error) {
	buildReg, err := buildregistry.New(ctx, deps.Pool)
	if err != nil {
		return nil, fmt.Errorf("registering build: %w", err)
	}

	multicaller, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return nil, fmt.Errorf("creating multicall client: %w", err)
	}
	txManager, err := postgres.NewTxManager(deps.Pool, deps.Logger)
	if err != nil {
		return nil, fmt.Errorf("creating tx manager: %w", err)
	}
	morphoRepo, err := postgres.NewMorphoRepository(deps.Pool, deps.Logger, buildReg.BuildID())
	if err != nil {
		return nil, fmt.Errorf("creating morpho repository: %w", err)
	}
	protocolRepo, err := postgres.NewProtocolRepository(deps.Pool, deps.Logger, buildReg.BuildID(), 0)
	if err != nil {
		return nil, fmt.Errorf("creating protocol repository: %w", err)
	}
	eventRepo := postgres.NewEventRepository(deps.Logger, buildReg.BuildID())

	svcConfig := morpho_indexer.ConfigDefaults()
	svcConfig.ChainID = chainID
	svcConfig.Logger = deps.Logger

	replayService, err := morpho_indexer.NewReplayService(svcConfig, multicaller, txManager, protocolRepo, morphoRepo, eventRepo)
	if err != nil {
		return nil, fmt.Errorf("creating morpho replay service: %w", err)
	}
	return replayService, nil
}

// parseSweepConfig reads the two sweep tunables, defaulting both. getenv is
// injected so the parsing is unit-testable.
func parseSweepConfig(getenv func(string) string) (morpho_v2_bootstrap.Config, error) {
	cfg := morpho_v2_bootstrap.ConfigDefaults()

	if v := getenv("BOOTSTRAP_BLOCK_CHUNK_SIZE"); v != "" {
		size, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return morpho_v2_bootstrap.Config{}, fmt.Errorf("parsing BOOTSTRAP_BLOCK_CHUNK_SIZE %q: %w", v, err)
		}
		cfg.BlockChunkSize = size
	}
	if v := getenv("BOOTSTRAP_ADDRESS_BATCH_SIZE"); v != "" {
		size, err := strconv.Atoi(v)
		if err != nil {
			return morpho_v2_bootstrap.Config{}, fmt.Errorf("parsing BOOTSTRAP_ADDRESS_BATCH_SIZE %q: %w", v, err)
		}
		cfg.AddressBatchSize = size
	}
	return cfg, nil
}

// resolveRPCURL builds the node URL from the same ALCHEMY_HTTP_URL +
// ALCHEMY_API_KEY pair every other indexer uses, so this cronjob's secret wiring
// matches the workers'.
func resolveRPCURL(getenv func(string) string) (string, error) {
	apiKey := getenv("ALCHEMY_API_KEY")
	if apiKey == "" {
		return "", fmt.Errorf("ALCHEMY_API_KEY environment variable is required")
	}
	baseURL := getenv("ALCHEMY_HTTP_URL")
	if baseURL == "" {
		baseURL = "https://eth-mainnet.g.alchemy.com/v2"
	}
	return fmt.Sprintf("%s/%s", baseURL, apiKey), nil
}
