package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"
	temporalsdk "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/blocktime"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
)

// maxPartitionsPerRun catches a mistyped range and keeps a run inside Temporal's
// 51,200-event history (~6 events/activity caps it at ~8,500); 8M blocks reaches 2028.
const maxPartitionsPerRun = 8_000

// maxPlausibleBlock bounds where a range may SIT, where maxPartitionsPerRun
// bounds how WIDE it may be. 1e11 is ~38,000 years of Ethereum blocks, while a
// pasted millisecond timestamp is ~1.75e12 and so is rejected.
//
// It is what keeps both partition walks — replayPartitionPrefixes, run in
// workflow code, and partitionsForRange, run inside the discovery activity —
// from overflowing. Each steps a cursor from at most `from` by BlockRangeSize
// while it is <= `to`, so with 0 < from <= to <= 1e11 the cursor tops out just
// above 1e11: eight orders of magnitude below math.MaxInt64, past which the
// cursor would wrap negative and the loop never terminate.
const maxPlausibleBlock = 100_000_000_000

// errStructuralData marks a failure that reproduces identically on every
// attempt: an S3 gap, an unparseable partition prefix, a log the archive cannot
// give coordinates for. A transient fault (S3/RPC/DB network error, timeout,
// throttling) must never wrap it — surviving those is what the retry envelope
// is for.
var errStructuralData = errors.New("structural data defect")

// discoveryScanPerPartition is what one 1000-block partition cost the discovery
// scan in the VEC-218 E2E: 253s over 22 partitions. It is the rate the discovery
// timeouts are sized from, so changing either moves the other.
const discoveryScanPerPartition = 11500 * time.Millisecond

// heartbeatInterval keeps the discovery scan visible to Temporal. Its
// StartToClose ceiling is hours, so without a heartbeat a worker killed mid-scan
// would hold the activity open for that whole ceiling instead of minutes.
const heartbeatInterval = time.Minute

// heartbeatTimeoutFactor is the grace Temporal allows between heartbeats,
// matching the shared cronjob path's factor.
const heartbeatTimeoutFactor = 3

// BackfillParams is the JSON an operator supplies in the Temporal UI's Input box:
//
//	{"from":24765588,"to":24786366}
//
// from and to are inclusive block numbers. fromV2Deploy defaults from to the
// chain's Morpho VaultV2 factory deploy block, so a whole-V2-era run is
// {"to":24786366,"fromV2Deploy":true}; an explicit from always wins. It narrows
// the WHOLE pipeline, not just the V2 replay: the discovery scan starts there
// too, so a V1/V1.1 vault whose only Morpho Blue activity predates the factory
// is not discovered by such a run.
type BackfillParams struct {
	From         int64 `json:"from"`
	To           int64 `json:"to"`
	FromV2Deploy bool  `json:"fromV2Deploy"`
}

// resolve applies fromV2Deploy and validates the result. chainID is a parameter
// rather than read from the environment because this runs in workflow code,
// where only replay-stable inputs are safe.
func (p BackfillParams) resolve(chainID int64) (BackfillParams, error) {
	if p.FromV2Deploy && p.From <= 0 {
		deployBlock, err := morpho_indexer.VaultV2FactoryDeployBlock(chainID)
		if err != nil {
			return BackfillParams{}, fmt.Errorf("fromV2Deploy: %w", err)
		}
		p.From = deployBlock
	}

	if p.From <= 0 {
		return BackfillParams{}, fmt.Errorf(
			"from must be a positive block number, or set fromV2Deploy to default it to the VaultV2 factory deploy block")
	}
	if p.To <= 0 {
		return BackfillParams{}, fmt.Errorf("to must be a positive block number")
	}
	if p.From > p.To {
		return BackfillParams{}, fmt.Errorf("from (%d) must be <= to (%d)", p.From, p.To)
	}
	if n := replayPartitionCount(p.From, p.To); n > maxPartitionsPerRun {
		return BackfillParams{}, fmt.Errorf(
			"this range expands to %d partitions, over the %d limit: split it into narrower ranges", n, maxPartitionsPerRun)
	}
	// Last, because the count guard above already rejects every implausible `to`
	// reached from a sane `from` — and does it with the more useful "split the
	// range" message. What it cannot see is a NARROW window sitting at the top of
	// int64, which is the one shape that overflows the partition walks.
	if p.To > maxPlausibleBlock {
		return BackfillParams{}, fmt.Errorf(
			"to (%d) is not a plausible block number: over the %d ceiling", p.To, maxPlausibleBlock)
	}
	return p, nil
}

// blockRange is the inclusive block window one run covers.
type blockRange struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

// partitionWork is one unit of replay: a single 1000-block S3 partition, clamped
// to the run's range.
type partitionWork struct {
	Range     blockRange `json:"range"`
	Partition string     `json:"partition"`
}

// discoveryResult is what phases 1-3 found: addresses worth probing, and the
// subset that probed as Morpho-family vaults and was persisted.
type discoveryResult struct {
	Candidates int `json:"candidates"`
	Vaults     int `json:"vaults"`

	// KnownV2Vaults is every VaultV2 in the database once this run's finds are
	// persisted, not just this run's own — it is what decides whether the replay
	// phase has anything to run against.
	KnownV2Vaults int `json:"knownV2Vaults"`
}

// BackfillResult is the workflow's return value, shown in the UI's Result panel.
type BackfillResult struct {
	Range          blockRange       `json:"range"`
	Discovered     *discoveryResult `json:"discovered,omitempty"`
	PartitionsRun  int              `json:"partitionsRun"`
	EventsReplayed int              `json:"eventsReplayed"`
}

type backfillProgress struct {
	Range           blockRange       `json:"range"`
	Discovered      *discoveryResult `json:"discovered,omitempty"`
	PartitionsTotal int              `json:"partitionsTotal"`
	PartitionsDone  int              `json:"partitionsDone"`
	EventsReplayed  int              `json:"eventsReplayed"`
}

// backfillWorkflows carries the deployment's chain, which the workflow needs to
// resolve fromV2Deploy without reading the environment from workflow code.
type backfillWorkflows struct {
	chainID int64
}

func (w *backfillWorkflows) Backfill(ctx workflow.Context, params BackfillParams) (BackfillResult, error) {
	logger := workflow.GetLogger(ctx)

	// Registered before validation so the Query tab answers for every run. Skip
	// it and a rejected run replies "unknown queryType progress", which reads
	// like a broken worker rather than a rejected request.
	var state backfillProgress
	if err := workflow.SetQueryHandler(ctx, progressQueryName, func() (backfillProgress, error) {
		return state, nil
	}); err != nil {
		return BackfillResult{}, fmt.Errorf("registering %q query handler: %w", progressQueryName, err)
	}

	resolved, err := params.resolve(w.chainID)
	if err != nil {
		// Bad input fails identically on every attempt, so retrying it would
		// only bury the mistake under the retry envelope.
		return BackfillResult{}, temporalsdk.NewNonRetryableApplicationError(
			"invalid backfill parameters", "InvalidParams", err)
	}

	rng := blockRange{From: resolved.From, To: resolved.To}
	parts := replayPartitionPrefixes(rng.From, rng.To)
	state.Range = rng
	state.PartitionsTotal = len(parts)

	logger.Info("starting morpho vault backfill",
		"from", rng.From, "to", rng.To, "partitions", len(parts))

	// Read from state at every return point, so the reported counts can never
	// lag the work actually done. On a FAILING run they do not reach the Result
	// panel at all — Temporal discards a workflow's result payload when it
	// returns a non-nil error — so the progress query is the channel an operator
	// uses to see how far the run got.
	resultOf := func() BackfillResult {
		return BackfillResult{
			Range:          state.Range,
			Discovered:     state.Discovered,
			PartitionsRun:  state.PartitionsDone,
			EventsReplayed: state.EventsReplayed,
		}
	}

	if err := discoverVaults(ctx, rng, &state); err != nil {
		return resultOf(), err
	}
	if err := replayPartitions(ctx, rng, parts, &state); err != nil {
		return resultOf(), err
	}

	logger.Info("morpho vault backfill complete",
		"discovered", state.Discovered, "partitions", state.PartitionsDone, "events", state.EventsReplayed)
	return resultOf(), nil
}

// discoverVaults runs phases 1-3 as ONE activity rather than one per partition.
// The candidate set is deduplicated across the whole range before a single
// on-chain probe at `to`, so splitting the scan would either push every Morpho
// Blue caller/onBehalf address through workflow history or re-probe them once
// per partition. Losing per-partition resume on this phase is the cheaper
// trade: it measured 253s against the replay's 436s over the same 22 partitions.
func discoverVaults(ctx workflow.Context, rng blockRange, state *backfillProgress) error {
	ctx = workflow.WithActivityOptions(ctx, discoverActivityOptions())

	var activities *backfillActivities
	var got discoveryResult
	if err := workflow.ExecuteActivity(ctx, activities.DiscoverVaults, rng).Get(ctx, &got); err != nil {
		return err
	}
	state.Discovered = &got
	return nil
}

// replayPartitions replays every partition in ascending block order and
// hard-stops on the first failure.
//
// One activity per partition, sequentially: every completed partition is already
// in the event history, so a retry or a rolled pod resumes at the next one
// instead of redoing the range. The hard stop is the usual rule rather than an
// ordering one — replaying out of order still reaches the same answers (see
// replayPartition) — but a partition that failed leaves a hole, and continuing
// past it would end the run reporting success over incomplete data.
func replayPartitions(ctx workflow.Context, rng blockRange, parts []string, state *backfillProgress) error {
	// Discovery persists before it counts, so a zero here is the post-persist
	// answer every partition's own registry load would reach after paying for it.
	if state.Discovered.KnownV2Vaults == 0 {
		workflow.GetLogger(ctx).Info("no VaultV2 vault is known; skipping the replay phase",
			"partitions", len(parts))
		return nil
	}

	ctx = workflow.WithActivityOptions(ctx, replayActivityOptions())

	var activities *backfillActivities
	for _, part := range parts {
		work := partitionWork{Range: rng, Partition: part}
		var events int
		if err := workflow.ExecuteActivity(ctx, activities.ReplayPartition, work).Get(ctx, &events); err != nil {
			return err
		}
		state.PartitionsDone++
		state.EventsReplayed += events
	}
	return nil
}

func discoverActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		// Sized to cover the widest range resolve accepts, not a typical one: at
		// discoveryScanPerPartition, maxPartitionsPerRun is 8000 x 11.5s = 25.6h of
		// scanning. Anything shorter would accept ranges it cannot finish — the
		// scan keeps no progress state, so an attempt killed here is redone from
		// block one and the envelope below expires having persisted nothing.
		//
		// A ceiling this high costs nothing on a dead worker: the 3-minute
		// HeartbeatTimeout, not this timeout, is what detects one.
		StartToCloseTimeout: 30 * time.Hour,

		// Total time for the phase INCLUDING retries. An attempt has nothing to
		// resume into (the scan keeps no progress state), so this envelope allows
		// one full redo and no more.
		ScheduleToCloseTimeout: 48 * time.Hour,

		HeartbeatTimeout: heartbeatTimeoutFactor * heartbeatInterval,

		RetryPolicy: &temporalsdk.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			// Deliberately no MaximumAttempts: ScheduleToCloseTimeout is the bound.
		},
	}
}

func replayActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		// One 1000-block partition measured ~20s in the VEC-218 E2E (436s over 22),
		// dominated by the hash-pinned archive reads rather than the S3 download.
		// The ceiling is 90x that because a partition's cost scales with the V2
		// governance events in it, not with its fixed block count.
		StartToCloseTimeout: 30 * time.Minute,

		// Total time for one partition INCLUDING retries. This, not a small
		// attempt cap, is what bounds a pathological partition: an attempt cap
		// turns slow-but-progressing work into a hard failure, whereas an
		// envelope lets a transient S3/RPC blip retry while still refusing to
		// hang the run forever.
		ScheduleToCloseTimeout: 2 * time.Hour,

		// The deployment rolls with strategy Recreate, so without this a rollout
		// mid-partition leaves the in-flight attempt undetected until StartToClose.
		HeartbeatTimeout: heartbeatTimeoutFactor * heartbeatInterval,

		RetryPolicy: &temporalsdk.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
		},
	}
}

// backfillActivities holds the dependencies both activities share. Everything
// here outlives a single run; anything scoped to one run (the block range)
// arrives as activity input.
type backfillActivities struct {
	cfg          config
	logger       *slog.Logger
	pool         *pgxpool.Pool
	buildID      buildregistry.BuildID
	s3Reader     outbound.S3Reader
	extractor    *morpho_indexer.EventExtractor
	prober       *vaultProber
	ethClient    *ethclient.Client
	multicaller  outbound.Multicaller
	archiveDrain func()
}

// DiscoverVaults scans the range's S3 receipts for candidate addresses, probes
// them on-chain and persists the confirmed vaults (phases 1-3).
//
// Idempotent: every write goes through GetOrCreate, so a retry — or an operator
// re-running an overlapping range — re-reaches the same rows rather than adding
// any.
func (a *backfillActivities) DiscoverVaults(ctx context.Context, rng blockRange) (result discoveryResult, err error) {
	defer func() { err = nonRetryableIfStructural(err) }()

	// Deferred before the drain so it stops LAST: the drain blocks on in-flight
	// archive writes, and an unheartbeated wait there reads as a dead worker.
	stopHeartbeat := startHeartbeat(ctx, heartbeatInterval)
	defer stopHeartbeat()
	defer a.archiveDrain()

	got, err := discoverAndPersistVaults(ctx, a.logger, a.s3Reader, a.extractor, a.prober, a.pool, a.buildID, a.cfg, rng)
	if err != nil {
		return discoveryResult{}, fmt.Errorf("discovering vaults over blocks %d-%d: %w", rng.From, rng.To, err)
	}

	got.KnownV2Vaults, err = knownV2VaultCount(ctx, a.logger, a.multicaller, a.pool, a.buildID, a.cfg.chainID)
	if err != nil {
		return discoveryResult{}, fmt.Errorf("counting the known VaultV2 vaults: %w", err)
	}

	activity.GetLogger(ctx).Info("discovery complete", "from", rng.From, "to", rng.To,
		"candidates", got.Candidates, "vaults", got.Vaults, "knownV2Vaults", got.KnownV2Vaults)
	return got, nil
}

// ReplayPartition replays one partition's VaultV2 structured events (phase 4)
// and reports how many logs it drove through the live handler path.
//
// The vault registry is reloaded per partition rather than cached on the struct:
// a retry may land on a worker that never ran DiscoverVaults, so reading it here
// is what makes the activity self-contained.
func (a *backfillActivities) ReplayPartition(ctx context.Context, work partitionWork) (events int, err error) {
	defer func() { err = nonRetryableIfStructural(err) }()

	// One wrap for every failure path, deferred rather than repeated per return.
	// The workflow surfaces only this error, so the paths that fail BEFORE the
	// replay begins — service build, registry load, topic derivation — would
	// otherwise reach the operator with no partition to re-run.
	defer func() {
		if err != nil {
			err = fmt.Errorf("replaying partition %s: %w", work.Partition, err)
		}
	}()

	stopHeartbeat := startHeartbeat(ctx, heartbeatInterval)
	defer stopHeartbeat()
	defer a.archiveDrain()

	svc, err := buildReplayService(a.logger, a.multicaller, a.pool, a.buildID, a.cfg.chainID)
	if err != nil {
		return 0, fmt.Errorf("building replay service: %w", err)
	}
	if err := svc.LoadVaultRegistry(ctx); err != nil {
		return 0, fmt.Errorf("loading the vault registry: %w", err)
	}

	v2Vaults := svc.V2VaultAddresses()
	if len(v2Vaults) == 0 {
		a.logger.Info("no VaultV2 vaults known — skipping structured-event replay", "partition", work.Partition)
		return 0, nil
	}

	topics, err := morpho_indexer.VaultV2StructuredEventTopics()
	if err != nil {
		return 0, fmt.Errorf("deriving VaultV2 structured topics: %w", err)
	}

	events, err = replayPartition(ctx, a.logger, a.s3Reader, svc, blocktime.New(a.ethClient),
		a.cfg, work.Range, work.Partition, v2Vaults, topics)
	if err != nil {
		return 0, err
	}

	activity.GetLogger(ctx).Info("replayed partition",
		"partition", work.Partition, "events", events, "v2Vaults", len(v2Vaults))
	return events, nil
}

// nonRetryableIfStructural stops Temporal retrying a verdict that cannot change.
// Neither activity caps its attempts, so an unclassified structural failure
// burns the whole ScheduleToClose envelope — 2h for a partition, 48h for
// discovery — before an operator sees a fault only an S3 repair or a code change
// can clear. Both activities apply it in a deferred assignment to their named
// error result, so no return path can escape it.
func nonRetryableIfStructural(err error) error {
	if errors.Is(err, errStructuralData) || errors.Is(err, morpho_indexer.ErrUnreplayableLog) {
		return temporalsdk.NewNonRetryableApplicationError(err.Error(), "StructuralData", err)
	}
	return err
}

// startHeartbeat's stop joins the goroutine rather than just signalling it, so
// no ping can land after the activity has reported its result.
func startHeartbeat(ctx context.Context, interval time.Duration) (stop func()) {
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	}()
	return sync.OnceFunc(func() {
		close(done)
		<-finished
	})
}
