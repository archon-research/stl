package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"
	temporalsdk "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/blocktime"
	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
)

// maxPartitionsPerRun catches a mistyped range and keeps a run inside Temporal's
// 51,200-event history: 8000 replay activities plus the 250 discovery
// sub-ranges spanning them is 8250 x ~6 events = ~49,500. 8M blocks reaches 2028.
const maxPartitionsPerRun = 8_000

// maxPlausibleBlock bounds where a range may SIT, where maxPartitionsPerRun
// bounds how WIDE it may be. 1e11 is ~38,000 years of Ethereum blocks, while a
// pasted millisecond timestamp is ~1.75e12 and so is rejected.
//
// It is what keeps the block walks — replayPartitionPrefixes and
// discoverySubRanges, run in workflow code, and partitionsForRange, run inside
// the discovery activity — from overflowing. Each steps a cursor from at most
// `from` by a whole number of partitions while it is <= `to`, so with
// 0 < from <= to <= 1e11 the cursor tops out just above 1e11: eight orders of
// magnitude below math.MaxInt64, past which the cursor would wrap negative and
// the loop never terminate.
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

// discoverySubRangePartitions is how many partitions one discovery activity
// scans. The rule that sizes it: a sub-range must finish inside the shortest gap
// between deploys at the conservative discoveryScanPerPartition rate, because a
// rolled pod costs the whole attempt. 32 x 11.5s = 6.1 min against a 25-minute
// closest observed gap.
//
// It buys that with ~7.8x more re-probing over the real era — 77 sub-ranges
// rather than 10, and a long-lived vault is a candidate in every one it appears
// in — plus one knownV2VaultCount read per sub-range.
//
// The widest accepted run costs 250 of these on top of its 8000 replay
// activities: 8250 x ~6 events = ~49,500, inside the 51,200 history ceiling. The
// budget breaks even at 16, so going lower needs maxPartitionsPerRun cut too.
const discoverySubRangePartitions = 32

// heartbeatInterval keeps the discovery scan visible to Temporal. Its
// StartToClose ceiling is tens of minutes, so without a heartbeat a worker killed
// mid-scan would hold the activity open for that whole ceiling instead of three.
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

// discoveryWork is one unit of discovery: the sub-range to scan, and the block
// its candidates are probed at.
//
// ProbeBlock is the RUN's `to`, not the sub-range's: name and symbol are
// mutable on a VaultV2 and are persisted first-write-wins, so probing each
// sub-range at its own end would make a split run store different values than a
// whole-range one.
type discoveryWork struct {
	Range      blockRange `json:"range"`
	ProbeBlock int64      `json:"probeBlock"`
}

// partitionWork is one unit of replay: a single 1000-block S3 partition, clamped
// to the run's range.
type partitionWork struct {
	Range     blockRange `json:"range"`
	Partition string     `json:"partition"`
}

// partitionReplay is what one partition's replay did: the logs it drove through the
// handler path, and the rows those logs actually appended. The two are independent —
// every event can replay and still write nothing, which is the shape a re-run of an
// already-replayed range takes.
type partitionReplay struct {
	EventsReplayed int          `json:"eventsReplayed"`
	RowsAppended   appendedRows `json:"rowsAppended"`
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
	RowsAppended   appendedRows     `json:"rowsAppended"`
}

type backfillProgress struct {
	Range           blockRange       `json:"range"`
	Discovered      *discoveryResult `json:"discovered,omitempty"`
	SubRangesTotal  int              `json:"subRangesTotal"`
	SubRangesDone   int              `json:"subRangesDone"`
	PartitionsTotal int              `json:"partitionsTotal"`
	PartitionsDone  int              `json:"partitionsDone"`
	EventsReplayed  int              `json:"eventsReplayed"`
	RowsAppended    appendedRows     `json:"rowsAppended"`
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
	subRanges := discoverySubRanges(rng)
	parts := replayPartitionPrefixes(rng.From, rng.To)
	state.Range = rng
	state.SubRangesTotal = len(subRanges)
	state.PartitionsTotal = len(parts)

	logger.Info("starting morpho vault backfill",
		"from", rng.From, "to", rng.To, "subRanges", len(subRanges), "partitions", len(parts))

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
			RowsAppended:   state.RowsAppended,
		}
	}

	if err := discoverVaults(ctx, rng, subRanges, &state); err != nil {
		return resultOf(), err
	}
	if err := replayPartitions(ctx, rng, parts, &state); err != nil {
		return resultOf(), err
	}

	// No emptiness check, breaking temporal_guide design rule 5: a range holding
	// zero VaultV2 governance events is ordinary, so a hole is not visible by count.
	logger.Info("morpho vault backfill complete",
		"discovered", state.Discovered, "partitions", state.PartitionsDone,
		"events", state.EventsReplayed, "rowsAppended", state.RowsAppended)
	return resultOf(), nil
}

// discoverVaults runs phases 1-3 over the range, one activity per sub-range and
// sequentially. A whole-era scan runs for hours while deploys roll this
// Recreate-strategy pod, and an activity banks nothing until it returns, so only
// a completed sub-range is progress a retry can resume past.
//
// Only the SCAN is split: every sub-range still probes its candidates at the
// run's own `to`, so a split run persists what a whole-range one would. Candidate
// dedup does narrow to a sub-range, which is why the summed Candidates and Vaults
// count an address once per sub-range it appears in.
func discoverVaults(ctx workflow.Context, rng blockRange, subRanges []blockRange, state *backfillProgress) error {
	ctx = workflow.WithActivityOptions(ctx, discoverActivityOptions())

	// Published before the first scan so a run that fails partway still reports
	// what the sub-ranges before it found.
	var found discoveryResult
	state.Discovered = &found

	var activities *backfillActivities
	for _, sub := range subRanges {
		work := discoveryWork{Range: sub, ProbeBlock: rng.To}
		var got discoveryResult
		if err := workflow.ExecuteActivity(ctx, activities.DiscoverVaults, work).Get(ctx, &got); err != nil {
			return err
		}
		found.Candidates += got.Candidates
		found.Vaults += got.Vaults
		// Last one wins: it is read after every earlier sub-range has persisted,
		// so it already answers for all of them.
		found.KnownV2Vaults = got.KnownV2Vaults
		state.SubRangesDone++
	}
	return nil
}

// discoverySubRanges splits the range into the sub-ranges discovery scans, in
// ascending block order. Every edge but the range's own two sits on a partition
// boundary, so no S3 partition is listed by two scans and none is skipped.
func discoverySubRanges(rng blockRange) []blockRange {
	const subRangeBlocks = discoverySubRangePartitions * partition.BlockRangeSize

	var subRanges []blockRange
	for start := rng.From - rng.From%partition.BlockRangeSize; start <= rng.To; start += subRangeBlocks {
		subRanges = append(subRanges, blockRange{
			From: max(start, rng.From),
			To:   min(start+subRangeBlocks-1, rng.To),
		})
	}
	return subRanges
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
		var replayed partitionReplay
		if err := workflow.ExecuteActivity(ctx, activities.ReplayPartition, work).Get(ctx, &replayed); err != nil {
			return err
		}
		state.PartitionsDone++
		state.EventsReplayed += replayed.EventsReplayed
		state.RowsAppended.add(replayed.RowsAppended)
	}
	return nil
}

func discoverActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		// One attempt covers ONE sub-range, not the run: at
		// discoveryScanPerPartition, discoverySubRangePartitions is 32 x 11.5s =
		// 6.1 minutes of scanning. The ceiling is ~3x that because a sub-range
		// also pays for the candidates it finds, which its block count does not
		// bound. The 3-minute HeartbeatTimeout, not this, is what detects a dead
		// worker.
		StartToCloseTimeout: 20 * time.Minute,

		// Total time for ONE sub-range INCLUDING retries. A killed attempt redoes
		// only its own sub-range, so this envelope allows one full redo and no
		// more: twice StartToClose, so the redo still exists for an attempt that
		// burned the whole ceiling above.
		ScheduleToCloseTimeout: 40 * time.Minute,

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

// DiscoverVaults scans one sub-range's S3 receipts for candidate addresses,
// probes them on-chain at the run's probe block and persists the confirmed
// vaults (phases 1-3).
//
// Idempotent: every write goes through GetOrCreate, so a retry — or an operator
// re-running an overlapping range — re-reaches the same rows rather than adding
// any.
func (a *backfillActivities) DiscoverVaults(ctx context.Context, work discoveryWork) (result discoveryResult, err error) {
	defer func() { err = nonRetryableIfStructural(err) }()

	// Deferred before the drain so it stops LAST: the drain blocks on in-flight
	// archive writes, and an unheartbeated wait there reads as a dead worker.
	stopHeartbeat := temporal.StartHeartbeat(ctx, heartbeatInterval, nil)
	defer stopHeartbeat()
	defer a.archiveDrain()

	rng := work.Range
	got, err := discoverAndPersistVaults(ctx, a.logger, a.s3Reader, a.extractor, a.prober, a.pool, a.buildID, a.cfg, rng, work.ProbeBlock)
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

// ReplayPartition replays one partition's VaultV2 structured events (phase 4) and reports
// how many logs it drove through the live handler path and how many rows those logs
// appended.
//
// The vault registry is reloaded per partition rather than cached on the struct:
// a retry may land on a worker that never ran DiscoverVaults, so reading it here
// is what makes the activity self-contained.
func (a *backfillActivities) ReplayPartition(ctx context.Context, work partitionWork) (replayed partitionReplay, err error) {
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

	stopHeartbeat := temporal.StartHeartbeat(ctx, heartbeatInterval, nil)
	defer stopHeartbeat()
	defer a.archiveDrain()

	svc, counted, err := buildReplayService(a.logger, a.multicaller, a.pool, a.buildID, a.cfg.chainID)
	if err != nil {
		return partitionReplay{}, fmt.Errorf("building replay service: %w", err)
	}
	if err := svc.LoadVaultRegistry(ctx); err != nil {
		return partitionReplay{}, fmt.Errorf("loading the vault registry: %w", err)
	}

	v2Vaults := svc.V2VaultAddresses()
	if len(v2Vaults) == 0 {
		a.logger.Info("no VaultV2 vaults known — skipping structured-event replay", "partition", work.Partition)
		return partitionReplay{}, nil
	}

	// Structural: the topics come from the ABI embedded in this binary, so a
	// failure here is a defect only a new build can clear.
	topics, err := morpho_indexer.VaultV2StructuredEventTopics()
	if err != nil {
		return partitionReplay{}, fmt.Errorf("deriving VaultV2 structured topics: %w: %w", err, errStructuralData)
	}

	events, err := replayPartition(ctx, a.logger, a.s3Reader, svc, blocktime.New(a.ethClient),
		a.cfg, work.Range, work.Partition, v2Vaults, topics)
	if err != nil {
		return partitionReplay{}, err
	}
	replayed = partitionReplay{EventsReplayed: events, RowsAppended: counted.counts}

	activity.GetLogger(ctx).Info("replayed partition",
		"partition", work.Partition, "events", events,
		"rowsAppended", replayed.RowsAppended, "v2Vaults", len(v2Vaults))
	return replayed, nil
}

// nonRetryableIfStructural stops Temporal retrying a verdict that cannot change.
// Neither activity caps its attempts, so an unclassified structural failure
// burns its whole ScheduleToClose envelope (two hours for a partition, forty
// minutes for a sub-range) before an operator sees a fault only an S3 repair or
// a code change can clear. Both activities apply it in a
// deferred assignment to their named error result, so no return path can escape
// it.
func nonRetryableIfStructural(err error) error {
	if errors.Is(err, errStructuralData) || errors.Is(err, morpho_indexer.ErrUnreplayableLog) {
		return temporalsdk.NewNonRetryableApplicationError(err.Error(), "StructuralData", err)
	}
	return err
}
