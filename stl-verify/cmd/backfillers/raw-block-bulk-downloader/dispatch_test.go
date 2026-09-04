package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const planTestBlock = int64(25395651)

// dispatched is what one applyDecision call handed the pipeline.
type dispatched struct {
	uploads []UploadJob
	traces  []traceRequest
	stats   *Stats
	err     error
}

func dispatch(t *testing.T, cfg Config, keys []string, objects map[string][]byte, r outbound.BlockData) dispatched {
	t.Helper()

	planner, stats := newTestPlanner(t, cfg.ChainID, keys, objects)
	report, err := newDecisionReport(cfg.ReportPath)
	if err != nil {
		t.Fatalf("newDecisionReport: %v", err)
	}
	uploadCh := make(chan UploadJob, 8)
	traceCh := make(chan traceRequest, 8)

	archiver := blockArchiver{
		planner:  planner,
		report:   report,
		cfg:      cfg,
		uploadCh: uploadCh,
		traceCh:  traceCh,
		stats:    stats,
		logger:   testutil.DiscardLogger(),
	}
	err = planAndApply(context.Background(), archiver, r)
	close(uploadCh)
	close(traceCh)
	if closeErr := report.close(); closeErr != nil {
		t.Fatalf("closing the report: %v", closeErr)
	}

	out := dispatched{stats: stats, err: err}
	for job := range uploadCh {
		out.uploads = append(out.uploads, job)
	}
	for req := range traceCh {
		out.traces = append(out.traces, req)
	}
	return out
}

// planAndApply drives one height through the two steps a block worker takes it
// through: the plan, then what the plan writes.
func planAndApply(ctx context.Context, a blockArchiver, r outbound.BlockData) error {
	state, err := a.planner.topVersion(ctx, r.BlockNumber)
	if err != nil {
		return err
	}
	if state.Version == noArchive {
		return a.applyDecision(ctx, a.planner.fresh(r.BlockNumber), r)
	}

	decision, err := a.planner.decide(ctx, r.BlockNumber, state, r.Block)
	if err != nil {
		return err
	}
	return a.applyDecision(ctx, decision, r)
}

func canonicalBlockData() outbound.BlockData {
	return outbound.BlockData{
		BlockNumber: planTestBlock,
		Block:       blockJSON(canonicalHash, 2),
		Receipts:    receiptsJSON(canonicalHash, 2),
	}
}

func archivedAt(version int, dataTypes ...s3key.DataType) []string {
	keys := make([]string, 0, len(dataTypes))
	for _, dt := range dataTypes {
		keys = append(keys, s3key.Build(planTestBlock, version, dt))
	}
	return keys
}

func uploadKeys(jobs []UploadJob) []string {
	keys := make([]string, 0, len(jobs))
	for _, job := range jobs {
		keys = append(keys, job.Key)
	}
	return keys
}

func TestApplyDecision_RepublishesALosingForkAtTheNextVersion(t *testing.T) {
	got := dispatch(t,
		Config{Bucket: "bucket", ChainID: ethereumChainID},
		archivedAt(0, s3key.Block, s3key.Receipts, s3key.Traces),
		archivedObjects(t, planTestBlock, 0, forkHash),
		canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyDecision() error = %v", got.err)
	}

	wantKeys := []string{
		s3key.Build(planTestBlock, 1, s3key.Block),
		s3key.Build(planTestBlock, 1, s3key.Receipts),
	}
	if keys := uploadKeys(got.uploads); !slices.Equal(keys, wantKeys) {
		t.Errorf("queued uploads = %v, want %v", keys, wantKeys)
	}
	if len(got.traces) != 1 || got.traces[0] != (traceRequest{BlockNum: planTestBlock, Version: 1}) {
		t.Errorf("trace requests = %v, want one at version 1", got.traces)
	}
	if got.stats.planRepublish.Load() != 1 {
		t.Errorf("planRepublish = %d, want 1", got.stats.planRepublish.Load())
	}
	if got.stats.blocksProcessed.Load() != 1 {
		t.Errorf("blocksProcessed = %d, want 1", got.stats.blocksProcessed.Load())
	}
}

func TestApplyDecision_FillsOnlyWhatTheCanonicalVersionLacks(t *testing.T) {
	got := dispatch(t,
		Config{Bucket: "bucket", ChainID: ethereumChainID},
		archivedAt(0, s3key.Block, s3key.Receipts),
		archivedObjects(t, planTestBlock, 0, canonicalHash),
		canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyDecision() error = %v", got.err)
	}
	if len(got.uploads) != 0 {
		t.Errorf("queued uploads = %v, want none: block and receipts are already archived", uploadKeys(got.uploads))
	}
	if len(got.traces) != 1 || got.traces[0] != (traceRequest{BlockNum: planTestBlock, Version: 0}) {
		t.Errorf("trace requests = %v, want one at version 0", got.traces)
	}
	if got.stats.planFill.Load() != 1 {
		t.Errorf("planFill = %d, want 1", got.stats.planFill.Load())
	}
	if got.stats.blocksSkipped.Load() != 1 {
		t.Errorf("blocksSkipped = %d, want 1", got.stats.blocksSkipped.Load())
	}
}

func TestApplyDecision_ArchivesAnUntouchedHeightAtVersionZero(t *testing.T) {
	got := dispatch(t, Config{Bucket: "bucket", ChainID: ethereumChainID}, nil, nil, canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyDecision() error = %v", got.err)
	}

	wantKeys := []string{
		s3key.Build(planTestBlock, 0, s3key.Block),
		s3key.Build(planTestBlock, 0, s3key.Receipts),
	}
	if keys := uploadKeys(got.uploads); !slices.Equal(keys, wantKeys) {
		t.Errorf("queued uploads = %v, want %v", keys, wantKeys)
	}
	if len(got.traces) != 1 || got.traces[0] != (traceRequest{BlockNum: planTestBlock, Version: 0}) {
		t.Errorf("trace requests = %v, want one at version 0", got.traces)
	}
	if got.stats.planFresh.Load() != 1 {
		t.Errorf("planFresh = %d, want 1", got.stats.planFresh.Load())
	}
}

func TestApplyDecision_RepublishesAZeroTxHeightNoArchivedObjectCanIdentify(t *testing.T) {
	got := dispatch(t,
		Config{Bucket: "bucket", ChainID: ethereumChainID},
		archivedAt(0, s3key.Receipts, s3key.Traces),
		map[string][]byte{s3key.Build(planTestBlock, 0, s3key.Receipts): gzipped(t, []byte(`[]`))},
		canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyDecision() error = %v, want the height repaired rather than failed on every run", got.err)
	}

	wantKeys := []string{
		s3key.Build(planTestBlock, 1, s3key.Block),
		s3key.Build(planTestBlock, 1, s3key.Receipts),
	}
	if keys := uploadKeys(got.uploads); !slices.Equal(keys, wantKeys) {
		t.Errorf("queued uploads = %v, want %v", keys, wantKeys)
	}
	if len(got.traces) != 1 || got.traces[0] != (traceRequest{BlockNum: planTestBlock, Version: 1}) {
		t.Errorf("trace requests = %v, want one at version 1", got.traces)
	}
}

func TestApplyDecision_SkipsACompleteCanonicalHeight(t *testing.T) {
	got := dispatch(t,
		Config{Bucket: "bucket", ChainID: ethereumChainID},
		archivedAt(0, s3key.Block, s3key.Receipts, s3key.Traces),
		archivedObjects(t, planTestBlock, 0, canonicalHash),
		canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyDecision() error = %v", got.err)
	}
	if len(got.uploads) != 0 || len(got.traces) != 0 {
		t.Errorf("queued uploads = %v and traces = %v, want neither", uploadKeys(got.uploads), got.traces)
	}
	if got.stats.planSkip.Load() != 1 || got.stats.tracesSkipped.Load() != 1 {
		t.Errorf("planSkip = %d, tracesSkipped = %d, want 1 and 1", got.stats.planSkip.Load(), got.stats.tracesSkipped.Load())
	}
}

func TestApplyDecision_DryRunQueuesNothing(t *testing.T) {
	got := dispatch(t,
		Config{Bucket: "bucket", ChainID: ethereumChainID, DryRun: true},
		archivedAt(0, s3key.Block, s3key.Receipts, s3key.Traces),
		archivedObjects(t, planTestBlock, 0, forkHash),
		canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyDecision() error = %v", got.err)
	}
	if len(got.uploads) != 0 || len(got.traces) != 0 {
		t.Errorf("a dry run queued uploads = %v and traces = %v, want neither", uploadKeys(got.uploads), got.traces)
	}
	if got.stats.planRepublish.Load() != 1 {
		t.Errorf("planRepublish = %d, want the decision counted even in a dry run", got.stats.planRepublish.Load())
	}
}

func TestApplyDecision_DryRunCountsTheHeightAsSkipped(t *testing.T) {
	got := dispatch(t, Config{Bucket: "bucket", ChainID: ethereumChainID, DryRun: true}, nil, nil, canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyDecision() error = %v", got.err)
	}
	if got.stats.blocksSkipped.Load() != 1 || got.stats.tracesSkipped.Load() != 1 {
		t.Errorf("blocksSkipped = %d, tracesSkipped = %d, want 1 and 1 so a dry run reports progress",
			got.stats.blocksSkipped.Load(), got.stats.tracesSkipped.Load())
	}
}

func TestApplyDecision_AMissingPayloadQueuesNothingAtAll(t *testing.T) {
	r := canonicalBlockData()
	r.Receipts = nil

	got := dispatch(t, Config{Bucket: "bucket", ChainID: ethereumChainID}, nil, nil, r)

	if got.err == nil {
		t.Fatal("expected an error for a plan the RPC payload cannot satisfy")
	}
	if len(got.uploads) != 0 {
		t.Errorf("queued uploads = %v, want none: a version must not land half-written", uploadKeys(got.uploads))
	}
}

func TestApplyDecision_FetchFailureStopsTheHeight(t *testing.T) {
	r := canonicalBlockData()
	r.ReceiptsErr = errors.New("eth_getBlockReceipts: upstream null result")

	got := dispatch(t, Config{Bucket: "bucket", ChainID: ethereumChainID}, nil, nil, r)

	if got.err == nil {
		t.Fatal("expected the fetch failure to surface rather than a partial archive")
	}
	if len(got.uploads) != 0 || len(got.traces) != 0 {
		t.Errorf("queued uploads = %v and traces = %v after a failed fetch, want neither", uploadKeys(got.uploads), got.traces)
	}
}

func TestLogDecision_AFreshHeightStaysOutOfTheInfoLogOfARealRun(t *testing.T) {
	tests := []struct {
		name   string
		dryRun bool
		action blockAction
		want   string
	}{
		{name: "a fresh height in a real run", action: actionFresh, want: "DEBUG"},
		{name: "a fresh height in a dry run", dryRun: true, action: actionFresh, want: "INFO"},
		{name: "a republish", action: actionRepublish, want: "INFO"},
		{name: "a fill", action: actionFill, want: "INFO"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, logs := captureLogger()

			logDecision(logger, tt.dryRun, blockDecision{BlockNumber: planTestBlock, Plan: blockPlan{Action: tt.action}})

			if !strings.Contains(logs.String(), "level="+tt.want) {
				t.Errorf("logged %q, want it at %s: a multi-million-block run must not narrate every fresh height", logs.String(), tt.want)
			}
		})
	}
}

// The same forked height on a chain whose watcher fetches no traces: the
// correction is block and receipts, and no trace fetch is scheduled at all.
func TestApplyDecision_AChainWithoutTracesRequestsNone(t *testing.T) {
	got := dispatch(t,
		Config{Bucket: "bucket", ChainID: baseChainID},
		archivedAt(0, s3key.Block, s3key.Receipts),
		archivedObjects(t, planTestBlock, 0, forkHash),
		canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyDecision() error = %v", got.err)
	}

	wantKeys := []string{
		s3key.Build(planTestBlock, 1, s3key.Block),
		s3key.Build(planTestBlock, 1, s3key.Receipts),
	}
	if keys := uploadKeys(got.uploads); !slices.Equal(keys, wantKeys) {
		t.Errorf("queued uploads = %v, want %v", keys, wantKeys)
	}
	if len(got.traces) != 0 {
		t.Errorf("trace requests = %v, want none: this chain's watcher fetches no traces", got.traces)
	}
}
