package main

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const planTestBlock = int64(25395651)

// dispatched is what one applyBlockPlan call handed the pipeline.
type dispatched struct {
	uploads []UploadJob
	traces  []traceRequest
	stats   *Stats
	err     error
}

func dispatch(t *testing.T, cfg Config, keys []string, objects map[string][]byte, r outbound.BlockData) dispatched {
	t.Helper()

	planner, stats := newTestPlanner(keys, objects)
	uploadCh := make(chan UploadJob, 8)
	traceCh := make(chan traceRequest, 8)

	err := applyBlockPlan(context.Background(), r, planner, cfg, uploadCh, traceCh, stats, discardLogger())
	close(uploadCh)
	close(traceCh)

	out := dispatched{stats: stats, err: err}
	for job := range uploadCh {
		out.uploads = append(out.uploads, job)
	}
	for req := range traceCh {
		out.traces = append(out.traces, req)
	}
	return out
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

func TestApplyBlockPlan_RepublishesALosingForkAtTheNextVersion(t *testing.T) {
	got := dispatch(t,
		Config{Bucket: "bucket"},
		archivedAt(0, s3key.Block, s3key.Receipts, s3key.Traces),
		archivedObjects(t, planTestBlock, 0, forkHash),
		canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyBlockPlan() error = %v", got.err)
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

func TestApplyBlockPlan_FillsOnlyWhatTheCanonicalVersionLacks(t *testing.T) {
	got := dispatch(t,
		Config{Bucket: "bucket"},
		archivedAt(0, s3key.Block, s3key.Receipts),
		archivedObjects(t, planTestBlock, 0, canonicalHash),
		canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyBlockPlan() error = %v", got.err)
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

func TestApplyBlockPlan_ArchivesAnUntouchedHeightAtVersionZero(t *testing.T) {
	got := dispatch(t, Config{Bucket: "bucket"}, nil, nil, canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyBlockPlan() error = %v", got.err)
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

func TestApplyBlockPlan_SkipsACompleteCanonicalHeight(t *testing.T) {
	got := dispatch(t,
		Config{Bucket: "bucket"},
		archivedAt(0, s3key.Block, s3key.Receipts, s3key.Traces),
		archivedObjects(t, planTestBlock, 0, canonicalHash),
		canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyBlockPlan() error = %v", got.err)
	}
	if len(got.uploads) != 0 || len(got.traces) != 0 {
		t.Errorf("queued uploads = %v and traces = %v, want neither", uploadKeys(got.uploads), got.traces)
	}
	if got.stats.planSkip.Load() != 1 || got.stats.tracesSkipped.Load() != 1 {
		t.Errorf("planSkip = %d, tracesSkipped = %d, want 1 and 1", got.stats.planSkip.Load(), got.stats.tracesSkipped.Load())
	}
}

func TestApplyBlockPlan_DryRunQueuesNothing(t *testing.T) {
	got := dispatch(t,
		Config{Bucket: "bucket", DryRun: true},
		archivedAt(0, s3key.Block, s3key.Receipts, s3key.Traces),
		archivedObjects(t, planTestBlock, 0, forkHash),
		canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyBlockPlan() error = %v", got.err)
	}
	if len(got.uploads) != 0 || len(got.traces) != 0 {
		t.Errorf("a dry run queued uploads = %v and traces = %v, want neither", uploadKeys(got.uploads), got.traces)
	}
	if got.stats.planRepublish.Load() != 1 {
		t.Errorf("planRepublish = %d, want the decision counted even in a dry run", got.stats.planRepublish.Load())
	}
}

func TestApplyBlockPlan_DryRunCountsTheHeightAsSkipped(t *testing.T) {
	got := dispatch(t, Config{Bucket: "bucket", DryRun: true}, nil, nil, canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyBlockPlan() error = %v", got.err)
	}
	if got.stats.blocksSkipped.Load() != 1 || got.stats.tracesSkipped.Load() != 1 {
		t.Errorf("blocksSkipped = %d, tracesSkipped = %d, want 1 and 1 so a dry run reports progress",
			got.stats.blocksSkipped.Load(), got.stats.tracesSkipped.Load())
	}
}

func TestApplyBlockPlan_AMissingPayloadQueuesNothingAtAll(t *testing.T) {
	r := canonicalBlockData()
	r.Receipts = nil

	got := dispatch(t, Config{Bucket: "bucket"}, nil, nil, r)

	if got.err == nil {
		t.Fatal("expected an error for a plan the RPC payload cannot satisfy")
	}
	if len(got.uploads) != 0 {
		t.Errorf("queued uploads = %v, want none: a version must not land half-written", uploadKeys(got.uploads))
	}
}

func TestApplyBlockPlan_FetchFailureStopsTheHeight(t *testing.T) {
	r := canonicalBlockData()
	r.ReceiptsErr = errors.New("eth_getBlockReceipts: upstream null result")

	got := dispatch(t, Config{Bucket: "bucket"}, nil, nil, r)

	if got.err == nil {
		t.Fatal("expected the fetch failure to surface rather than a partial archive")
	}
	if len(got.uploads) != 0 || len(got.traces) != 0 {
		t.Errorf("queued uploads = %v and traces = %v after a failed fetch, want neither", uploadKeys(got.uploads), got.traces)
	}
}
