package main

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

func stateAt(version int, present ...s3key.DataType) archiveState {
	state := archiveState{Version: version, Present: map[s3key.DataType]bool{}}
	for _, dt := range present {
		state.Present[dt] = true
	}
	return state
}

func TestPlanBlock(t *testing.T) {
	all := []s3key.DataType{s3key.Block, s3key.Receipts, s3key.Traces}

	tests := []struct {
		name          string
		state         archiveState
		archivedHash  string
		canonicalHash string
		want          blockPlan
	}{
		{
			name:          "no objects archives a first version",
			state:         archiveState{Version: noArchive},
			archivedHash:  "",
			canonicalHash: canonicalHash,
			want:          blockPlan{Action: actionFresh, Version: 0, DataTypes: all},
		},
		{
			name:          "complete canonical version needs nothing",
			state:         stateAt(0, s3key.Block, s3key.Receipts, s3key.Traces),
			archivedHash:  canonicalHash,
			canonicalHash: canonicalHash,
			want:          blockPlan{Action: actionSkip, Version: 0},
		},
		{
			name:          "canonical version missing traces is filled in place",
			state:         stateAt(0, s3key.Block, s3key.Receipts),
			archivedHash:  canonicalHash,
			canonicalHash: canonicalHash,
			want:          blockPlan{Action: actionFill, Version: 0, DataTypes: []s3key.DataType{s3key.Traces}},
		},
		{
			name:          "losing fork at version 0 is corrected at 1",
			state:         stateAt(0, s3key.Block, s3key.Receipts, s3key.Traces),
			archivedHash:  forkHash,
			canonicalHash: canonicalHash,
			want:          blockPlan{Action: actionRepublish, Version: 1, DataTypes: all},
		},
		{
			name:          "canonical top version above a superseded one needs nothing",
			state:         stateAt(1, s3key.Block, s3key.Receipts, s3key.Traces),
			archivedHash:  canonicalHash,
			canonicalHash: canonicalHash,
			want:          blockPlan{Action: actionSkip, Version: 1},
		},
		{
			name:          "losing fork at the top of two versions is corrected at 2",
			state:         stateAt(1, s3key.Block, s3key.Receipts, s3key.Traces),
			archivedHash:  forkHash,
			canonicalHash: canonicalHash,
			want:          blockPlan{Action: actionRepublish, Version: 2, DataTypes: all},
		},
		{
			name:          "unreadable archived hash is treated as a losing fork",
			state:         stateAt(0, s3key.Traces),
			archivedHash:  "",
			canonicalHash: canonicalHash,
			want:          blockPlan{Action: actionRepublish, Version: 1, DataTypes: all},
		},
		{
			name:          "hash case does not decide canonicity",
			state:         stateAt(0, s3key.Block, s3key.Receipts, s3key.Traces),
			archivedHash:  "0xF327FC5C0000000000000000000000000000000000000000000000000000CAFE",
			canonicalHash: canonicalHash,
			want:          blockPlan{Action: actionSkip, Version: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := planBlock(tt.state, tt.archivedHash, tt.canonicalHash)

			if got.Action != tt.want.Action || got.Version != tt.want.Version {
				t.Errorf("planBlock() action/version = %s/%d, want %s/%d", got.Action, got.Version, tt.want.Action, tt.want.Version)
			}
			if !slices.Equal(got.DataTypes, tt.want.DataTypes) {
				t.Errorf("planBlock() dataTypes = %v, want %v", got.DataTypes, tt.want.DataTypes)
			}
		})
	}
}

func TestIndexPartition(t *testing.T) {
	index := indexPartition([]string{
		"21000000-21000999/21000042_0_block.json.gz",
		"21000000-21000999/21000042_0_receipts.json.gz",
		"21000000-21000999/21000042_0_traces.json.gz",
		"21000000-21000999/21000042_1_block.json.gz",
		"21000000-21000999/21000042_1_receipts.json.gz",
		"21000000-21000999/21000043_0_receipts.json.gz",
		"21000000-21000999/manifest.txt",
	})

	got, ok := index[21000042]
	if !ok {
		t.Fatal("block 21000042 missing from the index")
	}
	if got.Version != 1 {
		t.Errorf("top version = %d, want 1", got.Version)
	}
	if got.Present[s3key.Traces] {
		t.Error("traces of the superseded version 0 must not count as present at version 1")
	}
	if !got.Present[s3key.Block] || !got.Present[s3key.Receipts] {
		t.Errorf("present at top version = %v, want block and receipts", got.Present)
	}
	if len(index) != 2 {
		t.Errorf("indexed heights = %d, want 2 (the unparsable key is not one)", len(index))
	}
}

func TestArchivedBlockHash_ReadsBlockHashFromATruncatedObject(t *testing.T) {
	const blockNum = int64(25395651)
	objects := archivedObjects(t, blockNum, 0, forkHash)
	reader := newFakeRangeReader(objects)
	state := stateAt(0, s3key.Block, s3key.Receipts, s3key.Traces)

	got, err := archivedBlockHash(context.Background(), reader, "bucket", blockNum, state)
	if err != nil {
		t.Fatalf("archivedBlockHash() error = %v", err)
	}
	if got != forkHash {
		t.Errorf("archivedBlockHash() = %q, want %q", got, forkHash)
	}

	key := s3key.Build(blockNum, 0, s3key.Block)
	if asked := reader.ranges[key]; asked != archiveHashPrefixBytes {
		t.Errorf("requested %d bytes of %s, want a %d-byte prefix", asked, key, archiveHashPrefixBytes)
	}
	if int64(len(objects[key])) <= archiveHashPrefixBytes {
		t.Fatalf("fixture object is %d bytes: too small to prove the read was partial", len(objects[key]))
	}
}

func TestArchivedBlockHash_FallsBackToReceiptsWhenTheBlockObjectIsMissing(t *testing.T) {
	const blockNum = int64(25395651)
	objects := archivedObjects(t, blockNum, 0, forkHash)
	delete(objects, s3key.Build(blockNum, 0, s3key.Block))
	state := stateAt(0, s3key.Receipts, s3key.Traces)

	got, err := archivedBlockHash(context.Background(), newFakeRangeReader(objects), "bucket", blockNum, state)
	if err != nil {
		t.Fatalf("archivedBlockHash() error = %v", err)
	}
	if got != forkHash {
		t.Errorf("archivedBlockHash() = %q, want the blockHash of the first receipt %q", got, forkHash)
	}
}

func TestArchivedBlockHash_UnknownWhenNoObjectCarriesOne(t *testing.T) {
	const blockNum = int64(25395651)
	state := stateAt(0, s3key.Traces)

	got, err := archivedBlockHash(context.Background(), newFakeRangeReader(nil), "bucket", blockNum, state)
	if err != nil {
		t.Fatalf("archivedBlockHash() error = %v", err)
	}
	if got != "" {
		t.Errorf("archivedBlockHash() = %q, want the empty hash of an archive that cannot answer", got)
	}
}

func TestArchivedBlockHash_ErrorsWhenTheHashIsBeyondThePrefix(t *testing.T) {
	const blockNum = int64(25395651)
	key := s3key.Build(blockNum, 0, s3key.Block)
	reader := newFakeRangeReader(map[string][]byte{key: gzipped(t, blockJSONWithLateHash(forkHash))})

	_, err := archivedBlockHash(context.Background(), reader, "bucket", blockNum, stateAt(0, s3key.Block))
	if err == nil {
		t.Fatal("expected an error: a hash the prefix could not answer must not be read as a losing fork")
	}
}

func TestArchivedBlockHash_ReadFailureIsNotAnUnknownHash(t *testing.T) {
	const blockNum = int64(25395651)
	reader := newFakeRangeReader(archivedObjects(t, blockNum, 0, forkHash))
	reader.err = errors.New("access denied")

	_, err := archivedBlockHash(context.Background(), reader, "bucket", blockNum, stateAt(0, s3key.Block))
	if err == nil {
		t.Fatal("expected the read failure to surface, not a silent republish")
	}
}

func TestPartitionCache_TopVersionReportsTheHighestVersionAndItsDataTypes(t *testing.T) {
	const blockNum = int64(25395651)
	cache := NewPartitionCache(&fakeListReader{keys: []string{
		s3key.Build(blockNum, 0, s3key.Block),
		s3key.Build(blockNum, 0, s3key.Receipts),
		s3key.Build(blockNum, 0, s3key.Traces),
		s3key.Build(blockNum, 1, s3key.Block),
		s3key.Build(blockNum, 1, s3key.Receipts),
	}}, "bucket", discardLogger())

	state, err := cache.TopVersion(context.Background(), blockNum)
	if err != nil {
		t.Fatalf("TopVersion() error = %v", err)
	}
	if state.Version != 1 {
		t.Errorf("TopVersion() version = %d, want 1", state.Version)
	}
	if !state.Present[s3key.Block] || !state.Present[s3key.Receipts] || state.Present[s3key.Traces] {
		t.Errorf("TopVersion() present = %v, want block and receipts only", state.Present)
	}
}

func TestPartitionCache_TopVersionOfAnUnarchivedHeightIsNoArchive(t *testing.T) {
	const blockNum = int64(25395651)
	cache := NewPartitionCache(&fakeListReader{keys: []string{
		s3key.Build(blockNum, 0, s3key.Block),
	}}, "bucket", discardLogger())

	state, err := cache.TopVersion(context.Background(), blockNum+1)
	if err != nil {
		t.Fatalf("TopVersion() error = %v", err)
	}
	if state.Version != noArchive {
		t.Errorf("TopVersion() version = %d, want %d", state.Version, noArchive)
	}
}

func TestPartitionCache_ConcurrentReadsListAPartitionOnce(t *testing.T) {
	const blockNum = int64(25395651)
	lister := &fakeListReader{
		keys:  []string{s3key.Build(blockNum, 0, s3key.Block)},
		delay: 20 * time.Millisecond,
	}
	cache := NewPartitionCache(lister, "bucket", discardLogger())

	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			if _, err := cache.TopVersion(context.Background(), blockNum); err != nil {
				t.Errorf("TopVersion() error = %v", err)
			}
		})
	}
	wg.Wait()

	if got := lister.listings(); got != 1 {
		t.Errorf("partition listings = %d, want 1: concurrent readers must share one listing", got)
	}
}

func TestPartitionCache_AFailedListingIsRetried(t *testing.T) {
	lister := &fakeListReader{err: errors.New("throttled")}
	cache := NewPartitionCache(lister, "bucket", discardLogger())

	for range 2 {
		if _, err := cache.TopVersion(context.Background(), 25395651); err == nil {
			t.Fatal("expected the listing failure to surface")
		}
	}

	if got := lister.listings(); got != 2 {
		t.Errorf("partition listings = %d, want 2: a failed listing must not be cached", got)
	}
}

func TestBlockPlanner_DecideRepublishesALosingFork(t *testing.T) {
	const blockNum = int64(25395651)
	planner, _ := newTestPlanner([]string{
		s3key.Build(blockNum, 0, s3key.Block),
		s3key.Build(blockNum, 0, s3key.Receipts),
		s3key.Build(blockNum, 0, s3key.Traces),
	}, archivedObjects(t, blockNum, 0, forkHash))

	decision, err := planner.decide(context.Background(), outbound.BlockData{
		BlockNumber: blockNum,
		Block:       blockJSON(canonicalHash, 2),
		Receipts:    receiptsJSON(canonicalHash, 2),
	})
	if err != nil {
		t.Fatalf("decide() error = %v", err)
	}

	if decision.Plan.Action != actionRepublish || decision.Plan.Version != 1 {
		t.Errorf("decide() plan = %s at version %d, want republish at version 1", decision.Plan.Action, decision.Plan.Version)
	}
	if decision.ArchivedHash != forkHash || decision.CanonicalHash != canonicalHash {
		t.Errorf("decide() hashes = archived %q / canonical %q, want %q / %q",
			decision.ArchivedHash, decision.CanonicalHash, forkHash, canonicalHash)
	}
}

func TestBlockPlanner_DecideFailsWhenTheRPCPayloadCarriesNoHash(t *testing.T) {
	planner, _ := newTestPlanner(nil, nil)

	_, err := planner.decide(context.Background(), outbound.BlockData{
		BlockNumber: 25395651,
		Block:       []byte(`{"number":"0x1830003"}`),
	})
	if err == nil {
		t.Fatal("expected an error rather than a plan built on an unknown canonical hash")
	}
}
