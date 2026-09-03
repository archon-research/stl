package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/smithy-go"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
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
			name:          "an archive that carries no hash is corrected at the next version",
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
	index, err := indexPartition([]string{
		"21000000-21000999/21000042_0_block.json.gz",
		"21000000-21000999/21000042_0_receipts.json.gz",
		"21000000-21000999/21000042_0_traces.json.gz",
		"21000000-21000999/21000042_1_block.json.gz",
		"21000000-21000999/21000042_1_receipts.json.gz",
		"21000000-21000999/21000043_0_receipts.json.gz",
	})
	if err != nil {
		t.Fatalf("indexPartition: %v", err)
	}

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
		t.Errorf("indexed heights = %d, want 2", len(index))
	}
}

// An object the tool cannot read says nothing about which versions the partition
// holds, so planning around it would write over an occupied slot.
func TestIndexPartition_FailsOnAKeyItCannotRead(t *testing.T) {
	_, err := indexPartition([]string{
		"21000000-21000999/21000042_0_block.json.gz",
		"21000000-21000999/manifest.txt",
	})

	if !errors.Is(err, s3key.ErrUnrecognisedKey) {
		t.Fatalf("error = %v, want ErrUnrecognisedKey", err)
	}
	if !strings.Contains(err.Error(), "manifest.txt") {
		t.Errorf("error = %v, want it to name the key", err)
	}
}

func TestPartitionCache_TopVersionReportsTheHighestVersionAndItsDataTypes(t *testing.T) {
	const blockNum = int64(25395651)
	cache := newTestPartitionCache(&fakeListReader{keys: []string{
		s3key.Build(blockNum, 0, s3key.Block),
		s3key.Build(blockNum, 0, s3key.Receipts),
		s3key.Build(blockNum, 0, s3key.Traces),
		s3key.Build(blockNum, 1, s3key.Block),
		s3key.Build(blockNum, 1, s3key.Receipts),
	}})

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
	cache := newTestPartitionCache(&fakeListReader{keys: []string{
		s3key.Build(blockNum, 0, s3key.Block),
	}})

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
	cache := newTestPartitionCache(lister)

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
	cache := newTestPartitionCache(lister)

	for range 2 {
		if _, err := cache.TopVersion(context.Background(), 25395651); err == nil {
			t.Fatal("expected the listing failure to surface")
		}
	}

	if got, want := lister.listings(), 2*listRetryAttempts; got != want {
		t.Errorf("partition listings = %d, want %d: a failed listing must not be cached", got, want)
	}
}

func TestPartitionCache_AThrottledListingIsRetriedRatherThanFailingEveryWorkerOnIt(t *testing.T) {
	const blockNum = int64(25395651)
	lister := &fakeListReader{
		keys:     []string{s3key.Build(blockNum, 0, s3key.Block)},
		err:      errors.New("throttled"),
		failures: 2,
	}
	cache := newTestPartitionCache(lister)

	state, err := cache.TopVersion(context.Background(), blockNum)
	if err != nil {
		t.Fatalf("TopVersion() error = %v, want the throttled listings retried before hundreds of heights fail", err)
	}
	if state.Version != 0 {
		t.Errorf("TopVersion() version = %d, want 0 from the listing that finally succeeded", state.Version)
	}
	if got := lister.listings(); got != 3 {
		t.Errorf("partition listings = %d, want 3: two throttled tries and the one that succeeded", got)
	}
}

func TestBlockPlanner_DecideRepublishesALosingFork(t *testing.T) {
	const blockNum = int64(25395651)
	planner, _ := newTestPlanner([]string{
		s3key.Build(blockNum, 0, s3key.Block),
		s3key.Build(blockNum, 0, s3key.Receipts),
		s3key.Build(blockNum, 0, s3key.Traces),
	}, archivedObjects(t, blockNum, 0, forkHash))

	decision, err := planner.decide(context.Background(), blockNum, stateAt(0, s3key.Block, s3key.Receipts, s3key.Traces), blockJSON(canonicalHash, 2))
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

	_, err := planner.decide(context.Background(), 25395651, stateAt(0, s3key.Block), []byte(`{"number":"0x1830003"}`))
	if err == nil {
		t.Fatal("expected an error rather than a plan built on an unknown canonical hash")
	}
}

func TestRetryableListing(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "a dropped connection", err: errors.New("dial tcp 52.0.0.1:443: i/o timeout"), want: true},
		{name: "a deadline", err: context.DeadlineExceeded, want: true},
		{name: "S3 asking for less traffic", err: &smithy.GenericAPIError{Code: "SlowDown"}, want: true},
		{name: "throttling", err: &smithy.GenericAPIError{Code: "Throttling"}, want: true},
		{name: "the request rate ceiling", err: &smithy.GenericAPIError{Code: "RequestLimitExceeded"}, want: true},
		{name: "a server fault", err: &smithy.GenericAPIError{Code: "InternalError", Fault: smithy.FaultServer}, want: true},
		{name: "an unmodelled 5xx", err: statusError{code: "GatewayProblem", status: 503}, want: true},
		{name: "a grant the run does not have", err: &smithy.GenericAPIError{Code: "AccessDenied", Fault: smithy.FaultClient}, want: false},
		{name: "a bucket that is not there", err: &smithy.GenericAPIError{Code: "NoSuchBucket", Fault: smithy.FaultClient}, want: false},
		{name: "credentials that have expired", err: &smithy.GenericAPIError{Code: "ExpiredToken", Fault: smithy.FaultClient}, want: false},
		{name: "credentials that are not ours", err: &smithy.GenericAPIError{Code: "InvalidAccessKeyId", Fault: smithy.FaultClient}, want: false},
		{
			name: "a permanent failure the caller wrapped",
			err:  fmt.Errorf("listing partition 0-999: %w", &smithy.GenericAPIError{Code: "AccessDenied", Fault: smithy.FaultClient}),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := retryableListing(tc.err); got != tc.want {
				t.Errorf("retryableListing(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// statusError is an API error whose code says nothing but whose HTTP status
// does, the shape a proxy or an unmodelled fault arrives as.
type statusError struct {
	code   string
	status int
}

func (e statusError) Error() string                 { return e.code }
func (e statusError) ErrorCode() string             { return e.code }
func (e statusError) ErrorMessage() string          { return e.code }
func (e statusError) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }
func (e statusError) HTTPStatusCode() int           { return e.status }
