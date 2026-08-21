package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestExtractCandidatesFromReceipts_CorruptMorphoBlueLogFailsRun locks in the
// backstop that mirrors the live indexer: a log whose topic0 is a recognised
// Morpho Blue event but whose body fails to decode is a hard error, not a
// silently dropped candidate.
//
// In the live indexer every Morpho Blue log from the singleton is re-extracted
// by processMorphoBlueLog (service.go), which returns an error on decode
// failure; the discovery pre-walk's WARN+continue is safe only because that
// second pass exists. The backfiller has no second pass, so
// emitMorphoBlueCandidates must itself hard-error — otherwise a corrupt log
// silently thins the discovered vault set (and, downstream, the V2 replay that
// runs off the vaults this pipeline persists).
func TestExtractCandidatesFromReceipts_CorruptMorphoBlueLogFailsRun(t *testing.T) {
	t.Parallel()

	extractor, err := morpho_indexer.NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor: %v", err)
	}

	morphoBlueABI, err := abis.GetMorphoBlueEventsABI()
	if err != nil {
		t.Fatalf("GetMorphoBlueEventsABI: %v", err)
	}
	supplyTopic0 := morphoBlueABI.Events["Supply"].ID.Hex()

	// Recognised topic0 (IsMorphoBlueEvent == true) but only topic0 present:
	// Supply declares three indexed params, so ExtractMorphoBlueEvent fails in
	// parseTopics with a topic-count mismatch.
	corruptLog := shared.Log{
		Address:         morpho_indexer.MorphoBlueAddress.Hex(),
		Topics:          []string{supplyTopic0},
		TransactionHash: "0xabc",
	}
	if !extractor.IsMorphoBlueEvent(corruptLog) {
		t.Fatal("precondition: corrupt log must be a recognised Morpho Blue event")
	}
	if _, decodeErr := extractor.ExtractMorphoBlueEvent(corruptLog); decodeErr == nil {
		t.Fatal("precondition: corrupt log must fail to decode")
	}

	receipts := []shared.TransactionReceipt{{Logs: []shared.Log{corruptLog}}}
	candidateCh := make(chan candidateEntry, 8)

	err = extractCandidatesFromReceipts(receipts, extractor, morpho_indexer.MorphoBlueAddress, 123, candidateCh)
	if !errors.Is(err, errStructuralData) {
		t.Fatalf("error = %v, want a structural defect the retry envelope must not absorb", err)
	}
}

// gatedLister is an S3Reader whose partition listings the test drives: one named
// partition fails at once, every other one waits for release and then answers with
// a complete set of receipt keys, minus an optionally omitted block. Gating is what
// places a worker on a channel send at a chosen moment rather than a hoped-for one.
type gatedLister struct {
	failOn    string
	omitBlock int64
	release   chan struct{}
}

func newGatedLister(failOn string) *gatedLister {
	return &gatedLister{failOn: failOn, omitBlock: -1, release: make(chan struct{})}
}

func (g *gatedLister) ListPrefix(_ context.Context, _, prefix string) ([]string, error) {
	part := strings.TrimSuffix(prefix, "/")
	if part == g.failOn {
		return nil, errors.New("the object store refused the listing")
	}
	<-g.release

	start, end, ok := partitionBlockRange(part)
	if !ok {
		return nil, fmt.Errorf("test fixture built an unparseable partition %q", part)
	}
	keys := make([]string, 0, end-start+1)
	for block := start; block <= end; block++ {
		if block == g.omitBlock {
			continue
		}
		keys = append(keys, s3key.BuildWithPartition(part, block, 1, s3key.Receipts))
	}
	return keys, nil
}

func (g *gatedLister) ListFiles(context.Context, string, string) ([]outbound.S3File, error) {
	return nil, errors.New("ListFiles is not part of the listing path under test")
}

func (g *gatedLister) StreamFile(context.Context, string, string) (io.ReadCloser, error) {
	return nil, errors.New("StreamFile is not part of the listing path under test")
}

// contiguousPartitions returns the prefixes covering blocks [0, n*1000).
func contiguousPartitions(n int) []string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = partition.GetPartition(int64(i) * partition.BlockRangeSize)
	}
	return parts
}

// TestListAllBlockKeys_AbandonedWorkersRetireOnCancellation: the collector stops
// reading the moment a partition listing fails, so every worker still running is
// sending into a channel nobody drains. Their sends must watch the scan's context,
// which scanBlockRange cancels as it returns — a bare send parks the worker for the
// life of the process, and this backfiller's worker is always on, so the leak
// accumulates across every run an S3 hiccup interrupts.
func TestListAllBlockKeys_AbandonedWorkersRetireOnCancellation(t *testing.T) {
	const workers = 2
	parts := contiguousPartitions(20)
	lister := newGatedLister(parts[0])

	// Owned here the way scanBlockRange owns it: it derives a cancellable context
	// and releases it as it returns, which is the moment the collector goes away.
	ctx, cancel := context.WithCancel(context.Background())
	_, err := listAllBlockKeys(ctx, testutil.DiscardLogger(), lister, "test-bucket",
		parts, 0, int64(len(parts))*partition.BlockRangeSize-1, workers, &progress{})
	if err == nil {
		t.Fatal("expected the failing partition listing to surface as an error")
	}

	// Released only now, so the survivor's first send lands in the buffer the
	// abandoned collector left and its next one has nowhere to go.
	close(lister.release)
	awaitStacks(t, "a listing worker to park on the result channel", func(stacks string) bool {
		return listWorkersParkedOnASend(stacks) > 0
	})

	cancel()
	awaitStacks(t, "the abandoned listing workers to retire", func(stacks string) bool {
		return listWorkersInFlight(stacks) == 0
	})
}

// TestListAllBlockKeys_ReturnsEveryPartitionsKeysInBlockOrder pins the semantics
// the cancellation arm must not change: every partition's in-range keys, sorted.
func TestListAllBlockKeys_ReturnsEveryPartitionsKeysInBlockOrder(t *testing.T) {
	parts := contiguousPartitions(5)
	lister := newGatedLister("")
	close(lister.release)
	lastBlock := int64(len(parts))*partition.BlockRangeSize - 1

	keys, err := listAllBlockKeys(context.Background(), testutil.DiscardLogger(), lister, "test-bucket",
		parts, 0, lastBlock, 2, &progress{})
	if err != nil {
		t.Fatalf("listAllBlockKeys: %v", err)
	}

	if int64(len(keys)) != lastBlock+1 {
		t.Fatalf("collected %d block keys, want one per block in [0,%d]", len(keys), lastBlock)
	}
	for i, key := range keys {
		if want := int64(i); key.blockNumber != want {
			t.Errorf("key %d is for block %d, want %d in ascending block order", i, key.blockNumber, want)
		}
	}
}

// TestListAllBlockKeys_RefusesAPartitionTheArchiveHasAHoleIn: the discovery scan
// is the only phase every run performs, so it is where archive completeness has to
// be established. A run over a database with no VaultV2 vault skips the replay
// phase entirely, and with it the check that used to be the sole one — leaving the
// run to report success over a range it never proved was archived.
func TestListAllBlockKeys_RefusesAPartitionTheArchiveHasAHoleIn(t *testing.T) {
	const missingBlock = int64(1_500)
	parts := contiguousPartitions(3)
	lister := newGatedLister("")
	lister.omitBlock = missingBlock
	close(lister.release)

	_, err := listAllBlockKeys(context.Background(), testutil.DiscardLogger(), lister, "test-bucket",
		parts, 0, int64(len(parts))*partition.BlockRangeSize-1, 2, &progress{})

	if !errors.Is(err, errStructuralData) {
		t.Fatalf("error = %v, want the S3 gap reported as a structural defect", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprint(missingBlock)) {
		t.Errorf("error %v should name the missing block %d", err, missingBlock)
	}
}

// listWorkersInFlight counts the listing workers still alive. Counted by frame
// rather than by a goroutine total: the test binary runs plenty of unrelated
// goroutines, and only the ones sitting in listAllBlockKeys' worker pool — which
// sync.WaitGroup.Go puts on the stack, and the partition feeder does not — say
// anything about this leak.
func listWorkersInFlight(stacks string) int {
	return countListWorkers(stacks, func(string) bool { return true })
}

// listWorkersParkedOnASend counts the workers blocked handing a result over.
// Parked, not merely alive: a worker still running would retire at its next
// loop-top context check whatever the send does, so cancelling before one has
// reached the send would pass against the leak it is meant to catch. "[select]"
// is the fixed code's parked state and "[chan send]" the bare send's.
func listWorkersParkedOnASend(stacks string) int {
	return countListWorkers(stacks, func(g string) bool {
		header, _, _ := strings.Cut(g, "\n")
		return strings.Contains(header, "[chan send]") || strings.Contains(header, "[select]")
	})
}

func countListWorkers(stacks string, match func(goroutine string) bool) int {
	count := 0
	for g := range strings.SplitSeq(stacks, "\n\n") {
		if strings.Contains(g, "listAllBlockKeys") && strings.Contains(g, "sync.(*WaitGroup).Go") && match(g) {
			count++
		}
	}
	return count
}

// awaitStacks re-reads the goroutine dump until want holds, because a goroutine is
// a few instructions short of parked (or of dead) when the event that moves it
// returns. Bounded, so a condition that never holds fails rather than hangs.
func awaitStacks(t *testing.T, waitingFor string, want func(stacks string) bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for {
		stacks := goroutineStacks(t)
		if want(stacks) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s:\n%s", waitingFor, stacks)
		}
		time.Sleep(time.Millisecond)
	}
}

func goroutineStacks(t *testing.T) string {
	t.Helper()
	buf := make([]byte, 1<<20)
	return string(buf[:runtime.Stack(buf, true)])
}
