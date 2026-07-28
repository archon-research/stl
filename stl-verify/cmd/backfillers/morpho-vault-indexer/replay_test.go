package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

func mustV2Topics(t *testing.T) map[common.Hash]struct{} {
	t.Helper()
	topics, err := morpho_indexer.VaultV2StructuredEventTopics()
	if err != nil {
		t.Fatalf("VaultV2StructuredEventTopics: %v", err)
	}
	return topics
}

func v2EventTopic(t *testing.T, name string) common.Hash {
	t.Helper()
	abiV2, err := abis.GetVaultV2EventsABI()
	if err != nil {
		t.Fatalf("GetVaultV2EventsABI: %v", err)
	}
	ev, ok := abiV2.Events[name]
	if !ok {
		t.Fatalf("event %q not in VaultV2 ABI", name)
	}
	return ev.ID
}

// TestFilterV2Logs keeps only logs from a known V2 vault whose topic0 is a
// structured V2 event, and carries block coordinates through.
func TestFilterV2Logs(t *testing.T) {
	vaultA := common.HexToAddress("0xaa00000000000000000000000000000000000001")
	vaultB := common.HexToAddress("0xaa00000000000000000000000000000000000002") // not in the V2 set
	v2Vaults := map[common.Address]struct{}{vaultA: {}}
	topics := mustV2Topics(t)

	addAdapter := v2EventTopic(t, "AddAdapter")
	setCurator := v2EventTopic(t, "SetCurator") // registered, but audit-log only (no structured handler)
	blockHash := "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"

	receipts := []shared.TransactionReceipt{{
		BlockHash: blockHash,
		Logs: []shared.Log{
			{Address: vaultA.Hex(), Topics: []string{addAdapter.Hex()}, LogIndex: "0x5"}, // keep
			{Address: vaultA.Hex(), Topics: []string{setCurator.Hex()}, LogIndex: "0x6"}, // drop: not structured
			{Address: vaultB.Hex(), Topics: []string{addAdapter.Hex()}, LogIndex: "0x7"}, // drop: not a V2 vault
			{Address: vaultA.Hex(), Topics: []string{}, LogIndex: "0x8"},                 // drop: no topics
		},
	}}

	entries, err := filterV2Logs(receipts, 100, v2Vaults, topics)
	if err != nil {
		t.Fatalf("filterV2Logs: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d: %+v", len(entries), entries)
	}
	e := entries[0]
	if e.blockNumber != 100 {
		t.Errorf("blockNumber = %d, want 100", e.blockNumber)
	}
	if e.logIndex != 5 {
		t.Errorf("logIndex = %d, want 5", e.logIndex)
	}
	if e.blockHash != common.HexToHash(blockHash) {
		t.Errorf("blockHash = %s, want %s", e.blockHash, blockHash)
	}
	if !common.IsHexAddress(e.log.Address) || common.HexToAddress(e.log.Address) != vaultA {
		t.Errorf("log.Address = %s, want %s", e.log.Address, vaultA.Hex())
	}
}

// TestFilterV2Logs_MalformedLogIndexErrors: a structural parse failure must
// bubble up, not be silently skipped.
func TestFilterV2Logs_MalformedLogIndexErrors(t *testing.T) {
	vault := common.HexToAddress("0xaa00000000000000000000000000000000000001")
	receipts := []shared.TransactionReceipt{{
		BlockHash: "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
		Logs: []shared.Log{
			{Address: vault.Hex(), Topics: []string{v2EventTopic(t, "AddAdapter").Hex()}, LogIndex: "not-a-number"},
		},
	}}
	_, err := filterV2Logs(receipts, 100, map[common.Address]struct{}{vault: {}}, mustV2Topics(t))
	if err == nil {
		t.Fatal("expected an error for a malformed logIndex")
	}
}

// TestFilterV2Logs_MissingBlockHashErrors: Allocate/Deallocate replay pins
// realAssets() to the block hash, so a matching receipt without one is a data
// defect, not a skippable row.
func TestFilterV2Logs_MissingBlockHashErrors(t *testing.T) {
	vault := common.HexToAddress("0xaa00000000000000000000000000000000000001")
	receipts := []shared.TransactionReceipt{{
		BlockHash: "",
		Logs: []shared.Log{
			{Address: vault.Hex(), Topics: []string{v2EventTopic(t, "AddAdapter").Hex()}, LogIndex: "0x1"},
		},
	}}
	_, err := filterV2Logs(receipts, 100, map[common.Address]struct{}{vault: {}}, mustV2Topics(t))
	if err == nil {
		t.Fatal("expected an error for a matching receipt with an empty block hash")
	}
}

// TestSortV2LogEntries sorts strictly by (blockNumber, logIndex) ascending.
func TestSortV2LogEntries(t *testing.T) {
	entries := []v2LogEntry{
		{blockNumber: 200, logIndex: 1},
		{blockNumber: 100, logIndex: 9},
		{blockNumber: 100, logIndex: 2},
		{blockNumber: 200, logIndex: 0},
	}
	sortV2LogEntries(entries)

	want := []struct{ bn, li int64 }{{100, 2}, {100, 9}, {200, 0}, {200, 1}}
	for i, w := range want {
		if entries[i].blockNumber != w.bn || entries[i].logIndex != w.li {
			t.Errorf("entries[%d] = (%d,%d), want (%d,%d)", i, entries[i].blockNumber, entries[i].logIndex, w.bn, w.li)
		}
	}
}

// TestReplayPartitionPrefixes_AscendingByBlock locks the partition order the
// replay depends on: strictly ascending by start block, NOT lexicographic. The
// range spans single- and five-digit-thousands partitions, where lexicographic
// order ("10000-10999" < "2000-2999") diverges from numeric block order.
func TestReplayPartitionPrefixes_AscendingByBlock(t *testing.T) {
	got := replayPartitionPrefixes(2000, 10999)
	want := []string{
		"2000-2999", "3000-3999", "4000-4999", "5000-5999",
		"6000-6999", "7000-7999", "8000-8999", "9000-9999", "10000-10999",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d partitions %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("partition[%d] = %s, want %s (full: %v)", i, got[i], want[i], got)
		}
	}
}

// TestPartitionFullyCovered pins the checkpoint-eligibility rule: a partition is
// fully covered only when its entire block range lies within [from,to]. A
// boundary partition (bounds aligned to a partition edge still count as fully
// covered; only a partition the range partially overlaps does not) and an
// unparseable prefix are not.
func TestPartitionFullyCovered(t *testing.T) {
	tests := []struct {
		name     string
		part     string
		from, to int64
		want     bool
	}{
		{"interior partition fully inside", "3000-3999", 2000, 4999, true},
		{"fully-covered boundary: from aligned to partition start", "2000-2999", 2000, 4999, true},
		{"fully-covered boundary: to aligned to partition end", "4000-4999", 2000, 4999, true},
		{"partially-covered first: from mid-partition", "1000-1999", 1500, 4999, false},
		{"partially-covered last: to mid-partition", "4000-4999", 2000, 4500, false},
		{"unparseable prefix", "not-a-range", 0, 10000, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := partitionFullyCovered(tt.part, tt.from, tt.to); got != tt.want {
				t.Errorf("partitionFullyCovered(%q, %d, %d) = %v, want %v", tt.part, tt.from, tt.to, got, tt.want)
			}
		})
	}
}

// discardLogger is the logger the replay-loop tests pass: they assert on the
// loop's effects, not its output.
func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func vaultSet(addrs ...common.Address) map[common.Address]struct{} {
	set := make(map[common.Address]struct{}, len(addrs))
	for _, a := range addrs {
		set[a] = struct{}{}
	}
	return set
}

// TestV2VaultSetDigest_IdentifiesTheSetNotItsOrder: the digest scopes checkpoint
// records to the vault set they were replayed for, so it must depend on set
// membership alone — not on map iteration order or address casing — and must
// change the moment a vault joins the set.
func TestV2VaultSetDigest_IdentifiesTheSetNotItsOrder(t *testing.T) {
	a := common.HexToAddress("0xaa00000000000000000000000000000000000001")
	b := common.HexToAddress("0xbb00000000000000000000000000000000000002")

	digestA := v2VaultSetDigest(vaultSet(a))
	if digestA == "" {
		t.Fatal("digest of a non-empty vault set is empty")
	}
	if got := v2VaultSetDigest(vaultSet(common.HexToAddress(strings.ToUpper("0xAA00000000000000000000000000000000000001")))); got != digestA {
		t.Errorf("digest is casing-sensitive: %s != %s", got, digestA)
	}
	if got := v2VaultSetDigest(vaultSet(b, a)); got != v2VaultSetDigest(vaultSet(a, b)) {
		t.Errorf("digest depends on insertion order: %s != %s", got, v2VaultSetDigest(vaultSet(a, b)))
	}
	if got := v2VaultSetDigest(vaultSet(a, b)); got == digestA {
		t.Error("a grown vault set must not share the smaller set's digest")
	}
}

// TestRunReplayPartitions_GrownVaultSetReplaysRecordedPartitions locks the
// vault-set-hole fix. Run 1 replays and checkpoints every partition with vault
// set {A}. Run 2 sees {A,B} — B was probe-skipped, discovered later by the live
// stream, or first active outside run 1's range — and must replay every
// partition again: run 1's records say nothing about B's governance history, and
// nothing else ever converges it (a later Allocate lazily self-heals B's adapter
// at the wrong added_at_block).
func TestRunReplayPartitions_GrownVaultSetReplaysRecordedPartitions(t *testing.T) {
	const from, to = int64(2000), int64(3999)
	path := t.TempDir() + "/progress.jsonl"
	vaultA := common.HexToAddress("0xaa00000000000000000000000000000000000001")
	vaultB := common.HexToAddress("0xbb00000000000000000000000000000000000002")

	scopeFor := func(vaults map[common.Address]struct{}) checkpointScope {
		return checkpointScope{ChainID: 1, Bucket: "raw-mainnet", VaultsDigest: v2VaultSetDigest(vaults)}
	}
	runWith := func(scope checkpointScope) []string {
		cp, err := loadCheckpoint(path, scope)
		if err != nil {
			t.Fatalf("loadCheckpoint: %v", err)
		}
		defer cp.Close()
		return replayRecorder(t, cp, from, to)
	}

	wantAll := []string{"2000-2999", "3000-3999"}
	if first := runWith(scopeFor(vaultSet(vaultA))); !slices.Equal(first, wantAll) {
		t.Fatalf("run 1 replayed %v, want %v", first, wantAll)
	}
	// Same vault set: the records are trusted, nothing replays again.
	if resumed := runWith(scopeFor(vaultSet(vaultA))); len(resumed) != 0 {
		t.Fatalf("same-vault-set resume replayed %v, want nothing", resumed)
	}
	if grown := runWith(scopeFor(vaultSet(vaultA, vaultB))); !slices.Equal(grown, wantAll) {
		t.Fatalf("grown-vault-set run replayed %v, want %v (run 1's records cover neither vault B nor its governance history)", grown, wantAll)
	}
}

// replayRecorder drives runReplayPartitions and records which partitions the
// production loop actually handed to the replay callback, in order.
func replayRecorder(t *testing.T, cp *checkpoint, from, to int64) []string {
	t.Helper()
	var replayed []string
	err := runReplayPartitions(discardLogger(), replayPartitionPrefixes(from, to), cp, from, to, func(part string) error {
		replayed = append(replayed, part)
		return nil
	})
	if err != nil {
		t.Fatalf("runReplayPartitions: %v", err)
	}
	return replayed
}

// TestReplayCheckpointsOnlyFullyCoveredPartitions locks the cross-run-hole fix:
// the replay loop checkpoints a partition only when its full block range lies
// within [from,to]. A partially-covered boundary partition replays every run but
// stays out of the checkpoint, so a later run reusing the same progress file
// with wider bounds still revisits it. Here from=2000 is partition-aligned (so
// "2000-2999" is a fully-covered boundary) and to=4500 falls mid-"4000-4999"
// (the partially-covered last partition).
func TestReplayCheckpointsOnlyFullyCoveredPartitions(t *testing.T) {
	const from, to = int64(2000), int64(4500)
	path := t.TempDir() + "/progress.jsonl"

	cp1, err := loadCheckpoint(path, testScope())
	if err != nil {
		t.Fatalf("loadCheckpoint: %v", err)
	}
	first := replayRecorder(t, cp1, from, to)
	if err := cp1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	wantFirst := []string{"2000-2999", "3000-3999", "4000-4999"}
	if !slices.Equal(first, wantFirst) {
		t.Fatalf("first pass replayed %v, want %v", first, wantFirst)
	}

	// Reopen from disk — the cross-run scenario the fix guards. The two
	// fully-covered partitions are recorded; the partially-covered last partition
	// is not, so its blocks stay replayable under wider bounds.
	cp2, err := loadCheckpoint(path, testScope())
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer cp2.Close()

	if !cp2.isDone("2000-2999") {
		t.Error(`"2000-2999" (fully-covered boundary) should be checkpointed`)
	}
	if !cp2.isDone("3000-3999") {
		t.Error(`"3000-3999" (interior) should be checkpointed`)
	}
	if cp2.isDone("4000-4999") {
		t.Error(`"4000-4999" (partially-covered last) must not be checkpointed`)
	}

	// Resume replays ONLY the uncheckpointed boundary partition.
	second := replayRecorder(t, cp2, from, to)
	wantSecond := []string{"4000-4999"}
	if !slices.Equal(second, wantSecond) {
		t.Fatalf("resume replayed %v, want %v", second, wantSecond)
	}
}

// --- missing-receipt-block guard ---

// fakeReplayS3Reader serves a fixed key listing and per-key JSON bodies so the
// partition-collection path can be driven without real S3. onStream, when set,
// runs inside StreamFile so a test can observe or stall concurrent downloads.
type fakeReplayS3Reader struct {
	keys      []string
	bodyByKey map[string]string
	onStream  func(key string)
}

func (f *fakeReplayS3Reader) ListFiles(context.Context, string, string) ([]outbound.S3File, error) {
	return nil, nil
}

func (f *fakeReplayS3Reader) ListPrefix(_ context.Context, _, prefix string) ([]string, error) {
	var out []string
	for _, k := range f.keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (f *fakeReplayS3Reader) StreamFile(_ context.Context, _, key string) (io.ReadCloser, error) {
	if f.onStream != nil {
		f.onStream(key)
	}
	body, ok := f.bodyByKey[key]
	if !ok {
		return nil, errors.New("no such key: " + key)
	}
	return io.NopCloser(strings.NewReader(body)), nil
}

// TestReplayPartition_MissingReceiptBlockErrorsAndLeavesCheckpointUnwritten:
// a block in the partition's [from,to] intersection with no receipt key would
// contribute no logs, yet the partition would still be marked done — silently
// dropping every event in that block, since a re-run skips the checkpointed
// partition. collectPartitionV2Logs must hard-fail so the production loop stops
// before markDone, leaving the checkpoint unwritten for a repaired-S3 re-run.
// The range is the whole partition, so the fully-covered guard would otherwise
// checkpoint it.
func TestReplayPartition_MissingReceiptBlockErrorsAndLeavesCheckpointUnwritten(t *testing.T) {
	ctx := context.Background()
	const from, to = int64(0), int64(999) // partition "0-999", fully covered

	keyFor := func(bn int64) string { return s3key.Build(bn, 0, s3key.Receipts) }
	// S3 has a receipt for every block in the partition except 500.
	reader := &fakeReplayS3Reader{bodyByKey: map[string]string{}}
	for bn := from; bn <= to; bn++ {
		if bn == 500 {
			continue
		}
		reader.keys = append(reader.keys, keyFor(bn))
		reader.bodyByKey[keyFor(bn)] = "[]"
	}
	part := partition.GetPartition(from)

	cp, err := loadCheckpoint(t.TempDir()+"/progress.jsonl", testScope())
	if err != nil {
		t.Fatalf("loadCheckpoint: %v", err)
	}
	defer cp.Close()

	err = runReplayPartitions(discardLogger(), []string{part}, cp, from, to, func(part string) error {
		_, collectErr := collectPartitionV2Logs(ctx, reader, "bucket", part, from, to, 4, map[common.Address]struct{}{}, mustV2Topics(t))
		return collectErr
	})
	if err == nil {
		t.Fatal("expected an error for a partition missing a receipt block")
	}
	if cp.isDone(part) {
		t.Fatal("checkpoint must stay unwritten when a partition is missing a receipt block")
	}
}

// TestCollectPartitionV2Logs_AllBlocksPresentNoError is the complement: when
// every block in [from,to] has a receipt, collection succeeds even if no receipt
// carries a structured V2 log.
func TestCollectPartitionV2Logs_AllBlocksPresentNoError(t *testing.T) {
	ctx := context.Background()
	const from, to = int64(100), int64(102)

	keyFor := func(bn int64) string { return s3key.Build(bn, 0, s3key.Receipts) }
	reader := &fakeReplayS3Reader{
		keys: []string{keyFor(100), keyFor(101), keyFor(102)},
		bodyByKey: map[string]string{
			keyFor(100): "[]",
			keyFor(101): "[]",
			keyFor(102): "[]",
		},
	}
	part := partition.GetPartition(from)

	entries, err := collectPartitionV2Logs(ctx, reader, "bucket", part, from, to, 4, map[common.Address]struct{}{}, mustV2Topics(t))
	if err != nil {
		t.Fatalf("unexpected error with all blocks present: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("want 0 entries (empty receipts), got %d", len(entries))
	}
}

// v2LogReceiptJSON is a one-receipt receipt file carrying a single AddAdapter
// log from vault, so a fixture block contributes exactly one collected entry.
func v2LogReceiptJSON(t *testing.T, vault common.Address, blockNumber int64) string {
	t.Helper()
	body, err := json.Marshal([]shared.TransactionReceipt{{
		TransactionHash: fmt.Sprintf("0x%064x", blockNumber),
		BlockHash:       fmt.Sprintf("0x%064x", blockNumber),
		Logs: []shared.Log{
			{Address: vault.Hex(), Topics: []string{v2EventTopic(t, "AddAdapter").Hex()}, LogIndex: "0x0"},
		},
	}})
	if err != nil {
		t.Fatalf("marshal receipts: %v", err)
	}
	return string(body)
}

// v2LogPartitionFixture builds a reader serving one AddAdapter-bearing receipt
// file per block in [from,to].
func v2LogPartitionFixture(t *testing.T, vault common.Address, from, to int64) *fakeReplayS3Reader {
	t.Helper()
	reader := &fakeReplayS3Reader{bodyByKey: map[string]string{}}
	for bn := from; bn <= to; bn++ {
		key := s3key.Build(bn, 0, s3key.Receipts)
		reader.keys = append(reader.keys, key)
		reader.bodyByKey[key] = v2LogReceiptJSON(t, vault, bn)
	}
	return reader
}

// TestCollectPartitionV2Logs_ConcurrentDownloadsPreserveBlockOrder: the per-block
// downloads run across the worker pool, but replay ordering is
// correctness-critical (an AddAdapter must land before that adapter's first
// Allocate), so completion order must not leak into the result. The fixture
// stalls low blocks longest, so an append-as-completed collector would return
// them last.
func TestCollectPartitionV2Logs_ConcurrentDownloadsPreserveBlockOrder(t *testing.T) {
	ctx := context.Background()
	const from, to = int64(100), int64(109)
	vault := common.HexToAddress("0xaa00000000000000000000000000000000000001")

	reader := v2LogPartitionFixture(t, vault, from, to)
	reader.onStream = func(key string) {
		parsed, ok := s3key.Parse(key)
		if !ok {
			t.Errorf("unparseable key %q", key)
			return
		}
		time.Sleep(time.Duration(to+1-parsed.BlockNumber) * 2 * time.Millisecond)
	}

	entries, err := collectPartitionV2Logs(ctx, reader, "bucket", partition.GetPartition(from), from, to, 10, vaultSet(vault), mustV2Topics(t))
	if err != nil {
		t.Fatalf("collectPartitionV2Logs: %v", err)
	}
	if len(entries) != int(to-from+1) {
		t.Fatalf("got %d entries, want %d", len(entries), to-from+1)
	}
	for i, e := range entries {
		if want := from + int64(i); e.blockNumber != want {
			t.Fatalf("entries[%d].blockNumber = %d, want %d (collection order is not block order)", i, e.blockNumber, want)
		}
	}
}

// TestCollectPartitionV2Logs_BoundsConcurrencyToWorkers: the pool size comes from
// -goroutines, so an unbounded fan-out over a 1000-block partition cannot open a
// thousand simultaneous S3 downloads.
func TestCollectPartitionV2Logs_BoundsConcurrencyToWorkers(t *testing.T) {
	ctx := context.Background()
	const from, to = int64(100), int64(139)
	const workers = 3
	vault := common.HexToAddress("0xaa00000000000000000000000000000000000001")

	var inFlight, maxInFlight atomic.Int64
	reader := v2LogPartitionFixture(t, vault, from, to)
	reader.onStream = func(string) {
		current := inFlight.Add(1)
		for {
			observed := maxInFlight.Load()
			if current <= observed || maxInFlight.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		inFlight.Add(-1)
	}

	if _, err := collectPartitionV2Logs(ctx, reader, "bucket", partition.GetPartition(from), from, to, workers, vaultSet(vault), mustV2Topics(t)); err != nil {
		t.Fatalf("collectPartitionV2Logs: %v", err)
	}
	if got := maxInFlight.Load(); got > workers {
		t.Errorf("peak concurrent downloads = %d, want <= %d", got, workers)
	}
	if maxInFlight.Load() < 2 {
		t.Error("downloads never overlapped; collection is still serial")
	}
}

// --- block timestamp cache ---

type fakeHeaderFetcher struct {
	calls      map[common.Hash]int
	timeByHash map[common.Hash]uint64
	err        error
}

func (f *fakeHeaderFetcher) HeaderByHash(_ context.Context, hash common.Hash) (*ethtypes.Header, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.calls[hash]++
	return &ethtypes.Header{Time: f.timeByHash[hash]}, nil
}

// TestBlockTimestampCache fetches each distinct block hash once and reuses the
// result.
func TestBlockTimestampCache(t *testing.T) {
	hashA := common.HexToHash("0xaa")
	hashB := common.HexToHash("0xbb")
	f := &fakeHeaderFetcher{calls: map[common.Hash]int{}, timeByHash: map[common.Hash]uint64{hashA: 1_700_000_000, hashB: 1_700_000_500}}
	c := newBlockTimestampCache(f)

	ts1, err := c.timestampAt(context.Background(), hashA)
	if err != nil {
		t.Fatalf("timestampAt(hashA): %v", err)
	}
	ts2, err := c.timestampAt(context.Background(), hashA)
	if err != nil {
		t.Fatalf("timestampAt(hashA) again: %v", err)
	}
	if !ts1.Equal(ts2) || !ts1.Equal(time.Unix(1_700_000_000, 0).UTC()) {
		t.Errorf("timestamps = %v / %v, want %v", ts1, ts2, time.Unix(1_700_000_000, 0).UTC())
	}
	if f.calls[hashA] != 1 {
		t.Errorf("hashA fetched %d times, want 1 (cached)", f.calls[hashA])
	}

	if _, err := c.timestampAt(context.Background(), hashB); err != nil {
		t.Fatalf("timestampAt(hashB): %v", err)
	}
	if f.calls[hashB] != 1 {
		t.Errorf("hashB fetched %d times, want 1", f.calls[hashB])
	}
}

// TestBlockTimestampCache_FetchErrorPropagates: a header fetch failure must
// surface (transient RPC failure → stop, retry), never yield a zero timestamp.
func TestBlockTimestampCache_FetchErrorPropagates(t *testing.T) {
	f := &fakeHeaderFetcher{calls: map[common.Hash]int{}, err: errors.New("rpc down")}
	c := newBlockTimestampCache(f)
	if _, err := c.timestampAt(context.Background(), common.HexToHash("0xaa")); err == nil {
		t.Fatal("expected the fetch error to propagate")
	}
}
