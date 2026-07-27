package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

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
// wide case spans single- and five-digit-thousands partitions, where
// lexicographic order ("10000-10999" < "2000-2999") diverges from numeric block
// order. The unaligned cases pin that the partition holding an off-boundary
// `to` is enumerated exactly once — the loop starts partition-aligned and steps
// by a whole partition, so it always lands there on its own.
func TestReplayPartitionPrefixes_AscendingByBlock(t *testing.T) {
	tests := []struct {
		name     string
		from, to int64
		want     []string
	}{
		{
			name: "aligned range crossing lexicographic divergence",
			from: 2000, to: 10999,
			want: []string{
				"2000-2999", "3000-3999", "4000-4999", "5000-5999",
				"6000-6999", "7000-7999", "8000-8999", "9000-9999", "10000-10999",
			},
		},
		{
			name: "both bounds mid-partition",
			from: 1500, to: 3500,
			want: []string{"1000-1999", "2000-2999", "3000-3999"},
		},
		{
			name: "single partition, both bounds mid-partition",
			from: 1200, to: 1800,
			want: []string{"1000-1999"},
		},
		{
			name: "to on a partition's last block",
			from: 1000, to: 1999,
			want: []string{"1000-1999"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replayPartitionPrefixes(tt.from, tt.to)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("replayPartitionPrefixes(%d, %d) = %v, want %v", tt.from, tt.to, got, tt.want)
			}
		})
	}
}

func vaultSet(addrs ...common.Address) map[common.Address]struct{} {
	set := make(map[common.Address]struct{}, len(addrs))
	for _, a := range addrs {
		set[a] = struct{}{}
	}
	return set
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

// TestRunReplayPartitions_ReplaysEveryPartitionInBlockOrder: a run replays the
// full requested range, in ascending block order, so an AddAdapter in an earlier
// partition always lands before a later partition's Allocate.
func TestRunReplayPartitions_ReplaysEveryPartitionInBlockOrder(t *testing.T) {
	const from, to = int64(2000), int64(4500)

	var replayed []string
	err := runReplayPartitions(replayPartitionPrefixes(from, to), func(part string) error {
		replayed = append(replayed, part)
		return nil
	})
	if err != nil {
		t.Fatalf("runReplayPartitions: %v", err)
	}

	want := []string{"2000-2999", "3000-3999", "4000-4999"}
	if !slices.Equal(replayed, want) {
		t.Fatalf("replayed %v, want %v", replayed, want)
	}
}

// TestRunReplayPartitions_StopsAtTheFirstFailingPartition: a failing partition
// stops the run there. Continuing would replay later partitions on top of the
// hole the failure left, and nothing downstream can detect that hole.
func TestRunReplayPartitions_StopsAtTheFirstFailingPartition(t *testing.T) {
	var replayed []string
	err := runReplayPartitions([]string{"0-999", "1000-1999", "2000-2999"}, func(part string) error {
		replayed = append(replayed, part)
		if part == "1000-1999" {
			return errors.New("boom")
		}
		return nil
	})

	if err == nil {
		t.Fatal("expected a failing partition to fail the run")
	}
	if !strings.Contains(err.Error(), "1000-1999") {
		t.Errorf("error %v does not name the failing partition", err)
	}
	if want := []string{"0-999", "1000-1999"}; !slices.Equal(replayed, want) {
		t.Fatalf("replayed %v, want %v (nothing after the failure)", replayed, want)
	}
}

// TestCollectPartitionV2Logs_MissingReceiptBlockErrors: a block in the
// partition's [from,to] intersection with no receipt key contributes no logs, so
// collection must hard-fail rather than hand the loop a silently thinned
// partition.
func TestCollectPartitionV2Logs_MissingReceiptBlockErrors(t *testing.T) {
	ctx := context.Background()
	const from, to = int64(0), int64(999)

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
	_, err := collectPartitionV2Logs(ctx, reader, "bucket", part, from, to, 4, map[common.Address]struct{}{}, mustV2Topics(t))
	if err == nil {
		t.Fatal("expected an error for a partition missing a receipt block")
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
// downloads run across the worker pool, and completion order must not leak into the
// result. Order no longer decides the answers (see replayPartition), but it decides
// whether an Allocate replayed ahead of its AddAdapter manufactures the
// inferred-membership WARN that otherwise means a discovery gap. The fixture stalls
// low blocks longest, so an append-as-completed collector would return them last.
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
