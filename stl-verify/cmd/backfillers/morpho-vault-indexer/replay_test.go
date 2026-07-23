package main

import (
	"context"
	"errors"
	"io"
	"strings"
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

// --- missing-receipt-block guard ---

// fakeReplayS3Reader serves a fixed key listing and per-key JSON bodies so the
// partition-collection path can be driven without real S3.
type fakeReplayS3Reader struct {
	keys      []string
	bodyByKey map[string]string
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
// partition. collectPartitionV2Logs must hard-fail so the outer loop's guard
// skips markDone, leaving the checkpoint unwritten for a repaired-S3 re-run.
func TestReplayPartition_MissingReceiptBlockErrorsAndLeavesCheckpointUnwritten(t *testing.T) {
	ctx := context.Background()
	const from, to = int64(100), int64(102) // partition "0-999"; blocks 100..102 all required

	keyFor := func(bn int64) string { return s3key.Build(bn, 0, s3key.Receipts) }
	// S3 has receipts for 100 and 102 but is missing 101.
	reader := &fakeReplayS3Reader{
		keys: []string{keyFor(100), keyFor(102)},
		bodyByKey: map[string]string{
			keyFor(100): "[]",
			keyFor(102): "[]",
		},
	}
	part := partition.GetPartition(from)

	cp, err := loadCheckpoint(t.TempDir() + "/progress.jsonl")
	if err != nil {
		t.Fatalf("loadCheckpoint: %v", err)
	}
	defer cp.Close()

	// Mirror the replayV2StructuredEvents per-partition guard: only markDone when
	// collection succeeds.
	processPartition := func(part string) error {
		if _, err := collectPartitionV2Logs(ctx, reader, "bucket", part, from, to, map[common.Address]struct{}{}, mustV2Topics(t)); err != nil {
			return err
		}
		return cp.markDone(part)
	}

	if err := processPartition(part); err == nil {
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

	entries, err := collectPartitionV2Logs(ctx, reader, "bucket", part, from, to, map[common.Address]struct{}{}, mustV2Topics(t))
	if err != nil {
		t.Fatalf("unexpected error with all blocks present: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("want 0 entries (empty receipts), got %d", len(entries))
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
