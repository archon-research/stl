package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// testScope is the scope the checkpoint tests record under unless they are
// exercising a scope mismatch.
func testScope() checkpointScope {
	return checkpointScope{ChainID: 1, Bucket: "raw-mainnet", VaultsDigest: "digest-a"}
}

// TestCheckpointRoundTrip records completed partitions, reopens the file, and
// confirms the recorded ones are skipped and unrecorded ones are not.
func TestCheckpointRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")

	c, err := loadCheckpoint(path, testScope())
	if err != nil {
		t.Fatalf("loadCheckpoint: %v", err)
	}
	if c.isDone("0-999") {
		t.Error("fresh checkpoint reports 0-999 done")
	}
	if err := c.markDone("0-999"); err != nil {
		t.Fatalf("markDone(0-999): %v", err)
	}
	if err := c.markDone("1000-1999"); err != nil {
		t.Fatalf("markDone(1000-1999): %v", err)
	}
	if !c.isDone("0-999") {
		t.Error("in-memory checkpoint lost 0-999 after markDone")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := loadCheckpoint(path, testScope())
	if err != nil {
		t.Fatalf("reopen loadCheckpoint: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if !reopened.isDone("0-999") {
		t.Error("reopened checkpoint lost 0-999")
	}
	if !reopened.isDone("1000-1999") {
		t.Error("reopened checkpoint lost 1000-1999")
	}
	if reopened.isDone("2000-2999") {
		t.Error("reopened checkpoint reports an unrecorded partition done")
	}
}

// TestCheckpointToleratesTruncatedTrailingLine mirrors the canonical
// null-payload-refill loader: a partial trailing line written by a crash must
// not abort the load; the complete records ahead of it still count.
func TestCheckpointToleratesTruncatedTrailingLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")

	content := recordLine(t, "0-999", testScope()) +
		recordLine(t, "1000-1999", testScope()) +
		`{"partition":"2000-29` // truncated, no newline
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	c, err := loadCheckpoint(path, testScope())
	if err != nil {
		t.Fatalf("loadCheckpoint: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if !c.isDone("0-999") {
		t.Error("lost 0-999 due to truncated trailing line")
	}
	if !c.isDone("1000-1999") {
		t.Error("lost 1000-1999 due to truncated trailing line")
	}
	if c.isDone("2000-2999") {
		t.Error("counted a truncated partition as done")
	}
}

// TestCheckpointRepairsTornTrailingLineBeforeAppending: readCheckpoint tolerates
// a crash-torn trailing line, but O_APPEND then concatenates the next record
// onto the fragment into one unparseable line — silently losing that record for
// good. loadCheckpoint must terminate the fragment before appending.
func TestCheckpointRepairsTornTrailingLineBeforeAppending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")

	content := recordLine(t, "0-999", testScope()) +
		`{"partition":"1000-19` // torn by a crash mid-write, no newline
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	c, err := loadCheckpoint(path, testScope())
	if err != nil {
		t.Fatalf("loadCheckpoint: %v", err)
	}
	if err := c.markDone("2000-2999"); err != nil {
		t.Fatalf("markDone: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := loadCheckpoint(path, testScope())
	if err != nil {
		t.Fatalf("reopen loadCheckpoint: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if !reopened.isDone("0-999") {
		t.Error("lost the record written before the torn line")
	}
	if !reopened.isDone("2000-2999") {
		t.Error("the record appended after a torn line did not survive a reload")
	}
}

// recordLine renders one JSONL checkpoint line the way markDone writes it, so
// the scope tests build fixtures without hand-writing the record's JSON shape.
func recordLine(t *testing.T, part string, scope checkpointScope) string {
	t.Helper()
	line, err := json.Marshal(checkpointRecord{Partition: part, Time: time.Unix(0, 0).UTC(), checkpointScope: scope})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return string(line) + "\n"
}

// TestCheckpointDistrustsRecordsFromAnotherScope: a "partition done" fact is
// only true for the chain, bucket, and V2 vault set it was replayed under.
// Reusing a progress file across any of those must not skip the partition —
// silently losing (for a grown vault set) the new vaults' whole governance
// history, with no error and no later convergence. A legacy record predating the
// scope fields carries no scope at all and is distrusted for the same reason.
func TestCheckpointDistrustsRecordsFromAnotherScope(t *testing.T) {
	current := testScope()
	tests := []struct {
		name     string
		recorded string
	}{
		{"grown vault set", recordLine(t, "0-999", checkpointScope{ChainID: 1, Bucket: "raw-mainnet", VaultsDigest: "digest-a-and-b"})},
		{"different chain", recordLine(t, "0-999", checkpointScope{ChainID: 8453, Bucket: "raw-mainnet", VaultsDigest: "digest-a"})},
		{"different bucket", recordLine(t, "0-999", checkpointScope{ChainID: 1, Bucket: "raw-staging", VaultsDigest: "digest-a"})},
		{"legacy record without scope", `{"partition":"0-999","ts":"2026-07-21T00:00:00Z"}` + "\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "progress.jsonl")
			if err := os.WriteFile(path, []byte(tt.recorded), 0o644); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			c, err := loadCheckpoint(path, current)
			if err != nil {
				t.Fatalf("loadCheckpoint: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			if c.isDone("0-999") {
				t.Error("a record from another scope was trusted as done")
			}
		})
	}
}

// TestCheckpointNilSafe: the "no checkpointing" mode uses a nil *checkpoint, so
// its methods must be safe to call.
func TestCheckpointNilSafe(t *testing.T) {
	var c *checkpoint
	if c.isDone("0-999") {
		t.Error("nil checkpoint reports done")
	}
	if err := c.markDone("0-999"); err != nil {
		t.Errorf("nil markDone: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("nil Close: %v", err)
	}
}
