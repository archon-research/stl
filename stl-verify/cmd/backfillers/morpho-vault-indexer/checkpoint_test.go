package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestCheckpointRoundTrip records completed partitions, reopens the file, and
// confirms the recorded ones are skipped and unrecorded ones are not.
func TestCheckpointRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "progress.jsonl")

	c, err := loadCheckpoint(path)
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

	reopened, err := loadCheckpoint(path)
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

	content := `{"partition":"0-999","ts":"2026-07-21T00:00:00Z"}` + "\n" +
		`{"partition":"1000-1999","ts":"2026-07-21T00:00:01Z"}` + "\n" +
		`{"partition":"2000-29` // truncated, no newline
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	c, err := loadCheckpoint(path)
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
