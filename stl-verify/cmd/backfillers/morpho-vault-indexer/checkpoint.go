package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// checkpoint is an append-only JSONL log of replay partitions completed. It
// adapts the canonical pattern in cmd/util/null-payload-refill/state.go
// (truncated-trailing-line tolerance on load, O_APPEND + fsync per record,
// latest-record-per-key wins) to a single "partition done" fact. A local copy
// rather than a shared helper: the record shapes don't overlap — that state
// tracks a five-stage lifecycle, this tracks a boolean — so sharing would mean
// genericising for two callers that need different things. A nil *checkpoint is
// the "no checkpointing" mode (-replay-progress-file unset); all methods are
// nil-safe.
type checkpoint struct {
	file *os.File
	mu   sync.Mutex
	done map[string]struct{}
}

// loadCheckpoint opens (creating if absent) the JSONL file at path, reads the
// completed partitions into memory tolerating a truncated trailing line, and
// leaves the file positioned at the end in append mode.
func loadCheckpoint(path string) (*checkpoint, error) {
	done, err := readCheckpoint(path)
	if err != nil {
		return nil, fmt.Errorf("reading checkpoint: %w", err)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening checkpoint file: %w", err)
	}

	return &checkpoint{file: f, done: done}, nil
}

func readCheckpoint(path string) (map[string]struct{}, error) {
	done := make(map[string]struct{})

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return done, nil
		}
		return nil, err
	}
	defer func() { _ = f.Close() }() // read-only

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec checkpointRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			// Tolerate a partial trailing line from a crash: Scan() has already
			// excluded any further lines by returning false at EOF.
			continue
		}
		if rec.Partition == "" {
			continue
		}
		done[rec.Partition] = struct{}{}
	}
	if err := scanner.Err(); err != nil && err != io.EOF {
		return nil, err
	}
	return done, nil
}

// checkpointRecord is one JSONL line: a partition prefix recorded done.
type checkpointRecord struct {
	Partition string    `json:"partition"`
	Time      time.Time `json:"ts"`
}

// isDone reports whether the partition has already been fully replayed.
func (c *checkpoint) isDone(key string) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.done[key]
	return ok
}

// markDone appends a record for the partition, fsyncs, and updates memory. Only
// called after every event in the partition has replayed successfully.
func (c *checkpoint) markDone(key string) error {
	if c == nil {
		return nil
	}
	if key == "" {
		return fmt.Errorf("markDone: empty key")
	}

	line, err := json.Marshal(checkpointRecord{Partition: key, Time: time.Now().UTC()})
	if err != nil {
		return fmt.Errorf("marshal checkpoint record: %w", err)
	}
	line = append(line, '\n')

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.file.Write(line); err != nil {
		return fmt.Errorf("write checkpoint line: %w", err)
	}
	if err := c.file.Sync(); err != nil {
		return fmt.Errorf("sync checkpoint file: %w", err)
	}
	c.done[key] = struct{}{}
	return nil
}

func (c *checkpoint) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.file == nil {
		return nil
	}
	err := c.file.Close()
	c.file = nil
	return err
}
