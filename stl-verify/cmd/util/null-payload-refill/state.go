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

// Stage represents the recorded progress of a single key.
type Stage string

const (
	StageS3     Stage = "s3"
	StageSNS    Stage = "sns"
	StageSkip   Stage = "skip"
	StageFail   Stage = "fail"
	StageDryRun Stage = "dry-run"
)

// Record is one JSONL line in the state file.
type Record struct {
	Key    string    `json:"key"`
	Stage  Stage     `json:"stage"`
	Reason string    `json:"reason,omitempty"`
	Time   time.Time `json:"ts"`
}

// State is an append-only JSONL checkpoint of per-key progress.
// Each call to Record appends one line and fsyncs the file.
// Latest record per key wins on lookup, which makes restart idempotent.
type State struct {
	path    string
	file    *os.File
	mu      sync.Mutex
	history map[string]Record
}

// Load opens the state file at path (creating it if missing), reads all
// complete lines into an in-memory map, and returns a State ready for
// further appends. Truncated trailing lines are tolerated; the file is
// left in append mode positioned at the end.
func Load(path string) (*State, error) {
	history, err := readHistory(path)
	if err != nil {
		return nil, fmt.Errorf("reading state history: %w", err)
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening state file: %w", err)
	}

	return &State{
		path:    path,
		file:    f,
		history: history,
	}, nil
}

func readHistory(path string) (map[string]Record, error) {
	history := make(map[string]Record)

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return history, nil
		}
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			// Tolerate a partial trailing line written by a crash;
			// continue rather than abort because further lines are
			// already excluded by Scan() returning false at EOF.
			continue
		}
		if rec.Key == "" {
			continue
		}
		history[rec.Key] = rec
	}
	if err := scanner.Err(); err != nil && err != io.EOF {
		return nil, err
	}
	return history, nil
}

// Lookup returns the last-recorded stage+reason for a key, or empty values
// if no record exists yet.
func (s *State) Lookup(key string) (Stage, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.history[key]
	if !ok {
		return "", ""
	}
	return rec.Stage, rec.Reason
}

// Record appends a new line to the state file, fsyncs it, and updates the
// in-memory map. Safe for concurrent callers.
func (s *State) Record(key string, stage Stage, reason string) error {
	if key == "" {
		return fmt.Errorf("Record: empty key")
	}
	rec := Record{
		Key:    key,
		Stage:  stage,
		Reason: reason,
		Time:   time.Now().UTC(),
	}

	line, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal record: %w", err)
	}
	line = append(line, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.file.Write(line); err != nil {
		return fmt.Errorf("write state line: %w", err)
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync state file: %w", err)
	}
	s.history[key] = rec
	return nil
}

// Close closes the underlying file.
func (s *State) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}
