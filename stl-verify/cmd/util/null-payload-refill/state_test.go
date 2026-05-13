package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestState_AppendAndReread(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.jsonl")

	first, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := first.Record("k1", StageS3, ""); err != nil {
		t.Fatalf("Record k1: %v", err)
	}
	if err := first.Record("k2", StageSNS, ""); err != nil {
		t.Fatalf("Record k2: %v", err)
	}
	if err := first.Record("k3", StageSkip, "already-healed"); err != nil {
		t.Fatalf("Record k3: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second, err := Load(path)
	if err != nil {
		t.Fatalf("re-Load: %v", err)
	}
	defer second.Close()

	if stage, _ := second.Lookup("k1"); stage != StageS3 {
		t.Errorf("k1 stage = %q, want %q", stage, StageS3)
	}
	if stage, _ := second.Lookup("k2"); stage != StageSNS {
		t.Errorf("k2 stage = %q, want %q", stage, StageSNS)
	}
	stage, reason := second.Lookup("k3")
	if stage != StageSkip {
		t.Errorf("k3 stage = %q, want %q", stage, StageSkip)
	}
	if reason != "already-healed" {
		t.Errorf("k3 reason = %q, want %q", reason, "already-healed")
	}
}

func TestState_MultipleRecordsPerKey_LatestWins(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.jsonl")

	s, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if err := s.Record("k1", StageS3, ""); err != nil {
		t.Fatalf("Record s3: %v", err)
	}
	if err := s.Record("k1", StageSNS, ""); err != nil {
		t.Fatalf("Record sns: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer reloaded.Close()

	stage, _ := reloaded.Lookup("k1")
	if stage != StageSNS {
		t.Errorf("expected latest stage %q, got %q", StageSNS, stage)
	}
}

func TestState_TruncatedTrailingLine_Recovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.jsonl")

	s, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, k := range []string{"k1", "k2", "k3"} {
		if err := s.Record(k, StageSNS, ""); err != nil {
			t.Fatalf("Record %s: %v", k, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append a partial line (no trailing newline, invalid JSON).
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := f.WriteString(`{"key":"k4","stage":"s`); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close partial: %v", err)
	}

	reloaded, err := Load(path)
	if err != nil {
		t.Fatalf("reload after partial: %v", err)
	}
	defer reloaded.Close()

	for _, k := range []string{"k1", "k2", "k3"} {
		if stage, _ := reloaded.Lookup(k); stage != StageSNS {
			t.Errorf("expected %s stage %q, got %q", k, StageSNS, stage)
		}
	}
	if stage, _ := reloaded.Lookup("k4"); stage != "" {
		t.Errorf("partial line for k4 should be dropped, got %q", stage)
	}
}

func TestState_ConcurrentRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.jsonl")

	s, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			if err := s.Record(fmt.Sprintf("k%d", i), StageSNS, ""); err != nil {
				t.Errorf("Record k%d: %v", i, err)
			}
		}()
	}
	wg.Wait()
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reloaded, err := Load(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	defer reloaded.Close()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%d", i)
		if stage, _ := reloaded.Lookup(key); stage != StageSNS {
			t.Errorf("missing record for %s", key)
		}
	}
}

func TestState_RecordEmptyKeyErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.jsonl")
	s, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer s.Close()

	if err := s.Record("", StageSNS, ""); err == nil {
		t.Error("expected error for empty key")
	}
}
