package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
)

// reportLines reads back what a run wrote, so a test asserts the file an
// operator reads rather than the calls that produced it.
func reportLines(t *testing.T, path string) []reportLine {
	t.Helper()

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the report: %v", err)
	}

	var lines []reportLine
	for raw := range strings.SplitSeq(strings.TrimSpace(string(body)), "\n") {
		if raw == "" {
			continue
		}
		var line reportLine
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("decoding report line %q: %v", raw, err)
		}
		lines = append(lines, line)
	}
	return lines
}

// A height needing nothing is the overwhelming majority of an audited range, so
// the report carries only the ones an operator has to act on.
func TestDecisionReport_WritesOneLinePerNonSkipDecision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "holes.jsonl")
	report, err := newDecisionReport(path)
	if err != nil {
		t.Fatalf("newDecisionReport: %v", err)
	}

	decisions := []blockDecision{
		{
			BlockNumber:   25395651,
			State:         stateAt(0, s3key.Block, s3key.Receipts, s3key.Traces),
			ArchivedHash:  forkHash,
			CanonicalHash: canonicalHash,
			Plan:          blockPlan{Action: actionRepublish, Version: 1, DataTypes: ethereumTypes()},
		},
		{
			BlockNumber:   25395652,
			CanonicalHash: canonicalHash,
			Plan:          blockPlan{Action: actionSkip, Version: 0},
		},
		{
			BlockNumber:   25395653,
			ArchivedHash:  canonicalHash,
			CanonicalHash: canonicalHash,
			Plan:          blockPlan{Action: actionFill, Version: 0, DataTypes: []s3key.DataType{s3key.Traces}},
		},
	}
	for _, d := range decisions {
		if err := report.record(d); err != nil {
			t.Fatalf("record(%d): %v", d.BlockNumber, err)
		}
	}
	if err := report.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	want := []reportLine{
		{
			Block:         25395651,
			Action:        actionRepublish,
			Version:       1,
			ArchivedHash:  forkHash,
			CanonicalHash: canonicalHash,
			Missing:       ethereumTypes(),
		},
		{
			Block:         25395653,
			Action:        actionFill,
			Version:       0,
			ArchivedHash:  canonicalHash,
			CanonicalHash: canonicalHash,
			Missing:       []s3key.DataType{s3key.Traces},
		},
	}
	got := reportLines(t, path)
	if len(got) != len(want) {
		t.Fatalf("report lines = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i].Block != want[i].Block || got[i].Action != want[i].Action || got[i].Version != want[i].Version ||
			got[i].ArchivedHash != want[i].ArchivedHash || got[i].CanonicalHash != want[i].CanonicalHash ||
			!slices.Equal(got[i].Missing, want[i].Missing) {
			t.Errorf("report line %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// A dry run writes nothing to the archive, so the report is its whole output:
// every `republish` row is a height whose only archived version is a losing fork.
func TestDecisionReport_ADryRunRecordsTheHoleItFound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "holes.jsonl")

	got := dispatch(t,
		Config{Bucket: "bucket", ChainID: ethereumChainID, DryRun: true, ReportPath: path},
		archivedAt(0, s3key.Block, s3key.Receipts, s3key.Traces),
		archivedObjects(t, planTestBlock, 0, forkHash),
		canonicalBlockData())

	if got.err != nil {
		t.Fatalf("applyDecision() error = %v", got.err)
	}
	lines := reportLines(t, path)
	if len(lines) != 1 {
		t.Fatalf("report lines = %+v, want the one forked height", lines)
	}
	if lines[0].Block != planTestBlock || lines[0].Action != actionRepublish || lines[0].Version != 1 {
		t.Errorf("report line = %+v, want block %d republished at version 1", lines[0], planTestBlock)
	}
	if lines[0].ArchivedHash != forkHash || lines[0].CanonicalHash != canonicalHash {
		t.Errorf("report line hashes = archived %q / canonical %q, want %q / %q",
			lines[0].ArchivedHash, lines[0].CanonicalHash, forkHash, canonicalHash)
	}
}
