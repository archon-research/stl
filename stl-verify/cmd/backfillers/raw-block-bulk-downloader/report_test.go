package main

import (
	"encoding/json"
	"errors"
	"maps"
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
			DataTypes:     ethereumTypes(),
		},
		{
			Block:         25395653,
			Action:        actionFill,
			Version:       0,
			ArchivedHash:  canonicalHash,
			CanonicalHash: canonicalHash,
			DataTypes:     []s3key.DataType{s3key.Traces},
		},
	}
	got := reportLines(t, path)
	if len(got) != len(want) {
		t.Fatalf("report lines = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i].Block != want[i].Block || got[i].Action != want[i].Action || got[i].Version != want[i].Version ||
			got[i].ArchivedHash != want[i].ArchivedHash || got[i].CanonicalHash != want[i].CanonicalHash ||
			!slices.Equal(got[i].DataTypes, want[i].DataTypes) {
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

// The keys are the operator-facing half of the report — what the runbook
// documents and what a jq filter over a million-height audit selects on — so a
// rename is a decision, not a refactor.
func TestDecisionReport_NamesTheFieldsTheRunbookDocuments(t *testing.T) {
	tests := []struct {
		name  string
		write func(*decisionReport) error
		want  []string
	}{
		{
			name: "a height whose archived version is a losing fork",
			write: func(r *decisionReport) error {
				return r.record(blockDecision{
					BlockNumber:   planTestBlock,
					ArchivedHash:  forkHash,
					CanonicalHash: canonicalHash,
					Plan:          blockPlan{Action: actionRepublish, Version: 1, DataTypes: ethereumTypes()},
				})
			},
			want: []string{"block", "action", "version", "archivedHash", "canonicalHash", "dataTypes"},
		},
		{
			name: "a height that reached no decision at all",
			write: func(r *decisionReport) error {
				return r.recordError(planTestBlock, errors.New("eth_getBlockByNumber: block not found"))
			},
			want: []string{"block", "action", "version", "archivedHash", "canonicalHash", "error"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "holes.jsonl")
			report, err := newDecisionReport(path)
			if err != nil {
				t.Fatalf("newDecisionReport: %v", err)
			}

			if err := tc.write(report); err != nil {
				t.Fatalf("writing the row: %v", err)
			}
			if err := report.close(); err != nil {
				t.Fatalf("close: %v", err)
			}

			got := slices.Sorted(maps.Keys(decodedFields(t, path)))
			slices.Sort(tc.want)
			if !slices.Equal(got, tc.want) {
				t.Errorf("report fields = %v, want %v", got, tc.want)
			}
		})
	}
}

// A height the run could not plan or fetch is something an operator has to act
// on too. Left out of the report it reads as a height that needed nothing,
// which is the one thing the file is supposed to rule out.
func TestDecisionReport_RecordsAHeightThatReachedNoDecision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "holes.jsonl")
	report, err := newDecisionReport(path)
	if err != nil {
		t.Fatalf("newDecisionReport: %v", err)
	}

	if err := report.recordError(planTestBlock, errors.New("eth_getBlockByNumber: block not found")); err != nil {
		t.Fatalf("recordError: %v", err)
	}
	if err := report.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	lines := reportLines(t, path)
	if len(lines) != 1 {
		t.Fatalf("report lines = %+v, want the one failed height", lines)
	}
	if lines[0].Block != planTestBlock || lines[0].Action != actionError {
		t.Errorf("report line = %+v, want block %d recorded as an error", lines[0], planTestBlock)
	}
	if !strings.Contains(lines[0].Error, "block not found") {
		t.Errorf("report line error = %q, want the failure that stopped the height", lines[0].Error)
	}
}

// decodedFields reads the first report line as the JSON it is, rather than
// through reportLine, so the tags are asserted and not assumed.
func decodedFields(t *testing.T, path string) map[string]any {
	t.Helper()

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the report: %v", err)
	}
	fields := map[string]any{}
	first, _, _ := strings.Cut(strings.TrimSpace(string(body)), "\n")
	if err := json.Unmarshal([]byte(first), &fields); err != nil {
		t.Fatalf("decoding report line %q: %v", first, err)
	}
	return fields
}

// --report is not a dry-run flag: a run that writes the archive records the same
// rows, so the file is the action list whichever way the run was started.
func TestDecisionReport_ARealRunRecordsTheSameRowAsADryRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "holes.jsonl")

	got := dispatch(t,
		Config{Bucket: "bucket", ChainID: ethereumChainID, ReportPath: path},
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
	if !slices.Equal(lines[0].DataTypes, ethereumTypes()) {
		t.Errorf("report line dataTypes = %v, want %v", lines[0].DataTypes, ethereumTypes())
	}
}

// A line the report could not write is a height an operator never sees, so it
// surfaces as an error naming the file rather than as a dropped row.
func TestDecisionReport_AWriteFailureNamesTheReportAndTheCause(t *testing.T) {
	report := unbufferedReport("holes.jsonl", failingSink{err: errors.New("no space left on device")})

	err := report.record(blockDecision{
		BlockNumber: planTestBlock,
		Plan:        blockPlan{Action: actionRepublish, Version: 1, DataTypes: ethereumTypes()},
	})

	if err == nil {
		t.Fatal("record() succeeded against a sink that refuses every write")
	}
	for _, want := range []string{"holes.jsonl", "no space left on device"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to mention %q", err, want)
		}
	}
}
