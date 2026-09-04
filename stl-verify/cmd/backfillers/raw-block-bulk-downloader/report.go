package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
)

// reportLine is one height's decision as the report carries it. A `republish`
// row is a hole: the archive's top version at that height is a losing fork.
type reportLine struct {
	Block         int64            `json:"block"`
	Action        blockAction      `json:"action"`
	Version       int              `json:"version"`
	ArchivedHash  string           `json:"archivedHash"`
	CanonicalHash string           `json:"canonicalHash"`
	Missing       []s3key.DataType `json:"missing"`
}

// decisionReport is the machine-readable half of a run: one JSON object per
// line for every height the run would touch, so a dry run over a million
// heights leaves a hole list rather than a log to grep. A nil report is the
// default — no --report, nothing written.
type decisionReport struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	enc    *json.Encoder
}

// newDecisionReport opens the report file, truncating an earlier run's. An
// empty path means no report.
func newDecisionReport(path string) (*decisionReport, error) {
	if path == "" {
		return nil, nil
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("creating the report %s: %w", path, err)
	}
	writer := bufio.NewWriter(file)
	return &decisionReport{file: file, writer: writer, enc: json.NewEncoder(writer)}, nil
}

// record writes one decision. Skips are left out: they are the overwhelming
// majority of an audited range and the whole point of the report is what needs
// acting on.
func (r *decisionReport) record(d blockDecision) error {
	if r == nil || d.Plan.Action == actionSkip {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	line := reportLine{
		Block:         d.BlockNumber,
		Action:        d.Plan.Action,
		Version:       d.Plan.Version,
		ArchivedHash:  d.ArchivedHash,
		CanonicalHash: d.CanonicalHash,
		Missing:       d.Plan.DataTypes,
	}
	if err := r.enc.Encode(line); err != nil {
		return fmt.Errorf("writing block %d to the report: %w", d.BlockNumber, err)
	}
	return nil
}

// close flushes the buffered lines. Nothing reads the report until the run is
// over, so a failure here is the whole report failing.
func (r *decisionReport) close() error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return errors.Join(r.writer.Flush(), r.file.Close())
}
