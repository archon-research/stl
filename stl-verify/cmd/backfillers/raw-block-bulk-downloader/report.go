package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
)

// reportLine is one height's decision as the report carries it. A `republish`
// row is a hole: the archive's top version at that height is a losing fork.
// DataTypes is what the plan writes, not what the height lacks: only a `fill`
// row's is the absent set.
type reportLine struct {
	Block         int64            `json:"block"`
	Action        blockAction      `json:"action"`
	Version       int              `json:"version"`
	ArchivedHash  string           `json:"archivedHash"`
	CanonicalHash string           `json:"canonicalHash"`
	DataTypes     []s3key.DataType `json:"dataTypes"`
}

// decisionReport is the machine-readable half of a run: one JSON object per
// line for every height the run would touch, so a dry run over a million
// heights leaves a hole list rather than a log to grep. A nil report is the
// default — no --report, nothing written.
type decisionReport struct {
	mu     sync.Mutex
	path   string
	sink   io.WriteCloser
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
	return reportTo(path, file), nil
}

// reportTo buffers the lines: a first pass over a million untouched heights
// records every one of them, and a write syscall each would outweigh the work.
func reportTo(path string, sink io.WriteCloser) *decisionReport {
	writer := bufio.NewWriter(sink)
	return &decisionReport{path: path, sink: sink, writer: writer, enc: json.NewEncoder(writer)}
}

// record writes one decision. Skips are left out: they are the overwhelming
// majority of an audited range and the whole point of the report is what needs
// acting on.
func (r *decisionReport) record(d blockDecision) error {
	if r == nil || d.Plan.Action == actionSkip {
		return nil
	}

	return r.write(reportLine{
		Block:         d.BlockNumber,
		Action:        d.Plan.Action,
		Version:       d.Plan.Version,
		ArchivedHash:  d.ArchivedHash,
		CanonicalHash: d.CanonicalHash,
		DataTypes:     d.Plan.DataTypes,
	})
}

func (r *decisionReport) write(line reportLine) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.enc.Encode(line); err != nil {
		return fmt.Errorf("writing block %d to the report %s: %w", line.Block, r.path, err)
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

	return errors.Join(r.writer.Flush(), r.sink.Close())
}
