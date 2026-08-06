// Package db_statement_stats exports per-table INSERT write cost from the
// application database's own statement statistics.
//
// Postgres already measures what every INSERT costs, but only as the cumulative
// counters described on outbound.StatementStatsReader — readable by hand, invisible
// to Grafana. This service reads them once per tick, turns successive readings into
// deltas, aggregates them per target table, and publishes them as OTel counters so
// the measurement becomes a permanent time series instead of an ad-hoc query.
package db_statement_stats

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// defaultMaxTables bounds how many distinct `table` series one tick may publish.
// The target schema has far fewer tables than this, so the cap is a guard against
// an unexpected source of table names (a partition or temp-table naming scheme
// leaking into statement text) turning one metric into a cardinality incident.
const defaultMaxTables = 200

// insertTarget matches the target table of a normalized INSERT. Schema and table
// are each either a quoted identifier or a bare one, captured separately because
// Postgres folds a bare identifier to lower case and preserves a quoted one.
//
// The trailing delimiter class is what makes a bare identifier match the WHOLE
// name: Postgres allows non-ASCII letters in an unquoted identifier, and without it
// `INSERT INTO café` would match the prefix `caf` and silently attribute that
// table's cost to a name that does not exist. Requiring a delimiter makes the match
// fail instead, which surfaces as a failed tick naming the statement.
var insertTarget = regexp.MustCompile(
	`(?i)^\s*INSERT\s+INTO\s+(?:(?:"([^"]+)"|([a-z_][a-z0-9_$]*))\s*\.\s*)?(?:"([^"]+)"|([a-z_][a-z0-9_$]*))(?:[\s(;]|$)`,
)

// StatementDelta is one unit of work observed between two readings.
type StatementDelta struct {
	Calls           int64
	ExecTimeSeconds float64
	Rows            int64
}

func (d *StatementDelta) add(o StatementDelta) {
	d.Calls += o.Calls
	d.ExecTimeSeconds += o.ExecTimeSeconds
	d.Rows += o.Rows
}

// ServiceConfig configures the exporter.
type ServiceConfig struct {
	// MaxTables caps the distinct `table` series per tick; zero means
	// defaultMaxTables.
	MaxTables int
	Logger    *slog.Logger
}

// Service publishes per-table INSERT cost deltas once per RunOnce.
type Service struct {
	reader    outbound.StatementStatsReader
	telemetry *Telemetry
	logger    *slog.Logger
	maxTables int

	// mu serializes the whole of RunOnce, not just access to baseline. Ticks are
	// expected to be serial, but two overlapping ticks would interleave
	// read -> diff -> publish and each measure part of the other's window, so the
	// ordering is locked rather than trusted to the scheduler.
	mu sync.Mutex
	// baseline is the previous reading per statement fingerprint. It is in-process
	// only: a restart deliberately starts from an empty map, so every fingerprint's
	// first sighting becomes a baseline rather than a delta.
	baseline map[int64]StatementDelta
}

// NewService creates a Service. reader is required; logger defaults to
// slog.Default(); telemetry may be nil (its metrics become no-ops).
func NewService(cfg ServiceConfig, reader outbound.StatementStatsReader, telemetry *Telemetry) (*Service, error) {
	if reader == nil {
		return nil, fmt.Errorf("statement stats reader is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	maxTables := cfg.MaxTables
	if maxTables <= 0 {
		maxTables = defaultMaxTables
	}
	return &Service{
		reader:    reader,
		telemetry: telemetry,
		logger:    logger.With("component", "db-statement-stats"),
		maxTables: maxTables,
		baseline:  map[int64]StatementDelta{},
	}, nil
}

// RunOnce reads the current statement counters and publishes one tick's per-table
// deltas.
//
// It is idempotent in the sense Temporal needs: the baseline advances with the
// reading, so a retried tick re-reads near-identical counters and publishes a
// near-zero delta rather than double-counting the window.
//
// A tick that fails leaves the baseline untouched, so the next successful tick
// measures from the last reading that was actually published — a failure costs
// resolution, never data.
func (s *Service) RunOnce(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats, err := s.reader.InsertStatements(ctx)
	if err != nil {
		return fmt.Errorf("reading INSERT statement stats: %w", err)
	}

	// Parse every row before touching the baseline: a half-advanced tracker would
	// silently lose the un-advanced statements' next delta.
	targets, err := resolveTargets(stats)
	if err != nil {
		return err
	}

	byTable := s.advanceBaseline(stats, targets)
	s.publish(ctx, byTable)
	return nil
}

// resolveTargets maps each reading to its target table, failing the tick if any
// statement's target cannot be read. The reader only returns INSERTs, so an
// unreadable target is a real inconsistency; attributing it to no table would drop
// that table's write cost while the tick still reported success.
func resolveTargets(stats []outbound.StatementStat) (map[int64]string, error) {
	targets := make(map[int64]string, len(stats))
	for _, st := range stats {
		table, err := targetTable(st.Query)
		if err != nil {
			return nil, fmt.Errorf("queryid %d: %w", st.QueryID, err)
		}
		targets[st.QueryID] = table
	}
	return targets, nil
}

// targetTable extracts the table an INSERT writes to, normalized the way Postgres
// resolves the identifier: a bare name folds to lower case, a quoted name is kept
// verbatim. Any schema qualifier is discarded.
func targetTable(query string) (string, error) {
	m := insertTarget.FindStringSubmatch(query)
	if m == nil {
		return "", fmt.Errorf("cannot read INSERT target table from %q", query)
	}
	if quoted := m[3]; quoted != "" {
		return quoted, nil
	}
	return strings.ToLower(m[4]), nil
}

// advanceBaseline turns the reading into per-table deltas and replaces the stored
// baseline with it. A fingerprint absent from the reading is forgotten: the
// collector evicted it, and a re-created entry starts from zero, so a retained
// baseline would only manufacture a bogus delta.
//
// Caller holds s.mu.
func (s *Service) advanceBaseline(stats []outbound.StatementStat, targets map[int64]string) map[string]StatementDelta {
	next := make(map[int64]StatementDelta, len(stats))
	byTable := map[string]StatementDelta{}
	for _, st := range stats {
		current := StatementDelta{Calls: st.Calls, ExecTimeSeconds: st.TotalExecTimeSeconds, Rows: st.Rows}
		prev, seen := s.baseline[st.QueryID]
		next[st.QueryID] = current

		delta := deltaSince(prev, current, seen)
		table := targets[st.QueryID]
		entry := byTable[table]
		entry.add(delta)
		byTable[table] = entry
	}
	s.baseline = next
	return byTable
}

// deltaSince computes the increment between two readings of one fingerprint. It is
// where this package answers the two properties documented on
// outbound.StatementStatsReader.
//
// A first sighting yields zero, not the current total, so a process restart cannot
// re-emit history.
//
// A reading below its baseline means the entry restarted, so the current value IS
// the increment — never the negative difference. Calls is the reset signal because
// it only ever rises within an entry's life; the other two are floored independently
// so a partially-lower reading cannot slip a negative through either.
func deltaSince(prev, current StatementDelta, seen bool) StatementDelta {
	if !seen {
		return StatementDelta{}
	}
	if current.Calls < prev.Calls {
		return current
	}
	return StatementDelta{
		Calls:           current.Calls - prev.Calls,
		ExecTimeSeconds: max(current.ExecTimeSeconds-prev.ExecTimeSeconds, 0),
		Rows:            max(current.Rows-prev.Rows, 0),
	}
}

// publish records the tick's deltas, dropping the cheapest tables if the reading
// exceeds the cardinality cap.
func (s *Service) publish(ctx context.Context, byTable map[string]StatementDelta) {
	for _, table := range s.tablesWithinCap(byTable) {
		s.telemetry.RecordInsertDelta(ctx, table, byTable[table])
	}
}

// tablesWithinCap returns the tables to publish, at most maxTables of them,
// keeping those that cost the most write time this tick — the quantity the job
// exists to surface. Anything dropped is named in a warning: a silently capped
// metric would show a table's cost simply missing, with nothing to explain it.
func (s *Service) tablesWithinCap(byTable map[string]StatementDelta) []string {
	tables := make([]string, 0, len(byTable))
	for table := range byTable {
		tables = append(tables, table)
	}
	if len(tables) <= s.maxTables {
		slices.Sort(tables)
		return tables
	}

	// Costliest first, table name breaking ties so the choice is deterministic.
	slices.SortFunc(tables, func(a, b string) int {
		if c := cmp.Compare(byTable[b].ExecTimeSeconds, byTable[a].ExecTimeSeconds); c != 0 {
			return c
		}
		return cmp.Compare(a, b)
	})

	kept, dropped := tables[:s.maxTables], tables[s.maxTables:]
	s.logger.Warn("INSERT statement stats exceeded the table cardinality cap; dropping the cheapest tables",
		"cap", s.maxTables, "tables", len(tables), "dropped", dropped)
	return kept
}
