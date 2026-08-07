package db_statement_stats

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const (
	metricCalls    = "db.statements.insert.calls.total"
	metricExecTime = "db.statements.insert.exec_time_seconds.total"
	metricRows     = "db.statements.insert.rows.total"
)

// stubReader returns a scripted reading per tick, so a test can drive the
// delta tracker through a sequence of pg_stat_statements snapshots.
type stubReader struct {
	ticks [][]outbound.StatementStat
	err   error
	calls int
}

func (s *stubReader) InsertStatements(context.Context) ([]outbound.StatementStat, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.calls >= len(s.ticks) {
		return nil, fmt.Errorf("stubReader: no scripted tick %d", s.calls)
	}
	tick := s.ticks[s.calls]
	s.calls++
	return tick, nil
}

// stat builds one pg_stat_statements row. Named fields keep the three
// same-typed counters from being transposed at a call site.
func stat(queryID int64, query string, calls int64, execSeconds float64, rows int64) outbound.StatementStat {
	return outbound.StatementStat{
		QueryID:              queryID,
		Query:                query,
		Calls:                calls,
		TotalExecTimeSeconds: execSeconds,
		Rows:                 rows,
	}
}

// newDeltaReader returns a manual reader with delta temporality, so each Collect
// reports only what the last tick added. Cumulative temporality would make a
// per-tick delta indistinguishable from a running total, which is precisely the
// distinction these tests exist to pin.
func newDeltaReader() *sdkmetric.ManualReader {
	return sdkmetric.NewManualReader(sdkmetric.WithTemporalitySelector(
		func(sdkmetric.InstrumentKind) metricdata.Temporality { return metricdata.DeltaTemporality },
	))
}

// newHarness wires a Service against a stubbed reader and a manual metric
// reader, returning everything a test asserts on.
func newHarness(t *testing.T, cfg ServiceConfig, ticks ...[]outbound.StatementStat) (*Service, *sdkmetric.ManualReader, *testutil.SlogRecorder) {
	t.Helper()

	reader := newDeltaReader()
	telemetry, err := NewTelemetryWithProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}

	recorder := &testutil.SlogRecorder{}
	cfg.Logger = slog.New(recorder)

	service, err := NewService(cfg, &stubReader{ticks: ticks}, telemetry)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return service, reader, recorder
}

// tickMetrics is one tick's emitted deltas, per metric name, per `table`.
type tickMetrics map[string]map[string]float64

func (m tickMetrics) table(name, table string) float64 { return m[name][table] }

// collectTick drains the reader once and returns every counter's data points
// grouped by metric name and `table` attribute. Delta temporality resets all
// instruments on collect, so a tick must be read in a single call — collecting
// per metric name would leave every metric after the first looking empty.
func collectTick(t *testing.T, reader *sdkmetric.ManualReader) tickMetrics {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}

	out := tickMetrics{}
	add := func(name, table string, v float64) {
		if out[name] == nil {
			out[name] = map[string]float64{}
		}
		out[name][table] += v
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch sum := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range sum.DataPoints {
					add(m.Name, tableAttr(t, dp.Attributes), float64(dp.Value))
				}
			case metricdata.Sum[float64]:
				for _, dp := range sum.DataPoints {
					add(m.Name, tableAttr(t, dp.Attributes), dp.Value)
				}
			default:
				t.Fatalf("metric %q has unexpected data type %T", m.Name, m.Data)
			}
		}
	}
	return out
}

// runTick runs one tick and returns the metrics it emitted.
func runTick(t *testing.T, service *Service, reader *sdkmetric.ManualReader) tickMetrics {
	t.Helper()
	if err := service.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	return collectTick(t, reader)
}

func tableAttr(t *testing.T, set attribute.Set) string {
	t.Helper()
	v, ok := set.Value("table")
	if !ok {
		t.Fatalf("metric data point is missing the `table` attribute")
	}
	return v.AsString()
}

// loggedTable reports whether any captured record names table, in its message or
// in one of its attribute values.
func loggedTable(recorder *testutil.SlogRecorder, table string) bool {
	for _, r := range recorder.Records {
		if strings.Contains(r.Message, table) {
			return true
		}
		found := false
		r.Attrs(func(a slog.Attr) bool {
			if strings.Contains(a.Value.String(), table) {
				found = true
				return false
			}
			return true
		})
		if found {
			return true
		}
	}
	return false
}

// TestRunOnce_FirstSightingRecordsBaselineWithoutEmitting is the process-restart
// guard. pg_stat_statements counters are cumulative since the last reset, so a
// freshly started process sees the whole of history on its first tick. Emitting
// that as a delta would re-publish every INSERT the database has ever served,
// once per deploy.
func TestRunOnce_FirstSightingRecordsBaselineWithoutEmitting(t *testing.T) {
	service, reader, _ := newHarness(t, ServiceConfig{},
		[]outbound.StatementStat{stat(1, `INSERT INTO block_states (a) VALUES ($1)`, 1_000_000, 5_000, 1_000_000)},
	)

	got := runTick(t, service, reader)

	for _, name := range []string{metricCalls, metricExecTime, metricRows} {
		if v := got.table(name, "block_states"); v != 0 {
			t.Errorf("%s for block_states = %v on first sighting, want 0 "+
				"(historical totals must be baselined, not emitted)", name, v)
		}
	}
}

// TestRunOnce_EmitsIncrementSinceLastTick covers the steady state: the second
// tick publishes only what happened between the two readings.
func TestRunOnce_EmitsIncrementSinceLastTick(t *testing.T) {
	service, reader, _ := newHarness(t, ServiceConfig{},
		[]outbound.StatementStat{stat(1, `INSERT INTO block_states (a) VALUES ($1)`, 100, 2.5, 100)},
		[]outbound.StatementStat{stat(1, `INSERT INTO block_states (a) VALUES ($1)`, 130, 3.0, 160)},
	)

	runTick(t, service, reader) // baseline tick
	got := runTick(t, service, reader)

	if v := got.table(metricCalls, "block_states"); v != 30 {
		t.Errorf("calls delta = %v, want 30 (130-100)", v)
	}
	if v := got.table(metricExecTime, "block_states"); v != 0.5 {
		t.Errorf("exec time delta = %v, want 0.5 (3.0-2.5)", v)
	}
	if v := got.table(metricRows, "block_states"); v != 60 {
		t.Errorf("rows delta = %v, want 60 (160-100)", v)
	}
}

// TestRunOnce_CounterResetEmitsCurrentValue covers a pg_stat_statements_reset()
// or a Postgres restart: the counters restart from zero, so the current reading
// IS the delta. Subtracting the stale baseline would emit a negative value.
func TestRunOnce_CounterResetEmitsCurrentValue(t *testing.T) {
	service, reader, _ := newHarness(t, ServiceConfig{},
		[]outbound.StatementStat{stat(1, `INSERT INTO block_states (a) VALUES ($1)`, 500, 10, 500)},
		[]outbound.StatementStat{stat(1, `INSERT INTO block_states (a) VALUES ($1)`, 7, 0.25, 7)},
	)

	runTick(t, service, reader) // baseline tick
	got := runTick(t, service, reader)

	if v := got.table(metricCalls, "block_states"); v != 7 {
		t.Errorf("calls delta after reset = %v, want 7 (the post-reset total, never negative)", v)
	}
	if v := got.table(metricExecTime, "block_states"); v != 0.25 {
		t.Errorf("exec time delta after reset = %v, want 0.25", v)
	}
	if v := got.table(metricRows, "block_states"); v != 7 {
		t.Errorf("rows delta after reset = %v, want 7", v)
	}
}

// TestRunOnce_AggregatesQueryIDsIntoOneTable covers the fan-in: the same table is
// written by several statement shapes (different column lists), each with its own
// queryid, and they must land on one series rather than several.
func TestRunOnce_AggregatesQueryIDsIntoOneTable(t *testing.T) {
	service, reader, _ := newHarness(t, ServiceConfig{},
		[]outbound.StatementStat{
			stat(1, `INSERT INTO token (a) VALUES ($1)`, 10, 1, 10),
			stat(2, `INSERT INTO token (a, b) VALUES ($1, $2)`, 20, 2, 20),
		},
		[]outbound.StatementStat{
			stat(1, `INSERT INTO token (a) VALUES ($1)`, 15, 1.5, 15),
			stat(2, `INSERT INTO token (a, b) VALUES ($1, $2)`, 40, 5, 40),
		},
	)

	runTick(t, service, reader) // baseline tick
	got := runTick(t, service, reader)

	calls := got[metricCalls]
	if len(calls) != 1 {
		t.Fatalf("got %d table series %v, want 1 (queryids must aggregate per table)", len(calls), calls)
	}
	if v := calls["token"]; v != 25 {
		t.Errorf("calls delta = %v, want 25 (5 + 20 across two queryids)", v)
	}
	if v := got.table(metricExecTime, "token"); v != 3.5 {
		t.Errorf("exec time delta = %v, want 3.5 (0.5 + 3.0)", v)
	}
}

// TestRunOnce_EvictedQueryIDIsRebaselined covers statement eviction. When
// pg_stat_statements drops an entry under its max-entries limit and the statement
// later runs again, Postgres starts a fresh entry from zero. A retained baseline
// would be meaningless; the reappearance must be treated as a first sighting
// rather than emitting a bogus delta.
func TestRunOnce_EvictedQueryIDIsRebaselined(t *testing.T) {
	const query = `INSERT INTO token (a) VALUES ($1)`
	service, reader, _ := newHarness(t, ServiceConfig{},
		[]outbound.StatementStat{stat(1, query, 100, 10, 100)},
		[]outbound.StatementStat{}, // evicted: the entry is gone
		[]outbound.StatementStat{stat(1, query, 40, 4, 40)},
	)

	runTick(t, service, reader) // baseline tick
	runTick(t, service, reader) // the entry is evicted
	got := runTick(t, service, reader)

	if v := got.table(metricCalls, "token"); v != 0 {
		t.Errorf("calls delta after eviction+reappearance = %v, want 0 "+
			"(a forgotten queryid is baselined afresh)", v)
	}
}

// TestRunOnce_ExtractsTargetTable pins how a normalized statement's target table
// is read. Postgres folds an unquoted identifier to lower case but preserves a
// quoted one, so "Foo" and Foo are different tables and Foo and foo are the same
// one; a parser that ignored that would split or merge series wrongly.
func TestRunOnce_ExtractsTargetTable(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"plain", `INSERT INTO block_states (a) VALUES ($1)`, "block_states"},
		{"schema qualified", `INSERT INTO public.block_states (a) VALUES ($1)`, "block_states"},
		{"quoted", `INSERT INTO "block_states" (a) VALUES ($1)`, "block_states"},
		{"quoted schema and table", `INSERT INTO "public"."block_states" (a) VALUES ($1)`, "block_states"},
		{"unquoted mixed case folds down", `INSERT INTO BlockStates (a) VALUES ($1)`, "blockstates"},
		{"quoted mixed case preserved", `INSERT INTO "BlockStates" (a) VALUES ($1)`, "BlockStates"},
		{"lowercase keyword", `insert into block_states (a) values ($1)`, "block_states"},
		{"leading whitespace and newlines", "\n\t  INSERT\n  INTO\n  block_states (a) VALUES ($1)", "block_states"},
		{"no column list", `INSERT INTO block_states VALUES ($1)`, "block_states"},
		{"aliased target", `INSERT INTO block_states AS bs (a) VALUES ($1)`, "block_states"},
		{"on conflict", `INSERT INTO block_states (a) VALUES ($1) ON CONFLICT DO NOTHING`, "block_states"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			service, reader, _ := newHarness(t, ServiceConfig{},
				[]outbound.StatementStat{stat(1, tc.query, 10, 1, 10)},
				[]outbound.StatementStat{stat(1, tc.query, 12, 1.5, 14)},
			)

			runTick(t, service, reader) // baseline tick
			calls := runTick(t, service, reader)[metricCalls]

			if got, ok := calls[tc.want]; !ok || got != 2 {
				t.Errorf("calls by table = %v, want a %q series with delta 2", calls, tc.want)
			}
		})
	}
}

// TestRunOnce_UnparseableStatementFailsTick guards the silent-hole path: a row the
// reader classified as an INSERT but whose target cannot be read in full is a real
// inconsistency. Attributing it to no table, or to a truncated name, would misreport
// write cost while the tick reported success.
func TestRunOnce_UnparseableStatementFailsTick(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{"no target", `INSERT INTO (a) VALUES ($1)`},
		// Postgres allows non-ASCII letters in an unquoted identifier. A parser that
		// matched only the ASCII prefix would attribute this to a table named "caf".
		{"non-ascii identifier", `INSERT INTO café (a) VALUES ($1)`},
		{"non-ascii schema", `INSERT INTO café.orders (a) VALUES ($1)`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			service, _, _ := newHarness(t, ServiceConfig{},
				[]outbound.StatementStat{stat(1, tc.query, 10, 1, 10)},
			)

			err := service.RunOnce(context.Background())
			if err == nil {
				t.Fatalf("RunOnce succeeded on %q; want an error so the tick fails loudly", tc.query)
			}
			if !strings.Contains(err.Error(), tc.query) {
				t.Errorf("error %q should quote the offending statement so it is fixable", err)
			}
		})
	}
}

// TestRunOnce_ReaderErrorFailsTick covers the missing/unreadable extension. If
// pg_stat_statements is not installed the read errors, and that must fail the
// tick rather than report a successful run that measured nothing.
func TestRunOnce_ReaderErrorFailsTick(t *testing.T) {
	reader := newDeltaReader()
	telemetry, err := NewTelemetryWithProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}
	service, err := NewService(ServiceConfig{}, &stubReader{err: fmt.Errorf("relation \"pg_stat_statements\" does not exist")}, telemetry)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if err := service.RunOnce(context.Background()); err == nil {
		t.Fatal("RunOnce succeeded with an unreadable pg_stat_statements; want an error")
	}
}

// cappedHarness sets up three tables of clearly different cost against a cap of
// two, which is the shared fixture for the two cardinality-cap behaviors.
func cappedHarness(t *testing.T) (*Service, *sdkmetric.ManualReader, *testutil.SlogRecorder) {
	t.Helper()
	return newHarness(t, ServiceConfig{MaxTables: 2},
		[]outbound.StatementStat{
			stat(1, `INSERT INTO cheap (a) VALUES ($1)`, 1, 1, 1),
			stat(2, `INSERT INTO middling (a) VALUES ($1)`, 1, 1, 1),
			stat(3, `INSERT INTO expensive (a) VALUES ($1)`, 1, 1, 1),
		},
		[]outbound.StatementStat{
			stat(1, `INSERT INTO cheap (a) VALUES ($1)`, 2, 1.1, 2),
			stat(2, `INSERT INTO middling (a) VALUES ($1)`, 2, 6, 2),
			stat(3, `INSERT INTO expensive (a) VALUES ($1)`, 2, 100, 2),
		},
	)
}

// TestRunOnce_CardinalityCapKeepsCostliestTables pins which tables survive the cap:
// the series count is bounded, and what is kept is what cost the most write time —
// the quantity the job exists to surface.
func TestRunOnce_CardinalityCapKeepsCostliestTables(t *testing.T) {
	service, reader, _ := cappedHarness(t)

	runTick(t, service, reader) // baseline tick
	calls := runTick(t, service, reader)[metricCalls]

	if len(calls) != 2 {
		t.Fatalf("got %d table series %v, want 2 (the cap)", len(calls), calls)
	}
	if _, ok := calls["cheap"]; ok {
		t.Errorf("series %v retained the cheapest table; the cap must keep the costliest", calls)
	}
	for _, want := range []string{"expensive", "middling"} {
		if _, ok := calls[want]; !ok {
			t.Errorf("series %v is missing %q", calls, want)
		}
	}
}

// TestRunOnce_CardinalityCapNamesDroppedTables pins that the cap is never silent. A
// dropped table's cost simply goes missing from the dashboard, so the logs must say
// which table and why, or the gap is unexplainable after the fact.
func TestRunOnce_CardinalityCapNamesDroppedTables(t *testing.T) {
	service, reader, recorder := cappedHarness(t)

	runTick(t, service, reader) // baseline tick
	runTick(t, service, reader)

	if n := recorder.CountWarn("cardinality"); n == 0 {
		t.Error("no warning logged for the dropped table; a silent cap is not acceptable")
	}
	if !loggedTable(recorder, "cheap") {
		t.Error("the warning must name the dropped table so the gap is explainable")
	}
}

// TestRunOnce_FailedTickDoesNotAdvanceBaseline pins that a failure costs resolution
// but never data. The baseline must only move when a delta was actually published,
// so the tick after a failure measures the whole span since the last good reading.
// Advancing on the failing path would silently drop that window's write cost.
func TestRunOnce_FailedTickDoesNotAdvanceBaseline(t *testing.T) {
	const good = `INSERT INTO block_states (a) VALUES ($1)`
	service, reader, _ := newHarness(t, ServiceConfig{},
		[]outbound.StatementStat{stat(1, good, 100, 10, 100)},
		// Same fingerprint has moved on, but an unparseable statement fails the tick.
		[]outbound.StatementStat{
			stat(1, good, 130, 13, 130),
			stat(2, `INSERT INTO (a) VALUES ($1)`, 5, 1, 5),
		},
		[]outbound.StatementStat{stat(1, good, 150, 15, 150)},
	)

	ctx := context.Background()
	runTick(t, service, reader) // baseline at 100

	if err := service.RunOnce(ctx); err == nil {
		t.Fatal("second RunOnce succeeded on an unparseable INSERT; want an error")
	}
	collectTick(t, reader) // discard anything the failed tick may have emitted

	got := runTick(t, service, reader)
	if v := got.table(metricCalls, "block_states"); v != 50 {
		t.Errorf("calls delta after a failed tick = %v, want 50 (150-100, measured from the last "+
			"published reading; 20 would mean the failed tick advanced the baseline)", v)
	}
}

// TestRunOnce_LowerSubCounterFloorsAtZero covers a partially-lower reading: calls
// rise, so this is not a reset, but exec time and rows come back below the baseline.
// Each sub-counter is floored independently, so neither can publish a negative
// increment onto a monotonic counter.
func TestRunOnce_LowerSubCounterFloorsAtZero(t *testing.T) {
	const query = `INSERT INTO block_states (a) VALUES ($1)`
	service, reader, _ := newHarness(t, ServiceConfig{},
		[]outbound.StatementStat{stat(1, query, 100, 10, 100)},
		[]outbound.StatementStat{stat(1, query, 110, 4, 40)},
	)

	runTick(t, service, reader) // baseline tick
	got := runTick(t, service, reader)

	if v := got.table(metricCalls, "block_states"); v != 10 {
		t.Errorf("calls delta = %v, want 10 (calls rose, so this is not a reset)", v)
	}
	if v := got.table(metricExecTime, "block_states"); v != 0 {
		t.Errorf("exec time delta = %v, want 0 (a lower reading floors, never goes negative)", v)
	}
	if v := got.table(metricRows, "block_states"); v != 0 {
		t.Errorf("rows delta = %v, want 0 (a lower reading floors, never goes negative)", v)
	}
}

// TestNewService_RequiresReader guards the composition root against a nil port.
func TestNewService_RequiresReader(t *testing.T) {
	if _, err := NewService(ServiceConfig{}, nil, nil); err == nil {
		t.Fatal("NewService accepted a nil reader; want an error")
	}
}
