package schemamaster

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func testReferenceRegister() *Register {
	return &Register{ReferenceReads: []ReferenceRead{{
		Table:       "oracle_asset",
		CurrentView: "oracle_asset_current",
		AsOf:        "oracle_asset_as_of",
	}}}
}

func TestCheckCalculationSQL(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     string
		wantKind string
	}{
		{
			name: "as_of read with a bind parameter",
			body: `SELECT 1 FROM oracle_asset_as_of(:reference_effective_at) oa WHERE oa.enabled`,
		},
		{
			name: "as_of read with a positional parameter and an explicit UTC cast",
			body: `SELECT 1 FROM oracle_asset_as_of($2::timestamptz) oa`,
		},
		{
			name: "appending a version is not a read",
			body: `INSERT INTO oracle_asset (oracle_id, token_id, enabled) VALUES ($1, $2, true)`,
		},
		{
			name: "wall clock in a source that reads no reference table",
			body: `SELECT EXTRACT(EPOCH FROM (NOW() - otp.timestamp)) FROM onchain_token_price otp`,
		},
		{
			name: "whitespace before the argument list",
			body: `SELECT 1 FROM oracle_asset_as_of (:reference_effective_at) oa`,
		},
		{
			name:     "whitespace before a wall-clock argument list",
			body:     `SELECT 1 FROM oracle_asset_as_of (CURRENT_DATE) oa`,
			wantKind: "wall_clock_effective_at",
		},
		{
			name: "wall clock alongside a reference read",
			body: `SELECT EXTRACT(EPOCH FROM (NOW() - otp.timestamp)) FROM onchain_token_price otp
			WHERE EXISTS (SELECT 1 FROM oracle_asset_as_of(:reference_effective_at) oa)`,
			wantKind: "wall_clock_in_reference_read_sql",
		},
		{
			name: "a comment naming the banned objects",
			body: `-- read oracle_asset_as_of, never the raw oracle_asset or oracle_asset_current
			SELECT 1 FROM oracle_asset_as_of(:reference_effective_at) oa`,
		},
		{
			name: "a Python comment naming the banned objects",
			body: "# FROM oracle_asset_current is banned here\nquery = 'SELECT 1 FROM oracle_asset_as_of(:reference_effective_at)'",
		},
		{
			name:     "the _current view",
			body:     `SELECT 1 FROM oracle_asset_current oa WHERE oa.enabled`,
			wantKind: "current_view_in_calculation_sql",
		},
		{
			name:     "the _current view in a join",
			body:     `SELECT 1 FROM token t JOIN oracle_asset_current oa ON oa.token_id = t.id`,
			wantKind: "current_view_in_calculation_sql",
		},
		{
			name:     "an unpinned read of the raw table",
			body:     `SELECT 1 FROM oracle_asset oa WHERE oa.enabled`,
			wantKind: "unpinned_reference_read",
		},
		{
			name:     "an unpinned join of the raw table",
			body:     `SELECT 1 FROM onchain_token_price otp JOIN oracle_asset oa ON oa.token_id = otp.token_id`,
			wantKind: "unpinned_reference_read",
		},
		{
			name:     "the wall clock as the effective instant",
			body:     `SELECT 1 FROM oracle_asset_as_of(now()::date) oa`,
			wantKind: "wall_clock_effective_at",
		},
		{
			name:     "CURRENT_DATE as the effective instant",
			body:     `SELECT 1 FROM oracle_asset_as_of(CURRENT_DATE) oa`,
			wantKind: "wall_clock_effective_at",
		},
		// Spellings that walk around a matcher knowing only the bare name.
		{
			name:     "a schema-qualified unpinned read",
			body:     `SELECT 1 FROM public.oracle_asset oa WHERE oa.enabled`,
			wantKind: "unpinned_reference_read",
		},
		{
			name:     "a quoted unpinned read",
			body:     `SELECT 1 FROM "oracle_asset" oa WHERE oa.enabled`,
			wantKind: "unpinned_reference_read",
		},
		// A known gap, not desired behaviour: matching a comma would also fire on
		// `TRUNCATE a, b, c`. See unpinnedReads.
		{
			name: "a comma-joined FROM list is a known miss",
			body: `SELECT 1 FROM token t, oracle_asset oa WHERE oa.token_id = t.id`,
		},
		{
			name: "a table list in TRUNCATE is not a read",
			body: `TRUNCATE oracle, token, oracle_asset CASCADE`,
		},
		{
			name:     "an unpinned ONLY read",
			body:     `SELECT 1 FROM ONLY oracle_asset oa WHERE oa.enabled`,
			wantKind: "unpinned_reference_read",
		},
		{
			name:     "an unpinned read sharing a line with a URL literal",
			body:     `SELECT 'https://sky.money' AS src FROM oracle_asset oa WHERE oa.enabled`,
			wantKind: "unpinned_reference_read",
		},
		{
			name:     "an unpinned read after a jsonb operator",
			body:     `SELECT meta #>> '{k}' FROM oracle_asset oa WHERE oa.enabled`,
			wantKind: "unpinned_reference_read",
		},
		{
			name:     "transaction_timestamp as the effective instant",
			body:     `SELECT 1 FROM oracle_asset_as_of(transaction_timestamp()) oa`,
			wantKind: "wall_clock_effective_at",
		},
		{
			name:     "clock_timestamp as the effective instant",
			body:     `SELECT 1 FROM oracle_asset_as_of(clock_timestamp()) oa`,
			wantKind: "wall_clock_effective_at",
		},
		{
			name:     "statement_timestamp as the effective instant",
			body:     `SELECT 1 FROM oracle_asset_as_of(statement_timestamp()) oa`,
			wantKind: "wall_clock_effective_at",
		},
		{
			name:     "CURRENT_TIME as the effective instant",
			body:     `SELECT 1 FROM oracle_asset_as_of(CURRENT_TIME) oa`,
			wantKind: "wall_clock_effective_at",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vs := testReferenceRegister().CheckCalculationSQL([]SQLSource{{Path: "q.py", Body: tc.body}})
			switch {
			case tc.wantKind == "":
				for _, v := range vs {
					t.Errorf("unexpected violation %s: %s", v.Kind, v.Detail)
				}
			case len(vs) == 0:
				t.Fatalf("no violation, want %s", tc.wantKind)
			default:
				if vs[0].Kind != tc.wantKind {
					t.Errorf("violation kind = %q, want %q (%s)", vs[0].Kind, tc.wantKind, vs[0].Detail)
				}
				if vs[0].Table != "q.py" {
					t.Errorf("violation table = %q, want the source path", vs[0].Table)
				}
			}
		})
	}
}

func TestCheckCalculationSQLHonoursTheWallClockExemption(t *testing.T) {
	src := SQLSource{
		Path: "/repo/python/app/adapters/postgres/token_catalog_repository.py",
		Body: `SELECT EXTRACT(EPOCH FROM (NOW() - otp.timestamp)) FROM onchain_token_price otp
		       WHERE EXISTS (SELECT 1 FROM oracle_asset_as_of(:reference_effective_at) oa)`,
	}
	reg := testReferenceRegister()
	if vs := reg.CheckCalculationSQL([]SQLSource{src}); len(vs) != 1 {
		t.Fatalf("got %d violations without an exemption, want 1", len(vs))
	}

	reg.WallClockExempt = []WallClockExempt{{
		Path:   "python/app/adapters/postgres/token_catalog_repository.py",
		Reason: "staleness_seconds",
	}}
	if vs := reg.CheckCalculationSQL([]SQLSource{src}); len(vs) != 0 {
		t.Errorf("got %d violations for an exempt file, want 0: %+v", len(vs), vs)
	}
}

func TestCheckCalculationSQLIgnoresUnconvertedTables(t *testing.T) {
	body := `SELECT 1 FROM morpho_adapter_current ma JOIN security_master sm ON sm.security_id = ma.security_id`
	if vs := testReferenceRegister().CheckCalculationSQL([]SQLSource{{Path: "q.go", Body: body}}); len(vs) != 0 {
		t.Errorf("got %d violations for an unconverted table, want 0: %+v", len(vs), vs)
	}
}

func TestReferenceReadsAreRegistered(t *testing.T) {
	reg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(reg.ReferenceReads) == 0 {
		t.Fatal("no reference_reads entries loaded")
	}
	for _, rr := range reg.ReferenceReads {
		if rr.CurrentView != rr.Table+"_current" {
			t.Errorf("%s: current_view = %q, want %s_current", rr.Table, rr.CurrentView, rr.Table)
		}
		if rr.AsOf != rr.Table+"_as_of" {
			t.Errorf("%s: as_of = %q, want %s_as_of", rr.Table, rr.AsOf, rr.Table)
		}
	}
}

func TestApplicationSQLPinsReferenceReads(t *testing.T) {
	reg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// An empty ReferenceReads makes the whole gate a no-op, so a mistyped JSON key would
	// pass rather than fail.
	if len(reg.ReferenceReads) == 0 {
		t.Fatal("register has no reference_reads — the lint would pass while checking nothing")
	}

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller: cannot locate this file, so the scan roots cannot be resolved")
	}
	serviceRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	sources, err := LoadSQLSources(
		filepath.Join(serviceRoot, "internal", "adapters", "outbound", "postgres"),
		filepath.Join(serviceRoot, "python", "app"),
	)
	if err != nil {
		t.Fatalf("load SQL sources: %v", err)
	}
	if len(sources) == 0 {
		t.Fatal("no sources scanned — did the adapter paths move?")
	}
	// A scan that no longer covers the repositories reading converted tables would report
	// a clean bill for nothing.
	for _, ref := range reg.ReferenceReads {
		scanned := false
		for _, src := range sources {
			if strings.Contains(src.Body, ref.AsOf) {
				scanned = true
				break
			}
		}
		if !scanned {
			t.Errorf("no scanned source reads %s — the lint is not covering %s's readers", ref.AsOf, ref.Table)
		}
	}

	for _, v := range reg.CheckCalculationSQL(sources) {
		t.Errorf("%s: %s — %s", v.Table, v.Kind, v.Detail)
	}
}
