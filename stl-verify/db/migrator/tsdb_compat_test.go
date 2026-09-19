package migrator

import (
	"os"
	"strings"
	"testing"
)

func TestStripTimescaleDBSyntax(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "WITH clause with only tsdb params is removed entirely",
			sql: `CREATE TABLE foo (
    id INT,
    ts TIMESTAMPTZ
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ts',
    tsdb.chunk_interval = '30 days',
    tsdb.columnstore = false
);`,
			want: `CREATE TABLE foo (
    id INT,
    ts TIMESTAMPTZ
);`,
		},
		{
			name: "WITH clause with mixed tsdb and non-tsdb params keeps non-tsdb",
			sql: `CREATE TABLE foo (
    id INT
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'ts',
    fillfactor = 90
);`,
			want: `CREATE TABLE foo (
    id INT
) WITH (
    fillfactor = 90
);`,
		},
		{
			name: "inline WITH clause with only tsdb params",
			sql: `) WITH ( tsdb.hypertable, tsdb.partition_column = 'snapshot_time', tsdb.chunk_interval = '1 day' );`,
			want: `);`,
		},
		{
			name: "ALTER TABLE SET with timescaledb params removed entirely",
			sql: `ALTER TABLE onchain_token_price SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'oracle_id, token_id',
    timescaledb.compress_orderby = 'block_number DESC, block_version DESC'
);`,
			want: "",
		},
		{
			name: "ALTER TABLE SET single line with timescaledb",
			sql: `ALTER TABLE borrower SET (timescaledb.columnstore = false);`,
			want: "",
		},
		{
			name: "ALTER TABLE SET with timescaledb.compress bare param",
			sql: `ALTER TABLE foo SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pool_id',
    timescaledb.compress_orderby = 'block_number DESC'
);`,
			want: "",
		},
		{
			name: "SET LOCAL timescaledb removed",
			sql: "SET LOCAL timescaledb.enable_tiered_reads = 'on';\n",
			want: "",
		},
		{
			name: "SET timescaledb without LOCAL removed",
			sql: "SET timescaledb.max_tuples_decompressed_per_dml_transaction = 500000;\n",
			want: "",
		},
		{
			name: "function SET timescaledb removed",
			sql: `CREATE OR REPLACE FUNCTION foo() RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    SET timescaledb.enable_tiered_reads = 'on'
    AS $fn$
BEGIN
    RETURN 1;
END;
$fn$;`,
			want: `CREATE OR REPLACE FUNCTION foo() RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    AS $fn$
BEGIN
    RETURN 1;
END;
$fn$;`,
		},
		{
			name: "function SET timescaledb with AS on same line",
			sql: `CREATE OR REPLACE FUNCTION foo() RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
BEGIN
    RETURN 1;
END;
$fn$;`,
			want: `CREATE OR REPLACE FUNCTION foo() RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT AS $fn$
BEGIN
    RETURN 1;
END;
$fn$;`,
		},
		{
			name: "tsdb reference inside single-quoted string is preserved",
			sql: `SELECT 'tsdb.hypertable is a param' AS note;`,
			want: `SELECT 'tsdb.hypertable is a param' AS note;`,
		},
		{
			name: "tsdb reference inside dollar-quoted string is preserved",
			sql: `DO $$
BEGIN
    RAISE NOTICE 'tsdb.hypertable setting is active';
    SET LOCAL timescaledb.enable_tiered_reads = 'on';
END $$;`,
			want: `DO $$
BEGIN
    RAISE NOTICE 'tsdb.hypertable setting is active';
    SET LOCAL timescaledb.enable_tiered_reads = 'on';
END $$;`,
		},
		{
			name: "tsdb reference inside comment is preserved",
			sql: `-- This sets tsdb.hypertable
SELECT 1;`,
			want: `-- This sets tsdb.hypertable
SELECT 1;`,
		},
		{
			name: "multiple SET timescaledb in a function preserves non-tsdb SET",
			sql: `CREATE OR REPLACE FUNCTION bar() RETURNS TRIGGER
    LANGUAGE plpgsql
    SET plan_cache_mode = 'force_custom_plan'
    SET timescaledb.enable_tiered_reads = 'on'
    AS $fn$
BEGIN
    RETURN NEW;
END;
$fn$;`,
			want: `CREATE OR REPLACE FUNCTION bar() RETURNS TRIGGER
    LANGUAGE plpgsql
    SET plan_cache_mode = 'force_custom_plan'
    AS $fn$
BEGIN
    RETURN NEW;
END;
$fn$;`,
		},
		{
			name: "multiline WITH clause with tsdb params",
			sql: `CREATE TABLE pool_state (
    pool_id BYTEA NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,
    block_number BIGINT NOT NULL,
    PRIMARY KEY (pool_id, block_timestamp, block_number)
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'block_timestamp',
    tsdb.chunk_interval = '30 days',
    tsdb.columnstore = false
);`,
			want: `CREATE TABLE pool_state (
    pool_id BYTEA NOT NULL,
    block_timestamp TIMESTAMPTZ NOT NULL,
    block_number BIGINT NOT NULL,
    PRIMARY KEY (pool_id, block_timestamp, block_number)
);`,
		},
		{
			name: "plain SQL without tsdb is untouched",
			sql: `CREATE TABLE plain (
    id SERIAL PRIMARY KEY,
    name TEXT
);

INSERT INTO plain (name) VALUES ('test');`,
			want: `CREATE TABLE plain (
    id SERIAL PRIMARY KEY,
    name TEXT
);

INSERT INTO plain (name) VALUES ('test');`,
		},
		{
			name: "SET LOCAL timescaledb.enable_tiered_reads off variant",
			sql: "SET LOCAL timescaledb.enable_tiered_reads = 'off';\n",
			want: "",
		},
		{
			name: "ALTER TABLE SET with timescaledb.columnstore = true and segmentby",
			sql: `ALTER TABLE allocation_position SET (
    timescaledb.columnstore = true,
    timescaledb.segmentby = 'chain_id, token_id, proxy_address',
    timescaledb.orderby   = 'block_number DESC, block_version DESC, processing_version DESC'
);`,
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripTimescaleDBSyntax(tt.sql)
			if normalizeWhitespace(got) != normalizeWhitespace(tt.want) {
				t.Errorf("stripTimescaleDBSyntax():\n--- got ---\n%s\n--- want ---\n%s", got, tt.want)
			}
		})
	}
}

func TestStripTimescaleDBSyntax_RealPatterns(t *testing.T) {
	t.Run("CREATE TABLE with tsdb inline then ALTER TABLE SET timescaledb", func(t *testing.T) {
		sql := `) WITH ( tsdb.hypertable, tsdb.partition_column = 'snapshot_time', tsdb.chunk_interval = '1 day' );

CREATE INDEX IF NOT EXISTS idx_snap ON foo (ts DESC);

ALTER TABLE foo SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'pool_id',
    timescaledb.compress_orderby = 'block_number DESC'
);

INSERT INTO migrations (filename) VALUES ('test.sql');`

		got := stripTimescaleDBSyntax(sql)

		if strings.Contains(got, "tsdb.") {
			t.Error("output still contains tsdb. references")
		}
		if strings.Contains(got, "timescaledb.") {
			t.Error("output still contains timescaledb. references")
		}
		if !strings.Contains(got, "CREATE INDEX") {
			t.Error("CREATE INDEX was incorrectly removed")
		}
		if !strings.Contains(got, "INSERT INTO migrations") {
			t.Error("INSERT INTO migrations was incorrectly removed")
		}
	})

	t.Run("function with multiple SET preserves dollar-quoted body", func(t *testing.T) {
		sql := `CREATE OR REPLACE FUNCTION materialize_anchorage_custody(p_build_id integer DEFAULT 0)
    RETURNS bigint
    LANGUAGE plpgsql
    SET search_path FROM CURRENT
    SET timescaledb.enable_tiered_reads = 'on' AS $fn$
DECLARE
    v_bad text;
BEGIN
    SET LOCAL timescaledb.enable_tiered_reads = 'on';
    RETURN 0;
END;
$fn$;`

		got := stripTimescaleDBSyntax(sql)

		if strings.Contains(got, "\n    SET timescaledb.") {
			t.Error("function-level SET timescaledb was not removed")
		}
		// The SET LOCAL inside the dollar-quoted body must be preserved.
		if !strings.Contains(got, "SET LOCAL timescaledb.enable_tiered_reads") {
			t.Error("SET LOCAL inside dollar-quoted body was incorrectly removed")
		}
		if !strings.Contains(got, "SET search_path FROM CURRENT") {
			t.Error("non-tsdb SET was incorrectly removed")
		}
	})
}

func TestSplitDollarQuotedRegions(t *testing.T) {
	t.Run("dollar-quoted string is non-code", func(t *testing.T) {
		segs := splitDollarQuotedRegions("DO $fn$ SET timescaledb.x = 1; $fn$;")
		found := false
		for _, s := range segs {
			if !s.isCode && strings.Contains(s.text, "timescaledb.x") {
				found = true
			}
		}
		if !found {
			t.Error("timescaledb reference inside dollar-quoted string was not marked non-code")
		}
	})

	t.Run("code outside dollar quotes is code", func(t *testing.T) {
		segs := splitDollarQuotedRegions("SELECT 1; $$ body $$ SELECT 2;")
		if len(segs) != 3 {
			t.Fatalf("expected 3 segments, got %d", len(segs))
		}
		if !segs[0].isCode || segs[1].isCode || !segs[2].isCode {
			t.Error("code/literal classification is wrong")
		}
	})
}

func TestStripTimescaleDBSyntax_AllMigrations(t *testing.T) {
	entries, err := os.ReadDir("../../db/migrations")
	if err != nil {
		// Fall back to the path from stl-verify/ root.
		entries, err = os.ReadDir("db/migrations")
		if err != nil {
			t.Skip("migration directory not accessible")
		}
	}

	dir := "../../db/migrations"
	if _, err := os.ReadDir(dir); err != nil {
		dir = "db/migrations"
	}

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		t.Run(e.Name(), func(t *testing.T) {
			content, err := os.ReadFile(dir + "/" + e.Name())
			if err != nil {
				t.Fatal(err)
			}
			sql := string(content)
			stripped := stripTimescaleDBSyntax(sql)

			// Check code segments for remaining tsdb/timescaledb references,
			// skipping content inside single-quoted strings and comments.
			segments := splitDollarQuotedRegions(stripped)
			for _, seg := range segments {
				if !seg.isCode {
					continue
				}
				codeOnly := stripQuotedAndCommented(seg.text)
				lower := strings.ToLower(codeOnly)
				for _, prefix := range []string{"tsdb.", "timescaledb."} {
					if idx := strings.Index(lower, prefix); idx >= 0 {
						// Find the line for context.
						lineStart := strings.LastIndex(codeOnly[:idx], "\n") + 1
						lineEnd := strings.Index(codeOnly[idx:], "\n")
						if lineEnd < 0 {
							lineEnd = len(codeOnly)
						} else {
							lineEnd += idx
						}
						t.Errorf("remaining %s reference in code: %s", prefix, strings.TrimSpace(codeOnly[lineStart:lineEnd]))
					}
				}
			}
		})
	}
}

// stripQuotedAndCommented replaces the content of single-quoted strings and
// line comments with spaces, so reference checks ignore them.
func stripQuotedAndCommented(s string) string {
	var out strings.Builder
	i := 0
	for i < len(s) {
		switch {
		case s[i] == '\'' :
			out.WriteByte('\'')
			i++
			for i < len(s) {
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						out.WriteString("  ")
						i += 2
						continue
					}
					out.WriteByte('\'')
					i++
					break
				}
				out.WriteByte(' ')
				i++
			}
		case s[i] == '-' && i+1 < len(s) && s[i+1] == '-':
			for i < len(s) && s[i] != '\n' {
				out.WriteByte(' ')
				i++
			}
		default:
			out.WriteByte(s[i])
			i++
		}
	}
	return out.String()
}

// normalizeWhitespace collapses runs of whitespace (including empty lines) for
// comparison, since the exact whitespace after removal is not semantically important.
func normalizeWhitespace(s string) string {
	s = strings.TrimSpace(s)
	lines := strings.Split(s, "\n")
	var kept []string
	for _, l := range lines {
		trimmed := strings.TrimRight(l, " \t")
		if trimmed != "" {
			kept = append(kept, trimmed)
		}
	}
	return strings.Join(kept, "\n")
}
