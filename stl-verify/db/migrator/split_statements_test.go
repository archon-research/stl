package migrator

import (
	"strings"
	"testing"
)

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		wantStmt []string // exact (trimmed) match per statement
	}{
		{
			name:     "single statement",
			content:  "SELECT 1;",
			wantStmt: []string{"SELECT 1;"},
		},
		{
			name: "two simple statements",
			content: `
ALTER TABLE t SET (x = 1);
ALTER TABLE t SET (y = 2);
`,
			wantStmt: []string{
				"ALTER TABLE t SET (x = 1);",
				"ALTER TABLE t SET (y = 2);",
			},
		},
		{
			name: "comment-only lines are dropped between statements",
			content: `
-- header
SELECT 1;
-- intermediate
SELECT 2;
`,
			wantStmt: []string{"SELECT 1;", "SELECT 2;"},
		},
		{
			name: "DO block with inner semicolons stays intact",
			content: `
DO $$
DECLARE
    v_job_id INT;
BEGIN
    SELECT 1 INTO v_job_id;
    PERFORM alter_job(v_job_id, scheduled => false);
END $$;
SELECT 1;
`,
			wantStmt: []string{
				`DO $$
DECLARE
    v_job_id INT;
BEGIN
    SELECT 1 INTO v_job_id;
    PERFORM alter_job(v_job_id, scheduled => false);
END $$;`,
				"SELECT 1;",
			},
		},
		{
			name: "comments inside dollar-quoted body are preserved",
			content: `
DO $$
BEGIN
    -- this comment must survive
    PERFORM 1;
END $$;
`,
			wantStmt: []string{
				`DO $$
BEGIN
    -- this comment must survive
    PERFORM 1;
END $$;`,
			},
		},
		{
			name: "tagged dollar quote (different tag inside is not treated as close)",
			content: `
DO $outer$
DECLARE
    v TEXT := $inner$has;semicolons;inside$inner$;
BEGIN
    PERFORM 1;
END $outer$;
SELECT 2;
`,
			wantStmt: []string{
				`DO $outer$
DECLARE
    v TEXT := $inner$has;semicolons;inside$inner$;
BEGIN
    PERFORM 1;
END $outer$;`,
				"SELECT 2;",
			},
		},
		{
			name: "multiple dollar-quoted DO blocks",
			content: `
DO $$ BEGIN PERFORM 1; END $$;
DO $$ BEGIN PERFORM 2; END $$;
`,
			wantStmt: []string{
				"DO $$ BEGIN PERFORM 1; END $$;",
				"DO $$ BEGIN PERFORM 2; END $$;",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitStatements(tt.content)
			if len(got) != len(tt.wantStmt) {
				t.Fatalf("got %d statements, want %d\ngot: %#v\nwant: %#v",
					len(got), len(tt.wantStmt), got, tt.wantStmt)
			}
			for i, g := range got {
				gotTrim := strings.TrimSpace(g)
				wantTrim := strings.TrimSpace(tt.wantStmt[i])
				if gotTrim != wantTrim {
					t.Errorf("statement %d:\n--- got ---\n%s\n--- want ---\n%s",
						i, gotTrim, wantTrim)
				}
			}
		})
	}
}
