// Tests for check-sql-comments.sh. The script's first version exited 0 having
// checked nothing (bash 3.2 has no mapfile), so a false pass is the failure
// mode these cases exist to catch.
package ci_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func scriptPath(t *testing.T) string {
	t.Helper()
	abs, err := filepath.Abs("check-sql-comments.sh")
	if err != nil {
		t.Fatalf("resolving script path: %v", err)
	}
	if _, err := os.Stat(abs); err != nil {
		t.Fatalf("script not found: %v", err)
	}
	return abs
}

// gitRepo returns an initialised repository rooted at a temp dir. The script
// resolves paths from the repo root, so every case needs one.
func gitRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, args := range [][]string{
		{"init", "-q", "-b", "main"},
		{"config", "user.email", "ci@example.com"},
		{"config", "user.name", "ci"},
		{"config", "commit.gpgsign", "false"},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	return dir
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func write(t *testing.T, dir, name, body string) {
	t.Helper()
	full := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

// run executes the script in dir and returns exit code plus combined output.
func run(t *testing.T, dir string, env []string, args ...string) (int, string) {
	t.Helper()
	cmd := exec.Command(scriptPath(t), args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	code := 0
	if exit, ok := err.(*exec.ExitError); ok {
		code = exit.ExitCode()
	} else if err != nil {
		t.Fatalf("running script: %v\n%s", err, out)
	}
	return code, string(out)
}

// rule is a divider line of the shape used as a section banner in the migrations.
const rule = "-- ---------------------------------------------------------------------------"

func block(n int) string {
	lines := make([]string, 0, n+2)
	for i := range n {
		lines = append(lines, "-- line")
		_ = i
	}
	lines = append(lines, "SELECT 1;", "")
	return strings.Join(lines, "\n")
}

func TestExplicitFiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		env      []string
		wantCode int
		wantOut  []string
	}{
		{
			name:     "block at the cap passes",
			body:     block(3),
			wantCode: 0,
		},
		{
			name:     "block over the cap fails and is located",
			body:     block(4),
			wantCode: 1,
			wantOut:  []string{"a.sql:1: comment block of 4 lines exceeds 3", "-- line"},
		},
		{
			name:     "exemption directly above the block passes",
			body:     "-- lint:allow-long-comment\n" + block(9),
			wantCode: 0,
		},
		{
			name:     "exemption one line further up does not apply",
			body:     "-- lint:allow-long-comment\n\n" + block(9),
			wantCode: 1,
		},
		{
			name:     "directive is not counted as part of the block",
			body:     "-- lint:allow-long-comment\n" + block(3),
			wantCode: 0,
		},
		{
			name:     "cap is configurable upwards",
			body:     block(5),
			env:      []string{"SQL_COMMENT_MAX_LINES=5"},
			wantCode: 0,
		},
		{
			name:     "cap is configurable downwards",
			body:     block(2),
			env:      []string{"SQL_COMMENT_MAX_LINES=1"},
			wantCode: 1,
			wantOut:  []string{"comment block of 2 lines exceeds 1"},
		},
		{
			name:     "indented comments are counted",
			body:     "    -- a\n\t-- b\n  -- c\n -- d\nSELECT 1;\n",
			wantCode: 1,
		},
		{
			name:     "a statement between comments resets the run",
			body:     "-- a\n-- b\nSELECT 1;\n-- c\n-- d\nSELECT 2;\n",
			wantCode: 0,
		},
		{
			name:     "blank line resets the run",
			body:     "-- a\n-- b\n\n-- c\n-- d\n",
			wantCode: 0,
		},
		{
			name:     "banner of dividers around two lines passes",
			body:     rule + "\n-- Section heading\n-- second line of heading\n" + rule + "\nSELECT 1;\n",
			wantCode: 0,
		},
		{
			name:     "dividers do not count toward the cap",
			body:     rule + "\n-- a\n--\n-- b\n-- c\n" + rule + "\nSELECT 1;\n",
			wantCode: 0,
		},
		{
			name:     "a divider cannot split a long block",
			body:     "-- a\n-- b\n" + rule + "\n-- c\n-- d\nSELECT 1;\n",
			wantCode: 1,
			wantOut:  []string{"comment block of 4 lines exceeds 3"},
		},
		{
			name:     "a bare -- cannot split a long block",
			body:     "-- a\n-- b\n--\n-- c\n-- d\nSELECT 1;\n",
			wantCode: 1,
			wantOut:  []string{"comment block of 4 lines exceeds 3"},
		},
		{
			name:     "a block of dividers alone is never reported",
			body:     rule + "\n" + rule + "\n" + rule + "\n" + rule + "\n" + rule + "\nSELECT 1;\n",
			wantCode: 0,
		},
		{
			name:     "the reported location is the first prose line, not the divider",
			body:     rule + "\n" + block(4),
			wantCode: 1,
			wantOut:  []string{"a.sql:2: comment block of 4 lines exceeds 3"},
		},
		{
			name:     "exemption above a banner still applies to the prose inside it",
			body:     "-- lint:allow-long-comment\n" + rule + "\n" + block(9),
			wantCode: 0,
		},
		{
			name:     "a divider does not carry the exemption forward to the next block",
			body:     "-- lint:allow-long-comment\n" + block(9) + rule + "\n" + block(9),
			wantCode: 1,
		},
		{
			name:     "a directive BELOW a long block does not exempt it",
			body:     "-- a\n-- b\n-- c\n-- d\n-- lint:allow-long-comment\n-- e\nSELECT 1;\n",
			wantCode: 1,
			wantOut:  []string{"a.sql:1: comment block of 4 lines exceeds 3"},
		},
		{
			name:     "the second block in a file can be the exempted one",
			body:     block(4) + "-- lint:allow-long-comment\n" + block(9),
			wantCode: 1,
			wantOut:  []string{"comment block of 4 lines exceeds 3"},
		},
		{
			name:     "an exempted first block does not exempt a later one",
			body:     "-- lint:allow-long-comment\n" + block(9) + block(5),
			wantCode: 1,
			wantOut:  []string{"comment block of 5 lines exceeds 3"},
		},
		{
			name:     "each offending block is reported at its OWN line",
			body:     block(4) + block(5),
			wantCode: 1,
			wantOut:  []string{"a.sql:1: comment block of 4 lines", "a.sql:6: comment block of 5 lines"},
		},
		{
			name:     "every offending block in a file is reported",
			body:     block(4) + block(5),
			wantCode: 1,
			wantOut:  []string{"comment block of 4 lines", "comment block of 5 lines"},
		},
		{
			name:     "block running to end of file is reported",
			body:     "-- a\n-- b\n-- c\n-- d\n",
			wantCode: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := gitRepo(t)
			write(t, dir, "a.sql", tc.body)
			code, out := run(t, dir, tc.env, "a.sql")
			if code != tc.wantCode {
				t.Fatalf("exit = %d, want %d\n%s", code, tc.wantCode, out)
			}
			for _, want := range tc.wantOut {
				if !strings.Contains(out, want) {
					t.Errorf("output missing %q\n%s", want, out)
				}
			}
		})
	}
}

func TestNonSQLFileIsIgnored(t *testing.T) {
	t.Parallel()
	dir := gitRepo(t)
	write(t, dir, "notes.md", block(20))
	if code, out := run(t, dir, nil, "notes.md"); code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, out)
	}
}

func TestMissingPathIsSkipped(t *testing.T) {
	t.Parallel()
	dir := gitRepo(t)
	if code, out := run(t, dir, nil, "gone.sql"); code != 0 {
		t.Fatalf("exit = %d, want 0\n%s", code, out)
	}
}

func TestSecondFileOffendsAndFails(t *testing.T) {
	t.Parallel()
	dir := gitRepo(t)
	write(t, dir, "clean.sql", block(3))
	write(t, dir, "dirty.sql", block(4))
	code, out := run(t, dir, nil, "clean.sql", "dirty.sql")
	if code != 1 {
		t.Fatalf("exit = %d, want 1\n%s", code, out)
	}
	if !strings.Contains(out, "dirty.sql:1") || strings.Contains(out, "clean.sql:") {
		t.Errorf("wrong file blamed\n%s", out)
	}
}

// The diff mode is what runs in CI, and its scope — files ADDED versus the base
// — is what keeps the checksum-frozen migrations on main out of scope.
func TestDiffMode(t *testing.T) {
	t.Parallel()

	baseRepo := func(t *testing.T) string {
		dir := gitRepo(t)
		write(t, dir, "db/frozen.sql", block(20))
		git(t, dir, "add", "-A")
		git(t, dir, "commit", "-qm", "base with a long block")
		git(t, dir, "checkout", "-qb", "topic")
		return dir
	}

	t.Run("added offender fails", func(t *testing.T) {
		t.Parallel()
		dir := baseRepo(t)
		write(t, dir, "db/new.sql", block(4))
		git(t, dir, "add", "-A")
		git(t, dir, "commit", "-qm", "add migration")
		code, out := run(t, dir, []string{"BASE=main"})
		if code != 1 {
			t.Fatalf("exit = %d, want 1\n%s", code, out)
		}
		if !strings.Contains(out, "db/new.sql:1") {
			t.Errorf("offender not named\n%s", out)
		}
		if strings.Contains(out, "db/frozen.sql") {
			t.Errorf("base file must be out of scope\n%s", out)
		}
	})

	t.Run("added clean file passes", func(t *testing.T) {
		t.Parallel()
		dir := baseRepo(t)
		write(t, dir, "db/new.sql", block(3))
		git(t, dir, "add", "-A")
		git(t, dir, "commit", "-qm", "add migration")
		if code, out := run(t, dir, []string{"BASE=main"}); code != 0 {
			t.Fatalf("exit = %d, want 0\n%s", code, out)
		}
	})

	t.Run("modifying a long base file is out of scope", func(t *testing.T) {
		t.Parallel()
		dir := baseRepo(t)
		write(t, dir, "db/frozen.sql", block(20)+"SELECT 2;\n")
		git(t, dir, "add", "-A")
		git(t, dir, "commit", "-qm", "touch frozen")
		if code, out := run(t, dir, []string{"BASE=main"}); code != 0 {
			t.Fatalf("exit = %d, want 0\n%s", code, out)
		}
	})

	t.Run("branch adding no sql is silent", func(t *testing.T) {
		t.Parallel()
		dir := baseRepo(t)
		write(t, dir, "README.md", "hello\n")
		git(t, dir, "add", "-A")
		git(t, dir, "commit", "-qm", "docs")
		code, out := run(t, dir, []string{"BASE=main"})
		if code != 0 || strings.Contains(out, "frozen") {
			t.Fatalf("exit = %d, out = %q; want a silent pass", code, out)
		}
	})

	// A base that cannot be resolved must fail loudly. Exiting 0 here is the
	// false pass that makes the whole check worthless.
	t.Run("unresolvable base exits 2", func(t *testing.T) {
		t.Parallel()
		dir := baseRepo(t)
		write(t, dir, "db/new.sql", block(4))
		git(t, dir, "add", "-A")
		git(t, dir, "commit", "-qm", "add migration")
		code, out := run(t, dir, []string{"BASE=no/such/ref"})
		if code != 2 {
			t.Fatalf("exit = %d, want 2\n%s", code, out)
		}
		if !strings.Contains(out, "not found") {
			t.Errorf("no diagnostic\n%s", out)
		}
	})
}
