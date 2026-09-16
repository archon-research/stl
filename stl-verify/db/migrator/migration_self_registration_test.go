package migrator

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Each migration ends by recording its own name in `migrations`, so a file renamed before it is applied
// leaves a name no file carries and the file itself unrecorded. Nothing else reads the two together.
func TestEveryMigrationRegistersItsOwnFilename(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(thisFile), "..", "migrations")
	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("list migrations in %s: %d files, err %v", dir, len(files), err)
	}
	for _, path := range files {
		name := filepath.Base(path)
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !strings.Contains(string(body), "VALUES ('"+name+"')") {
			t.Errorf("%s does not register itself: no INSERT INTO migrations (filename) VALUES ('%s')", name, name)
		}
	}
}
