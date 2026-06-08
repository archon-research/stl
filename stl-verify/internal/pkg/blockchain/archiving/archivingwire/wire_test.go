package archivingwire

import (
	"testing"
)

func TestEnabled(t *testing.T) {
	t.Setenv("ARCHIVE_SC_CALLS", "true")
	if !Enabled() {
		t.Fatal("Enabled() = false, want true when ARCHIVE_SC_CALLS=true")
	}
	t.Setenv("ARCHIVE_SC_CALLS", "false")
	if Enabled() {
		t.Fatal("Enabled() = true, want false when ARCHIVE_SC_CALLS=false")
	}
	t.Setenv("ARCHIVE_SC_CALLS", "")
	if Enabled() {
		t.Fatal("Enabled() = true, want false when unset")
	}
}

func TestNewS3WrapFromEnvRequiresBucket(t *testing.T) {
	t.Setenv("ARCHIVE_SC_CALLS", "true")
	t.Setenv("RAW_SC_BUCKET", "")
	_, _, err := NewS3WrapFromEnv(t.Context(), nil, 1, 47, "oracle-price")
	if err == nil {
		t.Fatal("expected error when RAW_SC_BUCKET is empty")
	}
}
