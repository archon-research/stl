package blockheader

import (
	"strings"
	"testing"
	"time"
)

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		payload  string
		wantUnix int64
		wantErr  string
	}{
		{
			name:     "valid hex timestamp",
			payload:  `{"timestamp":"0x67c00000"}`,
			wantUnix: 0x67c00000,
		},
		{
			name:     "valid without 0x prefix",
			payload:  `{"timestamp":"67c00000"}`,
			wantUnix: 0x67c00000,
		},
		{
			name:    "missing timestamp field",
			payload: `{"number":"0x1"}`,
			wantErr: "no timestamp field",
		},
		{
			name:    "empty timestamp",
			payload: `{"timestamp":""}`,
			wantErr: "no timestamp field",
		},
		{
			name:    "non-hex timestamp",
			payload: `{"timestamp":"0xzzzz"}`,
			wantErr: "parse block timestamp",
		},
		{
			name:    "int64 overflow (hex wider than 63 bits)",
			payload: `{"timestamp":"0xffffffffffffffff"}`,
			wantErr: "parse block timestamp",
		},
		{
			name:    "malformed json",
			payload: `{"timestamp":`,
			wantErr: "decode block header",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTimestamp([]byte(tt.payload))
			if tt.wantErr != "" {
				assertErrorContains(t, err, tt.wantErr, got)
				return
			}
			assertUTCUnix(t, err, got, tt.wantUnix)
		})
	}
}

func assertErrorContains(t *testing.T, err error, want string, got time.Time) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil (time %s)", want, got)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want it to contain %q", err, want)
	}
}

func assertUTCUnix(t *testing.T, err error, got time.Time, wantUnix int64) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Unix() != wantUnix {
		t.Errorf("timestamp = %d (%s), want %d", got.Unix(), got, wantUnix)
	}
	if loc := got.Location(); loc != nil && loc.String() != "UTC" {
		t.Errorf("timestamp not in UTC: %s", loc)
	}
}
