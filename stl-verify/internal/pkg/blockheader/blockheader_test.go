package blockheader

import (
	"fmt"
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

// A corrupt object can parse to 0 or a negative — ParseInt64 accepts a leading sign — and from a
// batched INSERT that arrives as a CHECK violation whose DETAIL pgx drops, naming no block and
// re-read on every retry. The bound is checked here so the payload is refused where it is read.
func TestParseTimestamp_RefusesInstantsOutsideTheStoredWindow(t *testing.T) {
	for _, c := range []struct{ name, hex string }{
		{"genesis zero", "0x0"},
		{"negative", "-0x1"},
		{"far future", "0xFFFFFFFFFF"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseTimestamp(fmt.Appendf(nil, `{"timestamp":%q}`, c.hex))
			if err == nil {
				t.Fatalf("timestamp %s was accepted; it violates block_meta_ts_sane_chk", c.hex)
			}
			if !strings.Contains(err.Error(), c.hex) {
				t.Errorf("error %q does not name the offending value", err)
			}
		})
	}
}

// The ordinary case still passes, so the bound cannot be satisfied by refusing everything.
func TestParseTimestamp_AcceptsARealHeaderTime(t *testing.T) {
	ts, err := ParseTimestamp([]byte(`{"timestamp":"0x67c00000"}`))
	if err != nil {
		t.Fatalf("ParseTimestamp: %v", err)
	}
	if ts.Unix() != 0x67c00000 {
		t.Errorf("timestamp = %d, want %d", ts.Unix(), 0x67c00000)
	}
}
