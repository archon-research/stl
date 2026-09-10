package transform_bootstrap

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// TestRun_RejectsNonPositiveStep covers the guard that protects the per-window
// loop in bootstrapSource: a non-positive step never advances it, so Run must
// reject one instead of spinning forever. It rejects before touching the pool,
// which is why a nil pool is safe here — and is itself the assertion that the
// guard runs first.
func TestRun_RejectsNonPositiveStep(t *testing.T) {
	tests := []struct {
		name string
		step time.Duration
	}{
		{name: "zero", step: 0},
		{name: "negative", step: -time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Run(context.Background(), nil, Params{Step: tt.step}, slog.Default())
			if err == nil {
				t.Fatalf("Run(step=%v) = nil, want an error", tt.step)
			}
			if !strings.Contains(err.Error(), "step must be positive") {
				t.Fatalf("Run(step=%v) error = %q, want it to name the step guard", tt.step, err)
			}
		})
	}
}

// TestParseTime covers the two forms an operator supplies BOOTSTRAP_FROM in, and
// that anything else is rejected rather than silently treated as the zero time —
// which Run would read as the "derive per source" sentinel and quietly backfill a
// different window than the one asked for.
func TestParseTime(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    time.Time
		wantErr bool
	}{
		{
			name: "date only",
			in:   "2025-01-01",
			want: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "rfc3339 utc",
			in:   "2025-01-01T00:00:00Z",
			want: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "rfc3339 with offset is normalised to utc",
			in:   "2025-01-01T02:00:00+02:00",
			want: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{name: "empty", in: "", wantErr: true},
		{name: "not a date", in: "yesterday", wantErr: true},
		{name: "us order", in: "01/02/2025", wantErr: true},
		{name: "date with trailing text", in: "2025-01-01 plus a bit", wantErr: true},
		{name: "month out of range", in: "2025-13-01", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTime(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ParseTime(%q) = %v, want an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseTime(%q): %v", tt.in, err)
			}
			if !got.Equal(tt.want) {
				t.Fatalf("ParseTime(%q) = %v, want %v", tt.in, got, tt.want)
			}
			if got.Location() != time.UTC {
				t.Fatalf("ParseTime(%q) location = %v, want UTC", tt.in, got.Location())
			}
		})
	}
}
