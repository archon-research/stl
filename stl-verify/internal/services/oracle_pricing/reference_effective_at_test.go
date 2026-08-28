package oracle_pricing

import (
	"testing"
	"time"
)

func TestResolveReferenceEffectiveAt(t *testing.T) {
	now := time.Date(2026, 8, 21, 14, 30, 0, 0, time.FixedZone("CEST", 2*60*60))

	for _, tc := range []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "unset falls back to the run's start in UTC", raw: "", want: "2026-08-21T12:30:00Z"},
		{name: "a supplied date wins over the clock", raw: "2026-06-01", want: "2026-06-01T00:00:00Z"},
		{name: "a supplied timestamp wins over the clock", raw: "2026-06-01T14:30:00Z", want: "2026-06-01T14:30:00Z"},
		{name: "a zoned timestamp is normalized to UTC", raw: "2026-06-01T14:30:00+02:00", want: "2026-06-01T12:30:00Z"},
		{name: "an unparseable value fails the run", raw: "01/06/2026", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveReferenceEffectiveAt(tc.raw, now)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ResolveReferenceEffectiveAt(%q) = %s, want an error", tc.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveReferenceEffectiveAt(%q): %v", tc.raw, err)
			}
			if got.Format(time.RFC3339) != tc.want {
				t.Errorf("ResolveReferenceEffectiveAt(%q) = %s, want %s", tc.raw, got.Format(time.RFC3339), tc.want)
			}
		})
	}
}
