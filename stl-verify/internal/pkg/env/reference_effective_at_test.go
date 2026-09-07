package env

import (
	"testing"
	"time"
)

func TestReferenceEffectiveAt(t *testing.T) {
	// A zoned clock: the unset path must still hand back UTC.
	now := time.Date(2026, 8, 21, 14, 30, 0, 0, time.FixedZone("CEST", 2*60*60))

	for _, tc := range []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "unset falls back to the run's start in UTC", want: "2026-08-21T12:30:00Z"},
		{name: "a supplied instant wins over the clock", raw: "2026-06-01T14:30:00Z", want: "2026-06-01T14:30:00Z"},
		{name: "a zero offset written out is still UTC", raw: "2026-06-01T14:30:00+00:00", want: "2026-06-01T14:30:00Z"},
		{name: "a non-UTC offset fails the run", raw: "2026-06-01T14:30:00+02:00", wantErr: true},
		{name: "a bare date fails the run", raw: "2026-06-01", wantErr: true},
		{name: "an unparseable value fails the run", raw: "01/06/2026", wantErr: true},
		{name: "an instant after the clock fails the run", raw: "2026-08-21T12:30:01Z", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(ReferenceEffectiveAtEnv, tc.raw)

			got, err := ReferenceEffectiveAt(now)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ReferenceEffectiveAt() = %s, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ReferenceEffectiveAt(): %v", err)
			}
			if got.Format(time.RFC3339) != tc.want {
				t.Errorf("ReferenceEffectiveAt() = %s, want %s", got.Format(time.RFC3339), tc.want)
			}
			if got.Location() != time.UTC {
				t.Errorf("ReferenceEffectiveAt() location = %s, want UTC", got.Location())
			}
		})
	}
}
