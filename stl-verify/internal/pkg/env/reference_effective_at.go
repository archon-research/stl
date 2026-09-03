package env

import (
	"fmt"
	"os"
	"time"
)

// ReferenceEffectiveAtEnv is the override a replay sets to rebuild a past run's reference view.
const ReferenceEffectiveAtEnv = "REFERENCE_EFFECTIVE_AT"

// ReferenceEffectiveAt returns the instant whose reference-table versions a run reads
// (ADR-0006 §4): REFERENCE_EFFECTIVE_AT as RFC 3339 UTC, or now when unset. The result is
// always UTC, whatever zone the caller's clock carries. Persisting it alongside the run is
// VEC-598 (writer_run).
//
// A non-UTC offset and a future instant are rejected rather than converted: reference versions
// are stored and compared in UTC, and an instant the run cannot have observed resolves versions
// that did not exist yet.
func ReferenceEffectiveAt(now time.Time) (time.Time, error) {
	now = now.UTC()

	raw := os.Getenv(ReferenceEffectiveAtEnv)
	if raw == "" {
		return now, nil
	}

	effectiveAt, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing %s=%q as RFC 3339 UTC (e.g. 2026-06-01T00:00:00Z): %w", ReferenceEffectiveAtEnv, raw, err)
	}
	if _, offset := effectiveAt.Zone(); offset != 0 {
		return time.Time{}, fmt.Errorf("%s=%q must be UTC; write the same instant with a Z offset (%s)", ReferenceEffectiveAtEnv, raw, effectiveAt.UTC().Format(time.RFC3339))
	}
	if effectiveAt.After(now) {
		return time.Time{}, fmt.Errorf("%s=%q is in the future (now is %s); a run cannot read reference versions that do not exist yet", ReferenceEffectiveAtEnv, raw, now.Format(time.RFC3339))
	}
	// Parse hands back the Local location when its offset matches, so the location is
	// normalised even though the instant is already right.
	return effectiveAt.UTC(), nil
}
