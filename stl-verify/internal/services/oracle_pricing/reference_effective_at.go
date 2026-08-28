package oracle_pricing

import (
	"fmt"
	"time"
)

// ReferenceEffectiveAtEnv is the override a replay sets to rebuild a past run's reference view.
const ReferenceEffectiveAtEnv = "REFERENCE_EFFECTIVE_AT"

// ResolveReferenceEffectiveAt returns the instant whose oracle_asset versions a run reads
// (ADR-0006 §4): raw as RFC 3339, or YYYY-MM-DD meaning that day's midnight UTC, falling
// back to now in UTC. Persisting it alongside the run is VEC-598 (writer_run).
func ResolveReferenceEffectiveAt(raw string, now time.Time) (time.Time, error) {
	if raw == "" {
		return now.UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return parsed.UTC(), nil
	}
	parsed, err := time.Parse(time.DateOnly, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing %s=%q as RFC 3339 or YYYY-MM-DD: %w", ReferenceEffectiveAtEnv, raw, err)
	}
	return parsed.UTC(), nil
}
