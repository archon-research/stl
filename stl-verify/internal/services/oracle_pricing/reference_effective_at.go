package oracle_pricing

import (
	"fmt"
	"time"
)

// ReferenceEffectiveAtEnv names the override a replay sets to rebuild the reference view a
// past run used.
const ReferenceEffectiveAtEnv = "REFERENCE_EFFECTIVE_AT"

// ResolveReferenceEffectiveAt returns the instant whose oracle_asset versions a run reads
// (ADR-0006 §4): the operator-supplied RFC 3339 timestamp (or YYYY-MM-DD, meaning that
// day's midnight UTC) when raw is set, otherwise now in UTC.
//
// A replay needs to resolve the same reference versions as the run it reproduces, which is
// impossible if the instant can only come from the clock. Persisting it alongside the run
// is VEC-598 (writer_run); until then it is supplied here and logged.
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
