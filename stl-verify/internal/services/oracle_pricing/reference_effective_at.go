package oracle_pricing

import (
	"fmt"
	"time"
)

// ReferenceEffectiveAtEnv names the override a replay sets to rebuild the reference view a
// past run used.
const ReferenceEffectiveAtEnv = "REFERENCE_EFFECTIVE_AT"

// ResolveReferenceEffectiveAt returns the date whose oracle_asset versions a run reads
// (ADR-0006 §4): the operator-supplied YYYY-MM-DD when raw is set, otherwise now in UTC.
//
// A replay needs to resolve the same reference versions as the run it reproduces, which is
// impossible if the date can only come from the clock. Persisting it alongside the run is
// VEC-598 (writer_run); until then it is supplied here and logged.
func ResolveReferenceEffectiveAt(raw string, now time.Time) (time.Time, error) {
	if raw == "" {
		return now.UTC(), nil
	}
	parsed, err := time.Parse(time.DateOnly, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing %s=%q as YYYY-MM-DD: %w", ReferenceEffectiveAtEnv, raw, err)
	}
	return parsed.UTC(), nil
}
