package sqs

import (
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
)

// TestConfigDefaults pins the consumer defaults. The critical invariant is the
// one the worker enforces at boot, so it is asserted through the same check
// rather than restated here: a future change to either side fails loudly
// instead of silently re-opening the duplicate-processing hazard.
func TestConfigDefaults(t *testing.T) {
	d := ConfigDefaults()

	visibility := time.Duration(d.VisibilityTimeout) * time.Second
	if err := sqsutil.ValidateVisibilityTimeout(visibility, 0, 1); err != nil {
		t.Errorf("the default visibility timeout cannot cover one message per receive: %v", err)
	}
	if d.WaitTimeSeconds != 20 {
		t.Errorf("WaitTimeSeconds = %d, want 20", d.WaitTimeSeconds)
	}
}
