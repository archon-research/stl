package sqs

import (
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
)

// TestConfigDefaults pins the consumer defaults. The critical invariant is that
// the visibility timeout exceeds the worker per-message handler budget, so a
// message is never redelivered while its handler is still running. Asserting
// the relationship (not a bare literal) means a future change to either side
// fails loudly instead of silently re-opening the duplicate-processing hazard.
func TestConfigDefaults(t *testing.T) {
	d := ConfigDefaults()

	handlerBudgetSecs := int32(sqsutil.DefaultHandlerTimeout / time.Second)
	if d.VisibilityTimeout <= handlerBudgetSecs {
		t.Errorf("VisibilityTimeout (%ds) must exceed the worker handler budget (%ds)",
			d.VisibilityTimeout, handlerBudgetSecs)
	}
	if d.WaitTimeSeconds != 20 {
		t.Errorf("WaitTimeSeconds = %d, want 20", d.WaitTimeSeconds)
	}
}
