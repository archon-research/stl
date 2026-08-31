package lifecycle_test

import (
	"testing"
	"time"

	sqsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving/archivingwire"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
)

// shutdownPathBudgets returns the wall time each mutually exclusive path
// through sqsutil.RunLoop needs, derived from the constants that govern it
// rather than restated. The poll loop is a single goroutine, so only one path
// runs per shutdown.
func shutdownPathBudgets() map[string]time.Duration {
	return map[string]time.Duration{
		// One poll running to its budget, then the batch it delivered is released
		// under one shared cleanup budget.
		"poll completes after SIGTERM, then its batch is released": sqsadapter.PollBudget(
			sqsadapter.ConfigDefaults().WaitTimeSeconds) + sqsutil.ShutdownCleanupTimeout,

		// The handler drains to its budget; settling that one message costs a
		// delete, the release that follows a failed delete, and then the release
		// of the rest of the batch — three cleanup budgets, not one.
		"handler drains to its budget, then the batch is settled": sqsutil.DefaultDrainTimeout +
			3*sqsutil.ShutdownCleanupTimeout,
	}
}

// shutdownTailBudget returns what a worker binary still spends after Run
// returns, in the order the deferred calls unwind.
func shutdownTailBudget() time.Duration {
	return archivingwire.DrainTimeout + telemetry.ShutdownFlushTimeout
}

// TestShutdownPathsFitTheLifecycleWindow pins stage 1 of the derivation
// documented on lifecycle.ShutdownTimeout: overrunning it means Stop() is
// abandoned with a message still in flight, which strands its FIFO group for
// the visibility timeout — the blackout these budgets exist to prevent.
func TestShutdownPathsFitTheLifecycleWindow(t *testing.T) {
	for name, budget := range shutdownPathBudgets() {
		t.Run(name, func(t *testing.T) {
			if budget >= lifecycle.ShutdownTimeout {
				t.Errorf("shutdown path needs %s, which does not fit lifecycle.ShutdownTimeout (%s)",
					budget, lifecycle.ShutdownTimeout)
			}
		})
	}
}

// TestDeferredTailFitsTheTailBudget pins stages 2 and 3: both run after Run has
// returned, so they are bounded only by what the kubelet's grace period leaves.
func TestDeferredTailFitsTheTailBudget(t *testing.T) {
	if got := shutdownTailBudget(); got > lifecycle.ShutdownTailBudget {
		t.Errorf("the deferred shutdown tail needs %s (archive drain %s + OTEL flush %s), "+
			"which exceeds lifecycle.ShutdownTailBudget (%s)",
			got, archivingwire.DrainTimeout, telemetry.ShutdownFlushTimeout, lifecycle.ShutdownTailBudget)
	}
}

// TestWholeShutdownChainFitsThePodGracePeriod is the assertion the per-stage
// ones feed: the kubelet SIGKILLs at PodTerminationGracePeriod whichever stage
// is running, and asserting only stage 1 hid a tail that pushed it to 70s.
func TestWholeShutdownChainFitsThePodGracePeriod(t *testing.T) {
	chain := lifecycle.ShutdownTimeout + shutdownTailBudget()
	if chain >= lifecycle.PodTerminationGracePeriod {
		t.Errorf("the whole shutdown chain needs %s (Run %s + tail %s), which does not fit "+
			"the pod grace period (%s)",
			chain, lifecycle.ShutdownTimeout, shutdownTailBudget(), lifecycle.PodTerminationGracePeriod)
	}
}
