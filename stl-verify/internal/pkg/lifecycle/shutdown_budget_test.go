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

// The poll loop is a single goroutine, so exactly one of these paths runs per
// shutdown.
func shutdownPathBudgets() map[string]time.Duration {
	return map[string]time.Duration{
		"poll completes after SIGTERM, then its batch is released": sqsadapter.PollBudget(
			sqsadapter.ConfigDefaults().WaitTimeSeconds) + sqsutil.ShutdownCleanupTimeout,

		// Three cleanup budgets: the delete, the release that follows a failed
		// delete, then the release of the rest of the batch.
		"handler drains to its budget, then the batch is settled": sqsutil.DefaultDrainTimeout +
			3*sqsutil.ShutdownCleanupTimeout,
	}
}

func shutdownTailBudget() time.Duration {
	return archivingwire.DrainTimeout + telemetry.ShutdownFlushTimeout
}

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

func TestDeferredTailFitsTheTailBudget(t *testing.T) {
	if got := shutdownTailBudget(); got > lifecycle.ShutdownTailBudget {
		t.Errorf("the deferred shutdown tail needs %s (archive drain %s + OTEL flush %s), "+
			"which exceeds lifecycle.ShutdownTailBudget (%s)",
			got, archivingwire.DrainTimeout, telemetry.ShutdownFlushTimeout, lifecycle.ShutdownTailBudget)
	}
}

// Asserting only the per-stage budgets once hid a tail that pushed the chain to
// 70s, which the kubelet SIGKILLs through.
func TestWholeShutdownChainFitsThePodGracePeriod(t *testing.T) {
	chain := lifecycle.ShutdownTimeout + shutdownTailBudget()
	if chain >= lifecycle.PodTerminationGracePeriod {
		t.Errorf("the whole shutdown chain needs %s (Run %s + tail %s), which does not fit "+
			"the pod grace period (%s)",
			chain, lifecycle.ShutdownTimeout, shutdownTailBudget(), lifecycle.PodTerminationGracePeriod)
	}
}
