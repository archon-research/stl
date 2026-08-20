package sqs

import (
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
)

// shutdownPathBudgets returns the wall time each mutually exclusive shutdown
// path needs, derived from the constants that govern it rather than restated.
// The poll loop is a single goroutine, so only one path runs per shutdown.
func shutdownPathBudgets() map[string]time.Duration {
	consumer := newTestConsumer(newBlockingReceiveAPI())
	return map[string]time.Duration{
		"poll completes after SIGTERM, then its batch is released":  consumer.pollBudget() + sqsutil.ShutdownCleanupTimeout,
		"handler drains to its budget, then its message is settled": sqsutil.DefaultDrainTimeout + sqsutil.ShutdownCleanupTimeout,
	}
}

// TestShutdownPathsFitTheLifecycleWindow pins the derivation documented on
// lifecycle.ShutdownTimeout: overrunning it means Stop() is abandoned with a
// message still in flight, which strands its FIFO group for the visibility
// timeout — the blackout these budgets exist to prevent.
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

// TestShutdownPathsFitThePodGracePeriod covers the workers with no
// lifecycle.Run window of their own (dex-indexer returns straight out of
// sqsutil.RunLoop), for which the kubelet's grace period is the only bound.
func TestShutdownPathsFitThePodGracePeriod(t *testing.T) {
	if lifecycle.ShutdownTimeout >= lifecycle.PodTerminationGracePeriod {
		t.Errorf("lifecycle.ShutdownTimeout (%s) must leave margin under the pod grace period (%s)",
			lifecycle.ShutdownTimeout, lifecycle.PodTerminationGracePeriod)
	}
	for name, budget := range shutdownPathBudgets() {
		t.Run(name, func(t *testing.T) {
			if budget >= lifecycle.PodTerminationGracePeriod {
				t.Errorf("shutdown path needs %s, which does not fit the pod grace period (%s)",
					budget, lifecycle.PodTerminationGracePeriod)
			}
		})
	}
}
