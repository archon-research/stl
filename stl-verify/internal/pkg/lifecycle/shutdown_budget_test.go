package lifecycle_test

import (
	"testing"
	"time"

	sqsadapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sqs"
	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving/archivingwire"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
)

// The poll loop is a single goroutine, so exactly one of these paths runs per
// shutdown.
func shutdownPathBudgets() map[string]time.Duration {
	return map[string]time.Duration{
		"poll completes after SIGTERM, then its batch is released": sqsadapter.PollBudget(
			sqsadapter.ConfigDefaults().WaitTimeSeconds) + sqsutil.SettleTimeout,

		// Two cleanup budgets: the delete, then the one release that hands back
		// everything the batch left unsettled — RunLoop batches are one chunk.
		"handler drains to its budget, then the batch is settled": sqsutil.DefaultDrainTimeout +
			2*sqsutil.SettleTimeout,
	}
}

func shutdownTailBudget() time.Duration {
	return archivingwire.DrainTimeout + telemetry.ShutdownFlushTimeout
}

// Every ceiling below is satisfied by a zero budget, which would abandon work
// that had already succeeded rather than wait the moment out.
func TestEveryShutdownBudgetHasAFloor(t *testing.T) {
	budgets := map[string]time.Duration{
		"archivingwire.DrainTimeout":     archivingwire.DrainTimeout,
		"sqsutil.DefaultDrainTimeout":    sqsutil.DefaultDrainTimeout,
		"sqsutil.SettleTimeout":          sqsutil.SettleTimeout,
		"telemetry.ShutdownFlushTimeout": telemetry.ShutdownFlushTimeout,
	}
	for name, budget := range budgets {
		t.Run(name, func(t *testing.T) {
			if budget <= 0 {
				t.Errorf("%s is %s; a non-positive budget abandons work instead of waiting for it", name, budget)
			}
		})
	}
}

// A write the drain gives up on is unrecoverable: its SQS message is already
// deleted, so nothing retries it. The drain must therefore outlast the bound
// the write itself runs under, or a healthy-but-slow PUT is lost on rollout.
func TestTheArchiveDrainOutlastsOneWritesOwnTimeout(t *testing.T) {
	if archivingwire.DrainTimeout < archiving.ArchiveTimeout {
		t.Errorf("archivingwire.DrainTimeout (%s) is shorter than archiving.ArchiveTimeout (%s), "+
			"so a write still inside its own budget is abandoned and counted lost",
			archivingwire.DrainTimeout, archiving.ArchiveTimeout)
	}
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
