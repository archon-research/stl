package lifecycle

import "time"

// ShutdownTimeout bounds the graceful shutdown after ctx is cancelled. It is
// the first stage of a chain that must fit PodTerminationGracePeriod, since the
// kubelet's clock runs from SIGTERM; shutdown_budget_test.go derives the chain.
const ShutdownTimeout = 40 * time.Second

// ShutdownTailBudget bounds what a binary still runs after Run returns — the
// deferred archive drain and OTEL flush, which must sum to no more than this.
// The resource Close() calls beside them carry no budget and are assumed prompt.
const ShutdownTailBudget = 45 * time.Second

// PodTerminationGracePeriod mirrors terminationGracePeriodSeconds on every Go
// worker Deployment: past it the kubelet SIGKILLs. No test can see those
// manifests, so a raise here must reach the cluster before the image reading it.
const PodTerminationGracePeriod = 90 * time.Second
