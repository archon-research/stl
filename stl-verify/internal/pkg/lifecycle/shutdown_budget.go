package lifecycle

import "time"

// ShutdownTimeout bounds the graceful shutdown after ctx is cancelled. It is
// the first stage of a chain that has to fit PodTerminationGracePeriod end to
// end, because the kubelet's clock starts at SIGTERM and keeps running through
// everything the process still does after Run returns:
//
//	stage                                          budget
//	-------------------------------------------------------
//	1. Run, bounded by ShutdownTimeout                40s  (longest real path: 30s)
//	2. deferred archive drain (archivingwire)          5s  \ ShutdownTailBudget
//	3. deferred OTEL flush (telemetry)                10s  / (15s)
//	-------------------------------------------------------
//	worst case                                        55s  < PodTerminationGracePeriod (60s)
//
// Stage 1's 30s is the worse of the two mutually exclusive paths through
// sqsutil.RunLoop — a poll loop is a single goroutine, so exactly one runs per
// shutdown:
//
//	poll : one long poll finishing (sqs.PollBudget: SQS wait 20s + 5s slack)
//	       plus releasing the batch it returns (1x ShutdownCleanupTimeout) = 30s
//	drain: a handler draining to sqsutil.DefaultDrainTimeout (15s), then its
//	       message settled (delete, then release when that delete fails) and the
//	       rest of the batch released — 3x ShutdownCleanupTimeout           = 30s
//
// ShutdownTimeout keeps 10s over that path for Stop() overhead. Raising any
// budget in the chain means re-deriving all of it; shutdown_budget_test.go
// asserts the whole chain from the real constants.
const ShutdownTimeout = 40 * time.Second

// ShutdownTailBudget bounds the two budgeted things a binary still runs AFTER
// Run returns — the deferred archive drain and the OTEL flush — since the
// kubelet's grace period covers Run's window and that tail together;
// archivingwire.DrainTimeout and telemetry.ShutdownFlushTimeout must sum to no
// more than this. The resource Close() calls deferred alongside them (pgx pool,
// Redis, RPC) carry no budget of their own and are assumed prompt.
const ShutdownTailBudget = 15 * time.Second

// PodTerminationGracePeriod mirrors the terminationGracePeriodSeconds every
// worker Deployment declares, and is the hard ceiling every shutdown budget
// must fit: past it the kubelet SIGKILLs, so a worker that has not settled its
// in-flight SQS message strands it for the queue's visibility timeout. Raising
// ShutdownTimeout above this requires raising the manifests first (and, for
// anything still on ECS Fargate, that task definition's stopTimeout).
const PodTerminationGracePeriod = 60 * time.Second
